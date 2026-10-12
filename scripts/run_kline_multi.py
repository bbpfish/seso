#!/usr/bin/env python3
"""K-line scheduler - 四源主采调度（--mode full|incremental + --overwrite 叠加）
版本号运行期 import 自 kline_env.KLINE_VERSION（--version / 启动日志对外显示，本文件不重复定义）。

2026-10-11 架构（职责链流水线 + 文件状态索引）：
  - build_code_list 返回统一名单 codes_all: [(code, is_delisted), ...]；退市牌在发号端分流：
    _Cursor.next(allow_delisted) 按取号源自动过滤，退市号独立队列仅发 sina/tx，不发 ths/tdx。
  - _Cursor 单游标一次取一号（原子自增，无方向互斥、无速率比均衡计算；谁快谁取）。
  - _ensure_parquet 通用保险模块：环节产出列集 ∩ 文件缺列集判定，杜绝无脑补采；
    索引已知缺列时直接映射 full_fetch 重建（不回退读盘，由存量最早日推导重建起点）。
  - _FileIndex 一次性持盘文件状态索引：启动扫描 data/*.parquet footer 元数据（列集/列级
    null 统计/日期上下界），判定由"每阶段每只全量 read_parquet"改为内存索引零 IO；
    写库成功后 _merge_write 增量更新索引（运行中索引反映最近一次写库状态）。
  - 精确判定原则：索引能精确复现读盘版判定结果时才走索引（如列全无 null 即 skip/uptodate、
    gap_fetch 需 last_date、stage_fetch 需 min_date）；缺列直接 full_fetch 重建；统计未知
    等无法精确场景自动回退锁内读盘，判定语义与读盘版一致，仅省掉可精确判定的读盘。
  - effective_end 时间边界：full -> min(end_date|today, prev_trading_day(cn_today()))。
  - run_collect 职责链流水线（无批次 join）：每源一线程贯穿各自阶段链
    （ths: standard→adjust→incremental；tdx: standard→incremental；
     sina/tx: standard→adjust→turnover_hist），快源完成 standard 份额即进入补采，不被慢源卡住；
    补采遇 standard 文件缺失（std_missing）记 pending，轮询待 standard 全部完成后补（std_done 事件）；
    目标列已齐全的 skip 绝不进 pending（语义区分，杜绝存量误标待补）。
    当日增量结束后统一执行当日 turnover 后置补齐（THS 批量预取主补 + TDX 备源）。
  - 四客户端 standard 均不携带/合并 turnover：ths 调 fetch_ths_daily(with_turnover=False)、
    tdx 客户端内置 turnover=NaN；历史段 turnover 唯一写入方 = Sina/TX；当日 turnover = 后置补齐环节。

模式与开关：
  --mode full|incremental（默认 incremental）
    full         全量：阶段一 standard（四源）+ 阶段二 adjust/turnover_hist + 自动当日增量闭环
    incremental  增量：只采最近一天（ths+tdx 两路）+ 当日 turnover 后置补齐
  --overwrite   独立叠加开关：跳过检测直接覆盖（full 历史段/当日行均覆盖；
                incremental 当日行覆盖时保留历史存量，防整文件被 1 行替换）

数据完备判定（9 列契约 date/open/high/low/close/volume/adj_close/hfq_close/turnover）：
  文件不存在/无数据 = 空；环节产出列集 ∩ 文件缺列集非空 = 真缺（采/补）；否则视为正常跳过
  （standard 缺 adj/hfq/turnover 属正常；adjust/turnover_hist 对空文件跳过）。
  补采段起点 = 存量最早残缺日-1；full 空库需 --start-date（有存量时缺省由存量最早日推导）。

写库：standard/incremental 走原列级合并（新数据非 NaN 优先）；adjust/turnover_hist 走局部列写
  （仅更新目标列，新值覆盖旧值、未覆盖处保留，原子替换）；全部在 code 分片锁内（含写前二次确认，不同文件并行写）。

目录定义统一见 kline_env（DATA_DIR / LOG_DIR / CACHE_DIR / SCRIPTS_DIR），本文件不再重复推导。
"""
import argparse
import json
import logging
import logging.handlers
import os
import sys
import threading
import time
from pathlib import Path

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.append(_HERE)
from io_atomic import atomic_replace  # noqa: E402
from trade_calendar import trade_days_between, prev_trading_day, cn_today  # noqa: E402
from kline_env import (CACHE_DIR, DATA_DIR, KLINE_VERSION, LOG_DIR,  # noqa: E402
                       SCRIPTS_DIR)
from kline_ths_client import THSConnectionError  # noqa: E402
from kline_tdx_client import TDXConnectionError  # noqa: E402

logger = logging.getLogger("run_kline_multi")

# ---------------------------------------------------------------- constants
# 目录统一定义在 kline_env（WORK_ROOT/DATA_DIR/LOG_DIR/CACHE_DIR/SCRIPTS_DIR），
# 此处直接引用，不再重复推导（曾以环境变量覆盖，系统/容器未定义，已移除）。
NEED_COLS = [
    "date", "open", "high", "low", "close", "volume",
    "adj_close", "hfq_close", "turnover",
]
# 各阶段产出列集（环节产出列集 ∩ 文件缺列集判定用，v4 §7-#3）
STAGE_COLS = {
    "standard": ["open", "high", "low", "close", "volume"],
    "adjust": ["adj_close", "hfq_close"],
    "turnover_hist": ["turnover"],
    "incremental": NEED_COLS[1:],  # 9 列判定（不含 date）
}
def _load_source_config():
    """源级调度配置（P1 配置外置）：默认值 + 环境变量 JSON 覆盖。

    - KLINE_ADJUST_SEQ：{"ths": ["qfq","hfq"], ...} 各源复权阶段序列
    - KLINE_SLEEP_MAP：{"ths": 0.15, ...} 各源请求间隔秒数
    仅当环境变量为合法 JSON 对象时覆盖对应键，其余保留默认（不因单键损坏影响全链路）。"""
    cfg = {
        "ADJUST_SEQ": {"ths": ["qfq", "hfq"], "sina": ["qfq", "hfq"],
                       "tx": ["qfq", "hfq"], "tdx": []},
        "SLEEP_MAP": {"ths": 0.15, "sina": 0.02, "tx": 0.02, "tdx": 0.05},
    }
    for env_key, key in (("KLINE_ADJUST_SEQ", "ADJUST_SEQ"),
                         ("KLINE_SLEEP_MAP", "SLEEP_MAP")):
        raw = os.environ.get(env_key)
        if not raw:
            continue
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                cfg[key] = {str(k): v for k, v in data.items()}
                logger.info("[config] %s 覆盖生效：%s", env_key, cfg[key])
        except (ValueError, TypeError) as e:
            logger.warning("[config] %s 解析失败，保留默认：%s", env_key, e)
    return cfg


_SRC_CONFIG = _load_source_config()
ADJUST_SEQ = _SRC_CONFIG["ADJUST_SEQ"]
SLEEP_MAP = _SRC_CONFIG["SLEEP_MAP"]

# 写锁（P1 分片）：同一 code 串行、不同 code 并行写库。
# 全局锁曾把四源全部写库串行化（一文件写其余全等）；按 code 分片后
# 只有同一文件的并发写被互斥（原子替换保证），跨文件写并行。
_write_locks = {}
_write_locks_guard = threading.Lock()


def _lock_for(code):
    """code 专属写锁（首次访问创建；guard 防多线程并发创建竞态）。"""
    with _write_locks_guard:
        return _write_locks.setdefault(code, threading.Lock())



# ---------------------------------------------------------------- logging
def _setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if root.handlers:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "run_kline_multi.log", maxBytes=20 * 1024 * 1024,
        backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)


# ---------------------------------------------------------------- helpers
def _patch_requests_timeout():
    try:
        import requests
        if not getattr(requests, "_marvis_timeout_patched", False):
            _orig = requests.Session.request

            def _req(self, method, url, **kwargs):
                kwargs.setdefault("timeout", 15)
                return _orig(self, method, url, **kwargs)

            requests.Session.request = _req
            requests._marvis_timeout_patched = True
            logger.info("requests timeout patched to 15s")
    except Exception as e:
        logger.warning("requests timeout patch failed: %s", str(e)[:80])


# ---------------------------------------------------------------- 改动点 #10（文件状态索引）
_FALLBACK = object()  # 索引无法精确判定时回退读盘的哨兵


def _column_stats(pf, col_idx):
    """聚合 parquet 全部 row group 的单列 statistics -> (null_count, min, max, ok)。

    ok=False 表示该列无 statistics / 类型不支持，调用方按"统计未知"处理并回退读盘。
    仅读 footer 元数据，不加载数据（文件状态索引核心）。"""
    null_count = None
    mn = mx = None
    try:
        for rg in range(pf.metadata.num_row_groups):
            st = pf.metadata.row_group(rg).column(col_idx).statistics
            if st is None or getattr(st, "null_count", None) is None:
                return None, None, None, False
            null_count = (null_count or 0) + st.null_count
            if st.min is None or st.max is None:
                return None, None, None, False
            vmin, vmax = pd.Timestamp(st.min), pd.Timestamp(st.max)
            if mn is None or vmin < mn:
                mn = vmin
            if mx is None or vmax > mx:
                mx = vmax
        return null_count, mn, mx, True
    except Exception:
        return None, None, None, False


class _FileIndex:
    """一次性持盘文件状态索引（文件状态索引）：启动时扫描 data/*.parquet 元信息
    （列集 / 每列 null 统计 / 日期上下界 / 行数），供 _ensure_parquet 零 IO 判定。

    - 只读 parquet footer 元数据（schema + statistics），不加载数据；
    - _merge_write 写库成功后增量更新，运行中索引始终反映"最近一次写库"状态；
    - 统计不可靠（null_cols=None）或判定所需字段缺失时，调用方回退 _read_existing
      读盘，保证与读盘版语义完全一致（索引只是"能精确判定时省读盘"的加速层，
      不改变任何判定结果）。
    """

    def __init__(self):
        self._meta = {}
        self._lock = threading.Lock()

    def scan(self):
        """启动一次性扫描：遍历 data 目录全部 parquet 文件的 footer 元信息。"""
        metas = {}
        for fp in sorted(DATA_DIR.glob("*.parquet")):
            m = self._read_meta(fp)
            if m is not None:
                metas[fp.stem] = m
        with self._lock:
            self._meta = metas
        logger.info("[index] 扫描持盘文件 %d 只，索引就绪", len(metas))

    @staticmethod
    def _read_meta(fp):
        """读取单文件元信息；空文件/解析失败返回 None（调用方按空文件处理）。"""
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(fp)
            if pf.metadata.num_rows <= 0:
                return None
            names = pf.schema.names
            cols = set(names)
            nulls, known = set(), True
            for c in names:
                if c == "date":
                    continue
                null_cnt, _, _, ok = _column_stats(pf, names.index(c))
                if not ok or null_cnt is None:
                    known = False
                elif null_cnt > 0:
                    nulls.add(c)
            min_date = last_date = None
            if "date" in cols:
                null_cnt, vmin, vmax, ok = _column_stats(pf, names.index("date"))
                if ok and vmin is not None and vmax is not None:
                    min_date = vmin.normalize()
                    last_date = vmax.normalize()
            return {"cols": cols, "null_cols": nulls if known else None,
                    "min_date": min_date, "last_date": last_date,
                    "rows": pf.metadata.num_rows}
        except Exception as e:
            logger.warning("[index] 扫描 %s 失败：%s", fp.name, str(e)[:80])
            return None

    def get(self, code):
        return self._meta.get(code)

    def update(self, code, df):
        """写库成功后增量更新：由刚写入的 DataFrame 直接推导元信息。"""
        if df is None or df.empty:
            return
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        names = list(d.columns)
        nulls = set()
        for c in names:
            if c == "date":
                continue
            try:
                if d[c].isna().any():
                    nulls.add(c)
            except Exception:
                nulls.add(c)
        with self._lock:
            self._meta[code] = {  # 单键赋值 O(1)（dict 全量重建 O(n) 无必要）
                "cols": set(names), "null_cols": nulls,
                "min_date": pd.Timestamp(d["date"].min()).normalize(),
                "last_date": pd.Timestamp(d["date"].max()).normalize(),
                "rows": len(d)}

    def verify(self, sample=5):
        """抽样重读校验索引与实际文件一致（P1：索引不盲信 footer）。

        启动后抽样重读 parquet footer 与内存索引对比（rows/末日期），
        不一致仅告警不阻断——判定层已对"统计未知"回退读盘，
        校验的价值是尽早暴露索引与实际文件漂移（如外部工具改文件）。"""
        import random
        codes = list(self._meta.keys())
        if not codes:
            return []
        picked = random.sample(codes, min(sample, len(codes)))
        bad = []
        for code in picked:
            m = self._read_meta(DATA_DIR / f"{code}.parquet")
            cur = self._meta.get(code)
            if m is None or cur is None:
                continue
            if (m.get("rows") != cur.get("rows")
                    or m.get("last_date") != cur.get("last_date")):
                bad.append(code)
        if bad:
            logger.warning("[index] 抽样校验 %d/%d 只与文件不一致：%s",
                           len(bad), len(picked), bad)
        return bad


def _ensure_with_index(stage, produce_cols, mode, overwrite, start_date, end, meta):
    """索引版判定（与读盘版语义一致）。返回 plan；无法精确判定时返回 _FALLBACK。

    精确判定原则：
    - standard：overwrite/空库直接 full_fetch；存量 OHLCV 全部无 null 且到 end -> uptodate；
      否则 gap_fetch；索引已知缺列（含整列 NaN）-> 直接映射 full_fetch 重建（由存量最早日
      推导重建起点，不回退读盘）；统计未知 -> 回退读盘。
    - incremental：count>0 直接采当日；存量到 end 且 8 列整列无 null -> 最新行必完整 -> skip；
      否则回退读盘（需最新行完整性精确判定）。
    - adjust/turnover_hist：meta 缺失（文件不存在/空/未入索引）回退读盘精确判定；
      目标列无 null 且非 overwrite -> skip（已齐全，绝不进 pending）；
      目标列有缺口 -> stage_fetch（count 依赖 min_date，缺失则回退读盘）。
    """
    if stage == "standard":
        if overwrite or meta is None:
            return _plan_fetch(mode, overwrite, None, start_date, end)
        if meta.get("null_cols") is None:
            return _FALLBACK  # 统计未知，无法判断列内 NaN -> 读盘
        for c in ["open", "high", "low", "close", "volume"]:
            if c not in meta["cols"] or c in meta["null_cols"]:
                # 索引已知缺列：直接 full_fetch 重建（由存量最早日推导重建起点）
                if meta.get("min_date") is None:
                    return _FALLBACK  # 无起点信息 -> 回退读盘
                base = pd.Timestamp(meta["min_date"]).normalize() - pd.Timedelta(days=1)
                count = max(trade_days_between(base, end), 1)
                return ("full_fetch", count, base)
        if meta.get("last_date") is None:
            return _FALLBACK
        existing_end = pd.Timestamp(meta["last_date"]).normalize()
        if existing_end >= end:
            return ("uptodate", 0, None)
        count = trade_days_between(existing_end, end)
        return ("gap_fetch", max(count, 1), existing_end)
    if stage == "incremental":
        if overwrite or meta is None:
            return ("incremental_fetch", 1, None)
        if meta.get("last_date") is None:
            return _FALLBACK
        existing_end = pd.Timestamp(meta["last_date"]).normalize()
        count = trade_days_between(existing_end, end)
        if count > 0:
            return ("incremental_fetch", 1, existing_end)
        # 存量已到 end：整列均无 null 时最新行必完整 -> skip（精确）；否则读盘判最新行
        if meta.get("null_cols") is None:
            return _FALLBACK
        if all(c in meta["cols"] and c not in meta["null_cols"] for c in produce_cols):
            return "skip"
        return _FALLBACK
    # adjust / turnover_hist
    if meta is None:
        # 文件不存在/空/未入索引：无法精确区分（可能刚被其他源写入但索引未更新）
        # -> 回退读盘精确判定（读盘版返回 std_missing / skip / stage_fetch）
        return _FALLBACK
    if meta.get("null_cols") is None:
        return _FALLBACK
    miss = [c for c in produce_cols if c not in meta["cols"] or c in meta["null_cols"]]
    if not miss and not overwrite:
        return "skip"
    if meta.get("min_date") is None:
        return _FALLBACK
    count = max(trade_days_between(pd.Timestamp(meta["min_date"]).normalize() - pd.Timedelta(days=1), end), 1)
    return ("stage_fetch", count, pd.Timestamp(meta["min_date"]).normalize() - pd.Timedelta(days=1))


def _read_existing(code):
    """读取单只存量 parquet（无则返回 None）。"""
    fp = DATA_DIR / f"{code}.parquet"
    if not fp.exists():
        return None
    try:
        df = pd.read_parquet(fp)
        if df.empty or "date" not in df.columns:
            return None
        return df
    except Exception as e:
        logger.warning("[%s] 存量读取失败：%s", code, str(e)[:100])
        return None


def _atomic_write(fp, df):
    """DataFrame -> parquet 原子写。

    注意：io_atomic.atomic_replace(tmp, dst) 签名为 (临时文件, 目标文件)，
    必须先写同目录临时 parquet 再原子替换；旧实现直接传 (fp, df)，
    参数顺序与类型均错误（df 非路径），导致写库必然失效。
    """
    fp = Path(fp)
    fp.parent.mkdir(parents=True, exist_ok=True)
    tmp = fp.with_name(f".{fp.name}.tmp")
    df = df.reset_index(drop=True)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.Table.from_pandas(df), tmp)
        atomic_replace(tmp, fp)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_fail_list(source, stage, fail_codes):
    """按阶段留痕失败清单：standard 兼容旧路径 {source}_fail.json，
    adjust/turnover_hist/incremental 独立留痕 {source}_{stage}_fail.json。"""
    if not fail_codes:
        return
    fname = f"{source}_fail.json" if stage == "standard" else f"{source}_{stage}_fail.json"
    fp = LOG_DIR / fname
    try:
        payload = {"stage": stage, "source": source, "fail_codes": fail_codes,
                   "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
        from io_atomic import atomic_write_json
        atomic_write_json(fp, payload)
        logger.warning("[%s-%s] 失败清单 %d 只 -> %s", source, stage, len(fail_codes), fp)
    except Exception as e:
        logger.warning("[%s-%s] 写失败清单异常：%s", source, stage, str(e)[:100])


def effective_end(mode, end_date):
    """时间边界（调度语义）：
    full        -> min(end_date 或 today, prev_trading_day(cn_today()))   # 只到前一日（含）
    incremental -> end_date 或 today                                       # 当日
    返回规范化 pd.Timestamp。"""
    today = pd.Timestamp(cn_today()).normalize()
    end = pd.Timestamp(end_date).normalize() if end_date else today
    if mode == "full":
        prev = pd.Timestamp(prev_trading_day(cn_today())).normalize()
        end = min(end, prev)
    return end


# ---------------------------------------------------------------- 改动点 #1
def build_code_list(use_list, start_date=None, end_date=None):
    """构造统一待采集名单 codes_all: [(code, is_delisted), ...]（含退市，升序）。
    返回单张统一名单；delisted 降级为元数据，
    由消费端（ths/tdx 线程）按 is_delisted 跳过，sina/tx 正常采（保持"退市股由 Sina/TX 采集"能力差异）。
    """
    from stock_code_utils import load_stock_basic_codes, load_delisted_codes, get_exchange_stock_codes
    if not use_list:
        codes = get_exchange_stock_codes()
    else:
        basic_codes = load_stock_basic_codes(start_date, end_date)
        if not basic_codes:
            logger.warning("use_list 但 load_stock_basic_codes 为空，退回 get_exchange_stock_codes")
            codes = get_exchange_stock_codes()
        else:
            codes = basic_codes
    delisted = load_delisted_codes()
    codes = sorted(set(c.rsplit(".", 1)[-1] for c in codes))
    delisted = set(c.rsplit(".", 1)[-1] for c in delisted)
    codes_all = sorted((c, c in delisted) for c in codes)
    logger.info("[build] 统一名单 %d 只（含退市 %d 只）", len(codes_all), len(delisted))
    return codes_all


# ---------------------------------------------------------------- 改动点 #2（简化）
class _Cursor:
    """全局发牌游标：一次取一号，原子自增，无方向互斥、无速率比均衡计算。

    - 名单拆成普通/退市两个队列（各自保持 code 升序，退市号集中独立管理）；
      next(allow_delisted) 在发号端自动分流：ths/tdx 以 allow_delisted=False 取号，
      只发普通号（普通号耗尽即结束，不触碰退市队列）；sina/tx 以 allow_delisted=True
      取号，普通号发完后继续发退市号。退市号全局只发一次（sina/tx 共享游标，谁快谁取），
      不会被 ths/tdx 消耗，也不依赖消费端判断。
    - 所有线程共享同一取号语义：next() 每次返回一个 (code, is_delisted)，谁快谁取；
      "按速率比均衡分配"是共享游标的自然结果，不存在任何显式计算。
    - register/unregister 跟踪并发 worker，最后一个退出时置位 exhausted（阶段完成事件，
      供补采 pending 判定 standard 是否全部结束，避免批次 join 同步）。
    - max_total>0 时对发牌总数设上限（--limit 试点口径）。
    """

    def __init__(self, codes_all, max_total=0):
        self._normal = [x for x in codes_all if not x[1]]
        self._delisted = [x for x in codes_all if x[1]]
        self._i = 0
        self._di = 0
        self._lock = threading.Lock()
        self._max_total = max_total
        self._issued = 0
        self._workers = 0
        self._workers_lock = threading.Lock()
        self.exhausted = threading.Event()

    def next(self, allow_delisted=False):
        """发号端退市分流：普通号任何源可发；退市号仅 allow_delisted=True（sina/tx）可取。"""
        with self._lock:
            if self._max_total > 0 and self._issued >= self._max_total:
                return None
            if self._i < len(self._normal):
                item = self._normal[self._i]
                self._i += 1
                self._issued += 1
                return item
            if not allow_delisted:
                return None  # 普通号发完，ths/tdx 结束（不消耗退市队列）
            if self._di < len(self._delisted):
                item = self._delisted[self._di]
                self._di += 1
                self._issued += 1
                return item
            return None

    def register(self):
        with self._workers_lock:
            self._workers += 1

    def unregister(self):
        with self._workers_lock:
            self._workers -= 1
            if self._workers <= 0:
                self.exhausted.set()


# ---------------------------------------------------------------- 改动点 #4（并入保险模块）
def _plan_fetch(mode, overwrite, existing_df, start_date=None, end_date=None):
    """单只股票拉取方案（standard/incremental 共用）。

    end 由调用方传入 effective_end(mode, end_date) 计算结果。
    返回 (action, count, repair_base)：
      full_fetch         全量采集（空库/overwrite）
      gap_fetch          补齐到 end（存量齐全但不足，count=到 end 缺口数）
      repair_fetch       缺列修复（count=最早残缺日-1 到 end 交易日数）
      incremental_fetch  增量（count=1）
      uptodate           已齐全且到 end
    """
    end = pd.Timestamp(end_date).normalize() if end_date else pd.Timestamp(cn_today()).normalize()
    # 仅 standard（full 模式）使用；incremental 判定在 _ensure_parquet/_ensure_with_index 内联实现
    if overwrite or existing_df is None or existing_df.empty:
        start = pd.Timestamp(start_date).normalize() if start_date else None
        if start is None:
            raise ValueError(
                f"--mode {mode} --overwrite/空库 需 --start-date（存量缺失时无法计算修复起点）")
        count = trade_days_between(start - pd.Timedelta(days=1), end)
        return ("full_fetch", max(count, 1), None)
    existing = existing_df.copy()
    existing["date"] = pd.to_datetime(existing["date"])
    existing_end = pd.Timestamp(existing["date"].max()).normalize()
    # 缺列修复优先：OHLCV 任一列存在 NaN 即真缺（存量即使已到 end 也需修复）
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in existing.columns or existing[c].isna().any():
            # 缺列：补采段起点 = 最早残缺日-1
            bad = existing.loc[existing[c].isna(), "date"]
            repair_base = pd.Timestamp(bad.min()).normalize() - pd.Timedelta(days=1)
            count = trade_days_between(repair_base, end)
            return ("repair_fetch", max(count, 1), repair_base)
    if existing_end >= end:
        return ("uptodate", 0, None)
    count = trade_days_between(existing_end, end)
    return ("gap_fetch", max(count, 1), existing_end)


# ---------------------------------------------------------------- 改动点 #3
def _ensure_parquet(code, stage, produce_cols, mode, overwrite, start_date=None, end_date=None, index=None):
    """通用保险模块（环节产出列集 ∩ 文件缺列集判定 + 文件状态索引加速）：锁内读存量 → 判定 →
    返回 (action, count, repair_base) 或 "skip"（无需处理）。

    - standard：交 _plan_fetch（OHLCV 判定；--overwrite 跳过检测直接覆盖）
    - adjust/turnover_hist：standard 文件缺失（无基础行）-> std_missing（补采期进 pending
      等 standard 建文件；std_done 后仍缺失记 fail）；目标列（复权/turnover）有缺口 -> 补全历史
      （count=覆盖全文件到 effective_end）；齐全 skip（绝不进 pending）；--overwrite 时目标列覆盖更新
    - incremental：--overwrite 直接采当日；最新交易日行缺失/缺列 → 采当日；齐全 skip
    - index 传入 _FileIndex 时优先查内存元信息零 IO 判定；无法精确判定（缺列修复需最早
      残缺日、最新行完整性、统计未知等）自动回退锁内读盘，语义与读盘版完全一致。
    """
    end = effective_end(mode, end_date)
    if index is not None:
        plan = _ensure_with_index(stage, produce_cols, mode, overwrite, start_date, end,
                                  index.get(code))
        if plan is not _FALLBACK:
            return plan
    with _lock_for(code):
        old = _read_existing(code)
        if stage == "standard":
            return _plan_fetch(mode, overwrite, old, start_date, end)
        if stage == "incremental":
            if overwrite:
                return ("incremental_fetch", 1, None)
            if old is None or old.empty:
                return ("incremental_fetch", 1, None)
            old["date"] = pd.to_datetime(old["date"])
            existing_end = pd.Timestamp(old["date"].max()).normalize()
            count = trade_days_between(existing_end, end)
            if count <= 0:
                return "skip"  # 最新交易日已采且无缺口（后置补齐负责 turnover）
            latest_row = old[old["date"] == old["date"].max()]
            miss = [c for c in produce_cols
                    if c not in latest_row.columns or latest_row[c].isna().any()]
            if miss:
                return ("incremental_fetch", 1, existing_end)
            return "skip"
        # adjust / turnover_hist：standard 文件缺失（无基础行，局部列无可更新）
        # -> std_missing（与"目标列已齐全"的 skip 区分：前者允许进 pending 等 standard）
        if old is None or old.empty:
            return ("std_missing", 0, None)
        old["date"] = pd.to_datetime(old["date"])
        miss = [c for c in produce_cols
                if c not in old.columns or old[c].isna().any()]
        if not miss and not overwrite:
            return "skip"
        count = max(trade_days_between(old["date"].min() - pd.Timedelta(days=1), end), 1)
        return ("stage_fetch", count, old["date"].min() - pd.Timedelta(days=1))


# ---------------------------------------------------------------- collect（改动点 #7）
def _collect_standard(fetch_std, code, plan, mode):
    """仅 standard 采集：ths 传 with_turnover=False；tdx 客户端内置 turnover=NaN；
    sina/tx standard 自带全历史 turnover（真实值）随 df 落库。
    mode=incremental 时裁剪只保留最新交易日一行（v4 §7-#7 增量裁剪保留于此）。
    返回 (df | None, msg)。"""
    action, count, repair_base = plan
    if action == "uptodate":
        return None, "uptodate"
    df = fetch_std(code, count, start=repair_base)
    if df is None or df.empty:
        return None, "no_data"
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    if repair_base is not None:
        df = df[df["date"] > repair_base].copy()
    if mode == "incremental":
        latest = df["date"].max()
        df = df[df["date"] == latest].copy()
    if df.empty:
        return None, "uptodate"
    return df, "ok"


def _collect_adjust(fetch_adj, code, count, mode="full", start=None):
    """qfq+hfq 复权采集（阶段二 adjust）：返回 (df[date, adj_close, hfq_close] | None, msg)。
    full：拉全历史复权局部列写；incremental：裁剪最新交易日一行。"""
    if fetch_adj is None:
        return None, "no_adj_cap"
    parts = []
    for adj in ("qfq", "hfq"):
        try:
            sub = fetch_adj(code, count, adj, start)
        except (THSConnectionError, TDXConnectionError):
            raise  # 会话失效穿透到 _run_chain/_drain_pending 触发重建重试
        except Exception as e:
            logger.warning("[%s] adjust %s 请求异常：%s", code, adj, str(e)[:100])
            sub = None
        if sub is None or sub.empty:
            continue
        s = sub.copy()
        s["date"] = pd.to_datetime(s["date"])
        col = "adj_close" if adj == "qfq" else "hfq_close"
        if col in s.columns:
            parts.append(s[["date", col]])
    if not parts:
        return None, "no_data"
    dates = pd.concat([p["date"] for p in parts]).drop_duplicates().sort_values()
    out = pd.DataFrame({"date": dates})
    for p in parts:
        out = out.merge(p, on="date", how="left")
    for _c in ("adj_close", "hfq_close"):
        if _c not in out.columns:
            out[_c] = float("nan")  # 单边复权失败时补 NaN 列防 KeyError（另一边本可写入）
    if mode == "incremental":
        latest = out["date"].max()
        out = out[out["date"] == latest].copy()
    if out.empty or out[["adj_close", "hfq_close"]].dropna(how="all").empty:
        return None, "no_data"
    return out, "ok"


def _collect_turnover_hist(fetch_std, code, count, start=None):
    """sina/tx 拉历史 turnover（阶段二 turnover_hist）：返回 (df[date, turnover] | None, msg)。
    仅提取 turnover 列，供局部列写回填；sina/tx standard 自带真实全历史 turnover。"""
    df = fetch_std(code, count, start)
    if df is None or df.empty:
        return None, "no_data"
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    if "turnover" not in df.columns:
        return None, "no_turnover_col"
    return df[["date", "turnover"]], "ok"


def _collect_incremental(fetch_std, fetch_adj, code, plan, adjust_seq):
    """当日增量采集（9 列）：standard 当日行 + 复权当日行合并；只保留最新交易日一行。
    ths 路 = std + qfq + hfq（3 请求）；tdx 路 = std（1 请求，无复权能力）。"""
    action, count, repair_base = plan
    if action == "uptodate":
        return None, "uptodate"
    df = fetch_std(code, count)
    if df is None or df.empty:
        return None, "no_data"
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    if repair_base is not None:
        df = df[df["date"] > repair_base].copy()
    if fetch_adj is not None:
        for adj in adjust_seq:
            sub = fetch_adj(code, count, adj)
            df = _merge_adj(df, sub, adj)
    latest = df["date"].max()
    df = df[df["date"] == latest].copy()
    if df.empty:
        return None, "uptodate"
    return df, "ok"


def _merge_adj(df, sub, adj):
    """将 sub 的复权列（qfq->adj_close / hfq->hfq_close）合并进 df（左对齐 date）。"""
    if sub is None or sub.empty:
        return df
    s = sub.copy()
    s["date"] = pd.to_datetime(s["date"])
    col = "adj_close" if adj == "qfq" else "hfq_close"
    if col not in s.columns:
        logger.warning("复权列 %s 缺失（源不支持 %s）", col, adj)
        return df
    s = s.drop_duplicates(subset="date", keep="last")  # 上游分段合并异常时的重复行防御
    m = df.set_index("date")
    m[col] = s.set_index("date")[col].reindex(m.index)
    return m.reset_index()


# ---------------------------------------------------------------- 改动点 #8
def _merge_write(code, df, overwrite, stage="standard", index=None):
    """列级合并 / 局部列写写库（stage 感知，全部在 code 分片锁内，含锁内二次确认）：

    - standard / incremental：原列级合并（新数据非 NaN 优先）；overwrite 或空库直接替换；
    - adjust：局部列写 adj_close/hfq_close（仅更新目标列，原子替换）；
    - turnover：局部列写 turnover（同上）。
    返回 msg：new/overwritten/merged/stage_merged/already/skip_empty。
    index 传入 _FileIndex 时，写库成功后同步更新该 code 元信息（增量一致性）。
    锁粒度：按 code 分片（_lock_for），同一文件串行、不同文件并行，四源写库不再全局串行。"""
    fp = DATA_DIR / f"{code}.parquet"
    with _lock_for(code):
        if stage in ("adjust", "turnover_hist"):
            target = ["adj_close", "hfq_close"] if stage == "adjust" else ["turnover"]
            old = _read_existing(code)
            if old is None or old.empty:
                return "skip_empty"  # 空文件跳过（双保险，保险模块已判）
            old["date"] = pd.to_datetime(old["date"])
            if not overwrite:
                # 锁内二次确认：目标列已被其他线程写全则跳过（正倒序相交兜底）
                miss = [c for c in target
                        if c not in old.columns or old[c].isna().any()]
                if not miss:
                    return "already"
            new = df.copy()
            new["date"] = pd.to_datetime(new["date"])
            m = old.set_index("date")
            for c in target:
                if c in new.columns:
                    sub = new[["date", c]].dropna(subset=[c]).set_index("date")
                    # 新值覆盖旧值，sub 未覆盖的日期保留存量旧值（防局部列写清空历史）
                    m[c] = sub[c].reindex(m.index).combine_first(m[c])
            m = m.reset_index().drop_duplicates("date").sort_values("date")
            _atomic_write(fp, m)
            if index is not None:
                index.update(code, m)
            return f"{stage}_merged"
        # standard / incremental
        if not fp.exists():
            _atomic_write(fp, df)
            if index is not None:
                index.update(code, df)
            return "new"
        if overwrite and stage != "incremental":
            # standard 全量拉取（full_fetch 覆盖全历史）直接替换
            _atomic_write(fp, df)
            if index is not None:
                index.update(code, df)
            return "overwritten"
        # incremental（含 overwrite）或非 overwrite：合并写，新数据非 NaN 优先。
        # incremental+overwrite 时 df 只有当日 1 行，直接替换会把历史清成 1 行，
        # 这里合并保留历史存量（当日行覆盖、旧日期保留）。
        try:
            old = pd.read_parquet(fp)
            if old.empty or "date" not in old.columns:
                raise ValueError("bad existing")
        except Exception as e:
            logger.warning("[%s] 存量读取失败(%s)，直接替换", code, str(e)[:80])
            _atomic_write(fp, df)
            if index is not None:
                index.update(code, df)
            return "new"
        new = df.copy()
        new["date"] = pd.to_datetime(new["date"])
        old["date"] = pd.to_datetime(old["date"])
        merged = (new.set_index("date").combine_first(old.set_index("date"))
                  .reset_index().drop_duplicates("date").sort_values("date"))
        for col in NEED_COLS[1:]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")
        _atomic_write(fp, merged)
        if index is not None:
            index.update(code, merged)
        return "overwritten" if overwrite else "merged"


# ---------------------------------------------------------------- fetch 闭包
def _fetch_by_days(fetch_func, code, count, end, adjust="", **kw):
    """sina/tx 按日期区间拉取：以 end 为终点向前 count 个交易日（含 end，日历精确）。

    旧实现按 count+3 自然日反推窗口：A 股约 1.5 自然日/交易日，count>6 即窗口不足，
    stage_fetch/full_fetch 大 count 场景会静默丢失前部数据且下轮 gap 判定不变
    （缺口永久补不上）。改用交易日历精确取起点；日历不可用时降级 count*2+10
    自然日缓冲（覆盖周末+最长法定长假）。start/end 统一传 ISO 字符串，
    规避 gtimg param 对 Timestamp 字符串化的格式问题。"""
    from trade_calendar import load_trading_calendar
    _end = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp(cn_today()).normalize()
    _n = max(count, 1)
    start = None
    cal = load_trading_calendar()
    if cal is not None and len(cal) > 0:
        arr = cal[cal <= _end]
        if len(arr) > 0:
            start = arr[-_n] if len(arr) >= _n else arr[0]
    if start is None:
        start = _end - pd.Timedelta(days=_n * 2 + 10)
    return fetch_func(code, start.strftime("%Y-%m-%d"), _end.strftime("%Y-%m-%d"), adjust, **kw)


def _make_fetches(source, ctx, start_date, end_date):
    """构造 (fetch_std, fetch_adj) 闭包。客户端 lazy import（仅本路线程内加载一次）。
    """
    end = pd.Timestamp(end_date).normalize() if end_date else pd.Timestamp(cn_today()).normalize()
    if source == "ths":
        from kline_ths_client import fetch_ths_daily

        def std(c, n, start=None):
            return fetch_ths_daily(ctx, c, n, start_date=start or start_date, adjust="",
                                   turnover_map={}, with_turnover=False)

        def adj(c, n, a, start=None):
            return fetch_ths_daily(ctx, c, n, start_date=start or start_date, adjust=a,
                                   turnover_map={}, with_turnover=False)
        return std, adj
    if source == "tdx":
        from kline_tdx_client import fetch_tdx_daily

        def std(c, n, start=None):
            return fetch_tdx_daily(c, n, client=ctx)
        return std, None
    if source == "sina":
        from kline_sina_client import fetch_sina_daily
        _f = fetch_sina_daily
    else:
        from kline_tx_client import fetch_tx
        _f = fetch_tx

    def std(c, n, start=None):
        return _fetch_by_days(_f, c, n, end, "")

    def std_th(c, n, start=None):
        # turnover 补采专用：newfqkline 自带 turnover（默认 fqkline 无 turnover 列）
        return _fetch_by_days(_f, c, n, end, "", use_newfqk=True)

    std.turnover_fetch = std_th

    def adj(c, n, a, start=None):
        return _fetch_by_days(_f, c, n, end, a)
    return std, adj


# ---------------------------------------------------------------- 改动点 #6（职责链）
def _process_item(source, stage, code, plan, fetch_std, fetch_adj, adjust_seq,
                  mode, overwrite, touched=None, index=None):
    """单只股票单阶段处理：采 + 写。返回 ("ok", detail) / ("uptodate", None) / ("fail", msg)。"""
    if stage == "incremental":
        df, msg = _collect_incremental(fetch_std, fetch_adj, code, plan, adjust_seq)
    elif stage == "standard":
        df, msg = _collect_standard(fetch_std, code, plan, mode)
    elif stage == "adjust":
        df, msg = _collect_adjust(fetch_adj, code, plan[1], mode="full", start=plan[2])
    else:  # turnover_hist
        f = getattr(fetch_std, "turnover_fetch", None) or fetch_std
        df, msg = _collect_turnover_hist(f, code, plan[1], start=plan[2])
    if df is None:
        if msg == "uptodate":
            return "uptodate", None
        return "fail", msg
    wmsg = _merge_write(code, df, overwrite, stage, index)
    if stage == "incremental" and touched is not None:
        touched.append(code)
    return "ok", (len(df), wmsg)


def _drain_pending(source, stage, pending, fetch_std, fetch_adj, adjust_seq,
                   mode, overwrite, start_date, stage_end, std_done, sleep_gap,
                   index=None, holder=None):
    """补采阶段（adjust/turnover_hist）待补队列：轮询处理直至 standard 全部完成或队列清空。

    快源完成 standard 后立即开始补采（主循环已处理就绪股票）；遇到 standard 文件缺失
    （std_missing）的股票进 pending。此处以 std_done 事件驱动轮询：慢源每完成一只即可补
    一只，直到 standard 全部完成（std_done 置位），对剩余 pending 做最终处理
    （此时 std_missing 即真缺数据，记 fail；skip 为目标列已齐全，跳过）。
    会话失效（THSConnectionError/TDXConnectionError）累计到阈值时重建会话并重试当前股。
    返回 (ok, fail, skip, fail_codes)。"""
    ok = fail = skip = 0
    fail_codes = []
    conn_streak = 0
    while pending and not std_done.is_set():
        time.sleep(1.0)
        still = []
        for code in pending:
            plan = _ensure_parquet(code, stage, STAGE_COLS[stage], mode,
                                   overwrite, start_date, stage_end, index)
            if isinstance(plan, tuple) and plan[0] == "std_missing":
                # standard 仍未建文件：留待下一轮
                still.append(code)
                continue
            if plan == "skip":
                # 目标列已齐全：直接跳过，不再滞留
                skip += 1
                continue
            try:
                r, detail = _process_item(source, stage, code, plan, fetch_std, fetch_adj,
                                          adjust_seq, mode, overwrite, index=index)
            except (THSConnectionError, TDXConnectionError) as e:
                conn_streak += 1
                if conn_streak >= _CONN_RECONNECT_STREAK:
                    ctx = _rebuild_source(source, holder) if holder is not None else None
                    conn_streak = 0
                    if ctx is None:
                        fail += 1
                        fail_codes.append(code)
                        logger.error("[%s-%s] %s 会话重建失败，记 fail", source, stage, code)
                        time.sleep(sleep_gap)
                        continue
                    fetch_std, fetch_adj = _make_fetches(source, ctx, start_date, stage_end)
                    logger.warning("[%s-%s] %s 会话已重建，重试当前股", source, stage, code)
                    time.sleep(sleep_gap)
                    continue
                fail += 1
                fail_codes.append(code)
                logger.warning("[%s-%s] %s 会话失效（连续%d次），暂记 fail，待重建",
                               source, stage, code, conn_streak)
                time.sleep(sleep_gap)
                continue
            except Exception as e:
                fail += 1
                fail_codes.append(code)
                logger.error("[%s-%s] %s 异常：%s", source, stage, code, str(e)[:150])
                continue
            if r == "ok":
                ok += 1
                logger.info("[%s-%s] %s: 补采完成(%s)", source, stage, code, detail[1])
            elif r == "uptodate":
                skip += 1
            else:
                fail += 1
                fail_codes.append(code)
                logger.warning("[%s-%s] %s: 无数据/采集失败(%s)", source, stage, code, detail)
            time.sleep(sleep_gap)
        pending = still
    # standard 已全部完成：最终扫剩余 pending
    # （std_missing 此时为真缺数据，放弃；skip 为目标列已齐全）
    for code in pending:
        plan = _ensure_parquet(code, stage, STAGE_COLS[stage], mode,
                               overwrite, start_date, stage_end, index)
        if isinstance(plan, tuple) and plan[0] == "std_missing":
            fail += 1
            fail_codes.append(code)
            logger.warning("[%s-%s] %s standard 文件缺失（std_done 后仍无），记 fail",
                           source, stage, code)
            continue
        if plan == "skip":
            skip += 1
            continue
        try:
            r, detail = _process_item(source, stage, code, plan, fetch_std, fetch_adj,
                                      adjust_seq, mode, overwrite, index=index)
        except (THSConnectionError, TDXConnectionError) as e:
            conn_streak += 1
            if conn_streak >= _CONN_RECONNECT_STREAK:
                ctx = _rebuild_source(source, holder) if holder is not None else None
                conn_streak = 0
                if ctx is None:
                    fail += 1
                    fail_codes.append(code)
                    logger.error("[%s-%s] %s 会话重建失败，记 fail", source, stage, code)
                    time.sleep(sleep_gap)
                    continue
                fetch_std, fetch_adj = _make_fetches(source, ctx, start_date, stage_end)
                logger.warning("[%s-%s] %s 会话已重建，重试当前股", source, stage, code)
                time.sleep(sleep_gap)
                continue
            fail += 1
            fail_codes.append(code)
            logger.warning("[%s-%s] %s 会话失效（连续%d次），暂记 fail，待重建",
                           source, stage, code, conn_streak)
            time.sleep(sleep_gap)
            continue
        except Exception as e:
            fail += 1
            fail_codes.append(code)
            logger.error("[%s-%s] %s 异常：%s", source, stage, code, str(e)[:150])
            continue
        if r == "ok":
            ok += 1
            logger.info("[%s-%s] %s: 补采完成(%s)", source, stage, code, detail[1])
        elif r == "uptodate":
            skip += 1
        else:
            fail += 1
            fail_codes.append(code)
            logger.warning("[%s-%s] %s: 无数据/采集失败(%s)", source, stage, code, detail)
        time.sleep(sleep_gap)
    return ok, fail, skip, fail_codes


def _run_chain(source, chain, holder, mode, overwrite, start_date, end_date,
               results, touched=None, std_done=None, index=None):
    """单源线程：按职责链依次推进各阶段，阶段间无批次 join 等待。

    chain: [(stage, cursor, stage_end), ...]。共享各阶段 _Cursor 一次取一号；
    补采阶段（adjust/turnover_hist）遇 standard 未就绪股票记 pending，由 _drain_pending
    轮询补齐——快源完成 standard 份额即进入补采，不被慢源卡住。
    发号端按源自动分流退市牌：ths/tdx 取号（allow_delisted=False）跳过退市号，
    sina/tx 取号（allow_delisted=True）正常采退市；fail 清单按阶段留痕。
    holder 为 run_collect 持有的会话容器（单元素列表）：THS/TDX 会话失效时按
    连续命中阈值重建会话并重试当前股票（重建后更新 holder[0]，其它线程/backfill
    取到新会话）；sina/tx 无会话传 None。
    index 传入 _FileIndex 时，各阶段判定优先走内存索引。"""
    t0 = time.time()
    try:
        ctx = holder[0] if holder is not None else None
        fetch_std, fetch_adj = _make_fetches(source, ctx, start_date, end_date)
        adjust_seq = ADJUST_SEQ[source]
        sleep_gap = SLEEP_MAP[source]
        allow_delisted = source in ("sina", "tx")
        conn_streak = 0
        for stage, cursor, stage_end in chain:
            ok = fail = skip = 0
            fail_codes = []
            pending_codes = []
            n = 0
            if stage == "standard":
                cursor.register()
            try:
                while True:
                    item = cursor.next(allow_delisted)
                    if item is None:
                        break
                    code, _ = item
                    try:
                        plan = _ensure_parquet(code, stage, STAGE_COLS[stage], mode,
                                               overwrite, start_date, stage_end, index)
                        if plan == "skip":
                            # 目标列已齐全：正常跳过（绝不进 pending）
                            skip += 1
                            continue
                        if isinstance(plan, tuple) and plan[0] == "std_missing":
                            # standard 文件缺失：仅 std_done 未置位时进 pending 等 standard 建文件
                            if stage in ("adjust", "turnover_hist") and not std_done.is_set():
                                logger.info("[%s-%s] %s standard 未就绪，暂记待补", source, stage, code)
                                pending_codes.append(code)
                                continue
                            fail += 1
                            fail_codes.append(code)
                            logger.warning("[%s-%s] %s standard 文件缺失（std_done 后仍无），记 fail",
                                           source, stage, code)
                            continue
                        r, detail = _process_item(source, stage, code, plan, fetch_std,
                                                  fetch_adj, adjust_seq, mode, overwrite, touched,
                                                  index=index)
                    except (THSConnectionError, TDXConnectionError) as e:
                        conn_streak += 1
                        if conn_streak >= _CONN_RECONNECT_STREAK:
                            ctx = _rebuild_source(source, holder) if holder is not None else None
                            conn_streak = 0
                            if ctx is None:
                                fail += 1
                                fail_codes.append(code)
                                logger.error("[%s-%s] %s 会话重建失败，记 fail", source, stage, code)
                                time.sleep(sleep_gap)
                                continue
                            fetch_std, fetch_adj = _make_fetches(source, ctx, start_date, end_date)
                            logger.warning("[%s-%s] %s 会话已重建，重试当前股", source, stage, code)
                            time.sleep(sleep_gap)
                            continue
                        fail += 1
                        fail_codes.append(code)
                        logger.warning("[%s-%s] %s 会话失效（连续%d次），暂记 fail，待重建",
                                       source, stage, code, conn_streak)
                        time.sleep(sleep_gap)
                        continue
                    except Exception as e:
                        fail += 1
                        fail_codes.append(code)
                        logger.error("[%s-%s] %s 异常：%s", source, stage, code, str(e)[:150])
                        continue
                    n += 1
                    if r == "ok":
                        conn_streak = 0
                        ok += 1
                        if stage in ("adjust", "turnover_hist"):
                            logger.info("[%s-%s] %s: ok(%s)", source, stage, code, detail[1])
                        else:
                            logger.info("[%s-%s] %s: ok(%d 行, %s)", source, stage, code,
                                        detail[0], detail[1])
                    elif r == "uptodate":
                        conn_streak = 0
                        skip += 1
                        logger.info("[%s-%s] %s: 已齐全跳过", source, stage, code)
                    else:
                        fail += 1
                        fail_codes.append(code)
                        logger.warning("[%s-%s] %s: 无数据/采集失败(%s)", source, stage, code, detail)
                    time.sleep(sleep_gap)
                    if n % 200 == 0:
                        logger.info("[%s-%s] 进度 %d 只（ok=%d fail=%d skip=%d）",
                                    source, stage, n, ok, fail, skip)
            finally:
                if stage == "standard":
                    cursor.unregister()
            if pending_codes:
                logger.info("[%s-%s] %d 只待 standard 完成，进入待补轮询", source, stage,
                            len(pending_codes))
                p_ok, p_fail, p_skip, p_fail_codes = _drain_pending(
                    source, stage, pending_codes, fetch_std, fetch_adj, adjust_seq,
                    mode, overwrite, start_date, stage_end, std_done, sleep_gap,
                    index=index, holder=holder)
                ok += p_ok
                fail += p_fail
                skip += p_skip
                fail_codes.extend(p_fail_codes)
            _write_fail_list(source, stage, fail_codes)
            results[(source, stage)] = {"ok": ok, "fail": fail, "skip": skip,
                                        "time_min": (time.time() - t0) / 60}
            logger.info("[%s-%s] stage done! ok=%d fail=%d skip=%d time=%.1fm",
                        source, stage, ok, fail, skip, (time.time() - t0) / 60)
    except Exception as e:
        logger.error("[%s] 职责链异常：%s", source, str(e)[:150])
        results.setdefault((source, "standard"), {"ok": 0, "fail": 0, "skip": 0, "time_min": 0.0})


# ---------------------------------------------------------------- 后置补齐（改动点 #7/#9）
def _apply_today_turnover(code, tv):
    """锁内读存量 → 最新交易日行 turnover 置值 → 原子写（只动当日行，二次确认防重复写）。"""
    with _lock_for(code):
        old = _read_existing(code)
        if old is None or old.empty:
            return
        old["date"] = pd.to_datetime(old["date"])
        latest = old["date"].max()
        mask = old["date"] == latest
        if not mask.any():
            return
        if "turnover" not in old.columns or not old.loc[mask, "turnover"].isna().all():
            return  # 二次确认：无 turnover 列或已被回填则跳过
        old.loc[mask, "turnover"] = tv
        _atomic_write(DATA_DIR / f"{code}.parquet", old)


def _backfill_today_turnover(ths_ctx, tdx_ctx, touched, results):
    """当日 turnover 后置补齐环节（增量采集完成后统一执行，v4 §5.2/§6/§7-#7）：

    - 主源 THS：fetch_ths_turnover_batch 预取当日 map（每市场 1 次批量）→ 回填
      ths 路与 tdx 路所有缺当日 turnover 的股票（命中股 0 额外请求）；
    - 备源 TDX：THS 某市场失败/缺股 → fetch_tdx_turnover_batch([code]) 独立补
      （逐股 finance 计算，每股 +1 请求，常态 0 次）。
    - 仅处理增量阶段实际采到的代码（touched）中最新交易日行 turnover 为 NaN 的股票；
      备源失败只留 NaN + 日志，下交易日增量自动重试。
    """
    if not touched:
        logger.info("[backfill] 当日增量未采任何股票，跳过后置补齐")
        return
    need = []
    for code in touched:
        old = _read_existing(code)
        if old is None or old.empty:
            continue
        old["date"] = pd.to_datetime(old["date"])
        latest = old["date"].max()
        rows = old[old["date"] == latest]
        if rows.empty:
            continue
        if "turnover" in rows.columns and not rows["turnover"].isna().all():
            continue
        need.append(code)
    if not need:
        logger.info("[backfill] 当日 turnover 已齐全，无需回填")
        return
    done = []
    ths_hits = 0
    ths_map = {}
    if ths_ctx is not None:
        try:
            from kline_ths_client import fetch_ths_turnover_batch
            ths_map = fetch_ths_turnover_batch(ths_ctx, need)
            logger.info("[backfill] THS 主源预取当日 turnover %d/%d 只（每市场一次批量请求）",
                        len(ths_map), len(need))
        except Exception as e:
            logger.warning("[backfill] THS 批量预取失败：%s", str(e)[:120])
    for code in need:
        tv = ths_map.get(code)
        if tv is None or pd.isna(tv):
            continue  # NaN（清洗后占位符）不算有效值，留给 TDX 备源
        try:
            _apply_today_turnover(code, float(tv))
            done.append(code)
            ths_hits += 1
        except Exception as e:
            logger.warning("[backfill] %s THS 回填异常：%s", code, str(e)[:120])
    missed = [c for c in need if c not in done]
    tdx_hits = 0
    if missed and tdx_ctx is not None:
        try:
            from kline_tdx_client import fetch_tdx_turnover_batch
        except Exception as e:
            logger.warning("[backfill] TDX 备源模块不可用：%s", str(e)[:120])
        else:
            for code in missed:
                try:
                    sub = fetch_tdx_turnover_batch([code], count=1, client=tdx_ctx)
                    tv = sub.get(code)
                except Exception as e:
                    logger.warning("[backfill] %s TDX 备源失败：%s", code, str(e)[:120])
                    tv = None
                if tv is None or pd.isna(tv):
                    continue  # 失败置 NaN 不回填（防 NaN 当有效值写入）
                try:
                    _apply_today_turnover(code, float(tv))
                    done.append(code)
                    tdx_hits += 1
                except Exception as e:
                    logger.warning("[backfill] %s TDX 回填异常：%s", code, str(e)[:120])
    results["backfill"] = {"need": len(need), "ths_hits": ths_hits,
                           "tdx_hits": tdx_hits, "missed": len(missed) - tdx_hits}
    logger.info("[backfill] 当日 turnover 补齐完成：need=%d ths=%d tdx=%d 未补齐=%d",
                len(need), ths_hits, tdx_hits, len(missed) - tdx_hits)


# ---------------------------------------------------------------- 连接管理
# 会话重建（断线自愈）：THS/TDX 为有状态 TCP 长连接，长时采集中途会话失效时
# 由 _run_chain/_drain_pending 连续命中连接失效累计到阈值触发 _rebuild_source，
# 关闭旧会话重建新会话并重试当前股票（同股重建后仍失败≥3 次记 fail）。
_CONN_RECONNECT_STREAK = 3  # 连续命中连接失效阈值：达到即重建会话


def _rebuild_source(source, holder):
    """重建指定源会话：关闭旧 ctx 后按 _open_source 逻辑重开。

    THS/TDX 均须先断开旧连接再重连（thsdk connect 已连接再连会被拒；
    TDX 旧 socket 已坏需重建）。holder 为单元素列表（run_collect 持有的可变
    容器），重建成功后更新 holder[0]，backfill 与其它线程取到新会话。
    sina/tx 无会话（holder None）直接返回 None。"""
    logger.warning("[%s] 会话失效，重建连接…", source)
    try:
        _close_source(holder[0])
    except Exception:
        pass
    try:
        new_ctx = _open_source(source)
    except Exception as e:
        logger.error("[%s] 会话重建失败：%s", source, str(e)[:150])
        holder[0] = None
        return None
    holder[0] = new_ctx
    logger.info("[%s] 会话重建完成", source)
    return new_ctx


def _open_source(source):
    if source == "ths":
        from thsdk import THS
        _u = os.environ.get("THS_USERNAME")
        _p = os.environ.get("THS_PASSWORD")
        if _u and _p:
            ctx = THS({"username": _u, "password": _p})
            logger.info("THS connected with formal account: %s***", _u[:2])
        else:
            ctx = THS()
            logger.warning("THS using guest account (THS_USERNAME/THS_PASSWORD unset)")
        ctx.__enter__()
        return ctx
    if source == "tdx":
        from tdxdata import TdxClient
        ctx = TdxClient()
        ctx.connect()
        return ctx
    return None


def _close_source(ctx):
    if ctx is None:
        return
    try:
        ctx.__exit__(None, None, None)
    except Exception:
        try:
            ctx.close()
        except Exception:
            pass


# ---------------------------------------------------------------- 改动点 #5（去批次）
def run_collect(codes_all, mode, overwrite, start_date=None, end_date=None, limit=0):
    """调度入口（职责链流水线，无批次 join）：

      - full：四源各一线程贯穿职责链（共享各阶段 _Cursor 一次取一号，谁快谁取）：
          ths: standard→adjust→incremental；tdx: standard→incremental；
          sina/tx: standard→adjust→turnover_hist。
        快源完成 standard 份额即进入补采，不被慢源卡住；补采遇 standard 文件缺失
        （std_missing）记 pending，轮询待 standard 全部完成后补（std_done = standard
        游标 exhausted）；目标列已齐全的 skip 绝不进 pending。
      - incremental：ths/tdx 两线程仅采当日增量。
      当日增量结束后统一执行当日 turnover 后置补齐（THS 批量预取主补 + TDX 备源）。

    返回 results: {(source, stage): {...}, "backfill": {...}}。
    """
    results = {}
    ths_ctx = tdx_ctx = None
    try:
        ths_ctx = [_open_source("ths")]
        tdx_ctx = [_open_source("tdx")]
    except Exception as e:
        logger.error("[open] 数据源连接失败：%s", str(e)[:150])
        for _ctx in (ths_ctx, tdx_ctx):
            if _ctx is not None:
                _close_source(_ctx[0])  # 已成功打开的连接必须关闭，防泄漏
        results[("open", "fail")] = {"ok": 0, "fail": 0, "skip": 0,
                                     "time_min": 0.0, "error": str(e)[:150]}
        return results
    touched = []
    try:
        file_index = _FileIndex()
        file_index.scan()
        file_index.verify(sample=5)  # P1：抽样校验索引与文件一致（不一致仅告警，判定回退读盘）
        full_end = effective_end("full", end_date)
        inc_end = effective_end("incremental", end_date)
        std_done = None
        chains = {}
        if mode == "full":
            std_cursor = _Cursor(codes_all, max_total=limit)
            adj_cursor = _Cursor(codes_all, max_total=limit)
            th_cursor = _Cursor(codes_all, max_total=limit)
            inc_cursor = _Cursor(codes_all, max_total=limit)
            std_done = std_cursor.exhausted
            chains = {
                "ths": [("standard", std_cursor, full_end),
                        ("adjust", adj_cursor, full_end),
                        ("incremental", inc_cursor, inc_end)],
                "tdx": [("standard", std_cursor, full_end),
                        ("incremental", inc_cursor, inc_end)],
                "sina": [("standard", std_cursor, full_end),
                         ("adjust", adj_cursor, full_end),
                         ("turnover_hist", th_cursor, full_end)],
                "tx": [("standard", std_cursor, full_end),
                       ("adjust", adj_cursor, full_end),
                       ("turnover_hist", th_cursor, full_end)],
            }
        else:
            inc_cursor = _Cursor(codes_all, max_total=limit)
            chains = {
                "ths": [("incremental", inc_cursor, inc_end)],
                "tdx": [("incremental", inc_cursor, inc_end)],
            }
        threads = []
        for source, chain in chains.items():
            holder = ths_ctx if source == "ths" else (tdx_ctx if source == "tdx" else None)
            t = threading.Thread(target=_run_chain,
                                 args=(source, chain, holder, mode, overwrite, start_date,
                                       end_date, results, touched, std_done, file_index),
                                 name=source)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        # 改动点 #11：指数当日增量调度（ths+tdx 两源发号，复用本阶段连接；
        # 独立全量触发见 run_index_collect.py --mode full）
        if mode == "incremental":
            try:
                from run_index_collect import collect_index_incremental
                results[("index", "incremental")] = collect_index_incremental(
                    ths_ctx=ths_ctx[0], tdx_ctx=tdx_ctx[0])
            except Exception as e:
                logger.warning("[index] 增量调度失败：%s", str(e)[:150])
                results[("index", "incremental")] = {"error": str(e)[:150]}
        _backfill_today_turnover(ths_ctx[0], tdx_ctx[0], touched, results)
    finally:
        _close_source(ths_ctx[0])
        _close_source(tdx_ctx[0])
    return results


# ---------------------------------------------------------------- 改动点 #9
def main():
    _setup_logging()
    # 改动点 #10：容器 tdxdata 指数补丁自愈（幂等，补丁随 git 同步于 tdxdata_patch/）
    try:
        from ensure_tdxdata_patch import ensure as _ensure_tdxdata_patch
        _ok, _chg, _reason = _ensure_tdxdata_patch()
        if _chg:
            logger.warning("TDXDATA_PATCH_REAPPLIED files=%s", ",".join(_chg))
        else:
            logger.info("TDXDATA_PATCH_OK %s", _reason)
    except Exception as _e:  # 自愈失败不阻塞采集主流程
        logger.warning("TDXDATA_PATCH_ENSURE_FAILED: %s", _e)
    ap = argparse.ArgumentParser(description=f"四源 K 线主采调度（职责链流水线，version={KLINE_VERSION}）")
    ap.add_argument("--version", action="version", version=f"%(prog)s {KLINE_VERSION}",
                    help="显示版本号并退出")
    ap.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                    help="full=全量四源（含自动当日增量闭环）；incremental=当日两路增量")
    ap.add_argument("--overwrite", action="store_true",
                    help="独立叠加开关：跳过检测直接覆盖（历史段/当日行均覆盖）")
    ap.add_argument("--start-date", default="", help="采集起点 YYYY-MM-DD（full 空库必填）")
    ap.add_argument("--end-date", default="", help="采集终点 YYYY-MM-DD（缺省由 effective_end 决定）")
    ap.add_argument("--limit", type=int, default=0, help="全局发牌总数上限（试点口径）")
    ap.add_argument("--use-list", action="store_true",
                    help="使用 load_stock_basic_codes 名单而非交易所全量")
    args = ap.parse_args()

    _patch_requests_timeout()
    t_start = time.time()

    # 改动点 #1：统一名单
    codes_all = build_code_list(args.use_list, args.start_date or None, args.end_date or None)
    if not codes_all:
        logger.warning("空清单，跳过采集（本地名单源不可用：确认 data 目录（DATA_DIR）"
                       "存在有效 *.parquet；冷启动需接入交易所全量名单）")
        return 0
    logger.info("KLINE_MULTI_START version=%s mode=%s overwrite=%s 名单=%d start=%s end=%s limit=%d",
                KLINE_VERSION, args.mode, args.overwrite, len(codes_all),
                args.start_date, args.end_date, args.limit)

    # 改动点 #5/#9：职责链流水线（无批次）+ full 自动当日增量闭环 + 后置补齐
    results = run_collect(codes_all, args.mode, args.overwrite,
                          args.start_date or None, args.end_date or None, args.limit)

    total = (time.time() - t_start) / 60
    parts = []
    for src in ("ths", "tdx", "sina", "tx"):
        _ok = _fail = _skip = 0
        _tm = 0.0
        for key, r in results.items():
            if not isinstance(key, tuple):
                continue  # "backfill" 等汇总键非阶段元组，跳过（防解包崩溃）
            s, stage = key
            if s == src:
                _ok += r["ok"]
                _fail += r["fail"]
                _skip += r["skip"]
                _tm += r["time_min"]
        if _ok or _fail or _skip:
            parts.append(f"{src}: ok={_ok} fail={_fail} skip={_skip} {_tm:.1f}m")
    _bf = results.get("backfill") or {}
    if _bf:
        logger.info("[汇总] backfill need=%d ths=%d tdx=%d missed=%d",
                    _bf.get("need", 0), _bf.get("ths_hits", 0),
                    _bf.get("tdx_hits", 0), _bf.get("missed", 0))
    _idx = results.get(("index", "incremental")) or {}
    if _idx:
        _parts_idx = []
        for _src in ("ths", "tdx"):
            _st = _idx.get(_src)
            if _st:
                _parts_idx.append(f"{_src}: ok={_st['ok']} fail={_st['fail']}")
        if _parts_idx:
            logger.info("[汇总] index_inc %s", " ".join(_parts_idx))
    logger.info("=== run_kline_multi done mode=%s overwrite=%s %s total=%.1fm ===",
                args.mode, args.overwrite, " ".join(parts), total)
    print("KLINE_MULTI_DONE mode=%s overwrite=%s %s total=%.1fm"
          % (args.mode, args.overwrite, "; ".join(parts), total))
    return 0


if __name__ == "__main__":
    sys.exit(main())
