#!/usr/bin/env python3
"""run_index_collect.py — 指数采集编排（v2 对齐 K 线采集模式）

能力（对齐 run_kline_multi 三语义）：
- incremental：恒 count=1 只拉当日（最新交易日），列级合并覆盖当日；存量已到 end
  且 8 列无 null 则 skip（零 IO 索引判定）
- full：四源（ths/sina/tx/tdx）并发发号，四分支查漏：
  skip（存量到 end 且无缺列）/ repair（缺列，由存量最早日推导重建起点）/
  gap（存量未到 end，按缺口交易日数补）/ full_fetch（空库/--overwrite 直接替换，
  需 --start-date 或由存量最早日推导起点）
- --overwrite：full -> full_fetch 整体替换；incremental -> 强制采当日（列级合并，
  当日行覆盖、历史保留）
- _IndexFileIndex 文件状态索引（对齐 K 线 _FileIndex）：启动扫描 INDEX_DIR/*.parquet
  footer 元数据，判定零 IO；无法精确判定回退锁内读盘
- index_effective_end 时间边界（对齐 K 线 effective_end）：
  full -> min(end_date|today, prev_trading_day(cn_today()))；incremental -> end_date|today

集成：
- 独立触发：python run_index_collect.py --mode full|incremental [--overwrite]
  [--codes 000001,000300] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
- v1.3.1（run_index_collect v4）：启动校验交易日历可用（fail-loud，拒绝日历不可用
  静默退化为"只拉当日"假成功）；--codes 参数化指数清单（默认 kline_env.INDEX_CODES）；
  写盘独占（对齐 K 线 _Cursor work-queue 取号模型）：多源共享发牌游标一次取一号，
  一 code 一源独占"锁内判定+采集+写盘"，消除 v3 四源并发写同一 parquet 的
  last-writer 竞争；sina/tx 指数请求复用共享 Session 连接池（akshare 指数模块局部 patch）
- 每日增量脚本：run_kline_multi.py incremental 路径在股票当日增量完成后调用
  collect_index_incremental(ths_ctx=..., tdx_ctx=...) 复用连接执行指数增量（count 恒 1）

落盘契约：统一 INDEX_COLS = [date, open, high, low, close, volume, amount]，
与 kline_ths/sina/tx/tdx_client 四客户端 fetch_index_daily / update_index 对称。
"""
import argparse
import logging
import logging.handlers
import sys
import threading

import pandas as pd

from kline_env import KLINE_VERSION, INDEX_DIR, INDEX_CODES, LOG_DIR  # noqa: E402
from kline_env import SCRIPT_DIR  # noqa: E402
from trade_calendar import (  # noqa: E402
    trade_days_between, prev_trading_day, cn_today, load_trading_calendar,
)

# 保证 scripts 目录在 sys.path（生产布局 <work>/scripts，模块同目录即满足）
sys.path.insert(0, str(SCRIPT_DIR))

# 指数落盘契约（与四客户端 INDEX_COLS 对齐）
INDEX_COLS = ["date", "open", "high", "low", "close", "volume", "amount"]

logger = logging.getLogger("run_index_collect")

# 写锁（对齐 K 线 _lock_for 分片）：同一 code 串行、不同 code 并行写库
_write_locks = {}
_write_locks_guard = threading.Lock()


def _lock_for(code):
    with _write_locks_guard:
        return _write_locks.setdefault(code, threading.Lock())


# 索引无法精确判定（列 null 统计不可用/日期缺失）时回退锁内读盘的哨兵
_FALLBACK = object()


class _Cursor:
    """全局发牌游标（照搬 run_kline_multi._Cursor work-queue 取号模型）：
    一次取一号，原子自增，谁快谁取；一 code 全局只发一次给一个源线程，
    该源独占"判定+采集+写盘"，完成后再取下一号，消除四源并发写同一 parquet
    的 last-writer 竞争。

    指数清单无退市概念（INDEX_CODES 均为存续指数），全部走普通队列；
    allow_delisted 参数保留仅为与 K 线 _Cursor.next 语义一致。
    register/unregister 跟踪并发 worker，最后一个退出时置位 exhausted
    （阶段完成事件，供外部判断全部源已取完）。max_total>0 时对发牌总数设上限。
    """

    def __init__(self, codes_all, max_total=0):
        self._normal = list(codes_all)
        self._i = 0
        self._lock = threading.Lock()
        self._max_total = max_total
        self._issued = 0
        self._workers = 0
        self._workers_lock = threading.Lock()
        self.exhausted = threading.Event()

    def next(self, allow_delisted=False):
        """取号：返回 (code, False)；普通号耗尽或达 max_total 上限返回 None。"""
        with self._lock:
            if self._max_total > 0 and self._issued >= self._max_total:
                return None
            if self._i < len(self._normal):
                item = self._normal[self._i]
                self._i += 1
                self._issued += 1
                return (item, False)
            return None

    def register(self):
        with self._workers_lock:
            self._workers += 1

    def unregister(self):
        with self._workers_lock:
            self._workers -= 1
            if self._workers <= 0:
                self.exhausted.set()


def _setup_logging():
    """与 run_kline_multi._setup_logging 同款：独立触发时配置，被导入时不重复。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if root.handlers:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "run_index_collect.log", maxBytes=20 * 1024 * 1024,
        backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)


def _open_source(source):
    """打开数据源连接（ths/tdx 需长连接；sina/tx 走 akshare 无连接）。"""
    if source == "ths":
        from thsdk import THS
        _u = __import__("os").environ.get("THS_USERNAME")
        _p = __import__("os").environ.get("THS_PASSWORD")
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


# ---------------- 文件状态索引（对齐 K 线 _FileIndex） ----------------
def _column_stats(pf, col_idx):
    """读取 parquet 单列 footer 统计：返回 (null_count, min, max, ok)。"""
    try:
        col = pf.metadata.row_group(0).column(col_idx)
        st = col.statistics
        if st is None or not st.has_min_max:
            return None, None, None, False
        return st.null_count, st.min, st.max, True
    except Exception:
        return None, None, None, False


class _IndexFileIndex:
    """扫描 INDEX_DIR/*.parquet footer 元信息（列集/null 统计/日期上下界）。

    仅读 footer（不加载行数据），用于零 IO 判定存量是否已到 end 且无缺列；
    无法精确判定（列 null 统计不可用）时由调用方回退锁内读盘。
    """

    def __init__(self):
        self._meta = {}
        self._lock = threading.Lock()

    def scan(self):
        metas = {}
        for fp in sorted(INDEX_DIR.glob("*.parquet")):
            m = self._read_meta(fp)
            if m is not None:
                metas[fp.stem] = m
        with self._lock:
            self._meta = metas
        logger.info("[index] 扫描持盘指数 %d 只，索引就绪", len(metas))

    @staticmethod
    def _read_meta(fp):
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
                if ok and null_cnt == 0 and vmin is not None and vmax is not None:
                    min_date = pd.Timestamp(vmin).normalize()
                    last_date = pd.Timestamp(vmax).normalize()
            return {
                "cols": cols, "null_cols": nulls if known else None,
                "min_date": min_date, "last_date": last_date,
            }
        except Exception:
            return None

    def get(self, code):
        with self._lock:
            return self._meta.get(code)


def _ensure_calendar_ready():
    """启动校验交易日历可用，不可用即 fail-loud（拒绝静默退化）。

    背景（v1.3.1）：trade_days_between 在日历不可用时返回 0，full 会经 max(...,1)
    退化成"只拉当日"假成功；prev_trading_day 返回 None 会让 full 的
    index_effective_end 直接崩溃。此处把校验前移到编排启动：日历加载失败或
    覆盖范围不含今日时抛 RuntimeError 终止本次采集，由上层/日志显式暴露。
    返回日历本体（调用方可复用，避免二次加载）。
    """
    cal = load_trading_calendar()
    if cal is None or len(cal) == 0:
        raise RuntimeError(
            "交易日历不可用（load_trading_calendar 返回空），无法计算交易日窗口，"
            "拒绝继续（fail-loud）")
    today = pd.Timestamp(cn_today()).normalize()
    c0, c1 = pd.Timestamp(cal[0]).normalize(), pd.Timestamp(cal[-1]).normalize()
    if today < c0 or today > c1:
        raise RuntimeError(
            f"交易日历覆盖 {c0.date()}~{c1.date()} 不含今日 {today.date()}，"
            "拒绝继续（fail-loud）")
    return cal


def index_effective_end(mode, end_date=None):
    """指数有效拉取终点（对齐 K 线 effective_end）：
    full -> min(end_date|today, prev_trading_day(cn_today()))；
    incremental -> end_date|today。返回 normalized Timestamp。"""
    today = pd.Timestamp(cn_today()).normalize()
    end = pd.Timestamp(end_date).normalize() if end_date else today
    if mode == "full":
        prev_d = prev_trading_day(cn_today())
        if prev_d is None:
            raise RuntimeError(
                "prev_trading_day 返回 None（交易日历不可用），full 模式无法截断终点，"
                "拒绝继续（fail-loud）")
        prev = pd.Timestamp(prev_d).normalize()
        end = min(end, prev)
    return end


def _df_meta(df):
    """由存量 DataFrame 归一化为 meta 等效结构（读盘回退路径用，对齐 footer 统计）。"""
    cols = set(df.columns)
    null_cols = set()
    for c in df.columns:
        if c == "date":
            continue
        try:
            if df[c].isna().any():
                null_cols.add(c)
        except Exception:
            return {"cols": cols, "null_cols": None,
                    "min_date": None, "last_date": None}
    try:
        return {"cols": cols, "null_cols": null_cols,
                "min_date": pd.Timestamp(df["date"].min()).normalize(),
                "last_date": pd.Timestamp(df["date"].max()).normalize()}
    except Exception:
        return {"cols": cols, "null_cols": null_cols,
                "min_date": None, "last_date": None}


def _plan_index_fetch(mode, overwrite, meta, existing_df, start_date=None, end=None):
    """单只指数拉取方案（对齐 K 线 _plan_fetch / _ensure_with_index）。

    返回 ("full_fetch"|"gap_fetch"|"repair_fetch"|"incremental_fetch", count, base)
    或 "skip"（存量已到 end 且无缺列）或 _FALLBACK（需读盘精确判定）。
    """
    if existing_df is not None and not existing_df.empty and meta is None:
        meta = _df_meta(existing_df)  # 读盘回退路径：存量即权威，归一化统一判定
    if mode == "full":
        if overwrite or (meta is None and existing_df is None):
            # 全量重建：起点 = --start-date 或存量最早日-1；空库无 start-date 则报错
            base = None
            if start_date:
                base = pd.Timestamp(start_date).normalize() - pd.Timedelta(days=1)
            elif meta is not None and meta.get("min_date") is not None:
                base = pd.Timestamp(meta["min_date"]).normalize() - pd.Timedelta(days=1)
            elif existing_df is not None and not existing_df.empty:
                base = pd.Timestamp(existing_df["date"].min()).normalize() - pd.Timedelta(days=1)
            if base is None:
                raise ValueError("full 模式空库/--overwrite 需 --start-date（无法推导重建起点）")
            count = max(trade_days_between(base, end), 1)
            return ("full_fetch", count, base)
        if meta is None:
            return _FALLBACK
        # 缺列修复（对齐 K 线 repair：由存量最早日-1 重建，覆盖缺失列区间）
        for c in INDEX_COLS[1:]:
            if c not in meta["cols"] or (meta["null_cols"] is not None and c in meta["null_cols"]):
                if meta["null_cols"] is None or meta.get("min_date") is None:
                    return _FALLBACK
                base = pd.Timestamp(meta["min_date"]).normalize() - pd.Timedelta(days=1)
                count = max(trade_days_between(base, end), 1)
                return ("repair_fetch", count, base)
        if meta["null_cols"] is None:
            return _FALLBACK  # 列 null 统计不可用，无法判定"无缺列"-> 回退读盘
        if meta.get("last_date") is None:
            return _FALLBACK
        existing_end = pd.Timestamp(meta["last_date"]).normalize()
        if existing_end >= end:
            return "skip"  # 存量已到 end 且无缺列 -> skip（对齐 K 线 uptodate）
        count = trade_days_between(existing_end, end)
        return ("gap_fetch", max(count, 1), existing_end)
    # incremental：恒 count=1 只拉当日（最新交易日）
    if overwrite or meta is None:
        return ("incremental_fetch", 1, None)
    if meta.get("last_date") is None:
        return _FALLBACK
    existing_end = pd.Timestamp(meta["last_date"]).normalize()
    if existing_end < end:
        return ("incremental_fetch", 1, existing_end)
    # 存量已到 end：8 列全无 null -> skip；存在缺列 -> 采当日（列级合并修复最新行，
    # 对齐 K 线 incremental"最新行缺列仍采当日"语义；中间行缺列由 full repair 修）
    if meta["null_cols"] is None:
        return _FALLBACK
    if all(c in meta["cols"] and c not in meta["null_cols"] for c in INDEX_COLS[1:]):
        return "skip"
    return ("incremental_fetch", 1, existing_end)


def _read_existing(code):
    fp = INDEX_DIR / f"{code}.parquet"
    if not fp.exists():
        return None
    try:
        df = pd.read_parquet(fp)
        if df.empty:
            return None
        return df
    except Exception:
        return None


def _ensure_index(code, mode, overwrite, start_date=None, end_date=None, index=None):
    """单只指数拉取方案：索引零 IO 优先；无法精确判定时锁内读盘回退。"""
    end = index_effective_end(mode, end_date)
    if index is not None:
        plan = _plan_index_fetch(mode, overwrite, index.get(code), None, start_date, end)
        if plan is not _FALLBACK:
            return plan
    with _lock_for(code):
        old = _read_existing(code)
        return _plan_index_fetch(mode, overwrite, None, old, start_date, end)


def _collect_one(source, code, plan, end, overwrite, ctx=None):
    """按源调用对应客户端 update_index 落盘单只指数，返回 bool。"""
    action, count, _repair = plan
    # overwrite 仅对 full_fetch 生效（整体替换）；incremental/gap/repair 一律
    # 列级合并（当日覆盖 / 缺口补齐 / 缺列修复，保留历史）
    _ow = overwrite and action == "full_fetch"
    _end = end.strftime("%Y-%m-%d") if end is not None else ""
    if source == "ths":
        import kline_ths_client
        return kline_ths_client.update_index(ctx, code, count=count,
                                             end_date=_end, overwrite=_ow) is True
    if source == "tdx":
        import kline_tdx_client
        return kline_tdx_client.update_index(code, count=count, client=ctx,
                                             overwrite=_ow) is True
    if source == "sina":
        import kline_sina_client
        return kline_sina_client.update_index(code, count=count, end_date=_end,
                                              overwrite=_ow) is True
    if source == "tx":
        import kline_tx_client
        return kline_tx_client.update_index(code, count=count, end_date=_end,
                                            overwrite=_ow) is True
    logger.error("[%s] 未知源，跳过 %s", source, code)
    return False


def _run_source(source, cursor, end, mode, overwrite, stats, start_date=None,
                ths_ctx=None, tdx_ctx=None, index=None):
    """单源线程体（对齐 run_kline_multi._run_chain work-queue 取号模型）：
    共享游标一次取一号，一 code 一源独占（锁内判定 → 采集 → 写盘），完成再取
    下一号；多源谁快谁取，天然负载均衡，无两源写同一 parquet 的 last-writer 竞争。

    end 由调用方 index_effective_end 预计算一次供各源复用（避免每线程重复
    调用 prev_trading_day，对齐 K 线 run_collect 的 stage_end 语义）。
    """
    st = {"ok": 0, "fail": 0, "skip": 0}
    stats[source] = st
    ctx = ths_ctx if source == "ths" else (tdx_ctx if source == "tdx" else None)
    cursor.register()
    try:
        while True:
            item = cursor.next()
            if item is None:
                break
            code, _ = item
            try:
                plan = _ensure_index(code, mode, overwrite, start_date, end, index)
                if plan == "skip":
                    st["skip"] += 1
                    logger.info("[index:%s] %s skip（存量已到 end 且无缺列）", source, code)
                    continue
                if plan is _FALLBACK:
                    st["fail"] += 1
                    logger.error("[index:%s] %s 存量状态不可判定（缺 date 列或统计异常），记 fail",
                                 source, code)
                    continue
                if _collect_one(source, code, plan, end, overwrite, ctx):
                    st["ok"] += 1
                    logger.info("[index:%s] %s ok", source, code)
                else:
                    st["fail"] += 1
                    logger.warning("[index:%s] %s fail（接口无数据或落盘失败）", source, code)
            except Exception as e:
                st["fail"] += 1
                logger.error("[index:%s] %s 异常：%s", source, code, str(e)[:120])
    finally:
        cursor.unregister()
    logger.info("[index:%s] done ok=%d fail=%d skip=%d",
                source, st["ok"], st["fail"], st["skip"])


def _run_threads(sources, cursor, end, mode, overwrite, stats, start_date=None,
                 ths_ctx=None, tdx_ctx=None, index=None):
    """并发执行各源采集（共享同一发牌游标，一 code 一源独占写盘），
    返回 stats: {source: {ok, fail, skip}}。"""
    threads = []
    for source in sources:
        t = threading.Thread(target=_run_source,
                             args=(source, cursor, end, mode, overwrite,
                                   stats, start_date, ths_ctx, tdx_ctx, index),
                             name=f"index-{source}")
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    return stats


def collect_index_full(codes=INDEX_CODES, overwrite=False, start_date=None, end_date=None):
    """全量采集：四源（ths/sina/tx/tdx）共享发牌游标并发取号，一 code 一源独占
    判定+采集+写盘（对齐 K 线 run_collect full 的 work-queue 模型）。

    各源独立连接；单源连接失败记录到 stats 不中断其它源。
    v1.3.1：启动校验交易日历 fail-loud；sina/tx 指数请求启用共享 Session 连接池。
    v4：写盘独占——消除 v3"主线程统一判定广播四源、四源并发写同一 parquet
    的 last-writer 竞争"。
    """
    sources = ("ths", "sina", "tx", "tdx")
    logger.info("INDEX_COLLECT_START version=%s mode=full codes=%d overwrite=%s start=%s end=%s",
                KLINE_VERSION, len(codes), overwrite, start_date, end_date)
    _ensure_calendar_ready()  # fail-loud：日历不可用拒绝继续（防 full 假成功）
    try:
        from kline_sina_client import ensure_index_session_pool
        ensure_index_session_pool()
    except Exception as e:
        logger.warning("sina/tx 会话池激活失败（继续逐请求短连接）：%s", str(e)[:120])
    ths_ctx = tdx_ctx = None
    try:
        ths_ctx = _open_source("ths")
    except Exception as e:
        logger.error("[index:ths] 连接失败：%s", str(e)[:150])
    try:
        tdx_ctx = _open_source("tdx")
    except Exception as e:
        logger.error("[index:tdx] 连接失败：%s", str(e)[:150])
    index = _IndexFileIndex()
    index.scan()
    end = index_effective_end("full", end_date)
    cursor = _Cursor(codes)
    try:
        stats = _run_threads(sources, cursor, end, "full", overwrite, {},
                             start_date, ths_ctx, tdx_ctx, index)
    finally:
        _close_source(ths_ctx)
        _close_source(tdx_ctx)
    _summarize("full", stats)
    return stats


def collect_index_incremental(codes=INDEX_CODES, overwrite=False, end_date=None,
                              ths_ctx=None, tdx_ctx=None):
    """当日增量采集：ths+tdx 两源共享发牌游标并发取号，一 code 一源独占
    判定+采集+写盘（对齐 K 线 run_collect incremental 的 work-queue 模型），
    恒 count=1 只拉当日列级合并落盘。

    ths_ctx/tdx_ctx 非 None 时复用调用方已开连接（每日增量脚本集成路径），
    否则本函数自建连接（独立触发路径）。
    v1.3.1：启动校验交易日历 fail-loud。
    v4：写盘独占——消除 v3"主线程统一判定广播两源、两源并发写同一 parquet
    的 last-writer 竞争"。
    """
    sources = ("ths", "tdx")
    own_ths = ths_ctx is None
    own_tdx = tdx_ctx is None
    logger.info("INDEX_COLLECT_START version=%s mode=incremental codes=%d overwrite=%s end=%s",
                KLINE_VERSION, len(codes), overwrite, end_date)
    _ensure_calendar_ready()  # fail-loud：日历不可用拒绝继续（防增量误判 skip）
    if own_ths:
        try:
            ths_ctx = _open_source("ths")
        except Exception as e:
            logger.error("[index:ths] 连接失败：%s", str(e)[:150])
    if own_tdx:
        try:
            tdx_ctx = _open_source("tdx")
        except Exception as e:
            logger.error("[index:tdx] 连接失败：%s", str(e)[:150])
    index = _IndexFileIndex()
    index.scan()
    end = index_effective_end("incremental", end_date)
    cursor = _Cursor(codes)
    try:
        stats = _run_threads(sources, cursor, end, "incremental", overwrite, {},
                             None, ths_ctx, tdx_ctx, index)
    finally:
        if own_ths:
            _close_source(ths_ctx)
        if own_tdx:
            _close_source(tdx_ctx)
    _summarize("incremental", stats)
    return stats


def _summarize(mode, stats):
    parts = []
    for source in ("ths", "tdx", "sina", "tx"):
        st = stats.get(source)
        if st:
            parts.append(f"{source}: ok={st['ok']} fail={st['fail']} skip={st['skip']}")
    msg = " ".join(parts)
    logger.info("=== run_index_collect done mode=%s %s ===", mode, msg)
    print("INDEX_COLLECT_DONE mode=%s %s" % (mode, msg))


def _parse_codes(raw):
    """解析 --codes（逗号/空白分隔、去空白保序去重）；空输入回退默认 INDEX_CODES。"""
    if not raw:
        return tuple(INDEX_CODES)
    codes = []
    for part in str(raw).replace("，", ",").replace(" ", ",").split(","):
        c = part.strip()
        if c and c not in codes:
            codes.append(c)
    return tuple(codes) if codes else tuple(INDEX_CODES)


def main():
    _setup_logging()
    ap = argparse.ArgumentParser(
        description=f"指数采集编排 work v2（对齐 K 线：full 四分支查漏 / incremental 恒 1 只拉当日，version={KLINE_VERSION}）")
    ap.add_argument("--version", action="version", version=f"%(prog)s {KLINE_VERSION}",
                    help="显示版本号并退出")
    ap.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                    help="full=四源（ths/sina/tx/tdx）查漏发号；incremental=ths+tdx 两源当日增量（恒拉 1 根）")
    ap.add_argument("--overwrite", action="store_true",
                    help="跳过存量检测直接覆盖：full=整体替换；incremental=强制采当日（历史保留）")
    ap.add_argument("--codes", default=None,
                    help="待采集指数代码列表（逗号分隔，如 000001,000300；默认取 kline_env.INDEX_CODES）")
    ap.add_argument("--start-date", default=None,
                    help="full 模式重建起点 YYYY-MM-DD（空库或 --overwrite 时必填，否则由存量最早日推导）")
    ap.add_argument("--end-date", default=None,
                    help="拉取终点 YYYY-MM-DD（默认今日；full 再截断至前一交易日）")
    args = ap.parse_args()

    codes = _parse_codes(args.codes)
    if args.mode == "full":
        stats = collect_index_full(codes=codes, overwrite=args.overwrite,
                                   start_date=args.start_date, end_date=args.end_date)
    else:
        stats = collect_index_incremental(codes=codes, overwrite=args.overwrite,
                                          end_date=args.end_date)
    return 1 if any(st["fail"] for st in stats.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
