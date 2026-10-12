#!/usr/bin/env python3
"""status_schema.py — 状态判定、schema 定义与落盘工具（状态采集专用）

依赖策略：pyarrow 为 HIST_SCHEMA 常量所需，顶层导入；pandas 统一函数内延迟导入，
避免轻量模块 import 本文件时付出不必要的加载成本。

- 状态判定口径：normal / suspended / pending_delist / delisted
- st_level / is_new 计算
- 沪深A股代码段正则（领域常量单一事实源，baostock_client / sources / collect 共用）
- parquet 读写：逐日快照、basic 缓存、主表年份平铺文件（append_partitioned /
  year_parts；全量重建按年流式 flush 直写分区，不再经单文件临时表）
- 落盘前 normal 阈值闸门（防名单源 fail-open 清空 universe）

"""
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pyarrow as pa

log = logging.getLogger(__name__)

__all__ = [
    "ASH_PAT", "ASZ_PAT",
    "COLS", "HIST_SCHEMA", "NEW_DAYS", "MIN_NORMAL_FOR_WRITE",
    "st_level_of", "is_new_at", "build_day_rows", "bj_kline_dates",
    "save_snapshot", "load_snapshot",
    "year_parts", "append_partitioned",
    "load_basic_cache", "save_basic_cache", "assert_normal_threshold",
]

# 沪深A股代码模式（排除指数/基金/B股）；北交所 bj 前缀由 akshare 备源补充。
# 单一事实源：baostock_client / collect_stock_status / sources 一律从这里引用
ASH_PAT = re.compile(r"^sh\.(60|68)\d{4}$")
ASZ_PAT = re.compile(r"^sz\.(00|30)\d{4}$")

# 次新阈值：上市不满 NEW_DAYS 个自然日视为次新（375 ≈ 一年半，覆盖新股上市
# 初期的走势不稳定期，与 check_stock_status 旧口径一致）
NEW_DAYS = 375
# 写盘闸门：最新截面 normal 股数低于该值即中止（3000 ≈ 当前 A 股在市数量的
# 安全下限；双源 fail-open 将全 universe 误标时远低于此值）
MIN_NORMAL_FOR_WRITE = int(os.environ.get("SESO_MIN_NORMAL_FOR_WRITE", "3000"))

COLS = ["date", "code", "name", "status", "st_level", "is_new", "ipo_date"]

HIST_SCHEMA = pa.schema([
    pa.field("date", pa.string()), pa.field("code", pa.string()),
    pa.field("name", pa.string()), pa.field("status", pa.string()),
    pa.field("st_level", pa.string()), pa.field("is_new", pa.bool_()),
    pa.field("ipo_date", pa.string()),
])

# basic 缓存 parquet 必需列（load_basic_cache 校验，防 schema 漂移静默产生错误 key）
# last_checked 为陈旧度轮换标记（新增列，旧缓存缺省时兼容补空，不作必需列校验）
_CORE_BASIC_COLS = ["code", "code_name", "ipoDate", "outDate", "status"]
_BASIC_COLS = _CORE_BASIC_COLS + ["last_checked"]


def st_level_of(name):
    """ST 与 *ST 分开判断：前缀严格匹配（先归一全角星号 ＊ 再匹配）。"""
    nu = name.upper().replace("＊", "*")
    if nu.startswith("*ST"):
        return "st*"
    if nu.startswith("ST"):
        return "st"
    return "none"


def is_new_at(ipo, d):
    """上市不满 NEW_DAYS 天为次新；ipo 缺失/解析失败/晚于 d（数据异常）返回 False。"""
    if not ipo:
        return False
    try:
        days = (datetime.strptime(d, "%Y-%m-%d").date()
                - datetime.strptime(ipo, "%Y-%m-%d").date()).days
    except ValueError:
        return False
    return 0 <= days < NEW_DAYS


def _day_rows_from_snap(d, snap, cache):
    """快照日在市行：normal / suspended / pending_delist。"""
    rows = []
    for code, (ts, nm) in snap.items():
        basic = cache.get(code)
        ipo = basic["ipoDate"] if basic else ""
        status = "pending_delist" if "退" in nm else ("normal" if ts == "1" else "suspended")
        rows.append((d, code, nm, status, st_level_of(nm), is_new_at(ipo, d), ipo))
    return rows


def _delisted_rows_for_day(d, snap, cache):
    """退市行：basic outDate 到达且当日已从快照消失 -> 落一行 delisted。

    - 仅当 d == outDate 且当日快照不含该 code 时写入（退市日口径，防逐日累积）；
    - 若退市日快照仍含该股（最后交易日），保留快照行（normal/pending_delist）并跳过；
    - 沪深A股正则过滤：basic 缓存可能含 B股/基金/可转债（K线目录推断路径混入），
      其 outDate 到达时不得写入主表（防主表污染）。
    """
    rows = []
    for code, basic in cache.items():
        if not (ASH_PAT.match(code) or ASZ_PAT.match(code)):
            continue
        outd = basic.get("outDate") or ""
        if not outd or outd != d or code in snap:
            continue
        nm = basic.get("code_name") or ""
        ipo = basic.get("ipoDate") or ""
        rows.append((d, code, nm, "delisted", "none", False, ipo))
    return rows


def _day_rows_bj(d, bj_universe, bj_kline):
    """北交所行：ak 列表当日在市/名称/次新；不判退市；K线断档 suspended。"""
    rows = []
    for code, info in bj_universe.items():
        kd = bj_kline.get(code)
        if not kd:
            continue
        if d < kd[0] or d > kd[1]:
            continue
        ld = info["list_date"]
        if ld and d < ld:
            continue
        nm = info["name"]
        status = "pending_delist" if "退" in nm else ("normal" if d in kd[2] else "suspended")
        rows.append((d, code, nm, status, st_level_of(nm), is_new_at(ld, d), ld))
    return rows


def build_day_rows(d, snap, cache, bj_universe, bj_kd):
    """单日状态行统一入口：快照在市行 + 北交所行 + 退市 delisted 行。"""
    rows = _day_rows_from_snap(d, snap, cache)
    rows += _day_rows_bj(d, bj_universe, bj_kd)
    rows += _delisted_rows_for_day(d, snap, cache)
    return rows


_BJ_KLINE_CACHE = {}


def bj_kline_dates(kline_dir):
    """北交所K线 -> {code: (first, last, set(dates))}。

    - first/last 取 min/max（不假设文件内 date 升序）；
    - 单文件损坏仅告警跳过（per-file 容错，不放大为全局失败）；
    - 按 (文件 mtime) 缓存，未变更文件免重复全量读。
    """
    out = {}
    for f in Path(kline_dir).glob("*.parquet"):
        code = f.stem
        # 4/8 覆盖旧段(43/83/87)，9 覆盖 2024 年启用的新段 920；
        # sh.900xxx B股K线即使混入也会被 _day_rows_bj 的 bj_universe 门禁挡住
        if not (code.isdigit() and code.startswith(("4", "8", "9")) and len(code) == 6):
            continue
        mtime = f.stat().st_mtime
        cached = _BJ_KLINE_CACHE.get(code)
        if cached and cached[0] == mtime:
            out[code] = cached[1]
            continue
        try:
            import pyarrow.parquet as pq
            t = pq.read_table(f, columns=["date"])
            ds = [str(x)[:10] for x in t.column(0).to_pylist()]
            if not ds:
                continue
            val = (min(ds), max(ds), set(ds))
        except Exception as e:
            log.warning("[bj_kline] %s 读取失败跳过: %s" % (code, e))
            continue
        _BJ_KLINE_CACHE[code] = (mtime, val)
        out[code] = val
    return out


def save_snapshot(snap, fp):
    """快照 dict -> parquet（code/tradeStatus/code_name）。"""
    import pandas as pd
    pd.DataFrame([(c, s[0], s[1]) for c, s in sorted(snap.items())],
                 columns=["code", "tradeStatus", "code_name"]).to_parquet(fp, index=False)


def load_snapshot(sfp):
    """读取单日快照 parquet -> {code: (tradeStatus, code_name)}。

    缺失/空返回 {}；损坏（读失败）先 log.warning 留痕再按空处理，避免
    "快照损坏"与"当日无数据"混同而难以排查。
    """
    import pandas as pd
    try:
        sdf = pd.read_parquet(sfp)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("[snapshot] %s 读取失败（疑似损坏），按空快照处理: %s" % (sfp, e))
        return {}
    if sdf.empty:
        return {}
    return {r.code: (r.tradeStatus, r.code_name) for r in sdf.itertuples(index=False)}


def load_basic_cache(basic_cache):
    """读取 basic 缓存 parquet -> {code: basic_dict}。

    必需列缺失时 log.error 并按空缓存处理（fail-closed 优于 _asdict 静默改名）；
    逐行按列名显式构造 dict，不依赖 itertuples._asdict（列名非合法标识符时会被
    pandas 静默改名，产生错误 key）。
    """
    import pandas as pd
    if not Path(basic_cache).exists():
        return {}
    df = pd.read_parquet(basic_cache)
    missing = [c for c in _CORE_BASIC_COLS if c not in df.columns]
    if missing:
        log.error("[basic_cache] %s 缺必需列 %s，按空缓存处理" % (basic_cache, missing))
        return {}
    present = [c for c in _BASIC_COLS if c in df.columns]
    out = {}
    for vals in df[present].itertuples(index=False, name=None):
        d = {k: ("" if pd.isna(v) else str(v)) for k, v in zip(present, vals)}
        for c in _BASIC_COLS:
            d.setdefault(c, "")
        out[d["code"]] = d
    return out


def save_basic_cache(rows, basic_cache):
    """写 basic 缓存 parquet（code/code_name/ipoDate/outDate/status/last_checked）。"""
    import pandas as pd
    df = pd.DataFrame(rows, columns=_BASIC_COLS)
    for c in _BASIC_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[_BASIC_COLS].fillna("")
    df.to_parquet(basic_cache, index=False)


def patch_missing_delisted(last_by_code, delisted_keys, cache):
    """退市日快照缺失兜底（全量路径内存态）：返回需补的 patch rows 列表。

    正常路径 _delisted_rows_for_day 只在 d == outDate 且当日快照存在时写；
    若退市日快照恰好缺失（双源失败），delisted 行会永久丢失。全量重建
    阶段4 逐日缓冲时同步维护 last_by_code / delisted_keys（免读分区二次全量），
    本函数据此补行：

    - 仅沪深A股（正则过滤，防 B股/基金/可转债混入）；
    - outDate 为空 / 主表从未出现该 code / outDate 早于主表最后出现日
      （缓存异常）跳过；
    - 主表已含 (outDate, code, delisted) 行则跳过；
    - 补行日期用 outDate（保持退市日口径）。

    返回 COLS 顺序的 patch rows（可能为空），由调用方 append_partitioned 写入。
    """
    patch = []
    for code, basic in cache.items():
        if not (ASH_PAT.match(code) or ASZ_PAT.match(code)):
            continue
        outd = basic.get("outDate") or ""
        if not outd:
            continue
        if code not in last_by_code or outd <= last_by_code[code]:
            continue
        if (outd, code) in delisted_keys:
            continue
        patch.append((outd, code, basic.get("code_name") or "",
                      "delisted", "none", False, basic.get("ipoDate") or ""))
    if patch:
        log.info("[patch] 补退市日快照缺失 delisted=%d", len(patch))
    return patch


def year_parts(hist_dir):
    """主存年份文件列表（4 位纯数字文件名，防杂散 parquet 混入主存统计/拼合）。

    读取方统一经本函数过滤文件名（单一事实源），勿直接 glob("*.parquet")。
    """
    return [p for p in sorted(Path(hist_dir).glob("*.parquet"))
            if p.stem.isdigit() and len(p.stem) == 4]


def _partition_path(hist_dir, year):
    """分区文件路径：<hist_dir>/<Y>.parquet（纯年份命名，平铺目录）。"""
    return Path(hist_dir) / ("%s.parquet" % year)


def _check_cols(df, where, src):
    """COLS 必需列校验（分区/镜像读路径统一 fail-closed）。"""
    missing = [c for c in COLS if c not in df.columns]
    if missing:
        raise ValueError("%s: %s 缺必需列 %s（fail-closed 拒绝合并）" % (where, src, missing))


def append_partitioned(hist_dir, rows):
    """每日增量写年份文件 dataset：新行按年归并 -> 受影响年份文件 sort+dedup；镜像已下线。

    分区化核心收益：只读写新行涉及的年份文件，读写量不再随历史线性增长。

    返回本次写入日（新行最大 date）的截面，供 assert_normal_threshold 使用
    （增量日即全局最新交易日）。
    """
    import pandas as pd
    df_new = pd.DataFrame(rows, columns=COLS)
    if df_new.empty:
        return df_new
    for year, g in df_new.groupby(df_new["date"].str[:4]):
        part = _partition_path(hist_dir, year)
        if part.exists():
            old = pd.read_parquet(part)
            _check_cols(old, "append_partitioned", part)
        else:
            old = pd.DataFrame(columns=COLS)
        merged = (pd.concat([old, g], ignore_index=True)
                    .sort_values(["date", "code"])
                    .drop_duplicates(["date", "code"], keep="last"))
        part.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(part, index=False)
    day = df_new["date"].max()
    return (df_new[df_new["date"] == day]
            .sort_values(["date", "code"])
            .drop_duplicates(["date", "code"], keep="last"))


def assert_normal_threshold(df, min_normal=None):
    """写盘前闸门：最新截面 normal 股数低于阈值则抛 RuntimeError（防 fail-open 清空 universe）。"""
    min_normal = MIN_NORMAL_FOR_WRITE if min_normal is None else min_normal
    latest = df[df["date"] == df["date"].max()]
    n_normal = int((latest["status"] == "normal").sum())
    if n_normal < min_normal:
        raise RuntimeError(
            "collect_stock_status 中止写入: 最新截面 normal 股数 %d < 阈值 %d，"
            "疑似双源 fail-open，拒绝写入 stock_status_history 主存" % (n_normal, min_normal))
    return n_normal
