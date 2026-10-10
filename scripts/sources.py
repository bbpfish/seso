#!/usr/bin/env python3
"""
sources.py — 备源快照与北交所补充（状态采集专用）

版本随 status_schema.SCHEMA_VERSION 单点维护，本文件不重复定义版本号。

职责：
- Tushare 备源快照（stock_basic 在市名单，覆盖 baostock 故障）
- akshare 北交所列表（覆盖 baostock 不覆盖 bj 的短板）

安全说明：TUSHARE_TOKEN 环境变量优先；兜底 token 文件与代码同目录
（已列入 .gitignore），打包分发镜像时注意勿将其带入（建议走环境变量）。
"""
import logging
import os

from pathlib import Path

log = logging.getLogger(__name__)

__all__ = ["tushare_snapshot", "fetch_bj_universe"]

TUSHARE_TOKEN_FILE = Path(__file__).with_name("tushare.token")

# 北交所 akshare 接口必需列（列名变更时显式报错，不静默吞 KeyError）
_BJ_REQUIRED_COLS = ["证券代码", "证券简称", "上市日期"]


def _norm_date(x):
    """Tushare list_date '20260929' -> '2026-09-29'；其它原样返回。"""
    if len(x) == 8 and x.isdigit():
        return "%s-%s-%s" % (x[:4], x[4:6], x[6:])
    return x


def _safe_str(v):
    """NaN/None -> ''，其余 str 化（防 itertuples 产生 'nan'/'None' 污染名称/日期）。"""
    import pandas as pd
    return "" if pd.isna(v) else str(v)


def tushare_snapshot(day, ash_pat, asz_pat):
    """Tushare 备源一次拉取（stock_basic 在市名单）。

    参数 day 仅用于日志标识（stock_basic 为全量在市名单，与具体交易日无关）；
    当日全部视为在市（tradeStatus=1），无停牌粒度，属备源已知局限。

    返回 (snap, bj_universe, basic_extra)：
      snap        {code: (tradeStatus, code_name)}   沪深口径
      bj_universe {code6: {'name','list_date'}}      北交所（覆盖 baostock 短板）
      basic_extra {code: basic_dict}                 ipoDate 用 list_date，outDate 空

    注意：任何失败均返回 ({}, {}, {})（fail-open 返回空三元组），调用方必须
    依赖 status_schema.assert_normal_threshold 写盘闸门兜底，不得据此判定
    "市场确实为空"。
    """
    token = os.environ.get("TUSHARE_TOKEN", "")
    if not token and TUSHARE_TOKEN_FILE.exists():
        token = TUSHARE_TOKEN_FILE.read_text(encoding="utf-8-sig").strip()
    if not token:
        log.warning("[tushare] 无 TUSHARE_TOKEN（环境变量或 %s 缺失）", TUSHARE_TOKEN_FILE)
        return {}, {}, {}
    try:
        import tushare as ts
        pro = ts.pro_api(token)
        df = pro.stock_basic(exchange="", list_status="L",
                             fields="ts_code,symbol,name,market,list_date")
    except Exception as e:
        log.error("[tushare] stock_basic FAIL %s（备源返回空三元组，交由阈值闸门兜底）", e)
        return {}, {}, {}
    if df is None or df.empty:
        log.warning("[tushare] %s stock_basic 返回空（备源返回空三元组）", day)
        return {}, {}, {}
    # 列名兼容：symbol / ts_code 统一归一为 code6（去交易所后缀）
    df.columns = [str(c).lower() for c in df.columns]
    code_col = "symbol" if "symbol" in df.columns else "ts_code"
    if code_col not in df.columns:
        log.error("[tushare] 返回列 %s 无 symbol/ts_code，放弃本源", list(df.columns))
        return {}, {}, {}
    df["code6"] = df[code_col].astype(str).str.split(".").str[0]
    if "ts_code" not in df.columns:
        df["ts_code"] = ""
    snap, bj_u, extra = {}, {}, {}
    for row in df[["code6", "name", "list_date", "market", "ts_code"]].itertuples(index=False, name=None):
        code6, nm, ld, mkt, ts_code = (_safe_str(v).strip() for v in row)
        ld = _norm_date(ld)
        if mkt == "北交所":
            bj_u[code6] = {"name": nm, "list_date": ld}
            continue
        code = None
        if ts_code.endswith(".SH") and ash_pat.match("sh." + code6):
            code = "sh." + code6
        elif ts_code.endswith(".SZ") and asz_pat.match("sz." + code6):
            code = "sz." + code6
        else:
            # 兜底：上游 ts_code 缺失或无交易所后缀（仅 symbol 列）时按代码段规则
            # 归属，防止沪深股全部静默漏采（60/68->SH，00/30->SZ，由正则裁决）
            if ash_pat.match("sh." + code6):
                code = "sh." + code6
            elif asz_pat.match("sz." + code6):
                code = "sz." + code6
        if code:
            snap[code] = ("1", nm)
            extra[code] = {"code": code, "code_name": nm, "ipoDate": ld,
                           "outDate": "", "status": "1"}
    log.info("[tushare] %s 备源 snap=%d bj=%d", day, len(snap), len(bj_u))
    return snap, bj_u, extra


def fetch_bj_universe():
    """akshare 北交所列表 -> {code: {'name','list_date'}}；失败返回空并告警。"""
    try:
        import akshare as ak
        import pandas as pd
        df = ak.stock_info_bj_name_code()
        missing = [c for c in _BJ_REQUIRED_COLS if c not in df.columns]
        if missing:
            log.error("[bj] ak 返回缺必需列 %s（akshare 接口可能变更），北交所补充缺失，继续",
                      missing)
            return {}
        out = {}
        for code, nm, ld in df[_BJ_REQUIRED_COLS].itertuples(index=False, name=None):
            code = str(code).strip().zfill(6)
            nm = str(nm).strip()
            ld = str(ld).strip() if pd.notna(ld) else ""
            out[code] = {"name": nm, "list_date": ld}
        return out
    except Exception as e:
        log.warning("[bj] ak fetch FAIL %s（北交所补充缺失，继续）", e)
        return {}
