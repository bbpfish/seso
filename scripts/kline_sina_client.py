#!/usr/bin/env python3
"""kline_sina_client.py - 新浪行情数据客户端层（从 kline_sina.py 拆分）

职责：Sina 源数据请求/映射/稳定化：
  - get_sina_symbol：代码 -> 新浪符号映射
  - fetch_sina_daily：akshare 日K拉取（raw/qfq/hfq 三口径，自带重试）
  - _patch_requests_timeout：akshare 底层 requests 超时补丁
  - 限流/重试常量：REQ_TIMEOUT / FETCH_RETRY / STOCK_BACKOFF / STOCK_HARD_LIMIT / SLEEP
  - update_index + INDEX_CODES：指数拉取（独立流程）
  - logger 命名 kline_sina_client

依赖: akshare（Sina 源）
"""
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.append(_HERE)
from kline_env import INDEX_DIR  # noqa: E402
from io_atomic import atomic_replace  # noqa: E402
from stock_code_utils import get_sina_symbol  # noqa: E402

# 指数目录统一定义在 kline_env（DATA_DIR/"index"），此处直接引用。

# 指数符号表（仅供独立指数采集流程 /data/index/ 使用，与股票流程隔离）。
# 2026-10-11 复核（P1 修正）：000001/000016/000300/000905/000852 均为沪市指数代码
# （sh000001 上证指数、sh000016 上证50、sh000300 沪深300、sh000905 中证500、
#  sh000852 中证1000），非深市股票——update_index 以 "000 开头 -> sh" 构造 symbol
# 恰好正确；深市指数 399xxx 走 sz 前缀亦正确。注意 000 开头同时是深市股票段
# （sz000001 平安银行），指数/股票区分完全依赖前缀，调用方须保证传入纯指数代码。
INDEX_CODES = {'000001', '000016', '000300', '000905', '000852'}

# ---- 新浪源稳定化参数（2026-09-08 修复：防 akshare 无超时导致整任务卡死）----
REQ_TIMEOUT = 5         # 单次 HTTP 请求超时（秒）：正常请求1.5s，5s阈值安全兜底
FETCH_RETRY = 2         # 每次拉取失败重试次数（含退避）
STOCK_BACKOFF = 1.0     # 重试退避基数（秒）
STOCK_HARD_LIMIT = 15   # 单只股票处理硬上限（秒）：正常票5s内，15s余量足够，超时跳票
SLEEP = 0.3

logger = logging.getLogger('kline_sina_client')


def _patch_requests_timeout():
    """akshare 新浪接口底层走 requests 且无 timeout，全局打补丁兜底，防 socket 永久阻塞。
    幂等：经 requests._marvis_timeout_patched 共享标志（与 run_kline_multi 同一标志），
    重复调用/跨模块调用不会层层包装 _orig 引用链（旧实现重复调用会嵌套包装且无法还原）。"""
    try:
        import requests
        if getattr(requests, "_marvis_timeout_patched", False):
            return

        _orig_get = requests.get
        def _get(url, *args, **kwargs):
            kwargs.setdefault('timeout', REQ_TIMEOUT)
            return _orig_get(url, *args, **kwargs)
        requests.get = _get

        _orig_post = requests.post
        def _post(url, *args, **kwargs):
            kwargs.setdefault('timeout', REQ_TIMEOUT)
            return _orig_post(url, *args, **kwargs)
        requests.post = _post

        _orig_req = requests.Session.request
        def _sreq(self, method, url, *args, **kwargs):
            kwargs.setdefault('timeout', REQ_TIMEOUT)
            return _orig_req(self, method, url, *args, **kwargs)
        requests.Session.request = _sreq

        requests._marvis_timeout_patched = True
        logger.info("Patched requests default timeout -> %ss", REQ_TIMEOUT)
    except Exception as e:
        logger.warning("requests timeout patch failed: %s", str(e)[:80])


def fetch_sina_daily(code, start_date='', end_date='', adjust=''):
    """用 akshare 获取日K线（含换手率）。自带重试，防新浪 hang 死。

    采集模式 adjust（与 kline_ths_client 同标准三模式，9 列契约）:
      ""  (standard): 不复权原价，close=原始收盘，adj_close/hfq_close 写 NaN
      "qfq" (前复权): adj_close=新浪返回 close（前复权收盘），open/high/low/volume
                      保留（同请求前复权口径），close 置 NaN 标识非原始价
      "hfq" (后复权): hfq_close=新浪返回 close，close 置 NaN 标识非原始价
    非法 adjust 返回 None+warning；返回列缺失补 NaN+warning 而非 KeyError；
    同一日期重复行保留最后一条（幂等）。
    口径（2026-10-10 统一）：turnover 为百分比数值（akshare 原始小数 ×100），
    与 ths/tdx 契约一致（如 0.2846 表示 0.2846%）。"""
    import akshare as ak
    if adjust not in ("", "qfq", "hfq"):
        logger.warning("fetch_sina_daily %s 非法 adjust=%r（仅支持 ''/qfq/hfq），返回 None",
                       code, adjust)
        return None
    symbol = get_sina_symbol(code)
    # 9/14审核：start_date='' 不能原样透传给 stock_zh_a_daily——akshare 内部对部分股票
    # （实测 13 只科创板 ST，如 sh688022）会执行 DatetimeIndex.loc['':end] 并抛
    # "cannot do slice indexing on DatetimeIndex with these indexers [] of type str"，
    # 导致全新文件全量兜底必败 no_data。空日期直接不传（接口默认取全历史）。
    kwargs = {"symbol": symbol, "adjust": adjust}
    if start_date:
        kwargs["start_date"] = start_date
    if end_date:
        kwargs["end_date"] = end_date
    # 单只硬上限：整段拉取（含重试）累计超过 STOCK_HARD_LIMIT 秒即跳票，
    # 防新浪接口异常慢/挂起时拖垮整批调度（P1 修复，2026-10-11）。
    _t0 = time.monotonic()
    for attempt in range(1, FETCH_RETRY + 1):
        try:
            df = ak.stock_zh_a_daily(**kwargs)
            if time.monotonic() - _t0 > STOCK_HARD_LIMIT:
                logger.warning("fetch_sina_daily %s 累计耗时超硬上限 %.0fs，跳票（下轮增量自愈）",
                               code, STOCK_HARD_LIMIT)
                return None
            if df is None or df.empty:
                return None
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            rename_map = {'date': 'date', 'open': 'open', 'high': 'high',
                          'low': 'low', 'close': 'close', 'volume': 'volume',
                          'turnover': 'turnover'}
            missing_cols = [cn for cn in rename_map if cn not in df.columns]
            if missing_cols:
                logger.warning("fetch_sina_daily %s 返回缺列 %s，已补 NaN（数据可能不完整）",
                               code, missing_cols)
                for cn in missing_cols:
                    df[cn] = np.nan
            df = df.rename(columns=rename_map)
            # NaT 日期行过滤（to_datetime coerce 产生；OHLC dropna 不含 date 列，NaT 行会漏网）
            if df['date'].isna().any():
                logger.warning("fetch_sina_daily %s 返回含 %d 行 NaT 日期，已过滤",
                               code, int(df['date'].isna().sum()))
                df = df[df['date'].notna()]
            # 口径统一（2026-10-10）：akshare 新浪 turnover 为小数（如 0.002846=0.28%），
            # ×100 转百分比数值（0.2846），与 ths/tdx 契约一致；NaN 保持 NaN
            df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce') * 100.0
            df = df[['date', 'open', 'high', 'low', 'close', 'volume',
                     'turnover']].sort_values('date')
            # 幂等：同一日期重复行保留最后一条
            df = df.drop_duplicates(subset='date', keep='last')
            # 三模式 9 列契约（与 kline_ths_client 一致）
            if adjust == 'qfq':
                df['adj_close'] = df['close']
                df['close'] = np.nan  # 非原始价
                df['hfq_close'] = np.nan
            elif adjust == 'hfq':
                df['hfq_close'] = df['close']
                df['close'] = np.nan  # 非原始价
                df['adj_close'] = np.nan
            else:  # standard
                df['adj_close'] = np.nan
                df['hfq_close'] = np.nan
            df = df[['date', 'open', 'high', 'low', 'close', 'volume',
                     'adj_close', 'hfq_close', 'turnover']]
            # OHLC 残缺行过滤（与 THS 侧同款，下轮增量自愈）
            _n_before = len(df)
            if adjust == 'qfq':
                df = df.dropna(subset=['open', 'high', 'low', 'adj_close'])
            elif adjust == 'hfq':
                df = df.dropna(subset=['open', 'high', 'low', 'hfq_close'])
            else:
                df = df.dropna(subset=['open', 'high', 'low', 'close'])
            if len(df) < _n_before:
                logger.warning("fetch_sina_daily %s 返回含 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                               code, _n_before - len(df))
            if df.empty:
                return None
            return df
        except Exception as e:
            _emsg = str(e)
            # 2026-10-09: akshare 对退市/无源股票抛 "No value to decode"（空 JSON 解析失败），
            # 属确定性无数据，重试第二次纯属浪费请求；命中直接放弃本次拉取。
            if "No value to decode" in _emsg:
                logger.warning("fetch_sina_daily %s adjust=%s no-data fatal (skip retry): %s",
                               code, adjust, _emsg[:100])
                return None
            logger.warning("fetch_sina_daily %s adjust=%s attempt %d/%d failed: %s",
                           code, adjust, attempt, FETCH_RETRY, _emsg[:100])
            if attempt < FETCH_RETRY:
                time.sleep(STOCK_BACKOFF * attempt)
    return None


INDEX_COLS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def fetch_index_daily(code: str, start_date: str = "", end_date: str = ""):
    """拉取单只指数日K（新浪源），返回统一指数契约 DataFrame 或 None。

    契约（INDEX_COLS）：date/open/high/low/close/volume/amount。
    - volume 单位=股（新浪指数接口直通）；amount 源无该列，置 NaN
    - 指数无复权/换手率概念，不做复权换算、不填 turnover
    - 新浪接口不支持日期参数，全量拉取后按 start_date/end_date（YYYY-MM-DD 或
      YYYYMMDD）本地过滤；为空则返回全部
    """
    import akshare as ak
    from stock_code_utils import get_index_symbol

    try:
        symbol = get_index_symbol(code)
        if symbol is None:
            logger.warning("Index %s: 非指数代码段（000/399），返回 None", code)
            return None
        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is None or df.empty:
            logger.warning("Index %s: no data", code)
            return None
        df = df.rename(columns={})
        for _c in ['date', 'open', 'high', 'low', 'close', 'volume']:
            if _c not in df.columns:
                logger.warning("Index %s: 新浪返回缺列 %s，已补 NaN", code, _c)
                df[_c] = float('nan')
        df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['amount'] = float('nan')
        if start_date:
            df = df[df['date'] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df['date'] <= pd.Timestamp(end_date)]
        df = df.sort_values('date').drop_duplicates(subset='date', keep='last')
        if df['date'].isna().any():
            logger.warning("Index %s: 返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
            df = df[df['date'].notna()]
        if df.empty:
            return None
        return df[INDEX_COLS]
    except Exception as e:
        _emsg = str(e)
        if "No value to decode" in _emsg:
            logger.warning("Index %s: no-data fatal (skip retry): %s", code, _emsg[:100])
            return None
        logger.warning("Index %s fetch failed: %s", code, _emsg[:100])
        return None


def update_index(code):
    """新浪源指数落盘（独立流程）：拉取指数日K，增量合并写 INDEX_DIR/{code}.parquet。

    落盘列为统一指数契约 INDEX_COLS（含 amount 列，全 NaN），兼容下游 date/close 读取。
    """
    from stock_code_utils import get_index_symbol

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    fp = INDEX_DIR / f"{code}.parquet"
    existing_end = None
    if fp.exists():
        try:
            df = pd.read_parquet(fp, columns=['date'])
            existing_end = pd.to_datetime(df['date']).max()
        except Exception:
            pass
    try:
        if get_index_symbol(code) is None:
            logger.warning("Index %s: 非指数代码段（000/399），跳过落盘", code)
            return False
        df = fetch_index_daily(code)
        if df is None or df.empty:
            logger.warning("Index %s: no data", code)
            return False
        if existing_end:
            new_part = df[df['date'] > existing_end]
            if new_part.empty:
                return True
            old_df = pd.read_parquet(fp)
            df = pd.concat([old_df, new_part], ignore_index=True)
            df.drop_duplicates(subset=['date'], keep='last', inplace=True)
            df.sort_values('date', inplace=True)
        _tmp = fp.with_suffix(".parquet.tmp")
        df.to_parquet(_tmp, index=False)
        atomic_replace(_tmp, fp)
        return True
    except Exception as e:
        logger.error("Index %s failed: %s", code, str(e)[:60])
        return False
