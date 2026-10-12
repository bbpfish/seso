#!/usr/bin/env python3
"""kline_tx_client.py - 腾讯行情数据客户端层（合并两处重复实现）

职责：TX 源统一拉取入口：
  - fetch_tencent_daily：akshare 拉取（常规补采链），raw/qfq/hfq 三口径
  - fetch_tx：gtimg HTTP（历史回补/退市模式），use_newfqk=True 时走
    newfqkline 接口（支持退市股，含换手率）
  - set_use_newfqk(flag)：由回补脚本 delisted 模式切换
  - logger 命名 kline_tx_client

依赖: akshare / requests
"""
import logging
import time
from datetime import date

import numpy as np
import pandas as pd

from stock_code_utils import get_tx_symbol, is_bse_zone  # noqa: E402
from kline_env import INDEX_DIR  # noqa: E402
from io_atomic import atomic_replace  # noqa: E402

# ---- 腾讯源稳定化参数（与 backfill / fill_multi_worker 原值一致）----
TX_RETRY = 2            # gtimg HTTP 拉取重试次数（原 backfill TX_RETRY）
TX_HTTP_BACKOFF = 2.0   # gtimg HTTP 重试退避基数（原 backfill BACKOFF）
TX_TIMEOUT = 20         # gtimg HTTP 单次请求超时（秒）
SLEEP_TX = 0.3          # gtimg 每请求延时
TX_AK_RETRY = 2         # akshare 拉取重试次数（原 fill_multi_worker FETCH_RETRY）
TX_AK_BACKOFF = 1.0     # akshare 重试退避基数（原 fill_multi_worker STOCK_BACKOFF）

logger = logging.getLogger('kline_tx_client')

# delisted 模式置 True：腾讯源走 newfqkline（支持退市股，含换手率）
# P1 修复（2026-10-11）：模块级标志降级为"默认模式"（_NEWFQK_DEFAULT），
# 多线程并发拉取请用 fetch_tx(..., use_newfqk=...) 请求级参数隔离，
# 不再依赖跨线程共享全局（旧 set_use_newfqk 仅作为单线程脚本兼容入口）。
_NEWFQK_DEFAULT = False


def set_use_newfqk(flag):
    """设置腾讯拉取默认模式（兼容旧单线程调用）：True=退市股 newfqkline，False=常规 fqkline。

    注意：本函数修改的是模块默认值，多线程并发场景请使用 fetch_tx 的
    use_newfqk 请求级参数，避免跨线程全局竞态。"""
    global _NEWFQK_DEFAULT
    _NEWFQK_DEFAULT = bool(flag)
    logger.info("kline_tx_client newfqk default -> %s", _NEWFQK_DEFAULT)


def fetch_tencent_daily(code, start_date='', end_date='', adjust=''):
    """akshare 腾讯日K（stock_zh_a_hist_tx）。

    采集模式 adjust（与 kline_ths_client 同标准三模式，9 列契约）:
      ""  (standard): 不复权原价，close=原始收盘，adj_close/hfq_close 写 NaN
      "qfq" (前复权): adj_close=腾讯返回 close（前复权收盘），open/high/low/volume
                      保留（同请求前复权口径），close 置 NaN 标识非原始价
      "hfq" (后复权): hfq_close=腾讯返回 close，close 置 NaN 标识非原始价
    非法 adjust 返回 None+warning；返回列缺失补 NaN+warning 而非 KeyError；
    同一日期重复行保留最后一条（幂等）。
    口径差异（2026-09 实测）：
      - 列序为 open,close,high,low，必须按列名取值；
      - volume：akshare stock_zh_a_hist_tx 内部已按"主板/创业板 ×100、
        sh688/sz399/sh000/sz000 不乘"归一为股（akshare stock_hist_tx 源码实证），
        但 sz000 深市主板股票 gtimg 原始为手、akshare 误归指数不乘
        （2026-09 实测 000001=手，THS 同日 71534143 股），客户端仅对 sz0 补 ×100；
      - turnover 为百分比数值（akshare 原始小数 ×100，2026-10-10 统一口径，
        与 ths/tdx 契约一致；gtimg newfqkline 分支已是百分比，不换算）；
      - 北交所代码区（4/8/92）腾讯无覆盖，返回 None（源能力限制，非客户端策略）。
    """
    import akshare as ak
    if is_bse_zone(code):
        logger.warning("fetch_tencent_daily %s 北交所代码区（4/8/92），腾讯源无覆盖，返回 None", code)
        return None
    if adjust not in ("", "qfq", "hfq"):
        logger.warning("fetch_tencent_daily %s 非法 adjust=%r（仅支持 ''/qfq/hfq），返回 None",
                       code, adjust)
        return None
    symbol = get_tx_symbol(code)
    kwargs = {"symbol": symbol, "adjust": adjust}
    if start_date:
        kwargs["start_date"] = start_date
    if end_date:
        kwargs["end_date"] = end_date
    for attempt in range(1, TX_AK_RETRY + 1):
        try:
            df = ak.stock_zh_a_hist_tx(**kwargs)
            if df is None or df.empty:
                return None
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            rename_map = {'date': 'date', 'open': 'open', 'close': 'close',
                          'high': 'high', 'low': 'low', 'volume': 'volume',
                          'turnover': 'turnover'}
            missing_cols = [cn for cn in rename_map if cn not in df.columns]
            if missing_cols:
                logger.warning("fetch_tencent_daily %s 返回缺列 %s，已补 NaN（数据可能不完整）",
                               code, missing_cols)
                for cn in missing_cols:
                    df[cn] = np.nan
            df = df.rename(columns=rename_map)
            # 口径统一（2026-10-10）：akshare 腾讯 turnover 为小数（如 0.0028=0.28%），
            # ×100 转百分比数值（0.28），与 ths/tdx 契约一致；NaN 保持 NaN
            df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce') * 100.0
            df = df[['date', 'open', 'high', 'low', 'close', 'volume',
                     'turnover']].sort_values('date')
            # 幂等：同一日期重复行保留最后一条
            df = df.drop_duplicates(subset='date', keep='last')
            # volume 口径（P1 收敛）：统一走 _tx_volume_to_shares(already_shares=True)。
            # akshare stock_zh_a_hist_tx 内部已按"主板/创业板 ×100、sh688/sz399/sh000/sz000
            # 不乘"归一为股，仅 sz000 深市主板误归指数不乘（实测 000001=手），
            # 公共函数只对 sz0 补 ×100，其余不重复换算
            # （修复 sz300 创业板被 akshare ×100 后客户端再 ×100 的双重换算）。
            df['volume'] = _tx_volume_to_shares(
                symbol, pd.to_numeric(df['volume'], errors='coerce'), already_shares=True)
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
                logger.warning("fetch_tencent_daily %s 返回含 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                               code, _n_before - len(df))
            if df.empty:
                return None
            return df
        except Exception as e:
            _emsg = str(e)
            # 2026-10-09: 对齐 fetch_sina_daily——akshare 腾讯对无覆盖股票
            # （如 301 系新股/无数据）抛 "invalid literal for int() with base 10"（空/畸形 JSON
            # 解析失败），属确定性无数据，重试第二次纯属浪费请求；命中直接放弃本次拉取。
            if "invalid literal for int()" in _emsg:
                logger.warning("fetch_tencent_daily %s adjust=%r no-data fatal (skip retry): %s",
                               code, adjust, _emsg[:100])
                return None
            logger.warning("fetch_tencent_daily %s adjust=%r attempt %d/%d failed: %s",
                           code, adjust, attempt, TX_AK_RETRY, _emsg[:100])
            if attempt < TX_AK_RETRY:
                time.sleep(TX_AK_BACKOFF * attempt)
    return None


def _tx_rows_to_frame(code, rows, adjust, has_turnover):
    """gtimg 行列表 -> 标准 9 列 DataFrame（三模式契约，缺列/重复行容错）。"""
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # NaT 日期行过滤（to_datetime coerce 产生；OHLC dropna 不含 date 列，NaT 行会漏网）
    if df['date'].isna().any():
        logger.warning("fetch_tx %s 返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
        df = df[df['date'].notna()]
    df = df.sort_values('date').drop_duplicates(subset='date', keep='last')
    for _c in ['open', 'high', 'low', 'close', 'volume']:
        if _c not in df.columns:
            logger.warning("fetch_tx %s 返回缺列 %s，已补 NaN（数据可能不完整）", code, _c)
            df[_c] = np.nan
    if 'turnover' not in df.columns or not has_turnover:
        df['turnover'] = np.nan
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
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
    _n_before = len(df)
    if adjust == 'qfq':
        df = df.dropna(subset=['open', 'high', 'low', 'adj_close'])
    elif adjust == 'hfq':
        df = df.dropna(subset=['open', 'high', 'low', 'hfq_close'])
    else:
        df = df.dropna(subset=['open', 'high', 'low', 'close'])
    if len(df) < _n_before:
        logger.warning("fetch_tx %s 返回含 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                       code, _n_before - len(df))
    if df.empty:
        return None
    return df


def _normalize_tx_date(v):
    """gtimg 接口日期统一为 YYYY-MM-DD。

    date/pd.Timestamp 统一 strftime（调度端 _fetch_by_days 传 Timestamp，旧实现
    str(Timestamp) 带时间导致 param 含空格）；8 位 YYYYMMDD 转横杠；YYYY-MM-DD 原样；
    空值保留空；其余非法格式原样透传，由接口裁决（可能 param error）。"""
    if not v:
        return v
    if isinstance(v, (date, pd.Timestamp)):
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    s = str(v).strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return v


def _to_float_pct(v):
    """turnover 占位符安全转换：'-'/''/None/非数值 -> NaN，其余 float（百分比数值）。"""
    if v is None:
        return np.nan
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s in ("-", "--"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


_TX_URL_FQK = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get"
_TX_URL_NEWFQK = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"


def _tx_volume_to_shares(symbol, raw_vol, already_shares=False):
    """volume 单位归一为股（akshare/gtimg 双路径共用，P1 口径收敛）。

    - gtimg 路径（already_shares=False，默认）：原始为手——主板/创业板 ×100；
      科创板 sh688/指数 sz399/sh000 已是股不乘；sz000 深市主板 gtimg 为手照常 ×100。
    - akshare 路径（already_shares=True）：akshare 内部已按"主板/创业板 ×100、
      sh688/sz399/sh000/sz000 不乘"归一为股，仅 sz0 深主板误归指数不乘，只补 ×100。
    """
    v = float(raw_vol)
    if already_shares:
        return v * 100.0 if symbol.startswith("sz0") else v
    if symbol.startswith(("sh688", "sz399", "sh000")):
        return v
    return v * 100.0


def _fetch_tx_once(code, symbol, start, end, fq, use_newfqk):
    """单段 gtimg 请求（use_newfqk 决定 newfqkline/fqkline），返回行列表（失败 None）。

    两接口仅 URL 与 turnover 字段（newfqkline 第 8 字段）不同，合并消除重复实现；
    turnover 经 _to_float_pct 清洗（'-'/'--' 停牌占位符不再抛 ValueError 触发无效重试）。
    use_newfqk 为请求级参数（P1 修复），不读模块全局，线程安全。"""
    import requests
    url = _TX_URL_NEWFQK if use_newfqk else _TX_URL_FQK
    p = f"{symbol},day,{start},{end},640,{fq}"
    for attempt in range(1, TX_RETRY + 1):
        try:
            r = requests.get(url, params={"param": p}, timeout=TX_TIMEOUT,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            d = r.json()
            if d.get("code") != 0 or not isinstance(d.get("data"), dict):
                raise RuntimeError(f"tx code={d.get('code')} msg={d.get('msg')}")
            node = d["data"].get(symbol)
            if not isinstance(node, dict):
                raise RuntimeError("tx bad node")
            key = (fq + "day") if fq else "day"
            arr = node.get(key) or []
            rows = []
            for it in arr:
                # [date, open, close, high, low, volume(手), ?, turnover%, amount(万), ?]
                row = {
                    "date": it[0],
                    "open": float(it[1]),
                    "close": float(it[2]),
                    "high": float(it[3]),
                    "low": float(it[4]),
                    "volume": _tx_volume_to_shares(symbol, it[5]),
                }
                if use_newfqk:
                    row["turnover"] = _to_float_pct(it[7]) if len(it) > 7 else np.nan
                rows.append(row)
            return rows
        except Exception as e:
            # 日志口径（2026-10-11）：fq='' 即 standard 模式走 fqkline 接口，
            # 旧写法 adjust=%s 空值显示为 adjust= 易被误读为复权，改为显式 mode 标注
            mode = "standard" if not fq else fq
            logger.warning("%s mode=%s tx(%s) attempt %d/%d failed: %s", code, mode,
                           "newfqk" if use_newfqk else "fqk", attempt, TX_RETRY, str(e)[:120])
            if attempt < TX_RETRY:
                time.sleep(TX_HTTP_BACKOFF * attempt)
    return None


def fetch_tx(code, start, end, adjust="", use_newfqk=None):
    """gtimg 腾讯日K：常规 fqkline；use_newfqk=True 时走 newfqkline（退市股，含换手率）。

    use_newfqk: 请求级模式（P1 修复，消除跨线程全局竞态）；None 时取模块默认
    （set_use_newfqk 设置，兼容旧单线程脚本）。多线程并发时务必显式传参隔离。
    日期约定：start/end 接受 date/pd.Timestamp/8 位 YYYYMMDD 或 YYYY-MM-DD，
    空值保留空（取最近数据）；入口统一经 _normalize_tx_date 规范化为 YYYY-MM-DD——
    gtimg param 严格要求横杠格式，传 YYYYMMDD 会触发 code=0 msg=param error。
    北交所代码区（4/8/92）腾讯无覆盖，客户端直接拦截（源能力限制，与 akshare 口径一致）；
    非法 adjust 返回 None+warning；三模式均返回 9 列契约（与 kline_ths_client 一致）。
    分段拉取：gtimg 单次返回根数上限 640，返回满 640 根时 end 前移续段，
    直至不满 640 或越过 start——消除长区间（退市股历史回补）早期段静默截断
    （与 ths/tdx 分段能力对齐）。"""
    if is_bse_zone(code):
        logger.warning("fetch_tx %s 北交所代码区（4/8/92），腾讯源无覆盖，返回 None", code)
        return None
    if adjust not in ("", "qfq", "hfq"):
        logger.warning("fetch_tx %s 非法 adjust=%r（仅支持 ''/qfq/hfq），返回 None", code, adjust)
        return None
    if use_newfqk is None:
        use_newfqk = _NEWFQK_DEFAULT
    start = _normalize_tx_date(start)
    end = _normalize_tx_date(end)
    symbol = get_tx_symbol(code)
    all_rows = []
    seg_end = end
    while True:
        rows = _fetch_tx_once(code, symbol, start, seg_end, adjust, use_newfqk)
        if rows is None:
            break  # 单段失败：前段已成功则返回已采部分，否则整体 None
        all_rows = rows + all_rows  # 新段时间更早，前插
        if not rows or len(rows) < 640:
            break  # 未达上限：区间数据已完整
        d_min = min(r["date"] for r in rows)
        if d_min <= start:
            break  # 已越过起点：start 之前无更多数据
        seg_end = (pd.Timestamp(d_min) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        if seg_end < start:
            break
        time.sleep(SLEEP_TX)  # 段间节流（原 finally sleep 仅保留在续段路径）
    if not all_rows:
        return None
    return _tx_rows_to_frame(code, all_rows, adjust, has_turnover=use_newfqk)


INDEX_COLS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def fetch_index_daily(code: str, start_date: str = "", end_date: str = ""):
    """拉取单只指数日K（腾讯源 akshare），返回统一指数契约 DataFrame 或 None。

    契约（INDEX_COLS）：date/open/high/low/close/volume/amount。
    - 腾讯指数接口返回 date/open/close/high/low/amount，无 volume 列 → 补 NaN+warning
    - amount 单位=万元（实测对照：2026-10-09 上证 amount=535149921.0 ≈ 当日成交额 5351 亿元；
      新浪 volume≈535 亿股口径吻合），契约保留源值，不做单位换算
    - 指数无复权/换手率概念：akshare 文档注明腾讯指数为前复权口径，本函数不做复权处理、
      不填 turnover；三复权模式对指数无意义
    - start_date/end_date 格式 YYYYMMDD 或 YYYY-MM-DD，为空则接口自行全区间
    """
    import akshare as ak
    from stock_code_utils import get_index_symbol

    try:
        symbol = get_index_symbol(code)
        if symbol is None:
            logger.warning("Index %s: 非指数代码段（000/399），返回 None", code)
            return None
        _s = start_date.replace("-", "") if start_date else ""
        _e = end_date.replace("-", "") if end_date else ""
        df = ak.stock_zh_index_daily_tx(symbol=symbol, start_date=_s, end_date=_e)
        if df is None or df.empty:
            logger.warning("Index %s: no data", code)
            return None
        for _c in ['date', 'open', 'high', 'low', 'close', 'amount']:
            if _c not in df.columns:
                logger.warning("Index %s: 腾讯返回缺列 %s，已补 NaN", code, _c)
                df[_c] = float('nan')
        df = df[['date', 'open', 'high', 'low', 'close', 'amount']].copy()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # 腾讯指数接口无 volume 列（只有 amount），契约 volume 置 NaN
        df['volume'] = float('nan')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.sort_values('date').drop_duplicates(subset='date', keep='last')
        if df['date'].isna().any():
            logger.warning("Index %s: 返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
            df = df[df['date'].notna()]
        if df.empty:
            return None
        return df[INDEX_COLS]
    except Exception as e:
        logger.warning("Index %s fetch failed: %s", code, str(e)[:100])
        return None


def update_index(code, start_date: str = "", end_date: str = ""):
    """腾讯源指数落盘（独立流程）：拉取指数日K，增量合并写 INDEX_DIR/{code}.parquet。

    落盘列为统一指数契约 INDEX_COLS（含 volume 全 NaN），兼容下游 date/close 读取。
    start_date/end_date 为空时接口自行全区间；已有本地文件则只增量合并新日期。
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
        df = fetch_index_daily(code, start_date=start_date, end_date=end_date)
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
