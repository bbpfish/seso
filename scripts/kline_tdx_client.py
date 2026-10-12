#!/usr/bin/env python3
"""TDX（通达信新协议）日K单连接封装 - tdxdata 0.1.1

数据契约（2026-10-06 实测对齐）：
- 仅支持 standard（未复权）模式：TDX 通达信协议不提供复权数据，客户端不暴露 adjust 参数
  （与 kline_ths_client/kline_sina_client/kline_tx_client"有能力才暴露 adjust"的签名策略一致）
- vol 单位=手，落库前 ×100 对齐 THS 的"股"单位（000001 2026-09-28: TDX 715341 手 ≈ THS 71534143 股）
- turnover：TDX 标准请求不计算 turnover；9 列契约保留 turnover 列并置 NaN
- 不支持北交所（4/8/92 开头），调用方负责把北交所全归 THS 分片
- 单连接单线程顺序调用：服务器端每请求固定约 3s 延迟，严禁多线程/多连接并发
  （与 THS 会话同级别封 IP 红线）
- 单次请求可带 count 根数（15 根增量与 600 根全量均一次请求，成本相同）

依赖: tdxdata==0.1.1（容器 backtrader 持久化）
"""
import logging

import pandas as pd

from stock_code_utils import is_bse_zone  # noqa: E402
from kline_env import INDEX_DIR  # noqa: E402
from io_atomic import atomic_replace  # noqa: E402

logger = logging.getLogger('kline_tdx_client')


class TDXConnectionError(Exception):
    """TDX 会话失效（socket 断开/连接重置/EOF），需上层重建会话后重试。"""


def _tdx_bars_or_raise(client, market, code, start, count):
    """封装 client.bars：socket 级异常映射为 TDXConnectionError（上层据此重建会话）。

    注意：tdxdata 0.1.1 的 bars 签名是 bars(category, code, start, count, market)，
    本函数第一个形参名为 market 实为 category（历史命名沿用，勿改位置语义）。"""
    try:
        return client.bars(market, code, start, count)
    except (OSError, ConnectionError, EOFError) as e:
        raise TDXConnectionError(str(e)[:200])


def _tdx_bars_with_market_or_raise(client, code, start, count, market):
    """指数专用封装：client.bars 显式传 market（000 沪指/399 深指）+ is_index=True
    （指数响应每行 vol/amount 后有 4 字节尾随字段，tdxdata 0.1.1 需 is_index 跳过，
    否则逐行错位 → count 大值分段全量不可靠）；
    socket 级异常映射为 TDXConnectionError。"""
    try:
        return client.bars(9, code, start, count, market=market, is_index=True)
    except (OSError, ConnectionError, EOFError) as e:
        raise TDXConnectionError(str(e)[:200])


def _tdx_verify_has_data(code, count):
    """用临时新连接验证某股是否有数据（区分"会话断开空响应"与"真无数据"）。

    TDX 断线时旧 socket 空读，bars 返回 [] 会被上层误判 no_data；临时新连接
    若查到数据则证明旧会话已断（返回 True），新连接也空则属真无数据（False）。
    仅异常路径调用：每只 no_data 股 +1 临时连接（单请求即关，不违反单连接红线）。"""
    try:
        from tdxdata import TdxClient
        vc = TdxClient()
        try:
            vc.connect()
        except Exception:
            return False
        try:
            rows = vc.bars(9, code, 0, min(count, 15) or 1)
            return bool(rows)
        finally:
            vc.close()
    except Exception:
        return False


MAX_BARS_PER_REQ = 800   # 服务器端单次请求根数上限（实测 900 起返回 0）


def fetch_tdx_daily(code: str, count: int = 15, client=None):
    """拉取单只股票日K（仅 standard 未复权），返回标准 9 列 DataFrame 或 None。

    client: 复用已连接的 TdxClient（主流程单连接循环）；为 None 时自建临时连接（补缺场景）。
    count>MAX_BARS_PER_REQ 时分段拉取合并。
    仅支持 standard 模式：TDX 通达信协议只提供未复权日K（bars 9），源无复权能力，
    故不暴露 adjust 参数。
    turnover：标准请求不计算 turnover，9 列契约中 turnover 列置 NaN；
    当日 turnover 仅在 THS 批量预取失败/缺股时，由 fetch_tdx_turnover_batch 独立补（本函数不做）。
    """
    if is_bse_zone(code):
        logger.warning("[%s] 北交所代码区（4/8/92），TDX 无覆盖，返回 None", code)
        return None
    from tdxdata import TdxClient

    own = client is None
    if own:
        client = TdxClient()
        try:
            client.connect()  # 纳入 try：连接失败返回 None 而非异常穿透（封 IP 红线下需资源可控）
        except Exception as e:
            logger.warning("[%s] TDX connect 失败：%s", code, str(e)[:120])
            return None
    try:
        if count <= MAX_BARS_PER_REQ:
            rows = _tdx_bars_or_raise(client, 9, code, 0, count)
            if not rows:
                if _tdx_verify_has_data(code, count):
                    raise TDXConnectionError("空响应但临时连接可查到数据：会话已断开")
                return None
            parts = [rows]
        else:
            parts = []
            offset = 0
            remain = count
            while remain > 0:
                n = min(remain, MAX_BARS_PER_REQ)
                rows = _tdx_bars_or_raise(client, 9, code, offset, n)
                if not rows:
                    if _tdx_verify_has_data(code, n):
                        raise TDXConnectionError("分段空响应但临时连接可查到数据：会话已断开")
                    break
                parts.append(rows)
                offset += n
                remain -= n
            if not parts:
                return None
        frames = [pd.DataFrame(r) for r in parts if r]
        df = pd.concat(frames, ignore_index=True)
    finally:
        if own:
            client.close()
    if df.empty:
        return None
    if 'datetime' not in df.columns:
        logger.warning("[%s] TDX 返回缺列 datetime，无法定位日期，返回 None（字段变更需排查 tdxdata）", code)
        return None
    # P0-1 防御（2026-10-11 实测）：tdxdata 0.1.1 parse_bars_response 已把日期格式化为
    # 'YYYY-MM-DD HH:MM' 字符串，直接 to_datetime 无纳秒风险；但若上游协议变更回传
    # 整数 YYYYMMDD，pd.to_datetime(int) 会按纳秒解释成 1970 附近日期，故按类型分支防御。
    _dt = df['datetime']
    if pd.api.types.is_integer_dtype(_dt):
        logger.warning("[%s] TDX datetime 为整数类型（协议变更？），按 %%Y%%m%%d 字符串解析防御", code)
        _dt = _dt.astype(str)
    df['date'] = pd.to_datetime(_dt, errors='coerce').dt.normalize()
    df = df.rename(columns={'vol': 'volume'})
    # 缺列补 NaN+warning 而非 KeyError（与 kline_ths_client 同款容错）
    for _c in ['open', 'high', 'low', 'close', 'volume']:
        if _c not in df.columns:
            logger.warning("[%s] TDX 返回缺列 %s，已补 NaN（数据可能不完整）", code, _c)
            df[_c] = float('nan')
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].sort_values('date')
    # 日期范围断言（P0-1）：合法区间 [1990-01-01, 采集日+2天]，越界视为脏数据过滤
    # （防未来协议回传整数被误解析、或字段错位产生的 1970 伪日期）
    _lo = pd.Timestamp('1990-01-01')
    _hi = pd.Timestamp.now().normalize() + pd.Timedelta(days=2)
    _n_out = int(((df['date'] < _lo) | (df['date'] > _hi)).sum())
    if _n_out:
        logger.warning("[%s] TDX 返回 %d 行日期越界，已过滤（合法范围 %s ~ %s）", code,
                       _n_out, _lo.date(), _hi.date())
        df = df[(df['date'] >= _lo) & (df['date'] <= _hi)]
    # NaT 日期行过滤（to_datetime coerce 产生；OHLC dropna 不含 date 列，NaT 行会漏网）
    if df['date'].isna().any():
        logger.warning("[%s] TDX 返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
        df = df[df['date'].notna()]
    # 幂等：同一日期重复行保留最后一条（分段循环/多次拉取防重复）
    df = df.drop_duplicates(subset='date', keep='last')
    # vol 单位=手 → 股（对齐 THS volume 口径）
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce') * 100.0
    # TDX 仅 standard 模式：close 为原始收盘，adj_close/hfq_close 按 9 列契约置 NaN
    df['adj_close'] = float('nan')
    df['hfq_close'] = float('nan')
    # turnover：TDX standard 请求不计算 turnover；
    #   9 列契约保留该列置 NaN。
    df['turnover'] = float('nan')
    df = df[['date', 'open', 'high', 'low', 'close', 'volume',
             'adj_close', 'hfq_close', 'turnover']]
    # 与 THS 侧同款守卫：OHLC 残缺行直接过滤，日期缺口下轮增量自愈
    _n_before = len(df)
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    if len(df) < _n_before:
        logger.warning("[%s] TDX 返回 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                       code, _n_before - len(df))
    if df.empty:
        return None
    return df


def fetch_tdx_turnover_batch(codes, count=1, client=None):
    """独立补 turnover 备源：批量计算当日 turnover，返回 {code: turnover} 映射。

    codes: 单只代码 str 或代码列表；内部逐股顺序调用（遵守单连接单线程红线）。
    count: 取最近 count 根 K 线 volume（默认 1，仅当日）。
    计算：turnover = volume(股) / 流通股本(股) × 100；volume 已 ×100 归一为股，
    finance 返回 liutongguben 单位为股。
    失败/缺值一律置 NaN 返回（不伪造），与标准请求 NaN 口径一致。
    """
    if isinstance(codes, str):
        codes = [codes]
    result = {}
    from tdxdata import TdxClient

    own = client is None
    if own:
        client = TdxClient()
        try:
            client.connect()
        except Exception as e:
            logger.warning("TDX connect 失败：%s", str(e)[:120])
            return {c: float('nan') for c in codes}
    try:
        for code in codes:
            try:
                rows = client.bars(9, code, 0, count)
                fin = client.finance(code)
                if not rows or not (fin and fin.get('ok') and fin.get('liutongguben')):
                    result[code] = float('nan')
                    continue
                last_row = max(rows, key=lambda r: str(r.get('datetime', '')))  # 按日期显式取 max（旧 rows[-1] 依赖升序假设，倒序时静默取错根）
                last_vol = float(last_row.get('vol', 0) or 0) * 100.0
                _lt = float(fin['liutongguben'])
                if _lt <= 0:
                    result[code] = float('nan')
                    continue
                result[code] = last_vol / _lt * 100.0
            except Exception as e:
                logger.warning("[%s] TDX 独立补 turnover 失败：%s", code, str(e)[:120])
                result[code] = float('nan')
    finally:
        if own:
            client.close()
    return result


INDEX_COLS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def fetch_index_daily(code: str, count: int = 15, client=None):
    """拉取单只指数日K（TDX 通达信协议），返回统一指数契约 DataFrame 或 None。

    契约（INDEX_COLS）：date/open/high/low/close/volume/amount。
    - 指数走 bars 通道（category=9 日K），须显式传 market（000->1 沪市 / 399->0 深市），
      否则 tdxdata market_of 对 000 开头默认按深市（0）处理，000001 上证指数会拉错标的
    - vol 单位=手，×100 对齐契约"股"口径（与 fetch_tdx_daily 同款归一）；amount 单位=元直通
    - 指数无复权/换手率概念：不做复权处理、不填 turnover（与股票 standard 口径一致）
    - count>MAX_BARS_PER_REQ 时分段拉取合并；client 为 None 时自建临时连接
    """
    from stock_code_utils import get_index_tdx_market
    from tdxdata import TdxClient

    market = get_index_tdx_market(code)
    if market is None:
        logger.warning("[%s] 非指数代码段（000/399），TDX 指数返回 None", code)
        return None
    own = client is None
    if own:
        client = TdxClient()
        try:
            client.connect()
        except Exception as e:
            logger.warning("[%s] TDX index connect 失败：%s", code, str(e)[:120])
            return None
    try:
        if count <= MAX_BARS_PER_REQ:
            rows = _tdx_bars_with_market_or_raise(client, code, 0, count, market)
            if not rows:
                return None
            parts = [rows]
        else:
            parts = []
            offset = 0
            remain = count
            while remain > 0:
                n = min(remain, MAX_BARS_PER_REQ)
                rows = _tdx_bars_with_market_or_raise(client, code, offset, n, market)
                if not rows:
                    break
                parts.append(rows)
                offset += n
                remain -= n
            if not parts:
                return None
        frames = [pd.DataFrame(r) for r in parts if r]
        df = pd.concat(frames, ignore_index=True)
    finally:
        if own:
            client.close()
    if df.empty:
        return None
    if 'datetime' not in df.columns:
        logger.warning("[%s] TDX 指数返回缺列 datetime，返回 None", code)
        return None
    _dt = df['datetime']
    if pd.api.types.is_integer_dtype(_dt):
        _dt = _dt.astype(str)
    df['date'] = pd.to_datetime(_dt, errors='coerce').dt.normalize()
    df = df.rename(columns={'vol': 'volume'})
    for _c in ['open', 'high', 'low', 'close', 'volume', 'amount']:
        if _c not in df.columns:
            logger.warning("[%s] TDX 指数返回缺列 %s，已补 NaN", code, _c)
            df[_c] = float('nan')
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']].sort_values('date')
    _lo = pd.Timestamp('1990-01-01')
    _hi = pd.Timestamp.now().normalize() + pd.Timedelta(days=2)
    df = df[(df['date'] >= _lo) & (df['date'] <= _hi)]
    if df['date'].isna().any():
        logger.warning("[%s] TDX 指数返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
        df = df[df['date'].notna()]
    df = df.drop_duplicates(subset='date', keep='last')
    # vol 单位=手 → 股（与 fetch_tdx_daily 同口径）
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce') * 100.0
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    _n_before = len(df)
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    if len(df) < _n_before:
        logger.warning("[%s] TDX 指数返回 %d 行 OHLC 残缺，已过滤（下轮增量自愈）", code, _n_before - len(df))
    if df.empty:
        return None
    return df[INDEX_COLS]


def _merge_index_write(fp, code, df, overwrite):
    """指数落盘统一合并（对齐 K 线 _merge_write）：overwrite=True 直接替换整文件；
    False 列级合并（新数据非 NaN 优先，可修复中间缺列，非仅追加新日期）。"""
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    if overwrite or not fp.exists():
        _tmp = fp.with_suffix(".parquet.tmp")
        df.to_parquet(_tmp, index=False)
        atomic_replace(_tmp, fp)
        return True
    try:
        old = pd.read_parquet(fp)
        if old.empty or "date" not in old.columns:
            raise ValueError("bad existing")
    except Exception as e:
        logger.warning("[%s] 存量读取失败(%s)，直接替换", code, str(e)[:80])
        _tmp = fp.with_suffix(".parquet.tmp")
        df.to_parquet(_tmp, index=False)
        atomic_replace(_tmp, fp)
        return True
    new = df.copy()
    new["date"] = pd.to_datetime(new["date"])
    old["date"] = pd.to_datetime(old["date"])
    merged = (new.set_index("date").combine_first(old.set_index("date"))
              .reset_index().drop_duplicates("date").sort_values("date"))
    for _c in INDEX_COLS[1:]:
        if _c in merged.columns:
            merged[_c] = pd.to_numeric(merged[_c], errors="coerce")
    _tmp = fp.with_suffix(".parquet.tmp")
    merged.to_parquet(_tmp, index=False)
    atomic_replace(_tmp, fp)
    return True


def update_index(code, count: int = 600, client=None, overwrite=False):
    """TDX 源指数落盘（独立流程）：拉取指数日K，落盘 INDEX_DIR/{code}.parquet。

    落盘列为统一指数契约 INDEX_COLS（volume 已 ×100 归股），兼容下游 date/close 读取。
    count 取最近 count 个交易日（增量场景编排层恒传 1 只拉当日）；
    overwrite=True 直接替换，False 列级合并（新数据非 NaN 优先，对齐 K 线 _merge_write）。
    """
    fp = INDEX_DIR / f"{code}.parquet"
    try:
        df = fetch_index_daily(code, count=count, client=client)
        if df is None or df.empty:
            logger.warning("[%s] TDX 指数 no data", code)
            return False
        return _merge_index_write(fp, code, df, overwrite)
    except Exception as e:
        logger.error("[%s] TDX 指数 failed: %s", code, str(e)[:60])
        return False
