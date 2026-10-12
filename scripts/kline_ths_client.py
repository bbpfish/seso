#!/usr/bin/env python3
"""K-line THS data client - THS 数据获取客户端层

数据契约（与 run_kline_multi.py 字段契约一致）:
  date, open, high, low, close, volume, adj_close, hfq_close, turnover
  - 采集模式 adjust（对外三值，内部映射 thsdk 合法值）:
      ""=standard 不复权原价：close=原始收盘，adj_close/hfq_close 写 NaN
      "qfq"=forward 前复权：adj_close=THS 返回"收盘价"列，close 置 NaN 标识非原始价
      "hfq"=backward 后复权：hfq_close=THS 返回"收盘价"列，adj_close/close 置 NaN
  - volume 单位=股，turnover 由 market_data_cn 实时换手率直通填充（单位=百分比数值）

职责边界:
  - 仅封装 THS 数据请求：代码映射(get_ths_code) + 入口过滤(filter_ths_uncollectable) + 单只拉取(fetch_ths_daily)
  - 过滤原则：只拦截 THS 真实无法采集的代码（thsdk 无市场代号段），
    北交所/B股/基金/可转债等 THS 可采集段一律放行，是否采集由调度层决策；
    客户端只做"跳过已存在数据(幂等去重)、采集能采集的数据、正确保存数据(9列契约)"
  - 代码段/市场识别逻辑统一收敛到公共模块 stock_code_utils.py（供 sina/tx/tdx 复用）

"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

import stock_code_utils
from kline_env import INDEX_DIR  # noqa: E402
from io_atomic import atomic_replace  # noqa: E402

# 按交易日历精确换算拉取根数（不再使用固定全量/增量根数常量）：
#   全量 count = trade_days_between(起点, 今日)；增量 count = trade_days_between(存量末尾, 今日)
#   单次请求根数上限：TDX 实测 800（900 起返回 0）。
#   [UNVERIFIED] THS 单次请求根数上限未实测（thsdk 1.7.18 源码无校验），按同一上限分段保险；
#   正式环境需实测（900/1000 是否返回 0）后更新本常量。
MAX_BARS_PER_REQ = 800

# 对外采集模式 -> thsdk 合法 adjust 值（公共模块统一定义，thsdk 仅接受 ""/forward/backward）
ADJUST_MAP = stock_code_utils.ADJUST_MAP

logger = logging.getLogger('kline_ths_client')


class THSConnectionError(Exception):
    """THS 会话失效（未登录/断开/超时/socket 错误等），需上层重建会话后重试。"""


# 会话失效特征串（thsdk Response.error / 异常文本命中即判定连接失效）
_CONN_ERROR_MARKERS = (
    "未登录", "登录已失效", "未连接", "连接已断开", "socket", "Socket",
    "broken pipe", "BrokenPipe", "connection reset", "ConnectionReset",
    "timed out", "Timeout", "超时", "connection refused", "ConnectionRefused",
)


def is_connection_error(msg: str) -> bool:
    """命中 THS 会话失效特征则返回 True（供上层判定是否需要重建会话）。"""
    if not msg:
        return False
    return any(m in msg for m in _CONN_ERROR_MARKERS)


def _request_klines(ths, *args, **kwargs):
    """封装 ths.klines：异常或 error Response 命中连接失效特征时抛 THSConnectionError。

    正常业务失败（无数据/缺列等）不抛异常，保持原返回语义；只有会话失效
    （未登录/断开/超时/socket/broken pipe）才抛连接错误，供调度层重建会话重试。"""
    try:
        resp = ths.klines(*args, **kwargs)
    except Exception as e:
        err = str(e)
        if is_connection_error(err):
            raise THSConnectionError(err[:200])
        raise
    if resp is not None and not getattr(resp, "success", True):
        err = str(getattr(resp, "error", "") or "")
        if is_connection_error(err):
            raise THSConnectionError(err[:200])
    return resp


# market_data_cn 汇总 query_key 中换手率字段的编号（thsdk 常量表 FieldNameMap）
#   1968584=换手率（单位=百分比数值，如 0.2846 表示 0.2846%）；407=流通股本（单位=股）
TURNOVER_QUERY_KEY = "汇总"


def _safe_float_pct(v):
    """换手率占位符安全转换：'-'/''/None/非数值 -> NaN，数值/数字字符串 -> float。
    THS 停牌快照换手率可能返回 '-'/'--'，float() 强转会抛 ValueError
    （旧实现导致整只股票拉取失败、损失整日 OHLC）。"""
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


def _ths_window_start(end_dt, max_bars, floor_dt):
    """取 end_dt 之前第 max_bars-1 个交易日作为补段窗口起点（不早于 floor_dt）。

    用交易日历精确约束补段窗口：[win_start, end_dt] 闭区间含 max_bars 个交易日，
    消除"不传 count 时单次跨度可达数年"的分段失控；日历不可用时退回 floor_dt
    （窗口不约束，行为与旧实现一致）。"""
    try:
        from trade_calendar import load_trading_calendar
        cal = load_trading_calendar()
        if cal is None or len(cal) == 0:
            return floor_dt
        ts = pd.Timestamp(end_dt).normalize()
        arr = cal[cal <= ts]
        if len(arr) <= max_bars:
            return floor_dt
        start_ts = arr[-max_bars]  # end_dt 前第 max_bars-1 个交易日（区间含 max_bars 根）
        return max(start_ts.to_pydatetime(), floor_dt)
    except Exception:
        return floor_dt


def fetch_ths_turnover_batch(ths, codes, query_key: str = TURNOVER_QUERY_KEY) -> dict:
    """批量获取 THS 实时换手率，返回 {6位代码: 换手率百分比数值}。

    利用 market_data_cn 批量能力（同市场一次请求多只，官方限频 20ms/次）：
      - query_key="汇总" 返回快照行（含 换手率/流通市值/名称）+ 股本行（含 总股本/流通股本）
      - 换手率字段单位=百分比数值（如 0.2846 = 0.2846%），实测验证：601398 成交量338703260股 /
        流通股本269612212539股 ×100 = 0.1256，与接口直通值一致
    失败/缺值单股跳过（不伪造，由调用方决定 NaN 兜底）；整个市场组失败记 warning。
    用法：
      - 批量场景：上层按市场分组各调用一次本函数（每市场一次），将返回值作为
        turnover_map 传入 fetch_ths_daily，避免逐只各打一次接口；
      - 单只备用：传单只代码列表（[code]）仍可用，作为 fetch_ths_daily 未传
        turnover_map 时的默认兜底路径。
    """
    result: dict = {}
    if not codes:
        return result
    # market_data_cn 要求同市场一次查询，按市场代号分组
    groups: dict = {}
    for c in codes:
        tc = get_ths_code(c)
        if tc is None or filter_ths_uncollectable(c):
            continue
        groups.setdefault(tc[:4], []).append(tc)
    for market, tc_list in groups.items():
        try:
            resp = ths.market_data_cn(tc_list, query_key=query_key)
            if not resp or not resp.success or not resp.data:
                logger.warning("[ths-tur] market_data_cn(%s) 失败：%s",
                               market, getattr(resp, 'error', '')[:120])
                continue
            for item in resp.data:
                if isinstance(item, dict) and item.get('代码') and '换手率' in item:
                    tc = item.get('代码')
                    code6 = tc[4:] if isinstance(tc, str) and len(tc) == 10 else str(tc)
                    result[code6] = _safe_float_pct(item['换手率'])
        except Exception as e:
            logger.warning("[ths-tur] market_data_cn(%s) 异常：%s", market, str(e)[:120])
    return result


def filter_ths_uncollectable(code: str) -> bool:
    """返回 True 表示 THS 真实无法采集（公共模块 stock_code_utils.is_ths_uncollectable）。

    仅拦截 thsdk 无市场代号、无法构造合法 ths_code 的段：
      非6位数字、43 新三板、400/420 老三板。
    北交所(8/92)、B股(900/200)、基金(5/15/16)、可转债(110/113/118/12x/13x)
    均为 THS 可采集市场，放行由调度层决策。"""
    return stock_code_utils.is_ths_uncollectable(code)


def get_ths_code(code: str) -> Optional[str]:
    """6位代码 -> THSCODE（4位市场代号+6位数字，公共模块 stock_code_utils.get_ths_code）。

    映射覆盖 THS 全部可采集市场（FIXED_LENGTH_SECURITY_MARKETS）：
      sh_a->USHA / sz_a->USZA / sh_b->USHB / sz_b->USZB / sh_fund->USHJ
      sz_fund->USZJ / sh_bond->USHD / sz_bond->USZD / bse(8/92)->USTM
    返回 None 仅表示 thsdk 无对应市场代号（新三板43/老三板400,420/非6位），真实无法采集。"""
    return stock_code_utils.get_ths_code(code)


def fetch_ths_daily(ths, code: str, count: int, start_date=None,
                    adjust: str = "",
                    turnover_map: Optional[dict] = None,
                    with_turnover: bool = True) -> Optional[pd.DataFrame]:
    """拉取 THS 日K，返回 9 列标准 Dataframe 或 None。

    采集模式 adjust（对外三值，内部映射 thsdk 合法值，见 ADJUST_MAP）:
      - ""  (standard): 不复权原价，close=原始收盘，adj_close/hfq_close=NaN
      - "qfq" (forward): 前复权，adj_close=THS 返回"收盘价"列，open/high/low/volume 保留
                         （同请求复权口径），close 置 NaN 标识非原始价
      - "hfq" (backward): 后复权，hfq_close=THS 返回"收盘价"列，adj_close/close 置 NaN
    turnover：market_data_cn 实时换手率直通填充（单位=百分比数值），三模式统一填；
    批量接口失败/单股缺值回退 NaN+warning（不伪造）。
    turnover_map：可选预取换手率映射 {6位代码: 换手率百分比数值}（与
      fetch_ths_turnover_batch 返回结构一致）。批量场景上层先按市场分组各调用一次
      fetch_ths_turnover_batch（每市场一次），把结果传入本参数实现"每市场一次"而非
      "每只一次"；传入后本函数仅查表不再打接口，未命中回退 NaN+warning（不伪造）。
      未传入（None，默认）：单只路径，内部调用 fetch_ths_turnover_batch(ths, [code])
      取实时换手率；批量场景请优先预取传入本参数，避免逐只各打一次接口。
    with_turnover：默认 True；False 时 turnover 列直接置 NaN，不触发
      fetch_ths_turnover_batch 请求、不产生 warning（full 阶段一 standard/qfq/hfq 使用）。
    非法 adjust 值返回 None+warning；返回列缺失时补 NaN+warning 而非 KeyError。
    count>MAX_BARS_PER_REQ 时循环分段：每次取窗口内最近 MAX 根、end_time 前移，
    直到覆盖 count 或返回为空（消除早期段静默截断）。"""
    sdk_adjust = ADJUST_MAP.get(adjust)
    if sdk_adjust is None:
        logger.warning("[%s] 非法 adjust=%r（仅支持 %s），返回 None", code, adjust,
                       sorted(ADJUST_MAP.keys()))
        return None
    tc = get_ths_code(code)
    if tc is None or filter_ths_uncollectable(code):
        # get_ths_code None ⟺ is_ths_uncollectable True（代码集重合，warning 合并后可达）
        logger.warning("[%s] 入口过滤：THS 无法采集（无市场代号或不可采集段：非6位/新三板43/老三板400,420），返回 None", code)
        return None
    if count <= MAX_BARS_PER_REQ:
        resp = _request_klines(ths, tc, interval="day", count=count, adjust=sdk_adjust)
        if not resp or resp.df is None or resp.df.empty:
            return None
        df = resp.df.copy()
    else:
        # THS 全量超单次上限：thsdk 的 count 与 start_time/end_time 互斥，
        # 因此分段 = 第一段 count 取最近 MAX 根 + 循环窗口补早期段。
        if start_date is None:
            logger.warning("[%s] THS 分段补早段需要 --start-date 指定起点（count=%d>%d），返回 None",
                           code, count, MAX_BARS_PER_REQ)
            return None
        start_dt = pd.Timestamp(start_date).to_pydatetime()
        resp = _request_klines(ths, tc, interval="day", count=MAX_BARS_PER_REQ, adjust=sdk_adjust)
        if not resp or resp.df is None or resp.df.empty:
            return None
        df = resp.df.copy()
        df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
        df = df.sort_values('时间')
        # 循环补早期段：每次取窗口内最近 MAX 根（end_time 前移），直到覆盖 count 或返回为空
        while len(df) < count:
            d_min = df['时间'].min()
            end_dt = (d_min - pd.Timedelta(days=1)).to_pydatetime()
            if end_dt < start_dt:  # 已越过起点，无需再补
                break
            # 补段窗口按 MAX_BARS_PER_REQ 约束（不传 count 时单次跨度可达数年，
            # 与分段保险初衷矛盾）
            win_start = _ths_window_start(end_dt, MAX_BARS_PER_REQ, start_dt)
            resp2 = _request_klines(ths, tc, start_time=win_start, end_time=end_dt,
                                    interval="day", adjust=sdk_adjust)
            if not resp2 or resp2.df is None or resp2.df.empty:
                break  # 返回为空：起点之前已无更多数据
            df2 = resp2.df.copy()
            df2['时间'] = pd.to_datetime(df2['时间'], errors='coerce')
            df2 = df2.sort_values('时间')
            n_before = len(df)
            df = pd.concat([df2, df], ignore_index=True)
            df = df.drop_duplicates(subset='时间', keep='last')
            if len(df) <= n_before:  # 本轮无新增行，防死循环
                break
    # 统一列名（缺列补 NaN+warning 而非 KeyError）；'时间' 统一转换一次
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    rename_map = {'时间': 'date', '开盘价': 'open', '最高价': 'high',
                  '最低价': 'low', '收盘价': 'close', '成交量': 'volume'}
    missing_cols = [cn for cn in rename_map if cn not in df.columns]
    if missing_cols:
        logger.warning("[%s] THS 返回缺列 %s，已补 NaN（数据可能不完整）", code, missing_cols)
        for cn in missing_cols:
            df[cn] = np.nan
    df = df.rename(columns=rename_map)
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].sort_values('date')
    # NaT 日期行过滤（to_datetime coerce 产生；OHLC dropna 不含 date 列，NaT 行会漏网）
    if df['date'].isna().any():
        logger.warning("[%s] THS 返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
        df = df[df['date'].notna()]
    # 幂等：同一日期重复行保留最后一条（分段循环/多次拉取不会产生重复行，
    # 配合 worker 增量 count 实现"跳过已存在数据"）
    df = df.drop_duplicates(subset='date', keep='last')
    # 三模式 9 列契约
    if sdk_adjust == 'forward':  # qfq
        df['adj_close'] = df['close']
        df['close'] = np.nan  # 非原始价
        df['hfq_close'] = np.nan
    elif sdk_adjust == 'backward':  # hfq
        df['hfq_close'] = df['close']
        df['close'] = np.nan  # 非原始价
        df['adj_close'] = np.nan
    else:  # standard
        df['adj_close'] = np.nan
        df['hfq_close'] = np.nan
    # turnover：market_data_cn 实时换手率直通（单位=百分比数值）。
    # 换手率为接口直通字段，不受复权影响，三模式（standard/qfq/hfq）统一填充；
    # 批量接口失败/单股缺值回退 NaN+warning（不伪造）。
    # turnover 取值路径：
    #   1) turnover_map 预取（批量场景）：上层已按市场分组批量调用
    #      fetch_ths_turnover_batch（每市场一次），此处仅查表不打接口；
    #      未命中回退 NaN+warning（不伪造）。
    #   2) turnover_map=None（默认/单只备用）：内部调用 fetch_ths_turnover_batch(ths, [code])
    #      单只获取实时换手率。
    #   3) with_turnover=False（full 阶段一）：turnover 列直接置 NaN，零请求零 warning。
    if not with_turnover:
        df['turnover'] = np.nan
    elif turnover_map is not None:
        _tv = turnover_map.get(code)
        if _tv is None:
            logger.warning("[%s] turnover_map 未命中 %s，turnover 置 NaN（调用方未预取该代码）",
                           code, code)
            df['turnover'] = np.nan
        else:
            df['turnover'] = _safe_float_pct(_tv)
    else:
        _tur = fetch_ths_turnover_batch(ths, [code])
        _tv = _tur.get(code)
        if _tv is None:
            logger.warning("[%s] THS market_data_cn 未返回换手率，turnover 置 NaN", code)
            df['turnover'] = np.nan
        else:
            df['turnover'] = _safe_float_pct(_tv)
    df = df[['date', 'open', 'high', 'low', 'close', 'volume',
             'adj_close', 'hfq_close', 'turnover']]
    _n_before = len(df)
    if sdk_adjust == 'forward':
        df = df.dropna(subset=['open', 'high', 'low', 'adj_close'])
    elif sdk_adjust == 'backward':
        df = df.dropna(subset=['open', 'high', 'low', 'hfq_close'])
    else:
        df = df.dropna(subset=['open', 'high', 'low', 'close'])
    if len(df) < _n_before:
        logger.warning("[%s] THS 返回含 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                       code, _n_before - len(df))
    if df.empty:
        return None
    return df


INDEX_COLS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def fetch_index_daily(ths, code: str, count: int = 15,
                      start_date=None, end_date=None,
                      turnover_map: Optional[dict] = None,
                      with_turnover: bool = True) -> Optional[pd.DataFrame]:
    """拉取 THS 指数日K，返回统一指数契约 DataFrame（INDEX_COLS）或 None。

    契约（INDEX_COLS）：date/open/high/low/close/volume/amount。
    - ths_code 用 get_index_ths_code 映射真实代码（000 沪市->USHI1A/1B+去前2位，
      如 000001->USHI1A0001、000300->USHI1B0300；399 深市->USZI+6位）；非 000/399 段返回 None
    - volume 单位=股（与股票口径一致）；amount 为 THS 返回"总金额"列（单位=元），
      缺列补 NaN+warning 而非 KeyError
    - 指数无复权/换手率概念：不做复权处理、不填 turnover；with_turnover/turnover_map
      参数仅为对齐 fetch_ths_daily 签名保留，传 True 也不产生接口请求（指数不走
      market_data_cn 换手率链路）
    - start_date/end_date 均传时走窗口请求（count 与 start/end 互斥，thsdk 约束）；
      否则按 count 拉最近 N 根。count>MAX_BARS_PER_REQ 时复用 fetch_ths_daily 分段逻辑
    """
    tc = stock_code_utils.get_index_ths_code(code)
    if tc is None:
        logger.warning("[%s] 非指数代码段（000/399），THS 指数返回 None", code)
        return None
    if count <= MAX_BARS_PER_REQ and not (start_date and end_date):
        if start_date and end_date:
            pass  # 互斥分支：走下方窗口（理论上不可达，防御性保留）
        resp = _request_klines(ths, tc, interval="day", count=count)
        if not resp or resp.df is None or resp.df.empty:
            return None
        df = resp.df.copy()
    else:
        if not (start_date and end_date):
            logger.warning("[%s] THS 指数分段补早段需要 start_date+end_date 成对（count=%d>%d），返回 None",
                           code, count, MAX_BARS_PER_REQ)
            return None
        start_dt = pd.Timestamp(start_date).to_pydatetime()
        end_dt = pd.Timestamp(end_date).to_pydatetime()
        resp = _request_klines(ths, tc, start_time=start_dt, end_time=end_dt,
                               interval="day")
        if not resp or resp.df is None or resp.df.empty:
            return None
        df = resp.df.copy()
        df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
        df = df.sort_values('时间')
        # 循环补早段（与 fetch_ths_daily 同款窗口约束），覆盖 count 或返回为空
        while len(df) < count:
            d_min = df['时间'].min()
            end_dt2 = (d_min - pd.Timedelta(days=1)).to_pydatetime()
            if end_dt2 < start_dt:
                break
            win_start = _ths_window_start(end_dt2, MAX_BARS_PER_REQ, start_dt)
            resp2 = _request_klines(ths, tc, start_time=win_start, end_time=end_dt2,
                                    interval="day")
            if not resp2 or resp2.df is None or resp2.df.empty:
                break
            df2 = resp2.df.copy()
            df2['时间'] = pd.to_datetime(df2['时间'], errors='coerce')
            df2 = df2.sort_values('时间')
            n_before = len(df)
            df = pd.concat([df2, df], ignore_index=True)
            df = df.drop_duplicates(subset='时间', keep='last')
            if len(df) <= n_before:
                break
    # 统一列名（缺列补 NaN+warning 而非 KeyError）
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    rename_map = {'时间': 'date', '开盘价': 'open', '最高价': 'high',
                  '最低价': 'low', '收盘价': 'close', '成交量': 'volume',
                  '总金额': 'amount'}
    missing_cols = [cn for cn in rename_map if cn not in df.columns]
    if missing_cols:
        logger.warning("[%s] THS 指数返回缺列 %s，已补 NaN（数据可能不完整）", code, missing_cols)
        for cn in missing_cols:
            df[cn] = np.nan
    df = df.rename(columns=rename_map)
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']].sort_values('date')
    if df['date'].isna().any():
        logger.warning("[%s] THS 指数返回含 %d 行 NaT 日期，已过滤", code, int(df['date'].isna().sum()))
        df = df[df['date'].notna()]
    df = df.drop_duplicates(subset='date', keep='last')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    _n_before = len(df)
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    if len(df) < _n_before:
        logger.warning("[%s] THS 指数返回含 %d 行 OHLC 残缺，已过滤（下轮增量自愈）",
                       code, _n_before - len(df))
    if df.empty:
        return None
    return df[INDEX_COLS]


def update_index(ths, code: str, count: int = 600, start_date=None, end_date=None):
    """THS 源指数落盘（独立流程）：拉取指数日K，增量合并写 INDEX_DIR/{code}.parquet。

    落盘列为统一指数契约 INDEX_COLS（volume 单位=股），兼容下游 date/close 读取。
    count 取最近 count 个交易日（默认 600 覆盖日常增量）；已有本地文件则只增量合并新日期。
    """
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
        df = fetch_index_daily(ths, code, count=count, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            logger.warning("[%s] THS 指数 no data", code)
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
        logger.error("[%s] THS 指数 failed: %s", code, str(e)[:60])
        return False
