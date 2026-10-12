#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_calendar.py — SESO 统一交易日历模块
=================================================
将 exchange_calendars 的 XSHG 日历接入 SESO，作为统一交易日历来源，
解决节假日休市被误判为数据缺口、prev 交易日前移错位等问题。

对外接口：
  load_trading_calendar()                    -> pd.DatetimeIndex | None（交易日序列，升序）
  is_trading_day(date)                       -> bool | None（None=超出覆盖或日历不可用，无后备）
  prev_trading_day(date)                     -> date（date 之前最近交易日，不含当天）
  next_trading_day(date)                     -> date（date 之后最近交易日，不含当天）
  expected_latest_trading_day(date=None)     -> date（预期最近已完成交易日，默认今天，
                                                 用于新鲜度检查）
  trading_days(start, end)                   -> list[str]（闭区间交易日 ISO 序列；日历不可用返回 []）
  trade_days_between(start, end)             -> int（(start, end] 交易日计数；日历不可用返回 0）
  cn_today()                                 -> date（Asia/Shanghai 口径的"今天"）

实现策略：
  1) exchange_calendars XSHG 日历（实测覆盖 2006-10-09 ~ 2026-12-31）；
  2) 超出覆盖范围或日历不可用时返回 None / [] / 0（不设近似后备，调用方自行处理）。

时区约束：交易日历口径的"今天"统一取 Asia/Shanghai（cn_today），防 UTC
容器（默认 TZ=UTC）下新鲜度检查与采集端末日在北京时间 0:00~8:00
之间偏移一天；zoneinfo/tzdata 不可用时退回本地时区（部署时建议设
TZ=Asia/Shanghai 双保险）。

注意：exchange_calendars 的 previous_session / next_session 入参必须是
交易日，非交易日需先 date_to_session(direction='next'/'previous') 转换。
"""
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# 保证本模块可独立运行自检（SESO 脚本目录在 sys.path 中）
_SESO_DIR = Path(__file__).resolve().parent
if str(_SESO_DIR) not in sys.path:
    sys.path.insert(0, str(_SESO_DIR))

log = logging.getLogger(__name__)

# 时区：Asia/Shanghai（模块级单例，避免每次调用 cn_today 重复 import）
try:
    from zoneinfo import ZoneInfo
    _CN_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _CN_TZ = None

_XSHG = None
_XSHG_LOADED = False
_CAL_CACHE = None     # pd.DatetimeIndex | None（XSHG 交易日序列缓存）


def _normalize(d) -> date:
    """入参归一化为 date 对象（支持 date/datetime/Timestamp/YYYY-MM-DD 字符串）。"""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        return date.fromisoformat(d)
    if isinstance(d, date):
        return d
    try:
        return date.fromisoformat(str(d))
    except (ValueError, TypeError):
        try:
            ts = pd.Timestamp(d)
            if ts is pd.NaT:
                raise ValueError(f"无法归一化日期: {d!r}")
            return ts.date()
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError(f"无法归一化日期: {d!r}") from exc


def cn_today() -> date:
    """Asia/Shanghai 口径的"今天"（交易日历/新鲜度检查/采集端统一使用）。

    防 UTC 容器（默认 TZ=UTC）下 date.today() 在北京时间 0:00~8:00
    偏移一天；zoneinfo/tzdata 不可用时退回本地时区（部署时应设 TZ=Asia/Shanghai）。
    """
    if _CN_TZ is not None:
        return datetime.now(_CN_TZ).date()
    log.warning("[trade_calendar] zoneinfo/tzdata 不可用，cn_today 退回本地时区")
    return date.today()


def _xshg_calendar():
    """加载 exchange_calendars XSHG 日历（惰性、单例）；失败 warning 留痕并返回 None。"""
    global _XSHG, _XSHG_LOADED
    if _XSHG_LOADED:
        return _XSHG
    _XSHG_LOADED = True
    try:
        from exchange_calendars import get_calendar
        _XSHG = get_calendar("XSHG")
        log.info("[trade_calendar] XSHG 日历可用")
    except Exception as e:
        _XSHG = None
        log.warning("[trade_calendar] XSHG 日历加载失败，交易日历不可用（返回 None，无后备）: %s", e)
    return _XSHG


def load_trading_calendar():
    """返回升序交易日序列 pd.DatetimeIndex；不可用时返回 None（调用方回退）。

    结果缓存于 _CAL_CACHE，首次加载后不再重复重建（P1 优化）。
    """
    global _CAL_CACHE
    if _CAL_CACHE is not None:
        return _CAL_CACHE
    cal = _xshg_calendar()
    if cal is not None:
        try:
            _CAL_CACHE = pd.DatetimeIndex(cal.sessions)
            return _CAL_CACHE
        except Exception as e:
            log.warning("[trade_calendar] XSHG sessions 提取失败: %s", e)
    return None


def is_trading_day(d) -> Optional[bool]:
    """d 是否为交易日。XSHG 覆盖范围内精确判断；超出覆盖范围或日历不可用时
    返回 None（无后备，调用方自行处理）。"""
    d = _normalize(d)
    cal = _xshg_calendar()
    if cal is not None and cal.first_session.date() <= d <= cal.last_session.date():
        try:
            return bool(cal.is_session(pd.Timestamp(d)))
        except Exception as exc:
            log.warning("[trade_calendar] is_trading_day XSHG 查询失败，返回 None（无后备）: %s", exc)
            return None
    log.warning("[trade_calendar] is_trading_day 超出 XSHG 覆盖或日历不可用，返回 None（无后备）")
    return None


def prev_trading_day(d) -> date:
    """d 之前最近交易日（不含 d）。节假日精确；超出覆盖范围或日历不可用时返回 None。"""
    d = _normalize(d)
    cal = _xshg_calendar()
    if cal is not None and cal.first_session.date() <= d <= cal.last_session.date():
        try:
            # previous_session 入参必须是交易日：非交易日先转下一个交易日
            s = cal.date_to_session(pd.Timestamp(d), direction="next")
            return cal.previous_session(s).date()
        except Exception as exc:
            log.warning("[trade_calendar] prev_trading_day XSHG 查询失败，返回 None（无后备）: %s", exc)
    log.warning("[trade_calendar] prev_trading_day 超出 XSHG 覆盖或日历不可用，返回 None（无后备）")
    return None


def next_trading_day(d) -> date:
    """d 之后最近交易日（不含 d）。节假日精确；超出覆盖范围或日历不可用时返回 None。"""
    d = _normalize(d)
    cal = _xshg_calendar()
    if cal is not None and cal.first_session.date() <= d <= cal.last_session.date():
        try:
            s = cal.date_to_session(pd.Timestamp(d), direction="next")
            if s.date() != d:      # d 非交易日：s 即下一个交易日
                return s.date()
            nxt = cal.next_session(s)  # d 是交易日：取严格下一个
            return nxt.date()
        except Exception as exc:
            log.warning("[trade_calendar] next_trading_day XSHG 查询失败，返回 None（无后备）: %s", exc)
    log.warning("[trade_calendar] next_trading_day 超出 XSHG 覆盖或日历不可用，返回 None（无后备）")
    return None


def trading_days(start, end):
    """返回 [start, end] 闭区间内的交易日序列（ISO 字符串，升序）。

    用于采集端批量排程（如逐日快照区间）。交易日历不可用时返回 []（无后备，
    调用方按不排程处理）。"""
    start = _normalize(start)
    end = _normalize(end)
    if end < start:
        return []
    cal = load_trading_calendar()
    if cal is not None:
        try:
            s, e = pd.Timestamp(start), pd.Timestamp(end)
            lo = cal.searchsorted(s, side="left")
            hi = cal.searchsorted(e, side="right")
            return [d.date().isoformat() for d in cal[lo:hi]]
        except Exception as exc:
            log.warning("[trade_calendar] trading_days XSHG 查询失败，返回 []（无后备）: %s", exc)
    log.warning("[trade_calendar] 无可用日历，trading_days 返回 []（无后备）%s~%s", start, end)
    return []


def trade_days_between(start, end):
    """统计 (start, end] 区间内的交易日数量（含 end 若为交易日，不含 start）。
    交易日历不可用时返回 0（无后备，调用方自行保守处理）。"""
    start = _normalize(start)
    end = _normalize(end)
    if end <= start:
        return 0
    cal = load_trading_calendar()
    if cal is not None:
        try:
            s, e = pd.Timestamp(start), pd.Timestamp(end)
            return int(cal.searchsorted(e, side="right") - cal.searchsorted(s, side="right"))
        except Exception as exc:
            log.warning("[trade_calendar] trade_days_between XSHG 查询失败，返回 0（无后备）: %s", exc)
    log.warning("[trade_calendar] 无可用日历，trade_days_between 返回 0（无后备）")
    return 0


def expected_latest_trading_day(d=None) -> date:
    """预期最近已完成交易日：d（默认 cn_today，Asia/Shanghai 口径）之前最近交易日
    （不含 d 当天），用于新鲜度检查（节假日正确）。"""
    d = cn_today() if d is None else _normalize(d)
    return prev_trading_day(d)


def _selfcheck() -> int:
    """独立自检：验证 2026 中秋/国庆休市、关键交易日与 prev 前移、时区口径。"""
    failures = []

    def check(name, ok, extra=""):
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f" ({extra})" if extra else ""))
        if not ok:
            failures.append(name)

    print("== trade_calendar selfcheck ==")
    cal = _xshg_calendar()
    if cal is None:
        print("  [WARN] XSHG 日历不可用，交易日相关断言将按无后备处理（可能 FAIL）")
    else:
        print(f"  XSHG 覆盖: {cal.first_session.date()} ~ {cal.last_session.date()}")

    # cn_today 时区口径
    ct = cn_today()
    check("cn_today 返回 date", isinstance(ct, date), str(ct))

    # 2026-09-25 中秋
    check("2026-09-25 中秋为非交易日", is_trading_day("2026-09-25") is False)
    # 2026-10-01 ~ 10-07 国庆休市
    for day in range(1, 8):
        check(f"2026-10-{day:02d} 国庆为非交易日", is_trading_day(f"2026-10-{day:02d}") is False)
    check("2026-09-30 为交易日", is_trading_day("2026-09-30") is True)
    check("2026-10-08 为交易日", is_trading_day("2026-10-08") is True)
    check("prev_trading_day(2026-10-08)=2026-09-30",
          prev_trading_day("2026-10-08") == date(2026, 9, 30),
          str(prev_trading_day("2026-10-08")))
    check("prev_trading_day(2026-09-30)=2026-09-29",
          prev_trading_day("2026-09-30") == date(2026, 9, 29),
          str(prev_trading_day("2026-09-30")))
    check("next_trading_day(2026-09-30)=2026-10-08",
          next_trading_day("2026-09-30") == date(2026, 10, 8),
          str(next_trading_day("2026-09-30")))
    check("next_trading_day(2026-10-08)=2026-10-09",
          next_trading_day("2026-10-08") == date(2026, 10, 9),
          str(next_trading_day("2026-10-08")))
    check("expected_latest_trading_day(2026-10-08)=2026-09-30",
          expected_latest_trading_day("2026-10-08") == date(2026, 9, 30))
    # --- P3 增补：trading_days 闭区间边界 ---
    td = trading_days("2026-10-08", "2026-10-08")
    check("trading_days 单日闭区间(交易日)=['2026-10-08']", td == ["2026-10-08"], str(td))
    td = trading_days("2026-10-08", "2026-10-09")
    check("trading_days 闭区间含首尾=2天", td == ["2026-10-08", "2026-10-09"], str(td))
    td = trading_days("2026-10-03", "2026-10-06")
    check("trading_days 全休市区间=[]", td == [], str(td))
    td = trading_days("2026-10-09", "2026-10-08")
    check("trading_days end<start=[]", td == [], str(td))

    # --- P3 增补：trade_days_between 半开区间 (start, end] ---
    n = trade_days_between("2026-09-30", "2026-10-09")
    check("trade_days_between(0930,1009]=2", n == 2, str(n))
    n = trade_days_between("2026-10-08", "2026-10-08")
    check("trade_days_between 同点=0", n == 0, str(n))
    n = trade_days_between("2026-10-08", "2026-10-09")
    check("trade_days_between(1008,1009]=1", n == 1, str(n))

    # --- P3 增补：归一化 ---
    check("_normalize 字符串", _normalize("2026-10-08") == date(2026, 10, 8))
    check("_normalize datetime", _normalize(datetime(2026, 10, 8, 15, 30)) == date(2026, 10, 8))
    check("is_trading_day(datetime) 归一化", is_trading_day(datetime(2026, 10, 8, 15, 30)) is True)

    # --- P3 增补：首尾边界不抛异常 ---
    cal2 = load_trading_calendar()
    if cal2 is not None and len(cal2) > 0:
        p = prev_trading_day(cal2[0].date())
        check("首日 prev_trading_day 超界返回 None", p is None, str(p))
        nx = next_trading_day(cal2[-1].date())
        check("末日 next_trading_day 超界返回 None", nx is None, str(nx))
    else:
        print("  [WARN] 无可用交易日历，跳过首尾边界测试")

    print(f"== selfcheck {'全部通过' if not failures else '失败 ' + str(len(failures)) + ' 项'} ==")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(_selfcheck())
