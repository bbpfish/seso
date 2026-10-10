#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_calendar.py — SESO 统一交易日历模块
=================================================
将 exchange_calendars 的 XSHG 日历接入 SESO，替换"指数 K 线反推交易日"的
零散实现，解决节假日休市被误判为数据缺口、prev 交易日前移错位等问题。

对外接口：
  load_trading_calendar()                    -> pd.DatetimeIndex | None（交易日序列，升序）
  is_trading_day(date)                       -> bool
  prev_trading_day(date)                     -> date（date 之前最近交易日，不含当天）
  next_trading_day(date)                     -> date（date 之后最近交易日，不含当天）
  expected_latest_trading_day(date=None)     -> date（预期最近已完成交易日，默认今天，
                                                 用于新鲜度检查）
  cn_today()                                 -> date（Asia/Shanghai 口径的"今天"）

实现策略（逐级 fallback，向后兼容；每级降级均有 log.warning 留痕）：
  1) exchange_calendars XSHG 日历（实测覆盖 2006-10-09 ~ 2026-12-31，
     超出覆盖范围或 import 失败时自动降级）；
  2) fallback1：SESO 现有指数 parquet（000001.parquet / 000300.parquet
     date 列）提取交易日序列（容器路径 /home/jovyan/data/index 与本地
     <SESO>/data/index 均可）；
  3) fallback2：周末近似（周一回上周五、周末回上周五、其余回昨天）。

时区约束：交易日历口径的"今天"统一取 Asia/Shanghai（cn_today），防 UTC
容器（jovyan 默认 TZ=UTC）下新鲜度检查与采集端末日在北京时间 0:00~8:00
之间偏移一天；zoneinfo/tzdata 不可用时退回本地时区（部署时建议设
TZ=Asia/Shanghai 双保险）。

注意：exchange_calendars 的 previous_session / next_session 入参必须是
交易日，非交易日需先 date_to_session(direction='next'/'previous') 转换。
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# 保证本模块可独立运行自检（SESO 脚本目录在 sys.path 中）
_SESO_DIR = Path(__file__).resolve().parent
if str(_SESO_DIR) not in sys.path:
    sys.path.insert(0, str(_SESO_DIR))
# exchange_calendars 所在第三方包目录：环境变量可配；append 尾部注入，
# 不劫持系统包解析优先级；建议正常 pip install exchange_calendars 后移除
_EC_PACKAGES = os.environ.get("EC_PACKAGES", r"D:\hi\python_packages")
if _EC_PACKAGES and _EC_PACKAGES not in sys.path and os.path.isdir(_EC_PACKAGES):
    sys.path.append(_EC_PACKAGES)

log = logging.getLogger(__name__)

# 时区：Asia/Shanghai（模块级单例，避免每次调用 cn_today 重复 import）
try:
    from zoneinfo import ZoneInfo
    _CN_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _CN_TZ = None

_XSHG = None
_XSHG_LOADED = False
_FALLBACK_CAL = None  # pd.DatetimeIndex | None（指数 parquet 交易日序列）
_CAL_CACHE = None     # pd.DatetimeIndex | None（XSHG/fallback 交易日序列缓存）
_FCAL_MIN = None      # 指数 parquet 日历下界（缓存，避免每次 fcal.min() O(n)）
_FCAL_MAX = None      # 指数 parquet 日历上界


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

    防 UTC 容器（jovyan 默认 TZ=UTC）下 date.today() 在北京时间 0:00~8:00
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
        log.warning("[trade_calendar] XSHG 日历加载失败，降级指数 parquet/周末近似: %s", e)
    return _XSHG


def _fallback_calendar():
    """fallback1：从指数 parquet（000001/000300 date 列）提取交易日序列；
    失败/损坏 warning 留痕并返回 None（继续降级周末近似）。"""
    global _FALLBACK_CAL
    if _FALLBACK_CAL is not None:
        return _FALLBACK_CAL
    try:
        base = _SESO_DIR
        cands = [
            base / "data" / "index" / "000001.parquet",
            base / "data" / "index" / "000300.parquet",
            Path("/home/jovyan/data/index/000001.parquet"),
            Path("/home/jovyan/data/index/000300.parquet"),
        ]
        for p in cands:
            if not p.exists():
                continue
            df = pd.read_parquet(p, columns=["date"])
            cal = pd.to_datetime(df["date"]).dt.normalize().drop_duplicates().sort_values()
            if len(cal) > 0:
                _FALLBACK_CAL = pd.DatetimeIndex(cal)
                log.info("[trade_calendar] 使用指数 parquet 交易日历 fallback: %s", p)
                return _FALLBACK_CAL
    except Exception as e:
        log.warning("[trade_calendar] 指数 parquet fallback 失败，降级周末近似: %s", e)
    return None


def _fcal_bounds(fcal: pd.DatetimeIndex):
    """返回指数 parquet 日历 (min, max) 的缓存版本，避免反复 O(n) 重算（P1 优化）。"""
    global _FCAL_MIN, _FCAL_MAX
    if _FCAL_MIN is None or _FCAL_MAX is None:
        _FCAL_MIN, _FCAL_MAX = fcal.min(), fcal.max()
    return _FCAL_MIN, _FCAL_MAX


def _weekday_approx_prev(d: date) -> date:
    """fallback2 prev：周一回上周五；周末回上周五；其余回昨天。"""
    wd = d.weekday()  # 0=周一
    if wd == 0:
        return d - timedelta(days=3)
    if wd >= 5:       # 周六 -1 天、周日 -2 天 → 上周五
        return d - timedelta(days=wd - 4)
    return d - timedelta(days=1)


def _weekday_approx_next(d: date) -> date:
    """fallback2 next：周五/周末回下周一；其余回明天。"""
    wd = d.weekday()  # 0=周一
    if wd == 4:       # 周五 → 下周一
        return d + timedelta(days=3)
    if wd >= 5:       # 周六 +2、周日 +1 → 下周一
        return d + timedelta(days=7 - wd)
    return d + timedelta(days=1)


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
            log.warning("[trade_calendar] XSHG sessions 提取失败，降级指数 parquet: %s", e)
    fcal = _fallback_calendar()
    if fcal is not None:
        _CAL_CACHE = fcal
        return _CAL_CACHE
    return None


def is_trading_day(d) -> bool:
    """d 是否为交易日。XSHG 覆盖范围内精确判断；范围外/不可用时降级。"""
    d = _normalize(d)
    cal = _xshg_calendar()
    if cal is not None and cal.first_session.date() <= d <= cal.last_session.date():
        try:
            return bool(cal.is_session(pd.Timestamp(d)))
        except Exception as exc:
            log.debug("[trade_calendar] is_trading_day XSHG 查询降级: %s", exc)
    fcal = _fallback_calendar()
    if fcal is not None and len(fcal) > 0:
        try:
            ts = pd.Timestamp(d)
            fmin, fmax = _fcal_bounds(fcal)
            if fmin <= ts <= fmax:
                return bool((fcal == ts).any())
        except Exception as exc:
            log.debug("[trade_calendar] is_trading_day fallback 查询降级: %s", exc)
    return d.weekday() < 5


def prev_trading_day(d) -> date:
    """d 之前最近交易日（不含 d）。节假日精确，超出范围/不可用时降级。

    边界说明（P2）：d 为日历首日（XSHG 首交易日）时 previous_session 超界，
    except 兜住后降级周末近似（日历未覆盖区间的"上一个交易日"语义未知，
    近似值仅供排程参考）。
    """
    d = _normalize(d)
    cal = _xshg_calendar()
    if cal is not None and cal.first_session.date() <= d <= cal.last_session.date():
        try:
            # previous_session 入参必须是交易日：非交易日先转下一个交易日
            s = cal.date_to_session(pd.Timestamp(d), direction="next")
            return cal.previous_session(s).date()
        except Exception as exc:
            log.debug("[trade_calendar] prev_trading_day XSHG 查询降级: %s", exc)
    fcal = _fallback_calendar()
    if fcal is not None and len(fcal) > 0:
        try:
            ts = pd.Timestamp(d)
            fmin, fmax = _fcal_bounds(fcal)
            if fmin <= ts <= fmax:
                arr = fcal[fcal < ts]
                if len(arr) > 0:
                    return arr.max().date()
        except Exception as exc:
            log.debug("[trade_calendar] prev_trading_day fallback 查询降级: %s", exc)
    return _weekday_approx_prev(d)


def next_trading_day(d) -> date:
    """d 之后最近交易日（不含 d）。节假日精确，超出范围/不可用时降级。

    边界说明：d 为日历最后交易日时 next_session 超界，降级周末近似
    （日历未覆盖区间的"下一个交易日"语义未知，近似值仅供排程参考）。
    """
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
            log.debug("[trade_calendar] next_trading_day XSHG 查询降级: %s", exc)
    fcal = _fallback_calendar()
    if fcal is not None and len(fcal) > 0:
        try:
            ts = pd.Timestamp(d)
            fmin, fmax = _fcal_bounds(fcal)
            if fmin <= ts <= fmax:
                arr = fcal[fcal > ts]
                if len(arr) > 0:
                    return arr.min().date()
        except Exception as exc:
            log.debug("[trade_calendar] next_trading_day fallback 查询降级: %s", exc)
    return _weekday_approx_next(d)


def trading_days(start, end):
    """返回 [start, end] 闭区间内的交易日序列（ISO 字符串，升序）。

    用于采集端批量排程（如逐日快照区间）。交易日历不可用时降级为
    工作日近似（周一~周五，降级 warning 留痕）。"""
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
            log.warning("[trade_calendar] trading_days XSHG 查询失败，降级周末近似: %s", exc)
    log.warning("[trade_calendar] 无可用日历，trading_days 降级周末近似 %s~%s", start, end)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def trade_days_between(start, end):
    """统计 (start, end] 区间内的交易日数量（含 end 若为交易日，不含 start）。
    交易日历不可用时降级为周末近似（降级 warning 留痕）。"""
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
            log.warning("[trade_calendar] trade_days_between XSHG 查询失败，降级周末近似: %s", exc)
    # fallback: 周末近似（不含 start，含 end）
    n = 0
    d = start
    while d < end:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


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
        print("  [WARN] XSHG 日历不可用，以下结果可能来自 fallback（本机无指数 parquet 时用周末近似）")
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
    # 周末近似一致性（XSHG 不可用路径）—— 保证 fallback2 不回归
    check("fallback2: prev(周一)=上周五",
          _weekday_approx_prev(date(2026, 10, 12)) == date(2026, 10, 9))
    check("fallback2: next(周五)=下周一",
          _weekday_approx_next(date(2026, 10, 9)) == date(2026, 10, 12))

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
        try:
            p = prev_trading_day(cal2[0].date())
            check("首日 prev_trading_day 不抛异常", isinstance(p, date), str(p))
        except Exception as ex:
            check("首日 prev_trading_day 不抛异常", False, f"抛异常 {ex}")
        try:
            nx = next_trading_day(cal2[-1].date())
            check("末日 next_trading_day 不抛异常", isinstance(nx, date), str(nx))
        except Exception as ex:
            check("末日 next_trading_day 不抛异常", False, f"抛异常 {ex}")
    else:
        print("  [WARN] 无可用交易日历，跳过首尾边界测试")

    print(f"== selfcheck {'全部通过' if not failures else '失败 ' + str(len(failures)) + ' 项'} ==")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(_selfcheck())
