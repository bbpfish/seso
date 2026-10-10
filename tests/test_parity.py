#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SESO 平价性回归测试（P1-2, 2026-09-26；2026-09-27 恢复）

回测 / 实盘 / 挖掘三端共享口径的纯函数回归保护，全部离线、无外部数据依赖。
覆盖：
  1. inverse_factor_mining.rank01  并列秩语义（average ranks，对齐 scipy kind='average'）
  2. inverse_factor_mining.spearman 边界（n<30、常数列、完全单调）
  3. seso_engine.compute_score     行业 cap、全零分守卫、未登记因子 fail-closed
  4. bridge_push_strategy._mom_ret_live vs BacktestEngine._mom_ret 平价
  5. collect_financial._parse_val 财务百分比量纲（'14.92%'→14.92；12.34→12.34）

运行（容器内，依赖 fcntl/akshare，Windows 本地不可直接跑）：
  python tests/test_parity.py        # 裸 assert 模式
  python -m pytest tests/ -q         # pytest 模式（若已安装）
"""
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from inverse_factor_mining import rank01, spearman
from collect_financial import _parse_val


# ── 1. rank01 并列秩 ─────────────────────────────────────────────
def test_rank01_ties_average():
    # [1,2,2,4]：两个 2 取平均序位 (1+2)/2=1.5，归一化分母 m-1=3
    r = rank01([1, 2, 2, 4])
    assert np.allclose(r, [0.0, 0.5, 0.5, 1.0]), f"rank01 ties: {r}"


def test_rank01_nan_and_single():
    r = rank01([1, np.nan, 2])
    assert np.isnan(r[1]) and np.isclose(r[0], 0.0) and np.isclose(r[2], 1.0), f"rank01 NaN: {r}"
    assert np.isclose(rank01([5.0])[0], 0.5), "rank01 单元素应返回 0.5"
    assert rank01([]).size == 0, "rank01 空输入应返回空数组"


# ── 2. spearman 边界 ─────────────────────────────────────────────
def test_spearman_edges():
    x = np.arange(30, dtype=float)
    assert np.isclose(spearman(x, x.copy()), 1.0), "完全正相关应为 +1"
    assert np.isclose(spearman(x, -x), -1.0), "完全负相关应为 -1"
    assert np.isnan(spearman(np.arange(10, dtype=float), np.arange(10, dtype=float))), "n<30 应返回 nan"
    assert np.isnan(spearman(np.full(30, 1.0), np.arange(30, dtype=float))), "常数列应返回 nan"


# ── 3. compute_score 行业 cap / 全零守卫 / 未登记因子 ────────────
def _make_cs_df():
    """30 只合成截面：3 行业（A/B/C 各 10 只），因子值 A>B>C，C 含一只 -5 离群
    （压低全截面 z-score 顶部，确保因子暴露约束不触发，断言只看行业 cap）。"""
    codes = [f"A{i}" for i in range(10)] + [f"B{i}" for i in range(10)] + [f"C{i}" for i in range(10)]
    industry = ["银行"] * 10 + ["白酒"] * 10 + ["医药"] * 10
    roe = (list(np.linspace(0.20, 0.11, 10))
           + list(np.linspace(0.10, 0.01, 10))
           + list(np.linspace(0.00, -0.08, 9)) + [-5.0])
    return pd.DataFrame({"roe_latest": roe, "industry": industry}, index=codes)


def test_compute_score_industry_cap():
    from seso_engine import compute_score
    out = compute_score(_make_cs_df(), {"roe_latest": 1.0}, top_n=5)
    inds = list(out["industry"])
    assert len(out) == 5, f"3 行业 cap=2 应能补足 5 只: {inds}"
    for ind in set(inds):
        assert inds.count(ind) <= 2, f"行业 cap(max(2,5*20%)=2) 被突破: {inds}"
    # 因子值最高的 A0/A1 必须入选
    assert "A0" in out.index and "A1" in out.index, f"排序错误: {list(out.index)}"


def test_compute_score_all_zero_guard():
    from seso_engine import compute_score
    df = _make_cs_df()
    df["ret_1m"] = 0.0
    out = compute_score(df, {"ret_1m": 1.0}, top_n=5)
    assert len(out) == 0, f"全零分截面应返回空（P3-6 守卫），实际 {len(out)} 只"


def test_compute_score_unregistered_fail_closed():
    from seso_engine import compute_score
    try:
        compute_score(_make_cs_df(), {"not_a_factor_xyz": 1.0}, top_n=5)
    except RuntimeError:
        return
    raise AssertionError("未登记因子应 raise RuntimeError（fail-closed）")


# ── 4. _mom_ret 回测/实盘平价 ────────────────────────────────────
def test_mom_ret_parity(tmp_path=None):
    from bridge_push_strategy import _mom_ret_live
    import bridge_push_strategy as bps
    from optimized_backtest import BacktestEngine

    dates = pd.date_range("2026-08-01", periods=30, freq="B")
    close = 100.0 * (1.001 ** np.arange(30))
    df = pd.DataFrame({"close": close}, index=dates)

    # 回测侧：绕过 __init__ 直接注入 full_cache（_mom_ret 只依赖该属性）
    be = object.__new__(BacktestEngine)
    be.full_cache = {"000001": df}
    ref = be._mom_ret("000001", dates[-1], 5)

    # 实盘侧：写临时 parquet 并重定向 HISTORY_KLINE_DIR
    tmp = Path(tempfile.mkdtemp(prefix="seso_mom_"))
    df.reset_index(names="date").to_parquet(tmp / "000001.parquet", index=False)
    orig = bps.HISTORY_KLINE_DIR
    bps.HISTORY_KLINE_DIR = tmp
    try:
        live = _mom_ret_live("000001", 5)
    finally:
        bps.HISTORY_KLINE_DIR = orig

    expected = float(close[-1] / close[-6] - 1)
    assert ref is not None and live is not None, "mom_ret 不应为 None（数据充足）"
    assert abs(ref - expected) < 1e-12 and abs(live - expected) < 1e-12, \
        f"口径分叉: 回测={ref} 实盘={live} 期望={expected}"


# ── 5. _parse_val 财务量纲 ───────────────────────────────────────
def test_parse_val_pct():
    assert _parse_val("14.92%") == 14.92, "'14.92%' 应得百分数值 14.92"
    assert _parse_val(12.34) == 12.34, "数值型 12.34 不应再除 100"
    assert _parse_val("0.15") == 0.15, "字符串小数直接转 float"
    assert _parse_val("1,234.5%") == 1234.5, "千分位逗号应剥离"
    assert _parse_val("--") is None and _parse_val("") is None, "占位符应为 None"
    assert _parse_val(None) is None and _parse_val(np.nan) is None


_ALL = [v for k, v in sorted(globals().items()) if k.startswith("test_")]

if __name__ == "__main__":
    failed = 0
    for fn in _ALL:
        try:
            fn()
            print(f"  PASS {fn.__name__}")
        except Exception as e:
            failed += 1
            print(f"  FAIL {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(_ALL) - failed}/{len(_ALL)} 通过")
    sys.exit(1 if failed else 0)

