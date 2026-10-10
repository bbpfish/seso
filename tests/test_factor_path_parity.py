#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""因子双路径一致性校验（P1-1, 2026-09-26；2026-09-27 恢复）

背景：同一套因子口径存在多处实现——
  库路径  data/factor_timeseries/*.parquet（export_daily_factors → RealFactorEngine 生成，
          回测方案D直读、实盘快照同源）
  挖掘路径 inverse_factor_mining._series_from_frames（全A逆向挖掘 X 矩阵）
历史上 macd/beta_60/KDJ 公式漂移事故均源于多副本实现。本脚本对最新截面抽样
重算比对，公式漂移即非零退出（供 cron / CI 告警）。

口径说明：
  - 仅覆盖 _series_from_frames 产出的技术因子（财务因子挖掘端走 FIN_MAP 披露日
    对齐合并，不在 _series_from_frames 内，两路径语义本就不同，不纳入比对）。
  - 挖掘端按 snapshot_date 截断输入（searchsorted side=right，与挖掘主流程同口径）。
  - 判定：rel_diff = |a-b|/max(|a|,|b|,1e-6)，rel>1e-6 且 abs>1e-9 记 mismatch；
    单因子 mismatch 占比 >2% 判定 DRIFT。浮点噪声（~1e-16）远低于阈值。

运行（容器内）：
  python tests/test_factor_path_parity.py            # 校验，漂移时退出码 1
  python -m pytest tests/test_factor_path_parity.py  # pytest 模式
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from inverse_factor_mining import FTS_DIR, FACTOR_COLS, compute_stock_factor_series

SAMPLE_N = 50          # 抽样股票数（固定种子，可复现）
SEED = 42
REL_TOL = 1e-6
ABS_TOL = 1e-9
MAX_MISMATCH_FRAC = 0.02   # 单因子允许的最大不一致占比（防个别脏数据误报）
MIN_PAIRS = 15             # 单因子最少有效对数，不足判 INSUFFICIENT（不告警）


def run_check():
    files = sorted(FTS_DIR.glob("factor_timeseries_*.parquet"))
    if not files:
        print("[parity] SKIP: factor_timeseries 库为空")
        return 0
    latest = files[-1]
    snap_date = pd.Timestamp(latest.stem.replace("factor_timeseries_", ""))
    stored = pd.read_parquet(latest)
    if "code" in stored.columns:
        stored = stored.set_index("code")
    stored.index = stored.index.astype(str).str.zfill(6)

    codes = sorted(stored.index.unique().tolist())
    rng = np.random.RandomState(SEED)
    sample = list(rng.choice(codes, size=min(SAMPLE_N, len(codes)), replace=False))

    # 收集 (stored, mining) 成对值
    pairs = {f: [] for f in FACTOR_COLS}
    used = 0
    for code in sample:
        if code not in stored.index:
            continue
        fdf, _ = compute_stock_factor_series(code, truncate_at=snap_date)
        if fdf is None or fdf.empty:
            continue
        mrow = fdf.iloc[-1]
        srow = stored.loc[code]
        used += 1
        for f in FACTOR_COLS:
            if f not in mrow.index or f not in srow.index:
                continue
            try:
                mv, sv = float(mrow[f]), float(srow[f])
            except (TypeError, ValueError):
                continue
            if np.isfinite(mv) and np.isfinite(sv):
                pairs[f].append((sv, mv))

    print(f"[parity] 截面={snap_date.date()} 抽样={used}/{len(sample)} 只（种子 {SEED}）")
    print("  %-22s %6s %10s %12s  %s" % ("因子", "对数", "不一致占比", "最大rel_diff", "判定"))
    n_drift = 0
    for f in FACTOR_COLS:
        pts = pairs[f]
        if len(pts) < MIN_PAIRS:
            continue  # 有效对不足不判定（新股长窗口缺失等，属合理缺失）
        sv = np.array([p[0] for p in pts])
        mv = np.array([p[1] for p in pts])
        denom = np.maximum(np.maximum(np.abs(sv), np.abs(mv)), 1e-6)
        rel = np.abs(sv - mv) / denom
        mism = (rel > REL_TOL) & (np.abs(sv - mv) > ABS_TOL)
        frac = float(mism.mean())
        verdict = "OK"
        if frac > MAX_MISMATCH_FRAC:
            verdict = "DRIFT"
            n_drift += 1
        print("  %-22s %6d %9.1f%% %12.2e  %s" % (f, len(pts), frac * 100, float(rel.max()), verdict))
    if n_drift:
        print(f"[parity] FAIL: {n_drift} 个因子双路径漂移——请核对公式变更是否同步到两条路径")
        return 1
    print("[parity] PASS: 抽样因子双路径一致")
    return 0


def test_factor_path_parity():
    assert run_check() == 0, "因子双路径一致性校验失败，详见上方 DRIFT 清单"


if __name__ == "__main__":
    sys.exit(run_check())
