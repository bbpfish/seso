#!/usr/bin/env python3
"""P1 单测：status_schema.append_partitioned / assert_normal_threshold。

append_partitioned 覆盖：年份分区写入、跨年分文件、同 (date, code) 幂等去重
（keep=last）、空输入返回空截面、返回最新日截面。
assert_normal_threshold 覆盖：正常截面通过、低于阈值抛 RuntimeError、
仅统计最新截面（历史截面 normal 数不计入）。
"""
import pandas as pd
import pytest

from status_schema import COLS, append_partitioned, assert_normal_threshold, year_parts


def _rows(dates):
    """构造 COLS 顺序的最小状态行（同一只股票，日期可变）。"""
    return [(d, "sh.600000", "浦发银行", "normal", "none", False, "1999-11-10")
            for d in dates]


def test_append_partitioned_writes_year_file(tmp_path):
    rows = _rows(["2026-10-09", "2026-10-10"])
    append_partitioned(tmp_path, rows)
    parts = year_parts(tmp_path)
    assert [p.name for p in parts] == ["2026.parquet"]
    df = pd.read_parquet(parts[0])
    assert set(df["date"]) == {"2026-10-09", "2026-10-10"}
    assert len(df) == 2


def test_append_partitioned_split_years(tmp_path):
    """跨年新行按年归并到不同年份文件。"""
    rows = _rows(["2025-12-31", "2026-01-02"])
    append_partitioned(tmp_path, rows)
    assert [p.name for p in year_parts(tmp_path)] == ["2025.parquet", "2026.parquet"]
    assert set(pd.read_parquet(tmp_path / "2025.parquet")["date"]) == {"2025-12-31"}
    assert set(pd.read_parquet(tmp_path / "2026.parquet")["date"]) == {"2026-01-02"}


def test_append_partitioned_dedup_keep_last(tmp_path):
    """同 (date, code) 重复写入时保留后写行（幂等覆盖，不重复累积）。"""
    append_partitioned(tmp_path, _rows(["2026-10-10"]))
    dup = [("2026-10-10", "sh.600000", "浦发银行改", "suspended", "st",
            True, "1999-11-10")]
    append_partitioned(tmp_path, dup)
    df = pd.read_parquet(tmp_path / "2026.parquet")
    assert len(df) == 1
    assert df.iloc[0]["name"] == "浦发银行改"
    assert df.iloc[0]["status"] == "suspended"


def test_append_partitioned_empty_rows(tmp_path):
    """空 rows 不落盘、返回空 DataFrame。"""
    out = append_partitioned(tmp_path, [])
    assert out.empty
    assert year_parts(tmp_path) == []


def test_append_partitioned_returns_latest_day_slice(tmp_path):
    """返回值为本次写入最新日的截面（供 assert_normal_threshold 使用）。"""
    rows = _rows(["2026-10-09", "2026-10-10"])
    day = append_partitioned(tmp_path, rows)
    assert set(day["date"]) == {"2026-10-10"}
    assert len(day) == 1


def test_assert_normal_threshold_ok():
    df = pd.DataFrame([
        ("2026-10-10", "sh.600000", "浦发银行", "normal", "none", False, "1999-11-10"),
        ("2026-10-10", "sz.000001", "平安银行", "suspended", "none", False, "1991-04-03"),
    ], columns=COLS)
    assert assert_normal_threshold(df, min_normal=1) == 1


def test_assert_normal_threshold_fail_below_min():
    df = pd.DataFrame([
        ("2026-10-10", "sh.600000", "浦发银行", "suspended", "none", False, "1999-11-10"),
    ], columns=COLS)
    with pytest.raises(RuntimeError, match="normal"):
        assert_normal_threshold(df, min_normal=1)


def test_assert_normal_threshold_uses_latest_day_only():
    """只统计最新截面：昨日 normal 充足、今日跌破阈值仍应拒绝写入。"""
    rows = [
        ("2026-10-09", "sh.600000", "浦发银行", "normal", "none", False, "1999-11-10"),
        ("2026-10-10", "sh.600000", "浦发银行", "suspended", "none", False, "1999-11-10"),
    ]
    df = pd.DataFrame(rows, columns=COLS)
    with pytest.raises(RuntimeError, match="normal"):
        assert_normal_threshold(df, min_normal=1)
