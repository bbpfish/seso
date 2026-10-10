#!/usr/bin/env python3
"""P1 单测：sources._norm_date（Tushare list_date 归一化）。"""
from sources import _norm_date


def test_norm_date_compact_8digit():
    """'20260929' -> '2026-09-29'（Tushare 紧凑日期格式）。"""
    assert _norm_date("20260929") == "2026-09-29"


def test_norm_date_already_dashed():
    """已为 'YYYY-MM-DD' 时原样返回（幂等）。"""
    assert _norm_date("2026-09-29") == "2026-09-29"


def test_norm_date_short_or_long():
    """长度非 8 的输入原样返回。"""
    assert _norm_date("") == ""
    assert _norm_date("2026092") == "2026092"
    assert _norm_date("202609291") == "202609291"


def test_norm_date_non_digit():
    """含非数字字符的输入原样返回（日期解析异常不静默改名）。"""
    assert _norm_date("2026-09-2a") == "2026-09-2a"
    assert _norm_date("abc") == "abc"
