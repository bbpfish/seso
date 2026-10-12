#!/usr/bin/env python3
"""P1 单测：stock_code_utils.classify_code 代码段 -> 市场分类矩阵。

2026-10-11 审核报告 P1-3 修复配套：沪深债券段按上交所《证券交易业务指南
第4号》/ 深交所《证券代码区间表》补齐，冲突段 123/127/128/129 归深市可转债
活跃段；本矩阵固化分类预期，防止后续段位调整回归。
"""
from stock_code_utils import classify_code, load_delisted_codes


def test_sh_bond_prefixes():
    """沪债券段 -> sh_bond（上交所指南段位 110-132）。"""
    for code in ["110001", "111001", "113001", "114001", "115001", "118001",
                 "120001", "121001", "122001", "124001", "125001", "126001",
                 "130001", "132001"]:
        assert classify_code(code) == "sh_bond", code


def test_sz_bond_conflict_prefixes():
    """沪深共用段 123/127/128/129 -> sz_bond（深市可转债活跃段）。"""
    for code in ["123001", "127001", "128001", "129001"]:
        assert classify_code(code) == "sz_bond", code


def test_sz_bond_other_prefixes():
    """深债券段（100-109/112/116/117 等）-> sz_bond。"""
    for code in ["100001", "104001", "112001", "116001", "117001"]:
        assert classify_code(code) == "sz_bond", code


def test_stock_segments_unchanged():
    """A股/基金/B股/北交所/三板分类不受债券段调整影响。"""
    cases = {
        "600001": "sh_a", "688001": "sh_a",
        "000001": "sz_a", "300001": "sz_a",
        "200001": "sz_b", "900001": "sh_b",
        "510001": "sh_fund", "159001": "sz_fund",
        "430001": "neeq", "400001": "otc", "420001": "otc",
        "830001": "bse", "920001": "bse",
        "abc123": "invalid", "12345": "invalid",
    }
    for code, exp in cases.items():
        assert classify_code(code) == exp, code


def test_delisted_placeholder_semantics():
    """退市名单占位：未接入数据源时返回空 list（不伪造名单，发牌端走告警）。"""
    assert isinstance(load_delisted_codes(), list)
    assert len(load_delisted_codes()) == 0
