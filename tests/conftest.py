#!/usr/bin/env python3
"""pytest 夹具/路径配置：将 SESO 根目录加入 sys.path，保证从任意目录
运行 pytest 均可 import 被测模块（status_schema/sources/baostock_client）。"""
import sys
from pathlib import Path

SESO_ROOT = Path(__file__).resolve().parents[1]
if str(SESO_ROOT) not in sys.path:
    sys.path.insert(0, str(SESO_ROOT))
