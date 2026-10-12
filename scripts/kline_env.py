#!/usr/bin/env python3
"""kline_env.py — 采集链项目常量：版本号 / 统一目录定义 / 布局推导

目录定义统一收敛到本模块一次推导，
各采集脚本直接引用（SCRIPT_DIR / WORK_ROOT / SCRIPTS_DIR / DATA_DIR / LOG_DIR /
CACHE_DIR / INDEX_DIR），禁止各自重复推导或引入新的覆盖层。
KLINE_VERSION 为采集模块唯一版本号（--version / 启动日志对外显示），其余文件
不重复定义。

依赖策略：仅标准库（pathlib），任意模块可顶层导入。
"""
from pathlib import Path

# ---- 采集模块唯一版本号（--version / 启动日志对外显示；禁止在各文件重复定义） ----
# v1.2.0: 四源客户端新增指数日K采集（fetch_index_daily / update_index，
#         统一 INDEX_COLS 7列契约，落盘 data/index；THS 成交额列名"总金额"修正）
# v1.3.0: 新增指数采集编排 work（run_index_collect.py）：full=四源全量发号，
#         incremental=THS+TDX 两源当日增量；增量调度已集成 run_kline_multi 每日增量路径
# v1.3.1: 指数采集小问题修复（对齐 K 线成熟做法）：
#         ① incremental 恒 count=1 只拉当日；full 四分支查漏（skip/repair/gap/full_fetch）；
#            --overwrite 复写；四客户端 update_index 增加 overwrite 可选参并改为列级合并
#            （新数据非 NaN 优先，可修复中间缺列）；新增 INDEX_CODES 收敛两处重复定义
#         ② 启动校验交易日历可用，失败 fail-loud（拒绝 trade_days_between 返 0 致
#            full 假成功 / prev_trading_day 为 None 崩溃）；INDEX_CODES 参数化：
#            CLI 新增 --codes 逗号分隔清单；按 code 主线程统一判定一次，广播四源；
#            sina/tx 指数请求复用共享 Session 连接池（full 模式短连接 -> keep-alive 复用）
#         ③ 写盘独占对齐 K 线 _Cursor 取号模型：共享游标一次取一号、一 code 一源独占
#            写盘，消除广播模型四源同写 parquet 的 last-writer 竞争
KLINE_VERSION = "1.3.1"


def resolve_work_root(script_dir):
    """统一布局推导：脚本位于 <work>/scripts/（生产）时工作根取父目录；
    本地单目录布局（脚本即项目根）时工作根取脚本目录。

    判据：<work>/scripts 目录是否存在（run_daily_collect / collect_stock_status
    原语义；生产两目录并存时与 data 判据结果等价，本地无子目录时均回退脚本目录）。"""
    script_dir = Path(script_dir)
    if (script_dir.parent / "scripts").exists():
        return script_dir.parent
    return script_dir


# ---- 目录统一定义（采集链各脚本唯一事实源；由 resolve_work_root 推导一次） ----
SCRIPT_DIR = Path(__file__).resolve().parent
WORK_ROOT = resolve_work_root(SCRIPT_DIR)
SCRIPTS_DIR = WORK_ROOT / "scripts" if (WORK_ROOT / "scripts").exists() else SCRIPT_DIR
DATA_DIR = WORK_ROOT / "data"
LOG_DIR = WORK_ROOT / "logs"
CACHE_DIR = WORK_ROOT / "cache"
INDEX_DIR = DATA_DIR / "index"

# ---- 指数发号清单（收敛两处重复定义：原 run_index_collect.py / kline_sina_client.py，
#      统一口径：上证/中证 + 深证代表指数；元组保序，消费端按此顺序发号） ----
INDEX_CODES = ("000001", "000016", "000300", "000905", "000852")
