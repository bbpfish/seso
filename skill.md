---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 041680e517c313fbd9efd67f2de7c4dd_e7f5b786c5e511f1a08152540064ee0f
    ReservedCode1: Oqfxe190/8bpA6LDEvEhIfipu+lS9Z0Y0YGxgzJXOqHCgc8fq4IpMiK7z7hBO3/S6rnUB3KIzWVuwOa7/8K6pvPVkhIX71ecV4uuCHt+Z+BhwIaW0f7hNyInSLI0ewpnjnOxnhKG1oESyBvXHr3klo6I3w1cmIEJurvJLrhtoWX8eMsW/6EiVS1Kdlo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 041680e517c313fbd9efd67f2de7c4dd_e7f5b786c5e511f1a08152540064ee0f
    ReservedCode2: Oqfxe190/8bpA6LDEvEhIfipu+lS9Z0Y0YGxgzJXOqHCgc8fq4IpMiK7z7hBO3/S6rnUB3KIzWVuwOa7/8K6pvPVkhIX71ecV4uuCHt+Z+BhwIaW0f7hNyInSLI0ewpnjnOxnhKG1oESyBvXHr3klo6I3w1cmIEJurvJLrhtoWX8eMsW/6EiVS1Kdlo=
---

# Skill: SESO 数据采集（A 股行情采集）

## 适用场景

- 每日收盘后对全市场或指定清单做增量采集（K 线、指数、状态快照）。
- 首次部署或数据修复时做全量重建（四源并发）。
- 排查采集失败：查看运行日志、核对落盘文件、验证数据契约。
- 判断指数是否需要独立状态采集（结论见下方"重要结论"）。

## 前置条件

1. **环境**：Python 3.9+，已 `pip install -r requirements.txt`。
2. **私有依赖**：`thsdk`（同花顺 SDK）不在 PyPI，需按同花顺官方渠道安装；THS 采集需要 `ths_credentials.env`（账户凭据，已被 `.gitignore` 排除，严禁入库或外泄）。
3. **数据源**：Tushare 备源需配置 token，未配置时自动跳过备源，不影响主源链路。
4. **目录约定**：脚本目录为本仓库（`scripts/` 或仓库根）；数据根目录由 `kline_env.py` 推导（`data/history_kline`、`data/index`、`data/stock_status`、`data/logs`）。

## 常用命令

| 目标 | 命令 |
| --- | --- |
| 每日增量采集（推荐） | `python run_daily_collect.py --incremental` |
| 全量重建 | `python run_daily_collect.py` |
| K 线四源全量 | `python run_kline_multi.py --mode full` |
| K 线两源增量 | `python run_kline_multi.py --mode incremental` |
| 指数四源全量 | `python run_index_collect.py --mode full` |
| 指数当日增量 | `python run_index_collect.py --mode incremental` |
| 状态快照增量 | `python collect_stock_status.py --incremental` |
| 状态快照全量 | `python collect_stock_status.py` |

> 运行入口可能在仓库根目录（`run_*.py`）或 `scripts/` 子目录，Agent 执行前先确认实际路径。

## 数据契约

- **日 K 线**：统一 7 列契约 `date / open / high / low / close / volume / amount`，每标的独立 parquet 文件落盘。
- **指数清单**：`INDEX_CODES = (000001, 000016, 000300, 000905, 000852)`，落盘 `data/index`。
- **状态快照**：股票停牌/退市/上市日期等，落盘 `data/stock_status`。

## 重要结论

- **指数不需要像股票一样单独采集状态快照**。股票状态链独立（`collect_stock_status.py`，baostock 主源 + Tushare 备源）；指数日 K 由 `run_index_collect.py` 统一以 7 列契约落盘，无独立状态快照需求。增量模式下 `run_kline_multi.py` 内嵌指数增量。

## 验收与排障

1. **成功标志**：日志末尾出现 `INDEX_COLLECT_DONE mode=full ... ok=5 fail=0`（指数）或对应任务的 done 摘要行。
2. **落盘核对**：`data/index` 下 5 个指数 parquet 文件存在、非空；K 线按标的核对文件与行数。
3. **常见错误**：
   - `ModuleNotFoundError`：确认第三方包已安装且位于当前解释器的 `sys.path`（若包装在自定义目录，需先注入该目录再 import）。
   - `module 'logging' has no attribute 'handlers'`：脚本缺 `import logging.handlers`，补上后重跑。
4. **日志位置**：`data/logs/` 或运行环境约定的 LOG_DIR。

## 安全约束（Agent 必须遵守）

- 本仓库为**公共仓库**，禁止提交任何凭据、内网地址、本机绝对路径（Windows 路径、VPS IP 等）。
- 提交前自查：`grep` 敏感关键字（`192.168`、`D:\`、token、密码），跑 `pyflakes` 静态检查，确保无新增告警。
- 修改代码后运行 `tests/` 下平台无关单测子集，避免破坏 CI。
*（内容由AI生成，仅供参考）*
