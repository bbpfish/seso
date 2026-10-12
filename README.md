# SESO — 数据采集模块（公共版）

SESO（Self-Evolving Strategy Optimizer）是一套面向 A 股市场的量化策略自进化优化系统。本仓库公开其**数据采集模块**：状态快照链、K 线四源采集与指数采集，覆盖从交易日历判断、股票状态快照、K 线多源并发采集到指数日 K 落盘的全链路。

> 说明：仓库仅包含采集模块相关代码；策略、回测、实盘等模块不在本仓库公开范围内。生产环境的部署路径、凭据等敏感信息不入库。

---

## 1. 采集模块架构

```
run_daily_collect.py（每日总编排：交易日判断 + 子链调度）
├── collect_stock_status.py     状态快照链（主源 baostock，备源 Tushare）
├── run_kline_multi.py          K 线四源调度（full 四路 / incremental 两路）
│   ├── kline_ths_client.py     同花顺 THS 源（thsdk 私有 SDK）
│   ├── kline_sina_client.py    Sina 源（akshare）
│   ├── kline_tx_client.py      Tencent 源（akshare）
│   └── kline_tdx_client.py     TDX 源（tdxdata，附 is_index 补丁自愈）
└── run_index_collect.py        指数采集编排（full 四源 / incremental 两源）
```

- **入口编排**：`run_daily_collect.py` 统一判断交易日，非交易日跳过整个采集链；容灾隔离——快照失败不阻断 K 线。
- **K 线四源**：`run_kline_multi.py --mode full` 四源并发（THS/Sina 正序 + TDX/TX 倒序）；`--mode incremental` 两源增量（THS + TDX），复用连接；`--overwrite` 可叠加强制重采。
- **指数采集**：`run_index_collect.py --mode full` 四源全量发号并发；`--mode incremental` THS+TDX 当日增量。发号清单 `INDEX_CODES = (000001, 000016, 000300, 000905, 000852)`，统一 7 列契约（date/open/high/low/close/volume/amount）落盘指数目录。

---

## 2. 文件功能说明

### 2.1 脚本（`scripts/`）

| 文件 | 功能 |
| --- | --- |
| `run_daily_collect.py` | 每日采集总编排入口：交易日判断 + 快照链 + K 线链调度，容灾隔离。 |
| `run_kline_multi.py` | K 线多源调度：四源并发/full 与两源增量/incremental，增量路径内嵌指数增量。 |
| `run_index_collect.py` | 指数采集编排：full 四源全量发号并发；incremental THS+TDX 当日增量。 |
| `kline_ths_client.py` | 同花顺 THS 日 K 客户端（thsdk 私有 SDK），含指数采集。 |
| `kline_sina_client.py` | Sina 日 K 客户端（akshare），含指数采集。 |
| `kline_tx_client.py` | Tencent 日 K 客户端（akshare），含指数采集。 |
| `kline_tdx_client.py` | TDX 日 K 客户端（tdxdata），含指数采集与 turnover 批量。 |
| `kline_env.py` | 采集模块统一目录/版本常量（SCRIPT_DIR/WORK_ROOT/DATA_DIR/LOG_DIR 等）。 |
| `collect_stock_status.py` | 股票状态快照链（停牌/退市/上市日期等），增量/全量。 |
| `baostock_client.py` | baostock 主源封装（状态快照）。 |
| `sources.py` | 快照数据源适配（baostock 主源 + akshare/tushare 备源）。 |
| `status_schema.py` | 状态快照 parquet schema 与读写。 |
| `trade_calendar.py` | 交易日历：A 股交易日判断、区间交易日列表（exchange_calendars）。 |
| `stock_code_utils.py` | 股票代码工具：basic 名单加载、交易所前缀剥离、指数符号映射等。 |
| `io_atomic.py` | 原子落盘工具（tmp + fsync + os.replace，失败降级 copyfile）。 |
| `ensure_tdxdata_patch.py` | tdxdata is_index 指数补丁自愈脚本（幂等），随仓库同步补丁文件。 |

### 2.2 补丁目录（`scripts/tdxdata_patch/`）

| 文件 | 功能 |
| --- | --- |
| `bars.py` | tdxdata 0.1.1 指数行情补丁：`parse_bars_response` 增加 `is_index` 参数。 |
| `client.py` | tdxdata 补丁：`bars()` 透传 `is_index`。 |

---

## 3. 环境与依赖

- **系统**：Python 3.9+，建议 Linux；Windows 亦可运行（内置 `fcntl`/`msvcrt` 兼容分支）。
- **依赖安装**：

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

- **私有依赖**：`thsdk`（同花顺 SDK，`from thsdk import THS`）不在 PyPI，需按同花顺官方渠道单独安装；THS 采集需提供 `ths_credentials.env`（账户凭据，不入库，已被 `.gitignore` 排除）。
- **Tushare 备源**：`sources.py` 的 tushare 备源需配置 token（环境变量或本地配置，不入库）；未配置时自动跳过备源，不影响主源链路。

---

## 4. 运行方式

### 4.1 每日采集（推荐）

```bash
python scripts/run_daily_collect.py --incremental      # 每日收盘后增量
python scripts/run_daily_collect.py                    # 全量重建（快照全量 + K 线全量）
python scripts/run_daily_collect.py --incremental --kline-limit 20   # 试点自检
```

### 4.2 K 线四源单独运行

```bash
python scripts/run_kline_multi.py --mode full          # 四源全量
python scripts/run_kline_multi.py --mode incremental   # 两源增量（默认）
python scripts/run_kline_multi.py --mode full --overwrite   # 叠加强制重采
```

### 4.3 指数采集单独运行

```bash
python scripts/run_index_collect.py --mode full          # 四源全量
python scripts/run_index_collect.py --mode incremental   # THS+TDX 增量
```

### 4.4 状态快照单独运行

```bash
python scripts/collect_stock_status.py --incremental     # 增量快照
python scripts/collect_stock_status.py                   # 全量快照
```

---

## 5. 配置说明

### 5.1 目录约定（相对运行环境的工作根目录）

| 路径 | 用途 |
| --- | --- |
| `work/scripts` | 脚本目录（本仓库） |
| `data` | 数据根目录 |
| `data/history_kline` | 日 K 线 parquet 库（每股票一文件） |
| `data/index` | 指数 K 线（000001/000300 等） |
| `data/stock_status` | 状态快照 parquet |
| `data/logs` | 运行日志 |
| `work/output` | 产出目录 |

> 实际路径由 `kline_env.py` 基于脚本位置推导（支持环境变量覆盖），部署时只需保证工作根目录可写。

### 5.2 环境变量与凭据

- `ths_credentials.env`：同花顺账户凭据，已被 `.gitignore` 排除，严禁入库。
- Tushare token：仅本地配置，不入库。
- 无其他外部配置依赖。

### 5.3 Git 与 CI

- 主分支 `main`；提交信息遵循 `P0-xx / P1-xx / chore` 等约定。
- CI（GitHub Actions）：push 至 `main` 触发，对 `scripts/` 全部 Python 文件执行 `py_compile` 语法检查 + `pyflakes` 静态检查，并运行平台无关的单测子集（`tests/test_sources.py`、`tests/test_status_schema.py`、`tests/test_baostock_client.py`、`tests/test_stock_code_utils.py`）。

*（内容由 AI 生成，仅供参考）*
