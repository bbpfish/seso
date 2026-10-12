#!/usr/bin/env python3
"""stock_code_utils.py - 证券代码段/市场识别公共模块（SESO 多客户端复用）

职责边界:
  - 纯代码/市场识别：6位校验、A股市场分类、THS 市场代号映射、新浪/腾讯 symbol 前缀
  - 不含任何网络请求与数据拉取；各客户端自行决定"源能力限制"（如腾讯/通达信
    不支持北交所由各客户端拦截，不属于本模块职责）

THS 真实可采集市场依据（thsdk 1.7.18 源码，source-driven）:
  - validators.FIXED_LENGTH_SECURITY_MARKETS 共 14 个市场代号:
      USHA 沪A / USZA 深A / USHB 沪B / USZB 深B / USHJ 沪基金 / USZJ 深基金
      USHD 沪债券 / USZD 深债券 / USHP 沪退市整理 / USZP 深退市整理
      USHT 沪ST风险警示板 / USHI 沪指数 / USZI 深指数 / USTM 北交所
  - klines 校验规则: ths_code 恰为 4位市场代号 + 6位数字（exact_length=10），
    前 4 位必须在上述集合内
  - 新三板(43)、老三板(400/420) 无对应市场代号 -> THS 真实无法采集
  - 非 6 位数字不符合"4位市场代号+6位数字"契约 -> 真实无法采集

对外三值 -> thsdk adjust 映射（ADJUST_MAP）也归本模块管理，供客户端引用。
"""
import logging
from typing import Optional

from kline_env import DATA_DIR

logger = logging.getLogger("stock_code_utils")

# 本地 K 线数据目录：统一定义在 kline_env（DATA_DIR=<work>/data），此处直接引用。
_KLINE_DATA_DIR = DATA_DIR

# 对外采集模式 -> thsdk 合法 adjust 值（thsdk klines 仅接受 ""/forward/backward）
ADJUST_MAP = {"": "", "qfq": "forward", "hfq": "backward"}

# 与 thsdk validators.FIXED_LENGTH_SECURITY_MARKETS 对齐（供断言/文档使用）
THS_KLINES_MARKETS = frozenset({
    "USHI", "USHA", "USHB", "USHD", "USHJ", "USHP", "USHT",
    "USZI", "USZA", "USZB", "USZD", "USZJ", "USZP", "USTM",
})

# ---- 6位代码段 -> 市场分类 ----
# 分类键:
#   sh_a/sz_a 沪深A股 | sh_b/sz_b 沪深B股 | sh_fund/sz_fund 沪深基金/ETF
#   sh_bond/sz_bond 沪深债券(含可转债) | bse 北交所 | neeq 新三板
#   otc 老三板 | invalid 非6位数字
#
# 沪深债券段依据（P1 修复，2026-10-11）：
#   - 上交所《证券交易业务指南第4号——证券代码段分配指南》(2023年第6次修订)：
#     110/111/113/114/115/118 可转换/公司债券，120/121 企业债/资产支持证券，
#     122 公司债/企业债，123 公司债/企业债/ABS，124/125 企业债/私募债，
#     126 分离交易可转债，127 企业债，130 地方政府债，132 可交换债；
#   - 深交所《证券代码区间表》(2024年12月修订)：
#     100-109 国债/地方债/政策性金融债，111 企业债，112 公司债，
#     113 资管计划转让，114 非公开发行公司债，115 分离交易可转债/次级债/可交换债，
#     120-129 区间可转债（历史发行代码保留原段）。
#   - 冲突段处理原则：123/127/128/129 沪深均有分配，但活跃主体为深市可转债
#     （123 创业板转债、127 深主板转债、128 中小板转债、129 老转债续号），按深归属；
#     116/117 上交所/深交所均未分配，保守归 sz_bond（无真实标的，影响面为零）。
SH_BOND_PREFIXES = ("110", "111", "113", "114", "115", "118",
                    "120", "121", "122", "124", "125", "126",
                    "130", "132")
# 沪深共用段，活跃主体为深市可转债（依深交所区间表 120-129 转债保留段）
SZ_BOND_CONFLICT_PREFIXES = ("123", "127", "128", "129")


def is_valid_6digit(code) -> bool:
    """是否为合法的 6 位数字证券代码。"""
    return isinstance(code, str) and code.isdigit() and len(code) == 6


def classify_code(code: str) -> str:
    """6位代码 -> 市场分类键（invalid / neeq / otc / bse / sh_a / ...）。

    基于公开 A 股代码段规则 + thsdk 市场代号覆盖范围：
      - 6 开头 -> sh_a（沪A，含 688 科创）
      - 0/3 开头 -> sz_a（深A：000/001/002/003、300/301）
      - 900 -> sh_b；200 -> sz_b
      - 5 开头 -> sh_fund（沪基金/ETF）
      - SH_BOND_PREFIXES -> sh_bond（沪可转债/公司债/企业债等，见段位注释）
      - SZ_BOND_CONFLICT_PREFIXES -> sz_bond（123/127/128/129 深转债活跃段）
      - 15x/16x -> sz_fund（深基金/ETF/LOF）；其余 1 开头 -> sz_bond（深债券大类）
      - 8 开头 / 92 -> bse（北交所，USTM 支持段）
      - 43 -> neeq（新三板，无 thsdk 市场代号）
      - 400/420 -> otc（老三板，无 thsdk 市场代号）
    """
    if not is_valid_6digit(code):
        return "invalid"
    if code.startswith(("43",)):
        return "neeq"
    if code.startswith(("400", "420")):
        return "otc"
    if code.startswith(("8", "92")):
        return "bse"
    if code.startswith("6"):
        return "sh_a"
    if code.startswith(("0", "3")):
        return "sz_a"
    if code.startswith("900"):
        return "sh_b"
    if code.startswith("200"):
        return "sz_b"
    if code.startswith("5"):
        return "sh_fund"
    if code.startswith(SH_BOND_PREFIXES):
        return "sh_bond"
    if code.startswith(SZ_BOND_CONFLICT_PREFIXES):
        return "sz_bond"
    if code.startswith(("15", "16")):
        return "sz_fund"
    if code.startswith("1"):
        return "sz_bond"
    return "invalid"


def is_ths_uncollectable(code: str) -> bool:
    """THS 真实无法采集的代码（返回 True 应跳过）。

    仅拦截 thsdk 无市场代号、无法构造合法 ths_code 的段：
      - 非 6 位数字（不符 4位市场代号+6位数字 契约）
      - 43 新三板（无市场代号）
      - 400/420 老三板（无市场代号）
    北交所(8/92)、B股(900/200)、基金(5/15/16)、可转债(110/113/118/12x/13x)
    均属 THS 可采集市场，一律放行（是否采集由调度层决策）。"""
    return classify_code(code) in ("invalid", "neeq", "otc")


_THS_CODE_PREFIX = {
    "sh_a": "USHA", "sz_a": "USZA", "sh_b": "USHB", "sz_b": "USZB",
    "sh_fund": "USHJ", "sz_fund": "USZJ",
    "sh_bond": "USHD", "sz_bond": "USZD",
    "bse": "USTM",
}


def get_ths_code(code: str) -> Optional[str]:
    """6位代码 -> THSCODE（4位市场代号 + 6位数字），无法映射返回 None。

    映射覆盖 THS 全部可采集市场（FIXED_LENGTH_SECURITY_MARKETS）：
      sh_a -> USHA / sz_a -> USZA / sh_b -> USHB / sz_b -> USZB
      sh_fund -> USHJ / sz_fund -> USZJ / sh_bond -> USHD / sz_bond -> USZD
      bse -> USTM
    新三板/老三板/非6位 -> None（thsdk 无对应市场代号，真实无法采集）。"""
    prefix = _THS_CODE_PREFIX.get(classify_code(code))
    return f"{prefix}{code}" if prefix else None


def is_bse_zone(code: str) -> bool:
    """北交所代码区（4/8/92 开头）识别，供不覆盖北交所的源（腾讯/通达信）做源能力拦截。

    注：4 开头含 43（北交所挂牌/原新三板精选层过渡段）与部分新三板段，
    具体哪些段各源能采由各客户端按源能力决策；本函数仅做段识别，不判断可采性。"""
    return isinstance(code, str) and code.startswith(("4", "8", "92"))


def get_sina_symbol(code: str) -> str:
    """6位代码 -> 新浪 symbol 前缀（sh/sz/bj），与 kline_sina_client 原口径一致。

    新浪源能力：北交所代码区（4/8/92 开头，含 43 过渡段）使用 bj 前缀；
    沪市（6/9/5 开头，含 900 B 股/沪基金）用 sh；深市（0/3/2xx 等）用 sz。"""
    if is_bse_zone(code):
        return f"bj{code}"
    cls = classify_code(code)
    if cls in ("sh_a", "sh_b", "sh_fund", "sh_bond"):
        return f"sh{code}"
    return f"sz{code}"


def get_tx_symbol(code: str) -> str:
    """6位代码 -> 腾讯 symbol 前缀（sh/sz），与 kline_tx_client 原口径一致。

    注：腾讯源对北交所/三板无覆盖，是否拦截由 kline_tx_client 自身源能力逻辑决定，
    本函数仅提供 symbol 前缀。"""
    cls = classify_code(code)
    if cls in ("sh_a", "sh_b", "sh_fund", "sh_bond"):
        return f"sh{code}"
    return f"sz{code}"


# ---- 指数代码识别与源映射（四客户端指数采集共用） ----
# 指数代码段（与股票段部分重叠，须由调用方保证传指数代码）：
#   000xxx 上证/中证指数（000001 上证、000300 沪深300、000905 中证500 等）
#   399xxx 深证指数（399001 深证成指、399006 创业板指 等）
INDEX_CODE_PREFIXES = ("000", "399")


def is_index_code(code: str) -> bool:
    """按代码段判断是否为指数代码（000/399 开头）。

    注：000 开头同时是深市 A 股段（000001 平安银行），本函数仅按段识别，
    不保证语义——调用方需自行保证传入的是指数代码（如 INDEX_CODES 集合）。"""
    return isinstance(code, str) and code.startswith(INDEX_CODE_PREFIXES)


def get_index_symbol(code: str) -> Optional[str]:
    """指数代码 -> 新浪/腾讯 symbol 前缀（sh/sz），非指数段返回 None。"""
    if not is_index_code(code):
        return None
    return f"sh{code}" if code.startswith("000") else f"sz{code}"


def get_index_ths_code(code: str) -> Optional[str]:
    """指数代码 -> THSCODE（同花顺真实代码），非指数段返回 None。

    实测映射（index_list 本地联调确认，2026-10）：
    - 沪市 000 段：USHI1A/1B + 去前 2 位（000001 上证指数 -> USHI1A0001；
      其余 000xxx 用 1B，如 000300 -> USHI1B0300、000905 -> USHI1B0905、
      000852 -> USHI1B0852、000688 -> USHI1B0688）
    - 深市 399 段：USZI + 6 位（399001 -> USZI399001，保持原格式）
    旧格式 USHI{code}（如 USHI000300）实测返回 not data，已废弃。"""
    if not is_index_code(code):
        return None
    if code.startswith("000"):
        prefix = "USHI1A" if code == "000001" else "USHI1B"
        return f"{prefix}{code[2:]}"
    return f"USZI{code}"


def get_index_tdx_market(code: str) -> Optional[int]:
    """指数代码 -> TDX market 编码（000->1 沪市，399->0 深市），非指数段返回 None。

    必须显式传 market：tdxdata market_of 对 000 开头默认按深市（0）处理，
    而 000001 上证指数实际是沪市（1），依赖默认值会拉错标的。"""
    if not is_index_code(code):
        return None
    return 1 if code.startswith("000") else 0


# ---- 本地名单源（run_kline_multi 调度名单统一入口）----
# 职责补充说明：以下三个函数为 K 线调度提供"交易所全量/持盘名单/退市名单"，
# 数据源取本地 K 线数据目录（data/*.parquet 文件名即 6 位代码），不发起网络请求，
# 保持本模块"无网络依赖"的职责边界；接入独立名单数据源后可替换实现而不改调用方。

def get_exchange_stock_codes():
    """本地持盘全量代码：扫描 K 线数据目录 *.parquet 文件名（6 位数字）升序返回。

    作为"交易所全量名单"的本地替代（run_kline_multi 缺省名单）：
    data 目录中每只股票的 parquet 文件名即 6 位代码，天然覆盖沪深北已采标的。
    数据目录统一见 kline_env.DATA_DIR（<work>/data，本地布局即 <SESO>/data）；仅扫顶层，
    不含 index 等子目录。无文件时返回 []（调用方按空名单处理）。"""
    base = _KLINE_DATA_DIR
    if not base.is_dir():
        return []
    return sorted(p.stem for p in base.glob("*.parquet")
                  if p.stem.isdigit() and len(p.stem) == 6)


def load_stock_basic_codes(start_date=None, end_date=None):
    """加载 basic 名单（start_date/end_date 仅留档）：优先读状态链基础缓存。

    缓存位置：K 线数据目录（kline_env.DATA_DIR，<work>/data）的父目录下的
    stock_basic_cache.parquet；其次尝试数据目录自身。读取 code 列
    （形如 sh.600594）返回原始 code 列表，由调用方剥离交易所前缀为 6 位。
    缓存缺失/损坏时回退持盘代码扫描，仍为空则调用方按空名单处理
    （冷启动需保证 basic 缓存存在）。"""
    import pandas as pd

    base = _KLINE_DATA_DIR
    candidates = [base.parent / "stock_basic_cache.parquet", base / "stock_basic_cache.parquet"]
    for fp in candidates:
        if not fp.is_file():
            continue
        try:
            df = pd.read_parquet(fp)
            if "code" not in df.columns:
                continue
            codes = sorted(df["code"].dropna().astype(str).unique())
            logger.info("load_stock_basic_codes 读缓存 %s：%d 只", fp, len(codes))
            return codes
        except Exception as e:
            logger.warning("load_stock_basic_codes 读缓存失败 %s：%s", fp, str(e)[:120])
    logger.warning("load_stock_basic_codes 无 basic 缓存，回退持盘代码扫描")
    return get_exchange_stock_codes()


def load_delisted_codes():
    """加载退市名单：优先读状态链基础缓存 stock_basic_cache.parquet 的 outDate 字段。

    退市判定：outDate 非空即已退市（生产实测 225 只）。返回原始 code 列表
    （形如 sh.600594），由调用方剥离交易所前缀为 6 位；调用方签名与
    load_stock_basic_codes 一致，缓存缺失/损坏时回退返回 []。
    status 字段全为 1（上市标记），不承载退市语义，不可用作退市判定。

    修正背景（2026-10-11）：原占位实现注释称"本地无退市标记数据源"不成立——
    stock_basic_cache.parquet 的 outDate 字段即退市日期；本实现直接复用该缓存。
    """
    import pandas as pd

    base = _KLINE_DATA_DIR
    candidates = [base.parent / "stock_basic_cache.parquet", base / "stock_basic_cache.parquet"]
    for fp in candidates:
        if not fp.is_file():
            continue
        try:
            df = pd.read_parquet(fp)
            if "code" not in df.columns or "outDate" not in df.columns:
                continue
            od = df["outDate"].astype(str)
            mask = ~od.isin(["NaT", "nan", "None", ""])
            codes = sorted(df.loc[mask, "code"].dropna().astype(str).unique())
            logger.info("load_delisted_codes 读缓存 %s：%d 只退市", fp, len(codes))
            return codes
        except Exception as e:
            logger.warning("load_delisted_codes 读缓存失败 %s：%s", fp, str(e)[:120])
    logger.warning("load_delisted_codes 无 basic 缓存，返回空集"
                   "（退市名单未知，退市股将按普通号发牌兜底）")
    return []
