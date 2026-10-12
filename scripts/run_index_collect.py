#!/usr/bin/env python3
"""run_index_collect.py — 指数采集编排 work（独立调度模块）

能力：
- full（全量）：四源（ths/sina/tx/tdx）并发发号，对 INDEX_CODES 全量拉取并
  增量合并落盘 INDEX_DIR/{code}.parquet（各客户端 update_index 幂等：
  已有文件只补新日期，缺列补 NaN，原子替换）。
- incremental（当日增量）：仅 ths+tdx 两源并发发号，拉最近 count（默认 15）
  根覆盖当日，update_index 增量合并（与股票 incremental 两路口径一致）。

集成：
- 独立触发：`python run_index_collect.py --mode full|incremental`
- 每日增量脚本：run_kline_multi.py incremental 路径在股票当日增量完成后调用
  collect_index_incremental(ths_ctx=..., tdx_ctx=...) 复用连接执行指数增量。

落盘契约：统一 INDEX_COLS = [date, open, high, low, close, volume, amount]，
与 kline_ths/sina/tx/tdx_client 四客户端 fetch_index_daily / update_index 对称。
"""
import argparse
import logging
import sys
import threading

from kline_env import KLINE_VERSION, LOG_DIR  # noqa: E402
from kline_env import SCRIPT_DIR  # noqa: E402

# 保证 scripts 目录在 sys.path（生产布局 <work>/scripts，模块同目录即满足）
sys.path.insert(0, str(SCRIPT_DIR))

# 指数发号清单（与 kline_sina_client.INDEX_CODES 同源口径：上证/中证 + 深证代表指数）
INDEX_CODES = ("000001", "000016", "000300", "000905", "000852")

# 当日增量拉取根数：覆盖当日 + 跨周末/节假日余量，足够增量合并（不追求全历史）
INCREMENTAL_COUNT = 15
# 全量拉取根数：与各客户端 update_index 默认一致（约 600 交易日，覆盖近 2.5 年）
FULL_COUNT = 600

logger = logging.getLogger("run_index_collect")


def _setup_logging():
    """与 run_kline_multi._setup_logging 同款：独立触发时配置，被导入时不重复。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if root.handlers:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "run_index_collect.log", maxBytes=20 * 1024 * 1024,
        backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)


def _open_source(source):
    """打开数据源连接（ths/tdx 需长连接；sina/tx 走 akshare 无连接）。"""
    if source == "ths":
        from thsdk import THS
        _u = __import__("os").environ.get("THS_USERNAME")
        _p = __import__("os").environ.get("THS_PASSWORD")
        if _u and _p:
            ctx = THS({"username": _u, "password": _p})
            logger.info("THS connected with formal account: %s***", _u[:2])
        else:
            ctx = THS()
            logger.warning("THS using guest account (THS_USERNAME/THS_PASSWORD unset)")
        ctx.__enter__()
        return ctx
    if source == "tdx":
        from tdxdata import TdxClient
        ctx = TdxClient()
        ctx.connect()
        return ctx
    return None


def _close_source(ctx):
    if ctx is None:
        return
    try:
        ctx.__exit__(None, None, None)
    except Exception:
        try:
            ctx.close()
        except Exception:
            pass


def _collect_one(source, code, count, ctx=None):
    """按源调用对应客户端 update_index 落盘单只指数，返回 bool。"""
    if source == "ths":
        import kline_ths_client
        return kline_ths_client.update_index(ctx, code, count=count) is True
    if source == "tdx":
        import kline_tdx_client
        return kline_tdx_client.update_index(code, count=count) is True
    if source == "sina":
        import kline_sina_client
        return kline_sina_client.update_index(code) is True
    if source == "tx":
        import kline_tx_client
        return kline_tx_client.update_index(code) is True
    logger.error("[%s] 未知源，跳过 %s", source, code)
    return False


def _collect_source(source, codes, count, stats, ths_ctx=None, tdx_ctx=None):
    """单源线程体：遍历发号清单逐只 update_index，结果写入 stats[source]。"""
    st = {"ok": 0, "fail": 0, "skip": 0}
    stats[source] = st
    ctx = ths_ctx if source == "ths" else (tdx_ctx if source == "tdx" else None)
    for code in codes:
        try:
            if _collect_one(source, code, count, ctx):
                st["ok"] += 1
                logger.info("[index:%s] %s ok", source, code)
            else:
                st["fail"] += 1
                logger.warning("[index:%s] %s fail（接口无数据或落盘失败）", source, code)
        except Exception as e:
            st["fail"] += 1
            logger.error("[index:%s] %s 异常：%s", source, code, str(e)[:120])
    logger.info("[index:%s] done ok=%d fail=%d", source, st["ok"], st["fail"])


def _run_threads(sources, codes, count, ths_ctx=None, tdx_ctx=None):
    """并发执行各源采集，返回 stats: {source: {ok, fail, skip}}。"""
    stats = {}
    threads = []
    for source in sources:
        t = threading.Thread(target=_collect_source,
                             args=(source, codes, count, stats, ths_ctx, tdx_ctx),
                             name=f"index-{source}")
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    return stats


def collect_index_full(codes=INDEX_CODES, count=FULL_COUNT):
    """全量采集：四源（ths/sina/tx/tdx）并发发号全量拉取并增量合并落盘。

    各源独立连接；单源连接失败记录到 stats 不中断其它源。
    """
    sources = ("ths", "sina", "tx", "tdx")
    logger.info("INDEX_COLLECT_START version=%s mode=full codes=%d count=%d",
                KLINE_VERSION, len(codes), count)
    ths_ctx = tdx_ctx = None
    try:
        ths_ctx = _open_source("ths")
    except Exception as e:
        logger.error("[index:ths] 连接失败：%s", str(e)[:150])
    try:
        tdx_ctx = _open_source("tdx")
    except Exception as e:
        logger.error("[index:tdx] 连接失败：%s", str(e)[:150])
    try:
        stats = _run_threads(sources, codes, count, ths_ctx, tdx_ctx)
    finally:
        _close_source(ths_ctx)
        _close_source(tdx_ctx)
    _summarize("full", stats)
    return stats


def collect_index_incremental(codes=INDEX_CODES, count=INCREMENTAL_COUNT,
                              ths_ctx=None, tdx_ctx=None):
    """当日增量采集：仅 ths+tdx 两源并发发号，拉最近 count 根增量合并落盘。

    ths_ctx/tdx_ctx 非 None 时复用调用方已开连接（每日增量脚本集成路径），
    否则本函数自建连接（独立触发路径）。
    """
    sources = ("ths", "tdx")
    own_ths = ths_ctx is None
    own_tdx = tdx_ctx is None
    logger.info("INDEX_COLLECT_START version=%s mode=incremental codes=%d count=%d",
                KLINE_VERSION, len(codes), count)
    if own_ths:
        try:
            ths_ctx = _open_source("ths")
        except Exception as e:
            logger.error("[index:ths] 连接失败：%s", str(e)[:150])
    if own_tdx:
        try:
            tdx_ctx = _open_source("tdx")
        except Exception as e:
            logger.error("[index:tdx] 连接失败：%s", str(e)[:150])
    try:
        stats = _run_threads(sources, codes, count, ths_ctx, tdx_ctx)
    finally:
        if own_ths:
            _close_source(ths_ctx)
        if own_tdx:
            _close_source(tdx_ctx)
    _summarize("incremental", stats)
    return stats


def _summarize(mode, stats):
    parts = []
    for source in ("ths", "tdx", "sina", "tx"):
        st = stats.get(source)
        if st:
            parts.append(f"{source}: ok={st['ok']} fail={st['fail']}")
    msg = " ".join(parts)
    logger.info("=== run_index_collect done mode=%s %s ===", mode, msg)
    print("INDEX_COLLECT_DONE mode=%s %s" % (mode, msg))


def main():
    _setup_logging()
    ap = argparse.ArgumentParser(
        description=f"指数采集编排 work（四源全量 / THS+TDX 当日增量，version={KLINE_VERSION}）")
    ap.add_argument("--version", action="version", version=f"%(prog)s {KLINE_VERSION}",
                    help="显示版本号并退出")
    ap.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                    help="full=四源（ths/sina/tx/tdx）全量发号；incremental=ths+tdx 两源当日增量")
    ap.add_argument("--count", type=int, default=0,
                    help="单只拉取根数（0=按模式默认：full 600 / incremental 15）")
    args = ap.parse_args()

    count = args.count or (FULL_COUNT if args.mode == "full" else INCREMENTAL_COUNT)
    if args.mode == "full":
        stats = collect_index_full(count=count)
    else:
        stats = collect_index_incremental(count=count)
    return 1 if any(st["fail"] for st in stats.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
