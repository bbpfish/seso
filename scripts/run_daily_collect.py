#!/usr/bin/env python3
"""run_daily_collect.py — 每日采集编排入口（快照 -> 状态合并 -> K线多源）

版本号运行期 import 自 kline_env.KLINE_VERSION（--version / 启动日志对外显示，本文件不重复定义）。

用法:
  python run_daily_collect.py --incremental                     # 每日收盘后增量（推荐）
  python run_daily_collect.py                                   # 全量重建（快照全量 + K线全量）
  python run_daily_collect.py --incremental --kline-limit 20    # 试点自检

设计约束（2026-10-07 用户确认, 2026-10-10 更新）:
  1. 交易日统一在入口判断: 非交易日直接跳过整个采集（快照+K线均不执行）
     - collect_stock_status.py / run_kline_multi.py 不再各自做非交易日判断
  2. 快照增量: collect_stock_status.py --incremental
     - baostock 主源直连一次! -> 失败切 Tushare 备源一次
     - 双源失败落空快照 + .degraded 标记, 不阻塞 K线
  3. K线多源: run_kline_multi.py (四源线程化调度: full 四路 THS/Sina 正序+TDX/TX 倒序 /
     incremental 两路 THS 正序+TDX 倒序; --mode full|incremental 默认 incremental, --overwrite 独立叠加)
  4. 快照失败不阻塞K线（容灾隔离）; 失败留痕: {source}_fail.json 清单

工作目录统一定义见 kline_env（SCRIPT_DIR / WORK_ROOT / SCRIPTS_DIR / DATA_DIR /
LOG_DIR / CACHE_DIR），本文件直接引用，不再重复推导；子进程同样 import kline_env
获取一致目录，不再经环境变量注入。
"""
import argparse
import logging
import logging.handlers
import os
import subprocess
import sys
import time

from kline_env import (KLINE_VERSION, LOG_DIR,  # noqa: E402
                       SCRIPTS_DIR)

# load THS credentials (thsdk reads THS_USERNAME/THS_PASSWORD; originally injected by run_kline_ths.sh, now self-contained)
ENV_FILE = SCRIPTS_DIR / "ths_credentials.env"
if ENV_FILE.exists():
    for _line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line.startswith("export "):
            _kv = _line[len("export "):].strip()
            _k, _, _v = _kv.partition("=")
            _v = _v.strip().strip(chr(39)).strip(chr(34))
            if _k and _v:
                os.environ.setdefault(_k, _v)
STATUS_COLLECT = SCRIPTS_DIR / "collect_stock_status.py"
RUN_KLINE = SCRIPTS_DIR / "run_kline_multi.py"

sys.path.insert(0, str(SCRIPTS_DIR))
import trade_calendar  # noqa: E402


def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.handlers.RotatingFileHandler(LOG_DIR / "daily_collect.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"),
                  logging.StreamHandler(sys.stderr)],
    )
    return logging.getLogger("daily_collect")


def run_step(logger, name, cmd):
    logger.info("STEP [%s] start: %s", name, " ".join(cmd))
    t0 = time.time()
    env = os.environ.copy()
    # 子进程目录一致性：子脚本 import kline_env 获取与编排相同的目录常量，无需环境变量注入
    p = subprocess.Popen(cmd, cwd=str(SCRIPTS_DIR), env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    last_lines = []
    for line in p.stdout:
        line = line.rstrip("\n")
        if line:
            logger.info("[%s] %s", name, line)
            last_lines.append(line)
            if len(last_lines) > 40:
                last_lines.pop(0)
    rc = p.wait()
    dur = (time.time() - t0) / 60
    logger.info("STEP [%s] done rc=%d time=%.1fm", name, rc, dur)
    return rc, dur, last_lines


def main():
    ap = argparse.ArgumentParser(description=f"每日采集编排入口（version={KLINE_VERSION}）")
    ap.add_argument("--version", action="version", version=f"%(prog)s {KLINE_VERSION}",
                    help="显示版本号并退出")
    ap.add_argument("--incremental", action="store_true",
                    help="增量模式: 快照只处理最新交易日, K线不指定范围")
    ap.add_argument("--kline-limit", type=int, default=0,
                    help="透传给 run_kline_multi 的 --limit（试点自检）")
    ap.add_argument("--kline-start-date", default="",
                    help="透传给 run_kline_multi 的 --start-date（full 全量起点/回补）")
    ap.add_argument("--kline-end-date", default="",
                    help="透传给 run_kline_multi 的 --end-date")
    ap.add_argument("--use-list", action="store_true",
                    help="透传给 run_kline_multi 的 --use-list")
    args = ap.parse_args()

    logger = setup_logger()
    logger.info("=== daily collect start version=%s mode=%s ===",
                KLINE_VERSION, "incremental" if args.incremental else "full")

    # ---- 交易日统一判断（入口级）: 非交易日直接跳过整个采集 ----
    today = trade_calendar.cn_today().isoformat()
    if not trade_calendar.is_trading_day(today):
        logger.info("今天 %s 非交易日, 直接跳过今日采集（快照+K线均不执行）", today)
        return 0

    # ---- 1) 快照（含状态合并+投影 json） ----
    # collect_stock_status.py 默认即增量；全量重建需显式 --full
    snap_cmd = [sys.executable, str(STATUS_COLLECT)]
    if args.incremental:
        snap_cmd.append("--incremental")
    else:
        snap_cmd.append("--full")
    snap_rc, snap_dur, snap_tail = run_step(logger, "snapshot", snap_cmd)

    degraded = any(("降级" in ln) or ("both_fail" in ln) or ("degraded" in ln)
                   for ln in snap_tail)
    if snap_rc != 0 or degraded:
        logger.warning("快照异常 rc=%d degraded=%s, 不阻塞 K线（容灾隔离）",
                       snap_rc, degraded)

    # ---- 2) K线多源 ----
    kl_cmd = [sys.executable, str(RUN_KLINE),
              "--mode", "incremental" if args.incremental else "full"]
    if args.kline_limit > 0:
        kl_cmd += ["--limit", str(args.kline_limit)]
    if args.kline_start_date:
        kl_cmd += ["--start-date", args.kline_start_date]
    if args.kline_end_date:
        kl_cmd += ["--end-date", args.kline_end_date]
    if args.use_list:
        kl_cmd.append("--use-list")
    kl_rc, kl_dur, kl_tail = run_step(logger, "kline", kl_cmd)

    logger.info("=== daily collect done snap_rc=%d snap_dur=%.1fm kline_rc=%d kline_dur=%.1fm ===",
                snap_rc, snap_dur, kl_rc, kl_dur)
    return 0 if (snap_rc == 0 or degraded) and kl_rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
