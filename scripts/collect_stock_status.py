#!/usr/bin/env python3
"""collect_stock_status.py ：状态采集唯一实现（baostock 主源 + Tushare 备源）

版本号运行期 import 自 kline_env.KLINE_VERSION（--version / 启动日志对外显示，本文件不重复定义）。

用法：
  python collect_stock_status.py                        # 每日增量（默认）
  python collect_stock_status.py --incremental          # 等价（与 --full 互斥）
  python collect_stock_status.py --full                 # 全量（断点续跑）
  python collect_stock_status.py --full --only-snapshots
  python collect_stock_status.py --full --start X --end Y
  python collect_stock_status.py --full --max-days N --hist-out P
  python collect_stock_status.py --refresh-basic        # 增量 + 退市候选自动重查 + 分批轮换重查

退出码：0=成功；1=前置条件失败（无交易日/history 缺失/login 失败等）；
        2=双源均无快照（安全跳过不落盘，下次自动重试，调度层应告警）。

主表存储（分区化，镜像与单文件临时表已下线）：
  主存  data/stock_status_history/<Y>.parquet   纯年份平铺文件，增量只读写当年文件；
        全量重建按年流式 flush 直写分区（不再生成 stock_status_history.tmp.parquet
        单文件临时表），--hist-out 仅用于指定主存目录。

日志：统一 logging 体系（INFO 同时写 logs/collect_stock_status.log 与 stderr，
重试/降级等异常路径为 WARNING 级，RotatingFileHandler 可追溯）。
"""
import argparse
import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path

from kline_env import DATA_DIR, KLINE_VERSION, LOG_DIR, SCRIPT_DIR

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
# 目录统一定义在 kline_env（SCRIPT_DIR/WORK_ROOT/DATA_DIR/LOG_DIR），此处直接引用。
KLINE_DIR = DATA_DIR / "history_kline"
import trade_calendar  # noqa: E402
import baostock_client  # noqa: E402
import status_schema  # noqa: E402
import sources  # noqa: E402

SNAP_DIR = DATA_DIR / "status_snapshots"
BASIC_CACHE = DATA_DIR / "stock_basic_cache.parquet"
# 主存目录：纯年份平铺文件（stock_status_history/2020.parquet ~ 2026.parquet）
HIST_DIR = DATA_DIR / "stock_status_history"
FALLBACK_START = "2020-01-01"

# 沪深口径正则（领域常量单一事实源：status_schema，与 baostock 主源一致）
ASH_PAT = status_schema.ASH_PAT
ASZ_PAT = status_schema.ASZ_PAT

log = logging.getLogger("collect_stock_status")

_LOG_READY = False


def _setup_logging():
    """入口配置日志（收敛模块级副作用，幂等）：INFO 同时写文件与 stderr。

    子模块（baostock_client/sources/status_schema）经 root handler 一并落盘，
    重试/降级记录不再只出现在控制台。
    """
    global _LOG_READY
    if _LOG_READY:
        return
    _LOG_READY = True
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(name)s %(message)s")
    _lfh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "collect_stock_status.log", maxBytes=10 * 1024 * 1024,
        backupCount=5, encoding="utf-8")
    _lfh.setFormatter(fmt)
    root.addHandler(_lfh)
    _sh = logging.StreamHandler(sys.stderr)
    _sh.setFormatter(fmt)
    root.addHandler(_sh)


def _acquire_lock(lock_path):
    """PID 互斥锁：防多采集进程并发覆盖年份文件（P0）。

    优先 fcntl（POSIX）/ msvcrt（Windows）文件锁防"检查-写入"竞态；两者
    均不可用时退化为纯 PID 存活检查（best effort，warning 留痕）。持有进程
    存活时返回 None（调用方中止启动）；否则返回 (kind, f) 供 _release_lock
    释放。锁文件内容为持有者 PID，进程异常退出（未释放）时按过期锁接管。
    """
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        f = lock_path.open("a+")
    except OSError as e:
        log.error("[lock] 打开锁文件失败 %s: %s", lock_path, e)
        return None
    # 过期锁接管：锁文件 PID 对应进程已退出 / 非数字 / 读失败 -> 继续加锁
    try:
        f.seek(0)
        old = f.read().strip()
        if old and old.isdigit():
            pid = int(old)
            try:
                os.kill(pid, 0)
                log.error("[lock] 已有采集进程运行中 pid=%d 锁=%s，拒绝并发启动",
                          pid, lock_path)
                f.close()
                return None
            except OSError:
                log.warning("[lock] 过期锁 pid=%d（进程已退出），接管", pid)
        elif old:
            log.warning("[lock] 锁文件内容非 PID（%r），按过期处理", old)
    except Exception as e:
        log.warning("[lock] 读取锁文件失败（可能被另一进程锁住），继续尝试加锁: %s", e)
    # 文件锁（防竞态）：fcntl 优先（POSIX），msvcrt 兜底（Windows）
    try:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        kind = "fcntl"
    except ImportError:
        try:
            import msvcrt
            f.seek(0)
            f.truncate()
            f.write(str(os.getpid()))
            f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            kind = "msvcrt"
        except ImportError:
            log.warning("[lock] fcntl/msvcrt 均不可用，退化纯 PID 检查（best effort）")
            kind = "pid"
        except OSError as e:
            log.error("[lock] 文件锁冲突（另一进程持有）: %s", e)
            try:
                f.close()
            except Exception:
                pass
            return None
    except OSError as e:
        log.error("[lock] 文件锁冲突（另一进程持有）: %s", e)
        try:
            f.close()
        except Exception:
            pass
        return None
    # 写入当前 PID（锁成功后；文件锁已生效，写失败不影响互斥）
    try:
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
    except OSError as e:
        log.warning("[lock] 写 PID 失败（不影响文件锁）: %s", e)
    return (kind, f)


def _release_lock(lock):
    """释放互斥锁并删除锁文件（正常/异常路径均安全调用）。"""
    if not lock:
        return
    kind, f = lock
    try:
        if kind == "fcntl":
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        elif kind == "msvcrt":
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
    except Exception as e:
        log.warning("[lock] 解锁失败: %s", e)
    try:
        f.close()
    except Exception:
        pass
    try:
        Path(f.name).unlink(missing_ok=True)
    except Exception as e:
        log.warning("[lock] 删除锁文件失败: %s", e)


def trading_days(start=None, end=None):
    """交易日序列（含 start/end，闭区间）。start 缺省为 2020-01-01；
    end 缺省为 cn_today（Asia/Shanghai 口径，防 UTC 容器时区偏移）。"""
    lo = start or FALLBACK_START
    hi = end or trade_calendar.cn_today().isoformat()
    ds = trade_calendar.trading_days(lo, hi)
    if not ds:
        log.error("[trading_days] %s~%s 无交易日", lo, hi)
        return [], lo, hi
    return ds, lo, hi


def stale_basic_codes(cache, exclude=None):
    """退市候选名单：outDate 为空 且（exclude 提供时）不在 exclude 中。

    exclude 传最近 N 日快照 code 并集，筛掉仍在市的股，剩余即疑似退市候选
    （量级小，重查成本可忽略）。basic 缓存只增不改，退市后 outDate 才会出现，
    不重查则 delisted 行永不生成（退市检测失效）。
    """
    out = [c for c, b in cache.items() if not (b.get("outDate") or "")]
    if exclude is not None:
        out = [c for c in out if c not in exclude]
    return out


def stale_rotation_codes(cache, batch=300):
    """陈旧度分批轮换：last_checked 最旧（含空）的 batch 只 outDate 空股。

    与 stale_basic_codes 互补：退市候选靠快照比对即时发现；本函数慢速轮换
    兜底覆盖"在市但长期未重查"的股（名称变更/缓存异常）。batch 建议 300
    （×1s 限流 ≈ 5 分钟），每轮全量/--refresh-basic 推进一批，长期收敛。
    """
    cands = [c for c, b in cache.items() if not (b.get("outDate") or "")]
    cands.sort(key=lambda c: (cache[c].get("last_checked") or "", c))
    return cands[:batch]


def _recent_snapshot_codes(snap_dir, n=5):
    """最近 n 个交易日快照 code 并集（退市候选排除依据；缺/损坏快照自动跳过）。"""
    import pandas as pd
    codes = set()
    files = sorted(Path(snap_dir).glob("*.parquet"))[-n:]
    for f in files:
        try:
            codes.update(pd.read_parquet(f, columns=["code"])["code"].tolist())
        except Exception as e:
            log.warning("[refresh] 快照 %s 读取失败跳过: %s", f, e)
    return codes


def backfill_basic(cache, need, prefix="basic", progress_every=0):
    """批量补查 query_stock_basic 并写缓存（新值覆盖旧值）；返回新增 basic dict 列表。"""
    new_rows = []
    for i, c in enumerate(need, 1):
        try:
            for row in baostock_client.query_basic(c):
                new_rows.append({"code": row[0], "code_name": row[1], "ipoDate": row[2],
                                 "outDate": row[3], "status": row[4],
                                 "last_checked": time.strftime("%Y-%m-%d")})
        except Exception as e:
            log.error("[%s] %s FAIL %s", prefix, c, e)
        if progress_every and i % progress_every == 0:
            log.info("[%s] %d/%d", prefix, i, len(need))
    if new_rows:
        for r in new_rows:
            cache[r["code"]] = r
        # stale 刷新（refresh_basic）时 new_rows 与旧 cache 含相同 code，先更新内存
        # 再全量落盘，避免同 code 新旧两行重复写入 basic 缓存
        status_schema.save_basic_cache(list(cache.values()), BASIC_CACHE)
    return new_rows


def _baostock_snapshot_once(d):
    """主源一次：login 或快照任一失败均视为快照缺失，返回 {}（不重试外提）。"""
    try:
        baostock_client.ensure_login()
    except Exception as e:
        log.warning("[incr] baostock login FAIL %s", e)
        return {}
    try:
        return baostock_client.fetch_snapshot(d)
    except Exception as e:
        log.warning("[incr] %s baostock 快照失败 %s", d, e)
        return {}


def run_incremental(T, snap_dir=None, hist_out=None, refresh_basic=False):
    """每日增量：只处理最新交易日，快照 append + 幂等覆盖。

    降级快照不落盘：Tushare 备源全 tradeStatus=1（无停牌粒度），一旦写入
    SNAP_DIR 会被全量重建断点检查跳过，永久固化降级口径；仅用于本次主表，
    由 assert_normal_threshold 闸门兜底。

    主表写入走 append_partitioned（按年分区文件，镜像已下线）；
    快照主备：baostock 直连一次（login/快照任一失败均视为缺失）；失败切
    Tushare 备源一次；双源均失败则不落盘（下次增量/断点续跑自动重试），
    不阻塞 K 线。

    返回码：0=成功；1=history 缺失；2=双源均无快照（安全跳过）。
    """
    hist_dir = Path(hist_out) if hist_out else HIST_DIR
    snap_dir_p = Path(snap_dir) if snap_dir else SNAP_DIR
    if not hist_dir.exists():
        log.warning("[incr] history %s 不存在，请先跑全量采集", hist_dir)
        return 1
    d = T[-1]

    snap = _baostock_snapshot_once(d)
    degraded = ""
    bj_u, basic_ts = {}, {}
    if not snap:
        log.warning("[incr] %s baostock 快照缺失 -> 切 Tushare 备源", d)
        snap, bj_u, basic_ts = sources.tushare_snapshot(d, ASH_PAT, ASZ_PAT)
        degraded = "tushare" if snap else "both_fail"
    baostock_client.logout()

    snap_dir_p.mkdir(parents=True, exist_ok=True)
    if snap and not degraded:
        status_schema.save_snapshot(snap, snap_dir_p / ("%s.parquet" % d))
    if degraded:
        log.warning("[incr] %s 降级快照 source=%s rows=%d（不落盘，仅本次主表使用）",
                    d, degraded, len(snap))

    cache = status_schema.load_basic_cache(BASIC_CACHE)
    if degraded:
        for code, b in basic_ts.items():
            cache.setdefault(code, b)
        need = []
    else:
        need = [c for c in snap if c not in cache]
    # 退市候选每日自动重查（最近 N 日快照均不含 → 疑似退市，量级小成本低）；
    # --refresh-basic 时追加陈旧度分批轮换兜底（last_checked 最旧 batch 只）
    recent_codes = _recent_snapshot_codes(snap_dir_p)
    delist_cand = sorted(set(stale_basic_codes(cache, exclude=recent_codes)) - set(need))
    rotation = sorted(set(stale_rotation_codes(cache, batch=300))
                      - set(need) - set(delist_cand))
    refresh = delist_cand + (rotation if refresh_basic and not degraded else [])
    if refresh:
        log.info("[incr basic] refresh stale=%d（退市候选=%d 轮换=%d）",
                 len(refresh), len(delist_cand), len(refresh) - len(delist_cand))
        need = sorted(set(need)) + refresh
    if need:
        backfill_basic(cache, need, prefix="incr basic")

    if not snap:
        log.warning("[incr] %s 双源均无快照，不落盘（下次自动重试）", d)
        return 2
    bj_universe = bj_u if degraded else sources.fetch_bj_universe()
    bj_kd = status_schema.bj_kline_dates(KLINE_DIR)
    rows = status_schema.build_day_rows(d, snap, cache, bj_universe, bj_kd)
    # 分区化主表：<hist_dir>/<Y>.parquet 纯年份平铺（只读写当年文件）；镜像已下线
    if rows:
        day_df = status_schema.append_partitioned(hist_dir, rows)
        status_schema.assert_normal_threshold(day_df)
        # 逐年文件统计唯一日期数（年份间日期不相交，求和即总数；避免 concat 全量；
        # year_parts 过滤 4 位数字年份文件名，防杂散 parquet 混入统计）
        # pyarrow 直读 date 列（P2 优化）：避免 pandas DataFrame 构造开销
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
        n_dates = sum(len(pc.unique(pq.read_table(p, columns=["date"])
                                    .column(0).combine_chunks()))
                      for p in status_schema.year_parts(hist_dir))
        log.info("[incr] %s rows_added=%d history_dates=%d degraded=%s",
                 d, len(rows), n_dates, degraded or "-")
    else:
        log.warning("[incr] %s 状态行生成为空，未追加（保留历史）", d)
    return 0


def _run(args):
    hist_dir = Path(args.hist_out) if args.hist_out else HIST_DIR
    snap_dir = Path(args.snap_dir) if args.snap_dir else SNAP_DIR

    T, lo, hi = trading_days(args.start, args.end)
    if not T:
        log.error("[main] 无交易日序列")
        return 1
    if not args.full:
        if args.only_snapshots:
            log.warning("[main] --only-snapshots 需配合 --full；本次按默认增量执行")
        log.info("[main] mode=incremental（默认）T_last=%s", T[-1])
        return run_incremental(T, snap_dir=snap_dir, hist_out=hist_dir,
                               refresh_basic=args.refresh_basic)

    import pandas as pd
    import pyarrow.parquet as pqw

    if args.start:
        T = [d for d in T if d >= args.start]
    if args.end:
        T = [d for d in T if d <= args.end]
    if args.max_days:
        T = T[: args.max_days]
    log.info("[main] mode=full T_days=%d range=%s~%s request=%s~%s",
             len(T), T[0], T[-1], lo, hi)

    try:
        baostock_client.ensure_login()
    except Exception as e:
        log.error("[main] baostock login FAIL %s，中止全量采集", e)
        return 1

    # ---- 阶段1：逐日拉取快照（断点续跑） ----
    snap_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    done = 0
    for i, d in enumerate(T, 1):
        fp = snap_dir / ("%s.parquet" % d)
        if fp.exists():
            try:
                if pqw.ParquetFile(fp).metadata.num_rows > 0:
                    done += 1
                    continue
            except Exception:
                pass
        try:
            snap = baostock_client.fetch_snapshot(d)
        except Exception as e:
            log.error("[snap] %s FAIL %s", d, e)
            continue
        if not snap:
            if fp.exists():
                fp.unlink()
            log.warning("[snap] %s 空结果，不落盘（下次重跑自动补采）", d)
            continue
        status_schema.save_snapshot(snap, fp)
        done += 1
        if i % 50 == 0 or i == len(T):
            el = time.time() - t0
            log.info("[snap] %d/%d done elapsed=%.0fs eta=%.0fs",
                     i, len(T), el, el / i * (len(T) - i))
    log.info("[snap] complete days=%d elapsed=%.0fs", done, time.time() - t0)
    if args.only_snapshots:
        baostock_client.logout()
        return 0

    # ---- 阶段2：query_stock_basic 全市场（ipoDate/outDate，增量补查 + stale 刷新） ----
    snap_files = sorted(snap_dir.glob("*.parquet"))
    all_codes = set()
    for f in snap_files:
        df = pd.read_parquet(f, columns=["code"])
        all_codes.update(df["code"].tolist())
    for f in KLINE_DIR.glob("*.parquet"):
        code = f.stem
        if not (code.isdigit() and len(code) == 6):
            continue
        c = None
        if code.startswith(("5", "6", "9")):
            c = "sh." + code
        elif code.startswith(("0", "1", "2", "3")):
            c = "sz." + code
        # 沪深A股正则过滤：K线目录中的基金(5x)/可转债(1x)/B股(2x,9x)不得混入
        # basic 查询与缓存（其 outDate 到达时会经 delisted 行污染主表）
        if c and (ASH_PAT.match(c) or ASZ_PAT.match(c)):
            all_codes.add(c)
    cache = status_schema.load_basic_cache(BASIC_CACHE)
    need = [c for c in all_codes if c not in cache]
    # 退市候选优先重查（最近 N 日快照均不含 → 疑似退市，量级小成本可忽略）
    # + 陈旧度分批轮换兜底（last_checked 最旧 batch 只，慢速覆盖在市股；
    #   替代旧策略"全部在市股全量重查 ≈ 5000 股 × 1s ≈ 1.3h"）
    recent_codes = _recent_snapshot_codes(snap_dir)
    delist_cand = sorted(set(stale_basic_codes(cache, exclude=recent_codes)) - set(need))
    rotation = sorted(set(stale_rotation_codes(cache, batch=500))
                      - set(need) - set(delist_cand))
    refresh = delist_cand + rotation
    if refresh:
        log.info("[basic] refresh stale=%d（退市候选=%d 轮换=%d）",
                 len(refresh), len(delist_cand), len(rotation))
        need = need + refresh
    log.info("[basic] total_codes=%d cached=%d need=%d refresh=%d",
             len(all_codes), len(cache), len(need), len(refresh))
    t0 = time.time()
    new_rows = backfill_basic(cache, need, prefix="basic", progress_every=500)
    log.info("[basic] complete added=%d elapsed=%.0fs", len(new_rows), time.time() - t0)

    # ---- 阶段3：ak 北交所列表 ----
    bj_universe = sources.fetch_bj_universe()
    bj_kd = status_schema.bj_kline_dates(KLINE_DIR)
    log.info("[bj] ak rows=%d kline_bj=%d", len(bj_universe), len(bj_kd))

    # ---- 阶段4：逐日生成主表（快照在市行 + 北交所行 + 退市行；按年流式 flush 直写分区） ----
    # 单文件临时表已下线：不再写临时单文件，每年缓冲满即 append_partitioned
    # （只读写当年分区，读旧+新 concat 幂等去重）；同步维护 patch/verify 所需
    # 内存态（last_by_code/delisted_keys/days_seen/last_rows），免读分区二次全量。
    hist_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total_rows = 0
    days_seen = set()
    last_by_code = {}
    delisted_keys = set()
    last_day, last_rows = None, None
    cur_year, year_rows = None, []
    for i, d in enumerate(T, 1):
        sfp = snap_dir / ("%s.parquet" % d)
        snap = status_schema.load_snapshot(sfp) if sfp.exists() else {}
        if not snap:
            log.warning("[hist] %s 快照缺失/空，跳过当日（不写空截面）", d)
            continue
        rows = status_schema.build_day_rows(d, snap, cache, bj_universe, bj_kd)
        if not rows:
            continue
        y = d[:4]
        if cur_year is None:
            cur_year = y
        elif y != cur_year:
            status_schema.append_partitioned(hist_dir, year_rows)
            year_rows = []
            cur_year = y
        year_rows.extend(rows)
        total_rows += len(rows)
        days_seen.add(d)
        last_day, last_rows = d, rows
        for r in rows:
            if r[3] == "delisted":
                delisted_keys.add((r[0], r[1]))
            if r[0] > last_by_code.get(r[1], ""):
                last_by_code[r[1]] = r[0]
        if i % 100 == 0:
            el = time.time() - t0
            log.info("[hist] %d/%d buffered=%d written=%d elapsed=%.0fs",
                     i, len(T), len(year_rows), total_rows, el)
    if cur_year is not None:
        status_schema.append_partitioned(hist_dir, year_rows)
    log.info("[hist] main done rows=%d elapsed=%.0fs", total_rows, time.time() - t0)
    if total_rows == 0:
        log.error("[main] 全量重建未生成任何状态行（快照全缺失），未写分区")
        baostock_client.logout()
        return 1

    # ---- 阶段5：退市日快照缺失兜底 + 阈值闸门（分区已由阶段4 流式写入） ----
    patch = status_schema.patch_missing_delisted(last_by_code, delisted_keys, cache)
    if patch:
        status_schema.append_partitioned(hist_dir, patch)
    last_df = pd.DataFrame(last_rows, columns=status_schema.COLS)
    status_schema.assert_normal_threshold(last_df)
    log.info("[partitioned] full rebuild -> %s（%d 行，%d 年文件），镜像已下线（无单文件）",
             hist_dir, total_rows, len(list(status_schema.year_parts(hist_dir))))

    # ---- 阶段6：校验（含快照缺失日汇总告警；latest 截面为阶段4 最后写入日） ----
    log.info("[verify] dates=%d rows=%d range=%s~%s",
             len(days_seen), total_rows, min(days_seen), max(days_seen))
    log.info("[verify] latest=%s rows=%d status=%s st=%s is_new=%d",
             last_day, len(last_df), last_df["status"].value_counts().to_dict(),
             last_df["st_level"].value_counts().to_dict(), int(last_df["is_new"].sum()))
    missing = sorted(set(T) - days_seen)
    if missing:
        log.warning("[verify] %d 个交易日快照缺失未入库: %s%s",
                    len(missing), ", ".join(missing[:5]),
                    " ..." if len(missing) > 5 else "")
    baostock_client.logout()
    return 0


def main():
    ap = argparse.ArgumentParser(description=f"状态采集（快照/主表，version={KLINE_VERSION}）")
    ap.add_argument("--version", action="version", version=f"%(prog)s {KLINE_VERSION}",
                    help="显示版本号并退出")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--incremental", action="store_true", help="每日增量（最新交易日；默认即增量）")
    mode.add_argument("--full", action="store_true", help="全量重建（断点续跑）")
    ap.add_argument("--only-snapshots", action="store_true", help="只拉逐日快照（断点，需 --full）")
    ap.add_argument("--refresh-basic", action="store_true",
                    help="增量时追加陈旧度分批轮换重查（退市候选每日默认自动重查）")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--max-days", type=int, default=0)
    ap.add_argument("--hist-out", default=None,
                    help="主存目录（纯年份平铺文件）")
    ap.add_argument("--snap-dir", default=None)
    args = ap.parse_args()

    _setup_logging()
    log.info("[main] collect_stock_status start version=%s", KLINE_VERSION)

    lock = _acquire_lock(DATA_DIR / "collect_stock_status.lock")
    if lock is None:
        log.error("[main] 互斥锁被占用：另一采集进程正在运行，退出（防并发覆盖年份文件）")
        return 1
    try:
        return _run(args)
    finally:
        _release_lock(lock)


if __name__ == "__main__":
    sys.exit(main())
