#!/usr/bin/env python3
"""Fecha lacunas internas dos CSVs gerados por download_binance.py.

Por que existe: os ZIPs MENSAIS publicados na data.binance.vision vem incompletos em
alguns periodos (ex.: 25-28/02/2022 e 31/03-02/04/2022). Os ZIPs DIARIOS do mesmo
periodo contem as barras corretas, e a API REST confirma o mesmo. Uma serie com buracos
desloca SMA/cruzamentos e invalida a comparacao com o TradingView.

Ordem de resolucao (default):
  1. data.binance.vision - klines diarios do mesmo mercado/intervalo (fonte primaria,
     mesma que gerou o restante da serie)
  2. API REST da Binance (fapi/api) - usada so para o que restar; pode devolver
     HTTP 451 "restricted location" dependendo do IP de saida

  python scripts/backfill_gaps.py --dataset-dir data --market futures --interval 2h
  python scripts/backfill_gaps.py --symbols XRPUSDT,SOLUSDT --dry-run
  python scripts/backfill_gaps.py --dataset-dir /home/ubuntu/bulk/data --source vision

Somente biblioteca padrao.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import io
import json
import pathlib
import time
import urllib.error
import urllib.request
import zipfile

VISION = "https://data.binance.vision"
HOSTS = {"futures": "https://fapi.binance.com", "spot": "https://api.binance.com"}
STEP_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
           "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
           "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
           "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000}
LIMIT = 1500
UA = "FAST-Trader-Backtest/1.3"
OUT_HEADER = ["symbol", "open_time", "open", "high", "low", "close", "volume",
              "close_time", "quote_volume", "trades", "taker_buy_base",
              "taker_buy_quote", "open_utc", "close_utc"]


def to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def http_get(url: str, retries: int = 4) -> bytes:
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}),
                                        timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise
            if exc.code in (429, 418):
                wait = int(exc.headers.get("Retry-After", "5") or 5)
                print(f"  RATE LIMIT {exc.code}, aguardando {wait}s")
                time.sleep(max(5, wait) * attempt)
            last = exc
            time.sleep(1.2 * attempt)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Falha em {url}: {last}")


def daily_prefix(market: str) -> str:
    return ("data/spot/daily/klines" if market == "spot"
            else "data/futures/um/daily/klines")


def vision_day(symbol: str, market: str, interval: str, day: str) -> list[list[str]]:
    """Le as barras de um ZIP diario da Vision; [] quando nao ha arquivo daquele dia."""
    url = f"{VISION}/{daily_prefix(market)}/{symbol}/{interval}/{symbol}-{interval}-{day}.zip"
    try:
        payload = http_get(url, retries=2)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"ZIP inesperado: {url}")
        with archive.open(names[0]) as source:
            rows = [r for r in csv.reader(io.TextIOWrapper(source, encoding="utf-8-sig"))
                    if r and r[0] != "open_time" and len(r) == 12]
    return rows


def api_klines(host: str, symbol: str, interval: str, start: int, end: int) -> list[list]:
    path = "fapi/v1/klines" if "fapi" in host else "api/v3/klines"
    out: list[list] = []
    cursor = start
    while cursor <= end:
        url = (f"{host}/{path}?symbol={symbol}&interval={interval}"
               f"&startTime={cursor}&endTime={end}&limit={LIMIT}")
        batch = json.loads(http_get(url).decode())
        if not batch:
            break
        out.extend(batch)
        cursor = int(batch[-1][0]) + STEP_MS[interval]
        if len(batch) < LIMIT:
            break
        time.sleep(0.15)
    return out


def find_gaps(tms: list[int], step: int, max_gap_ms: int) -> list[tuple[int, int]]:
    """Janelas internas [inicio, fim] (ms) com barras ausentes da grade."""
    windows: list[tuple[int, int]] = []
    for prev, cur in zip(tms, tms[1:]):
        if cur - prev == step:
            continue
        start, end = prev + step, cur - step
        if 0 < (end - start) <= max_gap_ms:
            windows.append((start, end))
    return windows


def candidate_days(windows: list[tuple[int, int]]) -> list[str]:
    days: set[str] = set()
    for start, end in windows:
        d = dt.datetime.fromtimestamp(start / 1000, tz=dt.timezone.utc).date()
        last = dt.datetime.fromtimestamp(end / 1000, tz=dt.timezone.utc).date()
        while d <= last:
            days.add(d.isoformat())
            d += dt.timedelta(days=1)
    return sorted(days)


def process(symbol: str, path: pathlib.Path, step: int, market: str, interval: str,
            max_gap_ms: int, source: str, host: str, dry: bool) -> dict | None:
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = [r for r in reader if r]
    if len(rows) < 2:
        return None
    have = {int(r[1]) for r in rows}
    tms = sorted(have)
    windows = find_gaps(tms, step, max_gap_ms)
    if not windows:
        return None
    low, high = tms[0], tms[-1]
    added: dict[int, list[str]] = {}

    def slot(t: int) -> bool:
        return t not in have and low <= t <= high and t % step == 0

    if source in ("vision", "both"):
        for day in candidate_days(windows):
            for row in vision_day(symbol, market, interval, day):
                t = int(row[0])
                if slot(t):
                    added[t] = [symbol] + row[:11] + [to_iso(t), to_iso(int(row[6]))]
            time.sleep(0.02)

    if source in ("api", "both") and len(added) < sum(
            (e - s) // step + 1 for s, e in windows):
        for start, end in windows:
            try:
                for k in api_klines(host, symbol, interval, start, end):
                    t = int(k[0])
                    if slot(t) and t not in added:
                        added[t] = [symbol] + [str(x) for x in k[:11]] + \
                                   [to_iso(t), to_iso(int(k[6]))]
            except Exception as exc:  # noqa: BLE001 - 451 / simbolo delistado
                print(f"  AVISO   {symbol}: API indisponivel em {to_iso(start)} "
                      f"({str(exc)[:40]})")

    before = len(rows)
    still = find_gaps(sorted(have | set(added)), step, max_gap_ms)
    if not added:
        print(f"  {symbol:14s} {len(windows)} lacuna(s) sem resolucao "
              f"({sum((e - s) // step + 1 for s, e in windows)} slots)")
        return {"symbol": symbol, "gaps_before": len(windows), "filled": 0,
                "candles": before, "gaps_after": len(still)}
    print(f"  {symbol:14s} +{len(added):>4d} candles  ({before} -> {before + len(added)})  "
          f"lacunas {len(windows)} -> {len(still)}")
    if not dry:
        rows.extend(added[t] for t in sorted(added))
        rows.sort(key=lambda r: int(r[1]))
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header if header == OUT_HEADER else header)
            writer.writerows(rows)
        tmp.replace(path)
    tms2 = [int(r[1]) for r in rows]
    return {
        "symbol": symbol, "gaps_before": len(windows), "filled": len(added),
        "candles": len(tms2), "gaps_after": len(find_gaps(tms2, step, max_gap_ms)),
        "last_open_utc": to_iso(tms2[-1]),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if not dry else None,
        "size_bytes": path.stat().st_size if not dry else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill de lacunas (Vision diarios + API)")
    ap.add_argument("--dataset-dir", default="data")
    ap.add_argument("--market", choices=["futures", "spot"], default="futures")
    ap.add_argument("--interval", default="2h")
    ap.add_argument("--symbols", help="subconjunto, separado por virgula")
    ap.add_argument("--source", choices=["vision", "api", "both"], default="vision",
                    help="vision = ZIPs diarios da Binance Vision (default)")
    ap.add_argument("--max-gap-hours", type=int, default=336,
                    help="ignora buracos maiores que isso (delisting / pausa estrutural)")
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-manifest", action="store_true")
    args = ap.parse_args()

    folder = pathlib.Path(args.dataset_dir) / args.market / args.interval
    if not folder.is_dir():
        print(f"Sem dataset em {folder}")
        return 1
    wanted = ({s.strip().upper() for s in args.symbols.split(",")} if args.symbols else None)
    paths = [p for p in sorted(folder.glob("*.csv"))
             if p.stem.upper() != "ALL_SYMBOLS"
             and (wanted is None or p.stem.upper() in wanted)]
    step = STEP_MS[args.interval]
    host = HOSTS[args.market]
    print(f"{len(paths)} arquivos | {args.interval} | {args.market} | fonte={args.source} | "
          f"max gap {args.max_gap_hours}h | jobs={args.jobs}{' | DRY-RUN' if args.dry_run else ''}")

    def one(path):
        return process(path.stem.upper(), path, step, args.market, args.interval,
                       args.max_gap_hours * 3_600_000, args.source, host, args.dry_run)

    with cf.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        results = [r for r in pool.map(one, paths) if r]
    fixed = [r for r in results if r["filled"]]
    added = sum(r["filled"] for r in fixed)
    left = sum(r["gaps_after"] for r in results)
    print(f"\nRESUMO  {len(results)} simbolos com lacuna | {len(fixed)} corrigidos | "
          f"{added} candles inseridos | {left} lacunas permanecem")

    manifest = folder / "manifest.json"
    if not args.no_manifest and manifest.exists() and not args.dry_run and fixed:
        data = json.loads(manifest.read_text())
        index = {s["symbol"]: s for s in data.get("symbols", [])}
        for r in fixed:
            s = index.get(r["symbol"])
            if not s:
                continue
            s.update({"candles": r["candles"], "missing_slots": r["gaps_after"],
                      "backfilled": s.get("backfilled", 0) + r["filled"],
                      "backfill_source": args.source, "sha256": r["sha256"],
                      "size_bytes": r["size_bytes"], "last_open_utc": r["last_open_utc"]})
        data["total_candles"] = sum(s["candles"] for s in data["symbols"])
        data["remaining_gaps"] = left
        data["backfill_note"] = (f"{added} candles de publicacao incompleta recuperados de "
                                 f"{VISION} (klines diarios) em {len(fixed)} simbolos")
        data["backfilled_at_utc"] = to_iso(int(time.time() * 1000))
        manifest.write_text(json.dumps(data, indent=2) + "\n")
        print(f"MANIFEST atualizado: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
