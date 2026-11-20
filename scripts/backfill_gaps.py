#!/usr/bin/env python3
"""Fecha lacunas internas dos CSVs gerados por download_binance.py usando a API
REST de klines da Binance (fapi para futuros USD-M, api para spot).

Por que existe: alguns ZIPs mensais publicados na data.binance.vision vem sem barras
de negociação real (ex.: XRPUSDT e SOLUSDT entre 25/02/2026 e 01/03/2022). Uma serie
com buracos desloca SMA/cruzamentos e invalida a comparacao com o TradingView.

  python scripts/backfill_gaps.py --dataset-dir data --market futures --interval 2h
  python scripts/backfill_gaps.py --symbols XRPUSDT,SOLUSDT --dry-run

Somente biblioteca padrao. Respeita o limite de peso da API com pausa adaptativa.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.request

HOSTS = {"futures": "https://fapi.binance.com", "spot": "https://api.binance.com"}
STEP_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
           "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
           "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
           "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000}
LIMIT = 1500
UA = "FAST-Trader-Backtest/1.2"


def to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def api_get(url: str, retries: int = 5) -> bytes:
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}),
                                        timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 429 or exc.code == 418:      # rate limit / ban temporario
                wait = int(exc.headers.get("Retry-After", "5") or 5)
                print(f"  RATE LIMIT {exc.code}, aguardando {wait}s")
                time.sleep(max(5, wait) * attempt)
                last = exc
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Falha em {url}: {last}")


def find_gaps(tms: list[int], step: int, max_gap_ms: int) -> list[tuple[int, int]]:
    """Janelas internas [inicio, fim] em ms onde faltam barras da grade."""
    windows: list[tuple[int, int]] = []
    for prev, cur in zip(tms, tms[1:]):
        if cur - prev == step:
            continue
        start, end = prev + step, cur - step
        if 0 < (end - start) <= max_gap_ms:
            windows.append((start, end))
    return windows


def fetch_window(host: str, symbol: str, interval: str, start: int, end: int) -> list[list]:
    out: list[list] = []
    cursor = start
    while cursor <= end:
        url = (f"{host}/fapi/v1/klines" if "fapi" in host else f"{host}/api/v3/klines")
        url = f"{url}?symbol={symbol}&interval={interval}&startTime={cursor}&endTime={end}&limit={LIMIT}"
        batch = json.loads(api_get(url).decode())
        if not batch:
            break
        out.extend(batch)
        cursor = int(batch[-1][0]) + STEP_MS[interval]
        if len(batch) < LIMIT:
            break
        time.sleep(0.15)
    step = STEP_MS[interval]
    dedup = {int(k[0]): k for k in out if start <= int(k[0]) < end + step}
    return [dedup[k] for k in sorted(dedup)]


def process(symbol: str, path: pathlib.Path, step: int, host: str, interval: str,
            max_gap_ms: int, dry: bool) -> dict | None:
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = [r for r in reader if r]
    if len(rows) < 2:
        return None
    tms = [int(r[1]) for r in rows]
    windows = find_gaps(tms, step, max_gap_ms)
    if not windows:
        return None
    have = set(tms)
    added: list[list[str]] = []
    for start, end in windows:
        try:
            candles = fetch_window(host, symbol, interval, start, end)
        except Exception as exc:  # noqa: BLE001 - simbolo delistado pode nao responder
            print(f"  AVISO   {symbol}: API indisponivel para {to_iso(start)}: {exc}")
            continue
        for k in candles:
            t = int(k[0])
            if t in have:
                continue
            added.append([symbol] + [str(x) for x in k[:11]] + [to_iso(t), to_iso(int(k[6]))])
            have.add(t)
    if not added:
        print(f"  {symbol:12s} {len(windows)} lacuna(s), nenhuma resolvida pela API")
        return {"symbol": symbol, "gaps_before": len(windows), "filled": 0, "candles": len(rows)}
    rows.extend(added)
    rows.sort(key=lambda r: int(r[1]))
    print(f"  {symbol:12s} +{len(added):>4d} candles recuperados "
          f"({len(rows) - len(added)} -> {len(rows)})  "
          f"lacunas: {len(windows)}")
    if not dry:
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)
        tmp.replace(path)
    tms2 = [int(r[1]) for r in rows]
    return {
        "symbol": symbol,
        "gaps_before": len(windows),
        "filled": len(added),
        "candles": len(rows),
        "gaps_after": sum(1 for a, b in zip(tms2, tms2[1:]) if b - a != step),
        "last_open_utc": to_iso(tms2[-1]),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if not dry else None,
        "size_bytes": path.stat().st_size if not dry else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill de lacunas via API da Binance")
    ap.add_argument("--dataset-dir", default="data")
    ap.add_argument("--market", choices=["futures", "spot"], default="futures")
    ap.add_argument("--interval", default="2h")
    ap.add_argument("--symbols", help="subconjunto, separado por virgula")
    ap.add_argument("--max-gap-hours", type=int, default=168,
                    help="ignora buracos maiores que isso (parada estrutural/delisting)")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-manifest", action="store_true")
    args = ap.parse_args()

    if args.interval not in STEP_MS:
        ap.error("intervalo invalido")
    folder = pathlib.Path(args.dataset_dir) / args.market / args.interval
    if not folder.is_dir():
        print(f"Sem dataset em {folder}")
        return 1
    host = HOSTS[args.market]
    wanted = ({s.strip().upper() for s in args.symbols.split(",")}
              if args.symbols else None)
    paths = [p for p in sorted(folder.glob("*.csv"))
             if p.stem not in {"ALL_SYMBOLS"}
             and (wanted is None or p.stem.split("-")[0].upper() in wanted)]
    step = STEP_MS[args.interval]
    print(f"{len(paths)} arquivos | {args.interval} | {args.market} | "
          f"max gap {args.max_gap_hours}h | jobs={args.jobs}{' | DRY-RUN' if args.dry_run else ''}")

    def one(path):
        return process(path.stem.upper(), path, step, host, args.interval,
                       args.max_gap_hours * 3_600_000, args.dry_run)

    with cf.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        results = [r for r in pool.map(one, paths) if r]
    fixed = [r for r in results if r.get("filled")]
    total_added = sum(r["filled"] for r in fixed)
    print(f"\nRESUMO  {len(results)} simbolos com lacuna, {len(fixed)} corrigidos, "
          f"{total_added} candles inseridos")

    manifest = folder / "manifest.json"
    if not args.no_manifest and manifest.exists() and not args.dry_run and fixed:
        data = json.loads(manifest.read_text())
        index = {s["symbol"]: s for s in data.get("symbols", [])}
        for r in fixed:
            s = index.get(r["symbol"])
            if not s:
                continue
            s.update({"candles": r["candles"], "missing_slots": r["gaps_after"],
                      "backfilled": r["filled"], "sha256": r["sha256"],
                      "size_bytes": r["size_bytes"], "last_open_utc": r["last_open_utc"]})
        data["total_candles"] = sum(s["candles"] for s in data["symbols"])
        data["backfill_note"] = (f"{total_added} candles recuperados da API "
                                 f"{host} em {len(fixed)} simbolos")
        data["backfilled_at_utc"] = to_iso(int(time.time() * 1000))
        manifest.write_text(json.dumps(data, indent=2) + "\n")
        print(f"MANIFEST atualizado: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
