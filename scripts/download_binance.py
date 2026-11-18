#!/usr/bin/env python3
"""Baixa CSVs mensais públicos de klines da Binance, validando o ZIP."""
import argparse
import calendar
import csv
import io
import pathlib
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timezone

BASE = "https://data.binance.vision/data"
FIELDS = ["open_time", "open", "high", "low", "close", "volume",
          "close_time", "quote_volume", "trades", "taker_buy_base",
          "taker_buy_quote", "ignore"]

def previous_months(count):
    today = date.today()
    y, m = today.year, today.month
    for _ in range(count):
        m -= 1
        if m == 0:
            y -= 1
            m = 12
        yield y, m

def download(symbol, interval, market, months, output):
    prefix = "spot" if market == "spot" else "futures/um"
    destination = pathlib.Path(output) / market / symbol / interval
    destination.mkdir(parents=True, exist_ok=True)
    for year, month in reversed(list(previous_months(months))):
        stem = f"{symbol}-{interval}-{year}-{month:02d}"
        url = f"{BASE}/{prefix}/monthly/klines/{symbol}/{interval}/{stem}.zip"
        target = destination / f"{stem}.csv"
        if target.exists() and target.stat().st_size:
            print("EXISTE", target)
            continue
        print("BAIXANDO", url)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "FAST-Trader-Backtest/1.0"})
            with urllib.request.urlopen(req, timeout=60) as response:
                archive = response.read()
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                csv_names = [name for name in z.namelist() if name.lower().endswith(".csv")]
                if len(csv_names) != 1:
                    raise ValueError("ZIP deve conter exatamente um CSV")
                with z.open(csv_names[0]) as source, target.open("w", newline="") as dest:
                    reader = csv.reader(io.TextIOWrapper(source, encoding="utf-8-sig"))
                    writer = csv.writer(dest)
                    writer.writerow(FIELDS)
                    rows = 0
                    for row in reader:
                        if not row or row[0] == "open_time":
                            continue
                        if len(row) != 12:
                            raise ValueError(f"Linha inválida em {stem}: {len(row)} campos")
                        writer.writerow(row)
                        rows += 1
                print("SALVO", target, rows, "candles")
        except (urllib.error.URLError, zipfile.BadZipFile, ValueError, OSError) as exc:
            target.unlink(missing_ok=True)
            print("ERRO", stem, str(exc))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="2h")
    p.add_argument("--market", choices=["spot", "futures"], default="futures")
    p.add_argument("--months", type=int, default=12)
    p.add_argument("--output", default="data/raw")
    args = p.parse_args()
    if args.months < 1 or args.months > 120:
        p.error("--months deve estar entre 1 e 120")
    download(args.symbol.upper(), args.interval, args.market, args.months, args.output)
