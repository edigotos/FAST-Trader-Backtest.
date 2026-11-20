#!/usr/bin/env python3
"""Baixa klines publicos da Binance (data.binance.vision), valida SHA-256 e consolida
uma serie continua por simbolo.

Somente biblioteca padrao (Python 3.10+).

Saidas:
  <raw-dir>/<market>/<SYMBOL>/<interval>/<stem>.csv    arquivos por mes/dia (gitignore)
  <dataset-dir>/<market>/<interval>/<SYMBOL>.csv       serie continua por par (versionavel)
  <dataset-dir>/<market>/<interval>/ALL_SYMBOLS.csv    todas as series juntas
  <dataset-dir>/<market>/<interval>/manifest.json      contagens, periodos e hashes

Exemplos:
  # os 5 pares do estudo, historico completo
  python scripts/download_binance.py --symbols BTCUSDT,ETHUSDT,ALICEUSDT,XRPUSDT,SOLUSDT \
      --interval 2h --market futures --full

  # todos os perpetuos USDT listados na Binance, 12 meses, 16 downloads simultaneos,
  # cache em ZIP (ocupa ~7x menos disco) e pacote final em tar.gz
  python scripts/download_binance.py --all-usdt --interval 2h --months 12 --cache zip \
      --jobs 16 --no-combined --archive data/binance-futures-2h-12m.tar.gz
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import pathlib
import tarfile
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

VISION = "https://data.binance.vision"
S3_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
EXCHANGE_INFO = {
    "futures": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
}
UA = "FAST-Trader-Backtest/1.2"

FIELDS = ["open_time", "open", "high", "low", "close", "volume",
          "close_time", "quote_volume", "trades", "taker_buy_base",
          "taker_buy_quote", "ignore"]
OUT_FIELDS = ["symbol", "open_time", "open", "high", "low", "close", "volume",
              "close_time", "quote_volume", "trades", "taker_buy_base",
              "taker_buy_quote", "open_utc", "close_utc"]
STEP_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
           "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
           "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
           "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000}
NO_SUFFIX = {"ignore": None}


# ------------------------------------------------------------------ HTTP/S3 ---
def http_get(url: str, retries: int = 4) -> bytes:
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=90) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
            if attempt < retries:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"Falha ao baixar {url}: {last}")


def _ns(root) -> str:
    return root.tag.split("}")[0] + "}" if "}" in root.tag else ""


def list_s3(prefix: str, want: str = "Contents") -> list[str]:
    """Lista chaves (Contents) ou sub-prefixos (CommonPrefixes) sob um prefixo S3."""
    out: list[str] = []
    marker = ""
    while True:
        url = f"{S3_ENDPOINT}?delimiter=/&prefix={prefix}&max-keys=1000"
        if marker:
            url += f"&marker={marker}"
        root = ET.fromstring(http_get(url))
        ns = _ns(root)
        for node in root.findall(f"{ns}{want}"):
            text = node.findtext(f"{ns}Key") if want == "Contents" else node.findtext(f"{ns}Prefix")
            if text:
                out.append(text.strip())
        truncated = (root.findtext(f"{ns}IsTruncated") or "false").lower()
        if truncated == "true" and out:
            marker = root.findtext(f"{ns}NextMarker") or out[-1]
        else:
            break
    return out


def universe(market: str, quote: str = "USDT") -> list[str]:
    """Simbolos com pasta de klines publicada na Binance Vision."""
    prefix = "data/spot/klines/" if market == "spot" else "data/futures/um/monthly/klines/"
    keys = list_s3(prefix, want="CommonPrefixes")
    syms = [k.rstrip("/").split("/")[-1] for k in keys]
    return sorted(s for s in syms if quote is None or s.endswith(quote))


def trading_perps(quote: str = "USDT") -> list[str]:
    """Perpetuos do mercado atual listados (status TRADING) na API da Binance."""
    data = json.loads(http_get(EXCHANGE_INFO["futures"]).decode())
    return sorted(s["symbol"] for s in data["symbols"]
                  if s.get("contractType") == "PERPETUAL"
                  and s.get("quoteAsset") == quote
                  and s.get("status") == "TRADING")


# ----------------------------------------------------------------- helpers ----
def to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def verify_checksum(zip_url: str, payload: bytes) -> bool:
    try:
        text = http_get(zip_url + ".CHECKSUM", retries=2).decode().strip().split()[0].lower()
    except Exception:  # noqa: BLE001 - checksum e opcional
        return True
    if len(text) == 64:                      # sha256 (formato atual da Vision)
        return sha256_bytes(payload) == text
    if len(text) == 40:                      # sha1 (arquivos mais antigos)
        return hashlib.sha1(payload).hexdigest() == text
    return True


def rows_from_zip(payload: bytes, stem: str) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError("ZIP deve conter exatamente um CSV")
        with archive.open(names[0]) as source:
            rows = []
            for row in csv.reader(io.TextIOWrapper(source, encoding="utf-8-sig")):
                if not row or row[0] == "open_time":
                    continue
                if len(row) != 12:
                    raise ValueError(f"Linha invalida em {stem}: {len(row)} campos")
                rows.append(row)
            return rows


def write_csv(target: pathlib.Path, header: list[str], rows: list[list[str]]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="") as dest:
        writer = csv.writer(dest)
        writer.writerow(header)
        writer.writerows(rows)


def fetch_rows(key: str, cache_dir: pathlib.Path, cache_mode: str,
               verify: bool, refresh: bool) -> list[list[str]]:
    """Retorna as linhas OHLCV de um .zip, usando o cache local quando possivel."""
    stem = os.path.basename(key)[:-4]
    if cache_mode == "csv":
        target = cache_dir / f"{stem}.csv"
        if target.exists() and target.stat().st_size and not refresh:
            with target.open(newline="") as fh:
                reader = csv.reader(fh)
                next(reader, None)
                return [r for r in reader if r]
    else:
        target = cache_dir / f"{stem}.zip"
        if target.exists() and target.stat().st_size and not refresh:
            return rows_from_zip(target.read_bytes(), stem)
    if cache_mode == "none":
        payload = http_get(f"{VISION}/{key}")
        if verify and not verify_checksum(f"{VISION}/{key}", payload):
            raise RuntimeError(f"SHA-256 nao confere: {stem}")
        return rows_from_zip(payload, stem)
    tmp = http_get(f"{VISION}/{key}")
    if verify and not verify_checksum(f"{VISION}/{key}", tmp):
        raise RuntimeError(f"SHA-256 nao confere: {stem}")
    rows = rows_from_zip(tmp, stem)
    cache_dir.mkdir(parents=True, exist_ok=True)
    if cache_mode == "csv":
        write_csv(target, FIELDS, rows)
    else:
        target.write_bytes(tmp)
    return rows


# ---------------------------------------------------------------- download ----
def available_keys(symbol: str, interval: str, market: str, months: int | None):
    """Mensais publicados + diarios apenas dos meses da janela ainda sem mensal."""
    prefix = "data/spot/klines" if market == "spot" else "data/futures/um/monthly/klines"
    daily_prefix_base = prefix.replace("monthly", "daily")
    monthly = [k for k in list_s3(f"{prefix}/{symbol}/{interval}/")
               if f"-{interval}-" in k and k.endswith(".zip")]
    if not monthly:
        return [], []
    published_all = {os.path.basename(k)[-11:-4] for k in monthly}      # YYYY-MM
    if months is not None:
        monthly = monthly[-months:]
    window_start = os.path.basename(monthly[0])[-11:-4]
    today = dt.date.today()
    daily: list[str] = []
    for back in range(0, 2):
        month = (today.replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m") if back else \
            today.strftime("%Y-%m")
        if month in published_all or month < window_start:
            continue
        stem = f"{symbol}-{interval}-"
        for key in list_s3(f"{daily_prefix_base}/{symbol}/{interval}/{stem}{month}-"):
            if key.endswith(".zip"):
                daily.append(key)
    return monthly, sorted(daily)


def download(symbol: str, interval: str, market: str, months: int | None,
             raw_root: pathlib.Path, dataset_root: pathlib.Path,
             jobs: int, verify: bool, refresh: bool, cache_mode: str) -> dict | None:
    monthly, daily = available_keys(symbol, interval, market, months)
    if not monthly and not daily:
        return None
    cache_dir = raw_root / market / symbol / interval
    keys = monthly + daily
    rows: dict[int, list[str]] = {}
    failed: list[str] = []
    quiet = jobs > 1

    def worker(key):
        try:
            return key, fetch_rows(key, cache_dir, cache_mode, verify, refresh), None
        except Exception as exc:  # noqa: BLE001 - um arquivo ruim nao derruba o simbolo
            return key, [], str(exc)

    with cf.ThreadPoolExecutor(max_workers=jobs) as pool:
        for key, chunk, err in pool.map(worker, keys):
            if err:
                failed.append(f"{os.path.basename(key)}: {err}")
                print(f"  ERRO    {os.path.basename(key)}: {err}")
                continue
            if not quiet:
                print(f"  BAIXO   {os.path.basename(key)[:-4]} ({len(chunk)} candles)")
            for row in chunk:
                try:
                    rows[int(row[0])] = row
                except (ValueError, IndexError):
                    continue
    if not rows:
        return None

    ordered = [rows[k] for k in sorted(rows)]
    out_rows = [[symbol] + r[:11] + [to_iso(int(r[0])), to_iso(int(r[6]))] for r in ordered]
    dataset_dir = dataset_root / market / interval
    target = dataset_dir / f"{symbol}.csv"
    write_csv(target, OUT_FIELDS, out_rows)

    step = STEP_MS.get(interval)
    gaps = 0
    if step:
        prev = None
        for row in ordered:
            t = int(row[0])
            if prev is not None and t - prev != step:
                gaps += 1
            prev = t
    info = {
        "symbol": symbol, "market": market, "interval": interval,
        "file": f"{market}/{interval}/{symbol}.csv",
        "candles": len(ordered),
        "first_open_utc": to_iso(int(ordered[0][0])),
        "last_open_utc": to_iso(int(ordered[-1][0])),
        "last_close_utc": to_iso(int(ordered[-1][6])),
        "first_open_ms": int(ordered[0][0]),
        "last_open_ms": int(ordered[-1][0]),
        "missing_slots": gaps,
        "last_close": float(ordered[-1][4]),
        "raw_files": len(keys),
        "failed_files": failed,
        "size_bytes": target.stat().st_size,
        "sha256": sha256_bytes(target.read_bytes()),
    }
    flag = "  LACUNAS" if gaps else "  OK     "
    print(f"{flag} {symbol:14s} {info['candles']:>6d} candles  "
          f"{info['first_open_utc'][:10]} -> {info['last_open_utc'][:10]}  "
          f"gaps={gaps}  {target.stat().st_size / 1e6:.1f} MB")
    return info


def write_combined(infos: list[dict], dataset_root: pathlib.Path, market: str,
                   interval: str) -> None:
    dataset_dir = dataset_root / market / interval
    rows: list[list[str]] = []
    for info in infos:
        with (dataset_dir.parent.parent / info["file"]).open(newline="") as fh:
            reader = csv.reader(fh)
            next(reader, None)
            rows.extend([r for r in reader if r])
    rows.sort(key=lambda r: (int(r[1]), r[0]))
    combined = dataset_dir / "ALL_SYMBOLS.csv"
    write_csv(combined, OUT_FIELDS, rows)
    print(f"\nCONSOLIDADO {combined} | {len(rows)} candles | {len(infos)} simbolos")


def write_manifest(infos: list[dict], dataset_root: pathlib.Path, market: str,
                   interval: str, combined_rows: int | None,
                   trading: set[str] | None = None) -> pathlib.Path:
    dataset_dir = dataset_root / market / interval
    infos = sorted(infos, key=lambda i: i["symbol"])
    if trading is not None:
        for info in infos:
            info["trading"] = info["symbol"] in trading
    payload = {
        "generated_at_utc": to_iso(int(time.time() * 1000)),
        "source": f"{VISION}/",
        "market": market,
        "interval": interval,
        "columns": OUT_FIELDS,
        "symbols_collected": len(infos),
        "symbols_trading": (sum(1 for i in infos if i.get("trading"))
                            if trading is not None else None),
        "total_candles": sum(i["candles"] for i in infos),
        "combined_candles": combined_rows,
        "symbols": infos,
    }
    manifest = dataset_dir / "manifest.json"
    manifest.write_text(json.dumps(payload, indent=2) + "\n")
    return manifest


def make_archive(infos: list[dict], dataset_root: pathlib.Path, market: str,
                 interval: str, archive: str) -> pathlib.Path:
    path = pathlib.Path(archive)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as tar:
        for info in infos:
            file = dataset_root / market / interval / f"{info['symbol']}.csv"
            if file.exists():
                tar.add(file, arcname=f"{info['symbol']}.csv")
    print(f"\nPACOTE {path} | {path.stat().st_size / 1e6:.1f} MB")
    return path


# --------------------------------------------------------------------- main ---
def main() -> int:
    parser = argparse.ArgumentParser(description="Downloader de klines da Binance Vision")
    which = parser.add_mutually_exclusive_group()
    which.add_argument("--symbol")
    which.add_argument("--symbols", help="lista separada por virgula")
    which.add_argument("--symbols-file", help="arquivo com um simbolo por linha")
    which.add_argument("--all-vision", action="store_true",
                       help="todos os simbolos com pasta de klines na Binance Vision")
    which.add_argument("--all-usdt", action="store_true",
                       help="todos os perpetuos USDT com status TRADING na Binance")
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--months", type=int, default=None,
                        help="janela em meses calendarios (default: historico completo)")
    window.add_argument("--full", action="store_true", help="historico completo publicado")
    parser.add_argument("--interval", default="2h")
    parser.add_argument("--market", choices=["spot", "futures"], default="futures")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--dataset-dir", default="data")
    parser.add_argument("--cache", choices=["csv", "zip", "none"], default="csv",
                        help="formato do cache em --raw-dir (zip ocupa ~7x menos)")
    parser.add_argument("--jobs", type=int, default=6,
                        help="downloads simultaneos por simbolo")
    parser.add_argument("--symbol-workers", type=int, default=1,
                        help="simbolos processados em paralelo")
    parser.add_argument("--limit", type=int, default=0, help="processa apenas N simbolos")
    parser.add_argument("--no-combined", action="store_true")
    parser.add_argument("--no-verify", action="store_true", help="omite a checagem de SHA-256")
    parser.add_argument("--archive", default=None, help="gera tar.gz com os CSVs por simbolo")
    parser.add_argument("--refresh", action="store_true", help="ignora o cache local")
    args = parser.parse_args()

    if args.months is not None and not 1 <= args.months <= 240:
        parser.error("--months deve estar entre 1 e 240")
    if args.interval not in STEP_MS:
        parser.error(f"intervalo invalido: {args.interval} "
                     f"(validos: {', '.join(STEP_MS)})")

    if args.all_usdt:
        symbols = trading_perps("USDT")
        print(f"{len(symbols)} perpetuos USDT TRADING na Binance")
    elif args.all_vision:
        symbols = universe(args.market, "USDT")
        print(f"{len(symbols)} simbolos USDT com klines na Binance Vision")
    elif args.symbols_file:
        symbols = [ln.strip().upper() for ln in
                   pathlib.Path(args.symbols_file).read_text().splitlines() if ln.strip()]
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    elif args.symbol:
        symbols = [args.symbol.upper()]
    else:
        parser.error("informe --symbol, --symbols, --symbols-file, --all-vision ou --all-usdt")
    if args.limit:
        symbols = symbols[:args.limit]

    raw_root = pathlib.Path(args.raw_dir)
    dataset_root = pathlib.Path(args.dataset_dir)
    raw_root.mkdir(parents=True, exist_ok=True)
    months = None if (args.full or args.months is None) else args.months
    started = time.time()
    print(f"{len(symbols)} simbolos | {args.interval} | {args.market} | "
          f"{'historico completo' if months is None else f'{months} meses'} | "
          f"jobs={args.jobs} x simbolos={args.symbol_workers} cache={args.cache}\n")

    infos: list[dict] = []
    missing: list[str] = []
    lock = threading.Lock()
    total = len(symbols)
    outer = max(1, args.symbol_workers)
    inner = max(1, args.jobs)
    print(f"conexoes simultaneas ~ {outer * inner}\n")

    def handle(index, symbol):
        try:
            info = download(symbol, args.interval, args.market, months, raw_root,
                            dataset_root, inner, not args.no_verify,
                            args.refresh, args.cache)
        except Exception as exc:  # noqa: BLE001 - nao derrubar a coleta inteira
            print(f"  ERRO    {symbol}: {exc}")
            info = None
        with lock:
            if info:
                infos.append(info)
            else:
                missing.append(symbol)
                print(f"  SEMDADO {symbol} ({index}/{total})")
            done = len(infos) + len(missing)
            if done % 25 == 0 or done == total:
                rate = done / max(1, time.time() - started)
                eta = (total - done) / max(0.01, rate) / 60
                print(f"  -- {done}/{total} simbolos, "
                      f"{sum(i['candles'] for i in infos):,} candles, ETA {eta:.0f} min",
                      flush=True)

    if outer == 1:
        for index, symbol in enumerate(symbols, 1):
            handle(index, symbol)
    else:
        with cf.ThreadPoolExecutor(max_workers=outer) as pool:
            futures = [pool.submit(handle, index, symbol)
                       for index, symbol in enumerate(symbols, 1)]
            for fut in cf.as_completed(futures):
                fut.result()

    combined_rows = None
    if infos:
        if not args.no_combined and len(infos) <= 40:
            write_combined(infos, dataset_root, args.market, args.interval)
        trading = None
        try:
            trading = set(trading_perps("USDT"))
        except Exception as exc:  # noqa: BLE001
            print(f"  AVISO   exchangeInfo indisponivel ({exc}); manifest sem status")
        manifest = write_manifest(infos, dataset_root, args.market, args.interval,
                                  combined_rows, trading)
        if args.archive:
            make_archive(infos, dataset_root, args.market, args.interval, args.archive)
        print(f"\nRESUMO  {len(infos)} simbolos ok, {len(missing)} sem dados, "
              f"{sum(i['candles'] for i in infos):,} candles, "
              f"{time.time() - started:,.0f}s")
        print(f"MANIFEST {manifest}")
        if missing:
            print("SEM DADOS: " + ", ".join(missing[:20]) + (" ..." if len(missing) > 20 else ""))
        return 0
    print("Nenhum dado obtido.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
