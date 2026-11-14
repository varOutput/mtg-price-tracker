#!/usr/bin/env python3
"""
One-off backfill of data/mtgjson_history/ from MTGJSON's AllPrices file,
which holds roughly the last 90 days of prices (including Card Kingdom
buylist). Writes one data/mtgjson_history/<YYYY-MM-DD>.csv.gz per day, in
exactly the format fetch_mtgjson_prices.py writes daily.

Days that already have a file are left alone (the daily job's own
snapshots win), so this is safe to re-run.

AllPrices is ~150 MB gzipped / ~1.3 GB of JSON, so it's streamed one card
at a time with the standard library instead of loaded whole; per-day rows
are spooled to temp files and written out at the end.

Usage:
    python scripts/backfill_mtgjson_history.py
    python scripts/backfill_mtgjson_history.py --allprices AllPrices.json.gz  # use a local copy
    python scripts/backfill_mtgjson_history.py --dry-run                      # report only
"""

import argparse
import gzip
import json
import logging
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_mtgjson_prices import (  # noqa: E402
    HISTORY_DIR, IDENTIFIERS_URL, MIN_BUYLIST_ROWS, MIN_ROWS, PRICE_COLUMNS,
    USER_AGENT, download, load_scryfall_ids, write_flat_snapshot,
)

ALLPRICES_URL = "https://mtgjson.com/api/v5/AllPrices.json.gz"
READ_CHUNK = 1 << 20

log = logging.getLogger("backfill_mtgjson_history")


def iter_price_entries(path: Path):
    """
    Stream (uuid, entry) pairs from the top-level "data" object of an MTGJSON
    price file without loading the whole document.
    """
    decoder = json.JSONDecoder()
    with gzip.open(path, "rt", encoding="utf-8") as f:
        buf = ""
        # Read until we've seen `"data"` and the `{` that opens its object.
        while '"data"' not in buf or "{" not in buf[buf.index('"data"'):]:
            chunk = f.read(READ_CHUNK)
            if not chunk:
                raise RuntimeError("No top-level \"data\" object found")
            buf += chunk
        pos = buf.index("{", buf.index('"data"') + len('"data"')) + 1
        eof = False

        def more() -> bool:
            nonlocal buf, pos, eof
            if eof:
                return False
            chunk = f.read(READ_CHUNK)
            if not chunk:
                eof = True
                return False
            buf = buf[pos:] + chunk  # drop consumed text so buf stays small
            pos = 0
            return True

        while True:
            # Skip whitespace / commas between members.
            while True:
                while pos < len(buf) and buf[pos] in " \t\r\n,":
                    pos += 1
                if pos < len(buf) or not more():
                    break
            if pos >= len(buf):
                raise RuntimeError("Unexpected end of file inside \"data\"")
            if buf[pos] == "}":
                return  # end of "data"
            while True:
                try:
                    key, p = decoder.raw_decode(buf, pos)
                    while p < len(buf) and buf[p] in " \t\r\n":
                        p += 1
                    if p >= len(buf) or buf[p] != ":":
                        raise json.JSONDecodeError("expected ':'", buf, p)
                    p += 1
                    while p < len(buf) and buf[p] in " \t\r\n":
                        p += 1
                    # raw_decode raises if the value is cut off at the end of
                    # the buffer; read more and retry the whole member.
                    value, p = decoder.raw_decode(buf, p)
                    break
                except json.JSONDecodeError:
                    if not more():
                        raise
            pos = p
            yield key, value


def explode_by_date(entry: dict, unknown: set) -> dict:
    """{format: {provider: {side: {finish: {date: price}}}}} -> {date: {column: price}}"""
    out: dict = defaultdict(dict)
    for _fmt, providers in entry.items():
        for provider, body in providers.items():
            for side in ("retail", "buylist"):
                for finish, series in (body.get(side) or {}).items():
                    col = f"{provider}_{side}_{finish}"
                    if col not in PRICE_COLUMNS:
                        unknown.add(col)
                    for date, value in (series or {}).items():
                        if value is not None:
                            out[date][col] = round(float(value), 4)
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--allprices", type=Path, help="Local AllPrices.json.gz (default: download)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report; write nothing")
    args = parser.parse_args()

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        allprices = args.allprices or download(session, ALLPRICES_URL, tmp / "AllPrices.json.gz")
        scryfall_ids = load_scryfall_ids(
            download(session, IDENTIFIERS_URL, tmp / "cardIdentifiers.csv.gz"))
        log.info(f"Loaded {len(scryfall_ids):,} uuid -> scryfallId mappings")

        # Pass 1: stream cards, spool each day's rows to its own JSONL file.
        log.info("Streaming AllPrices...")
        spools: dict = {}
        unknown: set = set()
        cards = 0
        try:
            for uuid, entry in iter_price_entries(allprices):
                for date, vals in explode_by_date(entry, unknown).items():
                    fh = spools.get(date)
                    if fh is None:
                        fh = spools[date] = open(tmp / f"{date}.jsonl", "w", encoding="utf-8")
                    fh.write(json.dumps([uuid, vals], separators=(",", ":")) + "\n")
                cards += 1
                if cards % 20_000 == 0:
                    log.info(f"  {cards:,} cards")
        finally:
            for fh in spools.values():
                fh.close()

        dates = sorted(spools)
        log.info(f"Parsed {cards:,} cards covering {len(dates)} day(s): {dates[0]} .. {dates[-1]}")

        # Pass 2: one CSV per day that doesn't already have one.
        written, skipped_existing, rejected = [], [], []
        for date in dates:
            final_path = HISTORY_DIR / f"{date}.csv.gz"
            if final_path.exists():
                skipped_existing.append(date)
                continue
            flat = {}
            with open(tmp / f"{date}.jsonl", encoding="utf-8") as fh:
                for line in fh:
                    uuid, vals = json.loads(line)
                    flat[uuid] = vals
            if args.dry_run:
                log.info(f"  {date}: {len(flat):,} rows (dry run)")
                continue
            tmp_path = HISTORY_DIR / f".tmp-{date}.csv.gz"
            try:
                rows, buylist_rows, _ = write_flat_snapshot(flat, scryfall_ids, date,
                                                            tmp_path, unknown)
                if rows < MIN_ROWS or buylist_rows < MIN_BUYLIST_ROWS:
                    log.warning(f"  {date}: only {rows:,} rows / {buylist_rows:,} with buylist "
                                f"- looks partial, not saving")
                    rejected.append(date)
                    continue
                tmp_path.replace(final_path)
                written.append(date)
                log.info(f"  {date}: {rows:,} rows ({buylist_rows:,} with buylist)")
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()

    log.info(f"Done. Wrote {len(written)} day(s); kept {len(skipped_existing)} existing "
             f"{skipped_existing}; rejected {len(rejected)} {rejected}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger("backfill_mtgjson_history").exception("backfill failed")
        sys.exit(1)
