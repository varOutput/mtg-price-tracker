#!/usr/bin/env python3
"""
Daily Scryfall price puller.

Downloads Scryfall's "default_cards" bulk data file (one entry per unique
card printing - i.e. every card in every set, in English or the card's only
language) and writes two things:

  1. data/latest.db                    SQLite DB: card metadata + latest prices
  2. data/history/<YYYY-MM-DD>.csv.gz  a compressed snapshot of every card's
                                        prices as of this run

Meant to be run once every 24 hours (see .github/workflows/fetch-prices.yml).
Safe to re-run manually any time - each run fully replaces latest.db and
writes (or overwrites) only *today's* history file.

Docs this script is based on:
  https://scryfall.com/docs/api/bulk-data
  https://scryfall.com/docs/api/cards
  https://scryfall.com/blog/two-new-ways-to-sync-scryfall-data-236
    (bulk files moved to gzipped JSONL - one JSON object per line - as the
    only format Scryfall serves as of July 20, 2026)
"""

import csv
import gzip
import json
import logging
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests

# --- Config -----------------------------------------------------------

# Scryfall asks that this be descriptive of your app. Edit the contact info
# if you'd like - it's only used in the header Scryfall sees, not published
# anywhere. See https://scryfall.com/docs/api
USER_AGENT = "MTGPriceTrackerScript/1.0"

BULK_DATA_LIST_URL = "https://api.scryfall.com/bulk-data"
BULK_DATA_TYPE = "default_cards"
REQUEST_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 180

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"
DB_PATH = DATA_DIR / "latest.db"

PRICE_FIELDS = ["usd", "usd_foil", "usd_etched", "eur", "eur_foil", "eur_etched", "tix"]
BATCH_SIZE = 2000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fetch_prices")


# --- Scryfall access ----------------------------------------------------

def get_bulk_data_info(session: requests.Session) -> dict:
    """Look up the current bulk-data entry for BULK_DATA_TYPE."""
    log.info("Requesting bulk-data listing from Scryfall...")
    resp = session.get(BULK_DATA_LIST_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    items = resp.json().get("data", [])
    for item in items:
        if item.get("type") == BULK_DATA_TYPE:
            return item
    available = [i.get("type") for i in items]
    raise RuntimeError(
        f"Scryfall's bulk-data list didn't include type={BULK_DATA_TYPE!r}. "
        f"Types it did return: {available}"
    )


def download_bulk_file(session: requests.Session, info: dict, dest: Path) -> Path:
    """Stream-download the bulk file (gzipped JSONL) to `dest` on disk."""
    # Scryfall is mid-migration to JSONL-only bulk files (as of 2026-07-20 the
    # only format offered). Prefer jsonl_download_uri; fall back to the older
    # download_uri key in case it's ever talking to a cached/older API shape.
    url = info.get("jsonl_download_uri") or info.get("download_uri")
    if not url:
        raise RuntimeError(
            "Bulk-data entry had neither 'jsonl_download_uri' nor 'download_uri': "
            f"{info}"
        )

    approx_mb = (info.get("size") or 0) / (1024 * 1024)
    log.info(f"Downloading {BULK_DATA_TYPE} bulk file (~{approx_mb:.0f} MB uncompressed)...")

    downloaded = 0
    with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
    log.info(f"Downloaded {downloaded / (1024 * 1024):.1f} MB to {dest.name}")
    return dest


def iter_cards(jsonl_gz_path: Path):
    """Yield one parsed dict per line of the gzipped JSONL bulk file."""
    with gzip.open(jsonl_gz_path, "rt", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                log.warning(f"Skipping unparseable line {line_num}: {e}")


def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --- Processing -----------------------------------------------------------

CARDS_INSERT_SQL = """
    INSERT OR REPLACE INTO cards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""
PRICES_INSERT_SQL = """
    INSERT OR REPLACE INTO latest_prices VALUES (?,?,?,?,?,?,?,?,?)
"""
CSV_COLUMNS = [
    "scryfall_id", "snapshot_date", "name", "set_code", "set_name",
    "collector_number", "rarity", "lang",
    *PRICE_FIELDS,
]


def create_schema(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE cards (
            scryfall_id TEXT PRIMARY KEY,
            oracle_id TEXT,
            name TEXT,
            set_code TEXT,
            set_name TEXT,
            collector_number TEXT,
            rarity TEXT,
            lang TEXT,
            layout TEXT,
            released_at TEXT,
            digital INTEGER,
            tcgplayer_id INTEGER,
            tcgplayer_etched_id INTEGER,
            cardmarket_id INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE latest_prices (
            scryfall_id TEXT PRIMARY KEY,
            snapshot_date TEXT,
            usd REAL, usd_foil REAL, usd_etched REAL,
            eur REAL, eur_foil REAL, eur_etched REAL,
            tix REAL,
            FOREIGN KEY (scryfall_id) REFERENCES cards(scryfall_id)
        )
    """)


def card_to_row(card: dict) -> tuple:
    return (
        card.get("id"),
        card.get("oracle_id"),
        card.get("name"),
        card.get("set"),
        card.get("set_name"),
        card.get("collector_number"),
        card.get("rarity"),
        card.get("lang"),
        card.get("layout"),
        card.get("released_at"),
        1 if card.get("digital") else 0,
        card.get("tcgplayer_id"),
        card.get("tcgplayer_etched_id"),
        card.get("cardmarket_id"),
    )


def process_bulk_file(jsonl_gz_path: Path, db_tmp_path: Path, csv_tmp_path: Path,
                       snapshot_date: str) -> int:
    """
    Single pass over the bulk file that writes both the SQLite DB and the
    CSV history file at once (avoids decompressing the file twice).
    Returns the number of cards processed.
    """
    if db_tmp_path.exists():
        db_tmp_path.unlink()

    conn = sqlite3.connect(db_tmp_path)
    cur = conn.cursor()
    create_schema(cur)

    count = 0
    card_batch, price_batch = [], []

    with gzip.open(csv_tmp_path, "wt", encoding="utf-8", newline="") as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(CSV_COLUMNS)

        for card in iter_cards(jsonl_gz_path):
            prices = card.get("prices") or {}
            price_values = [to_float(prices.get(f)) for f in PRICE_FIELDS]

            card_batch.append(card_to_row(card))
            price_batch.append((card.get("id"), snapshot_date, *price_values))

            writer.writerow([
                card.get("id"), snapshot_date, card.get("name"), card.get("set"),
                card.get("set_name"), card.get("collector_number"), card.get("rarity"),
                card.get("lang"), *price_values,
            ])

            count += 1
            if len(card_batch) >= BATCH_SIZE:
                cur.executemany(CARDS_INSERT_SQL, card_batch)
                cur.executemany(PRICES_INSERT_SQL, price_batch)
                conn.commit()
                card_batch.clear()
                price_batch.clear()

        if card_batch:
            cur.executemany(CARDS_INSERT_SQL, card_batch)
            cur.executemany(PRICES_INSERT_SQL, price_batch)
            conn.commit()

    cur.execute("CREATE INDEX idx_cards_name ON cards(name)")
    cur.execute("CREATE INDEX idx_cards_set ON cards(set_code)")
    cur.execute("CREATE INDEX idx_cards_oracle ON cards(oracle_id)")
    conn.commit()
    conn.close()
    return count


# --- Main -----------------------------------------------------------------

def main() -> None:
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})

    # Final destinations
    db_final_path = DB_PATH
    csv_final_path = HISTORY_DIR / f"{snapshot_date}.csv.gz"

    # Temp files live in the SAME directories as their final destinations so
    # the final os.replace() is an atomic same-filesystem rename - a crash
    # partway through a run can never leave a half-written latest.db or
    # half-written history file in place.
    db_tmp_path = DATA_DIR / f".tmp-{snapshot_date}-latest.db"
    csv_tmp_path = HISTORY_DIR / f".tmp-{snapshot_date}.csv.gz"

    try:
        with tempfile.TemporaryDirectory() as tmp:
            bulk_path = Path(tmp) / "default_cards.jsonl.gz"

            info = get_bulk_data_info(session)
            download_bulk_file(session, info, bulk_path)

            log.info("Processing cards (writing DB + CSV in one pass)...")
            count = process_bulk_file(bulk_path, db_tmp_path, csv_tmp_path, snapshot_date)
            log.info(f"Processed {count:,} card printings")

        if count == 0:
            raise RuntimeError("Parsed zero cards from the bulk file - aborting "
                                "without touching existing data")

        db_tmp_path.replace(db_final_path)
        csv_tmp_path.replace(csv_final_path)
        log.info(f"Wrote {db_final_path} and {csv_final_path}")

    finally:
        # Clean up any leftover temp files if something failed partway
        for p in (db_tmp_path, csv_tmp_path):
            if p.exists():
                p.unlink()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("fetch_prices.py failed")
        sys.exit(1)
