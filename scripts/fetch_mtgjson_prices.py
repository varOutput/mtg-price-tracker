#!/usr/bin/env python3
"""
Daily MTGJSON price puller (buylist + multi-vendor retail).

Downloads MTGJSON's AllPricesToday file - today's retail prices from
TCGplayer, Manapool, Card Kingdom and Cardmarket, Card Kingdom *buylist*
prices, and Cardhoarder (MTGO) prices - and writes one compressed snapshot:

    data/mtgjson_history/<YYYY-MM-DD>.csv.gz

The Scryfall job (fetch_prices.py) is untouched; this runs alongside it.

Each row is one MTGJSON card uuid. MTGJSON uuids are NOT one-to-one with
Scryfall ids: some printings are split into separate uuids (e.g. a foil and
a non-foil version) that share one scryfall_id but carry different prices,
and a few thousand priced uuids have no scryfall_id at all. So rows are kept
per uuid, with scryfall_id as a join column (blank when MTGJSON has none).

The date in the filename is MTGJSON's own price date (from its metadata),
not the date the job ran, so a delayed or repeated run never mislabels data.
Re-running on the same MTGJSON build just rewrites the same file.

Docs:
  https://mtgjson.com/data-models/price/price-formats/
  https://mtgjson.com/downloads/all-files/
"""

import csv
import gzip
import io
import json
import logging
import sys
import tempfile
import time
from pathlib import Path

import requests

# --- Config -----------------------------------------------------------

USER_AGENT = "MTGPriceTrackerScript/1.0"

PRICES_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.gz"
# Small (~15 MB) file mapping MTGJSON uuid -> scryfallId and other vendor ids.
IDENTIFIERS_URL = "https://mtgjson.com/api/v5/csv/cardIdentifiers.csv.gz"

DOWNLOAD_TIMEOUT = 300
DOWNLOAD_ATTEMPTS = 4

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORY_DIR = REPO_ROOT / "data" / "mtgjson_history"

# Columns are <provider>_<retail|buylist>_<normal|foil|etched>. These are
# every combination MTGJSON published as of Nov 2026, and they're always
# written (even if empty) so every day's file has the same layout.
# Currencies: cardmarket is EUR; everything else is USD. cardhoarder is MTGO.
PRICE_COLUMNS = [
    "tcgplayer_retail_normal", "tcgplayer_retail_foil", "tcgplayer_retail_etched",
    "manapool_retail_normal", "manapool_retail_foil", "manapool_retail_etched",
    "cardkingdom_retail_normal", "cardkingdom_retail_foil", "cardkingdom_retail_etched",
    "cardkingdom_buylist_normal", "cardkingdom_buylist_foil", "cardkingdom_buylist_etched",
    "cardmarket_retail_normal", "cardmarket_retail_foil", "cardmarket_retail_etched",
    "cardhoarder_retail_normal", "cardhoarder_retail_foil",
]
KEY_COLUMNS = ["mtgjson_uuid", "scryfall_id", "snapshot_date"]

# Sanity floors - if MTGJSON ships a truncated or reshaped file, fail the run
# instead of committing a bad snapshot. (Nov 2026: ~115k uuids, ~84k with a
# Card Kingdom buylist price.)
MIN_ROWS = 50_000
MIN_BUYLIST_ROWS = 10_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fetch_mtgjson_prices")


# --- Download -----------------------------------------------------------

def download(session: requests.Session, url: str, dest: Path) -> Path:
    """Stream `url` to `dest`, retrying transient failures with backoff."""
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            log.info(f"Downloading {url} (attempt {attempt})...")
            with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            log.info(f"  {dest.stat().st_size / (1024 * 1024):.1f} MB")
            return dest
        except requests.RequestException as e:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            wait = 2 ** attempt * 5
            log.warning(f"  failed ({e}); retrying in {wait}s")
            time.sleep(wait)
    raise AssertionError("unreachable")


def load_scryfall_ids(path: Path) -> dict:
    """uuid -> scryfallId (only uuids that have one)."""
    with gzip.open(path, "rb") as raw:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
        return {row["uuid"]: row["scryfallId"] for row in reader if row.get("scryfallId")}


# --- Processing -----------------------------------------------------------

def flatten_prices(entry: dict, snapshot_date: str, unknown: set) -> dict:
    """
    Turn one uuid's nested MTGJSON price object
        {format: {provider: {"retail"|"buylist": {finish: {date: price}}}}}
    into {"<provider>_<side>_<finish>": price}.
    Uses the price for `snapshot_date`; falls back to the latest date present.
    """
    out = {}
    for _fmt, providers in entry.items():
        for provider, body in providers.items():
            for side in ("retail", "buylist"):
                for finish, series in (body.get(side) or {}).items():
                    if not series:
                        continue
                    value = series.get(snapshot_date)
                    if value is None:
                        value = series[max(series)]
                    col = f"{provider}_{side}_{finish}"
                    if col not in PRICE_COLUMNS:
                        unknown.add(col)
                    # MTGJSON's JSON carries float noise (3.2800000000000002);
                    # 4 dp keeps sub-cent MTGO prices exact.
                    out[col] = round(float(value), 4)
    return out


def write_snapshot(prices: dict, scryfall_ids: dict, snapshot_date: str,
                   tmp_path: Path) -> tuple[int, int, list]:
    """Write the CSV. Returns (rows, rows_with_buylist, extra_columns)."""
    unknown: set = set()
    flat = {uuid: flatten_prices(entry, snapshot_date, unknown)
            for uuid, entry in prices.items()}
    return write_flat_snapshot(flat, scryfall_ids, snapshot_date, tmp_path, unknown)


def write_flat_snapshot(flat: dict, scryfall_ids: dict, snapshot_date: str,
                        tmp_path: Path, unknown: set) -> tuple[int, int, list]:
    """
    Write {uuid: {column: price}} as one day's CSV (shared with the
    backfill script). Returns (rows, rows_with_buylist, extra_columns).
    """
    # Keep data for any new provider/finish MTGJSON adds, rather than
    # silently dropping it: append those columns after the known ones.
    extra = sorted(unknown)
    if extra:
        log.warning(f"New price columns not in PRICE_COLUMNS (kept, appended): {extra}")
    columns = PRICE_COLUMNS + extra

    rows = buylist_rows = 0
    with gzip.open(tmp_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(KEY_COLUMNS + columns)
        for uuid in sorted(flat):  # stable order -> smaller, diffable files
            vals = flat[uuid]
            if not vals:
                continue
            writer.writerow([uuid, scryfall_ids.get(uuid, ""), snapshot_date,
                             *(vals.get(c, "") for c in columns)])
            rows += 1
            if any(k.startswith("cardkingdom_buylist_") for k in vals):
                buylist_rows += 1
    return rows, buylist_rows, extra


# --- Main -----------------------------------------------------------------

def main() -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})

    with tempfile.TemporaryDirectory() as tmp:
        prices_path = download(session, PRICES_URL, Path(tmp) / "AllPricesToday.json.gz")
        ids_path = download(session, IDENTIFIERS_URL, Path(tmp) / "cardIdentifiers.csv.gz")

        log.info("Parsing prices...")
        with gzip.open(prices_path, "rb") as f:
            doc = json.load(f)
        snapshot_date = (doc.get("meta") or {}).get("date")
        prices = doc.get("data") or {}
        if not snapshot_date or not prices:
            raise RuntimeError("AllPricesToday had no meta.date or no data - aborting")
        log.info(f"MTGJSON price date {snapshot_date}, version "
                 f"{doc['meta'].get('version')}, {len(prices):,} uuids")

        scryfall_ids = load_scryfall_ids(ids_path)
        log.info(f"Loaded {len(scryfall_ids):,} uuid -> scryfallId mappings")

    final_path = HISTORY_DIR / f"{snapshot_date}.csv.gz"
    # Temp file sits beside the final one so the replace() is an atomic rename.
    tmp_path = HISTORY_DIR / f".tmp-{snapshot_date}.csv.gz"
    try:
        rows, buylist_rows, _extra = write_snapshot(prices, scryfall_ids, snapshot_date, tmp_path)
        mapped = sum(1 for u in prices if u in scryfall_ids)
        log.info(f"Wrote {rows:,} rows ({buylist_rows:,} with a Card Kingdom buylist price; "
                 f"{mapped:,} uuids mapped to a scryfall_id)")
        if rows < MIN_ROWS or buylist_rows < MIN_BUYLIST_ROWS:
            raise RuntimeError(
                f"Snapshot looks wrong ({rows:,} rows, {buylist_rows:,} with buylist; "
                f"expected >= {MIN_ROWS:,} / {MIN_BUYLIST_ROWS:,}) - not saving it")
        if final_path.exists():
            log.info(f"{final_path.name} already exists (MTGJSON hasn't published a newer "
                     f"build yet); rewriting it with the same data")
        tmp_path.replace(final_path)
        log.info(f"Saved {final_path}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("fetch_mtgjson_prices.py failed")
        sys.exit(1)
