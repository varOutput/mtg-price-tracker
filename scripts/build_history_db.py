#!/usr/bin/env python3
"""
Combine the daily data/history/*.csv.gz snapshots into one local SQLite
database so you can query full price history (trends over time, not just
today's price).

This is a convenience tool you run *locally*, on demand. It's deliberately
NOT part of the daily GitHub Actions job and its output is not committed
back to the repo - the combined DB only keeps growing as history.csv files
pile up, and there's no reason to store that twice.

Usage:
    python scripts/build_history_db.py
    python scripts/build_history_db.py --out my_history.db --since 2026-06-01
"""

import argparse
import csv
import gzip
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORY_DIR = REPO_ROOT / "data" / "history"

PRICE_FIELDS = ["usd", "usd_foil", "usd_etched", "eur", "eur_foil", "eur_etched", "tix"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="history.db", help="Output SQLite file (default: history.db)")
    parser.add_argument("--since", default=None, help="Only include snapshots on/after this YYYY-MM-DD date")
    args = parser.parse_args()

    files = sorted(HISTORY_DIR.glob("*.csv.gz"))
    files = [f for f in files if not f.name.startswith(".tmp-")]
    if args.since:
        files = [f for f in files if f.name.removesuffix(".csv.gz") >= args.since]

    if not files:
        print(f"No history files found in {HISTORY_DIR} (matching --since, if given).")
        return

    out_path = Path(args.out)
    if out_path.exists():
        out_path.unlink()

    conn = sqlite3.connect(out_path)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE price_history (
            scryfall_id TEXT,
            snapshot_date TEXT,
            name TEXT,
            set_code TEXT,
            set_name TEXT,
            collector_number TEXT,
            rarity TEXT,
            lang TEXT,
            {", ".join(f"{f} REAL" for f in PRICE_FIELDS)}
        )
    """)

    insert_sql = f"INSERT INTO price_history VALUES ({','.join('?' * (8 + len(PRICE_FIELDS)))})"

    total = 0
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = [
                (
                    row["scryfall_id"], row["snapshot_date"], row["name"], row["set_code"],
                    row["set_name"], row["collector_number"], row["rarity"], row["lang"],
                    *(float(row[c]) if row.get(c) else None for c in PRICE_FIELDS),
                )
                for row in reader
            ]
        cur.executemany(insert_sql, rows)
        total += len(rows)
        print(f"  loaded {f.name}: {len(rows):,} rows")

    print("Indexing...")
    cur.execute("CREATE INDEX idx_hist_id_date ON price_history(scryfall_id, snapshot_date)")
    cur.execute("CREATE INDEX idx_hist_name ON price_history(name)")
    conn.commit()
    conn.close()

    print(f"\nDone: {total:,} rows from {len(files)} day(s) written to {out_path}")
    print(f'Example query: sqlite3 {out_path} "SELECT snapshot_date, usd FROM price_history '
          f'WHERE name = \'Lightning Bolt\' ORDER BY snapshot_date;"')


if __name__ == "__main__":
    main()
