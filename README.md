# MTG Price Tracker

Pulls Scryfall's full card database (every card, every set) once a day via
GitHub Actions, and keeps a running price history. No API keys needed.

## What it does

- Every day, a GitHub Actions workflow downloads Scryfall's `default_cards`
  bulk file - one entry per unique card printing, in English (or the card's
  only language) - which includes daily-updated prices sourced from
  TCGPlayer (USD) and Cardmarket (EUR), plus MTGO tix.
- It writes:
  - **`data/latest.db`** - a small SQLite database with card metadata and
    *today's* prices. Overwritten each run, so it always reflects the most
    recent snapshot.
  - **`data/history/YYYY-MM-DD.csv.gz`** - one compressed file per day,
    never overwritten, that preserves that day's full price snapshot.
- The workflow commits both back to the repo, so history accumulates
  automatically for as long as it keeps running.

### Second job: MTGJSON buylist + multi-vendor retail

A separate daily workflow (`.github/workflows/fetch-mtgjson-prices.yml`,
08:41 UTC) saves MTGJSON's `AllPricesToday` file - which adds **Card
Kingdom buylist prices** and retail from several vendors - to
**`data/mtgjson_history/YYYY-MM-DD.csv.gz`**, one file per day, never
overwritten, same as the Scryfall history. The date is MTGJSON's own price
date. It runs independently of the Scryfall job; either can fail without
affecting the other.

Columns:

- `mtgjson_uuid` - one row per MTGJSON card uuid.
- `scryfall_id` - join key to `latest.db` and `data/history/`. Blank for
  the few thousand uuids MTGJSON has no Scryfall id for. **Not unique**:
  MTGJSON sometimes splits one printing into several uuids (e.g. separate
  foil / non-foil versions) that share a `scryfall_id` with different prices.
- `snapshot_date`
- `<provider>_<retail|buylist>_<normal|foil|etched>` price columns:
  `tcgplayer`, `manapool`, `cardkingdom`, `cardmarket` (paper) and
  `cardhoarder` (MTGO). Only Card Kingdom has buylist prices. `cardmarket`
  is EUR; everything else is USD. If MTGJSON adds a new provider, its
  columns are appended automatically.

History starts 2026-08-15: `scripts/backfill_mtgjson_history.py` filled
the ~90 days MTGJSON keeps in its `AllPrices` file (MTGJSON itself has no
data for 2026-08-29 and 2026-10-14). It never overwrites an existing day, so
it's safe to re-run, e.g. to fill a gap after the daily job missed days:

```bash
python scripts/backfill_mtgjson_history.py            # downloads ~150 MB, takes ~6 min
python scripts/backfill_mtgjson_history.py --dry-run  # report what it would write
```

Buylist prices are for near-mint copies, while TCGplayer's market price
reflects whatever condition actually sold - so a buylist above retail
(common on old cards) is usually a condition gap, not free money.

## Setup

1. Push these files to a new GitHub repository (public repos get unlimited
   free Actions minutes; private repos get 2,000 free minutes/month on the
   Free plan, which this comfortably fits inside - one run should take a
   few minutes).
2. Go to the **Actions** tab and enable workflows if prompted.
   - **If you forked this instead of pushing to a fresh repo:** GitHub
     disables scheduled workflows on forks by default. Open the workflow
     under the Actions tab and click **Enable workflow**.
3. Trigger a run manually to confirm it works: Actions tab → "Fetch
   Scryfall prices" → **Run workflow**. Check the logs; the first run does
   the same thing the daily schedule will do.
4. After that, it runs on its own at 06:17 UTC daily (edit the `cron` line
   in `.github/workflows/fetch-prices.yml` to change the time).

No secrets or API keys to configure - Scryfall's bulk data is public and
unauthenticated.

## Querying the data

`data/latest.db` has two tables:

```sql
-- Today's price for a specific card
SELECT c.name, c.set_name, p.usd, p.usd_foil
FROM cards c JOIN latest_prices p USING (scryfall_id)
WHERE c.name = 'Lightning Bolt';

-- Most expensive cards in a given set
SELECT c.name, p.usd
FROM cards c JOIN latest_prices p USING (scryfall_id)
WHERE c.set_code = 'mh3'
ORDER BY p.usd DESC LIMIT 20;
```

### Full price history

`data/history/*.csv.gz` holds every daily snapshot, but isn't pre-loaded
into a database (see "Why not one big database?" below). To analyze trends,
build a local combined database on demand:

```bash
pip install -r requirements.txt   # just `requests`, but harmless either way
python scripts/build_history_db.py
# -> history.db in the current directory
```

```sql
-- Price of a card over time
SELECT snapshot_date, usd FROM price_history
WHERE name = 'Lightning Bolt' ORDER BY snapshot_date;

-- Card Kingdom buylist vs TCGplayer retail over time (MTGJSON history)
SELECT p.snapshot_date, p.collector_number,
       m.cardkingdom_buylist_normal, m.tcgplayer_retail_normal
FROM mtgjson_price_history m
JOIN price_history p ON p.scryfall_id = m.scryfall_id
                    AND p.snapshot_date = m.snapshot_date
WHERE p.name = 'Lightning Bolt' AND p.set_code = 'sta'
ORDER BY p.snapshot_date;
```

`build_history_db.py` loads `data/mtgjson_history/` into a second table,
`mtgjson_price_history`, whenever those files exist.

`history.db` isn't committed back to the repo - rebuild it locally whenever
you want it (`--since YYYY-MM-DD` limits how much history it loads).

## Why not one big database?

The obvious design is "just keep appending to one SQLite file forever."
The problem: GitHub rejects any single committed file over 100 MB, and at
roughly 30,000+ card printings gaining a new price row every day, a single
ever-growing price-history table would cross that within a few months -
not years. Splitting history into one small file per day sidesteps this
completely: each file is written once and never touched again, so the repo
grows by exactly one day's worth of data per day, indefinitely, with no
single file ever approaching the limit.

## A note on scope

TCGPlayer's own API is currently closed to new developer applications, so
this uses Scryfall, which already aggregates TCGPlayer (USD) and Cardmarket
(EUR) prices for every card once daily - matching this project's cadence
exactly. If you later get TCGPlayer API access directly (e.g. as an
approved seller/partner) and want per-listing or condition-specific pricing
that Scryfall doesn't expose, that would be a separate script using
TCGPlayer's OAuth2 flow - happy to help build that path too if it becomes
available to you.

Scryfall asks that data not be simply repackaged, resold, or paywalled -
fine for personal tracking, analysis, or an internal tool, but worth a read
of [their API guidelines](https://scryfall.com/docs/api) if you plan to
make anything here public-facing.

## Files

```
.github/workflows/fetch-prices.yml   the daily cron job
scripts/fetch_prices.py               downloads bulk data, writes latest.db + today's history file
.github/workflows/fetch-mtgjson-prices.yml  the daily MTGJSON (buylist) job
scripts/fetch_mtgjson_prices.py       downloads MTGJSON AllPricesToday, writes today's mtgjson_history file
scripts/backfill_mtgjson_history.py   (run locally) fills mtgjson_history/ from MTGJSON's ~90-day AllPrices
scripts/build_history_db.py           (run locally) combines history/ and mtgjson_history/ into one queryable DB
data/latest.db                        current snapshot (created on first run)
data/history/YYYY-MM-DD.csv.gz        one file per day (created over time)
data/mtgjson_history/YYYY-MM-DD.csv.gz  one MTGJSON buylist/retail file per day
```
