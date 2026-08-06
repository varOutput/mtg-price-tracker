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
```

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
scripts/build_history_db.py           (run locally) combines history/*.csv.gz into one queryable DB
data/latest.db                        current snapshot (created on first run)
data/history/YYYY-MM-DD.csv.gz        one file per day (created over time)
```
