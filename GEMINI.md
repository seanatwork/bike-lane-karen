# GEMINI.md

This file provides guidance to Gemini CLI when working with code in this repository.

## Project Overview

Unofficial Telegram bot for exploring Austin 311 service data. Users interact via Telegram slash commands and inline buttons to query live Open311/Socrata APIs for graffiti, bicycle infrastructure, restaurant inspections, animal services, traffic, noise, parking, and crime data.

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env  # add TELEGRAM_BOT_TOKEN

# Run the bot
python austin311_bot.py

# Utility entrypoints
open311-ingest       # Bulk SQLite ingestion
scrape-bicycle       # Bicycle data scraper
search-311           # Browse 311 service codes
open311-aggregate    # Heatmap aggregator
```

No test runner is configured; the `graffiti/tests/` directory contains unit tests that can be run with `python -m pytest graffiti/tests/`.

## Deployment

Auto-deploys to Fly.io on push to `main` via `.github/workflows/deploy.yml`. The Fly.io app (`austin311bot`) runs as a single container (1 shared CPU, 256MB) in `iad`. Deploys use the `FLY_API_TOKEN` GitHub secret.

## Architecture

`austin311_bot.py` (the main file, ~3,700 lines) owns all Telegram routing. It imports data-fetching/formatting functions from service packages and wires them to handlers.

**Adding a new 311 service** requires three things:
1. A new package directory with a `*_bot.py` module that queries the API and returns formatted Markdown
2. Importing that module in `austin311_bot.py`
3. Adding command/callback handlers in the `create_application()` function

**Service packages** (each is independent):
- `graffiti/` — Open311 service code `HHSGRAFF`; supports analysis and a Folium-generated map (`/graffiti` → redirects to web map)
- `bicycle/` — Open311 across 8 service codes (PWBICYCL, OBSTMIDB, SBDEBROW, ATCOCIRW, SBSIDERE, TPPECRNE, PWSIDEWL, ZZARSTSW); includes keyword filtering for bicycle relevance; supports stats and a Folium-generated map (`/bicycle` → redirects to web map)
- `restaurants/` — Socrata dataset `ecmv-9xxi` (health inspections)
- `animalsvc/` — Open311 across 7+ service codes (loose dogs, bites, coyotes, etc.)
- `infrastructureandtransportation/` — Open311 (potholes, signals, sidewalks)
- `noisecomplaints/` — Open311 service code `NOISECMP`
- `parking/` — Open311 service code `PARKINGV`
- `parks/` — Open311 park maintenance reports with drill-down by park name
- `waterconservation/` — Socrata water conservation violations (`/waterviolations`)
- `childcare/` — Socrata childcare facility inspections (`/childcare`)
- `homeless/` — Open311 encampment/unhoused 311 reports across 5 service codes (PRGRDISS, ATCOCIRW, OBSTMIDB, SBDEBROW, DRCHANEL); supports stats, open locations, and a Folium-generated map (`/homeless`)
- Crime/safety — Socrata datasets `fdj4-gpfu` (APD crime) and `i7fg-wrk5` (NIBRS homicides), handled directly in `austin311_bot.py`

## Data Sources

| Source | Base URL | Used for |
|--------|----------|----------|
| Open311 API | `https://311.austintexas.gov/open311/v2` | All 311 service requests |
| Socrata API | `data.austintexas.gov` | Restaurant inspections, crime reports |

Query patterns: ISO8601 dates with `Z` suffix, `per_page`/`page` pagination, `$where` SoQL filtering (Socrata).

## Caching System

**`open311_cache.py`** — Shared SQLite-based caching layer for Open311 API data.

**Purpose:** Reduce API calls and workflow runtime by storing fetched records.

**How it works:**
1. First run: Fetches all data from Open311 API, populates SQLite cache
2. Subsequent runs: Loads cached data, only fetches new records since last fetch
3. Cache persists via GitHub Actions cache (7-day retention, free)

**Usage in workflows:**
```yaml
- name: Restore Open311 cache
  uses: actions/cache@v4
  with:
    path: .cache/open311_cache.db
    key: open311-maps-${{ github.run_id }}
    restore-keys: open311-

# ... run generation scripts ...

- name: Save Open311 cache
  uses: actions/cache@v4
  if: always()
  with:
    path: .cache/open311_cache.db
    key: open311-maps-${{ github.run_id }}-${{ github.run_attempt }}
```

**API:**
- `init_cache()` — Create cache tables
- `get_cached_records(category, since, service_codes)` — Retrieve cached data
- `cache_records(category, records)` — Store new records
- `get_last_fetch_date(category)` — Get most recent cached record date
- `should_refresh_cache(category, max_age_hours)` — Check if refresh needed

**Cache location:** `.cache/open311_cache.db` (gitignored, excluded from repo)

## Key Conventions

- Each service module uses a module-level `_session` singleton for HTTP connection reuse.
- All network calls use retry logic with exponential backoff (up to 3 retries, starting at 2s).
- Telegram messages over 4KB are split via `_send_chunked()` in `austin311_bot.py`.
- All bot output is Markdown-formatted.
- Environment variables: `TELEGRAM_BOT_TOKEN` (required), `AUSTINAPIKEY` (optional, raises Open311 rate limits), `GOOGLE_MAPS_API_KEY` (optional, for `/directory`).

## Open311 API — Known Pagination Gotcha

**The API returns records in chronological order (oldest first).** A single request with `start_date` 365 days ago and `end_date` today will return the oldest records first — so with `MAX_PAGES=10` (1000 records) you only see records from the *start* of the window, never the recent months.

**Impact:** Any module fetching more than ~90 days in a single call will silently miss recent records. The 90-day map queries are fine (all records fit within the page cap). Only long-range trend queries are affected.

**Fix used in `homeless/trends.py`:** Fetch month by month — one 30-day window per API call — so each request is small enough that all records for that period are returned. See `fetch_encampment_reports_monthly()` in `homeless/homeless_bot.py`.

**Applies to:** any `_fetch_code`-style function across bicycle, graffiti, homeless, noise, parking, parks modules if they ever need historical trend data beyond 90 days.

## Static Map Website

Public maps are deployed via GitHub Pages (`docs/` folder), generated from the same data as Telegram commands.

**⚠️ Two-repo split (as of 2026-04-18 consolidation):** this `austin311bot/` folder inside `All-Telegram-Bots` is now the authoritative source for code, but GitHub Pages + the map-refresh workflows still live in the original `seanatwork/austin311bot-unofficial` repo. The `docs/` folder and `.github/workflows/generate-*-map.yml` files listed below exist in both repos; the ones that actually run are the copies on `austin311bot-unofficial`. After any edit here that affects map output (generator, `*_bot.py` modules, `scripts/generate_map.py`), run `bash sync-to-unofficial.sh` to push the code to the old repo and kick off its workflows. Pure-Telegram edits don't need this.

**Maps:**
- `docs/index.html` — Landing page hub (https://austin311.com/) — branded "Austin 311"
- `docs/homeless/index.html` — Homeless encampment map (https://austin311.com/homeless/)
- `docs/bicycle/index.html` — Bicycle infrastructure map (https://austin311.com/bicycle/)
- `docs/graffiti/index.html` — Graffiti abatement map (https://austin311.com/graffiti/)
- `docs/traffic/index.html` — Traffic & infrastructure map (https://austin311.com/traffic/)
- `docs/parking/index.html` — Parking enforcement map (https://austin311.com/parking/)
- `docs/crime/index.html` — APD crime choropleth by council district (https://austin311.com/crime/)
- `docs/noise/index.html` — Noise complaint point map (https://austin311.com/noise/)
- `docs/parks/index.html` — Park maintenance point map (https://austin311.com/parks/)
- `docs/water/index.html` — Water conservation violations point map (https://austin311.com/water/)
- `docs/childcare/index.html` — Childcare facility compliance map (https://austin311.com/childcare/)
- `docs/animal/index.html` — Animal services map (https://austin311.com/animal/)
- `docs/crashes/index.html` — APD crash map with sidebar (https://austin311.com/crashes/) — **client-side only**, fetches live from Socrata `y2wy-tgr5`; no Python generator
- `docs/budget/index.html` — City budget spending (https://austin311.com/budget/)
- `docs/court/index.html` — Austin court caseloads (https://austin311.com/court/)

**Files:**
- `scripts/generate_map.py` — generic map generator that accepts category as CLI argument
  - Usage: `python scripts/generate_map.py bicycle|graffiti|homeless|traffic|parking|crime`
- `.github/workflows/deploy-map.yml` — GitHub Actions cron for homeless map (daily noon UTC)
- `.github/workflows/generate-bicycle-map.yml` — GitHub Actions cron for bicycle map (daily noon UTC)
- `.github/workflows/generate-graffiti-map.yml` — GitHub Actions cron for graffiti map (daily noon UTC)
- `.github/workflows/generate-traffic-map.yml` — GitHub Actions cron for traffic map (daily noon UTC)
- `.github/workflows/generate-parking-map.yml` — GitHub Actions cron for parking map (daily noon UTC)
- `.github/workflows/generate-crime-map.yml` — GitHub Actions cron for crime map (daily noon UTC)
- `.github/workflows/generate-noise-map.yml` — GitHub Actions cron for noise map (daily noon UTC)
- `.github/workflows/generate-parks-map.yml` — GitHub Actions cron for parks map (daily noon UTC)
- `.github/workflows/generate-water-map.yml` — GitHub Actions cron for water map (daily noon UTC)
- `.github/workflows/generate-childcare-map.yml` — GitHub Actions cron for childcare map (weekly, Mondays)
- `.github/workflows/generate-animal-map.yml` — GitHub Actions cron for animal map (daily noon UTC)
- `.github/workflows/generate-budget.yml` — GitHub Actions cron for budget page (quarterly: 15th of Jan/Apr/Jul/Oct)
- `.github/workflows/generate-graffiti-trends.yml` — GitHub Actions cron for graffiti trends (weekly Monday 13:00 UTC)
- `.github/workflows/generate-crime-trends.yml` — GitHub Actions cron for crime trends (weekly Monday 13:00 UTC)
- `.github/workflows/generate-noise-trends.yml` — GitHub Actions cron for noise trends (weekly Monday 13:00 UTC)
- `.github/workflows/generate-parking-trends.yml` — GitHub Actions cron for parking trends (weekly Monday 13:00 UTC)
- `docs/*/index.html` — pre-generated Folium HTML maps (committed to repo)

**✅ Workflows Re-enabled with Caching (2026-04-29):** Open311 workflows now use GitHub Actions caching to minimize API calls:
- **3 consolidated workflows** replace 17 individual workflows:
  - `generate-all-open311-maps.yml` — Weekly batch for all Open311 maps (bicycle, graffiti, noise, parking, parks, traffic, animal)
  - `generate-all-open311-trends.yml` — Weekly batch for all Open311 trends (graffiti, noise, homeless)
  - `generate-all-socrata.yml` — Daily batch for Socrata-based maps/trends (crime, water, childcare, budget)
- **Caching:** `open311_cache.py` provides SQLite caching with GitHub Actions cache persistence
  - First run: Normal API calls (populates cache)
  - Subsequent runs: Loads cached data, only fetches new records (2-3 min vs 10 min)
- Individual workflow files deprecated but kept for reference (can be deleted after consolidation testing)

**Service Code Discovery (2026-04-29):**
Analysis of 1,000+ recent requests found homeless-related keywords across these codes:
- ✓ PRGRDISS (Park Maintenance) — has tent, homeless keywords
- ✓ OBSTMIDB (Obstruction in ROW) — has homeless, tent keywords
- ✓ SBDEBROW (Debris in Street) — has homeless keywords  
- ✓ DRCHANEL (Drainage/Creek) — has homeless, tent keywords
- ✗ APDNONNO (Non Emergency Noise Complaint) — has homeless but not included (adds noise complaints unrelated to encampments)
- ✗ HHSGRAFF (Graffiti Abatement) — has homeless, tent but graffiti-focused

The "Homeless - Violet Kiosk and Storage Carts" and "Homelessness Matters" categories from the 311 dataset use the same service codes as general maintenance (Park Maintenance, etc.), NOT unique homeless-specific codes. The current 5 codes are optimal.

**Map features (all maps follow the same pattern):**
- 90 days of data fetched; user can filter to 30d / 60d / 90d via buttons
- Open / Closed status toggles
- Title bar updates dynamically with count reflecting active filters
- Popups show: clickable ticket link (`https://311.austintexas.gov/tickets?filter%5Bsearch%5D={id}`), address, filed/updated dates, description (up to 500 chars) falling back to resolution notes
- Mobile-friendly viewport meta tag

**Deploy workflow:**
1. Test locally: `source .venv/bin/activate && PYTHONPATH=. python scripts/generate_map.py <category>`
2. GitHub Actions → "Refresh <category> map" → Run workflow (regenerates `docs/<category>/index.html` and pushes)
3. GitHub Pages serves the updated `docs/` folder automatically

**Notes:**
- `AUSTINAPIKEY` must be set as a GitHub Actions secret for rate limit headroom
- 429 rate limit errors during local runs are normal without the token; CI has the secret
- `.venv/` is the working virtualenv (system Python is externally managed)

## Richer Map Popups

**Goal:** Show all available data in map popups so users don't need to click through to `https://311.austintexas.gov/tickets/<id>`.

**Key finding:** The Open311 v2 API (`/requests.json`) does NOT return a `description` field for some service codes (notably `ACBITE2` Animal Bite, `WILDEXPO`, `ACINFORM`). But the 311 website at `https://311.austintexas.gov/tickets/<id>` shows an "Additional Details" section with form answers (e.g. "What type of animal? Cat", "What date did the bite occur? Apr 26, 2026"). These are in `<dd class="mt-1 text-sm text-gray-900">` elements following an `<dt>Additional Details</dt>` tag.

**Implemented (animal map — `animalsvc/animal_bot.py`):**
- Added `_get_scrape_session()`, `_fetch_ticket_page_details(req_id)`, and `_fetch_all_ticket_details(req_ids)` functions that scrape the additional details section from the 311 website using BeautifulSoup (already in requirements)
- `_SKIP_DETAILS_RE` skips the "preferred language for contact" question (not useful)
- `_fetch_all_ticket_details` uses `ThreadPoolExecutor(max_workers=15)` for parallel fetching
- `generate_animal_map()` now fetches additional details for all mapped records before building markers
- Popup HTML updated: description label becomes "Resolution" for closed tickets (vs. "Notes" for open), extra_block shows "Additional Details:" with scraped form answers
- Popup `max_width` increased from 300 to 320px

**Pattern to replicate for other maps:** The same `_fetch_ticket_page_details` / `_fetch_all_ticket_details` approach should work for graffiti, bicycle, homeless, noise, parking, and parks maps. Each is in its own `*_bot.py` module — copy the three helper functions and the fetch call in `generate_*_map()`.

## Crime Choropleth Map

Live at `docs/crime/index.html` — a Folium choropleth of APD incident counts by Austin council district.

**Implementation:**
- `crime/crime_map.py` — `generate_crime_map()` fetches Socrata `fdj4-gpfu` grouped by `council_district` for 30/60/90-day windows; fetches district polygon GeoJSON from City of Austin ArcGIS; builds a Leaflet choropleth injected into a Folium base map
- District boundaries GeoJSON source: `https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/Council_Districts/FeatureServer/0/query?where=1%3D1&outFields=COUNCIL_DI&f=geojson`
- Key field for joining crime data to GeoJSON: `COUNCIL_DI` (integer 1–10) matches Socrata `council_district` string "1"–"10"
- Color scale: YlOrRd 5-step (`#ffffb2` → `#bd0026`)

**Map features (differs from 311 point maps):**
- Choropleth polygon fill colored by incident count; no open/closed toggle (APD data has no status)
- 30d/60d/90d buttons update polygon fill colors via `geoLayer.setStyle()`
- Hover tooltip shows district number + count; click popup shows count + % of citywide total
- `docs/index.html` landing page branded "Austin 311" with footer referencing both 311 and APD sources
