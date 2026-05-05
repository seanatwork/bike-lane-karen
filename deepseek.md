# DeepSeek session: Pulse counters for austin311.com landing page

**Date:** 2026-05-04
**Goal:** Implement three live counters on `docs/index.html` that read from `docs/pulse.json`, generated daily.

## The three counters

| # | Counter | Source | Query |
|---|---------|--------|-------|
| 1 | New 311 reports in last 24h | Open311 `/requests.json` | `start_date=24h`, paginated |
| 2 | Fatal crashes in last 90 days | Socrata `dx9v-zd7x` | Filter `issue_reported` = "traffic fatality" |
| 3 | Violent crime incidents in last 7 days | Socrata `fdj4-gpfu` | Client-side filter on `crime_type` keywords |

## What was built

### 1. `scripts/generate_pulse.py` — generator script

- Three standalone fetch functions, each with retry logic
- Outputs `docs/pulse.json` with timestamps and counter values
- Graceful degradation: returns `-1` sentinel on failure → JSON stores `null`
- Debug logging shows crime_type breakdown so keywords can be tuned

### 2. API field discovery

Ran a test script (`scripts/_test_pulse.py`, now deleted) against all three APIs to discover exact field values:

**dx9v-zd7x (traffic incidents):**
- `issue_reported` values are uppercase: `"TRAFFIC FATALITY"`, `"COLLISION"`, `"Crash Urgent"`, etc.
- Only 1 fatality in last 90 days
- 5,000 rows returned in 90-day window

**fdj4-gpfu (APD crime):**
- `crime_type` values are uppercase abbreviations like `"AGG ASSAULT"`, `"ASSAULT WITH INJURY-FAM/DATING VIO"`, `"FAMILY DISTURBANCE"`
- 906 total incidents in 7 days
- 252 matched as violent (with `family disturbance` and `theft from person` included)

**Open311:**
- 100 reports in last 24h (evening hours — typical Austin daily range is 300–500)

### 3. Violent crime keyword decisions

**Included (252/week):**
- All homicide/murder codes
- Aggravated assault (all variants)
- Sexual assault (all variants)
- Robbery by threat or assault
- Assault with injury (all variants)
- Deadly conduct, terroristic threat
- Family disturbance / dating disturbance (102/week — domestic calls where police responded)
- Protective order violations
- Crimes against children/elderly
- Stalking, kidnapping, unlawful restraint
- **`theft from person`** (3/week — purse snatch / pickpocket with victim contact)

**Excluded (property crimes):**
- Theft (plain), theft by shoplifting, theft of bicycle, auto theft — no victim confrontation
- Burglary of residence/vehicle — victim not present
- Criminal mischief, trespass — property damage
- DWI, drug possession, fraud — non-violent

**Reasoning:** The FBI UCR distinction — robbery = taking from a person by force/threat (violent). Theft/burglary = taking without confronting victim (property). APD follows this. When a theft involves force (strong-arm mugging, armed robbery, carjacking), it's coded as `robbery by assault` or `agg robbery/deadly weapon`, not as `theft`.

### 4. Verification run

```
$ python3 scripts/generate_pulse.py
Generating pulse.json at 2026-05-05T02:02:42Z
  ✓ 311 reports (24h): 100
  ✓ fatal crashes (90d): 1
  ✓ violent crime (7d): 252 of 906 total incidents
       102  family disturbance
        34  assault with injury-fam/dating vio
        31  assault with injury
        16  terroristic threat
         8  dating disturbance
        ...
Wrote 682 bytes to docs/pulse.json
```

## What remains to build

1. **`docs/index.html`** — Add pulse bar HTML + CSS + JS fetch
2. **`.github/workflows/generate-all-socrata.yml`** — Add `generate_pulse.py` step + `git add docs/pulse.json`
3. **Test in CI** — First run will commit `pulse.json`, then landing page JS loads it

## Key design decisions

- **Daily generation, not real-time** — Hook into existing `generate-all-socrata.yml` (14:00 UTC daily). Open311 rate limits make sub-hourly impractical.
- **Graceful failure** — If any API is down, that counter shows `null` → JS displays "N/A". If `pulse.json` is entirely missing, JS hides the bar.
- **No new dependencies** — Reuses `requests.Session` pattern from existing scripts. No database, no bot changes.
- **Keyword set is maintainable** — The debug logging prints every matched crime type each run, so you can spot new codes APD introduces over time.