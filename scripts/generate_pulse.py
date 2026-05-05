#!/usr/bin/env python3
"""
Generate docs/pulse.json — live counters for the landing page.

Counters:
  1. New 311 reports in the last 24h      (Open311)
  2. Fatal crashes in the last 90 days     (Socrata dx9v-zd7x)
  3. Violent crime incidents in last 7d    (Socrata fdj4-gpfu)

Run:  AUSTINAPIKEY=sk... python scripts/generate_pulse.py
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from collections import Counter

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

OPEN311_URL = "https://311.austintexas.gov/open311/v2/requests.json"
SOCRATA_BASE = "https://data.austintexas.gov/resource"
FATAL_CRASHES_DATASET = "y2wy-tgr5"  # APD Crash Data (archival, historical)
CRIME_DATASET = "fdj4-gpfu"          # APD Crime Reports

# ── discovered from live API query ────────────────────────────────────────────


# crime_type values that indicate violent crime.
# From fdj4-gpfu: APD uses uppercase abbreviations. These cover:
#   homicide/murder, aggravated assault, sexual assault, robbery,
#   assault with injury, deadly conduct, terroristic threat,
#   and family-violence / protective-order variants.
#
# NOTE: "family disturbance" is the big one — ~100/week. It's included
# because APD labels actual injury cases separately ("assault with
# injury-fam/dating vio"), meaning "family disturbance" calls are
# situations where police were summoned to a potentially violent domestic
# incident. Remove from this set if you want a stricter count.
VIOLENT_CRIME_TYPES = frozenset({
    # Homicide / murder
    "murder",
    "capital murder",
    "homicide",
    "manslaughter",
    "criminally negligent homicide",
    # Aggravated assault (all variants)
    "agg assault",
    "agg aslt enhanc strangl/suffoc",
    "agg assault strangle/suffocate",
    "agg robbery/deadly weapon",
    "aggravated assault-fam/dating vio",
    # Sexual assault
    "aggravated sexual assault",
    "sexual assault",
    "sexual assault of a child",
    "indecency with a child",
    # Robbery
    "robbery by threat",
    "robbery by assault",
    # Assault with injury (bodily harm)
    "assault with injury",
    "assault with injury-fam/dating vio",
    "assault of pregnant woman-fam/dating vio",
    "assault on peace officer",
    "assault on public servant",
    # Deadly conduct / terroristic threat
    "deadly conduct",
    "terroristic threat",
    "terroristic threat-fam/dating vio",
    "terroristic threat-mass caslty",
    # Family violence / protective orders
    "family disturbance",
    "family disturbance/parental",
    "dating disturbance",
    "continued violence against family",
    "viol of protective order or bond fv/sex crime/human trafficking",
    "viol of emerg protective order",
    # Crimes against vulnerable persons
    "injury to elderly person",
    "injury to elderly person-fam/dating vio",
    "abandoning or endangering child, elderly, disabled",
    # Other violent / sex
    "stalking",
    "kidnapping",
    "unlawful restraint",
    "online solicitation of a minor",
    "sexting/transmit sexual photos",
    "harboring runaway child",
})

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/pulse (pulse generator)",
        })
    return _session


def _app_token() -> str:
    return os.getenv("AUSTINAPIKEY", "")


# ── Counter 1: 311 reports in last 24h ───────────────────────────────────────

def _count_311_24h() -> int:
    """Count Open311 requests filed in the last 24 hours."""
    start = (datetime.now(timezone.utc) - timedelta(days=1))
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    total = 0
    page = 1
    session = _get_session()

    while True:
        params = {
            "start_date": start_str,
            "status": "open,closed",
            "page": page,
            "page_size": 1000,
        }
        for attempt in range(3):
            try:
                resp = session.get(OPEN311_URL, params=params, timeout=30)
                resp.raise_for_status()
                batch = resp.json()
                if not isinstance(batch, list):
                    batch = []
                break
            except Exception as e:
                if attempt == 2:
                    logger.warning(f" 311 24h fetch failed: {e}")
                    return -1  # sentinel: unknown
                time.sleep(2 ** attempt * 2)
        else:
            return -1

        total += len(batch)
        if len(batch) < 1000:
            break
        page += 1

    logger.info(f"  ✓ 311 reports (24h): {total:,}")
    return total


# ── Counter 2: fatal crashes in last 90 days ─────────────────────────────────

def _count_fatal_crashes_90d() -> int:
    """Count fatal crashes from APD Crash Data (y2wy-tgr5), last 90 days reporting window."""
    session = _get_session()

    # Step 1: get the latest crash timestamp in the dataset (accounts for publication lag)
    max_params: dict = {"$select": "max(crash_timestamp)"}
    token = _app_token()
    if token:
        max_params["$$app_token"] = token
    try:
        resp = session.get(
            f"{SOCRATA_BASE}/{FATAL_CRASHES_DATASET}.json",
            params=max_params,
            timeout=15,
        )
        resp.raise_for_status()
        latest = resp.json()
        latest_ts = datetime.fromisoformat(latest[0]["max_crash_timestamp"])
    except Exception as e:
        logger.warning(f" fatal crashes — could not fetch latest timestamp: {e}")
        # fallback: today's date (but this would include lag days with no data)
        latest_ts = datetime.now(timezone.utc)

    cutoff = latest_ts - timedelta(days=90)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")

    params = {
        "$where": f"crash_timestamp >= '{cutoff_str}' AND crash_fatal_fl = true",
        "$limit": 5000,
        "$select": "count(*)",
    }
    token = _app_token()
    if token:
        params["$$app_token"] = token

    for attempt in range(3):
        try:
            resp = session.get(
                f"{SOCRATA_BASE}/{FATAL_CRASHES_DATASET}.json",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt == 2:
                logger.warning(f" fatal crashes fetch failed: {e}")
                return -1
            time.sleep(2 ** attempt * 2)
    else:
        return -1

    # Socrata returns [{"count": N}] when $select=count(*)
    fatal = int(data[0]["count"]) if data else 0
    logger.info(f"  ✓ fatal crashes (90d): {fatal:,}")
    return fatal


# ── Counter 3: violent crime in last 7 days ──────────────────────────────────

def _count_violent_crime_7d() -> int:
    """Count violent crime incidents from fdj4-gpfu, last 7 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7))
    cutoff_str = cutoff.strftime("%Y-%m-%dT00:00:00")

    session = _get_session()
    params = {
        "$where": f"occ_date >= '{cutoff_str}'",
        "$limit": 5000,
        "$select": "crime_type",
    }
    token = _app_token()
    if token:
        params["$$app_token"] = token

    for attempt in range(3):
        try:
            resp = session.get(
                f"{SOCRATA_BASE}/{CRIME_DATASET}.json",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json()
            break
        except Exception as e:
            if attempt == 2:
                logger.warning(f" violent crime fetch failed: {e}")
                return -1
            time.sleep(2 ** attempt * 2)
    else:
        return -1

    violent = sum(
        1 for r in rows
        if (r.get("crime_type") or "").lower().strip() in VIOLENT_CRIME_TYPES
    )

    # Debug: show breakdown so we can tune keywords over time
    all_types = Counter((r.get("crime_type") or "").lower().strip() for r in rows)
    matched_types = {k: v for k, v in all_types.items() if k in VIOLENT_CRIME_TYPES}
    total_all = len(rows)
    logger.info(f"  ✓ violent crime (7d): {violent:,} of {total_all:,} total incidents")
    for k, v in sorted(matched_types.items(), key=lambda x: -x[1]):
        logger.info(f"      {v:4d}  {k}")

    return violent


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now = datetime.now(timezone.utc)
    logger.info(f"Generating pulse.json at {now.isoformat()}")

    count_311 = _count_311_24h()
    count_fatal = _count_fatal_crashes_90d()
    count_violent = _count_violent_crime_7d()

    pulse = {
        "updated": now.isoformat(),
        "counters": [
            {
                "id": "311_24h",
                "label": "new 311 reports in the last 24h",
                "value": None if count_311 < 0 else count_311,
                "source": "Open311",
                "link": "https://311.austintexas.gov",
            },
            {
                "id": "fatal_crashes_90d",
                "label": "fatal crashes in the last 90 days",
                "value": None if count_fatal < 0 else count_fatal,
                "source": "APD Crash Data",
                "link": "https://data.austintexas.gov/d/y2wy-tgr5",
            },
            {
                "id": "violent_crime_7d",
                "label": "violent crime incidents in the last 7 days",
                "value": None if count_violent < 0 else count_violent,
                "source": "APD Crime Reports",
                "link": "https://data.austintexas.gov/d/fdj4-gpfu",
            },
        ],
    }

    out_path = Path(__file__).resolve().parent.parent / "docs" / "pulse.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(pulse, indent=2), encoding="utf-8")
    logger.info(f"Wrote {out_path.stat().st_size:,} bytes to {out_path}")


if __name__ == "__main__":
    main()