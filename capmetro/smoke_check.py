"""
MetroBike Trip Data — Smoke Check & Schema Verification.

Follows the same pattern as homeless/socrata_smoke_check.py.
Verifies the Socrata dataset schema and runs basic data quality checks.
"""

import os
import re
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.austintexas.gov/resource"
DATASET_ID = "tyfh-5r8s"
TIMEOUT = 30
MAX_RETRIES = 3

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/0.1 (MetroBike smoke check)",
        })
    return _session


def _make_request(url: str, params: dict, retries: int = 0) -> list:
    session = _get_session()
    try:
        resp = session.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        if retries < MAX_RETRIES:
            delay = 2 ** retries
            logger.warning(f"Request failed ({e}), retrying in {delay}s...")
            time.sleep(delay)
            return _make_request(url, params, retries + 1)
        raise


def verify_schema() -> dict:
    """Fetch one record and report all field names."""
    logger.info("=" * 60)
    logger.info("STEP 0: Schema Verification")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {"$limit": 1}

    try:
        data = _make_request(url, params)
        if not data:
            logger.error("No records returned from dataset")
            return {}

        record = data[0]
        fields = list(record.keys())

        logger.info(f"Dataset: {DATASET_ID}")
        logger.info(f"Total fields: {len(fields)}")
        logger.info("\nField names found:")
        for field in sorted(fields):
            logger.info(f"  - {field}")

        return {f: record[f] for f in fields}

    except Exception as e:
        logger.error(f"Schema verification failed: {e}")
        return {}


def run_total_trips_check() -> dict:
    """Check total trip count and year range."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: Total Trips & Year Range")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"

    # Total count
    params = {"$select": "count(trip_id) as total"}
    data = _make_request(url, params)
    total = int(data[0]["total"]) if data else 0
    logger.info(f"Total trips: {total:,}")

    # Year range
    params = {"$select": "min(year) as first, max(year) as last"}
    data = _make_request(url, params)
    first = int(data[0]["first"]) if data else 0
    last = int(data[0]["last"]) if data else 0
    logger.info(f"Year range: {first} - {last}")

    return {"total": total, "first_year": first, "last_year": last}


def run_bike_type_check() -> dict:
    """Check electric vs classic distribution."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Bike Type Distribution")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {"$select": "bike_type, count(trip_id) as count", "$group": "bike_type"}
    data = _make_request(url, params)

    counts = {}
    for row in data:
        bt = row.get("bike_type", "unknown")
        c = int(row.get("count", 0))
        counts[bt] = c
        logger.info(f"  {bt}: {c:,}")

    return counts


def run_kiosk_count_check() -> dict:
    """Count unique kiosks."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Kiosk Count")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {"$select": "count(distinct(checkout_kiosk_id)) as count"}
    data = _make_request(url, params)
    count = int(data[0]["count"]) if data else 0
    logger.info(f"Unique kiosk IDs: {count}")

    return {"unique_kiosks": count}


def run_membership_check() -> dict:
    """Check membership type distribution (top 10)."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Top Membership Types")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "membership_type, count(trip_id) as count",
        "$group": "membership_type",
        "$order": "count DESC",
        "$limit": 10,
    }
    data = _make_request(url, params)

    for row in data:
        mt = row.get("membership_type", "Unknown")
        c = int(row.get("count", 0))
        logger.info(f"  {mt}: {c:,}")

    return {row.get("membership_type", ""): int(row.get("count", 0)) for row in data}


def run_kiosk_location_coverage() -> dict:
    """Check how many kiosks have hardcoded locations."""
    from capmetro.metrobike import KIOSK_LOCATIONS, find_kiosk_id_by_name

    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Kiosk Location Coverage")
    logger.info("=" * 60)

    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "checkout_kiosk_id, count(trip_id) as trips",
        "$group": "checkout_kiosk_id",
        "$order": "trips DESC",
        "$limit": 200,
    }
    data = _make_request(url, params)

    total_kiosks = 0
    located = 0
    missing = []
    located_trips = 0
    total_trips = 0

    for row in data:
        kid = (row.get("checkout_kiosk_id") or "").strip()
        trips = int(row.get("trips", 0))
        if not kid or kid == "#N/A" or kid == "nan":
            continue
        total_kiosks += 1
        total_trips += trips

        if kid in KIOSK_LOCATIONS:
            located += 1
            located_trips += trips
        else:
            missing.append((kid, trips))

    logger.info(f"Kiosks with locations: {located} / {total_kiosks}")
    logger.info(f"Trip coverage: {located_trips:,} / {total_trips:,} ({located_trips/total_trips*100:.1f}%)")

    if missing:
        logger.warning(f"\nMissing kiosk IDs ({len(missing)}):")
        for kid, trips in sorted(missing, key=lambda x: -x[1])[:10]:
            logger.warning(f"  ID {kid}: {trips:,} trips")

    return {
        "total_kiosks": total_kiosks,
        "located": located,
        "coverage_pct": round(located / total_kiosks * 100, 1) if total_kiosks else 0,
        "trip_coverage_pct": round(located_trips / total_trips * 100, 1) if total_trips else 0,
        "missing": missing[:10],
    }


def run_smoke_check():
    """Run all smoke checks."""
    logger.info("\n" + "=" * 60)
    logger.info("METROBIKE TRIP DATA — SMOKE CHECK")
    logger.info("=" * 60)

    schema = verify_schema()
    if not schema:
        logger.error("Schema verification failed. Aborting.")
        return

    totals = run_total_trips_check()
    bike_types = run_bike_type_check()
    kiosks = run_kiosk_count_check()
    memberships = run_membership_check()
    coverage = run_kiosk_location_coverage()

    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Dataset: {DATASET_ID}")
    logger.info(f"Total trips: {totals['total']:,}")
    logger.info(f"Year range: {totals['first_year']} - {totals['last_year']}")
    logger.info(f"Unique kiosks: {kiosks['unique_kiosks']}")
    logger.info(f"Bike types: {bike_types}")
    logger.info(f"Location coverage: {coverage['coverage_pct']}% of kiosks, {coverage['trip_coverage_pct']}% of trips")

    if coverage["missing"]:
        logger.warning(f"\nMissing kiosk IDs to geocode: {[m[0] for m in coverage['missing']]}")

    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    run_smoke_check()