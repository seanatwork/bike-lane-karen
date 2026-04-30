"""Pre-fetch all court page Socrata data and write docs/court/data.json.

The court page JS already has a `cache` dict keyed by dataset ID combos.
This script writes a JSON file with the exact same keys so the page can
load it on startup and skip all live Socrata fetches.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://data.austintexas.gov/resource"
OUT  = Path("docs/court/data.json")

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "austin311bot/court-cache"})


def get(dataset_id: str, params: dict) -> list:
    resp = SESSION.get(f"{BASE}/{dataset_id}.json", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_grouped(ds_id: str, charge_field: str, status_field: str) -> list:
    return get(ds_id, {
        "$select": f"{charge_field},{status_field},count(*) as cnt",
        "$group":  f"{charge_field},{status_field}",
        "$limit":  2000,
    })


def fetch_demo(ds_id: str) -> dict:
    """Fetch race and gender counts — only cases where race IS recorded."""
    where = "race IS NOT NULL"
    race = get(ds_id, {
        "$select": "race,count(*) as cnt",
        "$where":  where,
        "$group":  "race",
        "$order":  "cnt DESC",
    })
    gender = get(ds_id, {
        "$select": "defendant_gender,count(*) as cnt",
        "$where":  where,
        "$group":  "defendant_gender",
        "$order":  "cnt DESC",
    })
    return {"race": race, "gender": gender}


def fetch_monthly_trend() -> dict:
    """Fetch Municipal Court monthly filing counts for FY2025 and FY2026.

    offense_date is stored as text ("YYYY-MM-DD"), so date functions fail;
    substring(offense_date, 1, 7) extracts the "YYYY-MM" prefix instead.
    """
    sel = "substring(offense_date,1,7) as month,count(*) as cnt"
    grp = "month"
    ord_ = "month"
    fy25_rows = get("t47c-f82f", {"$select": sel, "$group": grp, "$order": ord_, "$limit": 20})
    fy26_rows = get("tuwa-vk6q", {"$select": sel, "$group": grp, "$order": ord_, "$limit": 20})
    return {"fy25Rows": fy25_rows, "fy26Rows": fy26_rows}


def fetch_dacc_comparison() -> dict:
    """Pre-fetch DACC FY2025 vs FY2026 comparison using the same elapsed period."""
    from datetime import date, timedelta
    today = date.today()
    fy25_end = today.replace(year=today.year - 1).isoformat()

    sel = "charges_description,count(*) as cnt"
    grp = "charges_description"
    fy25_where = f"offense_date >= '2024-10-01' AND offense_date <= '{fy25_end}'"
    fy26_where  = "offense_date >= '2025-10-01'"

    fy25_rows = get("emdh-pf9u", {"$select": sel, "$where": fy25_where, "$group": grp, "$limit": 500})
    fy26_rows = get("88wy-rigr",  {"$select": sel, "$where": fy26_where, "$group": grp, "$limit": 500})

    return {"fy25Rows": fy25_rows, "fy26Rows": fy26_rows, "fy25EndStr": fy25_end}


def main():
    DATASETS = [
        ("tuwa-vk6q", "offense_charge_description", "case_closed"),
        ("qc59-phn7", "charge_description",         "disposition"),
    ]

    DEMO_ID = "tuwa-vk6q"

    cache = {}

    print("Fetching grouped caseload/disposition datasets...")
    for ds_id, charge_field, status_field in DATASETS:
        key = f"{ds_id}:{charge_field}:{status_field}"
        print(f"  {key}")
        cache[key] = fetch_grouped(ds_id, charge_field, status_field)

    print("Fetching monthly trend (FY2025 + FY2026 Municipal Court)...")
    cache["monthly-trend"] = fetch_monthly_trend()

    print("Fetching DACC year-over-year comparison...")
    cache["dacc-comparison"] = fetch_dacc_comparison()

    print("Fetching demographics...")
    cache[f"demo:{DEMO_ID}"] = fetch_demo(DEMO_ID)

    payload = {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cache": cache,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    size = OUT.stat().st_size
    print(f"Written {size:,} bytes to {OUT}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
