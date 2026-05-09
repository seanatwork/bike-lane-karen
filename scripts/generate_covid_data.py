#!/usr/bin/env python3
"""
Generate docs/covid/data.json — COVID-19 Austin Code Non-Compliance complaints.

Fetches all AUSCODCO records from Open311 (service_code=AUSCODCO, ~964 historical
records from 2020–2021). No profanity censoring — raw text in the name of transparency.

Run:   python scripts/generate_covid_data.py
Output: docs/covid/data.json
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

OPEN311_URL = "https://311.austintexas.gov/open311/v2/requests.json"
SERVICE_CODE = "AUSCODCO"
PER_PAGE = 100
MAX_PAGES = 15  # 964 records / 100 = ~10 pages; cap at 15 for safety

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/covid-data (pre-generator)",
        })
    return _session


def _app_token() -> str:
    return os.getenv("AUSTINAPIKEY", os.getenv("OPEN311_API_KEY", ""))


# Auto-generated / procedural responses that aren't real complaints
_SKIP_PATTERNS = [
    "unable to make contact", "contact made", "general broadcast",
    "sent text message", "close sr", "no problem found",
    "private property", "referred to 311", "inspection performed",
    "no action needed", "future work scheduled", "work order",
    "duplicate", "resolved", "cite vehicle", "no violation",
]


def _is_real_complaint(text: str) -> bool:
    if not text or len(text.strip()) < 40:
        return False
    t = text.lower()
    for pat in _SKIP_PATTERNS:
        if pat in t:
            return False
    return True


def fetch_all_records() -> list[dict]:
    session = _get_session()
    token = _app_token()
    all_records = []

    for page in range(1, MAX_PAGES + 1):
        params: dict = {
            "service_code": SERVICE_CODE,
            "per_page": PER_PAGE,
            "page": page,
        }
        if token:
            params["api_key"] = token

        for attempt in range(3):
            try:
                resp = session.get(OPEN311_URL, params=params, timeout=30)
                resp.raise_for_status()
                records = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    logger.warning(f"  Page {page} failed after 3 attempts: {e}")
                    records = []
                else:
                    time.sleep(2 ** attempt * 2)

        if not records:
            logger.info(f"  Page {page}: empty — stopping")
            break

        all_records.extend(records)
        logger.info(f"  Page {page}: {len(records)} records (total so far: {len(all_records)})")

        if len(records) < PER_PAGE:
            logger.info("  Last page reached")
            break

        # Be polite to the API
        time.sleep(1.0 if token else 2.0)

    return all_records


def build_items(records: list[dict]) -> list[dict]:
    items = []
    seen = set()

    for r in records:
        desc = (r.get("description") or "").strip()
        if not _is_real_complaint(desc):
            continue

        # Deduplicate on first 80 chars
        key = desc[:80].lower()
        if key in seen:
            continue
        seen.add(key)

        addr = (r.get("address") or "").strip()
        # Clean up address
        addr = addr.replace(", Austin", "").replace(", TX", "").strip()

        date_raw = r.get("requested_datetime", "")
        date = date_raw[:10] if date_raw else ""

        items.append({
            "text": desc[:500],
            "address": addr,
            "date": date,
            "id": r.get("service_request_id", ""),
        })

    # Sort newest first
    items.sort(key=lambda x: x["date"], reverse=True)
    return items


def main() -> None:
    now = datetime.now(timezone.utc)
    logger.info(f"Fetching AUSCODCO (COVID-19 Austin Code Non-Compliance) records…")

    records = fetch_all_records()
    logger.info(f"Total fetched: {len(records)} records")

    items = build_items(records)
    logger.info(f"Descriptions with real complaints: {len(items)}")

    # Date range stats
    dates = [r.get("requested_datetime", "")[:10] for r in records if r.get("requested_datetime")]
    dates = [d for d in dates if d]
    date_min = min(dates) if dates else ""
    date_max = max(dates) if dates else ""

    data = {
        "updated": now.isoformat(),
        "total": len(records),
        "withDescription": len(items),
        "dateMin": date_min,
        "dateMax": date_max,
        "serviceCode": SERVICE_CODE,
        "items": items,
    }

    out_path = Path(__file__).resolve().parent.parent / "docs" / "covid" / "data.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    logger.info(f"Wrote {out_path.stat().st_size:,} bytes → {out_path}")


if __name__ == "__main__":
    main()
