"""
Water Conservation Violations — data layer and formatters.

Queries Austin Open311 API for service code WWREPORT (Water Conservation Violation).
Residents report sprinkler misuse, leaks, water waste, and irrigation violations.
Austin Water investigates and sends postcards or confirms violations.
"""

import re
import time
import logging
import requests
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

OPEN311_BASE_URL = "https://311.austintexas.gov/open311/v2"
SERVICE_CODE     = "WWREPORT"
TIMEOUT          = 10
MAX_RETRIES      = 3
RETRY_DELAY      = 1.0

RETRYABLE_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/0.1 (Open311 water conservation queries)",
        })
    return _session


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_request(params: dict, retries: int = 0) -> list:
    session = _get_session()
    url = f"{OPEN311_BASE_URL}/requests.json"
    try:
        resp = session.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except RETRYABLE_ERRORS as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logger.warning(f"Water conservation request failed ({e}), retrying in {delay:.1f}s")
            time.sleep(delay)
            return _make_request(params, retries + 1)
        raise


def _fetch_violations(days_back: int, limit: int = 100) -> list:
    end   = _utc_now()
    start = end - timedelta(days=days_back)
    results = []
    page = 1

    while True:
        batch = _make_request({
            "service_code": SERVICE_CODE,
            "start_date":   _isoformat_z(start),
            "end_date":     _isoformat_z(end),
            "per_page":     limit,
            "page":         page,
        })
        if not batch:
            break
        results.extend(batch)
        if len(batch) < limit:
            break
        page += 1

    return results


# Map status_notes prefixes → readable outcome labels
_OUTCOME_PATTERNS = [
    (r"violation confirmed|confirmed violation",  "✅ Violation confirmed"),
    (r"2nd post",                                 "📬 2nd warning issued"),
    (r"postcard sent",                            "📬 Warning postcard sent"),
    (r"action already taken",                     "✔️ Action already taken"),
    (r"no problem found",                         "🔍 No problem found"),
    (r"insufficient information",                 "❓ Insufficient info"),
    (r"invalid complaint",                        "🚫 Invalid complaint"),
    (r"under investigation",                      "🔎 Under investigation"),
    (r"internal procedures",                      "✅ Violation confirmed"),
]

def _classify_outcome(status_notes: str) -> str:
    lower = (status_notes or "").lower()
    for pattern, label in _OUTCOME_PATTERNS:
        if re.search(pattern, lower):
            return label
    if not status_notes or not status_notes.strip():
        return "🔎 Under investigation"
    return "📋 Other"


# Bucket description text into violation types
_VIOLATION_TYPE_PATTERNS = [
    (r"rain|raining|rainy",               "🌧️ Watering during rain"),
    (r"wrong day|off day|not.*day|day.*not", "📅 Wrong watering day"),
    (r"leak|leaking|broken.*pipe|pipe.*broken", "🔧 Leak / broken pipe"),
    (r"flow.*street|street.*flow|gutter|runoff|overflow|drain", "🌊 Runoff into street"),
    (r"sprinkler|irrigation|spraying",    "💦 Sprinkler / irrigation"),
    (r"hose|washing|car wash|pressure wash", "🪣 Hose / washing"),
    (r"pool|fountain",                    "🏊 Pool / fountain"),
]

def _classify_violation_type(description: str) -> str:
    lower = (description or "").lower()
    for pattern, label in _VIOLATION_TYPE_PATTERNS:
        if re.search(pattern, lower):
            return label
    return "💧 Other water waste"


def _extract_street(address: str) -> str:
    """Pull street name from a full address like '1234 Main St, Austin'."""
    if not address:
        return ""
    # Strip house number and city suffix
    parts = address.split(",")
    street = parts[0].strip()
    street = re.sub(r"^\d+\s+", "", street)  # remove leading house number
    return street.title()


def get_water_conservation_stats(days_back: int = 90) -> dict:
    """Fetch and summarise water conservation violations."""
    records = _fetch_violations(days_back)

    total  = len(records)
    open_  = sum(1 for r in records if (r.get("status") or "").lower() == "open")
    closed = total - open_

    # Outcome breakdown
    outcome_counts: Counter = Counter()
    for r in records:
        outcome_counts[_classify_outcome(r.get("status_notes", ""))] += 1

    # Violation type from description
    type_counts: Counter = Counter()
    for r in records:
        type_counts[_classify_violation_type(r.get("description", ""))] += 1

    # Hotspot streets
    street_counts: Counter = Counter()
    for r in records:
        street = _extract_street(r.get("address", ""))
        if street:
            street_counts[street] += 1

    confirmed = sum(
        v for k, v in outcome_counts.items()
        if "confirmed" in k.lower()
    )

    return {
        "days_back":     days_back,
        "total":         total,
        "open":          open_,
        "closed":        closed,
        "confirmed":     confirmed,
        "outcomes":      outcome_counts.most_common(6),
        "violation_types": type_counts.most_common(6),
        "top_streets":   street_counts.most_common(6),
    }


def format_water_conservation(stats: dict) -> str:
    total    = stats.get("total", 0)
    days     = stats["days_back"]

    if total == 0:
        return f"💧 *Water Conservation Violations*\n\nNo reports found in the last {days} days."

    open_    = stats["open"]
    closed   = stats["closed"]
    confirmed = stats["confirmed"]
    per_day  = round(total / days, 1)

    msg  = f"💧 *Austin Water Conservation Violations — Last {days} Days*\n"
    msg += f"_Reports of sprinkler misuse, leaks, and water waste_\n\n"

    msg += f"📊 *Overview:*\n"
    msg += f"• Reports: {total:,} (~{per_day}/day)\n"
    msg += f"• Open: {open_}  ·  Closed: {closed}\n"
    if confirmed:
        confirm_pct = round(confirmed / total * 100)
        msg += f"• Confirmed violations: {confirmed} ({confirm_pct}%)\n"
    msg += "\n"

    outcomes = stats.get("outcomes", [])
    if outcomes:
        msg += "📋 *Investigation Outcomes:*\n"
        for label, cnt in outcomes:
            msg += f"  {label}: {cnt}\n"
        msg += "\n"

    vtypes = stats.get("violation_types", [])
    if vtypes:
        msg += "🚿 *Violation Types:*\n"
        for label, cnt in vtypes:
            msg += f"  {label}: {cnt}\n"
        msg += "\n"

    streets = stats.get("top_streets", [])
    if streets:
        msg += "📍 *Most Reports by Street:*\n"
        for street, cnt in streets:
            msg += f"  {street}: {cnt}\n"
        msg += "\n"

    msg += "_Source: [Austin 311 — Water Conservation](https://311.austintexas.gov)_"
    return msg
