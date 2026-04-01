"""
Parking Enforcement — data layer and formatters.

Queries Austin Open311 API live for PARKINGV (Parking Violation Enforcement) service requests.
"""

import re
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

OPEN311_BASE_URL = "https://311.austintexas.gov/open311/v2"
SERVICE_CODE = "PARKINGV"
TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 1.0

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
            "User-Agent": "austin311bot/0.1 (Open311 parking queries)",
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
            logger.warning(f"Request failed ({e}), retrying in {delay:.1f}s ({retries+1}/{MAX_RETRIES})")
            time.sleep(delay)
            return _make_request(params, retries + 1)
        raise


def get_recent_citations(limit: int = 10, days_back: int = 90) -> list:
    """Return most recent parking citations from the past N days."""
    end = _utc_now()
    start = end - timedelta(days=days_back)
    params = {
        "service_code": SERVICE_CODE,
        "start_date": _isoformat_z(start),
        "end_date": _isoformat_z(end),
        "per_page": limit,
        "page": 1,
    }
    logger.debug(f"Fetching recent parking citations (last {days_back} days)")
    return _make_request(params)


def get_stats(days_back: int = 90) -> dict:
    """Return meaningful statistics for parking citations."""
    citations = get_recent_citations(limit=100, days_back=days_back)
    if not citations:
        return {"total": 0, "days_back": days_back}

    now = _utc_now()
    resolution_days = []
    open_tickets = []
    street_counts: dict = {}
    hourly_counts: dict = defaultdict(int)

    # Split into two halves to show trend
    half = timedelta(days=days_back // 2)
    cutoff = now - half
    recent_half = 0
    older_half = 0

    for r in citations:
        status = (r.get("status") or "").lower()
        requested_str = r.get("requested_datetime") or ""
        updated_str = r.get("updated_datetime") or ""

        # Resolution time for closed tickets
        if status == "closed" and requested_str and updated_str:
            try:
                req = datetime.fromisoformat(requested_str.replace("Z", "+00:00"))
                upd = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                days = (upd - req).days
                if 0 <= days <= 365:
                    resolution_days.append(days)
            except ValueError:
                pass

        # Open tickets
        if status == "open":
            open_tickets.append(r)

        # Top streets from address
        address = r.get("address") or ""
        parts = address.replace(", Austin", "").strip().split()
        if len(parts) >= 2:
            street = " ".join(parts[1:])
            street_counts[street] = street_counts.get(street, 0) + 1

        # Hourly distribution
        if requested_str:
            try:
                req = datetime.fromisoformat(requested_str.replace("Z", "+00:00"))
                hourly_counts[req.hour] += 1
                if req >= cutoff:
                    recent_half += 1
                else:
                    older_half += 1
            except ValueError:
                pass

    avg_resolution = round(sum(resolution_days) / len(resolution_days), 1) if resolution_days else None
    top_streets = sorted(street_counts.items(), key=lambda x: -x[1])[:5]

    # Peak hour
    peak_hour = max(hourly_counts.items(), key=lambda x: x[1])[0] if hourly_counts else None

    # Oldest unresolved citation
    oldest_open = None
    if open_tickets:
        def req_date(r):
            try:
                return datetime.fromisoformat((r.get("requested_datetime") or "").replace("Z", "+00:00"))
            except ValueError:
                return now
        oldest = min(open_tickets, key=req_date)
        oldest_dt = req_date(oldest)
        oldest_open = {
            "id": oldest.get("service_request_id"),
            "address": oldest.get("address"),
            "days_ago": (now - oldest_dt).days,
        }

    return {
        "total": len(citations),
        "open": len(open_tickets),
        "closed": len(citations) - len(open_tickets),
        "avg_resolution_days": avg_resolution,
        "top_streets": top_streets,
        "peak_hour": peak_hour,
        "hourly_counts": dict(hourly_counts),
        "recent_half": recent_half,
        "older_half": older_half,
        "half_days": days_back // 2,
        "oldest_open": oldest_open,
        "days_back": days_back,
    }


def get_hotspots(days_back: int = 90) -> dict:
    """Return citation counts grouped by street for hot zone analysis."""
    citations = get_recent_citations(limit=100, days_back=days_back)
    if not citations:
        return {"hotspots": [], "total": 0, "days_back": days_back}

    street_counts: dict = {}
    street_locations: dict = {}

    for r in citations:
        address = r.get("address") or ""
        lat = r.get("lat")
        lon = r.get("long")

        # Extract street name
        parts = address.replace(", Austin", "").strip().split()
        if len(parts) >= 2:
            street = " ".join(parts[1:])
        else:
            street = address or "Unknown"

        street_counts[street] = street_counts.get(street, 0) + 1

        # Store location for first occurrence
        if street not in street_locations and lat and lon:
            street_locations[street] = (lat, lon)

    hotspots = sorted(street_counts.items(), key=lambda x: -x[1])

    return {
        "hotspots": hotspots,
        "locations": street_locations,
        "total": len(citations),
        "days_back": days_back,
    }


def format_citations(citations: list, title: str = "🅿️ Parking Citations") -> str:
    if not citations:
        return "📝 No parking citations found for that search."

    msg = f"{title}\n\n"
    msg += f"Showing {len(citations)} citation(s):\n\n"

    for i, r in enumerate(citations, 1):
        req_id = r.get("service_request_id") or "N/A"
        address = r.get("address") or "Address not available"
        status = (r.get("status") or "unknown").upper()
        requested = r.get("requested_datetime") or ""
        if "T" in requested:
            requested = requested.split("T")[0]
        notes = (r.get("status_notes") or "").strip()

        status_emoji = "🟢" if status == "CLOSED" else "🔴"

        msg += f"{i}. {status_emoji} *{address[:60]}*\n"
        msg += f"   📅 {requested} | 🎫 #{req_id}\n"
        if notes and len(notes) < 80:
            msg += f"   📝 {notes}\n"
        msg += "\n"

        if i >= 10:
            remaining = len(citations) - i
            if remaining > 0:
                msg += f"... and {remaining} more.\n"
            break

    return msg


def format_stats(stats: dict) -> str:
    if stats.get("total", 0) == 0:
        return f"📝 No parking citations found in the past {stats.get('days_back', 90)} days."

    total = stats["total"]
    msg = "🅿️ *Parking Enforcement — Last 90 Days*\n\n"

    # Volume trend
    recent = stats.get("recent_half", 0)
    older = stats.get("older_half", 0)
    half = stats.get("half_days", 45)
    if older > 0:
        trend = round(((recent - older) / older) * 100)
        arrow = "📈" if trend > 0 else "📉" if trend < 0 else "➡️"
        trend_str = f"+{trend}%" if trend > 0 else f"{trend}%"
        msg += f"{arrow} *Volume trend:* {trend_str} (last {half} days vs prior {half})\n"
    msg += f"📊 *Total citations:* {total} ({stats['open']} open · {stats['closed']} closed)\n\n"

    # Resolution time
    if stats.get("avg_resolution_days") is not None:
        msg += f"⏱ *Avg resolution time:* {stats['avg_resolution_days']} days\n\n"

    # Peak time
    peak = stats.get("peak_hour")
    if peak is not None:
        msg += f"🕐 *Peak reporting:* {peak:02d}:00\n\n"

    # Top streets (hot zones)
    top = stats.get("top_streets", [])
    if top:
        msg += "🔥 *Hot zones (top streets):*\n"
        for street, count in top:
            msg += f"   {street}: {count} citation{'s' if count > 1 else ''}\n"
        msg += "\n"

    # Oldest unresolved
    oldest = stats.get("oldest_open")
    if oldest:
        msg += f"🕰 *Oldest open ticket:* #{oldest['id']}\n"
        msg += f"   {oldest['address']} — {oldest['days_ago']} days unresolved\n"

    return msg


def format_hotspots(data: dict) -> str:
    hotspots = data.get("hotspots", [])
    locations = data.get("locations", {})
    total = data.get("total", 0)
    days_back = data.get("days_back", 90)

    if not hotspots:
        return "📝 No parking enforcement data found."

    msg = f"🅿️ *Parking Enforcement Hot Zones*\n"
    msg += f"_Last {days_back} days · {total} total citations_\n\n"

    top = hotspots[:8]
    max_count = top[0][1]

    for i, (street, count) in enumerate(top, 1):
        bar = "█" * min(10, round(count / max_count * 10))
        msg += f"{i}. *{street}*\n"
        msg += f"   {bar} {count} citation{'s' if count > 1 else ''}\n"
        if street in locations:
            lat, lon = locations[street]
            msg += f"   📍 {lat:.4f}, {lon:.4f}\n"
        msg += "\n"

    return msg
