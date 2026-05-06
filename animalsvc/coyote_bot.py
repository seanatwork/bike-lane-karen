"""
Coyote Complaints — data layer and formatters.

Queries Austin Open311 API for coyote complaints (service code: ACCOYTE).
Provides hotspot analysis, seasonal patterns, and neighborhood statistics.

Fun fact: Coyote pupping season is March–May, which typically spikes complaints.
"""

import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

OPEN311_BASE_URL = "https://311.austintexas.gov/open311/v2"
TIMEOUT = 45
MAX_RETRIES = 8
RETRY_DELAY = 1.0

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Get or create HTTP session with retry logic."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/0.1 (Open311 coyote queries)",
        })
    return _session


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fetch_coyote_complaints(days_back: int, limit: int = 500) -> list:
    """Fetch coyote complaints from Open311 API with pagination."""
    session = _get_session()
    end = _utc_now()
    start = end - timedelta(days=days_back)
    
    params = {
        "service_code": "ACCOYTE",
        "start_date": _isoformat_z(start),
        "end_date": _isoformat_z(end),
        "per_page": min(limit, 500),
        "page": 1,
    }
    
    all_records = []
    try:
        while True:
            url = f"{OPEN311_BASE_URL}/requests.json"
            resp = session.get(url, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            
            if not isinstance(data, list) or not data:
                break
                
            all_records.extend(data)
            
            # Check if we need to paginate
            if len(data) < params["per_page"]:
                break
                
            params["page"] += 1
            if len(all_records) >= limit:
                break
                
    except Exception as e:
        logger.warning(f"Failed to fetch coyote complaints: {e}")
        
    return all_records[:limit]


# =============================================================================
# SEASONAL PATTERN ANALYSIS
# =============================================================================

_MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]


def get_seasonal_patterns(days_back: int = 365) -> dict:
    """Analyze coyote complaints by month to identify seasonal patterns.
    
    Coyote pupping season is March-May, when pups are born and parents are
    more active seeking food and defending dens.
    """
    records = _fetch_coyote_complaints(days_back)
    
    if not records:
        return {"monthly": {}, "pupping_season": None, "total": 0, "days_back": days_back}
    
    # Group by month
    monthly_counts = defaultdict(int)
    pupping_season_count = 0  # Mar-May
    non_pupping_count = 0
    
    for r in records:
        date_str = r.get("requested_datetime", "")
        if not date_str:
            continue
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            month_key = f"{dt.year}-{dt.month:02d}"
            monthly_counts[month_key] += 1
            
            # Track pupping season (March=3, April=4, May=5)
            if dt.month in (3, 4, 5):
                pupping_season_count += 1
            else:
                non_pupping_count += 1
        except ValueError:
            continue
    
    # Sort monthly data
    sorted_months = dict(sorted(monthly_counts.items(), reverse=True))
    
    # Calculate pupping season impact
    total = pupping_season_count + non_pupping_count
    pupping_pct = (pupping_season_count / total * 100) if total > 0 else 0
    
    # Get top 3 months
    top_months = sorted(monthly_counts.items(), key=lambda x: -x[1])[:3]
    top_month_keys = [m[0] for m in top_months]
    
    return {
        "monthly": sorted_months,
        "pupping_season": {
            "count": pupping_season_count,
            "percentage": round(pupping_pct, 1),
            "months": "March–May",
            "is_peak": any(m[0].endswith(("-03", "-04", "-05")) for m in top_months),
        },
        "top_months": top_months,
        "total": total,
        "days_back": days_back,
    }


def format_seasonal_patterns(data: dict) -> str:
    """Format seasonal pattern data into readable Markdown."""
    total = data.get("total", 0)
    if total == 0:
        return "🐺 No coyote complaints found to analyze seasonal patterns."
    
    days_back = data.get("days_back", 365)
    pupping = data.get("pupping_season", {})
    top_months = data.get("top_months", [])
    
    msg = f"🐺 *Coyote Complaints — Seasonal Patterns*\n"
    msg += f"_Last {days_back} days · {total} total complaints_\n\n"
    
    # Pupping season analysis
    if pupping:
        pct = pupping.get("percentage", 0)
        count = pupping.get("count", 0)
        is_peak = pupping.get("is_peak", False)
        
        msg += f"🌸 *Pupping Season (March–May)*\n"
        if is_peak:
            msg += f"   📈 *Peak activity!* {count} complaints ({pct}% of total)\n"
            msg += f"   _Coyote pups are born in spring; parents are more active_\n"
        else:
            msg += f"   {count} complaints ({pct}% of total)\n"
            msg += f"   _Not the peak period this year_\n"
        msg += "\n"
    
    # Top months
    if top_months:
        msg += f"📊 *Busiest Months:*\n"
        for i, (month_key, count) in enumerate(top_months, 1):
            year, month = month_key.split("-")
            month_name = _MONTH_NAMES[int(month) - 1]
            medal = "🥇" if i == 1 else ("🥈" if i == 2 else "🥉")
            msg += f"   {medal} *{month_name} {year}:* {count} complaints\n"
        msg += "\n"
    
    # Monthly breakdown
    monthly = data.get("monthly", {})
    if monthly:
        msg += f"📅 *Monthly Breakdown:*\n"
        max_count = max(monthly.values())
        
        for month_key, count in sorted(monthly.items(), reverse=True):
            year, month = month_key.split("-")
            month_name = _MONTH_NAMES[int(month) - 1]
            bar_len = round(count / max_count * 10) if max_count > 0 else 0
            bar = "█" * bar_len + "░" * (10 - bar_len)
            
            # Highlight pupping season months
            is_pupping = int(month) in (3, 4, 5)
            marker = "🌸" if is_pupping else "  "
            msg += f"   {marker} *{month_name} {year}:* {bar} {count}\n"
    
    msg += "\n_Source: [Austin Open311 API](https://311.austintexas.gov/open311/v2)_"
    return msg


# =============================================================================
# NEIGHBORHOOD HOTSPOTS
# =============================================================================

def _extract_neighborhood(address: str) -> str:
    """Extract neighborhood/area indicator from address."""
    addr = address.replace(", Austin", "").strip()
    
    # Remove leading house number
    parts = addr.split(" ", 1)
    if len(parts) == 2 and parts[0].isdigit():
        addr = parts[1].strip()
    
    # Use street name or intersection as neighborhood proxy
    # Austin neighborhoods often correlate with major streets/areas
    return addr if addr else "Unknown"


def _extract_area(address: str) -> str:
    """Extract broader area from coordinates or address."""
    lat = address.get("lat")
    lon = address.get("long")
    
    if lat and lon:
        # Rough Austin area mapping by coordinates
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            
            # North/Central/South/West Austin approximation
            if lat_f >= 30.40:
                return "North Austin"
            elif lat_f >= 30.30:
                if lon_f <= -97.80:
                    return "West Austin"
                elif lon_f >= -97.70:
                    return "East Austin"
                else:
                    return "Central Austin"
            elif lat_f >= 30.20:
                return "South Austin"
            else:
                return "South Austin"
        except (ValueError, TypeError):
            pass
    
    return "Unknown"


def get_hotspots(days_back: int = 365) -> dict:
    """Get coyote complaint hotspots by area and street."""
    records = _fetch_coyote_complaints(days_back)
    
    if not records:
        return {"street_hotspots": [], "area_hotspots": [], "total": 0, "days_back": days_back}
    
    # Street-level hotspots
    street_counts = defaultdict(int)
    street_details = defaultdict(lambda: defaultdict(int))
    
    # Area-level hotspots (broader geographic areas)
    area_counts = defaultdict(int)
    area_details = defaultdict(lambda: {"count": 0, "streets": set()})
    
    for r in records:
        address = r.get("address", "")
        street = _extract_neighborhood(address)
        area = _extract_area(r)
        status = (r.get("status") or "").lower()
        
        street_counts[street] += 1
        street_details[street][status] += 1
        
        area_counts[area] += 1
        area_details[area]["count"] += 1
        if street != "Unknown":
            area_details[area]["streets"].add(street)
    
    # Sort hotspots
    street_hotspots = sorted(street_counts.items(), key=lambda x: -x[1])[:15]
    area_hotspots = sorted(area_counts.items(), key=lambda x: -x[1])
    
    return {
        "street_hotspots": street_hotspots,
        "area_hotspots": area_hotspots,
        "street_details": dict(street_details),
        "area_details": {k: {"count": v["count"], "streets": list(v["streets"])} for k, v in area_details.items()},
        "total": len(records),
        "days_back": days_back,
    }


def format_hotspots(data: dict) -> str:
    """Format hotspot data into readable Markdown."""
    total = data.get("total", 0)
    if total == 0:
        return "🐺 No coyote complaints found for hotspot analysis."
    
    days_back = data.get("days_back", 365)
    area_hotspots = data.get("area_hotspots", [])
    street_hotspots = data.get("street_hotspots", [])
    area_details = data.get("area_details", {})
    
    msg = f"🐺 *Coyote Complaint Hotspots*\n"
    msg += f"_Last {days_back} days · {total} total complaints_\n\n"
    
    # Area-level breakdown
    if area_hotspots:
        msg += f"🗺️ *By Area:*\n"
        max_area_count = area_hotspots[0][1] if area_hotspots else 1
        
        for area, count in area_hotspots:
            bar_len = round(count / max_area_count * 10) if max_area_count > 0 else 0
            bar = "█" * bar_len
            msg += f"   *{area}*: {bar} {count}\n"
            
            # Show top streets in this area
            details = area_details.get(area, {})
            streets = details.get("streets", [])
            if streets:
                top_streets = sorted(streets)[:3]
                street_str = " · ".join(st.split(",")[0] for st in top_streets)
                msg += f"   _{street_str}_\n"
        msg += "\n"
    
    # Street-level hotspots
    if street_hotspots:
        msg += f"🏘️ *Top Streets:*\n"
        max_street_count = street_hotspots[0][1] if street_hotspots else 1
        
        for i, (street, count) in enumerate(street_hotspots[:10], 1):
            bar_len = round(count / max_street_count * 10) if max_street_count > 0 else 0
            bar = "█" * bar_len
            msg += f"   {i}. *{street}*\n"
            msg += f"      {bar} {count} complaint{'s' if count > 1 else ''}\n"
    
    msg += "\n💡 _Pupping season (Mar–May) typically sees increased activity_\n"
    msg += "_Source: [Austin Open311 API](https://311.austintexas.gov/open311/v2)_"
    return msg


# =============================================================================
# COMPREHENSIVE OVERVIEW
# =============================================================================

def get_coyote_overview(days_back: int = 365) -> dict:
    """Get a comprehensive overview of coyote complaints."""
    records = _fetch_coyote_complaints(days_back)
    
    if not records:
        return {"total": 0, "days_back": days_back}
    
    # Basic stats
    total = len(records)
    closed = sum(1 for r in records if (r.get("status") or "").lower() == "closed")
    open_count = total - closed
    
    # Response time for closed tickets
    response_times = []
    for r in records:
        if (r.get("status") or "").lower() != "closed":
            continue
        req_str = r.get("requested_datetime", "")
        upd_str = r.get("updated_datetime", "")
        if not req_str or not upd_str:
            continue
        try:
            req = datetime.fromisoformat(req_str.replace("Z", "+00:00"))
            upd = datetime.fromisoformat(upd_str.replace("Z", "+00:00"))
            days = (upd - req).days
            if 0 <= days <= 365:
                response_times.append(days)
        except ValueError:
            continue
    
    avg_response = round(sum(response_times) / len(response_times), 1) if response_times else None
    
    # Geographic spread
    areas = set()
    for r in records:
        area = _extract_area(r)
        if area != "Unknown":
            areas.add(area)
    
    return {
        "total": total,
        "closed": closed,
        "open": open_count,
        "avg_response_days": avg_response,
        "areas_affected": len(areas),
        "days_back": days_back,
    }


def format_overview(data: dict) -> str:
    """Format overview data into readable Markdown."""
    total = data.get("total", 0)
    if total == 0:
        return "🐺 No coyote complaints found."
    
    days_back = data.get("days_back", 365)
    closed = data.get("closed", 0)
    open_count = data.get("open", 0)
    avg_response = data.get("avg_response_days")
    areas = data.get("areas_affected", 0)
    
    msg = f"🐺 *Coyote Complaints Overview*\n"
    msg += f"_Last {days_back} days_\n\n"
    
    msg += f"📊 *Total complaints:* {total}\n"
    msg += f"   ✅ Closed: {closed}\n"
    msg += f"   🔴 Open: {open_count}\n\n"
    
    if avg_response is not None:
        msg += f"⏱️ *Avg response time:* {avg_response} days\n\n"
    
    msg += f"🗺️ *Areas affected:* {areas}\n"
    
    msg += "\n💡 _Use /coyote seasonal or /coyote hotspots for detailed views_\n"
    msg += "_Source: [Austin Open311 API](https://311.austintexas.gov/open311/v2)_"
    return msg
