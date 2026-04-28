"""
Bicycle & Cycling Infrastructure — data layer and formatters.

Queries Austin Open311 API across 5 cycling-relevant service codes.
No keyword filtering — all records from these codes are included since
any obstruction, debris, or construction in the ROW is a hazard to cyclists
regardless of whether the reporter mentioned "bike" in their description.

Service codes:
- PWBICYCL: Bicycle Issues (primary — explicit bicycle complaints)
- OBSTMIDB: Obstruction in Right of Way (blocked lanes, illegally parked)
- SBDEBROW: Debris in Street (road hazards)
- ATCOCIRW: Construction Concerns in Right of Way (detours/closures)
- ZZARSTSW: Street Sweeping (bike lane cleaning requests)
"""

import re
import time
import logging
import requests
import os
import io
from open311_client import open311_get
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

OPEN311_BASE_URL = "https://311.austintexas.gov/open311/v2"
TIMEOUT = 12
MAX_RETRIES = 3
RETRY_DELAY = 1.0
MAX_PAGES = 10

API_KEY = os.getenv("AUSTIN_APP_TOKEN")

SERVICE_CODES = {
    "PWBICYCL": "Bicycle Issue",
    "OBSTMIDB": "Obstruction in ROW",
    "SBDEBROW": "Debris in Street",
    "ATCOCIRW": "Construction in ROW",
    "ZZARSTSW": "Street Sweeping",
}

RETRYABLE_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        headers = {
            "Accept": "application/json",
            "User-Agent": "austin311bot/0.1 (Open311 bicycle queries)",
        }
        if API_KEY:
            headers["X-Api-Key"] = API_KEY
        _session.headers.update(headers)
    return _session


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_request(params: dict) -> list:
    return open311_get(_get_session(), f"{OPEN311_BASE_URL}/requests.json", params)


def _fetch_code(service_code: str, days_back: int) -> list:
    """Fetch all requests for one service code with pagination."""
    end = _utc_now()
    start = end - timedelta(days=days_back)
    all_records: list = []
    seen_ids: set = set()
    page = 1

    while page <= MAX_PAGES:
        params = {
            "service_code": service_code,
            "start_date": _isoformat_z(start),
            "end_date": _isoformat_z(end),
            "per_page": 100,
            "page": page,
        }
        records = _make_request(params)
        if not records:
            break

        for r in records:
            sid = r.get("service_request_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                r["_service_label"] = SERVICE_CODES.get(service_code, service_code)
                r["_service_code"] = service_code
                all_records.append(r)

        if len(records) < 100:
            break

        page += 1
        time.sleep(1.0 if API_KEY else 2.0)

    return all_records


def fetch_bicycle_reports(days_back: int = 90) -> dict:
    """Fetch 311 reports across all cycling-relevant service codes.

    All records from these codes are included — no keyword filtering.
    """
    all_records: list = []
    by_code: dict = {}

    for code, label in SERVICE_CODES.items():
        try:
            records = _fetch_code(code, days_back)
            by_code[code] = {"label": label, "fetched": len(records)}
            all_records.extend(records)
            logger.debug(f"{code}: {len(records)} records")
        except Exception as e:
            logger.warning(f"Failed to fetch {code}: {e}")
            by_code[code] = {"label": label, "fetched": 0, "error": str(e)}
        time.sleep(3.0 if not API_KEY else 1.0)

    return {
        "records": all_records,
        "total_fetched": len(all_records),
        "days_back": days_back,
        "by_code": by_code,
        "fetched_at": _utc_now().strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── Telegram-facing functions (unchanged interface) ────────────────────────────

def get_recent_complaints(limit: int = 10, days_back: int = 90) -> list:
    """Return most recent bicycle complaints (PWBICYCL only)."""
    end = _utc_now()
    start = end - timedelta(days=days_back)
    params = {
        "service_code": "PWBICYCL",
        "start_date": _isoformat_z(start),
        "end_date": _isoformat_z(end),
        "per_page": limit,
        "page": 1,
    }
    return _make_request(params)


def lookup_ticket(ticket_id: str) -> Optional[dict]:
    """Look up any 311 service request by ticket ID."""
    session = _get_session()
    ticket_id = ticket_id.lstrip("#").strip()
    if re.match(r"^\d{10}$", ticket_id):
        ticket_id = f"{ticket_id[:2]}-{ticket_id[2:]}"
    url = f"{OPEN311_BASE_URL}/requests/{ticket_id}.json"
    try:
        resp = session.get(url, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data[0] if data else None
        return data
    except RETRYABLE_ERRORS as e:
        logger.warning(f"Ticket lookup failed ({e})")
        return None


def format_ticket(record: dict) -> str:
    ticket_id = record.get("service_request_id") or "N/A"
    service_name = record.get("service_name") or "Unknown"
    service_code = record.get("service_code") or "N/A"
    status = (record.get("status") or "unknown").upper()
    address = record.get("address") or "Address not available"
    lat = record.get("lat")
    lon = record.get("long")
    requested = record.get("requested_datetime") or ""
    updated = record.get("updated_datetime") or ""
    description = (record.get("description") or "").strip()
    notes = (record.get("status_notes") or "").strip()

    def fmt_dt(s):
        if not s:
            return "N/A"
        return s.replace("T", " ").replace("Z", " UTC")

    status_emoji = "🟢" if status == "CLOSED" else "🔴"
    msg = f"🎫 *311 Ticket Lookup*\n━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📋 *Service:* {service_name} `({service_code})`\n"
    msg += f"{status_emoji} *Status:* {status}\n\n"
    msg += f"📍 *Address:* {address}\n"
    if lat and lon:
        msg += f"🗺 *Coordinates:* {lat:.6f}, {lon:.6f}\n"
    msg += f"\n📅 *Filed:* {fmt_dt(requested)}\n"
    if updated:
        msg += f"🔄 *Last updated:* {fmt_dt(updated)}\n"
    if description:
        msg += f"\n💬 *Description:*\n{description}\n"
    if notes:
        msg += f"\n📝 *Status notes:*\n{notes}\n"
    if ticket_id != "N/A":
        msg += f"\n🔗 [View ticket on Austin 311](https://311.austintexas.gov/open311/v2/requests/{ticket_id}.json)"
    return msg


def format_complaints(complaints: list, title: str = "🚴 Bicycle Complaints") -> str:
    if not complaints:
        return "📝 No bicycle complaints found for that search."
    msg = f"{title}\n\nShowing {len(complaints)} complaint(s):\n\n"
    for i, r in enumerate(complaints, 1):
        req_id = r.get("service_request_id") or "N/A"
        address = r.get("address") or "Address not available"
        status = (r.get("status") or "unknown").upper()
        requested = (r.get("requested_datetime") or "").split("T")[0]
        description = r.get("description") or r.get("service_name") or "Bicycle complaint"
        msg += f"{i}. *{description[:80]}*\n"
        msg += f"📍 {address}\n"
        msg += f"🔖 Status: {status} | 📅 {requested}\n"
        msg += f"🎫 #{req_id}\n\n"
        if i >= 10:
            remaining = len(complaints) - i
            if remaining > 0:
                msg += f"... and {remaining} more.\n"
            break
    return msg


def get_stats(days_back: int = 90) -> dict:
    complaints = get_recent_complaints(limit=100, days_back=days_back)
    if not complaints:
        return {"total": 0, "days_back": days_back}
    now = _utc_now()
    resolution_days = []
    open_tickets = []
    street_counts: dict = {}
    for r in complaints:
        status = (r.get("status") or "").lower()
        requested_str = r.get("requested_datetime") or ""
        updated_str = r.get("updated_datetime") or ""
        if status == "closed" and requested_str and updated_str:
            try:
                req = datetime.fromisoformat(requested_str.replace("Z", "+00:00"))
                upd = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                days = (upd - req).days
                if 0 <= days <= 365:
                    resolution_days.append(days)
            except ValueError:
                pass
        if status == "open":
            open_tickets.append(r)
        address = r.get("address") or ""
        parts = address.replace(", Austin", "").strip().split()
        if len(parts) >= 2:
            street = " ".join(parts[1:])
            street_counts[street] = street_counts.get(street, 0) + 1
    avg_resolution = round(sum(resolution_days) / len(resolution_days), 1) if resolution_days else None
    top_streets = sorted(street_counts.items(), key=lambda x: -x[1])[:5]
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
        "total": len(complaints),
        "open": len(open_tickets),
        "closed": len(complaints) - len(open_tickets),
        "avg_resolution_days": avg_resolution,
        "top_streets": top_streets,
        "oldest_open": oldest_open,
        "days_back": days_back,
    }


def format_stats(stats: dict) -> str:
    if stats.get("total", 0) == 0:
        return f"📝 No bicycle complaints found in the past {stats.get('days_back', 90)} days."
    total = stats["total"]
    msg = "🚴 *Bicycle Complaints — Last 90 Days*\n\n"
    msg += f"📊 *Total complaints:* {total} ({stats['open']} open · {stats['closed']} closed)\n\n"
    if stats.get("avg_resolution_days") is not None:
        msg += f"⏱ *Avg resolution time:* {stats['avg_resolution_days']} days\n\n"
    top = stats.get("top_streets", [])
    if top:
        msg += "📍 *Most complained streets:*\n"
        for street, count in top:
            msg += f"   {street}: {count} complaint{'s' if count > 1 else ''}\n"
        msg += "\n"
    oldest = stats.get("oldest_open")
    if oldest:
        msg += f"🕰 *Oldest open ticket:* #{oldest['id']}\n"
        msg += f"   {oldest['address']} — {oldest['days_ago']} days unresolved\n"
    msg += "\n_Source: [Austin Open311 API](https://311.austintexas.gov/open311/v2)_"
    return msg


# =============================================================================
# MAP GENERATOR
# =============================================================================

# Open marker color/icon by service code
_CODE_STYLE = {
    "PWBICYCL": ("blue",      "info-sign"),
    "OBSTMIDB": ("red",       "exclamation-sign"),
    "SBDEBROW": ("orange",    "warning-sign"),
    "ATCOCIRW": ("purple",    "wrench"),
    "ZZARSTSW": ("cadetblue", "refresh"),
}
_DEFAULT_STYLE = ("gray", "info-sign")

# camelCase slugs — no underscores so split('_') on the layer key is safe
_CODE_SLUGS = {
    "PWBICYCL": "bicycleIssue",
    "OBSTMIDB": "obstruction",
    "SBDEBROW": "debris",
    "ATCOCIRW": "construction",
    "ZZARSTSW": "sweeping",
}
_DEFAULT_SLUG = "other"

_TYPE_OPTIONS = [
    ("bicycleIssue", "🔵 Bicycle Issue"),
    ("obstruction",  "🔴 Obstruction in ROW"),
    ("debris",       "🟠 Debris in Street"),
    ("construction", "🟣 Construction in ROW"),
    ("sweeping",     "🩵 Street Sweeping"),
]


def generate_bicycle_map(days_back: int = 90) -> tuple:
    """Generate an interactive HTML map of cycling-relevant 311 reports.

    Returns:
        tuple: (BytesIO buffer with HTML content, summary message)
    """
    try:
        import folium
        from folium.plugins import MarkerCluster
    except ImportError:
        return None, "❌ Map generation requires 'folium'. Install: pip install folium"

    import tempfile

    result = fetch_bicycle_reports(days_back)
    records_raw = result["records"]

    now_dt = _utc_now()
    records = []
    for r in records_raw:
        try:
            lat = float(r.get("lat") or 0)
            lon = float(r.get("long") or 0)
            if 30.0 <= lat <= 30.5 and -98.0 <= lon <= -97.5:
                r["_lat"] = lat
                r["_lon"] = lon
                records.append(r)
        except (TypeError, ValueError):
            pass

    if not records:
        return None, "🚴 No cycling reports with location data found."

    open_count   = sum(1 for r in records if (r.get("status") or "").lower() == "open")
    closed_count = len(records) - open_count

    def _age_days(r):
        try:
            dt = datetime.fromisoformat(r.get("requested_datetime", "").replace("Z", "+00:00"))
            return (now_dt - dt).days
        except Exception:
            return days_back

    # Count per type × bucket for summary bar
    all_slugs = list(_CODE_SLUGS.values()) + [_DEFAULT_SLUG]
    type_bucket_counts = {
        slug: {"30": {"open": 0, "closed": 0}, "60": {"open": 0, "closed": 0}, "90": {"open": 0, "closed": 0}}
        for slug in all_slugs + ["all"]
    }
    for r in records:
        age    = _age_days(r)
        status = (r.get("status") or "").lower()
        s      = status if status in ("open", "closed") else "closed"
        slug   = _CODE_SLUGS.get(r.get("_service_code", ""), _DEFAULT_SLUG)
        for bucket_days in (30, 60, 90):
            if age <= bucket_days:
                b = str(bucket_days)
                type_bucket_counts["all"][b][s]  += 1
                type_bucket_counts[slug][b][s]   += 1
    counts_js = str(type_bucket_counts).replace("'", '"')

    m = folium.Map(location=[30.2672, -97.7431], zoom_start=11, tiles="CartoDB positron")

    # Layer key: {status}_{bucket}_{typeSlug}
    fg_clusters = {}
    fg_objects  = {}
    for status_key in ("open", "closed"):
        for bucket in ("30", "60", "90"):
            for slug in all_slugs:
                name = f"{status_key}_{bucket}_{slug}"
                show = (bucket == "90")
                fg      = folium.FeatureGroup(name=name, show=show, overlay=True)
                cluster = MarkerCluster().add_to(fg)
                fg.add_to(m)
                fg_clusters[name] = cluster
                fg_objects[name]  = fg

    for r in records:
        lat     = r["_lat"]
        lon     = r["_lon"]
        status  = (r.get("status") or "").lower()
        code    = r.get("_service_code", "")
        label   = r.get("_service_label", "Cycling Report")
        desc    = (r.get("description") or "").strip()
        notes   = (r.get("status_notes") or "").strip()
        date_str    = (r.get("requested_datetime") or "").split("T")[0]
        updated_str = (r.get("updated_datetime") or "").split("T")[0]
        address = (r.get("address") or "").strip()
        req_id  = r.get("service_request_id", "N/A")

        age    = _age_days(r)
        bucket = "30" if age <= 30 else ("60" if age <= 60 else "90")
        slug   = _CODE_SLUGS.get(code, _DEFAULT_SLUG)
        cluster_key = f"{status}_{bucket}_{slug}"
        if cluster_key not in fg_clusters:
            cluster_key = f"closed_{bucket}_{slug}"

        address_line = (
            f'<b>Address:</b> <a href="https://www.google.com/maps/search/?api=1&query={lat},{lon}"'
            f' target="_blank">{address}</a><br/>'
        ) if address else ""
        updated_line = (
            f"<span style='color:#666;'>Updated: {updated_str}</span><br/>"
        ) if updated_str and updated_str != date_str else ""

        desc_block = ""
        if desc:
            short = (desc[:400] + "…") if len(desc) > 400 else desc
            desc_block = f"<b>Description:</b><br/><i>{short.replace(chr(10), '<br/>')}</i><br/>"
        elif notes:
            short = (notes[:300] + "…") if len(notes) > 300 else notes
            note_label = "Resolution" if status == "closed" else "Notes"
            desc_block = f"<b>{note_label}:</b><br/><i>{short}</i><br/>"

        ticket_url = f"https://311.austintexas.gov/tickets/{req_id}"
        popup_html = f"""
        <div style="font-family:sans-serif;max-width:300px;font-size:13px;">
            <b><a href="{ticket_url}" target="_blank" style="color:#0066cc;">Report #{req_id}</a></b><br/>
            <span style="color:#666;">Filed: {date_str}</span><br/>
            {updated_line}
            {address_line}
            <br/>
            <b>Status:</b> {'🔴 Open' if status == 'open' else '🟢 Closed'}<br/>
            <b>Type:</b> {label}<br/><br/>
            {desc_block}
        </div>
        """
        popup = folium.Popup(popup_html, max_width=300)

        if status == "open":
            color, icon_name = _CODE_STYLE.get(code, _DEFAULT_STYLE)
        else:
            color, icon_name = "green", "ok-sign"

        folium.Marker(
            location=[lat, lon],
            popup=popup,
            icon=folium.Icon(color=color, icon=icon_name, prefix="glyphicon"),
            tooltip=f"{'Open' if status == 'open' else 'Closed'}: {label}",
        ).add_to(fg_clusters[cluster_key])

    # Build type dropdown HTML
    type_options_html = '<option value="all">All Types</option>\n'
    for slug, label in _TYPE_OPTIONS:
        type_options_html += f'<option value="{slug}">{label}</option>\n'

    map_var      = m.get_name()
    layer_map_js = "{" + ", ".join(f'"{k}": {fg_objects[k].get_name()}' for k in fg_objects) + "}"

    panel_html = f"""
    <div id="map-panel" style="position:absolute;top:10px;left:50%;transform:translateX(-50%);
                background:white;padding:10px 16px;border-radius:6px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3);z-index:9999;
                font-family:sans-serif;text-align:center;">
        <b style="font-size:15px;">🚴 Austin Cycling 311 Reports</b><br/>
        <span id="map-summary" style="font-size:12px;color:#555;"></span>
        <div style="display:flex;justify-content:center;gap:4px;margin-top:7px;">
            <button id="btn-30" onclick="setDayFilter(30)" class="fbtn">30d</button>
            <button id="btn-60" onclick="setDayFilter(60)" class="fbtn">60d</button>
            <button id="btn-90" onclick="setDayFilter(90)" class="fbtn active">90d</button>
            <span style="margin:0 4px;color:#ccc;">|</span>
            <button id="btn-open"   onclick="toggleStatus('open')"   class="fbtn active">🔴 Open</button>
            <button id="btn-closed" onclick="toggleStatus('closed')" class="fbtn active">🟢 Closed</button>
        </div>
    </div>
    <div id="type-panel" style="position:absolute;top:10px;right:10px;
                background:white;padding:8px 12px;border-radius:6px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3);z-index:9999;
                font-family:sans-serif;">
        <label for="type-select" style="font-size:11px;font-weight:bold;color:#444;display:block;margin-bottom:4px;">Filter by Type</label>
        <select id="type-select" onchange="setTypeFilter(this.value)"
                style="font-size:12px;padding:3px 6px;border:1px solid #ccc;border-radius:4px;cursor:pointer;width:100%;">
            {type_options_html}
        </select>
    </div>
    <style>
        .fbtn {{ padding:3px 9px;border:1px solid #ccc;border-radius:4px;background:#f5f5f5;cursor:pointer;font-size:12px;color:#444; }}
        .fbtn.active {{ background:#2563eb;color:white;border-color:#2563eb; }}
        .fbtn:hover:not(.active) {{ background:#e0e7ff; }}
    </style>
    <script>
        var currentDays = 90;
        var showOpen    = true;
        var showClosed  = true;
        var currentType = 'all';
        var layerMap    = null;
        var leafletMap  = null;
        var typeBucketCounts = {counts_js};

        function updateSummary() {{
            var d       = String(currentDays);
            var catData = typeBucketCounts[currentType] || typeBucketCounts['all'];
            var counts  = catData[d] || {{}};
            var o = showOpen   ? (counts.open   || 0) : 0;
            var c = showClosed ? (counts.closed || 0) : 0;
            document.getElementById('map-summary').textContent =
                'Last ' + d + ' days · ' + (o + c) + ' total · ' + o + ' open · ' + c + ' closed';
        }}

        function initLayers() {{
            layerMap   = {layer_map_js};
            leafletMap = {map_var};
            updateLayers();
            updateSummary();
        }}

        function updateLayers() {{
            if (!layerMap || !leafletMap) return;
            Object.keys(layerMap).forEach(function(key) {{
                var parts    = key.split('_');
                var status   = parts[0];
                var bucket   = parseInt(parts[1]);
                var typeSlug = parts[2];
                var timeOk   = bucket <= currentDays;
                var statusOk = (status === 'open' && showOpen) || (status === 'closed' && showClosed);
                var typeOk   = (currentType === 'all') || (typeSlug === currentType);
                var layer    = layerMap[key];
                if (timeOk && statusOk && typeOk) {{
                    if (!leafletMap.hasLayer(layer)) leafletMap.addLayer(layer);
                }} else {{
                    if (leafletMap.hasLayer(layer)) leafletMap.removeLayer(layer);
                }}
            }});
        }}

        function setDayFilter(days) {{
            currentDays = days;
            [30, 60, 90].forEach(function(d) {{
                var btn = document.getElementById('btn-' + d);
                if (btn) btn.classList.toggle('active', d === days);
            }});
            updateLayers();
            updateSummary();
        }}

        function toggleStatus(status) {{
            if (status === 'open') showOpen = !showOpen;
            else showClosed = !showClosed;
            document.getElementById('btn-' + status).classList.toggle('active');
            updateLayers();
            updateSummary();
        }}

        function setTypeFilter(type) {{
            currentType = type;
            updateLayers();
            updateSummary();
        }}

        document.addEventListener('DOMContentLoaded', function() {{
            setTimeout(initLayers, 1000);
        }});
    </script>
    """
    m.get_root().html.add_child(folium.Element(panel_html))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as tmp:
        tmp_path = tmp.name
    try:
        m.save(tmp_path)
        with open(tmp_path, 'rb') as f:
            html_content = f.read()
        buffer = io.BytesIO(html_content)
        buffer.seek(0)
    finally:
        try:
            import os as _os
            _os.unlink(tmp_path)
        except Exception:
            pass

    # Breakdown by code for summary
    by_code = result.get("by_code", {})
    code_lines = "  ".join(
        f"{SERVICE_CODES[c]}: {by_code[c]['fetched']}"
        for c in SERVICE_CODES if c in by_code and by_code[c].get("fetched", 0) > 0
    )

    summary = (
        f"🚴 *Cycling 311 Reports Map*\n"
        f"_Last {days_back} days · all 5 cycling service codes_\n\n"
        f"📊 *{len(records):,} reports mapped*\n"
        f"🔴 *{open_count:,} open*  ·  🟢 *{closed_count:,} closed*\n\n"
        f"Use the type filter to focus on bicycle issues, obstructions, debris, construction, or street sweeping."
    )
    return buffer, summary
