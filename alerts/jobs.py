"""Background alert jobs: crime_daily, district_digest, nearby_311, animal_nearby, crash_nearby."""

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import requests

from alerts import db

logger = logging.getLogger(__name__)

CRIME_URL      = "https://data.austintexas.gov/resource/fdj4-gpfu.json"
OPEN311_URL    = "https://311.austintexas.gov/open311/v2/requests.json"
INCIDENTS_URL  = "https://data.austintexas.gov/resource/dx9v-zd7x.json"
CRIME_MAP      = "https://austin311.com/crime/"

ANIMAL_CODES = {"ACLONAG", "ACLOANIM", "ACBITE2", "COAACDD", "WILDEXPO", "ACINFORM"}
ANIMAL_LABELS = {
    "ACLONAG":  "Loose Dog",
    "ACLOANIM": "Loose Animal",
    "ACBITE2":  "Animal Bite",
    "COAACDD":  "Vicious Dog",
    "WILDEXPO": "Wildlife / Coyote",
    "ACINFORM": "Animal Concern",
}

CRASH_TYPES = {
    "crash urgent", "collision", "collision with injury",
    "collisn/ lvng scn", "collision/private property",
    "traffic fatality", "crash service",
}

DISTRICT_LABELS = {str(i): f"District {i}" for i in range(1, 11)}


def _headers() -> dict:
    token = os.getenv("AUSTINAPIKEY", "")
    return {"X-App-Token": token} if token else {}


# ── distance ───────────────────────────────────────────────────────────────────

def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── crime helpers ──────────────────────────────────────────────────────────────

def _fetch_district_crimes(district: str, start: datetime, end: datetime) -> list[dict]:
    try:
        resp = requests.get(
            CRIME_URL,
            params={
                "$where": (
                    f"council_district='{district}' "
                    f"AND rep_date >= '{start.strftime('%Y-%m-%dT%H:%M:%S')}' "
                    f"AND rep_date < '{end.strftime('%Y-%m-%dT%H:%M:%S')}'"
                ),
                "$limit": 2000,
                "$select": "incident_report_number,crime_type,rep_date",
            },
            headers=_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"crime fetch district={district}: {e}")
        return []


def _type_breakdown(rows: list[dict], top_n: int = 5) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in rows:
        ct = (r.get("crime_type") or "Unknown").title()
        counts[ct] = counts.get(ct, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1])[:top_n])


# ── daily crime alert ──────────────────────────────────────────────────────────

async def crime_daily_job(context) -> None:
    db.prune_sent_log()
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    today_str = now.strftime("%Y-%m-%d")

    subs = db.get_active_subscriptions("crime_daily")
    if not subs:
        return

    cache: dict[str, list] = {}
    for sub in subs:
        sub_id, district, chat_id = sub["id"], sub["district"], sub["chat_id"]
        if db.already_sent(sub_id, today_str):
            continue
        if district not in cache:
            cache[district] = _fetch_district_crimes(district, yesterday, now)
        rows = cache[district]
        label = DISTRICT_LABELS.get(district, f"District {district}")
        db.mark_sent(sub_id, today_str)
        if not rows:
            continue
        breakdown = _type_breakdown(rows)
        lines = "\n".join(f"  • {ct}: {cnt}" for ct, cnt in breakdown.items())
        others = len(rows) - sum(breakdown.values())
        if others > 0:
            lines += f"\n  • Other: {others}"
        msg = (
            f"🚨 *{label} — Daily Crime Report*\n"
            f"_{yesterday.strftime('%b %-d')} incidents newly reported_\n\n"
            f"*{len(rows)}* incident{'s' if len(rows) != 1 else ''} filed:\n"
            f"{lines}\n\n"
            f"[Full crime map →]({CRIME_MAP})\n"
            f"_/myalerts to manage_"
        )
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg,
                parse_mode="Markdown", disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"crime_daily send sub={sub_id}: {e}")


# ── weekly district digest ─────────────────────────────────────────────────────

async def district_digest_job(context) -> None:
    db.prune_sent_log()
    now        = datetime.now(timezone.utc)
    week_start = now - timedelta(days=7)
    prev_start = now - timedelta(days=14)
    week_key   = week_start.strftime("%Y-W%U")

    subs = db.get_active_subscriptions("district_digest")
    if not subs:
        return

    cache: dict[str, tuple] = {}
    for sub in subs:
        sub_id, district, chat_id = sub["id"], sub["district"], sub["chat_id"]
        if db.already_sent(sub_id, week_key):
            continue
        if district not in cache:
            cache[district] = (
                _fetch_district_crimes(district, week_start, now),
                _fetch_district_crimes(district, prev_start, week_start),
            )
        this_week, last_week = cache[district]
        label = DISTRICT_LABELS.get(district, f"District {district}")
        this_n, last_n = len(this_week), len(last_week)
        db.mark_sent(sub_id, week_key)
        if this_n == 0 and last_n == 0:
            continue
        if last_n > 0:
            pct   = round((this_n - last_n) / last_n * 100)
            arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
            trend = f"{arrow} {'+' if pct > 0 else ''}{pct}% vs last week ({last_n})"
        else:
            trend = f"📊 {this_n} incidents (no prior week data)"
        breakdown = _type_breakdown(this_week)
        lines = "\n".join(
            f"  {i+1}. {ct}: {cnt} ({round(cnt/this_n*100)}%)"
            for i, (ct, cnt) in enumerate(breakdown.items())
        ) if this_n else "  No incidents"
        date_range = f"{week_start.strftime('%b %-d')}–{now.strftime('%b %-d, %Y')}"
        msg = (
            f"📊 *{label} — Weekly Crime Digest*\n_{date_range}_\n\n"
            f"*{this_n}* incident{'s' if this_n != 1 else ''} reported\n{trend}\n\n"
            f"*Top offense types:*\n{lines}\n\n"
            f"[Full crime map →]({CRIME_MAP})\n_/myalerts to manage_"
        )
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg,
                parse_mode="Markdown", disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"district_digest send sub={sub_id}: {e}")


# ── nearby 311 job ─────────────────────────────────────────────────────────────

_SERVICE_ICONS = {
    "graffiti":   "🎨",
    "pothole":    "🕳️",
    "homeless":   "🏕️",
    "encampment": "🏕️",
    "noise":      "🔊",
    "parking":    "🅿️",
    "animal":     "🐾",
    "traffic":    "🚦",
    "sidewalk":   "🚶",
    "tree":       "🌳",
    "water":      "💧",
    "sign":       "🪧",
    "light":      "💡",
}


def _service_icon(service_name: str) -> str:
    name = service_name.lower()
    for keyword, icon in _SERVICE_ICONS.items():
        if keyword in name:
            return icon
    return "📋"


def _fetch_311_recent(start: datetime) -> list[dict]:
    """Fetch all citywide 311 requests since start datetime."""
    try:
        resp = requests.get(
            OPEN311_URL,
            params={
                "start_date": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "page_size":  1000,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"open311 fetch: {e}")
        return []


async def nearby_311_job(context) -> None:
    """Send daily digest of 311 requests near each subscriber's location."""
    db.prune_sent_log()
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    today_str = now.strftime("%Y-%m-%d")

    subs = db.get_active_subscriptions("nearby_311")
    if not subs:
        return

    # Fetch once, filter per subscription
    all_requests = _fetch_311_recent(yesterday)
    # Keep only requests with valid lat/long
    geotagged = [
        r for r in all_requests
        if r.get("lat") and r.get("long")
    ]

    for sub in subs:
        sub_id  = sub["id"]
        chat_id = sub["chat_id"]
        if not sub["params"]:
            continue
        if db.already_sent(sub_id, today_str):
            continue

        try:
            p = json.loads(sub["params"])
            center_lat = float(p["lat"])
            center_lon = float(p["lon"])
            radius     = float(p.get("radius_miles", 0.5))
        except Exception:
            continue

        nearby = [
            r for r in geotagged
            if _haversine_miles(center_lat, center_lon,
                                float(r["lat"]), float(r["long"])) <= radius
        ]

        db.mark_sent(sub_id, today_str)
        if not nearby:
            continue

        # Group by service name
        by_service: dict[str, int] = {}
        for r in nearby:
            svc = r.get("service_name", "Unknown")
            by_service[svc] = by_service.get(svc, 0) + 1
        top = sorted(by_service.items(), key=lambda x: -x[1])[:8]

        radius_label = f"{radius:.2g} mi"
        lines = "\n".join(
            f"  {_service_icon(svc)} {svc}: {cnt}"
            for svc, cnt in top
        )
        others = len(nearby) - sum(cnt for _, cnt in top)
        if others > 0:
            lines += f"\n  📋 Other: {others}"

        msg = (
            f"📍 *Nearby 311 Reports — {yesterday.strftime('%b %-d')}*\n"
            f"_Within {radius_label} of your location_\n\n"
            f"*{len(nearby)}* new request{'s' if len(nearby) != 1 else ''}:\n"
            f"{lines}\n\n"
            f"[View all maps →](https://austin311.com)\n"
            f"_/myalerts to manage_"
        )
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg,
                parse_mode="Markdown", disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"nearby_311 send sub={sub_id}: {e}")


# ── shared helper for location-based jobs ─────────────────────────────────────

def _load_location_sub(sub) -> tuple[float, float, float] | None:
    """Parse (lat, lon, radius_miles) from a subscription row, or None."""
    if not sub["params"]:
        return None
    try:
        p = json.loads(sub["params"])
        return float(p["lat"]), float(p["lon"]), float(p.get("radius_miles", 0.5))
    except Exception:
        return None


async def _send_location_alert(context, sub_id: int, chat_id: int, msg: str) -> None:
    try:
        await context.bot.send_message(
            chat_id=chat_id, text=msg,
            parse_mode="Markdown", disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"send sub={sub_id}: {e}")


# ── animal nearby job ──────────────────────────────────────────────────────────

def _fetch_animal_recent(start: datetime) -> list[dict]:
    """Fetch Open311 animal service requests since start, all codes in one pass."""
    results = []
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    for code in ANIMAL_CODES:
        try:
            resp = requests.get(
                OPEN311_URL,
                params={
                    "service_code": code,
                    "start_date":   start_str,
                    "page_size":    500,
                },
                timeout=20,
            )
            resp.raise_for_status()
            results.extend(resp.json())
        except Exception as e:
            logger.error(f"animal fetch code={code}: {e}")
    return [r for r in results if r.get("lat") and r.get("long")]


async def animal_nearby_job(context) -> None:
    """Daily digest of animal incidents near each subscriber's location."""
    db.prune_sent_log()
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    today_str = now.strftime("%Y-%m-%d")

    subs = db.get_active_subscriptions("animal_nearby")
    if not subs:
        return

    all_incidents = _fetch_animal_recent(yesterday)

    for sub in subs:
        sub_id, chat_id = sub["id"], sub["chat_id"]
        loc = _load_location_sub(sub)
        if not loc or db.already_sent(sub_id, today_str):
            continue

        center_lat, center_lon, radius = loc
        nearby = [
            r for r in all_incidents
            if _haversine_miles(center_lat, center_lon,
                                float(r["lat"]), float(r["long"])) <= radius
        ]
        db.mark_sent(sub_id, today_str)
        if not nearby:
            continue

        by_type: dict[str, int] = {}
        for r in nearby:
            label = ANIMAL_LABELS.get(r.get("service_code", ""), r.get("service_name", "Unknown"))
            by_type[label] = by_type.get(label, 0) + 1

        lines = "\n".join(
            f"  {'🐕' if 'Dog' in t else '🐺' if 'Coyote' in t or 'Wildlife' in t else '🐾'} {t}: {c}"
            for t, c in sorted(by_type.items(), key=lambda x: -x[1])
        )
        radius_label = f"{radius:.2g} mi"
        msg = (
            f"🐾 *Animal Incidents Near You — {yesterday.strftime('%b %-d')}*\n"
            f"_Within {radius_label} of your location_\n\n"
            f"*{len(nearby)}* report{'s' if len(nearby) != 1 else ''}:\n{lines}\n\n"
            f"_/myalerts to manage_"
        )
        await _send_location_alert(context, sub_id, chat_id, msg)


# ── crash nearby job ───────────────────────────────────────────────────────────

def _fetch_crashes_recent(start: datetime) -> list[dict]:
    """Fetch traffic incidents of crash type from the last 24h."""
    try:
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        resp = requests.get(
            INCIDENTS_URL,
            params={
                "$where":  f"published_date >= '{start_str}'",
                "$limit":  1000,
                "$select": "traffic_report_id,issue_reported,address,latitude,longitude,published_date,agency",
            },
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json()
        return [
            r for r in rows
            if r.get("issue_reported", "").lower().strip() in CRASH_TYPES
            and r.get("latitude") and r.get("longitude")
        ]
    except Exception as e:
        logger.error(f"crash fetch: {e}")
        return []


async def crash_nearby_job(context) -> None:
    """Daily digest of crashes near each subscriber's location."""
    db.prune_sent_log()
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    today_str = now.strftime("%Y-%m-%d")

    subs = db.get_active_subscriptions("crash_nearby")
    if not subs:
        return

    all_crashes = _fetch_crashes_recent(yesterday)

    for sub in subs:
        sub_id, chat_id = sub["id"], sub["chat_id"]
        loc = _load_location_sub(sub)
        if not loc or db.already_sent(sub_id, today_str):
            continue

        center_lat, center_lon, radius = loc
        nearby = [
            r for r in all_crashes
            if _haversine_miles(center_lat, center_lon,
                                float(r["latitude"]), float(r["longitude"])) <= radius
        ]
        db.mark_sent(sub_id, today_str)
        if not nearby:
            continue

        fatalities = sum(1 for r in nearby if "fatality" in r.get("issue_reported", "").lower())
        injuries   = sum(1 for r in nearby if "injury" in r.get("issue_reported", "").lower())
        lines = []
        for r in nearby[:8]:
            raw  = r.get("issue_reported", "Unknown").title()
            addr = r.get("address", "Unknown location")[:50]
            lines.append(f"  🚗 {raw} — {addr}")
        if len(nearby) > 8:
            lines.append(f"  _...and {len(nearby) - 8} more_")

        severity = ""
        if fatalities:
            severity = f"⚠️ *{fatalities} fatal crash{'es' if fatalities > 1 else ''}*\n"
        elif injuries:
            severity = f"⚠️ *{injuries} crash{'es' if injuries > 1 else ''} with injuries*\n"

        radius_label = f"{radius:.2g} mi"
        msg = (
            f"🚨 *Crashes Near You — {yesterday.strftime('%b %-d')}*\n"
            f"_Within {radius_label} of your location_\n\n"
            f"{severity}"
            f"*{len(nearby)}* crash report{'s' if len(nearby) != 1 else ''}:\n"
            f"\n".join(lines) + "\n\n"
            f"_/myalerts to manage_"
        )
        await _send_location_alert(context, sub_id, chat_id, msg)
