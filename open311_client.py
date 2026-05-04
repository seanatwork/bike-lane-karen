"""
Shared Open311 API retry logic used by all map-generating modules.

Improvements over per-module implementations:
- Respects the Retry-After response header when the server sends one
- 6 retries instead of 3 for rate-limit errors
- Exponential backoff starts at 15s for 429s (vs 10s before)
"""
import time
import logging
import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 8
RETRY_DELAY = 2.0
RETRYABLE_HTTP_CODES = {423, 429, 500, 502, 503, 504}
RETRYABLE_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)


def open311_get(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 0,
) -> list:
    """GET request to the Open311 API with retry/backoff logic.

    On a 429, checks the Retry-After response header first; falls back to
    exponential backoff (15s, 30s, 60s, 120s, 240s, 480s) if not present.
    Retries up to MAX_RETRIES times before re-raising.
    """
    try:
        resp = session.get(url, params=params, timeout=45)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status in RETRYABLE_HTTP_CODES and retries < MAX_RETRIES:
            retry_after = e.response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = 15.0 * (2 ** retries)
            elif status in {423, 429}:
                delay = 15.0 * (2 ** retries)
            else:
                delay = RETRY_DELAY * (2 ** retries)
            logger.warning(
                f"HTTP {status}, retrying in {delay:.1f}s ({retries + 1}/{MAX_RETRIES})"
            )
            time.sleep(delay)
            return open311_get(session, url, params, retries + 1)
        raise
    except RETRYABLE_ERRORS as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logger.warning(
                f"Request failed ({e}), retrying in {delay:.1f}s ({retries + 1}/{MAX_RETRIES})"
            )
            time.sleep(delay)
            return open311_get(session, url, params, retries + 1)
        raise


# ── Telegram deep-link helper for map popups ──────────────────────────────────

TELEGRAM_BOT_USERNAME = "austin311bot"


def telegram_subscribe_link(lat: float, lon: float, alert_code: str = "311") -> str:
    """Build a t.me deep link that pre-fills the alerts subscription flow.

    alert_code is "311" for nearby_311 alerts or "animal" for animal_nearby.
    Lat/lon are encoded as signed integer microdegrees so the payload only
    contains characters Telegram permits ([A-Za-z0-9_-]).
    """
    lat_int = int(round(lat * 1_000_000))
    lon_int = int(round(lon * 1_000_000))
    return f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=sub_{alert_code}_{lat_int}_{lon_int}"


def subscribe_popup_html(lat: float, lon: float, alert_code: str = "311") -> str:
    """HTML snippet to embed at the bottom of a Folium map popup."""
    href = telegram_subscribe_link(lat, lon, alert_code)
    return (
        '<div style="margin-top:6px;padding-top:6px;border-top:1px solid #e5e7eb;">'
        f'<a href="{href}" target="_blank" '
        'style="color:#0088cc;font-size:12px;text-decoration:none;font-weight:600;">'
        '🔔 Alert me near here →</a></div>'
    )
