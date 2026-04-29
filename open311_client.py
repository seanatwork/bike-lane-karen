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
