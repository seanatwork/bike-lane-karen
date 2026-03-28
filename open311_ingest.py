from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import requests
import typer

app = typer.Typer(add_completion=False, help="Ingest Austin Open311 requests into SQLite (no scraping).")


OPEN311_BASE_URL = "https://311.austintexas.gov/open311/v2"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str) -> datetime:
    # Open311 uses ISO8601 like "2026-03-27T00:36:19Z"
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _isoformat_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _chunks_90_days(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    cursor = start
    while cursor < end:
        nxt = min(cursor + timedelta(days=90), end)
        yield cursor, nxt
        cursor = nxt


@dataclass(frozen=True)
class ServiceGuess:
    service_code: str
    service_name: str
    count: int


def _requests_session(api_key: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "bike-lane-karen/0.1 (Open311 ingestion; contact: local script)",
            "Accept": "application/json",
        }
    )
    if api_key:
        s.headers["Authorization"] = f"Bearer {api_key}"
    return s


def _request_json_with_backoff(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    timeout_s: int = 45,
    max_retries: int = 8,
) -> Any:
    sleep_s = 2.0
    for attempt in range(max_retries):
        resp = session.get(url, params=params, timeout=timeout_s)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else sleep_s
            time.sleep(wait_s)
            sleep_s = min(sleep_s * 1.8, 60.0)
            continue

        if 500 <= resp.status_code < 600:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.8, 60.0)
            continue

        resp.raise_for_status()
        return resp.json()

    resp.raise_for_status()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS open311_requests (
          service_request_id TEXT PRIMARY KEY,
          service_code TEXT,
          service_name TEXT,
          status TEXT,
          status_notes TEXT,
          requested_datetime TEXT,
          updated_datetime TEXT,
          address TEXT,
          zipcode TEXT,
          lat REAL,
          long REAL,
          media_url TEXT,
          token TEXT,
          attributes_json TEXT,
          extended_attributes_json TEXT,
          notes_json TEXT,
          ingested_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_open311_service_code ON open311_requests(service_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_open311_requested_datetime ON open311_requests(requested_datetime)"
    )
    conn.commit()


def _upsert_requests(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    ingested_at = _isoformat_z(_utc_now())
    n = 0
    for r in rows:
        conn.execute(
            """
            INSERT INTO open311_requests (
              service_request_id, service_code, service_name, status, status_notes,
              requested_datetime, updated_datetime, address, zipcode, lat, long,
              media_url, token, attributes_json, extended_attributes_json, notes_json,
              ingested_at
            ) VALUES (
              :service_request_id, :service_code, :service_name, :status, :status_notes,
              :requested_datetime, :updated_datetime, :address, :zipcode, :lat, :long,
              :media_url, :token, :attributes_json, :extended_attributes_json, :notes_json,
              :ingested_at
            )
            ON CONFLICT(service_request_id) DO UPDATE SET
              status=excluded.status,
              status_notes=excluded.status_notes,
              updated_datetime=excluded.updated_datetime,
              address=excluded.address,
              zipcode=excluded.zipcode,
              lat=excluded.lat,
              long=excluded.long,
              media_url=excluded.media_url,
              token=excluded.token,
              attributes_json=excluded.attributes_json,
              extended_attributes_json=excluded.extended_attributes_json,
              notes_json=excluded.notes_json,
              ingested_at=excluded.ingested_at
            """,
            {
                "service_request_id": str(r.get("service_request_id") or ""),
                "service_code": r.get("service_code"),
                "service_name": r.get("service_name"),
                "status": r.get("status"),
                "status_notes": r.get("status_notes"),
                "requested_datetime": r.get("requested_datetime"),
                "updated_datetime": r.get("updated_datetime"),
                "address": r.get("address"),
                "zipcode": r.get("zipcode"),
                "lat": r.get("lat"),
                "long": r.get("long"),
                "media_url": r.get("media_url"),
                "token": r.get("token"),
                "attributes_json": json.dumps(r.get("attributes")) if r.get("attributes") is not None else None,
                "extended_attributes_json": json.dumps(r.get("extended_attributes"))
                if r.get("extended_attributes") is not None
                else None,
                "notes_json": json.dumps(r.get("notes")) if r.get("notes") is not None else None,
                "ingested_at": ingested_at,
            },
        )
        n += 1
    conn.commit()
    return n


def _discover_service_code_via_q(
    session: requests.Session,
    query: str,
    *,
    max_pages: int = 5,
    per_page: int = 100,
) -> list[ServiceGuess]:
    url = f"{OPEN311_BASE_URL}/requests.json"
    counts: dict[tuple[str, str], int] = {}
    for page in range(1, max_pages + 1):
        payload = _request_json_with_backoff(
            session,
            url,
            params={
                "q": query,
                "per_page": per_page,
                "page": page,
            },
        )
        if not isinstance(payload, list) or len(payload) == 0:
            break
        for r in payload:
            sc = str(r.get("service_code") or "")
            sn = str(r.get("service_name") or "")
            if not sc or not sn:
                continue
            counts[(sc, sn)] = counts.get((sc, sn), 0) + 1

    guesses = [
        ServiceGuess(service_code=sc, service_name=sn, count=c)
        for (sc, sn), c in counts.items()
    ]
    guesses.sort(key=lambda g: g.count, reverse=True)
    return guesses


@app.command("discover")
def discover(
    query: str = typer.Argument(..., help="Text query for Open311 `q` parameter (e.g. 'Parking Violation')."),
    api_key: Optional[str] = typer.Option(None, help="Optional Open311 API key to raise limits."),
    max_pages: int = typer.Option(5, min=1, max=50),
    per_page: int = typer.Option(100, min=1, max=100, help="Results per page for discovery."),
) -> None:
    """Discover likely `service_code`s by searching recent requests (works even if services.json is flaky)."""
    session = _requests_session(api_key)
    guesses = _discover_service_code_via_q(session, query, max_pages=max_pages, per_page=per_page)
    if not guesses:
        typer.echo("No matches found. Try a different query.")
        raise typer.Exit(code=2)

    typer.echo("Likely service codes (most frequent first):")
    for g in guesses[:30]:
        typer.echo(f"- {g.service_code}\t{g.service_name}\t(count={g.count})")


@app.command("ingest")
def ingest(
    service_code: str = typer.Option(..., help="Open311 service_code to ingest (e.g. discovered via `discover`)."),
    start_date: str = typer.Option(..., help="UTC ISO8601 (e.g. 2026-01-01T00:00:00Z)."),
    end_date: Optional[str] = typer.Option(None, help="UTC ISO8601; defaults to now."),
    db_path: str = typer.Option("311_categories.db", help="SQLite DB path to store `open311_requests`."),
    api_key: Optional[str] = typer.Option(None, help="Optional Open311 API key to raise limits."),
    per_page: int = typer.Option(100, min=1, max=100, help="Requests per page (API commonly caps at 100)."),
    extensions: bool = typer.Option(True, help="Request `extensions=true` for nested fields."),
    max_pages_per_window: int = typer.Option(1000, min=1, help="Safety cap for pagination per 90-day window."),
    min_interval_s: float = typer.Option(
        6.2,
        min=0.0,
        help="Minimum seconds between successful requests when no API key is provided.",
    ),
) -> None:
    """Ingest Open311 requests for a service_code, backfilled in 90-day slices."""
    session = _requests_session(api_key)
    url = f"{OPEN311_BASE_URL}/requests.json"

    start_dt = _parse_dt(start_date)
    end_dt = _parse_dt(end_date) if end_date else _utc_now()
    if start_dt >= end_dt:
        raise typer.BadParameter("start_date must be < end_date")

    conn = sqlite3.connect(db_path)
    try:
        _ensure_schema(conn)
        total = 0
        for w_start, w_end in _chunks_90_days(start_dt, end_dt):
            typer.echo(f"Ingest window {w_start.date()} → {w_end.date()} (UTC)")
            window_inserted = 0
            seen_ids_window: set[str] = set()
            seen_page_fingerprints: set[tuple[str, ...]] = set()
            for page in range(1, max_pages_per_window + 1):
                params: dict[str, Any] = {
                    "service_code": service_code,
                    "start_date": _isoformat_z(w_start),
                    "end_date": _isoformat_z(w_end),
                    "per_page": per_page,
                    "page": page,
                }
                if extensions:
                    params["extensions"] = "true"

                payload = _request_json_with_backoff(session, url, params=params)
                if not isinstance(payload, list) or len(payload) == 0:
                    break

                page_ids = tuple(
                    str(r.get("service_request_id") or "")
                    for r in payload
                    if r.get("service_request_id")
                )
                if page_ids and page_ids in seen_page_fingerprints:
                    typer.echo("  Repeated page fingerprint detected; stopping window pagination.")
                    break
                if page_ids:
                    seen_page_fingerprints.add(page_ids)

                new_rows = []
                for r in payload:
                    srid = str(r.get("service_request_id") or "")
                    if not srid:
                        continue
                    if srid in seen_ids_window:
                        continue
                    seen_ids_window.add(srid)
                    new_rows.append(r)

                if not new_rows:
                    typer.echo("  No net-new IDs on page; stopping window pagination.")
                    break

                inserted = _upsert_requests(conn, new_rows)
                window_inserted += inserted
                if not api_key and min_interval_s > 0:
                    time.sleep(min_interval_s)

            total += window_inserted
            typer.echo(f"  Upserted {window_inserted} rows.")

        typer.echo(f"Done. Total upserted rows: {total}")
    finally:
        conn.close()


def main() -> None:
    app()


if __name__ == "__main__":
    main()

