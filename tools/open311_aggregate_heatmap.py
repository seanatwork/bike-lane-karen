from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import typer

app = typer.Typer(add_completion=False, help="Aggregate Open311 requests into public-safe spatial bins.")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _isoformat_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS open311_parking_heatmap_bins (
          window_start TEXT NOT NULL,
          window_end TEXT NOT NULL,
          bin_precision INTEGER NOT NULL,
          bin_lat REAL NOT NULL,
          bin_long REAL NOT NULL,
          bin_id TEXT NOT NULL,
          count_requests INTEGER NOT NULL,
          count_open INTEGER NOT NULL,
          count_closed INTEGER NOT NULL,
          PRIMARY KEY (window_start, window_end, bin_precision, bin_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_heatmap_bins_window
        ON open311_parking_heatmap_bins(window_start, window_end, bin_precision)
        """
    )
    conn.commit()


def _bin_round(value: float, precision: int) -> float:
    # precision = decimal places (e.g. 3 ≈ ~110m lat)
    return round(float(value), precision)


@dataclass(frozen=True)
class BinKey:
    lat: float
    lon: float

    def id(self, precision: int) -> str:
        return f"{self.lat:.{precision}f},{self.lon:.{precision}f}"


@app.command("run")
def run(
    service_code: str = typer.Option(..., help="Open311 service_code to aggregate (parking enforcement)."),
    window_days: int = typer.Option(30, min=1, max=3650, help="Aggregate over the last N days."),
    end_date: Optional[str] = typer.Option(None, help="UTC ISO8601 end date; defaults to now."),
    bin_precision: int = typer.Option(
        3, min=2, max=5, help="Decimal rounding for bins: 3≈110m lat, 4≈11m lat."
    ),
    db_path: str = typer.Option("311_categories.db", help="SQLite DB path containing `open311_requests`."),
    min_count: int = typer.Option(3, min=1, help="Suppress bins with fewer than N requests (privacy)."),
) -> None:
    """
    Aggregate Open311 request points into rounded-lat/long bins.

    This writes aggregate-only output into `open311_parking_heatmap_bins`.
    """
    end_dt = _parse_dt(end_date) if end_date else _utc_now()
    start_dt = end_dt - timedelta(days=window_days)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_schema(conn)

        rows = conn.execute(
            """
            SELECT lat, long, status
            FROM open311_requests
            WHERE service_code = ?
              AND requested_datetime >= ?
              AND requested_datetime < ?
              AND lat IS NOT NULL
              AND long IS NOT NULL
            """,
            (service_code, _isoformat_z(start_dt), _isoformat_z(end_dt)),
        ).fetchall()

        if not rows:
            typer.echo("No rows found for that window/service_code with coordinates.")
            raise typer.Exit(code=2)

        bins: dict[str, dict[str, int | float]] = {}
        for r in rows:
            lat = _bin_round(r["lat"], bin_precision)
            lon = _bin_round(r["long"], bin_precision)
            if math.isnan(lat) or math.isnan(lon):
                continue
            key = BinKey(lat=lat, lon=lon).id(bin_precision)
            if key not in bins:
                bins[key] = {
                    "bin_lat": lat,
                    "bin_long": lon,
                    "count_requests": 0,
                    "count_open": 0,
                    "count_closed": 0,
                }
            bins[key]["count_requests"] = int(bins[key]["count_requests"]) + 1
            if str(r["status"]).lower() == "open":
                bins[key]["count_open"] = int(bins[key]["count_open"]) + 1
            if str(r["status"]).lower() == "closed":
                bins[key]["count_closed"] = int(bins[key]["count_closed"]) + 1

        window_start_s = _isoformat_z(start_dt)
        window_end_s = _isoformat_z(end_dt)

        conn.execute(
            """
            DELETE FROM open311_parking_heatmap_bins
            WHERE window_start = ? AND window_end = ? AND bin_precision = ?
            """,
            (window_start_s, window_end_s, bin_precision),
        )

        inserted = 0
        for bin_id, agg in bins.items():
            if int(agg["count_requests"]) < min_count:
                continue
            conn.execute(
                """
                INSERT INTO open311_parking_heatmap_bins (
                  window_start, window_end, bin_precision, bin_lat, bin_long, bin_id,
                  count_requests, count_open, count_closed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    window_start_s,
                    window_end_s,
                    bin_precision,
                    float(agg["bin_lat"]),
                    float(agg["bin_long"]),
                    bin_id,
                    int(agg["count_requests"]),
                    int(agg["count_open"]),
                    int(agg["count_closed"]),
                ),
            )
            inserted += 1

        conn.commit()
        typer.echo(f"Wrote {inserted} bins (min_count={min_count}) for {len(rows)} points.")
        typer.echo(f"Window: {window_start_s} → {window_end_s} (precision={bin_precision})")
    finally:
        conn.close()


def main() -> None:
    app()


if __name__ == "__main__":
    main()

