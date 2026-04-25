"""Crime trends — monthly APD incident counts over 12 months."""

import io
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.austintexas.gov/resource"
CRIME_DATASET = "fdj4-gpfu"
LOOKBACK_DAYS = 365
TOP_TYPES = 5

LINE_COLORS = ["#3b82f6", "#ef4444", "#22c55e", "#f59e0b", "#8b5cf6"]

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        token = os.getenv("AUSTIN_APP_TOKEN", "")
        headers = {"Accept": "application/json", "User-Agent": "austin311bot/0.1 (crime trends)"}
        if token:
            headers["X-App-Token"] = token
        _session.headers.update(headers)
    return _session


def _rolling_avg(counts: list, window: int = 3) -> list:
    result = []
    for i in range(len(counts)):
        if i < window - 1:
            result.append(None)
        else:
            result.append(round(sum(counts[i - window + 1 : i + 1]) / window, 1))
    return result


def _fetch_monthly_by_type(days_back: int) -> list:
    """Fetch monthly crime counts grouped by month and crime_type via SoQL."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime(
        "%Y-%m-%dT00:00:00"
    )
    url = f"{SOCRATA_BASE}/{CRIME_DATASET}.json"
    params = {
        "$select": "date_trunc_ym(occ_date) as month, crime_type, count(*) as cnt",
        "$where": f"occ_date >= '{cutoff}' AND crime_type IS NOT NULL",
        "$group": "month, crime_type",
        "$order": "month ASC",
        "$limit": 100000,
    }
    resp = _get_session().get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _aggregate(rows: list) -> dict:
    # Accumulate totals per type and per month
    type_totals: dict = defaultdict(int)
    month_type: dict = defaultdict(lambda: defaultdict(int))
    all_months: set = set()

    for row in rows:
        month = (row.get("month") or "")[:7]  # "YYYY-MM"
        crime_type = (row.get("crime_type") or "").title()
        cnt = int(row.get("cnt", 0))
        if not month or not crime_type:
            continue
        type_totals[crime_type] += cnt
        month_type[month][crime_type] += cnt
        all_months.add(month)

    months = sorted(all_months)
    top_types = [t for t, _ in sorted(type_totals.items(), key=lambda x: -x[1])[:TOP_TYPES]]

    monthly_totals = [sum(month_type[m].values()) for m in months]
    total = sum(monthly_totals)
    avg_per_month = round(total / max(1, len(months)), 1)

    peak_idx = monthly_totals.index(max(monthly_totals)) if monthly_totals else -1
    peak_month = months[peak_idx] if peak_idx >= 0 else "—"
    peak_count = monthly_totals[peak_idx] if peak_idx >= 0 else 0

    type_datasets = []
    for i, crime_type in enumerate(top_types):
        type_datasets.append(
            {
                "label": crime_type,
                "data": [month_type[m].get(crime_type, 0) for m in months],
                "color": LINE_COLORS[i % len(LINE_COLORS)],
            }
        )

    return {
        "total": total,
        "months": months,
        "monthly_totals": monthly_totals,
        "rolling_avg": _rolling_avg(monthly_totals),
        "avg_per_month": avg_per_month,
        "peak_month": peak_month,
        "peak_count": peak_count,
        "type_datasets": type_datasets,
    }


def _render_html(data: dict, fetched_at: str) -> str:
    total = data["total"]
    avg_per_month = data["avg_per_month"]
    peak_month = data["peak_month"]
    peak_count = data["peak_count"]
    months = data["months"]

    peak_label = (
        datetime.strptime(peak_month, "%Y-%m").strftime("%b %Y")
        if peak_month not in ("—", "")
        else "—"
    )
    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]

    js_type_datasets = []
    for ds in data["type_datasets"]:
        js_type_datasets.append(
            {
                "label": ds["label"],
                "data": ds["data"],
                "borderColor": ds["color"],
                "backgroundColor": "transparent",
                "tension": 0.3,
                "pointRadius": 3,
                "pointHoverRadius": 5,
                "fill": False,
            }
        )

    payload = json.dumps(
        {
            "months": month_labels,
            "monthlyTotals": data["monthly_totals"],
            "rollingAvg": data["rolling_avg"],
            "typeDatasets": js_type_datasets,
        }
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Crime Trends</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }}
    #panel {{ position: sticky; top: 0; z-index: 100; background: #1e2230; border-bottom: 1px solid #2d3348; padding: 10px 16px 12px; display: flex; flex-direction: column; align-items: center; gap: 6px; }}
    #panel-title {{ font-size: 15px; font-weight: 700; color: #f1f5f9; }}
    #panel-subtitle {{ font-size: 12px; color: #64748b; text-align: center; }}
    #last-ran {{ font-size: 11px; color: #475569; }}
    .btn-row {{ display: flex; gap: 4px; flex-wrap: wrap; justify-content: center; }}
    .fbtn {{ background: #252b3b; border: 1px solid #3d4868; color: #94a3b8; padding: 5px 13px; border-radius: 4px; font-size: 12px; white-space: nowrap; text-decoration: none; display: inline-block; }}
    .fbtn:hover {{ background: #2d3453; color: #e2e8f0; }}
    #stats {{ border-bottom: 1px solid #2d3348; }}
    .stats-inner {{ display: flex; justify-content: center; }}
    .stat {{ flex: 1; max-width: 170px; text-align: center; padding: 10px 8px 9px; border-right: 1px solid #2d3348; }}
    .stat:last-child {{ border-right: none; }}
    .stat-value {{ font-size: 1.25rem; font-weight: 700; line-height: 1.1; }}
    .stat-label {{ font-size: 0.67rem; color: #475569; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 3px; }}
    .stat-sub {{ font-size: 0.67rem; color: #475569; margin-top: 1px; }}
    #chart-wrap {{ flex: 1; padding: 16px; display: flex; flex-direction: column; gap: 20px; max-width: 1100px; width: 100%; margin: 0 auto; }}
    .chart-block {{ background: #161a24; border: 1px solid #2d3348; border-radius: 8px; padding: 14px; }}
    .chart-title {{ font-size: 13px; font-weight: 600; color: #e2e8f0; margin-bottom: 10px; }}
    .chart-container {{ position: relative; height: 300px; }}
    footer {{ text-align: center; padding: 14px 16px; font-size: 0.74rem; color: #475569; border-top: 1px solid #1e2230; }}
    footer a {{ color: #64748b; text-decoration: none; }}
    footer a:hover {{ color: #94a3b8; }}
    @media (max-width: 520px) {{ .stat-value {{ font-size: 1rem; }} .chart-container {{ height: 240px; }} }}
  </style>
</head>
<body>
  <div id="panel">
    <div id="panel-title">🚔 Austin Crime Trends</div>
    <div id="panel-subtitle">Monthly APD incidents — last 12 months</div>
    <div id="last-ran">Last ran: {fetched_at}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Crime Map</a>
      <a class="fbtn" href="../../">Austin 311 Home</a>
    </div>
  </div>

  <div id="stats">
    <div class="stats-inner">
      <div class="stat">
        <div class="stat-value" style="color:#3b82f6;">{total:,}</div>
        <div class="stat-label">Total incidents</div>
        <div class="stat-sub">last 12 months</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#22c55e;">{avg_per_month:.0f}</div>
        <div class="stat-label">Avg / month</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#ef4444;">{peak_count:,}</div>
        <div class="stat-label">Peak month</div>
        <div class="stat-sub">{peak_label}</div>
      </div>
    </div>
  </div>

  <div id="chart-wrap">
    <div class="chart-block">
      <div class="chart-title">Monthly incidents (all types)</div>
      <div class="chart-container"><canvas id="totalChart"></canvas></div>
    </div>
    <div class="chart-block">
      <div class="chart-title">Top {TOP_TYPES} crime types over time</div>
      <div class="chart-container"><canvas id="typesChart"></canvas></div>
    </div>
  </div>

  <footer>
    Data: <a href="https://data.austintexas.gov/d/fdj4-gpfu" target="_blank" rel="noopener">APD Crime Reports (Socrata)</a>
    &nbsp;·&nbsp; <a href="../">← Crime Map</a>
    &nbsp;·&nbsp; <a href="../../">← Austin 311</a>
  </footer>

  <script>
    const DATA = {payload};

    const BASE_OPTS = {{
      plugins: {{
        legend: {{ labels: {{ color: "#94a3b8", font: {{ size: 11 }} }} }},
        tooltip: {{ backgroundColor: "#1e2230", borderColor: "#3d4868", borderWidth: 1, titleColor: "#f1f5f9", bodyColor: "#e2e8f0" }},
      }},
      scales: {{
        x: {{ ticks: {{ color: "#64748b", font: {{ size: 11 }} }}, grid: {{ color: "#252b3b" }} }},
        y: {{ ticks: {{ color: "#64748b", font: {{ size: 11 }} }}, grid: {{ color: "#252b3b" }}, beginAtZero: true }},
      }},
      responsive: true,
      maintainAspectRatio: false,
    }};

    new Chart(document.getElementById("totalChart"), {{
      type: "line",
      data: {{
        labels: DATA.months,
        datasets: [
          {{ label: "Monthly incidents", data: DATA.monthlyTotals, borderColor: "#3b82f6", backgroundColor: "rgba(59,130,246,0.08)", fill: true, tension: 0.3, pointRadius: 3, pointHoverRadius: 5 }},
          {{ label: "3-month avg", data: DATA.rollingAvg, borderColor: "#ef4444", borderWidth: 2, borderDash: [5, 3], pointRadius: 0, tension: 0.4, fill: false, spanGaps: true }},
        ],
      }},
      options: BASE_OPTS,
    }});

    new Chart(document.getElementById("typesChart"), {{
      type: "line",
      data: {{ labels: DATA.months, datasets: DATA.typeDatasets }},
      options: BASE_OPTS,
    }});
  </script>
</body>
</html>
"""


def generate_crime_trends(
    days_back: int = LOOKBACK_DAYS,
) -> tuple[Optional[io.BytesIO], str]:
    try:
        rows = _fetch_monthly_by_type(days_back)
    except Exception as e:
        logger.error(f"crime trends fetch: {e}")
        return None, f"🚔 Error fetching crime data: {e}"

    if not rows:
        return None, f"🚔 No crime data found for last {days_back} days."

    data = _aggregate(rows)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _render_html(data, fetched_at)

    buf = io.BytesIO(html.encode("utf-8"))
    buf.seek(0)
    summary = f"🚔 *Crime Trends*\n_{data['total']:,} incidents over {len(data['months'])} months_"
    return buf, summary
