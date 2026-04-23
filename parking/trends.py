"""
Parking Complaints Trends — aggregates PARKINGV 311 reports over time.

Note: This uses Open311 PARKINGV (resident-reported complaints), NOT actual
citations issued by enforcement. Austin's parking citation data is not
exposed via public API — it's only available through the Municipal Court
case-lookup portal or a Public Information Request.
"""

import io
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from parking.parking_bot import get_all_citations, _extract_violation_type, _extract_street

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 180
TOP_STREETS = 15
TOP_VIOLATIONS = 10


def _aggregate(records: list) -> dict:
    """Bucket records by month, street, and violation type."""
    monthly: dict = defaultdict(int)
    monthly_open: dict = defaultdict(int)
    streets: dict = defaultdict(int)
    violations: dict = defaultdict(int)
    total = 0

    for r in records:
        ts = r.get("requested_datetime") or ""
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue

        month_key = dt.strftime("%Y-%m")
        monthly[month_key] += 1
        if (r.get("status") or "").lower() == "open":
            monthly_open[month_key] += 1

        addr = r.get("address") or ""
        if addr:
            streets[_extract_street(addr)] += 1

        vt = _extract_violation_type(r.get("description") or "")
        if vt:
            violations[vt] += 1

        total += 1

    months_sorted = sorted(monthly.keys())
    top_streets = sorted(streets.items(), key=lambda x: -x[1])[:TOP_STREETS]
    top_violations = sorted(violations.items(), key=lambda x: -x[1])[:TOP_VIOLATIONS]

    return {
        "total": total,
        "months": months_sorted,
        "monthly_counts": [monthly[m] for m in months_sorted],
        "monthly_open_counts": [monthly_open[m] for m in months_sorted],
        "top_streets": top_streets,
        "top_violations": top_violations,
    }


def _render_html(data: dict, days_back: int, fetched_at: str) -> str:
    total = data["total"]
    months = data["months"]
    monthly_counts = data["monthly_counts"]
    monthly_open_counts = data["monthly_open_counts"]
    top_streets = data["top_streets"]
    top_violations = data["top_violations"]

    avg_per_month = round(total / max(1, len(months)), 0) if months else 0
    peak_month_idx = monthly_counts.index(max(monthly_counts)) if monthly_counts else -1
    peak_month = months[peak_month_idx] if peak_month_idx >= 0 else "—"
    peak_count = monthly_counts[peak_month_idx] if peak_month_idx >= 0 else 0

    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]

    payload = {
        "months": month_labels,
        "monthlyCounts": monthly_counts,
        "monthlyOpenCounts": monthly_open_counts,
        "streets": [{"name": s, "count": c} for s, c in top_streets],
        "violations": [{"name": v, "count": c} for v, c in top_violations],
    }
    payload_json = json.dumps(payload)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>ATX Pulse — Parking Complaints Trends</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f1117; color: #e2e8f0;
      min-height: 100vh; display: flex; flex-direction: column;
    }}
    #panel {{
      position: sticky; top: 0; z-index: 100;
      background: #1e2230; border-bottom: 1px solid #2d3348;
      padding: 10px 16px 12px;
      display: flex; flex-direction: column; align-items: center; gap: 6px;
    }}
    #panel-title {{ font-size: 15px; font-weight: 700; color: #f1f5f9; }}
    #panel-subtitle {{ font-size: 12px; color: #64748b; text-align: center; }}
    #last-ran {{ font-size: 11px; color: #475569; }}
    .btn-row {{ display: flex; gap: 4px; flex-wrap: wrap; justify-content: center; }}
    .fbtn {{
      background: #252b3b; border: 1px solid #3d4868; color: #94a3b8;
      padding: 5px 13px; border-radius: 4px; font-size: 12px; cursor: pointer;
      transition: background 0.12s, border-color 0.12s, color 0.12s;
      white-space: nowrap; text-decoration: none; display: inline-block;
    }}
    .fbtn:hover {{ background: #2d3453; color: #e2e8f0; }}
    .fbtn.active {{ background: #3b82f6; border-color: #3b82f6; color: #fff; font-weight: 600; }}

    #stats {{ border-bottom: 1px solid #2d3348; }}
    .stats-inner {{ display: flex; justify-content: center; }}
    .stat {{
      flex: 1; max-width: 170px; text-align: center;
      padding: 10px 8px 9px; border-right: 1px solid #2d3348;
    }}
    .stat:last-child {{ border-right: none; }}
    .stat-value {{ font-size: 1.25rem; font-weight: 700; line-height: 1.1; }}
    .stat-label {{ font-size: 0.67rem; color: #475569; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 3px; }}
    .stat-sub   {{ font-size: 0.67rem; color: #475569; margin-top: 1px; }}

    #chart-wrap {{ flex: 1; padding: 16px; display: flex; flex-direction: column; gap: 20px; max-width: 1100px; width: 100%; margin: 0 auto; }}
    .chart-block {{ background: #161a24; border: 1px solid #2d3348; border-radius: 8px; padding: 14px; }}
    .chart-title {{ font-size: 13px; font-weight: 600; color: #e2e8f0; margin-bottom: 10px; }}
    .chart-container {{ position: relative; height: 320px; }}

    #data-note {{
      padding: 12px 14px; background: #1e2230;
      border: 1px solid #2d3348; border-left: 3px solid #f59e0b;
      border-radius: 6px; font-size: 0.8rem; color: #94a3b8; line-height: 1.6;
    }}
    #data-note strong {{ color: #f1f5f9; }}

    footer {{
      text-align: center; padding: 14px 16px;
      font-size: 0.74rem; color: #475569; border-top: 1px solid #1e2230;
    }}
    footer a {{ color: #64748b; text-decoration: none; }}
    footer a:hover {{ color: #94a3b8; }}
    @media (max-width: 520px) {{ .stat-value {{ font-size: 1rem; }} .chart-container {{ height: 260px; }} }}
  </style>
</head>
<body>

  <div id="panel">
    <div id="panel-title">🅿️ Austin Parking Complaints Trends</div>
    <div id="panel-subtitle">Resident-reported parking violations — last {days_back} days</div>
    <div id="last-ran">Last ran: {fetched_at}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Parking Map</a>
      <a class="fbtn" href="../../">ATX Pulse Home</a>
    </div>
  </div>

  <div id="stats">
    <div class="stats-inner">
      <div class="stat">
        <div class="stat-value" style="color:#3b82f6;">{total:,}</div>
        <div class="stat-label">Total complaints</div>
        <div class="stat-sub">last {days_back} days</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#22c55e;">{int(avg_per_month):,}</div>
        <div class="stat-label">Avg / month</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#f59e0b;">{peak_count:,}</div>
        <div class="stat-label">Peak month</div>
        <div class="stat-sub">{peak_month}</div>
      </div>
    </div>
  </div>

  <div id="chart-wrap">
    <div class="chart-block">
      <div class="chart-title">Complaints per month (citywide)</div>
      <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
    </div>

    <div class="chart-block">
      <div class="chart-title">Top {TOP_STREETS} streets / corridors</div>
      <div class="chart-container" style="height: {max(320, len(top_streets) * 26)}px;"><canvas id="streetsChart"></canvas></div>
    </div>

    <div class="chart-block">
      <div class="chart-title">Top {TOP_VIOLATIONS} violation types</div>
      <div class="chart-container" style="height: {max(300, len(top_violations) * 30)}px;"><canvas id="violationsChart"></canvas></div>
    </div>

    <div id="data-note">
      <strong>Important caveat:</strong> This shows <em>resident-reported complaints</em> via Austin 311,
      not actual citations issued by parking enforcement. A complaint does not necessarily result in a citation,
      and many citations are issued without any 311 complaint.
      <br /><br />
      Austin's actual parking citation data is not published on the city's open data portal. It is only
      available through the <a href="https://austin-portal.ecourt.com/public-portal/?q=node/412" target="_blank" rel="noopener">Municipal Court Public Portal</a>
      (individual lookup) or via a <a href="https://www.austintexas.gov/services/submit-public-information-request" target="_blank" rel="noopener">Public Information Request</a>.
      <br /><br />
      Still useful as a proxy for <strong>where residents care enough to report</strong> — which tracks
      high-friction parking corridors and chronic problem locations.
    </div>
  </div>

  <footer>
    Data: <a href="https://311.austintexas.gov/open311/v2" target="_blank" rel="noopener">Austin Open311 (PARKINGV)</a>
    &nbsp;·&nbsp;
    <a href="../">← Parking Map</a>
    &nbsp;·&nbsp;
    <a href="../../">← ATX Pulse</a>
  </footer>

  <script>
    const DATA = {payload_json};

    const chartDefaults = {{
      plugins: {{
        legend: {{ labels: {{ color: "#94a3b8", font: {{ size: 11 }} }} }},
        tooltip: {{
          backgroundColor: "#1e2230",
          borderColor: "#3d4868",
          borderWidth: 1,
          titleColor: "#f1f5f9",
          bodyColor: "#e2e8f0",
        }},
      }},
      scales: {{
        x: {{ ticks: {{ color: "#64748b", font: {{ size: 11 }} }}, grid: {{ color: "#252b3b" }} }},
        y: {{ ticks: {{ color: "#64748b", font: {{ size: 11 }} }}, grid: {{ color: "#252b3b" }}, beginAtZero: true }},
      }},
      responsive: true,
      maintainAspectRatio: false,
    }};

    // Monthly trend
    new Chart(document.getElementById("monthlyChart"), {{
      type: "bar",
      data: {{
        labels: DATA.months,
        datasets: [
          {{
            label: "Total complaints",
            data: DATA.monthlyCounts,
            backgroundColor: "#3b82f6",
            borderRadius: 4,
          }},
          {{
            label: "Still open",
            data: DATA.monthlyOpenCounts,
            backgroundColor: "#ef4444",
            borderRadius: 4,
          }},
        ],
      }},
      options: chartDefaults,
    }});

    // Top streets
    new Chart(document.getElementById("streetsChart"), {{
      type: "bar",
      data: {{
        labels: DATA.streets.map(s => s.name),
        datasets: [{{
          label: "Complaints",
          data: DATA.streets.map(s => s.count),
          backgroundColor: "#8b5cf6",
          borderRadius: 4,
        }}],
      }},
      options: {{ ...chartDefaults, indexAxis: "y", plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }} }},
    }});

    // Top violation types
    new Chart(document.getElementById("violationsChart"), {{
      type: "bar",
      data: {{
        labels: DATA.violations.map(v => v.name),
        datasets: [{{
          label: "Complaints",
          data: DATA.violations.map(v => v.count),
          backgroundColor: "#f59e0b",
          borderRadius: 4,
        }}],
      }},
      options: {{ ...chartDefaults, indexAxis: "y", plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }} }},
    }});
  </script>
</body>
</html>
"""


def generate_parking_trends(days_back: int = LOOKBACK_DAYS) -> tuple[Optional[io.BytesIO], str]:
    """Generate the parking complaints trends HTML page.

    Returns (BytesIO buffer, summary string) — matches the signature used by
    scripts/generate_map.py for consistency.
    """
    records = get_all_citations(days_back=days_back)
    if not records:
        return None, f"🅿️ No parking complaint data found for last {days_back} days."

    data = _aggregate(records)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _render_html(data, days_back, fetched_at)

    buf = io.BytesIO(html.encode("utf-8"))
    buf.seek(0)

    summary = (
        f"🅿️ *Parking Complaints Trends*\n"
        f"_Last {days_back} days · {data['total']:,} complaints across "
        f"{len(data['months'])} months_"
    )
    return buf, summary
