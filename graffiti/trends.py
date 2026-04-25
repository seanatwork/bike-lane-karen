"""Graffiti abatement trends — monthly ticket counts over 12 months."""

import io
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from graffiti.graffiti_bot import _fetch_graffiti

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 365


def _rolling_avg(counts: list, window: int = 3) -> list:
    result = []
    for i in range(len(counts)):
        if i < window - 1:
            result.append(None)
        else:
            result.append(round(sum(counts[i - window + 1 : i + 1]) / window, 1))
    return result


def _aggregate(records: list) -> dict:
    monthly: dict = defaultdict(int)
    monthly_open: dict = defaultdict(int)
    res_days: dict = defaultdict(list)

    for r in records:
        ts = r.get("requested_datetime") or ""
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue

        mk = dt.strftime("%Y-%m")
        monthly[mk] += 1

        if (r.get("status") or "").lower() == "open":
            monthly_open[mk] += 1
        else:
            upd = r.get("updated_datetime") or ""
            if upd:
                try:
                    upd_dt = datetime.fromisoformat(upd.replace("Z", "+00:00"))
                    days = (upd_dt - dt).days
                    if 0 <= days <= 365:
                        res_days[mk].append(days)
                except ValueError:
                    pass

    months = sorted(monthly.keys())
    counts = [monthly[m] for m in months]
    avg_res = [
        round(sum(res_days[m]) / len(res_days[m]), 1) if res_days.get(m) else None
        for m in months
    ]
    return {
        "total": sum(counts),
        "months": months,
        "monthly_counts": counts,
        "monthly_open": [monthly_open[m] for m in months],
        "rolling_avg": _rolling_avg(counts),
        "avg_resolution_days": avg_res,
    }


def _render_html(data: dict, fetched_at: str) -> str:
    total = data["total"]
    months = data["months"]
    counts = data["monthly_counts"]

    avg_per_month = round(total / max(1, len(months)), 1)
    peak_idx = counts.index(max(counts)) if counts else -1
    peak_month = months[peak_idx] if peak_idx >= 0 else "—"
    peak_count = counts[peak_idx] if peak_idx >= 0 else 0
    peak_label = (
        datetime.strptime(peak_month, "%Y-%m").strftime("%b %Y")
        if peak_month != "—"
        else "—"
    )
    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]

    payload = json.dumps(
        {
            "months": month_labels,
            "monthlyCounts": counts,
            "rollingAvg": data["rolling_avg"],
            "avgResolutionDays": data["avg_resolution_days"],
        }
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Graffiti Trends</title>
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
    <div id="panel-title">🎨 Austin Graffiti Abatement Trends</div>
    <div id="panel-subtitle">New reports per month — last 12 months</div>
    <div id="last-ran">Last ran: {fetched_at}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Graffiti Map</a>
      <a class="fbtn" href="../../">Austin 311 Home</a>
    </div>
  </div>

  <div id="stats">
    <div class="stats-inner">
      <div class="stat">
        <div class="stat-value" style="color:#3b82f6;">{total:,}</div>
        <div class="stat-label">Total reports</div>
        <div class="stat-sub">last 12 months</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#22c55e;">{avg_per_month:.0f}</div>
        <div class="stat-label">Avg / month</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#f59e0b;">{peak_count:,}</div>
        <div class="stat-label">Peak month</div>
        <div class="stat-sub">{peak_label}</div>
      </div>
    </div>
  </div>

  <div id="chart-wrap">
    <div class="chart-block">
      <div class="chart-title">New reports per month</div>
      <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
    </div>
    <div class="chart-block">
      <div class="chart-title">Avg days to resolve (closed tickets)</div>
      <div class="chart-container"><canvas id="resolutionChart"></canvas></div>
    </div>
  </div>

  <footer>
    Data: <a href="https://311.austintexas.gov/open311/v2" target="_blank" rel="noopener">Austin Open311 (HHSGRAFF)</a>
    &nbsp;·&nbsp; <a href="../">← Graffiti Map</a>
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

    new Chart(document.getElementById("monthlyChart"), {{
      type: "line",
      data: {{
        labels: DATA.months,
        datasets: [
          {{ label: "Monthly reports", data: DATA.monthlyCounts, borderColor: "#3b82f6", backgroundColor: "rgba(59,130,246,0.08)", fill: true, tension: 0.3, pointRadius: 3, pointHoverRadius: 5 }},
          {{ label: "3-month avg", data: DATA.rollingAvg, borderColor: "#f59e0b", borderWidth: 2, borderDash: [5, 3], pointRadius: 0, tension: 0.4, fill: false, spanGaps: true }},
        ],
      }},
      options: BASE_OPTS,
    }});

    new Chart(document.getElementById("resolutionChart"), {{
      type: "line",
      data: {{
        labels: DATA.months,
        datasets: [
          {{ label: "Avg days to resolve", data: DATA.avgResolutionDays, borderColor: "#22c55e", backgroundColor: "rgba(34,197,94,0.08)", fill: true, tension: 0.3, pointRadius: 3, spanGaps: true }},
        ],
      }},
      options: BASE_OPTS,
    }});
  </script>
</body>
</html>
"""


def generate_graffiti_trends(
    days_back: int = LOOKBACK_DAYS,
) -> tuple[Optional[io.BytesIO], str]:
    records = _fetch_graffiti(days_back)
    if not records:
        return None, f"🎨 No graffiti data found for last {days_back} days."

    data = _aggregate(records)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _render_html(data, fetched_at)

    buf = io.BytesIO(html.encode("utf-8"))
    buf.seek(0)
    summary = f"🎨 *Graffiti Trends*\n_{data['total']:,} reports over {len(data['months'])} months_"
    return buf, summary
