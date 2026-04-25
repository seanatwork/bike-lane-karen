"""Noise complaint trends — monthly counts per complaint type over 12 months."""

import io
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from noisecomplaints.noise_bot import SERVICE_CODES, _get_session, _isoformat_z, _utc_now

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 365
OPEN311_URL = "https://311.austintexas.gov/open311/v2/requests.json"

# Shortened labels for chart legend
CODE_LABELS = {
    "APDNONNO": "Non-Emergency Noise",
    "DSOUCVMC": "Outdoor Venue / Music",
    "AFDFIREW": "Fireworks",
}
LINE_COLORS = ["#3b82f6", "#22c55e", "#f59e0b"]


def _fetch_code_paginated(service_code: str, days_back: int) -> list:
    session = _get_session()
    end = _utc_now()
    start = end - timedelta(days=days_back)
    params = {
        "service_code": service_code,
        "start_date": _isoformat_z(start),
        "end_date": _isoformat_z(end),
        "per_page": 100,
        "page": 1,
    }
    records = []
    while True:
        try:
            resp = session.get(OPEN311_URL, params=params, timeout=20)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            logger.warning(f"fetch {service_code} p{params['page']}: {e}")
            break
        if not isinstance(batch, list) or not batch:
            break
        records.extend(batch)
        if len(batch) < 100:
            break
        params["page"] += 1
        time.sleep(1.0)
    return records


def _aggregate(records_by_code: dict) -> dict:
    # Build the full sorted month list across all codes
    all_months: set = set()
    code_monthly: dict = {}

    for code, records in records_by_code.items():
        monthly: dict = defaultdict(int)
        for r in records:
            ts = r.get("requested_datetime") or ""
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            monthly[dt.strftime("%Y-%m")] += 1
            all_months.add(dt.strftime("%Y-%m"))
        code_monthly[code] = monthly

    months = sorted(all_months)
    total = sum(
        sum(m.values()) for m in code_monthly.values()
    )
    peak_month = max(months, key=lambda m: sum(code_monthly[c].get(m, 0) for c in code_monthly)) if months else "—"
    peak_count = sum(code_monthly[c].get(peak_month, 0) for c in code_monthly) if peak_month != "—" else 0
    avg_per_month = round(total / max(1, len(months)), 1)

    datasets = []
    for i, code in enumerate(SERVICE_CODES):
        monthly = code_monthly.get(code, {})
        datasets.append({
            "label": CODE_LABELS.get(code, code),
            "data": [monthly.get(m, 0) for m in months],
            "color": LINE_COLORS[i % len(LINE_COLORS)],
        })

    return {
        "total": total,
        "months": months,
        "peak_month": peak_month,
        "peak_count": peak_count,
        "avg_per_month": avg_per_month,
        "datasets": datasets,
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

    js_datasets = []
    for ds in data["datasets"]:
        js_datasets.append({
            "label": ds["label"],
            "data": ds["data"],
            "borderColor": ds["color"],
            "backgroundColor": "transparent",
            "tension": 0.3,
            "pointRadius": 3,
            "pointHoverRadius": 5,
            "fill": False,
        })

    payload = json.dumps({"months": month_labels, "datasets": js_datasets})

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Noise Complaint Trends</title>
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
    .chart-container {{ position: relative; height: 320px; }}
    footer {{ text-align: center; padding: 14px 16px; font-size: 0.74rem; color: #475569; border-top: 1px solid #1e2230; }}
    footer a {{ color: #64748b; text-decoration: none; }}
    footer a:hover {{ color: #94a3b8; }}
    @media (max-width: 520px) {{ .stat-value {{ font-size: 1rem; }} .chart-container {{ height: 260px; }} }}
  </style>
</head>
<body>
  <div id="panel">
    <div id="panel-title">🔊 Austin Noise Complaint Trends</div>
    <div id="panel-subtitle">Monthly complaints by type — last 12 months</div>
    <div id="last-ran">Last ran: {fetched_at}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Noise Map</a>
      <a class="fbtn" href="../../">Austin 311 Home</a>
    </div>
  </div>

  <div id="stats">
    <div class="stats-inner">
      <div class="stat">
        <div class="stat-value" style="color:#3b82f6;">{total:,}</div>
        <div class="stat-label">Total complaints</div>
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
      <div class="chart-title">Monthly complaints by type</div>
      <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
    </div>
  </div>

  <footer>
    Data: <a href="https://311.austintexas.gov/open311/v2" target="_blank" rel="noopener">Austin Open311 (APDNONNO · DSOUCVMC · AFDFIREW)</a>
    &nbsp;·&nbsp; <a href="../">← Noise Map</a>
    &nbsp;·&nbsp; <a href="../../">← Austin 311</a>
  </footer>

  <script>
    const DATA = {payload};

    new Chart(document.getElementById("monthlyChart"), {{
      type: "line",
      data: {{ labels: DATA.months, datasets: DATA.datasets }},
      options: {{
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
      }},
    }});
  </script>
</body>
</html>
"""


def generate_noise_trends(
    days_back: int = LOOKBACK_DAYS,
) -> tuple[Optional[io.BytesIO], str]:
    records_by_code = {}
    for code in SERVICE_CODES:
        try:
            records_by_code[code] = _fetch_code_paginated(code, days_back)
            logger.info(f"noise trends {code}: {len(records_by_code[code])} records")
        except Exception as e:
            logger.warning(f"noise trends failed for {code}: {e}")
            records_by_code[code] = []

    total = sum(len(v) for v in records_by_code.values())
    if total == 0:
        return None, f"🔊 No noise data found for last {days_back} days."

    data = _aggregate(records_by_code)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _render_html(data, fetched_at)

    buf = io.BytesIO(html.encode("utf-8"))
    buf.seek(0)
    summary = f"🔊 *Noise Trends*\n_{data['total']:,} complaints over {len(data['months'])} months_"
    return buf, summary
