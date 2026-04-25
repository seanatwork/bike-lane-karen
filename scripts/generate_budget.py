#!/usr/bin/env python3
"""Generate docs/budget/index.html from Austin's operating budget API.

Data source: data.austintexas.gov/resource/g5k8-8sud (Operating Budget)
Fetches the latest fiscal year + quarter automatically.
Run quarterly via .github/workflows/generate-budget.yml.
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── config ─────────────────────────────────────────────────────────────────────

API_URL = "https://data.austintexas.gov/resource/g5k8-8sud.json"
GENERAL_FUND_CODE = "1000"
OUT_PATH = Path(__file__).parent.parent / "docs" / "budget" / "index.html"

DEPT_DISPLAY = {
    "Austin Police": "Police (APD)",
    "Austin Fire": "Fire",
    "Austin-Travis County Emergency Medical Services": "EMS",
    "Austin Parks & Recreation": "Parks & Recreation",
    "Austin Public Library": "Public Library",
    "Austin Public Health": "Public Health",
    "Nondepartmental Revenue/Expenses": "Nondepartmental",
    "Austin Municipal Court": "Municipal Court",
    "Social Service Contracts - HSO": "Social Services (Homeless)",
    "Social Service Contracts - APH": "Social Services (Health)",
    "Austin Animal Services": "Animal Services",
    "Austin Forensic Science": "Forensic Science",
    "Austin Planning": "Planning",
    "Austin Housing": "Housing",
    "Austin Arts, Culture, Music & Entertainment": "Arts & Culture",
    "Austin Homeless Strategies & Operations": "Homeless Strategy & Ops",
    "Social Service Contracts - CC": "Social Services (Courts)",
    "Social Service Contracts - EDD": "Social Services (Econ Dev)",
    "Austin Human Resources": "Human Resources",
}

DEPT_COLORS = {
    "Austin Police": "#ef4444",
    "Austin Fire": "#f97316",
    "Austin-Travis County Emergency Medical Services": "#f59e0b",
    "Austin Parks & Recreation": "#22c55e",
    "Austin Public Library": "#10b981",
    "Austin Public Health": "#06b6d4",
    "Nondepartmental Revenue/Expenses": "#64748b",
    "Austin Municipal Court": "#6366f1",
    "Social Service Contracts - HSO": "#8b5cf6",
    "Social Service Contracts - APH": "#a78bfa",
    "Austin Animal Services": "#22c55e",
    "Austin Forensic Science": "#6366f1",
    "Austin Planning": "#64748b",
    "Austin Housing": "#8b5cf6",
    "Austin Arts, Culture, Music & Entertainment": "#ec4899",
    "Austin Homeless Strategies & Operations": "#a78bfa",
    "Social Service Contracts - CC": "#818cf8",
    "Social Service Contracts - EDD": "#818cf8",
    "Austin Human Resources": "#64748b",
}
DEFAULT_COLOR = "#64748b"

PUBLIC_SAFETY = {
    "Austin Police",
    "Austin Fire",
    "Austin-Travis County Emergency Medical Services",
}

# Quarter end months; Austin FY runs Oct 1 – Sep 30
QUARTER_END_MONTH = {1: "Dec", 2: "Mar", 3: "Jun", 4: "Sep"}


# ── fetch + aggregate ──────────────────────────────────────────────────────────

def fetch_rows():
    headers = {}
    token = os.getenv("AUSTIN_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    rows, offset, limit = [], 0, 10_000
    while True:
        resp = requests.get(
            API_URL,
            params={"fund_code": GENERAL_FUND_CODE, "$limit": limit, "$offset": offset},
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return rows


def aggregate(rows):
    max_fy = max(int(r["budget_fiscal_year"]) for r in rows)
    fy_rows = [r for r in rows if int(r["budget_fiscal_year"]) == max_fy]
    max_q = max(int(r["thru_quarter"]) for r in fy_rows)
    q_rows = [r for r in fy_rows if int(r["thru_quarter"]) == max_q]

    totals = defaultdict(lambda: {"budget": 0.0, "spent": 0.0})
    for r in q_rows:
        k = r["department_name"]
        totals[k]["budget"] += float(r.get("budget") or 0)
        totals[k]["spent"] += float(r.get("expenditures") or 0)

    depts = sorted(
        [{"dept_name": k, **v} for k, v in totals.items()],
        key=lambda d: d["budget"],
        reverse=True,
    )
    return max_fy, max_q, depts


# ── html ───────────────────────────────────────────────────────────────────────

def fmt_stat(n):
    return f"${n/1e9:.2f}B" if n >= 1e9 else f"${n/1e6:.0f}M"


def generate_html(fy, quarter, depts):
    total_budget = sum(d["budget"] for d in depts)
    total_spent  = sum(d["spent"]  for d in depts)
    ps_budget    = sum(d["budget"] for d in depts if d["dept_name"] in PUBLIC_SAFETY)
    ps_pct       = ps_budget / total_budget * 100 if total_budget else 0
    spend_pct    = total_spent / total_budget * 100 if total_budget else 0

    fy_start  = fy - 1
    q_end_mo  = QUARTER_END_MONTH.get(quarter, "Sep")
    q_end_yr  = fy_start if quarter == 1 else fy
    period    = f"Oct {fy_start}–{q_end_mo} {q_end_yr}"
    updated   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    js_depts = json.dumps([
        {
            "name":   DEPT_DISPLAY.get(d["dept_name"], d["dept_name"]),
            "budget": round(d["budget"], 2),
            "spent":  round(d["spent"],  2),
            "color":  DEPT_COLORS.get(d["dept_name"], DEFAULT_COLOR),
        }
        for d in depts
    ], indent=6)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Austin General Fund FY{fy}</title>
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
    #last-updated {{ font-size: 11px; color: #475569; }}
    .btn-row {{ display: flex; gap: 4px; flex-wrap: wrap; justify-content: center; }}
    .fbtn {{
      background: #252b3b; border: 1px solid #3d4868; color: #94a3b8;
      padding: 5px 13px; border-radius: 4px; font-size: 12px;
      text-decoration: none; display: inline-block; white-space: nowrap;
      transition: background 0.12s, border-color 0.12s, color 0.12s;
    }}
    .fbtn:hover {{ background: #2d3453; color: #e2e8f0; }}
    #stats {{ border-bottom: 1px solid #2d3348; }}
    .stats-inner {{ display: flex; justify-content: center; flex-wrap: wrap; }}
    .stat {{
      flex: 1; min-width: 120px; max-width: 200px; text-align: center;
      padding: 10px 8px 9px; border-right: 1px solid #2d3348;
    }}
    .stat:last-child {{ border-right: none; }}
    .stat-value {{ font-size: 1.2rem; font-weight: 700; line-height: 1.1; }}
    .stat-label {{ font-size: 0.67rem; color: #475569; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 3px; }}
    .stat-sub   {{ font-size: 0.67rem; color: #475569; margin-top: 1px; }}
    #main {{ flex: 1; padding: 16px; display: flex; flex-direction: column; gap: 20px; max-width: 1100px; width: 100%; margin: 0 auto; }}
    .chart-block {{ background: #161a24; border: 1px solid #2d3348; border-radius: 8px; padding: 14px; }}
    .chart-title {{ font-size: 13px; font-weight: 600; color: #e2e8f0; margin-bottom: 4px; }}
    .chart-sub   {{ font-size: 11px; color: #475569; margin-bottom: 10px; }}
    .chart-container {{ position: relative; height: 480px; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 8px 16px; margin-bottom: 10px; }}
    .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 11px; color: #94a3b8; }}
    .legend-dot {{ width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }}
    .section-divider {{ border: none; border-top: 1px solid #2d3348; }}
    .section-heading {{ font-size: 1.1rem; font-weight: 700; color: #f1f5f9; margin-bottom: 0.3rem; }}
    .section-sub {{ font-size: 0.82rem; color: #64748b; line-height: 1.5; margin-bottom: 1.2rem; }}
    .fund-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 1rem; }}
    .fund-card {{
      background: #1e2230; border: 1px solid #2d3348;
      border-top: 3px solid var(--accent);
      border-radius: 10px; padding: 1.2rem;
      display: flex; flex-direction: column; gap: 0.45rem;
    }}
    .fund-icon {{ font-size: 1.4rem; line-height: 1; }}
    .fund-name {{ font-size: 0.95rem; font-weight: 700; color: #f1f5f9; }}
    .fund-source {{ font-size: 0.72rem; font-weight: 600; color: var(--accent); text-transform: uppercase; letter-spacing: 0.04em; }}
    .fund-card ul {{ margin: 0.2rem 0 0 1rem; font-size: 0.78rem; color: #94a3b8; line-height: 1.7; }}
    .fund-note {{
      margin-top: 0.5rem; font-size: 0.72rem; color: #475569;
      border-top: 1px solid #2d3348; padding-top: 0.5rem; line-height: 1.4;
    }}
    .fund-note strong {{ color: #64748b; }}
    #data-source {{ font-size: 0.75rem; color: #334155; text-align: center; padding-bottom: 4px; }}
    #data-source a {{ color: #475569; text-decoration: none; }}
    #data-source a:hover {{ color: #64748b; }}
    footer {{ text-align: center; padding: 14px 16px; font-size: 0.74rem; color: #475569; border-top: 1px solid #1e2230; }}
    footer a {{ color: #64748b; text-decoration: none; }}
    footer a:hover {{ color: #94a3b8; }}
    @media (max-width: 520px) {{ .stat-value {{ font-size: 1rem; }} .chart-container {{ height: 560px; }} }}
  </style>
</head>
<body>

  <div id="panel">
    <div id="panel-title">💰 Austin General Fund — FY{fy}</div>
    <div id="panel-subtitle">Adopted budget by department · actuals through Q{quarter} ({period})</div>
    <div id="last-updated">Updated: {updated}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Austin 311 Home</a>
    </div>
  </div>

  <div id="stats">
    <div class="stats-inner">
      <div class="stat">
        <div class="stat-value" style="color:#60a5fa;">{fmt_stat(total_budget)}</div>
        <div class="stat-label">Total adopted</div>
        <div class="stat-sub">FY{fy} General Fund</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#f87171;">{ps_pct:.1f}%</div>
        <div class="stat-label">Public safety</div>
        <div class="stat-sub">Police + Fire + EMS</div>
      </div>
      <div class="stat">
        <div class="stat-value" style="color:#fbbf24;">{spend_pct:.1f}%</div>
        <div class="stat-label">Spent through Q{quarter}</div>
        <div class="stat-sub">{fmt_stat(total_spent)} of {fmt_stat(total_budget)}</div>
      </div>
    </div>
  </div>

  <div id="main">

    <div class="chart-block">
      <div class="chart-title">General Fund — adopted budget by department</div>
      <div class="chart-sub">Solid = spent through Q{quarter} · Dim = remaining budget · Hover for details</div>
      <div class="legend">
        <span class="legend-item"><span class="legend-dot" style="background:#ef4444;"></span>Public Safety</span>
        <span class="legend-item"><span class="legend-dot" style="background:#22c55e;"></span>Parks &amp; Community</span>
        <span class="legend-item"><span class="legend-dot" style="background:#06b6d4;"></span>Health &amp; Wellbeing</span>
        <span class="legend-item"><span class="legend-dot" style="background:#8b5cf6;"></span>Housing &amp; Social Services</span>
        <span class="legend-item"><span class="legend-dot" style="background:#6366f1;"></span>Courts &amp; Justice</span>
        <span class="legend-item"><span class="legend-dot" style="background:#64748b;"></span>Admin &amp; Other</span>
      </div>
      <div class="chart-container"><canvas id="deptChart"></canvas></div>
    </div>

    <hr class="section-divider" />

    <div>
      <div class="section-heading">How Austin&#39;s Budget Works</div>
      <p class="section-sub">Three separate money pools fund city services — understanding which is which explains why the city can&#39;t simply &#34;move money around.&#34;</p>
      <div class="fund-grid">
        <div class="fund-card" style="--accent:#60a5fa;">
          <span class="fund-icon">🏛️</span>
          <div class="fund-name">General Fund</div>
          <div class="fund-source">Property tax &amp; sales tax</div>
          <ul>
            <li>Police (APD)</li>
            <li>Fire &amp; EMS</li>
            <li>Parks maintenance</li>
            <li>Libraries</li>
            <li>Code enforcement</li>
          </ul>
          <div class="fund-note"><strong>Where policy happens.</strong> Council allocates this fund — it reflects actual priorities. Operational gaps (staffing, response times) can only be fixed here.</div>
        </div>
        <div class="fund-card" style="--accent:#34d399;">
          <span class="fund-icon">⚡</span>
          <div class="fund-name">Enterprise Funds</div>
          <div class="fund-source">User fees &amp; utility bills</div>
          <ul>
            <li>Austin Energy</li>
            <li>Austin Water</li>
            <li>Airport (AUS)</li>
            <li>Resource Recovery</li>
          </ul>
          <div class="fund-note"><strong>Legally ring-fenced.</strong> Your electric and water bills fund these departments only — they cannot be redirected to police, parks, or anything else.</div>
        </div>
        <div class="fund-card" style="--accent:#fbbf24;">
          <span class="fund-icon">🏗️</span>
          <div class="fund-name">Bonds</div>
          <div class="fund-source">Voter-authorized debt</div>
          <ul>
            <li>Roads &amp; sidewalks</li>
            <li>Parks infrastructure</li>
            <li>Libraries &amp; buildings</li>
            <li>Trails &amp; bike lanes</li>
          </ul>
          <div class="fund-note"><strong>Capital only.</strong> Bonds build and repair physical things — they cannot hire staff, improve response times, or fund any ongoing service.</div>
        </div>
      </div>
    </div>

    <div id="data-source">
      Data: <a href="https://data.austintexas.gov/resource/g5k8-8sud" target="_blank" rel="noopener">City of Austin Operating Budget (g5k8-8sud)</a>
      · FY{fy} · refreshed quarterly
    </div>

  </div>

  <footer>
    <a href="../">← Austin 311 Home</a>
  </footer>

  <script>
    const DEPTS = {js_depts};

    const fmt = n => "$" + (n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : (n / 1e3).toFixed(0) + "K");
    const pct = (a, b) => ((a / b) * 100).toFixed(1) + "%";

    new Chart(document.getElementById("deptChart"), {{
      type: "bar",
      data: {{
        labels: DEPTS.map(d => d.name),
        datasets: [
          {{
            label: "Spent (Q{quarter})",
            data: DEPTS.map(d => d.spent),
            backgroundColor: DEPTS.map(d => d.color),
            borderRadius: 0,
            stack: "budget",
          }},
          {{
            label: "Remaining budget",
            data: DEPTS.map(d => d.budget - d.spent),
            backgroundColor: DEPTS.map(d => d.color + "40"),
            borderRadius: 4,
            stack: "budget",
          }},
        ],
      }},
      options: {{
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            backgroundColor: "#1e2230",
            borderColor: "#3d4868",
            borderWidth: 1,
            titleColor: "#f1f5f9",
            bodyColor: "#e2e8f0",
            callbacks: {{
              title: ctx => ctx[0].label,
              afterTitle: ctx => `Adopted: ${{fmt(DEPTS[ctx[0].dataIndex].budget)}}`,
              label: ctx => {{
                const d = DEPTS[ctx.dataIndex];
                return ctx.datasetIndex === 0
                  ? ` Spent through Q{quarter}: ${{fmt(d.spent)}} (${{pct(d.spent, d.budget)}})`
                  : ` Remaining: ${{fmt(d.budget - d.spent)}}`;
              }},
            }},
          }},
        }},
        scales: {{
          x: {{
            stacked: true,
            ticks: {{ color: "#64748b", font: {{ size: 11 }}, callback: v => "$" + (v / 1e6).toFixed(0) + "M" }},
            grid: {{ color: "#252b3b" }},
            beginAtZero: true,
          }},
          y: {{
            stacked: true,
            ticks: {{ color: "#94a3b8", font: {{ size: 11 }} }},
            grid: {{ color: "#252b3b" }},
          }},
        }},
      }},
    }});
  </script>
</body>
</html>"""


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    print("Fetching General Fund data...")
    rows = fetch_rows()
    print(f"  {len(rows)} rows fetched")

    fy, quarter, depts = aggregate(rows)
    print(f"  FY{fy} Q{quarter} — {len(depts)} departments")

    html = generate_html(fy, quarter, depts)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"  Written → {OUT_PATH}")


if __name__ == "__main__":
    main()
