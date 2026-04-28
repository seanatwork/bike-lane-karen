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

# ── expense categorization ─────────────────────────────────────────────────────

# Expense codes explicitly mapped to a category (takes priority over range rules)
EXPENSE_CODE_MAP = {
    # Technology
    "5723", "5724", "5726", "5727", "5729", "5760",
    "6240", "6245", "6248", "6249", "6387", "6388", "7580", "7610",
    # Fleet & Facilities
    "6231", "6250", "6251", "6255", "6256",
    "6370", "6371", "6372", "6373", "6381", "6382", "6383", "6389", "6395",
    # Grants
    "6820", "6825", "6828", "6830",
}

CATEGORY_META = {
    "Labor":              {"color": "#3b82f6",  "icon": "👷"},
    "Benefits":           {"color": "#8b5cf6",  "icon": "🏥"},
    "Contracted Services":{"color": "#22c55e",  "icon": "🤝"},
    "Technology":         {"color": "#06b6d4",  "icon": "💻"},
    "Facilities & Fleet": {"color": "#f59e0b",  "icon": "🏗️"},
    "Supplies & Equip.":  {"color": "#f97316",  "icon": "📦"},
    "Grants":             {"color": "#ec4899",  "icon": "💰"},
    "Transfers & Other":  {"color": "#64748b",  "icon": "🔄"},
}

CATEGORY_ORDER = list(CATEGORY_META.keys())


def categorize_expense(code: str, name: str) -> str:
    c = int(code)
    # Labor: all wages, overtime, specialty pay (5001-5179)
    if 5001 <= c <= 5179:
        return "Labor"
    # Benefits: insurance, retirement contributions, payroll taxes (5180-5299)
    if 5180 <= c <= 5299:
        return "Benefits"
    # Technology: IT, software, radio, wireless (specific codes)
    if code in {"5723", "5724", "5726", "5727", "5729", "5760",
                "6240", "6245", "6248", "6249", "6387", "6388", "7580", "7610"}:
        return "Technology"
    # Facilities & Fleet: rent, utilities, fleet, building maint (specific codes + ranges)
    if (6120 <= c <= 6176) or code in {
        "6231", "6250", "6251", "6255", "6256",
        "6370", "6371", "6372", "6373", "6381", "6382", "6383", "6389", "6395",
    }:
        return "Facilities & Fleet"
    # Contracted services: external professional/specialized services (5300-5899)
    if 5300 <= c <= 5899:
        return "Contracted Services"
    # Supplies & Equipment: physical goods, minor equipment (7000-7999, 9031-9056)
    if 7000 <= c <= 7999 or 9031 <= c <= 9056:
        return "Supplies & Equip."
    # Grants: money given to external organizations
    if code in {"6820", "6825", "6828", "6830"}:
        return "Grants"
    # Everything else: internal charges, fund transfers, admin overhead, reimbursements
    return "Transfers & Other"


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

    dept_totals = defaultdict(lambda: {"budget": 0.0, "spent": 0.0})
    # category breakdown: dept_name -> category -> {budget, spent}
    dept_cats = defaultdict(lambda: defaultdict(lambda: {"budget": 0.0, "spent": 0.0}))

    for r in q_rows:
        dept = r["department_name"]
        budget = float(r.get("budget") or 0)
        spent = float(r.get("expenditures") or 0)
        dept_totals[dept]["budget"] += budget
        dept_totals[dept]["spent"] += spent

        cat = categorize_expense(r["expense_code"], r.get("expense_name", ""))
        dept_cats[dept][cat]["budget"] += budget
        dept_cats[dept][cat]["spent"] += spent

    depts = sorted(
        [{"dept_name": k, **v} for k, v in dept_totals.items()],
        key=lambda d: d["budget"],
        reverse=True,
    )
    return max_fy, max_q, depts, dept_cats


# ── html ───────────────────────────────────────────────────────────────────────

def fmt_stat(n):
    return f"${n/1e9:.2f}B" if n >= 1e9 else f"${n/1e6:.0f}M"


def generate_html(fy, quarter, depts, dept_cats):
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

    # Build per-department category breakdown keyed by display name
    breakdown_map = {}
    for d in depts:
        display = DEPT_DISPLAY.get(d["dept_name"], d["dept_name"])
        cats = dept_cats.get(d["dept_name"], {})
        breakdown_map[display] = {
            cat: {
                "budget": round(cats[cat]["budget"], 2),
                "spent":  round(cats[cat]["spent"],  2),
            }
            for cat in CATEGORY_ORDER
            if cat in cats and cats[cat]["budget"] != 0
        }

    js_breakdown = json.dumps(breakdown_map, indent=6)
    js_cat_meta  = json.dumps(CATEGORY_META)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Austin General Fund FY{fy}</title>
  <!-- Google tag (gtag.js) -->
  <script async src="https://www.googletagmanager.com/gtag/js?id=G-TS158R7XSN"></script>
  <script>
    window.dataLayer = window.dataLayer || [];
    function gtag(){{dataLayer.push(arguments);}}
    gtag('js', new Date());
    gtag('config', 'G-TS158R7XSN');
  </script>
  <script>if(localStorage.getItem("theme")==="dark")document.documentElement.classList.add("dark");</script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg: #f8fafc; --bg-panel: #f1f5f9; --bg-card: #ffffff; --bg-card2: #f8fafc;
      --border: #e2e8f0; --text: #1e293b; --text-head: #0f172a;
      --text-sub: #64748b; --text-muted: #94a3b8;
      --btn-bg: #e2e8f0; --btn-border: #cbd5e1; --btn-color: #475569;
      --btn-hover-bg: #d1dae3; --btn-hover-color: #1e293b;
      --chart-title: #374151; --chart-sub: #6b7280;
      --section-head: #0f172a; --section-sub: #64748b;
      --fund-card-bg: #ffffff; --fund-name: #0f172a; --fund-list: #64748b; --fund-note: #6b7280; --fund-note-strong: #374151;
      --footer-border: #e2e8f0; --footer-color: #94a3b8;
      --source-color: #6b7280; --source-a: #475569;
      --drill-bg: #f1f5f9; --drill-border: #3b82f6; --drill-title: #0f172a;
      --drill-close-border: #cbd5e1; --drill-close-color: #64748b;
      --drill-name: #1e293b; --drill-vals: #64748b; --drill-pct: #94a3b8;
      --drill-hint: #94a3b8;
      --legend-color: #64748b;
    }}
    html.dark {{
      --bg: #0f1117; --bg-panel: #1e2230; --bg-card: #161a24; --bg-card2: #1a1f2e;
      --border: #2d3348; --text: #e2e8f0; --text-head: #f1f5f9;
      --text-sub: #64748b; --text-muted: #475569;
      --btn-bg: #252b3b; --btn-border: #3d4868; --btn-color: #94a3b8;
      --btn-hover-bg: #2d3453; --btn-hover-color: #e2e8f0;
      --chart-title: #e2e8f0; --chart-sub: #475569;
      --section-head: #f1f5f9; --section-sub: #64748b;
      --fund-card-bg: #1e2230; --fund-name: #f1f5f9; --fund-list: #94a3b8; --fund-note: #475569; --fund-note-strong: #64748b;
      --footer-border: #1e2230; --footer-color: #475569;
      --source-color: #334155; --source-a: #475569;
      --drill-bg: #1a1f2e; --drill-border: #3d4868; --drill-title: #f1f5f9;
      --drill-close-border: #3d4868; --drill-close-color: #64748b;
      --drill-name: #e2e8f0; --drill-vals: #64748b; --drill-pct: #94a3b8;
      --drill-hint: #334155;
      --legend-color: #94a3b8;
    }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg); color: var(--text);
      min-height: 100vh; display: flex; flex-direction: column; transition: background 0.2s, color 0.2s;
    }}
    #panel {{
      position: sticky; top: 0; z-index: 100;
      background: var(--bg-panel); border-bottom: 1px solid var(--border);
      padding: 10px 16px 12px;
      display: flex; flex-direction: column; align-items: center; gap: 6px;
    }}
    #panel-title {{ font-size: 15px; font-weight: 700; color: var(--text-head); }}
    #panel-subtitle {{ font-size: 12px; color: var(--text-sub); text-align: center; }}
    #last-updated {{ font-size: 11px; color: var(--text-muted); }}
    .btn-row {{ display: flex; gap: 4px; flex-wrap: wrap; justify-content: center; }}
    .fbtn {{
      background: var(--btn-bg); border: 1px solid var(--btn-border); color: var(--btn-color);
      padding: 5px 13px; border-radius: 4px; font-size: 12px;
      text-decoration: none; display: inline-block; white-space: nowrap;
      transition: background 0.12s, color 0.12s; cursor: pointer;
    }}
    .fbtn:hover {{ background: var(--btn-hover-bg); color: var(--btn-hover-color); }}
    #theme-toggle {{
      position: fixed; top: 10px; right: 12px; z-index: 200;
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 6px; padding: 4px 9px; font-size: 11px; color: var(--text-sub); cursor: pointer;
    }}
    #stats {{ border-bottom: 1px solid var(--border); }}
    .stats-inner {{ display: flex; justify-content: center; flex-wrap: wrap; }}
    .stat {{
      flex: 1; min-width: 120px; max-width: 200px; text-align: center;
      padding: 10px 8px 9px; border-right: 1px solid var(--border);
    }}
    .stat:last-child {{ border-right: none; }}
    .stat-value {{ font-size: 1.2rem; font-weight: 700; line-height: 1.1; }}
    .stat-label {{ font-size: 0.67rem; color: var(--text-sub); text-transform: uppercase; letter-spacing: 0.05em; margin-top: 3px; }}
    .stat-sub   {{ font-size: 0.67rem; color: var(--text-muted); margin-top: 1px; }}
    #main {{ flex: 1; padding: 16px; display: flex; flex-direction: column; gap: 20px; max-width: 1100px; width: 100%; margin: 0 auto; }}
    .chart-block {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }}
    .chart-title {{ font-size: 13px; font-weight: 600; color: var(--chart-title); margin-bottom: 4px; }}
    .chart-sub   {{ font-size: 11px; color: var(--chart-sub); margin-bottom: 10px; }}
    .chart-container {{ position: relative; height: 480px; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 8px 16px; margin-bottom: 10px; }}
    .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--legend-color); }}
    .legend-dot {{ width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }}
    .section-divider {{ border: none; border-top: 1px solid var(--border); }}
    .section-heading {{ font-size: 1.1rem; font-weight: 700; color: var(--section-head); margin-bottom: 0.3rem; }}
    .section-sub {{ font-size: 0.82rem; color: var(--section-sub); line-height: 1.5; margin-bottom: 1.2rem; }}
    .fund-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 1rem; }}
    .fund-card {{
      background: var(--fund-card-bg); border: 1px solid var(--border);
      border-top: 3px solid var(--accent);
      border-radius: 10px; padding: 1.2rem;
      display: flex; flex-direction: column; gap: 0.45rem;
    }}
    .fund-icon {{ font-size: 1.4rem; line-height: 1; }}
    .fund-name {{ font-size: 0.95rem; font-weight: 700; color: var(--fund-name); }}
    .fund-source {{ font-size: 0.72rem; font-weight: 600; color: var(--accent); text-transform: uppercase; letter-spacing: 0.04em; }}
    .fund-card ul {{ margin: 0.2rem 0 0 1rem; font-size: 0.78rem; color: var(--fund-list); line-height: 1.7; }}
    .fund-note {{
      margin-top: 0.5rem; font-size: 0.72rem; color: var(--fund-note);
      border-top: 1px solid var(--border); padding-top: 0.5rem; line-height: 1.4;
    }}
    .fund-note strong {{ color: var(--fund-note-strong); }}
    #data-source {{ font-size: 0.75rem; color: var(--source-color); text-align: center; padding-bottom: 4px; }}
    #data-source a {{ color: var(--source-a); text-decoration: none; }}
    #data-source a:hover {{ color: var(--text-sub); }}
    footer {{ text-align: center; padding: 14px 16px; font-size: 0.74rem; color: var(--footer-color); border-top: 1px solid var(--footer-border); }}
    footer a {{ color: var(--text-sub); text-decoration: none; }}
    footer a:hover {{ color: var(--text); }}
    @media (max-width: 520px) {{ .stat-value {{ font-size: 1rem; }} .chart-container {{ height: 560px; }} }}

    /* ── drill-down panel ── */
    #drill-panel {{
      display: none;
      background: var(--drill-bg);
      border: 1px solid var(--drill-border);
      border-radius: 8px;
      padding: 18px 16px 20px;
      animation: slideIn 0.18s ease;
    }}
    @keyframes slideIn {{
      from {{ opacity: 0; transform: translateY(-6px); }}
      to   {{ opacity: 1; transform: translateY(0); }}
    }}
    #drill-header {{
      display: flex; align-items: baseline; justify-content: space-between;
      margin-bottom: 14px; flex-wrap: wrap; gap: 6px;
    }}
    #drill-title {{ font-size: 15px; font-weight: 700; color: var(--drill-title); }}
    #drill-totals {{ font-size: 11px; color: var(--text-sub); }}
    #drill-close {{
      background: none; border: 1px solid var(--drill-close-border); color: var(--drill-close-color);
      border-radius: 4px; padding: 3px 9px; font-size: 11px; cursor: pointer;
      transition: border-color 0.12s, color 0.12s;
    }}
    #drill-close:hover {{ border-color: var(--text-sub); color: var(--text); }}
    #drill-body {{
      display: grid; grid-template-columns: 240px 1fr; gap: 20px; align-items: start;
    }}
    @media (max-width: 600px) {{
      #drill-body {{ grid-template-columns: 1fr; }}
      #drill-donut-wrap {{ height: 220px; }}
    }}
    #drill-donut-wrap {{ position: relative; height: 260px; }}
    #drill-legend {{ display: flex; flex-direction: column; gap: 9px; padding: 4px 0; }}
    .drill-legend-row {{ display: flex; align-items: center; gap: 9px; }}
    .drill-swatch {{ width: 11px; height: 11px; border-radius: 2px; flex-shrink: 0; }}
    .drill-legend-name {{ font-size: 12px; font-weight: 600; color: var(--drill-name); flex: 1; }}
    .drill-legend-vals {{ text-align: right; font-size: 11px; color: var(--drill-vals); white-space: nowrap; }}
    .drill-legend-pct {{ font-size: 11px; font-weight: 700; color: var(--drill-pct); min-width: 36px; text-align: right; }}
    #drill-hint {{ font-size: 11px; color: var(--drill-hint); margin-top: 12px; text-align: center; }}
  </style>
</head>
<body>

  <button id="theme-toggle" onclick="toggleTheme()">🌙 Dark</button>

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
      <div class="chart-sub">Solid = spent through Q{quarter} · Dim = remaining budget · <strong style="color:#60a5fa;">Click a bar to see the spending breakdown</strong></div>
      <div class="legend">
        <span class="legend-item"><span class="legend-dot" style="background:#ef4444;"></span>Public Safety</span>
        <span class="legend-item"><span class="legend-dot" style="background:#22c55e;"></span>Parks &amp; Community</span>
        <span class="legend-item"><span class="legend-dot" style="background:#06b6d4;"></span>Health &amp; Wellbeing</span>
        <span class="legend-item"><span class="legend-dot" style="background:#8b5cf6;"></span>Housing &amp; Social Services</span>
        <span class="legend-item"><span class="legend-dot" style="background:#6366f1;"></span>Courts &amp; Justice</span>
        <span class="legend-item"><span class="legend-dot" style="background:#64748b;"></span>Admin &amp; Other</span>
      </div>
      <div class="chart-container"><canvas id="deptChart"></canvas></div>
      <div id="drill-hint" style="margin-top:8px;">👆 Click any bar to see how that department spends its budget</div>
    </div>

    <!-- Drill-down panel (hidden until a bar is clicked) -->
    <div id="drill-panel">
      <div id="drill-header">
        <div>
          <div id="drill-title">Department</div>
          <div id="drill-totals"></div>
        </div>
        <button id="drill-close" onclick="closeDrill()">✕ Close</button>
      </div>
      <div id="drill-body">
        <div id="drill-donut-wrap"><canvas id="drillChart"></canvas></div>
        <div id="drill-legend"></div>
      </div>
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
    const BREAKDOWN = {js_breakdown};
    const CAT_META  = {js_cat_meta};

    const isDark   = document.documentElement.classList.contains("dark");
    const gridColor = isDark ? "#252b3b" : "#e8ecf0";
    const tickColor = isDark ? "#64748b" : "#6b7280";
    const legColor  = isDark ? "#94a3b8" : "#4b5563";
    const tipBg     = isDark ? "#1e2230" : "#ffffff";
    const tipBorder = isDark ? "#3d4868" : "#e2e8f0";
    const tipTitle  = isDark ? "#f1f5f9" : "#111827";
    const tipBody   = isDark ? "#e2e8f0" : "#374151";

    const toggleBtn = document.getElementById("theme-toggle");
    toggleBtn.textContent = isDark ? "☀️ Light" : "🌙 Dark";
    function toggleTheme() {{
      const dark = document.documentElement.classList.toggle("dark");
      localStorage.setItem("theme", dark ? "dark" : "light");
      location.reload();
    }}

    const fmt = n => {{
      const abs = Math.abs(n);
      const s = abs >= 1e6 ? (abs / 1e6).toFixed(1) + "M" : (abs / 1e3).toFixed(0) + "K";
      return (n < 0 ? "-$" : "$") + s;
    }};
    const pct = (a, b) => b ? ((a / b) * 100).toFixed(1) + "%" : "—";

    // ── main dept bar chart ──────────────────────────────────────────────────
    const deptChart = new Chart(document.getElementById("deptChart"), {{
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
        onClick(evt, elements) {{
          if (elements.length) openDrill(DEPTS[elements[0].index].name);
        }},
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            backgroundColor: tipBg,
            borderColor: tipBorder,
            borderWidth: 1,
            titleColor: tipTitle,
            bodyColor: tipBody,
            callbacks: {{
              title: ctx => ctx[0].label,
              afterTitle: ctx => `Adopted: ${{fmt(DEPTS[ctx[0].dataIndex].budget)}}`,
              label: ctx => {{
                const d = DEPTS[ctx.dataIndex];
                return ctx.datasetIndex === 0
                  ? ` Spent through Q{quarter}: ${{fmt(d.spent)}} (${{pct(d.spent, d.budget)}})`
                  : ` Remaining: ${{fmt(d.budget - d.spent)}}`;
              }},
              afterBody: () => ["", "  Click to see spending breakdown →"],
            }},
          }},
        }},
        scales: {{
          x: {{
            stacked: true,
            ticks: {{ color: tickColor, font: {{ size: 11 }}, callback: v => "$" + (v / 1e6).toFixed(0) + "M" }},
            grid: {{ color: gridColor }},
            beginAtZero: true,
          }},
          y: {{
            stacked: true,
            ticks: {{ color: tickColor, font: {{ size: 11 }} }},
            grid: {{ color: gridColor }},
          }},
        }},
      }},
    }});

    // ── drill-down ────────────────────────────────────────────────────────────
    let drillChart = null;

    function openDrill(deptName) {{
      const cats = BREAKDOWN[deptName];
      if (!cats) return;

      const panel = document.getElementById("drill-panel");
      const dept  = DEPTS.find(d => d.name === deptName);

      document.getElementById("drill-title").textContent = deptName;
      document.getElementById("drill-totals").textContent =
        `Adopted: ${{fmt(dept.budget)}}  ·  Spent Q{quarter}: ${{fmt(dept.spent)}} (${{pct(dept.spent, dept.budget)}})`;

      // Filter to categories with non-zero budget, sort descending
      const entries = Object.entries(cats)
        .filter(([, v]) => v.budget > 0)
        .sort(([, a], [, b]) => b.budget - a.budget);

      const labels = entries.map(([cat]) => cat);
      const budgets = entries.map(([, v]) => v.budget);
      const colors  = entries.map(([cat]) => CAT_META[cat]?.color ?? "#64748b");
      const totalBudget = budgets.reduce((a, b) => a + b, 0);

      // Destroy previous drill chart
      if (drillChart) {{ drillChart.destroy(); drillChart = null; }}

      drillChart = new Chart(document.getElementById("drillChart"), {{
        type: "doughnut",
        data: {{
          labels,
          datasets: [{{
            data: budgets,
            backgroundColor: colors,
            borderWidth: 2,
            borderColor: isDark ? "#1a1f2e" : "#ffffff",
            hoverOffset: 8,
          }}],
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          cutout: "58%",
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{
              backgroundColor: "#1e2230",
              borderColor: "#3d4868",
              borderWidth: 1,
              titleColor: "#f1f5f9",
              bodyColor: "#e2e8f0",
              callbacks: {{
                label: ctx => {{
                  const p = totalBudget ? ((ctx.parsed / totalBudget) * 100).toFixed(1) : "0";
                  return ` ${{fmt(ctx.parsed)}} (${{p}}% of dept budget)`;
                }},
              }},
            }},
          }},
        }},
      }});

      // Build legend
      const legendEl = document.getElementById("drill-legend");
      legendEl.innerHTML = "";
      entries.forEach(([cat, v]) => {{
        const p = totalBudget ? ((v.budget / totalBudget) * 100).toFixed(1) : "0";
        const icon = CAT_META[cat]?.icon ?? "";
        const color = CAT_META[cat]?.color ?? "#64748b";
        const row = document.createElement("div");
        row.className = "drill-legend-row";
        row.innerHTML = `
          <span class="drill-swatch" style="background:${{color}};"></span>
          <span class="drill-legend-name">${{icon}} ${{cat}}</span>
          <span class="drill-legend-vals">${{fmt(v.budget)}}</span>
          <span class="drill-legend-pct">${{p}}%</span>`;
        legendEl.appendChild(row);
      }});

      panel.style.display = "block";
      document.getElementById("drill-hint").style.display = "none";
      panel.scrollIntoView({{ behavior: "smooth", block: "nearest" }});
    }}

    function closeDrill() {{
      document.getElementById("drill-panel").style.display = "none";
      document.getElementById("drill-hint").style.display = "block";
      if (drillChart) {{ drillChart.destroy(); drillChart = null; }}
    }}
  </script>
</body>
</html>"""


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    print("Fetching General Fund data...")
    rows = fetch_rows()
    print(f"  {len(rows)} rows fetched")

    fy, quarter, depts, dept_cats = aggregate(rows)
    print(f"  FY{fy} Q{quarter} — {len(depts)} departments")

    html = generate_html(fy, quarter, depts, dept_cats)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"  Written → {OUT_PATH}")


if __name__ == "__main__":
    main()
