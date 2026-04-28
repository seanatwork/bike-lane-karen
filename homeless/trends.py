"""
Generate a static HTML trends page for homeless-related 311 reports.

Primary story: total matched complaint volume over time — how many 311 tickets
are filed each month that match homeless/encampment keywords across all relevant
service codes. Since Austin 311 has no homeless-specific category, keyword
matching across multiple codes is the only way to track this.

Secondary layer: how many of those tickets are closed with the HSO boilerplate.
"""

import io
from datetime import datetime, timezone
from collections import defaultdict


HSO_BOILERPLATE = (
    "“The Service Request submitted has been reviewed and administratively "
    "closed out. All reports related to encampments will be sent to the "
    "Homeless Strategy Office to ensure prioritization of the issue.”"
)

GA_SNIPPET = """<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-TS158R7XSN"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-TS158R7XSN');
</script>
"""


def _is_hso_deflected(record: dict) -> bool:
    """True if this ticket was closed with the HSO deflection boilerplate."""
    notes = (record.get("status_notes") or "").lower()
    if "homeless strategy" in notes:
        return True
    # Handles asterisk-formatted variant: "Homeless* *Strategy* *Office"
    if "homeless" in notes and "strategy" in notes:
        return True
    return False


def generate_homeless_trends(days_back: int = 365) -> tuple:
    """Fetch homeless 311 data and generate a static HTML trends page.

    Returns:
        tuple: (BytesIO buffer with HTML, summary string)
    """
    from homeless.homeless_bot import fetch_encampment_reports

    result = fetch_encampment_reports(days_back)
    records = result["records"]

    if not records:
        buf = io.BytesIO(b"<p>No data found.</p>")
        buf.seek(0)
        return buf, "No homeless-related 311 records found."

    MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    monthly = defaultdict(lambda: {"total": 0, "hso": 0, "other_closed": 0, "open": 0})

    total_hso = 0

    for r in records:
        dt_str = r.get("requested_datetime") or ""
        if not dt_str:
            continue
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            continue

        key = f"{dt.year}-{dt.month:02d}"
        status = (r.get("status") or "").lower()
        is_hso = _is_hso_deflected(r) and status == "closed"

        monthly[key]["total"] += 1
        if is_hso:
            monthly[key]["hso"] += 1
            total_hso += 1
        elif status == "closed":
            monthly[key]["other_closed"] += 1
        else:
            monthly[key]["open"] += 1

    sorted_months = sorted(monthly.keys())
    total_reports = len(records)
    total_open    = sum(1 for r in records if (r.get("status") or "").lower() == "open")
    deflection_rate = round(total_hso / total_reports * 100) if total_reports else 0

    # Volume trend: first half vs second half total count
    half = len(sorted_months) // 2
    first_half  = sorted_months[:half]
    second_half = sorted_months[half:]
    vol_first  = sum(monthly[m]["total"] for m in first_half)
    vol_second = sum(monthly[m]["total"] for m in second_half)
    avg_first  = round(vol_first  / len(first_half))  if first_half  else 0
    avg_second = round(vol_second / len(second_half)) if second_half else 0
    vol_arrow  = "📈 rising"  if avg_second > avg_first  else \
                 ("📉 falling" if avg_second < avg_first else "➡️ stable")

    # HSO rate trend
    hso_first  = sum(monthly[m]["hso"] for m in first_half)
    hso_second = sum(monthly[m]["hso"] for m in second_half)
    rate_first  = round(hso_first  / vol_first  * 100) if vol_first  else 0
    rate_second = round(hso_second / vol_second * 100) if vol_second else 0
    hso_arrow   = "📈 rising"  if rate_second > rate_first  else \
                  ("📉 falling" if rate_second < rate_first else "➡️ stable")

    # Monthly bar rows
    max_total = max((monthly[m]["total"] for m in sorted_months), default=1)
    month_rows_html = ""
    for key in sorted_months:
        d = monthly[key]
        yr, mo = key.split("-")
        label  = f"{MONTH_NAMES[int(mo)-1]} {yr}"
        total  = d["total"]
        hso    = d["hso"]
        other  = d["other_closed"]
        open_  = d["open"]

        hso_pct   = round(hso   / max_total * 100)
        other_pct = round(other / max_total * 100)
        open_pct  = round(open_ / max_total * 100)
        hso_rate  = round(hso / total * 100) if total else 0

        month_rows_html += f"""
        <div class="month-row">
          <span class="month-lbl">{label}</span>
          <div class="month-track">
            <div class="bar-seg hso-seg"   style="width:{hso_pct}%"   title="HSO deflected: {hso}"></div>
            <div class="bar-seg other-seg" style="width:{other_pct}%" title="Other closed: {other}"></div>
            <div class="bar-seg open-seg"  style="width:{open_pct}%"  title="Open: {open_}"></div>
          </div>
          <span class="month-total">{total}</span>
          <span class="month-hso">{hso_rate}% HSO</span>
        </div>"""

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Austin 311 — Homeless Report Trends</title>
  {GA_SNIPPET}
  <script>if(localStorage.getItem("theme")==="dark")document.documentElement.classList.add("dark");</script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg: #f8fafc; --bg-panel: #f1f5f9; --bg-card: #ffffff;
      --border: #e2e8f0; --text: #1e293b; --text-head: #0f172a;
      --text-sub: #64748b; --text-muted: #94a3b8;
      --btn-bg: #e2e8f0; --btn-border: #cbd5e1; --btn-color: #475569;
      --btn-hover-bg: #dbeafe; --btn-hover-color: #1e293b;
      --stat-border: #e2e8f0; --bar-bg: #e2e8f0;
      --note-bg: #fff7ed; --note-border: #fed7aa; --note-text: #9a3412;
    }}
    html.dark {{
      --bg: #0f1117; --bg-panel: #1e2230; --bg-card: #161a24;
      --border: #2d3348; --text: #e2e8f0; --text-head: #f1f5f9;
      --text-sub: #64748b; --text-muted: #475569;
      --btn-bg: #252b3b; --btn-border: #3d4868; --btn-color: #94a3b8;
      --btn-hover-bg: #2d3453; --btn-hover-color: #e2e8f0;
      --stat-border: #2d3348; --bar-bg: #252b3b;
      --note-bg: #2d1506; --note-border: #7c2d12; --note-text: #fdba74;
    }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg); color: var(--text);
      min-height: 100vh; display: flex; flex-direction: column; align-items: center;
      padding: 0 0 3rem; transition: background 0.2s, color 0.2s;
    }}
    #theme-toggle {{
      position: fixed; top: 10px; right: 12px; z-index: 200;
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 6px; padding: 4px 9px; font-size: 11px; color: var(--text-sub); cursor: pointer;
    }}
    #panel {{
      width: 100%; background: var(--bg-panel); border-bottom: 1px solid var(--border);
      padding: 10px 16px 12px; display: flex; flex-direction: column; align-items: center; gap: 5px;
      position: sticky; top: 0; z-index: 100;
    }}
    #panel-title {{ font-size: 15px; font-weight: 700; color: var(--text-head); }}
    #panel-sub   {{ font-size: 12px; color: var(--text-sub); text-align: center; }}
    #last-ran    {{ font-size: 11px; color: var(--text-muted); }}
    .btn-row {{ display: flex; gap: 4px; flex-wrap: wrap; justify-content: center; }}
    .fbtn {{
      background: var(--btn-bg); border: 1px solid var(--btn-border); color: var(--btn-color);
      padding: 4px 11px; border-radius: 4px; font-size: 12px; cursor: pointer;
      text-decoration: none; display: inline-block;
      transition: background 0.12s, color 0.12s; white-space: nowrap;
    }}
    .fbtn:hover {{ background: var(--btn-hover-bg); color: var(--btn-hover-color); }}
    #main {{ width: 100%; max-width: 900px; padding: 20px 16px; display: flex; flex-direction: column; gap: 20px; }}
    .stats-row {{
      display: flex; flex-wrap: wrap; gap: 0;
      background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; overflow: hidden;
    }}
    .stat {{
      flex: 1; min-width: 130px; text-align: center;
      padding: 14px 10px 12px; border-right: 1px solid var(--stat-border);
    }}
    .stat:last-child {{ border-right: none; }}
    .stat-val   {{ font-size: 1.4rem; font-weight: 700; line-height: 1.1; }}
    .stat-label {{ font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 3px; }}
    .chart-card {{
      background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px;
    }}
    .chart-title {{ font-size: 13px; font-weight: 700; color: var(--text-head); margin-bottom: 3px; }}
    .chart-sub   {{ font-size: 11px; color: var(--text-sub); margin-bottom: 14px; }}
    .legend-inline {{ display: flex; gap: 14px; margin-bottom: 12px; flex-wrap: wrap; }}
    .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text-sub); }}
    .legend-dot  {{ width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }}
    .month-row  {{ display: flex; align-items: center; gap: 8px; margin-bottom: 5px; font-size: 11px; }}
    .month-lbl  {{ flex: 0 0 62px; color: var(--text-sub); font-size: 10px; }}
    .month-track {{ flex: 1; display: flex; height: 14px; border-radius: 3px; overflow: hidden; background: var(--bar-bg); }}
    .bar-seg    {{ height: 14px; }}
    .hso-seg    {{ background: #dc2626; }}
    .other-seg  {{ background: #6b7280; }}
    .open-seg   {{ background: #f59e0b; }}
    .month-total {{ flex: 0 0 36px; text-align: right; color: var(--text-muted); font-size: 10px; }}
    .month-hso  {{ flex: 0 0 62px; text-align: right; color: #dc2626; font-size: 10px; font-weight: 600; }}
    .trend-badges {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }}
    .trend-badge {{
      display: inline-block; padding: 4px 12px; border-radius: 4px; font-size: 11px; font-weight: 600;
      background: var(--bg-panel); border: 1px solid var(--border); color: var(--text-sub);
    }}
    .hso-note {{
      background: var(--note-bg); border: 1px solid var(--note-border);
      border-left: 4px solid #dc2626; border-radius: 8px; padding: 14px 16px;
    }}
    .hso-note-title {{ font-size: 12px; font-weight: 700; color: var(--text-head); margin-bottom: 8px; }}
    .hso-quote {{
      font-size: 12px; font-style: italic; color: var(--note-text);
      line-height: 1.6; border-left: 3px solid #dc2626; padding-left: 10px; margin: 8px 0;
    }}
    .hso-note p {{ font-size: 12px; color: var(--text-sub); line-height: 1.6; margin-top: 8px; }}
    footer {{ font-size: 0.72rem; color: var(--text-muted); text-align: center; margin-top: 8px; }}
    footer a {{ color: var(--text-sub); text-decoration: none; }}
  </style>
</head>
<body>

  <button id="theme-toggle" onclick="toggleTheme()">🌙 Dark</button>

  <div id="panel">
    <div id="panel-title">🏕️ Homeless 311 — Complaint Trends</div>
    <div id="panel-sub">Keyword-matched reports across all relevant service codes · last {days_back} days<br/>
    Austin 311 has no homeless-specific category — complaints are filed under parks, ROW, debris &amp; noise codes</div>
    <div id="last-ran">Last ran: {now_str}</div>
    <div class="btn-row">
      <a class="fbtn" href="../">← Homeless Map</a>
      <a class="fbtn" href="../../">🏠 Home</a>
    </div>
  </div>

  <div id="main">

    <div class="stats-row">
      <div class="stat">
        <div class="stat-val" style="color:#3b82f6;">{total_reports:,}</div>
        <div class="stat-label">Total matched ({days_back}d)</div>
      </div>
      <div class="stat">
        <div class="stat-val" style="color:#f59e0b;">{total_open:,}</div>
        <div class="stat-label">Currently open</div>
      </div>
      <div class="stat">
        <div class="stat-val" style="color:#dc2626;">{total_hso:,}</div>
        <div class="stat-label">Closed → HSO</div>
      </div>
      <div class="stat">
        <div class="stat-val" style="color:#dc2626;">{deflection_rate}%</div>
        <div class="stat-label">HSO deflection rate</div>
      </div>
    </div>

    <div class="chart-card">
      <div class="chart-title">📊 Monthly Complaint Volume</div>
      <div class="chart-sub">Total keyword-matched 311 tickets filed each month, broken down by outcome</div>
      <div class="legend-inline">
        <div class="legend-item"><div class="legend-dot" style="background:#dc2626;"></div> Closed → HSO</div>
        <div class="legend-item"><div class="legend-dot" style="background:#6b7280;"></div> Other closed</div>
        <div class="legend-item"><div class="legend-dot" style="background:#f59e0b;"></div> Open</div>
      </div>
      {month_rows_html}
      <div class="trend-badges">
        <span class="trend-badge">Volume: avg {avg_first}/mo → {avg_second}/mo &nbsp;{vol_arrow}</span>
        <span class="trend-badge">HSO rate: {rate_first}% → {rate_second}% &nbsp;{hso_arrow}</span>
      </div>
    </div>

    <div class="hso-note">
      <div class="hso-note-title">⚠️ What is an HSO Deflection?</div>
      <div class="hso-quote">{HSO_BOILERPLATE}</div>
      <p>
        When a homeless-related 311 report is closed with this note, the ticket exits
        the standard 311 system and is routed to the Homeless Strategy Office — a separate
        department with no public-facing ticket tracking. The red portion of each bar above
        shows how many matched tickets received this closure each month.
      </p>
    </div>

    <footer>
      Data: <a href="https://311.austintexas.gov/open311/v2" target="_blank" rel="noopener">Austin Open311 API</a>
      &nbsp;·&nbsp; Keyword-matched across PRGRDISS, ATCOCIRW, OBSTMIDB, SBDEBROW, DRCHANEL, NOISECMP
      &nbsp;·&nbsp; Generated {now_str}
    </footer>

  </div>

  <script>
    const isDark = document.documentElement.classList.contains("dark");
    document.getElementById("theme-toggle").textContent = isDark ? "☀️ Light" : "🌙 Dark";
    function toggleTheme() {{
      const dark = document.documentElement.classList.toggle("dark");
      localStorage.setItem("theme", dark ? "dark" : "light");
      location.reload();
    }}
  </script>

</body>
</html>"""

    buf = io.BytesIO(html.encode("utf-8"))
    buf.seek(0)

    summary = (
        f"🏕️ *Homeless 311 Trends*\n"
        f"_Last {days_back} days_\n\n"
        f"📊 *{total_reports:,} matched reports*\n"
        f"🔴 *{total_hso:,} closed → HSO* ({deflection_rate}%)\n"
        f"Volume: avg {avg_first}/mo → {avg_second}/mo {vol_arrow}\n"
        f"HSO rate: {rate_first}% → {rate_second}% {hso_arrow}\n\n"
        f"_Source: Austin Open311 API_"
    )
    return buf, summary
