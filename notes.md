# Ideas: Tighter integration between austin311.com and @austin311bot

Here's what I see and a set of ideas grouped by theme. Treat this as a menu — happy to drill into any of them or skip the rest.

## What you have today

- 17 map pages on the site, all with rich popups (ticket link, address, status, description, sometimes scraped Additional Details). Hub at [docs/index.html](docs/index.html) is a clean card grid with a single "Get alerts on Telegram" CTA at the bottom.
- The bot exposes ~25 commands ([austin311_bot.py:2384-2391](austin311_bot.py#L2384-L2391)) and links *out* to the maps in handlers ([austin311_bot.py:317-2324](austin311_bot.py#L317)), but the maps don't link *into* a specific bot action.
- Geographic alerts (`nearby_311`, `animal_nearby`, `crash_nearby`) all run as daily 08:00 UTC digests ([austin311_bot.py:2413-2414](austin311_bot.py#L2413)).

The biggest gap: the site and the bot share data but don't share *user intent*. A visitor who sees a hot spot on the parking map has to manually `/subscribe` and re-type the address.

## Ideas, ordered by effort

### Quick wins

1. **"Subscribe to alerts here" button inside every popup.** Telegram supports start-payload deep links: `https://t.me/austin311bot?start=sub_311_30.245_-97.789_05`. Parse the payload in `start()` ([austin311_bot.py:2396](austin311_bot.py#L2396)) and skip straight to the radius-confirmation step in the alerts flow. One click on the map → subscribed.
2. **"Open in bot" link in each map's header.** Next to your dark-mode toggle, add `t.me/austin311bot?start=<service>` so people who hit the map from Twitter can land in the bot pre-loaded on that command.
3. **Right-click / long-press a map to subscribe to that point.** Leaflet contextmenu plugin → drop a pin → "📍 Alert me on Telegram for this spot." Lat/lon embedded in the same start-payload from #1.
4. **Use the canonical short URL everywhere.** Bot handlers and alert messages currently mix `austin311.com` and `seanatwork.github.io/austin311bot-unofficial`. Pick one and replace the long URL in [austin311_bot.py:317-2324](austin311_bot.py#L317).
5. **Open Graph cards per map.** Add `og:title`, `og:description`, `og:image` (static PNG snapshot of the map) to each `docs/<svc>/index.html`. Twitter/Slack/iMessage previews drive a lot more bot signups than bare links.

### Medium

6. **Telegram WebApp buttons.** Replace the current "🗺️ Open Map" `url=` buttons with `WebAppInfo(url=...)`. Maps then open *inside* Telegram on mobile — no browser context switch, theme syncs to the chat. Game-changer for the parking/traffic flow.
7. **Per-report deep link in alerts.** The new richer alert links to the ticket page; also link to the map with the specific marker open, e.g. `austin311.com/animal/#report=26-00131487`. Add a small JS snippet to each map that reads `location.hash` and pops that marker on load.
8. **Photo attachments in alerts.** Open311 returns `media_url` on some reports. When present, send via `bot.send_photo()` instead of plain text — same "popup" parity goal as the change you just made.
9. **`/myblock` unified digest.** Single command that runs every category's nearby-radius query against the user's saved location and returns a one-shot snapshot ("This week within 0.5 mi: 3 potholes, 1 crash, 0 crimes, 4 parking, 2 graffiti"). Reuses the existing `_haversine_miles` and existing fetch helpers.
10. **Resolution-update alerts.** You now render status notes nicely. Tracking known-open tickets and pinging when they flip to Closed turns the bot into "the city's response tracker for me," not just "what got reported."

### Bigger bets

11. **Report-from-bot via Open311 POST.** The city's Open311 v2 supports `requests.json` POSTs with an API key. `/report Pothole 4500 S Lamar` becomes a real ticket. Closes the read/write loop and is the only meaningful differentiator vs. just being a viewer.
12. **Personalized overlays on the map.** If a user opens the site via a signed bot link, render their alert circles (radius rings) on top of the heatmap. "Here's *my* area, here's everything in it."
13. **Live-pulse landing page.** Replace the static "Real data on what your city is doing" copy with three live counters pulled from a JSON snapshot the workflows already produce: "Today: 412 open 311 · 3 fatal crashes (90d) · 17 districts reporting." Makes the site feel alive and keeps people hitting refresh.

## My picks if you want a starting trio

- **#1 + #6** together (deep-link subscribe + WebApp buttons): biggest UX leap for mobile users with one workflow's worth of work.
- **#5** (OG cards): cheapest signup-conversion lift you'll get all year.

Which thread do you want to pull?

Here's a menu of counter ideas grouped by feel, with the data source + computation noted. The site has access to the Open311 API, four Socrata datasets, ArcGIS districts, and the budget API — that's plenty to draw from.
Live-feeling activity ("right now")
Counter	Source	Compute
X new 311 reports in last 24h	Open311 /requests.json?start_date=24h	count of returned rows
X currently open (status=open across all categories)	Open311 + your existing cache	sum of cached status=open
X reports closed in last 24h (responsiveness)	Open311 with status=closed filter	count
X active crashes right now	Socrata y2wy-tgr5 (live, no cache lag)	rows with traffic_report_status=ACTIVE
Most recent crash: <address> — <time ago>	Socrata y2wy-tgr5	first row sorted by published_date desc
Severity / "this matters" headlines
Counter	Source	Compute
X fatal crashes in last 90 days (your example)	Socrata dx9v-zd7x	filter issue_reported ∈ {fatality terms}
X animal bite reports this week	Open311 service_code=ACBITE2	7-day count
X vicious-dog cases open	Open311 service_code=COAACDD, status=open	count
X violent crime incidents in last 7d	Socrata fdj4-gpfu	filter on crime_type keywords
X 311 tickets open >30 days (backlog)	Open311 cache	open & requested_datetime > 30d ago
Civic accountability ("how's the city doing")
Counter	Source	Compute
Median days to close a pothole	Open311 service_code for potholes	median(updated_at − requested_at) for closed
% of graffiti closed within 14 days	Open311 graffiti cache	closed-on-time / total-closed
Oldest still-open ticket: filed <date>, <service>	Open311 cache	min(requested_datetime) for status=open
District with biggest WoW increase (already have weekly digest logic)	Socrata fdj4-gpfu	_type_breakdown() by district, sorted by Δ
Days since last fatal crash	Socrata dx9v-zd7x	now − max(published_date) where fatal
Coverage / scope ("the data is alive")
Counter	Source	Compute
All 10 council districts reported today	Open311 + ArcGIS district polygons	unique districts in last-24h reports
X services tracked, X total reports	Static across all maps	sum of cache row counts
Map last refreshed <X minutes ago>	Workflow timestamp	now − snapshot timestamp
Trend / motion ("what's changing")
Counter	Source	Compute
Top-filed 311 category today: <category> (X reports)	Open311 24h	groupby + max
Trending up: <category> with arrow	Open311 7d vs prior 7d	groupby Δ, biggest gainer
Animal bites: 18 this week (+22% vs last)	Open311 ACBITE2	week / prior-week ratio
Local / quirky (Austin flavor)
Counter	Source	Compute
Loudest neighborhood this week	Open311 noise codes by zip	groupby zip, top
Park with most maintenance complaints	Open311 PRGRDISS by address	groupby park name
Coyote sightings this week	Open311 WILDEXPO	7-day count
Bar of the month	(already on /fun)	reuse
My pick for the hero trio
Three counters that together cover volume, severity, and coverage, all from sources you already fetch:
    1. 412 new 311 reports in the last 24h — Open311. Always changes, immediate "alive" signal.
    2. 3 fatal crashes in the last 90 days — Socrata dx9v-zd7x (the one your daily traffic-incident workflow already hits). Heavy emotional weight.
    3. All 10 council districts reporting today or 9 of 10 reporting — Open311 lat/lon → ArcGIS lookup. Plays nicely with how you've already split crime by district. When it's <10 it raises a question ("which district is silent?") which is itself interesting.
A nice fallback if any number looks low/empty: rotate in the trend counter ("Animal bites +22% vs last week") so the page is never showing a zero.
Suggested plumbing
One cron-driven workflow → docs/pulse.json:
{
  "updated": "2026-05-04T13:00:00Z",
  "open311": {"filed_24h": 412, "closed_24h": 380, "open_total": 4127, "districts_today": 10},
  "crashes":  {"fatal_90d": 3, "injury_30d": 247, "active_now": 12},
  "animal":   {"bites_7d": 18, "loose_dogs_7d": 67},
  "trends":   {"top_category_today": "Loose Dog", "fastest_growing": "Encampment", "growth_pct": 22}
}
Landing page does one fetch('/pulse.json') on load, swaps the three counters, and shows Updated X min ago underneath. ~40 lines of JS.
A new scripts/generate_pulse.py reuses your existing fetch helpers (open311_get, the Socrata calls in crime/crime_map.py and alerts/jobs.py _fetch_crashes_recent) — no new HTTP plumbing. Run it in generate-all-socrata.yml (already daily) so cache key alignment stays sensible.
Want me to build it? If so, which trio?


    4. 412 new 311 reports in the last 24h — Open311. Always changes, immediate "alive" signal.
    5. 3 fatal crashes in the last 90 days — Socrata dx9v-zd7x (the one your daily traffic-incident workflow already hits). Heavy emotional weight.
X violent crime incidents in last 7d	Socrata fdj4-gpfu	filter on crime_type keywords

