# Austin 311 Bot — Prioritized Gap Analysis

Cross-reference of current coverage (Telegram bot + web pages + internal modules) against the master 311 service codes (`analysis/311categories.txt`) and standard city open-data reporting categories used by comparable civic-tech projects.


## 🔴 HIGH PRIORITY (large-volume categories, Austin-relevant, missing from your stack)

### 2. Shared Micromobility (E-scooters, Bikeshare) - WIP
- **311 volume:** \#14 at **39,043 reports**
- **Current status:** Nothing. This is a uniquely Austin pain-point (dockless scooter complaints, sidewalk obstructions, improperly parked bikes/scooters).

- **Common in peer cities:** Yes, especially cities with Lime/Bird/Veo/CapMetro Bikeshare.

### 4. Debris in Street (non-storm) WIP

- **311 volume:** \#13 at **40,332 reports**

- **Current status:** Mentioned in `/traffic` description but no dedicated stats, hotspots, or trends.

- **Common in peer cities:** Yes — illegal dumping, construction debris, trash in right-of-way.

- **Suggested approach:** Either break it out as its own command or give it a sub-view in `/traffic`/`/code`.

### 6. Code Violations & Property Standards (full scope) -WIP Needs anaylsis

- **311 volume:** \#26 DSD Follow-Up (16,270), \#27 Concerns in ROW (15,827), \#40 Billboard (10,407), \#58 Construction Concerns (5,275), \#42 Construction in ROW (8,465) = **~46K+ reports**

- **Current status:** Only `/code` for building permits (4 codes). Missing the broader code enforcement picture — tall weeds, junked vehicles, substandard structures, zoning violations.

- **Common in peer cities:** Code enforcement is a staple civic data category.


## 🟡 MEDIUM PRIORITY (moderate volume or high civic interest)

### 7. Trash/Recycling/Compost (Austin Resource Recovery)

- **311 volume:** \#77 ARR Pet Search (1,833), \#93 ARR Street Sweeping (780), \#99 ARR Special Services (383), \#127 Missed Yard Trimmings (1) = **~3K reports** (likely undercounted — many ARR categories not in top 127)

- **Current status:** Nothing. Missed collection is the \#1 reason people call non-emergency lines in most cities.

- **Suggested approach:** `/trash` or `/recycling` command tracking missed collections, bulk pickup, etc.


### 9. Fireworks Complaints

- **311 volume:** \#61 at **4,777 reports** — highly seasonal (July 4, NYE)

- **Current status:** Nothing.

- **Suggested approach:** Seasonal sub-view under noise complaints with holiday-trend analysis.

### 10. Street Sweeping

- **311 volume:** \#63 at 4,770 + \#93 ARR at 780 = **~5.5K**

- **Current status:** Nothing.

- **Suggested approach:** Low-complexity add to `/traffic` or standalone.



## ✅ COMPLETED (Fun Data page additions)

### Graffiti Hall of Fame
- **Added:** May 2026
- **What it shows:** Most frequently tagged locations in Austin based on Open311 HHSGRAFF data
- **Features:** Winner banner, top 15 leaderboard, repeat rate stats, link to graffiti map

### Parking Shenanigans
- **Added:** May 2026
- **What it shows:** Hottest enforcement times by hour, most common violation types, carousel of funniest violation descriptions
- **Features:** Peak hour highlight, hourly bar chart, top 8 violation types, prev/next carousel nav

### 🗒️ Future Fun Data Ideas (not yet started)

- **#7 Most Complained-About Address in Austin** — Aggregate across all 311 categories to find the single address generating the most requests

### 🏗️ Infrastructure Improvements

- **Pre-generate Fun Data cards via GitHub Actions workflow** — Currently all Fun Data cards (Bar of the Month, Coyotes, Graffiti Hall of Fame, Parking Shenanigans) make live API calls from the visitor's browser. This is wasteful and slow. Instead, add a `scripts/generate_fun_data.py` script that pre-generates the JSON data for each card and saves it to `docs/fun/data.json`. Then:
  1. Add a `generate-fun-data.yml` workflow that runs daily and regenerates the JSON
  2. Update the Fun Data page to fetch from `data.json` instead of making live API calls
  3. This eliminates redundant API calls, speeds up page load, and reduces Open311 API load

- **Graffiti Speedrun Leaderboard (to include in pre-generation)** — Once the pre-generation workflow exists, add speedrun stats (avg/median resolution time, fastest-resolved tickets with medals, resolution time distribution buckets, longest-open ticket) to the graffiti card. The data is already fetched for the Hall of Fame — just needs to be computed and displayed. This was built and then removed because it added a duplicate client-side API call.

## 🟢 LOWER PRIORITY (niche, low volume, or already partially covered)

### 12. Vision Zero / Pedestrian Safety

- **311 volume:** Speed Management \#39 (9,355), Pedestrian Crossing New/Modify \#101 (345), School Zone Beacons \#52 (6,304), Guardrail \#84 (1,214) = **~17K combined**

- **Current status:** Crash stats, but no aggregated "safety infrastructure backlogs" view.

- **Common in peer cities:** Vision Zero dashboards are increasingly standard.

- **Suggested approach:** Aggregate these under a `/safety` or `/visionzero` view.

### 13. 311 Agency Performance / Response Times (Cross-Service)

- **Current status:** Per-service resolution times (graffiti, noise, parking, parks). No **city-wide** average response times, backlog aging, or service-level trends.

- **Common in peer cities:** Many 311 dashboards include agency-by-agency performance metrics.

- **Suggested approach:** A `/311stats` command that shows global open/closed counts, average resolution days across all services, and oldest-open tickets.

### 14. Austin Resource Recovery — Special Services

- **311 volume:** \#99 at 383 — bulk pickup, household hazardous waste, etc.

- **Current status:** Nothing.

### 15. Fire Incident / AFD Data

- **Common in peer cities:** Fire response times, incident types, station coverage.

- **Current status:** Nothing.

- **Suggested approach:** AFD publishes incident data on Socrata — could be a `/fire` module.

### 16. Air Quality

- **Current status:** Nothing. Environmental air quality (ozone, PM2.5) is a common civic dashboard metric.

- **Suggested approach:** Could complement the water quality page as an `/air` or `/environment` dashboard.

### 17. City Council / Meeting Data

- **Current status:** Nothing. Common in civic-data projects.

- **Suggested approach:** Low-tech HTML page scraping city meeting agendas/minutes.

### 18. Demographics & Census Integration

- **Current status:** Budget page for FY2026. Nothing demographic.

- **Suggested approach:** Add census tract overlays to existing map pages for equity analysis (e.g., "are 311 response times worse in lower-income tracts?").


## 🛠️ Known Technical Debt

### Parks hub — address-based park name extraction

The `_extract_park_name()` function in `parks/parks_bot.py` relies on a park keyword appearing in the first segment of the 311 address field (e.g. `"Zilker Park, 2100 Barton Springs Rd"`). Many parks are filed with only a bare street address — e.g. Parque Zaragoza (2608 Gonzales St) has no park name in the address at all, so its complaints get bucketed as "Gonzales St" in the hub and can't be found by searching the park name.

**Fix:** Build a `_PARK_ADDRESS_MAP` lookup dict keyed by street address → canonical park name. Seed it from the [Austin Parks Locations dataset](https://data.austintexas.gov) which has official addresses for all city parks. Apply the lookup in `_extract_park_name()` before falling back to keyword matching.

**Secondary issue:** Parks with no complaints in the last 90 days don't appear in the hub at all. Consider extending the hub window to 365 days, or showing a "no recent activity" state for known parks once the address map exists.

---

## 📊 Summary Table

| Priority | Category | 311 Volume | Current Status |
| - | - | - | - |
| 🔴 HIGH | Dead Animal Collection | ~33K | ❌ Missing |
| 🔴 HIGH | Shared Micromobility | ~39K | ❌ Missing |
| 🔴 HIGH | Storm Debris/Drainage/Flooding | ~97K | ❌ Missing |
| 🔴 HIGH | Debris in Street | ~40K | ⚠️ Mentioned only |
| 🔴 HIGH | Tree Issues (ROW/Parks/Env) | ~18K | ⚠️ Partial (parks only) |
| 🔴 HIGH | Code Violations (full scope) | ~46K | ⚠️ Building permits only |
| 🟡 MED | Trash/Recycling/Compost | ~3K+ | ❌ Missing |
| 🟡 MED | Obstruction in ROW | ~22K | ❌ Missing |
| 🟡 MED | Fireworks Complaints | ~5K | ❌ Missing |
| 🟡 MED | Street Sweeping | ~5.5K | ❌ Missing |
| 🟡 MED | Environmental Spills/Algae | ~850 | ⚠️ Water quality only |
| 🟢 LOW | Vision Zero/Safety Backlog | ~17K | ⚠️ Crash stats only |
| 🟢 LOW | 311 Performance (cross-service) | N/A | ⚠️ Per-service only |
| 🟢 LOW | Fire Incidents (AFD) | N/A | ❌ Missing |
| 🟢 LOW | Air Quality | N/A | ❌ Missing |
| 🟢 LOW | Council/Meeting Data | N/A | ❌ Missing |


**Current strong coverage:** graffiti, noise, parking, animal services, bicycle, parks, traffic/crashes, homeless encampments, restaurant inspections, court caseloads, child care.

**Biggest blind spots by volume:**

1. Stormwater/drainage/flooding (~97K)

2. Code enforcement (~46K)

3. Debris in street (~40K)

4. Micromobility/scooters (~39K)



CapMetro's Bikeshare program (formerly MetroBike/Austin B-cycle) certainly draws strong opinions, and the good news is there's a wealth of public data available for you to form your own conclusions. Here are the key datasets, dashboards, and data points to get you started.

### 📊 Public Datasets & Dashboards

You have several options for accessing the raw data, though you should be aware of some recent gaps in reporting.

- **City of Austin Open Data Portal (Primary Source)**: The official source for historic trip-level data.

  - **Austin MetroBike Trips**: This dataset includes details on trip duration, start/end stations, and user membership type. It's accessible for download in multiple formats (CSV, JSON) and can be accessed via APIs.

  - **🔴 Important Caveat on Data Currency**: The data flow has been disrupted by the 2024 transition to a new vendor and fleet. As of late 2025, the city is working with*** **CapMetro to restore the refresh, meaning **recent trip data is not yet available**.

- **Station Locations & Status**:

  - **Austin MetroBike Kiosk Locations**: This dataset provides the latitude and longitude for all bike rental kiosks.

  - **Real-time Station Status**: For current bike and dock availability, you can query the programmatically via the **CityBikes API**, which includes CapMetro's network.

- **Third-Party Ridership Dashboard**: The city's official data portal points to the **Ride Report public dashboard** for "current micromobility data". This may be your best bet for the most up-to-date aggregated usage statistics.

### 📈 Key Performance Metrics (Usage & Cost)

The data tells an interesting story of growth, disruption, and rising costs:

- **Usage Trends**: The program saw significant growth from fewer than 5,000 monthly trips before 2019 to nearly 28,000 by April 2023. However, the transition caused significant disruption, with annual ridership dipping from 316,980 in 2022 to 283,965 in 2023.

- **Typical Trip**: In 2024, the median ride was quite short at just **8 minutes**.

- **The Fare Hike (August 2025)**: The first price increase since the program's 2013 launch significantly altered the cost for users. Here is a summary of the changes based on CapMetro's published rates:

| ***Fare Option** | ***Previous Price** | ***New Price** |
| - | - | - |
| ***Pay-As-You-Go** | *![]()**1.09+0.23/min** | *![]()**2.50+0.25/min** |
| ***1-Day Pass** | ***$12.99 (unlimited 60-min rides)** | ***$10.00 (unlimited 30-min rides)** |
| ***31-Day Pass** | ***$11.00 (unlimited 60-min rides)** | ***$25.00 (unlimited 30-min rides)** |
| ***Annual Pass** | ***$86.60 (unlimited 60-min rides)** | ***$150.00 (unlimited 30-min rides)** |
| ***UT Student Pass** | ***$12.00/year** | ***$30.00/year (subsidized)** |

- **Program Costs & Financing**: The financial picture at a high level shows a program heavily reliant on public funding to supplement fares.

  - ![]()**Major Public Investment**: The 2024 revamp and expansion were funded by an 11.3millionfederalgrantand2.8 million in city matching funds. The 10-year operating contract with CapMetro is valued at up to **$18.55 million**.

  - **Operating Deficit**: CapMetro officials have stated that even with the increased fares, the revenue generated **will not cover the entire operating cost** of the service.

  - **System Investment**: CapMetro's FY 2024 budget allocated $2.4 million specifically for Bikeshare operations and expansion.

  - ![]()**Cost Per Trip (Rough Calculation)**: Combining an unconstrained monthly trip number (~28,000/month) with the 20.6millionfederalgrantover10yearsyieldsasimplifiedcapitalcost∗∗northof6 per trip\*\*, *before* accounting for any operating expenses, fare revenue, or the separate $18.55 million for management.

### ⚙️ The User Experience & "Boondoggle" Criticism

The program's history and user feedback provide context for the public criticism:

- **From BCycle to Bikeshare**: CapMetro took over the popular Austin B-cycle system and rebranded it as MetroBike in 2020.

- **The 2024 Revamp**: The system was temporarily shut down in July 2024 to transition to an entirely new, 100% electric fleet under the name "CapMetro Bikeshare". The goal was to expand from 76 stations to over 300 in the next decade.

- **App Functionality & Data Void**: The transition was marred by the launch of a new app riddled with complaints. Users reported being completely unable to add payment cards or purchase passes. The app has a very poor aggregate user rating of **2.1 to 2.7 out of 5 stars** across platforms. This software issue coincides with the disruption of the official city data, creating a double frustration for those wanting to track the program's performance.

If you'd like to explore any of these datasets or angles further, let me know.



