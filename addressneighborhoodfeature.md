# "311 Near You" — Address / Neighborhood Search Feature

## Overview

A dynamic map feature that lets users enter an address or neighborhood name, geocodes it client-side via the browser (or server-side via Nominatim), and displays all nearby 311 requests within a dynamically-computed radius.

The radius is **density-aware**:
- **Downtown / urban core** → smaller radius (~0.5–1 mi) because requests are dense
- **Suburbs / outskirts** → larger radius (~2–5 mi) because there are fewer requests per square mile

This ensures a meaningful number of results regardless of where you live.

---

## Files Modified / Created

### `austin311bot/scripts/generate_nearby_page.py` — NEW

A self-contained script that:

1. **Fetches ALL 311 requests** (no `service_code` filter) for the last 180 days via the Austin Open311 API.
2. **Filters** to valid Austin-area coordinates (lat 30.0–30.6, lon -98.0–-97.4).
3. **Writes a compact `data.json`** file with every request's `lat`, `lon`, `service_name`, `status`, and `requested_datetime`.
4. **Generates a self-contained Leaflet map page** at `docs/nearby/index.html` that includes:
   - A search input + geolocate button
   - Dynamic radius calculation based on **distance from Austin city center** (Congress & 6th)
   - Client-side distance filtering using the Haversine formula
   - Clustering via `Leaflet.markercluster` (loaded from CDN)
   - Dark mode toggle
   - URL parameter support: `?lat=X&lon=Y&q=address` (so the landing page can link directly)

**Key constants:**
```python
AUSTIN_LAT = 30.2672          # city center
AUSTIN_LON = -97.7431
MIN_RADIUS_MILES = 0.5        # downtown (dense)
MAX_RADIUS_MILES = 5.0        # far suburbs (sparse)
LAT_MIN, LAT_MAX = 30.0, 30.6
LON_MIN, LON_MAX = -98.0, -97.4
```

**Radius heuristic:**
```python
distance from city center (miles) → interpolates between MIN and MAX
```

**Run:**
```bash
cd austin311bot
AUSTINAPIKEY=sk_... python scripts/generate_nearby_page.py
```

### `austin311bot/scripts/generate_map.py` — MODIFIED

- Added `generate_nearby_page()` wrapper function (calls `scripts.generate_nearby_page.main()`)
- Added `"nearby": (generate_nearby_page, "nearby/index.html")` to the `CATEGORY_MAPS` dictionary

Allows CI to regenerate with:
```bash
python scripts/generate_map.py nearby
```

### `austin311bot/docs/index.html` — MODIFIED

Added a **"311 Near You" hero section** between the `<header>` and the `#pulse-bar`:

```html
<div class="nearby-hero">
  <h2>📍 311 Near You</h2>
  <p>Enter your address or neighborhood to see recent 311 requests on a map.</p>
  <div class="nearby-search-row">
    <input type="text" id="nearby-address" placeholder="e.g. Downtown Austin, 78701, Zilker…" />
    <button class="primary" onclick="searchNearby()">Search</button>
    <button onclick="locateNearby()" title="Use my location">📍</button>
  </div>
  <div class="nearby-stats">
    <span>Radius: <strong id="nearby-radius-display">1.5 mi</strong></span>
    <span>·</span>
    <span>Visible: <strong id="nearby-count">—</strong> requests</span>
  </div>
  <div id="nearby-error"></div>
</div>
```

**JavaScript on the landing page:**
- `searchNearby()` — geocodes the typed address via **Nominatim** (free, no API key), then redirects to `nearby/?lat=...&lon=...&q=...`
- `locateNearby()` — uses the **Geolocation API**, then redirects to `nearby/?lat=...&lon=...`
- Enter key in the input triggers search

**CSS:**
- Blue gradient hero background (`#1e3a5f` → `#2563eb` → `#3b82f6`)
- Responsive search row with flexbox
- Dark mode compatible shadows
- Error message styling

---

## Architecture / Data Flow

```
User types address → Nominatim geocoding (client-side fetch)
                           ↓
                    lat, lon, query
                           ↓
              Redirect to nearby/?lat=X&lon=Y&q=...
                           ↓
          nearby/index.html loads data.json (embedded requests)
                           ↓
          Client-side: compute distance from (lat,lon) to each request
          Client-side: dynamic radius based on distance from city center
                           ↓
          Display matching markers + cluster group on Leaflet map
```

The heavy work (fetching 311 data) is done server-side by the Python script once per day (via CI). The user interaction (geocoding, filtering, clustering) is all client-side JavaScript — no server needed at query time.

---

## Current Status

- ✅ `scripts/generate_nearby_page.py` created
- ✅ `scripts/generate_map.py` updated with `nearby` category
- ✅ `docs/index.html` updated with search widget
- ❌ **Bug**: The HTML template uses Python `%` string formatting (`_HTML_TEMPLATE % (...)`) but the template contains literal `%` characters in CSS/JS (e.g., `transform: translate(-50%, -50%)`), causing a `ValueError: unsupported format character ';' (0x3b)`.
  - **Fix needed**: Either escape all `%` signs in the template as `%%`, or switch the template to use an f-string or `.format()` + `{{` escaping.
- ❌ Test run succeeded in fetching data (96 records with coords) but failed on HTML generation due to above bug.