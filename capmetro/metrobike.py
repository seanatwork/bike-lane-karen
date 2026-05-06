"""
MetroBike Trip Data — core data layer.

Queries Austin's Socrata OpenData portal for MetroBike trip data
(https://data.austintexas.gov/dataset/Austin-MetroBike-Trip-Data/tyfh-5r8s).

Provides aggregated analytics:
- Electric vs classic bike usage over time
- Total trip counts by year
- Kiosk-to-kiosk flow mapping
- Kiosk lifecycle (stations added/removed over time)
- Membership type breakdown
- Kiosk locations (hardcoded from Google Places geocoding)
"""

import os
import re
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── Socrata API Configuration ────────────────────────────────────────────────

SOCRATA_BASE = "https://data.austintexas.gov/resource"
DATASET_ID = "tyfh-5r8s"
TIMEOUT = 30
MAX_RETRIES = 3

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "application/json",
            "User-Agent": "austin311bot/0.1 (MetroBike queries)",
        })
    return _session


def _make_request(url: str, params: dict, retries: int = 0) -> list:
    session = _get_session()
    try:
        resp = session.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        if retries < MAX_RETRIES:
            delay = 2 ** retries
            logger.warning(f"Request failed ({e}), retrying in {delay}s...")
            time.sleep(delay)
            return _make_request(url, params, retries + 1)
        raise


# ── Kiosk Locations ──────────────────────────────────────────────────────────
# Hardcoded lat/lng for every unique kiosk ID in the dataset.
# Derived from Google Places API geocoding of kiosk names.
# Format: {kiosk_id: (lat, lng, canonical_name)}

KIOSK_LOCATIONS: dict[str, tuple[float, float, str]] = {
    # ── Downtown / Central ──
    "2494": (30.26432, -97.74422, "Congress Ave. & W 2nd St"),
    "2495": (30.26619, -97.74352, "W 4th St & N Congress Ave"),
    "2496": (30.26993, -97.74213, "E 8th St & N Congress Ave"),
    "2497": (30.27265, -97.73925, "Texas Capitol Visitors Center"),
    "2498": (30.28947, -97.73681, "Speedway & E Dean Keeton St"),
    "2499": (30.26506, -97.74746, "Austin City Hall"),
    "2500": (30.26779, -97.74733, "Republic Square"),
    "2501": (30.26952, -97.75263, "5th/Bowie"),
    "2502": (30.25866, -97.74847, "W Riverside Dr & Barton Springs Rd"),
    "2503": (30.25115, -97.74912, "W James St & S Congress Ave"),
    "2504": (30.24890, -97.74995, "E Elizabeth St & S Congress Ave"),
    "2536": (30.26441, -97.73052, "Waller St & E 6th St"),
    "2537": (30.27259, -97.75676, "W 6th St"),
    "2538": (30.28034, -97.73907, "Bullock Texas State History Museum"),
    "2539": (30.26850, -97.73948, "Trinity Center Austin"),
    "2540": (30.27972, -97.74233, "Guadalupe St & W 17th St"),
    "2541": (30.27467, -97.74035, "Texas Capitol"),
    "2542": (30.26205, -97.72761, "Plaza Saltillo"),
    "2544": (30.25893, -97.71468, "Pedernales St & E 6th St"),
    "2545": (30.27676, -97.74760, "ACC Rio Grande Campus"),
    "2546": (30.27676, -97.74760, "ACC Rio Grande Campus"),
    "2547": (30.28381, -97.74193, "Guadalupe St & W 21st St"),
    "2548": (30.28616, -97.74192, "University Co-op"),
    "2549": (30.26025, -97.75109, "The Long Center for the Performing Arts"),
    "2550": (30.26733, -97.74704, "Guadalupe/4th"),
    "2552": (30.26764, -97.75160, "W 3rd St & West Ave"),
    "2561": (30.27364, -97.73743, "Capitol Visitors Parking Garage"),
    "2562": (30.26925, -97.73974, "E 8th St & San Jacinto Blvd"),
    "2563": (30.26009, -97.73830, "Rainey St & Davis St"),
    "2564": (30.26418, -97.73295, "E 5th St. & San Marcos St"),
    "2565": (30.26709, -97.73934, "Trinity St & E 6th St"),
    "2566": (30.26536, -97.75599, "Pfluger Pedestrian Bridge"),
    "2567": (30.26038, -97.75326, "Palmer Events Center"),
    "2568": (30.26917, -97.72856, "Victory East"),
    "2569": (30.26978, -97.73083, "E 11th St & San Marcos St"),
    "2570": (30.25216, -97.74874, "S Congress Ave & Academy Dr"),
    "2571": (30.26834, -97.73652, "E 8th St & Red River St"),
    "2572": (30.26373, -97.77140, "Barton Springs Pool"),
    "2574": (30.26698, -97.77297, "Zilker Metropolitan Park"),
    "2575": (30.26446, -97.75706, "W Riverside Dr & S Lamar Blvd"),
    "2576": (30.25758, -97.73813, "River St"),
    "2707": (30.25579, -97.73988, "Rainey St & Cummings St"),
    "2711": (30.26204, -97.76145, "Kinney Ave & Barton Springs Rd"),
    "2712": (30.26292, -97.75769, "S Lamar Blvd & Toomey Rd"),
    "2822": (30.26029, -97.71861, "Robert T Martinez Jr St & E 6th St"),
    "2823": (30.25644, -97.70987, "CapMetro"),
    "3291": (30.27206, -97.73866, "E 11th St & San Jacinto Blvd"),
    "3292": (30.25981, -97.72340, "Chicon St & E 4th St"),
    "3293": (30.25866, -97.74847, "W Riverside Dr & Barton Springs Rd"),
    "3294": (30.26874, -97.74521, "W 6th St & Lavaca St"),
    "3377": (30.24272, -97.81065, "Mopac Mobility Bike and Pedestrian Bridge"),
    "3381": (30.26010, -97.70949, "N Pleasant Valley Rd & E 7th St"),
    "3390": (30.26769, -97.74149, "Brazos St & E 6th St"),
    "3455": (30.26733, -97.74704, "Guadalupe/4th"),
    "3456": (30.26807, -97.74690, "5th/Guadalupe (Republic Square)"),
    "3464": (30.28142, -97.75167, "Pease District Park"),
    "3513": (30.25854, -97.74416, "Austin American-Statesman"),
    "3619": (30.26806, -97.74282, "W 6th St & N Congress Ave"),
    "3621": (30.28681, -97.74407, "The Quarters Nueces House"),
    "3635": (30.27631, -97.74478, "San Antonio St & W 13th St"),
    "3660": (30.26476, -97.73152, "Medina St & E 6th St"),
    "3684": (30.26339, -97.74455, "W Cesar Chavez St & N Congress Ave"),
    "3685": (30.27361, -97.75172, "W 9th St & Henderson St"),
    "3686": (30.26385, -97.76364, "Sterzing St & Barton Springs Rd"),
    "3687": (30.24830, -97.73213, "The Boardwalk at Lady Bird Lake"),
    "3790": (30.27796, -97.77237, "Deep Eddy Ave & Lake Austin Blvd"),
    "3791": (30.29613, -97.78381, "Enfield Rd & Lake Austin Blvd"),
    "3792": (30.28533, -97.74653, "Pearl St & W 22nd St"),
    "3793": (30.29341, -97.74415, "W 28th St & Rio Grande St"),
    "3794": (30.28947, -97.73681, "Speedway & E Dean Keeton St"),
    "3795": (30.28972, -97.74027, "W Dean Keeton St & Whitis Ave"),
    "3797": (30.28392, -97.74362, "W 21st St"),
    "3798": (30.28266, -97.73821, "Perry-Castañeda Library"),
    "3799": (30.28354, -97.73252, "Darrell K Royal-Texas Memorial Stadium"),
    "3838": (30.29056, -97.74291, "W 26th St & Nueces St"),
    "3841": (30.28721, -97.74472, "W 23rd St & Rio Grande St"),
    "4047": (30.27060, -97.74453, "Lavaca St & W 8th St"),
    "4048": (30.24846, -97.75011, "S Congress Ave"),
    "4050": (30.26588, -97.74471, "DuBois, Bryant & Campbell"),
    "4051": (30.27023, -97.73581, "E 10th St & Red River St"),
    "4052": (30.26888, -97.72430, "Rosewood Ave & Angelina St"),
    "4054": (30.26971, -97.71905, "Rosewood Ave & Chicon St"),
    "4055": (30.26638, -97.72146, "Salina St & E 11th St"),
    "4057": (30.26236, -97.72450, "Chalmers Ave & E 6th St"),
    "4058": (30.26174, -97.77266, "Hollow Creek Dr & Barton Hills Dr"),
    "4059": (30.25233, -97.73370, "RBJ Public Health Center"),
    "4060": (30.26208, -97.73822, "Fairmont Austin"),
    "4061": (30.26821, -97.74109, "Firehouse Hostel"),
    "4062": (30.24250, -97.71721, "S Lakeshore Blvd & S Pleasant Valley Rd"),
    "4699": (30.25166, -97.69841, "Eastside Bus Plaza BAY A"),
    "4879": (30.27916, -97.74375, "San Antonio St & W 16th St"),
    "4938": (30.28656, -97.74455, "Twenty Two 15"),
    "7125": (30.28718, -97.74793, "W 23rd St & San Gabriel St"),
    "7131": (30.27159, -97.73512, "Waterloo Greenway"),
    "7186": (30.25738, -97.74921, "One Texas Center"),
    "7187": (30.24517, -97.75133, "S Congress Ave & E Mary St"),
    "7188": (30.28533, -97.74653, "Pearl St & W 22nd St"),
    "7189": (30.29341, -97.74415, "W 28th St & Rio Grande St"),
    "7190": (30.27572, -97.74738, "W 12th St & Rio Grande St"),
    "7253": (30.25866, -97.74847, "W Riverside Dr & Barton Springs Rd"),
    "7341": (30.28924, -97.73274, "Park Pl & E Dean Keeton St"),
    "7637": (30.26273, -97.76297, "1701 Barton Springs Rd"),

    # ── Special / Event Kiosks ──
    "1001": (30.27033, -97.74346, "Downtown Austin"),
    "1006": (30.26698, -97.77297, "Zilker Metropolitan Park"),
    "1007": (30.26874, -97.74521, "W 6th St & Lavaca St"),
    "1008": (30.26685, -97.74950, "W 3rd St & Nueces St"),
}

# Aliases for kiosk IDs that have multiple names but share an ID
# This maps the canonical kiosk_id to use when grouping
KIOSK_ID_ALIASES: dict[str, str] = {
    "2498": "2498",  # Dean Keeton/Speedway (also Convention Center / 4th St. @ MetroRail)
    "2575": "2575",  # Riverside/South Lamar (also Riverside @ S. Lamar)
    "2707": "2707",  # Rainey/Cummings (also Rainey St @ Cummings)
    "3794": "3794",  # Dean Keeton & Speedway (also 4th/Sabine)
    "3838": "3838",  # 26th/Nueces (also Nueces & 26th, Bullock Museum)
    "3841": "3841",  # 23rd & Rio Grande (also State Capitol @ 14th & Colorado)
    "3455": "3455",  # Republic Square variants
    "3293": "3293",  # Barton Springs/Riverside (also East 2nd/Pedernales)
    "3621": "3621",  # 3rd/Nueces (also Nueces & 3rd, Nueces @ 3rd)
    "7189": "7189",  # 28th/Rio Grande (also 28th/Rio)
    "3294": "3294",  # 6th/Lavaca (also Lavaca & 6th, Guadalupe & 6th)
    "2563": "2563",  # Davis at Rainey Street (also Rainey/Davis, Rainey/Driskill)
    "4061": "4061",  # Lakeshore/Austin Hostel (also Lakeshore @ Austin Hostel)
    "3684": "3684",  # Cesar Chavez/Congress (also Congress & Cesar Chavez)
    "2569": "2569",  # East 11th/San Marcos (also East 11th St. & San Marcos)
    "2568": "2568",  # East 11th/Victory Grill (also East 11th St. at Victory Grill)
    "2823": "2823",  # East 5th/Broadway @ Capital Metro HQ (also Capital Metro HQ)
    "4057": "4057",  # 6th/Chalmers
}


# ── Dataset Kiosk Name → ID Mapping ─────────────────────────────────────────
# Maps the most popular dataset name variant for each kiosk ID to its ID.
# Used to resolve flow origin/destination names to kiosk locations.

DATASET_KIOSK_NAMES: dict[str, str] = {
    "2nd/Congress": "2494",
    "4th/Congress": "2495",
    "8th/Congress": "2496",
    "11th/Congress @ The Texas Capitol": "2497",
    "Dean Keeton/Speedway": "2498",
    "2nd/Lavaca @ City Hall": "2499",
    "Republic Square": "2500",
    "5th/Bowie": "2501",
    "Barton Springs & Riverside": "2502",
    "South Congress/James": "2503",
    "South Congress/Elizabeth": "2504",
    "Waller & 6th St.": "2536",
    "6th/West": "2537",
    "Bullock Museum @ Congress & MLK": "2538",
    "3rd/Trinity @ The Convention Center": "2539",
    "17th & Guadalupe": "2540",
    "State Capitol @ 14th & Colorado": "2541",
    "Plaza Saltillo": "2542",
    "East 6th/Pedernales": "2544",
    "ACC - Rio Grande & 12th": "2545",
    "ACC - West & 12th Street": "2546",
    "21st/Guadalupe": "2547",
    "Guadalupe/West Mall @ University Co-op": "2548",
    "South 1st/Riverside @ Long Center": "2549",
    "Republic Square @ Guadalupe & 4th St.": "2550",
    "3rd/West": "2552",
    "12th/San Jacinto @ State Capitol Visitors Garage": "2561",
    "8th/San Jacinto": "2562",
    "Davis at Rainey Street": "2563",
    "5th & San Marcos": "2564",
    "6th/Trinity": "2565",
    "Electric Drive/Sandra Muraida Way @ Pfluger Ped Bridge": "2566",
    "Barton Springs/Bouldin @ Palmer Auditorium": "2567",
    "East 11th/Victory Grill": "2568",
    "East 11th/San Marcos": "2569",
    "South Congress/Academy": "2570",
    "8th/Red River": "2571",
    "Barton Springs Pool": "2572",
    "Zilker Park": "2574",
    "Riverside/South Lamar": "2575",
    "Rainey @ River St": "2576",
    "Rainey/Cummings": "2707",
    "Barton Springs/Kinney": "2711",
    "Toomey Rd @ South Lamar": "2712",
    "East 6th/Robert T. Martinez": "2822",
    "East 5th/Broadway @ Capital Metro HQ": "2823",
    "11th & San Jacinto": "3291",
    "East 4th/Chicon": "3292",
    "Barton Springs/Riverside": "3293",
    "6th/Lavaca": "3294",
    "MoPac Pedestrian Bridge @ Veterans Drive": "3377",
    "East 7th & Pleasant Valley": "3381",
    "Brazos & 6th": "3390",
    "4th/Guadalupe @ Republic Square": "3455",
    "Republic Square @ 5th & Guadalupe": "3456",
    "Pease Park": "3464",
    "South Congress & Barton Springs at the Austin American-Statesman": "3513",
    "6th/Congress": "3619",
    "3rd/Nueces": "3621",
    "13th & San Antonio": "3635",
    "East 6th/Medina": "3660",
    "Cesar Chavez/Congress": "3684",
    "9th/Henderson": "3685",
    "Sterzing/Barton Springs": "3686",
    "Boardwalk West": "3687",
    "Lake Austin Blvd/Deep Eddy": "3790",
    "Lake Austin/Enfield": "3791",
    "22nd & Pearl": "3792",
    "Rio Grande & 28th": "3793",
    "Dean Keeton & Speedway": "3794",
    "Dean Keeton/Whitis": "3795",
    "21st/University": "3797",
    "21st/Speedway @ PCL": "3798",
    "23rd/San Jacinto @ DKR Stadium": "3799",
    "26th/Nueces": "3838",
    "23rd & Rio Grande": "3841",
    "8th/Lavaca": "4047",
    "South Congress @ Bouldin Creek": "4048",
    "5th/Campbell": "4050",
    "10th/Red River": "4051",
    "Rosewood/Angelina": "4052",
    "Rosewood/Chicon": "4054",
    "11th/Salina": "4055",
    "6th/Chalmers": "4057",
    "Hollow Creek/Barton Hills": "4058",
    "Nash Hernandez/East @ RBJ South": "4059",
    "Red River/Cesar Chavez @ The Fairmont": "4060",
    "Lakeshore/Austin Hostel": "4061",
    "Lakeshore/Pleasant Valley": "4062",
    "East 5th/Shady @ Eastside Bus Plaza": "4699",
    "16th/San Antonio": "4879",
    "22.5/Rio Grande": "4938",
    "23rd/Pearl": "7125",
    "13th/Trinity @ Waterloo Greenway": "7131",
    "One Texas Center": "7186",
    "South Congress/Mary": "7187",
    "22nd/Pearl": "7188",
    "28th/Rio Grande": "7189",
    "Rio Grande/12th": "7190",
    "Barton Springs/Riverside": "7253",
    "Dean Keeton/Park Place": "7341",
    "1701 Barton Springs Road": "7637",
    "Zilker Park West": "1006",
    "Lavaca & 6th": "1007",
    "Nueces @ 3rd": "1008",
}


def get_kiosk_locations() -> dict[str, tuple[float, float, str]]:
    """Return the kiosk location map."""
    return dict(KIOSK_LOCATIONS)


def resolve_kiosk_name_to_id(name: str) -> Optional[str]:
    """Resolve a dataset kiosk name to a kiosk ID using fuzzy matching.

    Tries exact match first, then checks if the name is a known variant
    by looking up common substrings.
    """
    if not name:
        return None

    name_clean = name.strip()

    # Exact match
    if name_clean in DATASET_KIOSK_NAMES:
        return DATASET_KIOSK_NAMES[name_clean]

    # Try normalized match
    norm = normalize_kiosk_name(name_clean)
    if norm in DATASET_KIOSK_NAMES:
        return DATASET_KIOSK_NAMES[norm]

    # Fuzzy: check if any dataset name contains this name or vice versa
    name_lower = name_clean.lower()
    for dname, kid in DATASET_KIOSK_NAMES.items():
        d_lower = dname.lower()
        if name_lower in d_lower or d_lower in name_lower:
            return kid

    return None


# ── Data Fetching Functions ──────────────────────────────────────────────────

def get_total_trips() -> dict:
    """Get total trip counts by year over the entire dataset history."""
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "year, count(trip_id) as trips",
        "$group": "year",
        "$order": "year ASC",
    }
    data = _make_request(url, params)

    yearly = {}
    total = 0
    for row in data:
        y = int(row.get("year", 0))
        c = int(row.get("trips", 0))
        yearly[y] = c
        total += c

    return {
        "total_trips": total,
        "by_year": yearly,
        "first_year": min(yearly.keys()) if yearly else None,
        "last_year": max(yearly.keys()) if yearly else None,
    }


def get_electric_vs_classic() -> dict:
    """Get electric vs classic bike usage, broken down by year."""
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "year, bike_type, count(trip_id) as trips",
        "$group": "year, bike_type",
        "$order": "year ASC",
    }
    data = _make_request(url, params)

    by_year: dict[int, dict[str, int]] = {}
    totals: dict[str, int] = {"classic": 0, "electric": 0}

    for row in data:
        y = int(row.get("year", 0))
        bt = (row.get("bike_type") or "").lower()
        c = int(row.get("trips", 0))
        if bt not in ("classic", "electric"):
            continue
        if y not in by_year:
            by_year[y] = {"classic": 0, "electric": 0}
        by_year[y][bt] = c
        totals[bt] += c

    return {
        "by_year": by_year,
        "totals": totals,
        "electric_pct": round(totals["electric"] / (totals["classic"] + totals["electric"]) * 100, 1) if (totals["classic"] + totals["electric"]) > 0 else 0,
    }


def get_kiosk_flow(top_n: int = 20) -> dict:
    """Get top kiosk-to-kiosk routes by trip count.

    Returns the most popular origin→destination pairs.
    Uses SoQL to query the dataset (limited to avoid timeout).
    """
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "checkout_kiosk, return_kiosk, count(trip_id) as trips",
        "$group": "checkout_kiosk, return_kiosk",
        "$order": "trips DESC",
        "$limit": top_n * 2,  # Fetch extra to allow for dedup
    }
    data = _make_request(url, params)

    flows = []
    seen_pairs: set[tuple[str, str]] = set()

    for row in data:
        origin = (row.get("checkout_kiosk") or "").strip()
        dest = (row.get("return_kiosk") or "").strip()
        count = int(row.get("trips", 0))

        # Skip same-station round trips and empty names
        if not origin or not dest or origin == dest:
            continue

        pair = (origin.lower(), dest.lower())
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        flows.append({
            "origin": origin,
            "destination": dest,
            "trips": count,
        })

        if len(flows) >= top_n:
            break

    return {
        "flows": flows,
        "total_routes": len(flows),
    }


def get_kiosk_evolution() -> dict:
    """Track kiosk lifecycle — which stations appeared and disappeared over time.

    Uses checkout_kiosk_id to identify unique stations.
    Determines first and last year each kiosk ID appears in the dataset.
    """
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "checkout_kiosk_id, min(year) as first_year, max(year) as last_year, count(trip_id) as trips",
        "$group": "checkout_kiosk_id",
        "$order": "first_year ASC",
        "$limit": 200,
    }
    data = _make_request(url, params)

    kiosks = []
    for row in data:
        kid = (row.get("checkout_kiosk_id") or "").strip()
        if not kid or kid == "#N/A" or kid == "nan":
            continue
        try:
            first = int(row.get("first_year", 0))
            last = int(row.get("last_year", 0))
            trips = int(row.get("trips", 0))
        except (ValueError, TypeError):
            continue

        location = KIOSK_LOCATIONS.get(kid)
        kiosks.append({
            "kiosk_id": kid,
            "name": location[2] if location else f"Kiosk {kid}",
            "lat": location[0] if location else None,
            "lng": location[1] if location else None,
            "first_year": first,
            "last_year": last,
            "total_trips": trips,
            "active": last == 2024,  # Still active if data goes to 2024
        })

    return {
        "kiosks": kiosks,
        "total_unique": len(kiosks),
        "active_kiosks": sum(1 for k in kiosks if k["active"]),
        "removed_kiosks": sum(1 for k in kiosks if not k["active"] and k["last_year"] < 2024),
    }


def get_membership_breakdown() -> dict:
    """Get membership type distribution over time."""
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "membership_type, count(trip_id) as trips",
        "$group": "membership_type",
        "$order": "trips DESC",
        "$limit": 50,
    }
    data = _make_request(url, params)

    membership = []
    total = 0
    for row in data:
        mt = (row.get("membership_type") or "Unknown").strip()
        c = int(row.get("trips", 0))
        if not mt or c == 0:
            continue
        membership.append({
            "type": mt,
            "trips": c,
        })
        total += c

    return {
        "membership_types": membership,
        "total_trips": total,
    }


def get_yearly_membership() -> dict:
    """Get membership type breakdown by year."""
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "year, membership_type, count(trip_id) as trips",
        "$group": "year, membership_type",
        "$order": "year ASC",
        "$limit": 500,
    }
    data = _make_request(url, params)

    by_year: dict[int, dict[str, int]] = {}
    for row in data:
        y = int(row.get("year", 0))
        mt = (row.get("membership_type") or "Unknown").strip()
        c = int(row.get("trips", 0))
        if not mt or c == 0:
            continue
        if y not in by_year:
            by_year[y] = {}
        by_year[y][mt] = by_year[y].get(mt, 0) + c

    return {"by_year": by_year}


def get_kiosk_yearly_trips(kiosk_id: str) -> dict:
    """Get yearly trip counts for a specific kiosk."""
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "year, count(trip_id) as trips",
        "$where": f"checkout_kiosk_id='{kiosk_id}'",
        "$group": "year",
        "$order": "year ASC",
    }
    data = _make_request(url, params)

    yearly = {}
    total = 0
    for row in data:
        y = int(row.get("year", 0))
        c = int(row.get("trips", 0))
        yearly[y] = c
        total += c

    return {
        "kiosk_id": kiosk_id,
        "by_year": yearly,
        "total_trips": total,
    }


def get_daily_trip_volume(days_back: int = 90) -> dict:
    """Get daily trip volume for the last N days.

    NOTE: Data ends June 30, 2024, so this will be sparse for recent dates.
    """
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
    params = {
        "$select": "checkout_date, count(trip_id) as trips",
        "$group": "checkout_date",
        "$order": "checkout_date DESC",
        "$limit": days_back,
    }
    data = _make_request(url, params)

    daily = []
    for row in data:
        date = (row.get("checkout_date") or "")[:10]
        c = int(row.get("trips", 0))
        if date:
            daily.append({"date": date, "trips": c})

    return {
        "daily": daily,
        "days": len(daily),
    }


# ── Kiosk Name Normalization ─────────────────────────────────────────────────

def normalize_kiosk_name(name: str) -> str:
    """Normalize kiosk name variants to a canonical form.

    Handles common inconsistencies:
    - '/' vs '&' vs 'and' separators
    - 'St.' vs 'Street' vs 'St'
    - Trailing/leading whitespace
    - '@' vs 'at' vs '&' variations
    """
    if not name:
        return ""

    name = name.strip()

    # Normalize separators
    name = re.sub(r'\s*/\s*', '/', name)
    name = re.sub(r'\s*&\s*', ' & ', name)
    name = re.sub(r'\s*@\s*', ' @ ', name)
    name = re.sub(r'\s+', ' ', name)

    # Normalize common suffixes
    name = re.sub(r'\bSt\.?\b', 'St', name)
    name = re.sub(r'\bBlvd\.?\b', 'Blvd', name)
    name = re.sub(r'\bAve\.?\b', 'Ave', name)

    return name.strip()


def find_kiosk_id_by_name(name: str) -> Optional[str]:
    """Find the kiosk ID for a given kiosk name (fuzzy match)."""
    if not name:
        return None

    name_lower = name.lower().strip()
    normalized = normalize_kiosk_name(name).lower()

    # Direct match in locations
    for kid, (lat, lng, canonical) in KIOSK_LOCATIONS.items():
        if canonical.lower() == name_lower or canonical.lower() == normalized:
            return kid

    # Search by partial match (name is a substring of canonical or vice versa)
    for kid, (lat, lng, canonical) in KIOSK_LOCATIONS.items():
        c_lower = canonical.lower()
        if (name_lower in c_lower or c_lower in name_lower or
                normalized in c_lower or c_lower in normalized):
            return kid

    return None