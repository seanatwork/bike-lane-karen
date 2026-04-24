#!/usr/bin/env python3
"""
Generate static Folium maps for 311 service categories.

Run locally:
    python scripts/generate_map.py bicycle
    python scripts/generate_map.py graffiti
    python scripts/generate_map.py homeless

Run in CI (GitHub Actions) with AUSTIN_APP_TOKEN set for higher rate limits.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()


def generate_bicycle_map(days_back: int = 90) -> tuple:
    """Generate bicycle infrastructure map."""
    from bicycle.bicycle_bot import generate_bicycle_map
    return generate_bicycle_map(days_back)


def generate_graffiti_map(days_back: int = 90) -> tuple:
    """Generate graffiti abatement map."""
    from graffiti.graffiti_bot import generate_graffiti_map
    return generate_graffiti_map(days_back)


def generate_homeless_map(days_back: int = 90) -> tuple:
    """Generate homeless encampment map."""
    from homeless.homeless_bot import generate_encampment_map
    return generate_encampment_map(days_back)


def generate_traffic_map(days_back: int = 90) -> tuple:
    """Generate traffic & infrastructure map."""
    from infrastructureandtransportation.traffic_bot import generate_traffic_map
    return generate_traffic_map(days_back)


def generate_parking_map(days_back: int = 90) -> tuple:
    """Generate parking enforcement map."""
    from parking.parking_bot import generate_parking_map
    return generate_parking_map(days_back)


def generate_parking_trends_page(days_back: int = 365) -> tuple:
    """Generate parking complaints trends page."""
    from parking.trends import generate_parking_trends
    return generate_parking_trends(days_back)


def generate_graffiti_trends_page(days_back: int = 365) -> tuple:
    """Generate graffiti abatement trends page."""
    from graffiti.trends import generate_graffiti_trends
    return generate_graffiti_trends(days_back)


def generate_crime_trends_page(days_back: int = 365) -> tuple:
    """Generate APD crime trends page."""
    from crime.trends import generate_crime_trends
    return generate_crime_trends(days_back)


def generate_noise_trends_page(days_back: int = 365) -> tuple:
    """Generate noise complaint trends page."""
    from noisecomplaints.trends import generate_noise_trends
    return generate_noise_trends(days_back)


def generate_crime_map(days_back: int = 90) -> tuple:
    """Generate APD crime choropleth map by council district."""
    from crime.crime_map import generate_crime_map
    return generate_crime_map(days_back)


def generate_noise_map(days_back: int = 90) -> tuple:
    """Generate noise complaints point map."""
    from noisecomplaints.noise_bot import generate_noise_map
    return generate_noise_map(days_back)


def generate_parks_map(days_back: int = 90) -> tuple:
    """Generate park maintenance point map."""
    from parks.parks_bot import generate_parks_map
    return generate_parks_map(days_back)


def generate_water_map(days_back: int = 90) -> tuple:
    """Generate water conservation violations point map."""
    from waterconservation.water_conservation_bot import generate_water_map
    return generate_water_map(days_back)


def generate_childcare_map(days_back: int = 90) -> tuple:
    """Generate childcare facility compliance map."""
    from childcare.childcare_bot import generate_childcare_map
    return generate_childcare_map(days_back)


def generate_animal_map(days_back: int = 90) -> tuple:
    """Generate animal services point map."""
    from animalsvc.animal_bot import generate_animal_map
    return generate_animal_map(days_back)


CATEGORY_MAPS = {
    "bicycle": (generate_bicycle_map, "bicycle/index.html"),
    "graffiti": (generate_graffiti_map, "graffiti/index.html"),
    "homeless": (generate_homeless_map, "homeless/index.html"),
    "traffic": (generate_traffic_map, "traffic/index.html"),
    "parking": (generate_parking_map, "parking/index.html"),
    "parking-trends": (generate_parking_trends_page, "parking/trends/index.html"),
    "crime": (generate_crime_map, "crime/index.html"),
    "crime-trends": (generate_crime_trends_page, "crime/trends/index.html"),
    "noise": (generate_noise_map, "noise/index.html"),
    "noise-trends": (generate_noise_trends_page, "noise/trends/index.html"),
    "graffiti-trends": (generate_graffiti_trends_page, "graffiti/trends/index.html"),
    "parks": (generate_parks_map, "parks/index.html"),
    "water": (generate_water_map, "water/index.html"),
    "childcare": (generate_childcare_map, "childcare/index.html"),
    "animal": (generate_animal_map, "animal/index.html"),
}


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/generate_map.py <category>")
        print(f"Available categories: {', '.join(CATEGORY_MAPS.keys())}")
        sys.exit(1)

    category = sys.argv[1].lower()
    if category not in CATEGORY_MAPS:
        print(f"Unknown category: {category}")
        print(f"Available categories: {', '.join(CATEGORY_MAPS.keys())}")
        sys.exit(1)

    generator_func, output_path = CATEGORY_MAPS[category]
    if category == "traffic":
        days_back = 30
    elif category.endswith("-trends"):
        days_back = 365
    else:
        days_back = 90

    print(f"Generating {category} map (last {days_back} days)...")
    buf, summary = generator_func(days_back)

    if not buf:
        print(f"Map generation failed: {summary}")
        sys.exit(1)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    last_ran_span = (
        f'<span style="font-size: 11px; color: #888;">Last ran: {now_str}</span><br/>\n        '
    )
    html = buf.getvalue().decode("utf-8").replace(
        '<span id="map-summary"',
        last_ran_span + '<span id="map-summary"',
        1,
    )

    out = Path("docs") / output_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"Written {out.stat().st_size:,} bytes to {out}")
    print(summary)


if __name__ == "__main__":
    main()
