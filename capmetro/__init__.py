"""
CapMetro & MetroBike data module.

Queries Austin's Socrata OpenData portal for MetroBike trip data
(https://data.austintexas.gov/dataset/Austin-MetroBike-Trip-Data/tyfh-5r8s).

Provides aggregated analytics for the web map and static data generation.
"""

from .metrobike import (
    get_electric_vs_classic,
    get_total_trips,
    get_kiosk_flow,
    get_kiosk_evolution,
    get_membership_breakdown,
    get_kiosk_locations,
    KIOSK_LOCATIONS,
)

__all__ = [
    "get_electric_vs_classic",
    "get_total_trips",
    "get_kiosk_flow",
    "get_kiosk_evolution",
    "get_membership_breakdown",
    "get_kiosk_locations",
    "KIOSK_LOCATIONS",
]