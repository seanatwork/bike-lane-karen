"""
Water Conservation Violations service module.

Queries Austin Open311 API for WWREPORT service code.
"""

from .water_conservation_bot import get_water_conservation_stats, format_water_conservation

__all__ = ["get_water_conservation_stats", "format_water_conservation"]
