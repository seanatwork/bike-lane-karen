"""
Animal Services module.

Queries Austin Open311 API for animal-related service codes.
"""

from .animal_bot import (
    get_hotspots,
    get_stats,
    get_response_times,
    format_hotspots,
    format_stats,
    format_response_times,
)

from .coyote_bot import (
    get_seasonal_patterns,
    get_hotspots as get_coyote_hotspots,
    get_coyote_overview,
    format_seasonal_patterns,
    format_hotspots as format_coyote_hotspots,
    format_overview as format_coyote_overview,
)

__all__ = [
    "get_hotspots",
    "get_stats",
    "get_response_times",
    "format_hotspots",
    "format_stats",
    "format_response_times",
    # Coyote complaints
    "get_seasonal_patterns",
    "get_coyote_hotspots",
    "get_coyote_overview",
    "format_seasonal_patterns",
    "format_coyote_hotspots",
    "format_coyote_overview",
]
