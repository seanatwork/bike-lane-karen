"""
Child Care Licensing service module.

Queries HHSC Child Care Licensing datasets on data.texas.gov:
  bc5r-88dy — facility master
  tqgd-mf4x — non-compliance detail
"""

from .childcare_bot import get_childcare_stats, format_childcare

__all__ = ["get_childcare_stats", "format_childcare"]
