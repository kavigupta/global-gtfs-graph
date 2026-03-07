"""
Geographic distance utilities.
"""

from __future__ import annotations

import numpy as np

# Earth mean radius in meters (WGS84)
EARTH_RADIUS_M = 6_371_009.0


def haversine_m(
    lat1: float | np.ndarray,
    lon1: float | np.ndarray,
    lat2: float | np.ndarray,
    lon2: float | np.ndarray,
    *,
    degrees: bool = True,
) -> float | np.ndarray:
    """
    Great-circle distance between point(s) (lat1, lon1) and (lat2, lon2).

    Args:
        lat1, lon1: First point(s); scalars or arrays of the same shape.
        lat2, lon2: Second point(s); scalars or arrays (broadcast with first).
        degrees: If True (default), coordinates are in degrees; if False, radians.

    Returns:
        Distance in meters; scalar or array matching the broadcast shape.
    """
    if degrees:
        lat1 = np.deg2rad(lat1)
        lon1 = np.deg2rad(lon1)
        lat2 = np.deg2rad(lat2)
        lon2 = np.deg2rad(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def haversine_km(
    lat1: float | np.ndarray,
    lon1: float | np.ndarray,
    lat2: float | np.ndarray,
    lon2: float | np.ndarray,
    *,
    degrees: bool = True,
) -> float | np.ndarray:
    """Great-circle distance in kilometers. See haversine_m for arguments."""
    return haversine_m(lat1, lon1, lat2, lon2, degrees=degrees) / 1000.0
