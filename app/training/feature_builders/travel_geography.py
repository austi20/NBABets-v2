"""Arena coordinate and travel-distance helpers for feature construction."""

from __future__ import annotations

import numpy as np
import pandas as pd

# NBA home-arena coordinates (degrees). Used only for travel-distance features (B2).
TEAM_HOME_ARENA_COORDS: dict[str, tuple[float, float]] = {
    "ATL": (33.7573, -84.3963),
    "BOS": (42.3662, -71.0621),
    "BKN": (40.6826, -73.9750),
    "CHA": (35.2251, -80.8391),
    "CHI": (41.8807, -87.6742),
    "CLE": (41.4965, -81.6882),
    "DAL": (32.7905, -96.8103),
    "DEN": (39.7487, -105.0077),
    "DET": (42.6970, -83.2456),
    "GSW": (37.7680, -122.3877),
    "HOU": (29.7508, -95.3621),
    "IND": (39.7639, -86.1555),
    "LAC": (34.0430, -118.2673),
    "LAL": (34.0430, -118.2673),
    "MEM": (35.1382, -90.0506),
    "MIA": (25.7814, -80.1881),
    "MIL": (43.0451, -87.9172),
    "MIN": (44.9795, -93.2761),
    "NOP": (29.9490, -90.0821),
    "NYK": (40.7505, -73.9934),
    "OKC": (35.4634, -97.5151),
    "ORL": (28.5392, -81.3839),
    "PHI": (39.9012, -75.1720),
    "PHX": (33.4457, -112.0712),
    "POR": (45.5316, -122.6668),
    "SAC": (38.5804, -121.4996),
    "SAS": (29.4270, -98.4375),
    "TOR": (43.6435, -79.3791),
    "UTA": (40.7683, -111.9011),
    "WAS": (38.8981, -77.0209),
}
for _alias, _ref in (("BRK", "BKN"), ("NO", "NOP")):
    TEAM_HOME_ARENA_COORDS[_alias] = TEAM_HOME_ARENA_COORDS[_ref]
_TEAM_HOME_LAT = {k: v[0] for k, v in TEAM_HOME_ARENA_COORDS.items()}
_TEAM_HOME_LON = {k: v[1] for k, v in TEAM_HOME_ARENA_COORDS.items()}


def _arena_coords_series(abbr: pd.Series) -> tuple[pd.Series, pd.Series]:
    up = abbr.fillna("").astype(str).str.strip().str.upper()
    lat = up.map(_TEAM_HOME_LAT).astype(float)
    lon = up.map(_TEAM_HOME_LON).astype(float)
    return lat, lon


def _haversine_km_series(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    earth_km = 6371.0
    lat1r = np.radians(lat1.astype(float))
    lat2r = np.radians(lat2.astype(float))
    dlat = np.radians((lat2 - lat1).astype(float))
    dlon = np.radians((lon2 - lon1).astype(float))
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    km = pd.Series(earth_km * c, index=lat1.index, dtype=float)
    invalid = lat1.isna() | lon1.isna() | lat2.isna() | lon2.isna()
    km = km.mask(invalid, 0.0)
    return km.fillna(0.0)

