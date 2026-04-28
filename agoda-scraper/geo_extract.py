"""
Quét JSON lồng nhau để tìm cặp vĩ độ/kinh độ (OTA hay đặt tên field khác nhau hoặc lồng sâu).
"""

from __future__ import annotations

import math

_LAT_KEYS = frozenset(
    {
        "lat",
        "latitude",
        "hotellat",
        "hotellatitude",
        "googlelatitude",
        "maplatitude",
        "maplat",
        "geolat",
        "bdlat",
        "gcj02lat",
        "wgslat",
        "coordlat",
        "centerlatitude",
    }
)
_LNG_KEYS = frozenset(
    {
        "lng",
        "lon",
        "longitude",
        "hotellng",
        "hotellongitude",
        "googlelongitude",
        "maplongitude",
        "maplng",
        "geolon",
        "geolng",
        "bdlng",
        "gcj02lng",
        "wgslng",
        "coordlng",
        "centerlongitude",
    }
)


def fmt_coord_value(v) -> str:
    if v is None:
        return ""
    try:
        x = float(v)
        if not math.isfinite(x):
            return ""
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s
    except (TypeError, ValueError):
        return ""


def _plausible_pair(lat: float, lng: float) -> bool:
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return False
    if abs(lat) < 1e-7 and abs(lng) < 1e-7:
        return False
    return True


def scan_json_for_latlng(obj, max_visits: int = 350) -> tuple[str, str]:
    """Duyệt dict/list, trả về cặp (vĩ độ, kinh độ) đầu tiên hợp lệ."""
    stack = [obj]
    visits = 0
    while stack and visits < max_visits:
        node = stack.pop()
        visits += 1
        if isinstance(node, dict):
            norm: dict[str, object] = {}
            for k, v in node.items():
                kn = str(k).lower().replace("_", "").replace("-", "")
                norm[kn] = v
            la = lo = None
            for lk in _LAT_KEYS:
                if lk in norm:
                    la = norm[lk]
                    break
            for gk in _LNG_KEYS:
                if gk in norm:
                    lo = norm[gk]
                    break
            if la is not None and lo is not None:
                s1, s2 = fmt_coord_value(la), fmt_coord_value(lo)
                if s1 and s2:
                    try:
                        if _plausible_pair(float(s1), float(s2)):
                            return s1, s2
                    except ValueError:
                        pass
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for it in node[:60]:
                if isinstance(it, (dict, list)):
                    stack.append(it)
    return "", ""
