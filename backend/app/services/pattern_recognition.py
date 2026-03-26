"""Pattern recognition for 天干 (Heavenly Stems) candlestick patterns.

Classifies intraday OHLC key points into one of 10 天干 patterns based on
the time-ordering of Open, High, Low, Close events.

Input key_points format:
{
    "open":  {"time": "09:30", "price_pct": 0.0},
    "high":  {"time": "10:30", "price_pct": 3.5},
    "low":   {"time": "14:00", "price_pct": -1.2},
    "close": {"time": "15:00", "price_pct": 2.1}
}
"""

from typing import Dict, Any, Optional, List, Tuple


def _parse_time_minutes(time_str: str) -> int:
    parts = time_str.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def recognize_pattern(key_points: Dict[str, Any]) -> Optional[str]:
    required = {"open", "high", "low", "close"}
    if not required.issubset(key_points.keys()):
        return None

    try:
        events: List[Tuple[int, str]] = []
        for role in ["open", "high", "low", "close"]:
            t = _parse_time_minutes(key_points[role]["time"])
            events.append((t, role))
    except (KeyError, TypeError, ValueError):
        return None

    o_t = events[0][0]
    h_t = events[1][0]
    l_t = events[2][0]
    c_t = events[3][0]

    if o_t == l_t and h_t == c_t:
        return "己"
    if o_t == l_t:
        return "戊"
    if h_t == c_t and l_t < h_t:
        return "癸"
    if l_t == c_t and h_t < l_t:
        return "壬"

    sorted_events = sorted(events, key=lambda x: x[0])
    seq = [e[1] for e in sorted_events]

    if seq == ["open", "low", "high", "close"]:
        return "庚"
    if seq == ["open", "high", "low", "close"]:
        return "辛"

    if h_t < l_t and c_t > o_t:
        return "甲"
    if h_t > l_t and c_t < o_t:
        return "乙"

    mid_start = min(o_t, c_t)
    mid_end = max(o_t, c_t)

    if mid_start < h_t < mid_end:
        return "丙"
    if mid_start < l_t < mid_end:
        return "丁"

    if c_t >= o_t:
        return "甲"
    return "乙"
