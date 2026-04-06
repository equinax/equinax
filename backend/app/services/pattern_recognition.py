"""Pattern recognition for 天干 (Heavenly Stems) candlestick patterns.

Classifies intraday OHLC key points into one of 10 天干 patterns based on
two dimensions:
  1. Time coincidences: whether O/C share time with H/L
  2. For non-degenerate cases: H-before-L vs L-before-H × close-up vs close-down

Decision tree:
  Stage 1 — time coincidences (priority order):
    甲: O_time == L_time AND C_time == H_time  (pure bull: low@open, high@close)
    乙: O_time == H_time AND C_time == L_time  (pure bear: high@open, low@close)
    丙: O_time == L_time                        (open = low, main rise)
    丁: O_time == H_time                        (open = high, main drop)
    戊: C_time == H_time                        (close = high, valley recovery)
    己: C_time == L_time                        (close = low, rise then crash)

  Stage 2 — general case (H/L order × close direction):
    庚: H before L, close >= open  (先升后降, 收盘涨)
    癸: H before L, close <  open  (先升后降, 收盘跌)
    壬: L before H, close >= open  (先降后升, 收盘涨)
    辛: L before H, close <  open  (先降后升, 收盘跌)

Input key_points format:
{
    "open":  {"time": "09:30", "price_pct": 0.0},
    "high":  {"time": "10:30", "price_pct": 3.5},
    "low":   {"time": "14:00", "price_pct": -1.2},
    "close": {"time": "15:00", "price_pct": 2.1}
}
"""

from typing import Dict, Any, Optional


def _parse_time_minutes(time_str: str) -> int:
    parts = time_str.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def recognize_pattern(
    key_points: Dict[str, Any],
    real_open: Optional[float] = None,
    real_close: Optional[float] = None,
) -> Optional[str]:
    required = {"open", "high", "low", "close"}
    if not required.issubset(key_points.keys()):
        return None

    try:
        o_t = _parse_time_minutes(key_points["open"]["time"])
        h_t = _parse_time_minutes(key_points["high"]["time"])
        l_t = _parse_time_minutes(key_points["low"]["time"])
        c_t = _parse_time_minutes(key_points["close"]["time"])
    except (KeyError, TypeError, ValueError):
        return None

    # Stage 1: time coincidences (check compound cases first)
    o_is_l = o_t == l_t
    o_is_h = o_t == h_t
    c_is_h = c_t == h_t
    c_is_l = c_t == l_t

    if o_is_l and c_is_h:
        return "甲"  # pure bull: low at open, high at close
    if o_is_h and c_is_l:
        return "乙"  # pure bear: high at open, low at close
    if o_is_l:
        return "丙"  # open = low, main rise pattern
    if o_is_h:
        return "丁"  # open = high, main drop pattern
    if c_is_h:
        return "戊"  # close = high, valley recovery
    if c_is_l:
        return "己"  # close = low, rise then crash

    # Stage 2: general case — H/L time order × close vs open price
    # Use real OHLC prices when available (key_points price is just canvas position)
    if real_open is not None and real_close is not None:
        close_up = real_close >= real_open
    else:
        o_p = float(key_points["open"].get("price_pct", key_points["open"].get("price", 0)))
        c_p = float(key_points["close"].get("price_pct", key_points["close"].get("price", 0)))
        close_up = c_p >= o_p

    h_before_l = h_t < l_t

    if h_before_l:
        return "庚" if close_up else "癸"
    else:
        return "壬" if close_up else "辛"
