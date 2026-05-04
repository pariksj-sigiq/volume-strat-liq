"""Upstox Websocket V3 feed normalization."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import struct
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True, frozen=True)
class MinuteOHLC:
    """One OHLC bucket embedded inside an Upstox full-feed message."""

    interval: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True, frozen=True)
class MarketTick:
    """Normalized market tick used by the live scanner."""

    instrument_key: str
    symbol: str
    ts: datetime
    ltp: float
    close_price: float | None = None
    last_quantity: int | None = None
    volume_traded_today: int | None = None
    open_interest: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    minute_ohlc: MinuteOHLC | None = None
    raw_mode: str | None = None

    def with_volume_traded_today(self, value: int) -> "MarketTick":
        """Return a copy with a changed cumulative volume."""

        return replace(self, volume_traded_today=value)

    def with_price(
        self,
        value: float,
        *,
        ts: datetime,
        volume_traded_today: int | None = None,
    ) -> "MarketTick":
        """Return a copy with changed price, timestamp, and optional volume."""

        return replace(
            self,
            ltp=value,
            ts=ts,
            volume_traded_today=volume_traded_today,
        )


def feed_dict_to_ticks(
    payload: dict[str, Any],
    *,
    symbol_by_instrument: dict[str, str],
) -> list[MarketTick]:
    """Convert a decoded Upstox feed dictionary into normalized ticks."""

    if payload.get("type") == "market_info":
        return []
    feeds = payload.get("feeds") or {}
    if not isinstance(feeds, dict):
        return []
    current_ts = _parse_epoch_millis(payload.get("currentTs"))
    ticks: list[MarketTick] = []
    for instrument_key, feed in feeds.items():
        if not isinstance(feed, dict):
            continue
        extracted = _extract_feed(feed)
        ltpc = extracted.get("ltpc")
        if not isinstance(ltpc, dict):
            continue
        ltp = _float_or_none(ltpc.get("ltp"))
        if ltp is None:
            continue
        ts = _parse_epoch_millis(ltpc.get("ltt")) or current_ts
        if ts is None:
            ts = datetime.now(tz=IST)
        tick = MarketTick(
            instrument_key=str(instrument_key),
            symbol=symbol_by_instrument.get(str(instrument_key), str(instrument_key)),
            ts=ts,
            ltp=ltp,
            close_price=_float_or_none(ltpc.get("cp")),
            last_quantity=_int_or_none(ltpc.get("ltq")),
            volume_traded_today=_int_or_none(extracted.get("vtt")),
            open_interest=_float_or_none(extracted.get("oi")),
            best_bid=_best_depth_value(extracted.get("depth"), "bidP"),
            best_ask=_best_depth_value(extracted.get("depth"), "askP"),
            minute_ohlc=_extract_minute_ohlc(extracted.get("ohlc")),
            raw_mode=extracted.get("mode"),
        )
        ticks.append(tick)
    return ticks


def decode_feed_message(
    message: bytes | bytearray | str | dict[str, Any],
    *,
    symbol_by_instrument: dict[str, str],
) -> list[MarketTick]:
    """Decode a websocket message into normalized ticks.

    Upstox V3 sends protobuf frames. JSON support is kept for fixture replay and
    local diagnostics.
    """

    if isinstance(message, dict):
        return feed_dict_to_ticks(message, symbol_by_instrument=symbol_by_instrument)
    if isinstance(message, str):
        return feed_dict_to_ticks(json.loads(message), symbol_by_instrument=symbol_by_instrument)
    raw = bytes(message)
    stripped = raw.lstrip()
    if stripped.startswith(b"{"):
        return feed_dict_to_ticks(json.loads(stripped.decode("utf-8")), symbol_by_instrument=symbol_by_instrument)
    decoded = _decode_feed_response(raw)
    return feed_dict_to_ticks(decoded, symbol_by_instrument=symbol_by_instrument)


def _extract_feed(feed: dict[str, Any]) -> dict[str, Any]:
    if isinstance(feed.get("ltpc"), dict):
        return {"ltpc": feed["ltpc"], "mode": "ltpc"}
    first = feed.get("firstLevelWithGreeks")
    if isinstance(first, dict):
        return {
            "ltpc": first.get("ltpc"),
            "depth": [first.get("firstDepth")],
            "vtt": first.get("vtt"),
            "oi": first.get("oi"),
            "mode": "firstLevelWithGreeks",
        }
    full = feed.get("fullFeed")
    if isinstance(full, dict):
        market = full.get("marketFF") or full.get("indexFF") or {}
        depth = (market.get("marketLevel") or {}).get("bidAskQuote")
        ohlc = (market.get("marketOHLC") or {}).get("ohlc")
        return {
            "ltpc": market.get("ltpc"),
            "depth": depth,
            "ohlc": ohlc,
            "vtt": market.get("vtt"),
            "oi": market.get("oi"),
            "mode": "fullFeed",
        }
    return {}


def _decode_feed_response(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    response_type = _enum_name(_first_varint(fields.get(1)), {0: "initial_feed", 1: "live_feed", 2: "market_info"})
    payload: dict[str, Any] = {
        "type": response_type,
        "feeds": {},
        "currentTs": str(_first_varint(fields.get(3)) or 0),
    }
    for item in fields.get(2, []):
        if not isinstance(item, bytes):
            continue
        entry = _read_message(item)
        key = _first_string(entry.get(1))
        value = _first_bytes(entry.get(2))
        if key and value:
            payload["feeds"][key] = _decode_feed(value)
    return payload


def _decode_feed(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    ltpc = _first_bytes(fields.get(1))
    if ltpc:
        return {"ltpc": _decode_ltpc(ltpc)}
    full = _first_bytes(fields.get(2))
    if full:
        return {"fullFeed": _decode_full_feed(full)}
    first = _first_bytes(fields.get(3))
    if first:
        return {"firstLevelWithGreeks": _decode_first_level(first)}
    return {}


def _decode_ltpc(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {
        "ltp": _first_double(fields.get(1)),
        "ltt": str(_first_varint(fields.get(2)) or 0),
        "ltq": str(_first_varint(fields.get(3)) or 0),
        "cp": _first_double(fields.get(4)),
    }


def _decode_full_feed(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    market = _first_bytes(fields.get(1))
    if market:
        return {"marketFF": _decode_market_full_feed(market)}
    index = _first_bytes(fields.get(2))
    if index:
        return {"indexFF": _decode_index_full_feed(index)}
    return {}


def _decode_market_full_feed(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    level = _first_bytes(fields.get(2))
    ohlc = _first_bytes(fields.get(4))
    return {
        "ltpc": _decode_ltpc(_first_bytes(fields.get(1)) or b""),
        "marketLevel": _decode_market_level(level or b""),
        "marketOHLC": _decode_market_ohlc(ohlc or b""),
        "atp": _first_double(fields.get(5)),
        "vtt": str(_first_varint(fields.get(6)) or 0),
        "oi": _first_double(fields.get(7)),
        "iv": _first_double(fields.get(8)),
        "tbq": _first_double(fields.get(9)),
        "tsq": _first_double(fields.get(10)),
    }


def _decode_index_full_feed(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {
        "ltpc": _decode_ltpc(_first_bytes(fields.get(1)) or b""),
        "marketOHLC": _decode_market_ohlc(_first_bytes(fields.get(2)) or b""),
    }


def _decode_first_level(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {
        "ltpc": _decode_ltpc(_first_bytes(fields.get(1)) or b""),
        "firstDepth": _decode_quote(_first_bytes(fields.get(2)) or b""),
        "vtt": str(_first_varint(fields.get(4)) or 0),
        "oi": _first_double(fields.get(5)),
        "iv": _first_double(fields.get(6)),
    }


def _decode_market_level(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {"bidAskQuote": [_decode_quote(item) for item in fields.get(1, []) if isinstance(item, bytes)]}


def _decode_quote(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {
        "bidQ": str(_first_varint(fields.get(1)) or 0),
        "bidP": _first_double(fields.get(2)),
        "askQ": str(_first_varint(fields.get(3)) or 0),
        "askP": _first_double(fields.get(4)),
    }


def _decode_market_ohlc(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {"ohlc": [_decode_ohlc(item) for item in fields.get(1, []) if isinstance(item, bytes)]}


def _decode_ohlc(raw: bytes) -> dict[str, Any]:
    fields = _read_message(raw)
    return {
        "interval": _first_string(fields.get(1)) or "",
        "open": _first_double(fields.get(2)),
        "high": _first_double(fields.get(3)),
        "low": _first_double(fields.get(4)),
        "close": _first_double(fields.get(5)),
        "vol": str(_first_varint(fields.get(6)) or 0),
        "ts": str(_first_varint(fields.get(7)) or 0),
    }


def _read_message(raw: bytes) -> dict[int, list[Any]]:
    fields: dict[int, list[Any]] = {}
    offset = 0
    size = len(raw)
    while offset < size:
        key, offset = _read_varint(raw, offset)
        field_no = key >> 3
        wire_type = key & 0x07
        if wire_type == 0:
            value, offset = _read_varint(raw, offset)
        elif wire_type == 1:
            value = struct.unpack("<d", raw[offset : offset + 8])[0]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(raw, offset)
            value = raw[offset : offset + length]
            offset += length
        elif wire_type == 5:
            value = struct.unpack("<f", raw[offset : offset + 4])[0]
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type: {wire_type}")
        fields.setdefault(field_no, []).append(value)
    return fields


def _read_varint(raw: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        if offset >= len(raw):
            raise ValueError("truncated protobuf varint")
        byte = raw[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7
        if shift > 70:
            raise ValueError("protobuf varint too long")


def _first_varint(values: list[Any] | None) -> int | None:
    value = values[0] if values else None
    return int(value) if isinstance(value, int) else None


def _first_double(values: list[Any] | None) -> float:
    value = values[0] if values else None
    return float(value) if isinstance(value, float | int) else 0.0


def _first_bytes(values: list[Any] | None) -> bytes | None:
    value = values[0] if values else None
    return value if isinstance(value, bytes) else None


def _first_string(values: list[Any] | None) -> str | None:
    value = _first_bytes(values)
    if value is None:
        return None
    return value.decode("utf-8")


def _enum_name(value: int | None, names: dict[int, str]) -> str:
    return names.get(value or 0, str(value or 0))


def _extract_minute_ohlc(values: Any) -> MinuteOHLC | None:
    if not isinstance(values, list):
        return None
    for row in values:
        if not isinstance(row, dict) or row.get("interval") != "I1":
            continue
        ts = _parse_epoch_millis(row.get("ts"))
        if ts is None:
            return None
        return MinuteOHLC(
            interval="I1",
            ts=ts,
            open=float(row.get("open") or 0),
            high=float(row.get("high") or 0),
            low=float(row.get("low") or 0),
            close=float(row.get("close") or 0),
            volume=float(row.get("vol") or 0),
        )
    return None


def _best_depth_value(depth: Any, key: str) -> float | None:
    if not isinstance(depth, list) or not depth or not isinstance(depth[0], dict):
        return None
    return _float_or_none(depth[0].get(key))


def _parse_epoch_millis(value: Any) -> datetime | None:
    millis = _int_or_none(value)
    if millis is None or millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc).astimezone(IST)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
