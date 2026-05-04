"""Upstox REST client for historical data and instrument metadata."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.signals.base import Bar


IST = ZoneInfo("Asia/Kolkata")
DEFAULT_REST_URL = "https://api.upstox.com/v3/historical-candle"
HistoricalInterval = Literal["1minute"]
GetBytes = Callable[[str, dict[str, str], float], bytes]


def _default_get_bytes(url: str, headers: dict[str, str], timeout: float) -> bytes:
    request = Request(url, headers=headers, method="GET")
    for attempt in range(4):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code == 401:
                raise PermissionError("Upstox REST returned 401; refresh UPSTOX_ACCESS_TOKEN") from exc
            if exc.code != 429 or attempt == 3:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            delay = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else 1.0 + attempt
            time.sleep(delay)
    raise RuntimeError("unreachable Upstox retry state")


@dataclass(slots=True)
class UpstoxRestClient:
    """Small direct-token REST client for Upstox historical candles."""

    access_token: str = field(repr=False)
    base_url: str = DEFAULT_REST_URL
    timeout_sec: float = 10.0
    get_bytes: GetBytes = field(default=_default_get_bytes, repr=False)

    @classmethod
    def from_env(cls) -> "UpstoxRestClient":
        """Build a REST client from environment variables."""

        token = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
        if not token:
            raise ValueError("UPSTOX_ACCESS_TOKEN is required for Upstox REST warmup")
        return cls(
            access_token=token,
            base_url=os.environ.get("UPSTOX_REST_URL", DEFAULT_REST_URL).rstrip("/") or DEFAULT_REST_URL,
        )

    def fetch_historical_candles(
        self,
        *,
        instrument_key: str,
        interval: HistoricalInterval,
        from_date: date,
        to_date: date,
        symbol: str,
    ) -> list[Bar]:
        """Fetch historical candles for one instrument and return sorted bars."""

        encoded_key = quote(instrument_key, safe="")
        interval_path = "minutes/1" if interval == "1minute" else interval
        url = f"{self.base_url}/{encoded_key}/{interval_path}/{to_date.isoformat()}/{from_date.isoformat()}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "Mozilla/5.0 liq-sweep/0.1",
        }
        raw = self.get_bytes(url, headers, self.timeout_sec)
        return parse_historical_candles(
            raw,
            symbol=symbol,
            instrument_key=instrument_key,
            source="upstox_rest_warmup",
        )


def parse_historical_candles(
    raw: bytes,
    *,
    symbol: str,
    instrument_key: str,
    source: str,
) -> list[Bar]:
    """Parse an Upstox historical-candle response into sorted bar DTOs."""

    payload = json.loads(raw.decode("utf-8"))
    candles = (payload.get("data") or {}).get("candles") or []
    bars: list[Bar] = []
    for candle in candles:
        if not isinstance(candle, list | tuple) or len(candle) < 6:
            continue
        ts = _parse_candle_ts(candle[0])
        bars.append(
            Bar(
                symbol=symbol.upper(),
                ts=ts,
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
                instrument_key=instrument_key,
                open_interest=float(candle[6]) if len(candle) > 6 and candle[6] is not None else None,
                source=source,
            )
        )
    return sorted(bars, key=lambda item: item.ts)


def _parse_candle_ts(value: object) -> datetime:
    timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=IST)
    return timestamp.astimezone(IST)
