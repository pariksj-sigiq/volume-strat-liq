"""Direct-token Upstox Websocket V3 client."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from src.ingest.feeds import MarketTick, decode_feed_message


LOGGER = logging.getLogger(__name__)
DEFAULT_WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"


@dataclass(slots=True)
class UpstoxWsConfig:
    """Runtime configuration for a direct-token Upstox market-data socket."""

    ws_url: str
    access_token: str = field(repr=False)
    mode: str = "full"
    reconnect_cap_sec: float = 30.0
    heartbeat_timeout_sec: float = 10.0
    tick_rate_log_interval_sec: float = 60.0

    @classmethod
    def from_env(cls) -> "UpstoxWsConfig":
        """Load websocket config from environment variables."""

        token = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
        if not token:
            raise ValueError("UPSTOX_ACCESS_TOKEN is required for the realtime terminal")
        return cls(
            ws_url=os.environ.get("UPSTOX_MARKET_DATA_WS_URL", DEFAULT_WS_URL).strip() or DEFAULT_WS_URL,
            access_token=token,
            mode=(os.environ.get("UPSTOX_MARKET_DATA_MODE", "full").strip() or "full").lower(),
            heartbeat_timeout_sec=float(os.environ.get("UPSTOX_WS_HEARTBEAT_TIMEOUT_SEC", "10")),
            tick_rate_log_interval_sec=float(os.environ.get("LIQ_SWEEP_TICK_RATE_LOG_SEC", "60")),
        )


def build_subscription_message(
    instrument_keys: list[str],
    *,
    mode: str,
    guid: str | None = None,
) -> bytes:
    """Build the binary JSON subscription payload expected by Upstox V3."""

    payload = {
        "guid": guid or uuid.uuid4().hex,
        "method": "sub",
        "data": {
            "mode": mode,
            "instrumentKeys": instrument_keys,
        },
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


TickCallback = Callable[[list[MarketTick]], Awaitable[None] | None]
StatusCallback = Callable[[str, dict[str, Any]], Awaitable[None] | None]


class UpstoxMarketDataClient:
    """Resilient Upstox market-data websocket client."""

    def __init__(
        self,
        config: UpstoxWsConfig,
        *,
        instrument_keys: list[str],
        symbol_by_instrument: dict[str, str],
        on_ticks: TickCallback,
        on_status: StatusCallback | None = None,
    ) -> None:
        if not instrument_keys:
            raise ValueError("at least one instrument key is required")
        self._config = config
        self._instrument_keys = instrument_keys
        self._symbol_by_instrument = symbol_by_instrument
        self._on_ticks = on_ticks
        self._on_status = on_status
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the reconnect loop to stop."""

        self._stopped.set()

    async def run_forever(self) -> None:
        """Connect, subscribe, decode, and reconnect with capped backoff."""

        attempt = 0
        while not self._stopped.is_set():
            try:
                await self._connect_once()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                delay = min(self._config.reconnect_cap_sec, (2 ** min(attempt, 5)) + random.random())
                await self._emit_status("reconnect_wait", {"error": str(exc), "delay_sec": round(delay, 2)})
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=delay)
                except TimeoutError:
                    continue

    async def _connect_once(self) -> None:
        import websockets

        headers = {
            "Authorization": f"Bearer {self._config.access_token}",
            "Accept": "*/*",
        }
        await self._emit_status("connecting", {"url": self._config.ws_url, "mode": self._config.mode})
        connect_kwargs = {
            "additional_headers": headers,
            "ping_interval": 20,
            "ping_timeout": 20,
            "max_size": 8 * 1024 * 1024,
        }
        try:
            websocket = await websockets.connect(self._config.ws_url, **connect_kwargs)
        except TypeError:
            connect_kwargs["extra_headers"] = connect_kwargs.pop("additional_headers")
            websocket = await websockets.connect(self._config.ws_url, **connect_kwargs)
        async with websocket:
            await websocket.send(
                build_subscription_message(self._instrument_keys, mode=self._config.mode)
            )
            await self._emit_status(
                "subscribed",
                {"mode": self._config.mode, "instrument_count": len(self._instrument_keys)},
            )
            ticks_this_window = 0
            window_started = time.monotonic()
            while not self._stopped.is_set():
                message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=self._config.heartbeat_timeout_sec,
                )
                ticks = decode_feed_message(message, symbol_by_instrument=self._symbol_by_instrument)
                if ticks:
                    ticks_this_window += len(ticks)
                    result = self._on_ticks(ticks)
                    if result is not None:
                        await result
                elapsed = time.monotonic() - window_started
                if elapsed >= self._config.tick_rate_log_interval_sec:
                    await self._emit_status(
                        "tick_rate",
                        {"ticks": ticks_this_window, "interval_sec": round(elapsed, 2)},
                    )
                    ticks_this_window = 0
                    window_started = time.monotonic()

    async def _emit_status(self, event: str, payload: dict[str, Any]) -> None:
        redacted = {key: value for key, value in payload.items() if key != "access_token"}
        LOGGER.info("upstox_ws_%s", event, extra=redacted)
        if self._on_status is None:
            return
        result = self._on_status(event, redacted)
        if result is not None:
            await result
