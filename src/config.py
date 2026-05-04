"""Typed configuration loading for the realtime breakout scanner."""

from __future__ import annotations

from datetime import date, time
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveFloat,
    PositiveInt,
    field_validator,
    model_validator,
)


DEFAULT_STRATEGY_CONFIG = Path("config/strategy.yaml")


class ConfigError(RuntimeError):
    """Raised when a configuration file cannot be loaded at all."""


class StrictModel(BaseModel):
    """Base model that rejects unknown configuration keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionMode(StrEnum):
    """Supported executor modes."""

    PAPER = "paper"
    LIVE = "live"


class MetaConfig(StrictModel):
    """Strategy metadata used in logs and output artifacts."""

    version: PositiveInt
    name: str = Field(min_length=1)


class CapitalConfig(StrictModel):
    """Portfolio-level capital and exposure limits."""

    total_inr: PositiveFloat
    per_trade_risk_pct: PositiveFloat
    max_concurrent_positions: PositiveInt
    max_positions_per_sector: PositiveInt
    daily_loss_cutoff_pct: PositiveFloat
    daily_profit_lock_pct: PositiveFloat


class UniverseConfig(StrictModel):
    """Universe filters applied after the nightly universe build."""

    min_avg_turnover_cr: PositiveFloat
    min_price_inr: PositiveFloat
    exclude_tags: tuple[str, ...]


class TimeWindow(StrictModel):
    """Closed-open intraday time window in IST."""

    start: time
    end: time

    @model_validator(mode="after")
    def validate_order(self) -> "TimeWindow":
        """Reject windows that do not move forward through the trading day."""

        if self.end <= self.start:
            raise ValueError("skip window end must be after start")
        return self


class SessionConfig(StrictModel):
    """NSE session parameters and entry blackout windows."""

    market_open: time
    market_close: time
    skip_windows: tuple[TimeWindow, ...]
    square_off_time: time
    pre_market_warmup_min: PositiveInt

    @field_validator("skip_windows", mode="before")
    @classmethod
    def parse_skip_windows(cls, value: Any) -> Any:
        """Allow YAML skip windows to be written as compact two-item lists."""

        if isinstance(value, list):
            windows: list[dict[str, Any]] = []
            for item in value:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    windows.append({"start": item[0], "end": item[1]})
                else:
                    windows.append(item)
            return windows
        return value

    @model_validator(mode="after")
    def validate_session_order(self) -> "SessionConfig":
        """Ensure all configured session times are internally consistent."""

        if self.market_close <= self.market_open:
            raise ValueError("market_close must be after market_open")
        if not (self.market_open < self.square_off_time < self.market_close):
            raise ValueError("square_off_time must be inside the regular session")
        return self


class BarsConfig(StrictModel):
    """Bar interval and minimum history requirements."""

    interval: Literal["1min"]
    min_bars_for_signal: PositiveInt


class IndicatorsConfig(StrictModel):
    """Indicator periods used by both live and backtest engines."""

    breakout_lookback_bars: PositiveInt
    atr_period: PositiveInt
    vwap_reset: Literal["session"]


class TodBaselineConfig(StrictModel):
    """Time-of-day volume baseline controls."""

    source: Literal["rolling_sessions"]
    window_sessions: PositiveInt
    min_sessions_required: PositiveInt
    aggregation: Literal["median"]
    smoothing_minutes: PositiveInt
    holiday_handling: Literal["skip"]
    staleness_max_age_hours: PositiveInt

    @model_validator(mode="after")
    def validate_minimum_sessions(self) -> "TodBaselineConfig":
        """Require the readiness threshold to fit inside the history window."""

        if self.min_sessions_required > self.window_sessions:
            raise ValueError("min_sessions_required cannot exceed window_sessions")
        return self


class SignalRulesConfig(StrictModel):
    """Human-readable signal rules plus numeric thresholds for code paths."""

    breakout: str = Field(min_length=1)
    volume: str = Field(min_length=1)
    volume_mult: PositiveFloat
    trend: str = Field(min_length=1)
    range: str = Field(min_length=1)
    min_range_atr: PositiveFloat
    not_in_skip_window: bool
    not_at_circuit: bool
    circuit_limit_buffer_pct: PositiveFloat


class SignalProfileConfig(StrictModel):
    """Named scanner profile using the shared breakout signal logic."""

    name: str = Field(min_length=1, pattern=r"^[a-z0-9_]+$")
    label: str = Field(min_length=1)
    lookback_bars: PositiveInt
    volume_mult: PositiveFloat


class SignalConfig(StrictModel):
    """Primary signal configuration."""

    type: Literal["volume_breakout_long"]
    rules: SignalRulesConfig
    profiles: tuple[SignalProfileConfig, ...]
    cooldown_minutes_after_exit: PositiveInt
    one_signal_per_symbol_per_day: bool

    @field_validator("profiles")
    @classmethod
    def validate_profiles(cls, value: tuple[SignalProfileConfig, ...]) -> tuple[SignalProfileConfig, ...]:
        """Require unique named profiles."""

        if not value:
            raise ValueError("signal.profiles must contain at least one profile")
        names = [profile.name for profile in value]
        if len(set(names)) != len(names):
            raise ValueError("signal profile names must be unique")
        return value


class RiskConfig(StrictModel):
    """Stop, target, trailing, and concentration-risk configuration."""

    sl_method: Literal["max_of"]
    sl_components: tuple[str, ...]
    tp_r_multiple: PositiveFloat
    trail_after_r: PositiveFloat
    trail_atr_mult: PositiveFloat
    hard_time_stop_min: PositiveInt
    single_name_notional_cap_pct: PositiveFloat

    @field_validator("sl_components")
    @classmethod
    def validate_sl_components(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Require at least one explicit stop component."""

        if not value:
            raise ValueError("sl_components must contain at least one component")
        return value


class ExecutionConfig(StrictModel):
    """Execution controls shared by paper and live wiring."""

    mode: ExecutionMode
    order_type: Literal["LIMIT"]
    limit_offset_bps: PositiveFloat
    retry_on_partial: bool
    unfilled_cancel_after_sec: PositiveInt
    slippage_bps_paper: PositiveFloat
    live_requires_flag: bool
    live_confirmation_text: str = Field(min_length=1)
    duplicate_order_window_sec: PositiveInt

    @field_validator("order_type", mode="before")
    @classmethod
    def reject_market_orders(cls, value: Any) -> Any:
        """Reject MARKET orders before literal validation hides the intent."""

        if isinstance(value, str) and value.upper() == "MARKET":
            raise ValueError("MARKET orders are forbidden in v1")
        return value


class CostsConfig(StrictModel):
    """NSE intraday equity cost parameters."""

    brokerage_per_order_inr: PositiveFloat
    stt_sell_pct: PositiveFloat
    exchange_txn_pct: PositiveFloat
    sebi_pct: PositiveFloat
    stamp_duty_buy_pct: PositiveFloat
    gst_pct: PositiveFloat


class WalkForwardConfig(StrictModel):
    """Walk-forward optimization window sizes in months."""

    train_months: PositiveInt
    validate_months: PositiveInt
    step_months: PositiveInt


class BacktestConfig(StrictModel):
    """Historical replay and walk-forward configuration."""

    data_path: Path
    start_date: date
    end_date: date
    walk_forward: WalkForwardConfig
    initial_capital_inr: PositiveFloat
    reproduce_seed: int

    @model_validator(mode="after")
    def validate_dates(self) -> "BacktestConfig":
        """Reject empty or inverted backtest windows."""

        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self


class ObservabilityConfig(StrictModel):
    """Logging and metrics settings."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    metrics_port: PositiveInt
    tick_rate_log_interval_sec: PositiveInt
    production_json_logs: bool


class StrategyConfig(StrictModel):
    """Complete strategy configuration tree."""

    meta: MetaConfig
    capital: CapitalConfig
    universe: UniverseConfig
    session: SessionConfig
    bars: BarsConfig
    indicators: IndicatorsConfig
    tod_baseline: TodBaselineConfig
    signal: SignalConfig
    risk: RiskConfig
    execution: ExecutionConfig
    costs: CostsConfig
    backtest: BacktestConfig
    observability: ObservabilityConfig


def load_strategy_config(path: str | Path = DEFAULT_STRATEGY_CONFIG) -> StrategyConfig:
    """Load and validate a strategy YAML file."""

    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"strategy config does not exist: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"strategy config is not valid YAML: {config_path}") from exc
    if raw is None:
        raise ConfigError(f"strategy config is empty: {config_path}")
    if not isinstance(raw, dict):
        raise ConfigError(f"strategy config must be a mapping: {config_path}")
    return StrategyConfig.model_validate(raw)
