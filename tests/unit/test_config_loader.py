from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config import ConfigError, ExecutionMode, StrategyConfig, load_strategy_config


ROOT = Path(__file__).resolve().parents[2]


def test_loads_strategy_yaml_with_explicit_thresholds() -> None:
    config = load_strategy_config(ROOT / "config" / "strategy.yaml")

    assert isinstance(config, StrategyConfig)
    assert config.meta.version == 1
    assert config.meta.name == "vol_breakout_v1"
    assert config.capital.total_inr == 1_000_000
    assert config.capital.per_trade_risk_pct == 0.5
    assert config.session.market_open.hour == 9
    assert config.session.market_open.minute == 15
    assert config.session.square_off_time.hour == 15
    assert config.session.skip_windows[0].start.minute == 15
    assert config.session.skip_windows[0].end.minute == 20
    assert config.tod_baseline.window_sessions == 20
    assert config.tod_baseline.min_sessions_required == 10
    assert config.signal.rules.volume_mult == 2.5
    assert [profile.name for profile in config.signal.profiles] == ["slow", "fast"]
    assert config.signal.profiles[0].lookback_bars == 20
    assert config.signal.profiles[0].volume_mult == 2.5
    assert config.signal.profiles[1].lookback_bars == 5
    assert config.signal.profiles[1].volume_mult == 4.0
    assert config.signal.cooldown_minutes_after_exit == 30
    assert config.execution.mode is ExecutionMode.PAPER
    assert config.execution.order_type == "LIMIT"
    assert config.execution.live_confirmation_text == "I UNDERSTAND"
    assert config.costs.stt_sell_pct == 0.025
    assert config.backtest.walk_forward.train_months == 9


def test_missing_required_values_raise_instead_of_defaulting(tmp_path: Path) -> None:
    config_path = tmp_path / "strategy.yaml"
    config_path.write_text(
        """
meta:
  version: 1
  name: "broken"
""",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError) as error:
        load_strategy_config(config_path)

    missing_paths = {".".join(str(part) for part in item["loc"]) for item in error.value.errors()}
    assert "capital" in missing_paths
    assert "session" in missing_paths
    assert "execution" in missing_paths


def test_rejects_unknown_keys_and_market_orders(tmp_path: Path) -> None:
    raw = (ROOT / "config" / "strategy.yaml").read_text(encoding="utf-8")
    config_path = tmp_path / "strategy.yaml"
    config_path.write_text(raw.replace('order_type: "LIMIT"', 'order_type: "MARKET"'), encoding="utf-8")

    with pytest.raises(ValidationError, match="MARKET orders are forbidden"):
        load_strategy_config(config_path)

    unknown_path = tmp_path / "unknown.yaml"
    unknown_path.write_text(raw + "\nunexpected: true\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_strategy_config(unknown_path)


def test_loader_fails_loudly_for_missing_or_empty_files(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="does not exist"):
        load_strategy_config(tmp_path / "missing.yaml")

    empty_path = tmp_path / "empty.yaml"
    empty_path.write_text("", encoding="utf-8")

    with pytest.raises(ConfigError, match="empty"):
        load_strategy_config(empty_path)
