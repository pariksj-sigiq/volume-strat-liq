"""Async entrypoint placeholder for the realtime scanner lifecycle."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.config import DEFAULT_STRATEGY_CONFIG, ExecutionMode, load_strategy_config


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for scanner startup."""

    parser = argparse.ArgumentParser(description="Run the liq-sweep realtime scanner.")
    parser.add_argument("--config", type=Path, default=DEFAULT_STRATEGY_CONFIG)
    parser.add_argument("--paper", action="store_true", help="Force paper execution mode.")
    parser.add_argument("--live", action="store_true", help="Request live execution mode.")
    parser.add_argument("--check-config", action="store_true", help="Validate config and exit.")
    return parser


def main() -> int:
    """Validate configuration and prepare the scanner process."""

    args = build_parser().parse_args()
    config = load_strategy_config(args.config)
    requested_mode = ExecutionMode.LIVE if args.live else config.execution.mode
    if args.paper:
        requested_mode = ExecutionMode.PAPER
    if args.check_config:
        print(f"config ok: {config.meta.name} ({requested_mode.value})")
        return 0
    raise SystemExit("runtime wiring starts in delivery step 11; use --check-config for now")


if __name__ == "__main__":
    raise SystemExit(main())
