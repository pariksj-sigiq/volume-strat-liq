PYTHON ?= /opt/homebrew/bin/python3.12
PYTHONPATH := $(CURDIR)
PYCACHE := /tmp/liq-sweep-pycache

.PHONY: install install-hooks test test-python test-node check run-paper config-check web-build build-tod clean

install:
	$(PYTHON) -m pip install -e ".[dev]"

install-hooks:
	bash scripts/install_git_hooks.sh

test: test-python test-node

test-python:
	PYTHONPATH=$(PYTHONPATH) PYTHONPYCACHEPREFIX=$(PYCACHE) $(PYTHON) -m pytest -q

test-node:
	npm test

check:
	PYTHONPATH=$(PYTHONPATH) PYTHONPYCACHEPREFIX=$(PYCACHE) $(PYTHON) -m py_compile \
		app/server.py backtest/liquidity_sweep_backtest.py src/app.py src/config.py
	node --check src/upstox/backfill.mjs
	node --check app/static/app.js
	test -s web/dist/index.html
	test -n "$$(find web/dist/assets -maxdepth 1 -type f -name '*.js' -print -quit)"
	test -n "$$(find web/dist/assets -maxdepth 1 -type f -name '*.css' -print -quit)"

web-build:
	npm run web:check

config-check:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m src.app --check-config

run-paper:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m src.app --paper

build-tod:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/build_tod_baseline.py

clean:
	rm -rf .pytest_cache
