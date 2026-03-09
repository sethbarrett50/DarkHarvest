SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

UV   ?= uv
PY   ?= python
RUFF ?= ruff
PKG  ?= dark_harvest

START         ?= 2025-01-01
END           ?= 2026-01-01
PORTS         ?= 23 2323 7547 5555
BOTNET_METRIC ?= sources
USER_AGENT    ?= SethBarrettResearch/1.0\ \(sebarrett@augusta.edu\)
OUT_CSV       ?= outages.csv
OUT_PLOT      ?= overlay.png

RUN_ARGS = \
	--start $(START) \
	--end $(END) \
	--ports $(PORTS) \
	--botnet-metric $(BOTNET_METRIC) \
	--user-agent "$(USER_AGENT)" \
	--out-csv $(OUT_CSV) \
	--out-plot $(OUT_PLOT)

.PHONY: \
	help sync sync-dev format lint check test clean \
	run debug run-3month build preflight

help: ## Show available targets
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make sync-dev"
	@echo "  make run"
	@echo "  make debug START=2025-01-01 END=2025-03-01 OUT_PLOT=display_graph.png"

sync: ## Sync runtime dependencies
	$(UV) sync

sync-dev: ## Sync runtime + development dependencies
	$(UV) sync --all-groups

lint: ## Lint with ruff and apply safe auto-fixes
	$(UV) run $(RUFF) format .
	$(UV) run $(RUFF) check . --fix

test: ## Run test suite
	$(UV) run pytest

run: ## Build outage timetable + botnet proxy overlay plot
	$(UV) run $(PY) -m $(PKG).cli $(RUN_ARGS)

debug: ## Run main program with debug logging enabled
	$(UV) run $(PY) -m $(PKG).cli $(RUN_ARGS) --debug

run-3month: ## Generate a short-range sample graph
	$(UV) run $(PY) -m $(PKG).cli \
		--start 2025-01-01 \
		--end 2025-03-01 \
		--ports $(PORTS) \
		--botnet-metric $(BOTNET_METRIC) \
		--user-agent "$(USER_AGENT)" \
		--out-csv $(OUT_CSV) \
		--out-plot display_graph.png

build: ## Build sdist and wheel
	$(UV) build

preflight: ## Build package and run metadata checks
	$(UV) build
	$(UV) run $(PY) -m twine check dist/*

clean: ## Remove build/test/cache artifacts
	rm -rf .pytest_cache .ruff_cache build dist *.egg-info htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
