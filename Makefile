SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

UV  ?= uv
PY  ?= python
RUFF ?= ruff


# .PHONY: help sync test clean \
#         sim-bin sim-mc xseciot \
#         bin-label merge \
#         overall-perf overall-scrape

help: 
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make sync"
	@echo "  make main DOCUMENT_PATH=datasets/CEFlows/CEFlows2_merged.csv"

sync: 
	$(UV) sync

lint: ## Lint with ruff and apply safe auto-fixes
	$(UV) run $(RUFF) format .
	$(UV) run $(RUFF) check . --fix

main: ## Build outage timetable + botnet proxy overlay plot
	$(UV) run python -m src.cli \
		--start 2025-01-01 --end 2026-01-01 \
		--ports 23 2323 7547 5555 \
		--botnet-metric sources \
		--user-agent "SethBarrettResearch/1.0 (sebarrett@augusta.edu)" \
		--out-csv outages.csv \
		--out-plot overlay.png \

debug: ## Debugging mode for main
	$(UV) run python -m src.cli \
		--start 2025-01-01 --end 2026-01-01 \
		--ports 23 2323 7547 5555 \
		--botnet-metric sources \
		--user-agent "SethBarrettResearch/1.0 (sebarrett@augusta.edu)" \
		--out-csv outages.csv \
		--out-plot overlay.png \
		--debug

3month: 
	$(UV) run python -m src.cli \
		--start 2025-01-01 --end 2025-03-01 \
		--ports 23 2323 7547 5555 \
		--botnet-metric sources \
		--user-agent "SethBarrettResearch/1.0 (sebarrett@augusta.edu)" \
		--out-csv outages.csv \
		--out-plot overlay.png 

last-month:
	$(UV) run python -m src.cli \
		--start 2025-12-01 --end 2026-01-01 \
		--ports 23 2323 7547 5555 \
		--botnet-metric sources \
		--user-agent "SethBarrettResearch/1.0 (sebarrett@augusta.edu)" \
		--out-csv outages.csv \
		--out-plot overlay.png 