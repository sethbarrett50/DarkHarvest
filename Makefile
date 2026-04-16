SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

UV   ?= uv
PY   ?= python
RUFF ?= ruff
PKG  ?= dark_harvest

START             ?= 2026-01-10
END               ?= 2026-03-10
PORTS             ?= 23 2323 7547 5555
BOTNET_METRIC     ?= sources
USER_AGENT        ?= SethBarrettResearch/1.0\ \(sebarrett@augusta.edu\)

RUN_ANALYSIS      ?= 0
EVENT_WINDOW      ?= 7
MAX_LAG           ?= 14
N_PERMUTATIONS    ?= 2000
RANDOM_SEED       ?= 42
REGRESSION_MODEL  ?= auto

RUN_ARGS = \
	--start $(START) \
	--end $(END) \
	--ports $(PORTS) \
	--botnet-metric $(BOTNET_METRIC) \
	--user-agent "$(USER_AGENT)"

ifeq ($(RUN_ANALYSIS),1)
RUN_ARGS += \
	--run-analysis \
	--event-window $(EVENT_WINDOW) \
	--max-lag $(MAX_LAG) \
	--n-permutations $(N_PERMUTATIONS) \
	--random-seed $(RANDOM_SEED) \
	--regression-model $(REGRESSION_MODEL)
endif

.PHONY: \
	help sync sync-dev format lint check test clean \
	run debug run-3month run-analysis debug-analysis \
	build preflight show-config output-dir

help: ## Show available targets
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make sync-dev"
	@echo "  make run"
	@echo "  make debug START=2025-01-01 END=2025-03-01"
	@echo "  make run-analysis"
	@echo "  make run-analysis START=2025-01-01 END=2025-03-31 EVENT_WINDOW=10 MAX_LAG=21"
	@echo "  make output-dir"

sync: ## Sync runtime dependencies
	$(UV) sync

sync-dev: ## Sync runtime + development dependencies
	$(UV) sync --all-groups

format: ## Format with ruff
	$(UV) run $(RUFF) format .

lint: ## Format and lint with ruff, applying safe fixes
	$(UV) run $(RUFF) format .
	$(UV) run $(RUFF) check . --fix

check: ## Run ruff check without modifying files
	$(UV) run $(RUFF) check .

test: ## Run test suite
	$(UV) run pytest

run: ## Run collection pipeline and generate overlay outputs under output/*
	$(UV) run $(PY) -m $(PKG).cli $(RUN_ARGS)

debug: ## Run collection pipeline with debug logging
	$(UV) run $(PY) -m $(PKG).cli $(RUN_ARGS) --debug

run-analysis: ## Run collection + statistical analysis under output/*
	$(MAKE) run RUN_ANALYSIS=1

debug-analysis: ## Run collection + analysis with debug logging
	$(MAKE) debug RUN_ANALYSIS=1

run-3month: ## Generate a short-range sample run under output/*
	$(UV) run $(PY) -m $(PKG).cli \
		--start 2025-01-01 \
		--end 2025-03-01 \
		--ports $(PORTS) \
		--botnet-metric $(BOTNET_METRIC) \
		--user-agent "$(USER_AGENT)"

build: ## Build sdist and wheel
	$(UV) build

preflight: ## Build package and run metadata checks
	$(UV) build
	$(UV) run $(PY) -m twine check dist/*

show-config: ## Print the effective runtime configuration
	@echo "START=$(START)"
	@echo "END=$(END)"
	@echo "PORTS=$(PORTS)"
	@echo "BOTNET_METRIC=$(BOTNET_METRIC)"
	@echo "USER_AGENT=$(USER_AGENT)"
	@echo "RUN_ANALYSIS=$(RUN_ANALYSIS)"
	@echo "EVENT_WINDOW=$(EVENT_WINDOW)"
	@echo "MAX_LAG=$(MAX_LAG)"
	@echo "N_PERMUTATIONS=$(N_PERMUTATIONS)"
	@echo "RANDOM_SEED=$(RANDOM_SEED)"
	@echo "REGRESSION_MODEL=$(REGRESSION_MODEL)"

output-dir: ## Show the output directory for the current config
	@PORTS_FMT="$$(echo "$(PORTS)" | tr ' ' '-')"; \
	echo "output/$(shell echo $(START) | tr -d -)_$(shell echo $(END) | tr -d -)__metric-$(BOTNET_METRIC)__ports-$$PORTS_FMT"

clean: ## Remove build/test/cache artifacts
	rm -rf .pytest_cache .ruff_cache build dist *.egg-info htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete

deps.check: ## Check for dependency issues
	$(UV) run deptry .