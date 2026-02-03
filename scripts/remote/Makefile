# Makefile for K8s remote build operations
# Usage: make -C scripts/remote <target> [profile=<profile>]

PROFILE ?=

.PHONY: help setup shell sync clean deploy test run logs

help: ## Show this help
	@echo "Remote Build Commands:"
	@echo "  setup    - Set up build environment (one-time)"
	@echo "  shell    - Open interactive shell in builder"
	@echo "  sync     - Sync source code to builder"
	@echo "  clean    - Kill existing cluster"
	@echo "  deploy   - Deploy RisingWave cluster (rebuilds if needed)"
	@echo "  test     - Run tests (assumes cluster is running)"
	@echo "  run      - Full workflow: sync -> clean -> deploy -> test"
	@echo "  logs     - View all logs"
	@echo ""
	@echo "Options:"
	@echo "  profile  - risedev profile to use (default: empty, use spanner-only for emulator)"
	@echo ""
	@echo "Examples:"
	@echo "  make -C scripts/remote deploy                           # Default profile (no emulator, for real Spanner)"
	@echo "  make -C scripts/remote deploy profile=spanner-only      # Use emulator"
	@echo "  make -C scripts/remote run profile=spanner-only         # Full workflow with emulator"

setup: ## Set up build environment
	./remote.sh setup

shell: ## Open shell in builder
	./remote.sh shell

sync: ## Sync source code
	./remote.sh sync

clean: ## Clean cluster
	./remote.sh clean

deploy: ## Deploy cluster
	./remote.sh deploy $(if $(PROFILE),--profile=$(PROFILE))

test: ## Run tests
	./remote.sh test

run: ## Full workflow: sync -> clean -> deploy -> test
	./remote.sh run $(if $(PROFILE),--profile=$(PROFILE))

logs: ## View logs
	./remote.sh logs
