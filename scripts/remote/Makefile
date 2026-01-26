# Makefile for K8s remote build operations
# Usage: make -C scripts/remote <target>

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

setup: ## Set up build environment
	./remote.sh setup

shell: ## Open shell in builder
	./remote.sh shell

sync: ## Sync source code
	./remote.sh sync

clean: ## Clean cluster
	./remote.sh clean

deploy: ## Deploy cluster
	./remote.sh deploy

test: ## Run tests
	./remote.sh test

run: ## Full workflow: sync -> clean -> deploy -> test
	./remote.sh run

logs: ## View logs
	./remote.sh logs
