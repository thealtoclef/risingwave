# Makefile for K8s remote build operations
# Usage: make -C scripts/remote <target> [profile=<profile>]

profile ?=

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
	@echo "  profile  - risedev profile to use (default: empty)"
	@echo ""
	@echo "Examples:"
	@echo "  make -C scripts/remote deploy                                    # Default profile"
	@echo "  make -C scripts/remote deploy profile=spanner-emulator            # Use emulator"
	@echo "  make -C scripts/remote deploy profile=spanner-real                 # Real Spanner"
	@echo "  make -C scripts/remote run profile=spanner-emulator               # Full workflow with emulator"

setup: ## Set up build environment
	./remote.sh setup

shell: ## Open shell in builder
	./remote.sh shell

sync: ## Sync source code
	./remote.sh sync

clean: ## Clean cluster
	./remote.sh clean

deploy: ## Deploy cluster
	./remote.sh deploy $(if $(profile),--profile=$(profile))

test: ## Run tests
	./remote.sh test

run: ## Full workflow: sync -> clean -> deploy -> test
	./remote.sh run $(if $(profile),--profile=$(profile))

logs: ## View logs
	./remote.sh logs
