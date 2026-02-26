#!/bin/bash
# remote - Single entry point for remote build operations via SSH
# Usage: ./scripts/remote/remote.sh <command> [args]
#
# Commands:
#   setup       - Set up build environment on remote server (one-time)
#   shell       - Open interactive SSH shell on remote server
#   sync        - Sync source code to remote server via git
#   clean       - Kill existing cluster on remote server
#   deploy      - Deploy RisingWave cluster (rebuilds if needed)
#   test        - Run tests (assumes cluster is running)
#   run         - Full workflow: sync -> clean -> deploy -> test
#   logs [file] - View logs (default: all)
#
# Environment Variables (or set in scripts/remote/.remote.env):
#   REMOTE_HOST     - SSH destination, e.g. user@10.0.0.1 [required]
#   REMOTE_DIR      - Repository path on remote server [required]
#   REMOTE_PASSWORD - SSH password (optional; uses key-based auth if unset)
#
# Options:
#   --profile=<name>  - risedev profile to use

set -e

# ============ CONFIGURATION ============

SYNC_BRANCH="_remote-sync"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

CONFIG_FILE="$(dirname "${BASH_SOURCE[0]}")/.remote.env"
[ -f "$CONFIG_FILE" ] && source "$CONFIG_FILE"

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_DIR="${REMOTE_DIR:-}"
REMOTE_PASSWORD="${REMOTE_PASSWORD:-}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=30)

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# ============ HELPER FUNCTIONS ============

validate_config() {
    if [ -z "$REMOTE_HOST" ]; then
        echo -e "${RED}REMOTE_HOST is not set. Set it via environment or .remote.env${NC}"
        exit 1
    fi
    if [ -z "$REMOTE_DIR" ]; then
        echo -e "${RED}REMOTE_DIR is not set. Set it via environment or .remote.env${NC}"
        exit 1
    fi
}

# Run a command on the remote server via SSH (non-interactive).
run_remote() {
    if [ -n "$REMOTE_PASSWORD" ]; then
        sshpass -p "$REMOTE_PASSWORD" ssh "${SSH_OPTS[@]}" -o LogLevel=ERROR "$REMOTE_HOST" "$1"
    else
        ssh "${SSH_OPTS[@]}" -o LogLevel=ERROR "$REMOTE_HOST" "$1"
    fi
}

# Run a command inside the repository directory on the remote server.
run_in_repo() {
    run_remote "cd '$REMOTE_DIR' && $1"
}

# Push a refspec to the remote server's git repo over SSH.
git_push_to_remote() {
    local refspec="$1"
    local ssh_cmd="ssh ${SSH_OPTS[*]} -o LogLevel=ERROR"
    if [ -n "$REMOTE_PASSWORD" ]; then
        ssh_cmd="sshpass -p $REMOTE_PASSWORD $ssh_cmd"
    fi
    GIT_SSH_COMMAND="$ssh_cmd" git -C "$PROJECT_ROOT" push -f "$REMOTE_HOST:$REMOTE_DIR" "$refspec"
}

# ============ MAIN LOGIC ============

show_usage() {
    echo -e "${BLUE}Remote Build Commands${NC}"
    echo ""
    echo "Usage: ./scripts/remote/remote.sh <command> [args]"
    echo ""
    echo "Commands:"
    echo "  setup       - Set up build environment on remote server (one-time)"
    echo "  shell       - Open interactive SSH shell on remote server"
    echo "  sync        - Sync source code to remote server via git"
    echo "  clean       - Kill existing cluster on remote server"
    echo "  deploy      - Deploy RisingWave cluster (rebuilds if needed)"
    echo "  test        - Run tests (assumes cluster is running)"
    echo "  run         - Full workflow: sync -> clean -> deploy -> test"
    echo "  logs [file] - View logs (default: all)"
    echo ""
    echo "Environment Variables (or set in scripts/remote/.remote.env):"
    echo "  REMOTE_HOST     - SSH destination (e.g., user@10.0.0.1)"
    echo "  REMOTE_DIR      - Repository path on remote server"
    echo "  REMOTE_PASSWORD - SSH password (optional)"
    echo ""
    echo "Options:"
    echo "  --profile=<name>  - risedev profile to use"
    echo ""
    echo "Examples:"
    echo "  ./scripts/remote/remote.sh setup"
    echo "  ./scripts/remote/remote.sh sync"
    echo "  ./scripts/remote/remote.sh deploy --profile=spanner-emulator"
    echo "  ./scripts/remote/remote.sh run --profile=spanner-real"
    echo ""
    echo "  make -C scripts/remote setup"
    echo "  make -C scripts/remote run"
    echo "  make -C scripts/remote deploy profile=spanner-emulator"
}

# Parse arguments
PROFILE="${PROFILE:-}"
COMMAND=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile=*) PROFILE="${1#*=}"; shift ;;
        *)
            if [ -z "$COMMAND" ]; then
                COMMAND="$1"
            else
                EXTRA_ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

case "$COMMAND" in
    setup)
        validate_config
        echo -e "${YELLOW}Setting up remote build environment on $REMOTE_HOST...${NC}"

        echo -e "${BLUE}Initializing git repository at $REMOTE_DIR...${NC}"
        run_remote "
            set -e
            mkdir -p '$REMOTE_DIR'
            cd '$REMOTE_DIR'
            if [ ! -d .git ]; then
                git init
                echo '✓ Git repository initialized'
            else
                echo '✓ Git repository already exists'
            fi
            git config receive.denyCurrentBranch ignore
        "

        echo -e "${BLUE}Installing build dependencies...${NC}"
        run_remote '
            set -e

            install_pkg() {
                echo "Installing: $1"
                apt-get install -y $1 >/dev/null 2>&1 || { echo "ERROR: Failed to install $1"; exit 1; }
            }

            verify_cmd() {
                command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 not found"; exit 1; }
                echo "✓ $1: $("$1" --version 2>/dev/null | head -1 || echo installed)"
            }

            apt-get update -qq

            install_pkg "make build-essential cmake protobuf-compiler curl postgresql-client tmux lld pkg-config libssl-dev libsasl2-dev libblas-dev liblapack-dev libomp-dev parallel apt-transport-https ca-certificates gnupg sysvinit-utils git"

            if ! grep -q "cloud-sdk" /etc/apt/sources.list.d/google-cloud-sdk.list 2>/dev/null; then
                curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
                echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list
                apt-get update -qq
            fi
            install_pkg "google-cloud-cli google-cloud-cli-spanner-emulator"

            echo "Verifying installations..."
            for cmd in git rustc cargo gcloud make cmake protoc curl; do
                verify_cmd "$cmd"
            done

            echo "✓ Setup complete"
        '

        echo -e "${GREEN}Setup complete!${NC}"
        ;;

    shell)
        validate_config
        echo -e "${BLUE}Opening SSH shell on $REMOTE_HOST (dir: $REMOTE_DIR)...${NC}"
        if [ -n "$REMOTE_PASSWORD" ]; then
            sshpass -p "$REMOTE_PASSWORD" ssh "${SSH_OPTS[@]}" -t "$REMOTE_HOST" "cd '$REMOTE_DIR' 2>/dev/null; exec \$SHELL -l"
        else
            ssh "${SSH_OPTS[@]}" -t "$REMOTE_HOST" "cd '$REMOTE_DIR' 2>/dev/null; exec \$SHELL -l"
        fi
        ;;

    sync)
        validate_config
        echo -e "${YELLOW}Syncing source code to $REMOTE_HOST:$REMOTE_DIR...${NC}"

        cd "$PROJECT_ROOT"

        # Ensure the remote repo exists and accepts pushes to the checked-out branch.
        echo -e "${BLUE}Preparing remote repository...${NC}"
        run_remote "
            mkdir -p '$REMOTE_DIR'
            cd '$REMOTE_DIR'
            if [ ! -d .git ]; then git init; fi
            git config receive.denyCurrentBranch ignore
        "

        # Build a sync commit from the current working tree using git plumbing,
        # without modifying HEAD or the user's staged/unstaged state.
        echo -e "${BLUE}Creating sync commit...${NC}"

        INDEX_BACKUP=".git/index.sync.bak"
        cp .git/index "$INDEX_BACKUP"
        trap 'mv -f "$PROJECT_ROOT/$INDEX_BACKUP" "$PROJECT_ROOT/.git/index" 2>/dev/null' EXIT

        git add -A
        tree=$(git write-tree)

        mv -f "$INDEX_BACKUP" .git/index
        trap - EXIT

        if git rev-parse HEAD >/dev/null 2>&1; then
            commit=$(git commit-tree "$tree" -p HEAD -m "remote sync $(date '+%Y-%m-%d %H:%M:%S')")
        else
            commit=$(git commit-tree "$tree" -m "remote sync $(date '+%Y-%m-%d %H:%M:%S')")
        fi

        git update-ref "refs/heads/$SYNC_BRANCH" "$commit"

        echo -e "${BLUE}Pushing to remote server...${NC}"
        git_push_to_remote "$SYNC_BRANCH:$SYNC_BRANCH"

        echo -e "${BLUE}Checking out on remote...${NC}"
        run_in_repo "git checkout -f '$SYNC_BRANCH' && git reset --hard '$SYNC_BRANCH'"

        echo -e "${GREEN}Sync complete${NC}"
        ;;

    clean)
        validate_config
        echo -e "${YELLOW}Cleaning cluster on $REMOTE_HOST...${NC}"
        run_in_repo "./risedev k" 2>/dev/null || true
        run_in_repo "sleep 5" 2>/dev/null || true

        echo -e "${YELLOW}Stopping Spanner emulator...${NC}"
        run_in_repo "
            pidof emulator_main gateway_main 2>/dev/null | xargs -r kill -9 2>/dev/null || true
            sleep 2
        " 2>/dev/null || true

        run_in_repo "./risedev remove-spanner" 2>/dev/null || true
        echo -e "${GREEN}Clean complete${NC}"
        ;;

    deploy)
        validate_config
        echo -e "${YELLOW}Deploying RisingWave cluster on $REMOTE_HOST...${NC}"

        profile_arg=""
        [ -n "$PROFILE" ] && profile_arg=" $PROFILE"

        run_in_repo "./risedev d$profile_arg"
        echo -e "${GREEN}Deploy complete${NC}"
        ;;

    test)
        validate_config
        echo -e "${YELLOW}Running tests on $REMOTE_HOST...${NC}"
        run_in_repo "./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'"
        echo -e "${GREEN}Tests complete${NC}"
        ;;

    run)
        validate_config
        echo -e "${BLUE}=== Full Workflow: sync -> clean -> deploy -> test ===${NC}"

        echo -e "${YELLOW}Step 1/4: Syncing source code...${NC}"
        "$0" sync

        echo -e "${YELLOW}Step 2/4: Cleaning existing cluster...${NC}"
        "$0" clean

        echo -e "${YELLOW}Step 3/4: Deploying cluster...${NC}"
        "$0" deploy $([ -n "$PROFILE" ] && echo "--profile=$PROFILE")

        echo -e "${YELLOW}Step 4/4: Running tests...${NC}"
        "$0" test

        echo -e "${GREEN}=== Full workflow complete ===${NC}"
        ;;

    logs)
        validate_config
        log_file="${EXTRA_ARGS[0]:-}"

        if [ -z "$log_file" ]; then
            echo "Tailing all logs (Ctrl+C to exit)..."
            run_in_repo "tail -f .risingwave/log/*.log" 2>/dev/null ||
                run_in_repo "find .risingwave/log -name '*.log' -exec tail -f {} +" 2>/dev/null ||
                echo "No logs found"
        else
            run_in_repo "tail -100 '.risingwave/log/${log_file}.log'" 2>/dev/null ||
                echo "Log file not found: ${log_file}.log"
        fi
        ;;

    *)
        show_usage
        ;;
esac
