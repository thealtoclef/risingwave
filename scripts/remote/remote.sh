#!/bin/bash
# remote - Single entry point for K8s remote build operations
# Usage: ./scripts/remote/remote.sh <command> [args]
#
# Commands:
#   setup       - Set up build environment (one-time)
#   shell       - Open interactive shell in builder
#   sync        - Sync source code to builder
#   clean       - Kill existing cluster in builder
#   deploy      - Deploy RisingWave cluster (rebuilds if needed)
#   test        - Run tests (assumes cluster is running)
#   run         - Full workflow: sync -> clean -> deploy -> test
#   logs [file] - View logs (default: all)
#
# Options:
#   --profile=<name>  - risedev profile to use

set -e

# ============ CONFIGURATION ============

NAMESPACE="devspace"
LABEL="app=risingwave-builder"
WORKSPACE="/root/workspace"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# SSH configuration - uses kubectl port-forward automatically
SSH_PORT="${SSH_PORT:-2222}"
SSH_USER="${SSH_USER:-root}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# ============ HELPER FUNCTIONS ============

# Get the first running pod with the label
get_pod() {
    local pod
    pod=$(kubectl get pod -n $NAMESPACE -l $LABEL --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [ -n "$pod" ] && echo "$pod"
}

# Setup kubectl port-forward for SSH access
setup_port_forward() {
    local pod=$1 local_port=$2 remote_port=${3:-22}

    local existing_pid=$(lsof -ti :$local_port 2>/dev/null)
    if [ -n "$existing_pid" ]; then
        echo -e "${BLUE}Port-forward already running on port $local_port (PID: $existing_pid)${NC}"
        echo "$existing_pid"
        return 0
    fi

    echo -e "${YELLOW}Setting up port-forward: localhost:$local_port -> $pod:$remote_port${NC}"
    kubectl port-forward -n $NAMESPACE "$pod" $local_port:$remote_port >/dev/null 2>&1 &
    local pf_pid=$!

    local attempt=0
    while [ $attempt -lt 10 ]; do
        if lsof -Pi :$local_port -sTCP:LISTEN >/dev/null 2>&1; then
            echo "$pf_pid"
            return 0
        fi
        kill -0 $pf_pid 2>/dev/null || break
        sleep 0.5
        ((attempt++))
    done

    kill $pf_pid 2>/dev/null || true
    return 1
}

# Cleanup port-forward
cleanup_port_forward() {
    local pid=$1 port=$2

    [ -n "$pid" ] && kill -0 $pid 2>/dev/null && kill $pid 2>/dev/null || true
    sleep 1
    lsof -ti :$port | xargs kill -9 2>/dev/null || true
}

# Execute command in the builder pod via kubectl exec
exec_in_builder() {
    local pod=$(get_pod)
    [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

    kubectl exec -n $NAMESPACE "$pod" -- bash -lc "$1"
}

# Setup SSH key for passwordless authentication
setup_ssh_key() {
    local pod=$1

    mkdir -p ~/.ssh && chmod 700 ~/.ssh

    [ ! -f ~/.ssh/id_rsa.pub ] && ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -q 2>/dev/null

    if [ -f ~/.ssh/id_rsa.pub ]; then
        local pub_key=$(cat ~/.ssh/id_rsa.pub)
        kubectl exec -n $NAMESPACE "$pod" -- bash -c "
            mkdir -p /root/.ssh && chmod 700 /root/.ssh
            if [ ! -f /root/.ssh/authorized_keys ] || ! grep -qF '$pub_key' /root/.ssh/authorized_keys 2>/dev/null; then
                echo '$pub_key' >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys
            fi
        " 2>/dev/null
    fi
}

# Execute command in the builder pod via kubectl exec
exec_in_builder() {
    local pod=$(get_pod)
    [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

    kubectl exec -n $NAMESPACE "$pod" -- bash -lc "$1"
}

# ============ MAIN LOGIC ============

# Show usage
show_usage() {
    echo -e "${BLUE}Remote Build Commands${NC}"
    grep "^# Commands:" "$0" -A 12 | sed 's/^# //'
    echo ""
    echo "Examples:"
    echo "  ./scripts/remote/remote.sh setup                    # Initial setup"
    echo "  ./scripts/remote/remote.sh sync                     # Sync source code"
    echo "  ./scripts/remote/remote.sh deploy                   # Deploy cluster"
    echo "  ./scripts/remote/remote.sh deploy --profile=spanner-emulator"
    echo "  ./scripts/remote/remote.sh test                     # Run tests"
    echo "  ./scripts/remote/remote.sh run                      # Full workflow"
    echo "  ./scripts/remote/remote.sh shell                    # Open shell"
    echo "  ./scripts/remote/remote.sh logs meta                # View logs"
    echo ""
    echo "  make -C scripts/remote setup                                       # Using Makefile"
    echo "  make -C scripts/remote run"
    echo "  make -C scripts/remote deploy profile=spanner-emulator"
    echo "  make -C scripts/remote deploy profile=spanner-real"
}

# Parse arguments
PROFILE="${PROFILE:-}"

COMMAND=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile=*) PROFILE="${1#*=}"; shift ;;
        *) [ -z "$COMMAND" ] && COMMAND="$1"; shift ;;
    esac
done

# Setup-specific helper functions are defined within the setup heredoc below

case "$COMMAND" in
    setup)
        echo -e "${YELLOW}Setting up build environment...${NC}"
        pod=$(get_pod)
        [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

        kubectl exec -i -n $NAMESPACE "$pod" -- bash << 'EOF'
set -e

# Helper functions for setup
setup_install_packages() {
    local packages="$1"
    echo "Installing: $packages"
    apt-get install -y $packages >/dev/null 2>&1 || { echo "ERROR: Failed to install packages"; exit 1; }
}

setup_verify_command() {
    local cmd="$1"
    command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd not found"; exit 1; }
    echo "✓ $cmd: $("$cmd" --version 2>/dev/null | head -1 || echo "installed")"
}

setup_ssh_server() {
    echo "Setting up SSH server..."

    mkdir -p /var/run/sshd /root/.ssh && chmod 700 /root/.ssh
    [ ! -f /etc/ssh/ssh_host_rsa_key ] && ssh-keygen -A -q

    local ssh_config="/etc/ssh/sshd_config"
    [ -f "$ssh_config" ] && cp "$ssh_config" "${ssh_config}.bak" 2>/dev/null || true

    grep -q "^PermitRootLogin" "$ssh_config" 2>/dev/null ||
        echo "PermitRootLogin yes" >> "$ssh_config" ||
        sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' "$ssh_config"

    if ! timeout 1 bash -c '</dev/tcp/localhost/22' 2>/dev/null; then
        /usr/sbin/sshd -t -f "$ssh_config" || { echo "ERROR: SSH config invalid"; exit 1; }
        nohup /usr/sbin/sshd -f "$ssh_config" >/dev/null 2>&1 &
        sleep 2
        timeout 1 bash -c '</dev/tcp/localhost/22' 2>/dev/null || { echo "ERROR: SSH server failed to start"; exit 1; }
        echo "✓ SSH server started"
    else
        echo "✓ SSH server already running"
    fi
}

# Infrastructure setup
echo "Setting up workspace..."
mkdir -p /root/workspace

echo "Installing dependencies..."
apt-get update -qq

# Remote script dependencies
setup_install_packages "rsync openssh-server neovim"

# Build dependencies
setup_install_packages "make build-essential cmake protobuf-compiler curl postgresql-client tmux lld pkg-config libssl-dev libsasl2-dev libblas-dev liblapack-dev libomp-dev parallel apt-transport-https ca-certificates gnupg sysvinit-utils"

# Google Cloud SDK
if ! grep -q "cloud-sdk" /etc/apt/sources.list.d/google-cloud-sdk.list 2>/dev/null; then
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update -qq
fi
setup_install_packages "google-cloud-cli google-cloud-cli-spanner-emulator"

# Verify installations
echo "Verifying installations..."
setup_verify_command rustc
setup_verify_command cargo
setup_verify_command gcloud

for cmd in make cmake protoc curl pidof; do
    setup_verify_command "$cmd"
done

# Setup SSH
setup_ssh_server

# Final verification
[ -d "/root/workspace" ] || { echo "ERROR: Workspace directory missing"; exit 1; }
timeout 1 bash -c '</dev/tcp/localhost/22' 2>/dev/null || echo "WARNING: SSH not accessible"

echo "✓ Setup complete"
EOF

        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Setup complete!${NC}"
        else
            echo -e "${RED}Setup failed!${NC}"
            exit 1
        fi
        ;;

    shell)
        pod=$(get_pod)
        [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

        echo "Opening shell in $pod..."
        kubectl exec -it -n $NAMESPACE "$pod" -- bash
        ;;

    sync)
        echo -e "${YELLOW}Syncing source code...${NC}"
        pod=$(get_pod)
        [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

        # Ensure workspace directory exists
        exec_in_builder "mkdir -p $WORKSPACE"

        # Check SSH server
        kubectl exec -n $NAMESPACE "$pod" -- timeout 1 bash -c '</dev/tcp/localhost/22' 2>/dev/null ||
            { echo -e "${RED}SSH server not running. Run setup first.${NC}"; exit 1; }

        # Setup SSH key
        setup_ssh_key "$pod"

        # Setup port-forward
        pf_pid=$(setup_port_forward "$pod" "$SSH_PORT" 22) ||
            { echo -e "${RED}Failed to setup port-forward${NC}"; exit 1; }

        # Sync with rsync
        echo "Syncing via rsync..."
        rsync -avPh --delete \
            --exclude='.git' \
            --exclude='target' \
            --exclude='build' \
            --exclude='.risingwave' \
            -e "ssh -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=5" \
            "$PROJECT_ROOT/" "$SSH_USER@localhost:$WORKSPACE/"

        sync_exit_code=$?

        # Cleanup
        cleanup_port_forward "$pf_pid" "$SSH_PORT"

        if [ $sync_exit_code -eq 0 ]; then
            echo -e "${GREEN}Sync complete${NC}"
        else
            echo -e "${RED}Sync failed!${NC}"
            exit 1
        fi
        ;;

    clean)
        echo -e "${YELLOW}Cleaning cluster...${NC}"
        exec_in_builder "cd $WORKSPACE && ./risedev k" 2>/dev/null || true
        exec_in_builder "sleep 5" 2>/dev/null || true

        echo -e "${YELLOW}Stopping Spanner emulator...${NC}"
        exec_in_builder "
            pidof emulator_main gateway_main 2>/dev/null | xargs -r kill -9 2>/dev/null || true
            sleep 2
        " 2>/dev/null || true

        exec_in_builder "cd $WORKSPACE && ./risedev remove-spanner" 2>/dev/null || true
        echo -e "${GREEN}Clean complete${NC}"
        ;;

    deploy)
        echo -e "${YELLOW}Deploying RisingWave cluster...${NC}"

        profile_arg=""
        [ -n "$PROFILE" ] && profile_arg=" $PROFILE"

        exec_in_builder "cd $WORKSPACE && ./risedev d$profile_arg"
        echo -e "${GREEN}Deploy complete${NC}"
        ;;

    test)
        echo -e "${YELLOW}Running tests...${NC}"
        exec_in_builder "cd $WORKSPACE && ./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'"
        echo -e "${GREEN}Tests complete${NC}"
        ;;

    run)
        echo -e "${BLUE}=== Full Workflow: sync -> clean -> deploy -> test ===${NC}"
        echo -e "${YELLOW}Step 1/4: Syncing source code...${NC}"
        "$0" sync
        echo -e "${YELLOW}Step 2/4: Cleaning existing cluster...${NC}"
        "$0" clean
        echo -e "${YELLOW}Step 3/4: Deploying cluster (will rebuild if needed)...${NC}"
        "$0" deploy $(if [ -n "$PROFILE" ]; then echo "--profile=$PROFILE"; fi)
        echo -e "${YELLOW}Step 4/4: Running tests...${NC}"
        "$0" test
        echo -e "${GREEN}=== Full workflow complete ===${NC}"
        ;;

    logs)
        log_file="${1:-}"
        pod=$(get_pod)
        [ -z "$pod" ] && { echo -e "${RED}No running pod found${NC}"; exit 1; }

        if [ -z "$log_file" ]; then
            echo "Tailing all logs (Ctrl+C to exit)..."
            kubectl exec -n $NAMESPACE "$pod" -- tail -f $WORKSPACE/.risingwave/log/*.log 2>/dev/null ||
                kubectl exec -n $NAMESPACE "$pod" -- bash -c "find $WORKSPACE/.risingwave/log -name '*.log' -exec tail -f {} +" 2>/dev/null ||
                echo "No logs found"
        else
            kubectl exec -n $NAMESPACE "$pod" -- tail -100 "$WORKSPACE/.risingwave/log/${log_file}.log" 2>/dev/null ||
                echo "Log file not found: ${log_file}.log"
        fi
        ;;

    *)
        show_usage
        ;;
esac
