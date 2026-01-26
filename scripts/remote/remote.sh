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

set -e

# Configuration
NAMESPACE="devspace"
LABEL="app=risingwave-builder"
WORKSPACE="/root/workspace"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# SSH configuration - uses kubectl port-forward automatically
SSH_PORT="${SSH_PORT:-2222}"  # Local port for port-forward
SSH_USER="${SSH_USER:-root}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Get the first running pod with the label
get_pod() {
    kubectl get pod -n $NAMESPACE -l $LABEL --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# Setup kubectl port-forward for SSH access
setup_port_forward() {
    local pod=$1
    local local_port=$2
    local remote_port=${3:-22}
    
    # Check if port-forward is already running
    local existing_pid=$(lsof -ti :$local_port 2>/dev/null)
    if [ -n "$existing_pid" ]; then
        echo -e "${BLUE}Port-forward already running on port $local_port (PID: $existing_pid)${NC}"
        echo "$existing_pid"
        return 0
    fi
    
    echo -e "${YELLOW}Setting up port-forward: localhost:$local_port -> $pod:$remote_port${NC}"
    kubectl port-forward -n $NAMESPACE "$pod" $local_port:$remote_port > /dev/null 2>&1 &
    local pf_pid=$!
    
    # Wait for port-forward to be ready
    local max_attempts=10
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if lsof -Pi :$local_port -sTCP:LISTEN > /dev/null 2>&1; then
            echo "$pf_pid"
            return 0
        fi
        # Check if process died
        if ! kill -0 $pf_pid 2>/dev/null; then
            break
        fi
        sleep 0.5
        attempt=$((attempt + 1))
    done
    
    # Port-forward failed to start
    kill $pf_pid 2>/dev/null || true
    return 1
}

# Cleanup port-forward
cleanup_port_forward() {
    local pid=$1
    local port=$2
    
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        kill $pid 2>/dev/null || true
        # Wait a bit for the port to be released
        sleep 1
    fi
    
    # Also kill any port-forward on this port (in case pid tracking failed)
    lsof -ti :$port | xargs kill -9 2>/dev/null || true
}

# Execute command in the builder pod via kubectl exec (SSH only needed for rsync)
exec_in_builder() {
    local pod=$(get_pod)
    if [ -z "$pod" ]; then
        echo -e "${RED}Error: No running pod found with label '$LABEL' in namespace '$NAMESPACE'${NC}"
        echo "Please ensure your builder pod is running and has the label: $LABEL"
        exit 1
    fi
    
    # Use kubectl exec directly (no SSH needed for command execution)
    kubectl exec -n $NAMESPACE "$pod" -- bash -lc "$1"
}

# Show usage
show_usage() {
    echo -e "${BLUE}Remote Build Commands${NC}"
    grep "^# Commands:" "$0" -A 10 | sed 's/^# //'
    echo ""
    echo "Examples:"
    echo "  ./scripts/remote/remote.sh setup      # Initial setup"
    echo "  ./scripts/remote/remote.sh sync       # Sync source code"
    echo "  ./scripts/remote/remote.sh deploy     # Deploy cluster"
    echo "  ./scripts/remote/remote.sh test       # Run tests"
    echo "  ./scripts/remote/remote.sh run        # Full workflow: sync -> clean -> deploy -> test"
    echo "  ./scripts/remote/remote.sh shell      # Open shell in builder"
    echo "  ./scripts/remote/remote.sh logs meta  # View meta logs"
    echo ""
    echo "  make -C scripts/remote setup          # Using Makefile"
    echo "  make -C scripts/remote run"
}

# Main
COMMAND="${1:-}"
shift || true

case "$COMMAND" in
    setup)
        echo -e "${YELLOW}Setting up build environment...${NC}"
        pod=$(get_pod)
        if [ -z "$pod" ]; then
            echo -e "${RED}No running pod found${NC}"
            exit 1
        fi
        # Run setup - use -i to ensure output is shown
        kubectl exec -i -n $NAMESPACE "$pod" -- bash << 'EOF'
set -e

echo "=== Setting Up Remote Script Infrastructure ==="
echo "Creating workspace directory..."
mkdir -p /root/workspace
if [ ! -d "/root/workspace" ]; then
    echo "ERROR: Failed to create /root/workspace directory"
    exit 1
fi

echo "Installing remote script dependencies (rsync and openssh-server)..."
apt-get update -qq
if ! apt-get install -y rsync openssh-server; then
    echo "ERROR: Failed to install remote script dependencies"
    exit 1
fi

# Verify remote script dependencies
echo "Verifying remote script dependencies..."
for cmd in rsync ssh; do
    if command -v $cmd > /dev/null 2>&1; then
        echo "✓ $cmd is installed: $(command -v $cmd)"
    else
        echo "ERROR: $cmd is not installed"
        exit 1
    fi
done
echo "Remote script dependencies installed successfully"
echo ""

echo "=== Setting Up Build Environment ==="
echo "Prerequisite check: Verifying Rust is available..."
RUSTC_CMD=$(command -v rustc 2>/dev/null || echo "")
CARGO_CMD=$(command -v cargo 2>/dev/null || echo "")

if [ -z "$RUSTC_CMD" ] || [ -z "$CARGO_CMD" ]; then
    echo "ERROR: Rust is not available in the container"
    echo "  rustc: $RUSTC_CMD"
    echo "  cargo: $CARGO_CMD"
    exit 1
fi

# Verify rustc and cargo work
if ! "$RUSTC_CMD" --version > /dev/null 2>&1; then
    echo "ERROR: rustc is not working"
    exit 1
fi

if ! "$CARGO_CMD" --version > /dev/null 2>&1; then
    echo "ERROR: cargo is not working"
    exit 1
fi

echo "✓ Rust verified: $($RUSTC_CMD --version | head -1)"
echo "✓ Cargo verified: $($CARGO_CMD --version | head -1)"

echo "Installing build dependencies (apt packages)..."
if ! apt-get install -y make build-essential cmake protobuf-compiler curl postgresql-client tmux lld pkg-config libssl-dev libsasl2-dev libblas-dev liblapack-dev libomp-dev parallel apt-transport-https ca-certificates gnupg sysvinit-utils; then
    echo "ERROR: Failed to install build dependencies"
    exit 1
fi

echo "Installing Google Cloud CLI (for Spanner emulator)..."
# Install gcloud CLI using official Debian/Ubuntu package method
# This handles Python compatibility automatically
if ! grep -q "cloud-sdk" /etc/apt/sources.list.d/google-cloud-sdk.list 2>/dev/null; then
    echo "Adding Google Cloud SDK repository..."
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update -qq
fi
if ! apt-get install -y google-cloud-cli google-cloud-cli-spanner-emulator; then
    echo "ERROR: Failed to install google-cloud-cli or google-cloud-cli-spanner-emulator"
    exit 1
fi

# Verify gcloud installation
echo "Verifying gcloud installation..."
if ! command -v gcloud >/dev/null 2>&1; then
    echo "ERROR: gcloud command not found after installation"
    exit 1
fi
if ! gcloud --version >/dev/null 2>&1; then
    echo "ERROR: gcloud --version failed"
    exit 1
fi
echo "✓ Google Cloud CLI installed: $(gcloud --version | head -1)"

# Verify spanner emulator is available
if ! gcloud emulators spanner --help >/dev/null 2>&1; then
    echo "WARNING: Spanner emulator command not available (may need google-cloud-cli-spanner-emulator)"
else
    echo "✓ Spanner emulator available"
fi

# Verify critical dependencies are installed
echo "Verifying build dependencies..."
for cmd in make cmake protoc curl pidof; do
    if command -v $cmd > /dev/null 2>&1; then
        echo "✓ $cmd is installed: $(command -v $cmd)"
    else
        echo "ERROR: $cmd is not installed"
        exit 1
    fi
done
echo "Build dependencies installed successfully"

echo "Setting up SSH server..."
# Create SSH directory if it doesn't exist
mkdir -p /var/run/sshd /root/.ssh
chmod 700 /root/.ssh

# Generate host keys if they don't exist
if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "Generating SSH host keys..."
    ssh-keygen -A -q
fi

# Configure SSH server
SSH_CONFIG="/etc/ssh/sshd_config"
# Backup original config if it exists
[ -f "$SSH_CONFIG" ] && cp "$SSH_CONFIG" "${SSH_CONFIG}.bak" 2>/dev/null || true

# Configure SSH to allow root login with key-based authentication
if ! grep -q "^PermitRootLogin" "$SSH_CONFIG" 2>/dev/null; then
    echo "PermitRootLogin yes" >> "$SSH_CONFIG"
else
    sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' "$SSH_CONFIG"
fi

# Start SSH server in background
# Check if sshd is running by trying to connect to port 22
if ! bash -c 'timeout 1 bash -c "</dev/tcp/localhost/22" 2>/dev/null'; then
    echo "Starting SSH server..."
    # Test config first
    if ! /usr/sbin/sshd -t -f /etc/ssh/sshd_config; then
        echo "ERROR: SSH config test failed"
        /usr/sbin/sshd -t -f /etc/ssh/sshd_config 2>&1 || true
        exit 1
    fi
    # Start sshd in background (redirect output to avoid blocking)
    nohup /usr/sbin/sshd -f /etc/ssh/sshd_config > /dev/null 2>&1 &
    sshd_pid=$!
    sleep 3
    # Verify it's listening on port 22
    if ! bash -c 'timeout 1 bash -c "</dev/tcp/localhost/22" 2>/dev/null'; then
        echo "ERROR: Failed to start SSH server (not listening on port 22)"
        exit 1
    fi
    echo "SSH server started successfully on port 22 (PID: $sshd_pid)"
else
    echo "SSH server already running"
fi


echo "Configuring Cargo..."
mkdir -p /root/.cargo
cat > /root/.cargo/config.toml << 'CARGO'
[build]
jobs = 16
[profile.dev.package."*"]
opt-level = 2
CARGO

echo "Verifying setup..."
if [ ! -d "/root/workspace" ]; then
    echo "ERROR: Workspace directory /root/workspace does not exist after setup"
    exit 1
fi

# Verify SSH server is running
if ! bash -c 'timeout 1 bash -c "</dev/tcp/localhost/22" 2>/dev/null'; then
    echo "WARNING: SSH server is not listening on port 22"
else
    echo "SSH server is running on port 22"
fi

echo "=== Final Verification ==="
RUSTC_CMD=$(command -v rustc)
CARGO_CMD=$(command -v cargo)
echo "Rust: $($RUSTC_CMD --version | head -1)"
echo "Cargo: $($CARGO_CMD --version | head -1)"
echo "All checks passed!"
EOF
        setup_exit_code=$?
        
        if [ $setup_exit_code -eq 0 ]; then
            echo -e "${GREEN}Setup complete!${NC}"
        else
            echo -e "${RED}Setup failed with exit code $setup_exit_code!${NC}"
            echo -e "${YELLOW}Please check the output above for errors.${NC}"
            exit 1
        fi
        ;;

    shell)
        pod=$(get_pod)
        if [ -z "$pod" ]; then
            echo -e "${RED}No running pod found${NC}"
            exit 1
        fi
        
        echo "Opening shell in $pod..."
        kubectl exec -it -n $NAMESPACE "$pod" -- bash
        ;;

    sync)
        echo -e "${YELLOW}Syncing source code...${NC}"
        pod=$(get_pod)
        if [ -z "$pod" ]; then
            echo -e "${RED}No running pod found${NC}"
            exit 1
        fi
        
        # Ensure workspace directory exists before syncing
        exec_in_builder "mkdir -p $WORKSPACE"
        
        # Check if SSH server is running in the container
        echo "Checking SSH server status..."
        if ! kubectl exec -n $NAMESPACE "$pod" -- bash -c "timeout 1 bash -c '</dev/tcp/localhost/22' 2>/dev/null" 2>/dev/null; then
            echo -e "${RED}SSH server is not running in the container${NC}"
            echo -e "${YELLOW}Please run 'make -C scripts/remote setup' first to set up SSH server${NC}"
            exit 1
        fi
        echo "SSH server is running"
        
        # Setup SSH key for passwordless authentication (before port-forward)
        echo "Setting up SSH key for passwordless authentication..."
        mkdir -p ~/.ssh
        chmod 700 ~/.ssh
        if [ ! -f ~/.ssh/id_rsa.pub ]; then
            echo "Generating SSH key..."
            ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -q 2>/dev/null || true
        fi
        
        # Copy public key to container (using kubectl exec)
        if [ -f ~/.ssh/id_rsa.pub ]; then
            pub_key=$(cat ~/.ssh/id_rsa.pub)
            echo "Adding SSH public key to container..."
            kubectl exec -n $NAMESPACE "$pod" -- bash -c "
                mkdir -p /root/.ssh
                chmod 700 /root/.ssh
                if [ ! -f /root/.ssh/authorized_keys ] || ! grep -qF '${pub_key}' /root/.ssh/authorized_keys 2>/dev/null; then
                    echo '${pub_key}' >> /root/.ssh/authorized_keys
                    chmod 600 /root/.ssh/authorized_keys
                    echo 'SSH key added successfully'
                else
                    echo 'SSH key already present'
                fi
            " 2>&1
            echo "SSH key configured"
        else
            echo "Warning: Could not find or generate SSH public key"
        fi
        
        # Setup port-forward for rsync
        echo "Setting up port-forward..."
        pf_pid=$(setup_port_forward "$pod" "$SSH_PORT" 22)
        if [ -z "$pf_pid" ]; then
            echo -e "${RED}Failed to setup port-forward${NC}"
            exit 1
        fi
        
        # Use rsync over SSH via port-forward
        echo "Syncing via rsync (port-forward on localhost:$SSH_PORT)..."
        rsync -avz --delete \
            -e "ssh -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=5" \
            --exclude='target/' \
            --exclude='.git/' \
            --exclude='.risingwave/' \
            --exclude='*.log' \
            --exclude='.DS_Store' \
            --exclude='node_modules/' \
            --exclude='._*' \
            --exclude='.cursor/' \
            "$PROJECT_ROOT/" "$SSH_USER@localhost:$WORKSPACE/"
        
        sync_exit_code=$?
        
        # Cleanup port-forward
        cleanup_port_forward "$pf_pid" "$SSH_PORT"
        
        if [ $sync_exit_code -eq 0 ]; then
            echo -e "${GREEN}Sync complete${NC}"
        else
            echo -e "${RED}Sync failed!${NC}"
            echo -e "${YELLOW}Troubleshooting tips:${NC}"
            echo "  1. Ensure SSH server is running: kubectl exec -n $NAMESPACE $pod -- bash -c 'timeout 1 bash -c \"</dev/tcp/localhost/22\" 2>/dev/null'"
            echo "  2. Verify port-forward: lsof -i :$SSH_PORT"
            echo "  3. Test SSH manually: ssh -p $SSH_PORT $SSH_USER@localhost"
            exit 1
        fi
        ;;

    clean)
        echo -e "${YELLOW}Cleaning cluster...${NC}"
        exec_in_builder "cd $WORKSPACE && ./risedev k" || echo "No existing cluster to clean"
        echo -e "${YELLOW}Waiting for services to shut down...${NC}"
        exec_in_builder "sleep 5" || true
        echo -e "${YELLOW}Stopping Spanner emulator processes...${NC}"
        exec_in_builder "
            # Kill Spanner emulator processes using pidof (recommended by gcloud docs)
            # The emulator runs two processes: emulator_main (gRPC) and gateway_main (REST)
            pidof emulator_main 2>/dev/null | xargs -r kill -9 2>/dev/null || true
            pidof gateway_main 2>/dev/null | xargs -r kill -9 2>/dev/null || true
            echo \"Killed emulator processes via pidof\"
            sleep 2
        " || echo "Emulator cleanup completed"
        echo -e "${YELLOW}Cleaning Spanner emulator files...${NC}"
        exec_in_builder "cd $WORKSPACE && export ENABLE_SPANNER=true && ./risedev remove-spanner" || echo "No Spanner files to clean"
        echo -e "${YELLOW}Checking ports...${NC}"
        exec_in_builder "
            # Quick check if ports are in use (non-blocking)
            for port in 9010 9020; do
                if timeout 0.1 bash -c \"echo > /dev/tcp/127.0.0.1/\$port\" 2>/dev/null; then
                    echo \"WARNING: Port \$port appears to be in use\"
                    # Try to show what's using it
                    if command -v lsof >/dev/null 2>&1; then
                        echo \"Processes holding port \$port:\"
                        lsof -i :\$port 2>/dev/null || echo \"  (lsof found nothing)\"
                    fi
                    echo \"Note: risedev will check ports during deployment and fail with a clear error if needed\"
                else
                    echo \"Port \$port appears to be free\"
                fi
            done
        " || echo "Port check completed"
        echo -e "${GREEN}Clean complete${NC}"
        ;;

    deploy)
        echo -e "${YELLOW}Deploying RisingWave cluster...${NC}"
        echo -e "${YELLOW}Note: This will rebuild if needed.${NC}"
        exec_in_builder "export ENABLE_SPANNER=true && cd $WORKSPACE && ./risedev d spanner-only"
        echo -e "${GREEN}Deploy complete${NC}"
        ;;

    test)
        echo -e "${YELLOW}Running tests...${NC}"
        echo -e "${YELLOW}Note: This assumes RisingWave cluster is already running.${NC}"
        echo -e "${YELLOW}If cluster is not running, use 'deploy' command first or 'run' command instead.${NC}"
        exec_in_builder "export ENABLE_SPANNER=true && cd $WORKSPACE && ./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'"
        echo -e "${GREEN}Tests complete${NC}"
        ;;

    run)
        echo -e "${BLUE}=== Full Workflow: sync -> clean -> deploy -> test ===${NC}"
        echo -e "${YELLOW}Step 1/4: Syncing source code...${NC}"
        "$0" sync
        echo -e "${YELLOW}Step 2/4: Cleaning existing cluster...${NC}"
        "$0" clean
        echo -e "${YELLOW}Step 3/4: Deploying cluster (will rebuild if needed)...${NC}"
        "$0" deploy
        echo -e "${YELLOW}Step 4/4: Running tests...${NC}"
        "$0" test
        echo -e "${GREEN}=== Full workflow complete ===${NC}"
        ;;

    logs)
        log_file="${1:-}"
        pod=$(get_pod)
        if [ -z "$pod" ]; then
            echo -e "${RED}No running pod found${NC}"
            exit 1
        fi
        if [ -z "$log_file" ]; then
            echo "Tailing all logs (Ctrl+C to exit)..."
            kubectl exec -n $NAMESPACE "$pod" -- tail -f $WORKSPACE/.risingwave/log/*.log 2>/dev/null || kubectl exec -n $NAMESPACE "$pod" -- bash -c "find $WORKSPACE/.risingwave/log -name '*.log' -exec tail -f {} +" 2>/dev/null || echo "No logs found in $WORKSPACE/.risingwave/log/"
        else
            kubectl exec -n $NAMESPACE "$pod" -- tail -100 "$WORKSPACE/.risingwave/log/${log_file}.log" 2>/dev/null || echo "Log file not found: $WORKSPACE/.risingwave/log/${log_file}.log"
        fi
        ;;

    *)
        show_usage
        ;;
esac
