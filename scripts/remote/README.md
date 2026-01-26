# K8s Remote Build

Build and test RisingWave on a remote K8s builder pod using rsync for fast incremental syncing.

## Requirements

- Builder pod running in K8s with label `app=risingwave-builder`
- Namespace: `devspace` (configurable in `remote.sh`)
- Container image must have Rust pre-installed
- **SSH server will be automatically set up** by the `setup` command
  - The `setup` command installs and configures OpenSSH server
  - SSH server runs on port 22 (no need to declare it in pod spec)
  - Uses SSH key-based authentication (automatically configured during sync)

## Quick Start

```bash
# One-time setup (installs dependencies and configures SSH)
make -C scripts/remote setup

# Sync source code (uses rsync over SSH for fast incremental sync)
make -C scripts/remote sync

# Deploy RisingWave cluster (rebuilds if needed)
make -C scripts/remote deploy

# Run tests (assumes cluster is running)
make -C scripts/remote test

# Full workflow: sync -> clean -> deploy -> test
make -C scripts/remote run

# Or use the script directly
./scripts/remote/remote.sh sync
./scripts/remote/remote.sh deploy
./scripts/remote/remote.sh test
```

## Commands

| Command | Description |
|---------|-------------|
| `setup` | Set up remote script infrastructure and build environment (one-time) |
| `shell` | Open interactive shell in builder (uses kubectl exec) |
| `sync` | Sync source code to builder (uses rsync over SSH) |
| `clean` | Kill existing RisingWave cluster |
| `deploy` | Deploy RisingWave cluster (rebuilds if needed via `./risedev d`) |
| `test` | Run tests (assumes cluster is running) |
| `run` | Full workflow: sync -> clean -> deploy -> test |
| `logs [file]` | View logs (all logs or specific file) |

## Examples

```bash
# Set up builder pod (first time only)
# Installs: remote script dependencies (rsync, openssh-server) + build dependencies
make -C scripts/remote setup

# Sync source code (incremental, fast)
make -C scripts/remote sync

# Deploy RisingWave cluster (rebuilds if needed)
make -C scripts/remote deploy

# Run tests (assumes cluster is running)
make -C scripts/remote test

# Full workflow: sync -> clean -> deploy -> test
make -C scripts/remote run

# Clean up RisingWave cluster
make -C scripts/remote clean

# Open shell to debug (uses kubectl exec)
make -C scripts/remote shell

# View all logs
make -C scripts/remote logs

# View specific log file
make -C scripts/remote logs meta
```

## How It Works

### Sync (rsync over SSH)
- Uses **kubectl port-forward** automatically to create an SSH tunnel
- Uses **rsync over SSH** for fast incremental syncing
- Port-forward is set up and torn down automatically
- SSH keys are automatically generated and configured for passwordless authentication (first time only)

### Deploy/Test/Commands (kubectl exec)
- Uses **kubectl exec** directly (no SSH needed)
- Simpler and more reliable for command execution
- `deploy` runs `./risedev d` which automatically rebuilds if needed

### Setup Phases

The `setup` command runs in two phases:

1. **Remote Script Infrastructure** - Sets up tools needed for the script:
   - Installs `rsync` and `openssh-server`
   - Configures and starts SSH server

2. **Build Environment** - Sets up tools needed for building:
   - Verifies Rust is available (prerequisite check)
   - Installs build dependencies (make, cmake, protobuf-compiler, etc.)

## Configuration

Edit `remote.sh` to change:
- `NAMESPACE` - K8s namespace (default: `devspace`)
- `LABEL` - Pod label selector (default: `app=risingwave-builder`)
- `WORKSPACE` - Workspace path in pod (default: `/root/workspace`)
- `SSH_PORT` - Local port for kubectl port-forward (default: `2222`)
- `SSH_USER` - SSH user (default: `root`)

**Note:** You don't need to declare port 22 in your pod's `containerPorts` - `kubectl port-forward` works directly with any listening port in the container. However, your container **must have SSH server running** and listening on port 22 (set up by the `setup` command).

If port `2222` is already in use, you can change it:
```bash
export SSH_PORT="2223"
./remote.sh sync
```
