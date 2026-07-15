# syntax=docker/dockerfile:1

# =============================================================================
# Stage 0: base — lightweight runtime dependencies.
# Shared by both the build chain and the final runtime image.
# =============================================================================
FROM ubuntu:24.04 AS base

ENV LANG=en_US.utf8

RUN apt-get update \
  && apt-get -y install ca-certificates libsasl2-dev openjdk-21-jdk \
    python3.12 python3.12-dev openssl pkg-config curl git \
    libblas-dev liblapack-dev libomp-dev

# =============================================================================
# Stage 1: chef — build toolchain on top of base.
# This layer is registry-cached and only invalidates when rust-toolchain,
# Node version, or system packages change.
# =============================================================================
FROM base AS chef

RUN apt-get update \
  && apt-get -y install build-essential software-properties-common \
    make cmake protobuf-compiler bash lld unzip rsync clang mold

# Install Node.js as dependency for building the dashboard.
# Bump version together with `dashboard/.node-version`.
ENV NVM_DIR=/root/.nvm
ENV NODE_VERSION=20.11.1
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash \
  && . $NVM_DIR/nvm.sh \
  && nvm install $NODE_VERSION
ENV PATH=$NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH=/root/.cargo/bin:$PATH
ENV CARGO_INCREMENTAL=0

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup toolchain install --profile minimal \
  && rustup show \
  && rustup component add rustfmt \
  && rustup target add wasm32-wasip1

# Install pre-built binaries via cargo-binstall (seconds, not minutes)
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
    https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall -y --locked cargo-chef@0.1.77 flamegraph
# addr2line needs --features bin, not supported by binstall
RUN cargo install addr2line --features bin --bin addr2line

# mold linker for 10-50× faster linking in all subsequent cargo invocations
ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"

# =============================================================================
# Stage 2: planner — compute recipe.json from all Cargo manifests.
# This runs on every build but is fast (<30s). Its output drives layer caching.
# =============================================================================
FROM chef AS planner
WORKDIR /risingwave
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 3: builder — compile Rust dependencies (registry-cacheable) then app.
#
# Two-phase profile strategy:
#   Phase 1 (cook):  speed-optimized — LTO disabled, max parallel codegen, opt-level 2
#   Phase 2 (build): quality-optimized — inherits Cargo.toml [profile.${CARGO_PROFILE}]
#
# On ~90% of commits (no Cargo.toml/lock changes), the cook layer hits the
# registry cache (`--cache-from type=registry` in docker.sh), skipping the
# 25–30 minute dependency compilation entirely.
# =============================================================================
FROM chef AS builder

ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

ARG CARGO_PROFILE
ENV CARGO_PROFILE=$CARGO_PROFILE

COPY --from=planner /risingwave/recipe.json recipe.json

ENV OPENSSL_STATIC=1

# Phase 1: cook dependencies — speed-optimized profile via inline env vars.
# Scoped to this RUN only so Phase 2 inherits clean Cargo.toml defaults.
RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/root/.cargo/git,sharing=locked \
    CARGO_PROFILE_PRODUCTION_LTO=off \
    CARGO_PROFILE_PRODUCTION_CODEGEN_UNITS=256 \
    CARGO_PROFILE_PRODUCTION_OPT_LEVEL=2 \
    cargo chef cook --profile ${CARGO_PROFILE} --recipe-path recipe.json

# Phase 2: build application — Cargo.toml profile defaults for binary quality.
# No env overrides active, so [profile.production] / [profile.patch-production]
# settings (including lto) take effect.

COPY ./ /risingwave
WORKDIR /risingwave

ENV ENABLE_BUILD_DASHBOARD=1

RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/root/.cargo/git,sharing=locked \
    cargo build -p risingwave_cmd_all --profile ${CARGO_PROFILE} --features "rw-static-link" --features udf --features datafusion && \
    mkdir -p /risingwave/bin && \
    mv /risingwave/target/${CARGO_PROFILE}/risingwave /risingwave/bin/ && \
    mv /risingwave/target/${CARGO_PROFILE}/risingwave.dwp /risingwave/bin/ && \
    cp ./target/${CARGO_PROFILE}/build/tikv-jemalloc-sys-*/out/build/bin/jeprof /risingwave/bin/ && \
    chmod +x /risingwave/bin/jeprof && \
    mkdir -p /risingwave/lib/adbc && \
    DEST_DIR="/risingwave/lib/adbc" TMP_DIR="/tmp" bash ./src/risedevtool/scripts/download_adbc_snowflake_driver.sh

# =============================================================================
# Stage 4: java-planner — isolate Maven poms for dependency caching
# =============================================================================
FROM base AS java-planner

RUN mkdir -p /risingwave
WORKDIR /risingwave

COPY java /risingwave/java/

# Move java/**/pom.xml to poms/**/pom.xml
RUN find . -name pom.xml -exec bash -c 'mkdir -p poms/$(dirname {}); mv {} poms/{}' \;

# =============================================================================
# Stage 5: java-builder — compile Java connector node.
# Uses FROM chef because rust-maven-plugin needs the Rust toolchain.
# =============================================================================
FROM chef AS java-builder

RUN apt-get update && apt-get -y install maven

RUN mkdir -p /risingwave
WORKDIR /risingwave/java

# 1. copy only poms
COPY --from=java-planner /risingwave/poms /risingwave/java/

# 2. start downloading dependencies
RUN mvn dependency:go-offline --fail-never

# 3. add all source code and start compiling
# TODO: only add java related code so that changing rust code won't recompile java code
# Currently java-binding depends on the workspace Cargo.toml, which depends on the whole rust codebase
# Besides, rust-maven-plugin sets --target-dir, so the dependencies are built twice. How to dedup?
COPY ./ /risingwave

RUN mvn -B package -Dmaven.test.skip=true -Dno-build-rust && \
  mkdir -p /risingwave/bin/connector-node && \
  tar -zxvf /risingwave/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz -C /risingwave/bin/connector-node

# =============================================================================
# Stage 6: risingwave — production runtime image.
# FROM base avoids pulling build toolchain (cmake, rust, node, etc.) into the
# final image.
# =============================================================================
FROM base AS risingwave

RUN apt-get update && apt-get -y install linux-tools-generic \
  && ln -s "$(find /usr/lib/linux-tools/*/perf | head -1)" /usr/local/bin/perf

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install gdb libpam-krb5 krb5-user telnet kafkacat less rustup \
  && rm -rf /var/lib/{apt,dpkg,cache,log}/

# Set default Rust toolchain but don't install it to keep the image size small
# The toolchain will be installed when it is first used
# Do not use `rustup default stable` because it will install immediately
RUN rustup show && sed -i '1s/^/default_toolchain = "stable"\n/' ~/.rustup/settings.toml

RUN mkdir -p /risingwave/bin/connector-node && mkdir -p /risingwave/lib

COPY --from=builder /risingwave/bin/risingwave /risingwave/bin/risingwave
COPY --from=builder /risingwave/bin/risingwave.dwp /risingwave/bin/risingwave.dwp
COPY --from=java-builder /risingwave/bin/connector-node /risingwave/bin/connector-node
COPY --from=builder /risingwave/bin/jeprof /usr/local/bin/jeprof
COPY --from=chef /root/.cargo/bin/flamegraph /usr/local/bin/flamegraph
COPY --from=chef /root/.cargo/bin/addr2line /usr/local/bin/addr2line

COPY --from=builder /risingwave/lib/adbc /risingwave/lib/adbc

# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE=docker-playground
# Set default connector libs path
ENV CONNECTOR_LIBS_PATH=/risingwave/bin/connector-node/libs
ENV IN_CONTAINER=1

# Runtime dynamic library search path for ADBC (Linux containers use LD_LIBRARY_PATH).
ENV ADBC_DRIVER_PATH=/risingwave/lib/adbc
ENV LD_LIBRARY_PATH=/risingwave/lib/adbc:/usr/local/lib

ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "single_node" ]
