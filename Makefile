# Lark — top-level Makefile.
#
# `make help` lists the common targets. Rust work runs inside the dev
# container because Glommio needs Linux/io_uring; Go cross-compiles
# natively on macOS so no container is needed for the edge build.

PROJECT     := lark
DEV_IMAGE   ?= lark-dev:latest          # Native arch — fast tests on Apple Silicon.
BUILD_IMAGE ?= lark-builder:latest      # Forced linux/amd64 — produces deploy-ready binaries.

# Cargo registry/git caches are arch-agnostic and shared between dev and builder.
# Target dir is per-arch (mixed-arch artifacts would force rebuilds on every switch).
CARGO_CACHE_FLAGS := \
	-v $(PROJECT)-cargo-registry:/root/.cargo/registry \
	-v $(PROJECT)-cargo-git:/root/.cargo/git

# Dev container — native host architecture. Used for `make check`, `make test`,
# `make shell`. On Apple Silicon this runs ARM64 natively (no qemu emulation).
DOCKER_RUN := docker run --rm \
	-v "$$PWD":/work \
	$(CARGO_CACHE_FLAGS) \
	-v $(PROJECT)-cargo-target:/work/target \
	--security-opt seccomp=unconfined \
	-w /work \
	$(DEV_IMAGE)

DOCKER_RUN_IT := docker run --rm -it \
	-v "$$PWD":/work \
	$(CARGO_CACHE_FLAGS) \
	-v $(PROJECT)-cargo-target:/work/target \
	--security-opt seccomp=unconfined \
	-w /work \
	$(DEV_IMAGE)

# Build container — always linux/amd64 so the binary runs on prod x86-64 hosts.
# Separate target volume from DOCKER_RUN so dev/builder don't trash each other's
# artifacts. Slower on Apple Silicon (qemu-user emulation) but only used for
# release builds.
DOCKER_BUILD_RUN := docker run --rm --platform linux/amd64 \
	-v "$$PWD":/work \
	$(CARGO_CACHE_FLAGS) \
	-v $(PROJECT)-cargo-target-amd64:/work/target \
	--security-opt seccomp=unconfined \
	-w /work \
	$(BUILD_IMAGE)

.PHONY: help
help:
	@echo "Common targets:"
	@echo "  make dev-image    Build the dev container (native arch — fast tests)."
	@echo "  make build-image  Build the release builder image (forced linux/amd64)."
	@echo "  make shell        Open a shell in the dev container."
	@echo ""
	@echo "  make check       cargo check --workspace (in Linux container)."
	@echo "  make test        cargo test --lib (the common case)."
	@echo "  make test-all    Full integration suite (./test-everything.sh) + go test ./... (edge)."
	@echo "  make fmt         cargo fmt --all + go fmt ./... (edge)."
	@echo "  make lint        cargo clippy with -D warnings."
	@echo ""
	@echo "  make build         Release binaries for lark-server + lark-edge."
	@echo "  make build-server  Just lark-server (Linux/amd64)."
	@echo "  make build-edge    Just lark-edge (Linux/amd64; also builds the SPA)."
	@echo "  make build-spa     Just the dashboard SPA (Vite, into edge/dashboard/dist)."
	@echo ""
	@echo "  make up          docker compose up --build (compiles from source)."
	@echo "  make up-release  docker compose up using prebuilt GHCR images (fast; no toolchain)."
	@echo "  make pull-release  Pull the latest published images without starting them."
	@echo "  make down        docker compose down."
	@echo "  make reset       docker compose down -v (also drops data volumes — wipes SQLite + per-DB blobs)."
	@echo "  make logs        docker compose logs -f."
	@echo ""
	@echo "  make clean       Drop cargo target volumes + local build outputs."

# ---------------------------------------------------------------------------
# Dev container
# ---------------------------------------------------------------------------

.PHONY: dev-image
dev-image:
	docker build -t $(DEV_IMAGE) -f Dockerfile.dev .

# Build image — same Dockerfile, but forced to linux/amd64 so the resulting
# image (and any binaries built inside it) target x86-64 even on ARM hosts.
.PHONY: build-image
build-image:
	docker build --platform linux/amd64 -t $(BUILD_IMAGE) -f Dockerfile.dev .

.PHONY: shell
shell: dev-image
	$(DOCKER_RUN_IT) /bin/bash

# ---------------------------------------------------------------------------
# Rust workflow (always in the Linux container)
# ---------------------------------------------------------------------------

.PHONY: check
check: dev-image
	$(DOCKER_RUN) cargo check --workspace

.PHONY: test
test: dev-image
	$(DOCKER_RUN) cargo test --lib

.PHONY: test-all
test-all: dev-image
	$(DOCKER_RUN) ./test-everything.sh
	cd edge && go test ./...

.PHONY: fmt
fmt: dev-image
	$(DOCKER_RUN) cargo fmt --all
	cd edge && go fmt ./...

.PHONY: lint
lint: dev-image
	$(DOCKER_RUN) cargo clippy --workspace --all-targets -- -D warnings

# ---------------------------------------------------------------------------
# Release builds
# ---------------------------------------------------------------------------

.PHONY: build
build: build-server build-edge

.PHONY: build-server
build-server: build-image
	# Build inside the linux/amd64 builder container, then copy the
	# binary onto the bind-mounted source dir so it's visible on the
	# host. `target/` lives in a docker volume, so the cp has to run
	# inside the container.
	$(DOCKER_BUILD_RUN) bash -c "cargo build --release -p lark-server && cp target/release/lark-server /work/lark-server"

.PHONY: build-spa
build-spa:
	cd edge/dashboard && npm ci --no-audit --no-fund && npm run build

.PHONY: build-edge
build-edge: build-spa
	cd edge && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
		go build -ldflags="-s -w" -o ../lark-edge .

# ---------------------------------------------------------------------------
# OSS compose stack
# ---------------------------------------------------------------------------

.PHONY: up
up: .env
	docker compose up --build

# First `make up` writes a .env with a unique, strong SERVER_SECRET so the stack
# never boots with the publicly-known compose default (security audit H-1). An
# existing .env is left untouched.
.env:
	@command -v openssl >/dev/null 2>&1 || { echo "openssl not found: create .env with SERVER_SECRET set to a 32+ byte random value (e.g. from another generator)"; exit 1; }
	@printf 'SERVER_SECRET=%s\n' "$$(openssl rand -hex 32)" > .env
	@echo "Generated .env with a random SERVER_SECRET."

# Run the stack from prebuilt GHCR images (docker-compose.prod.yml) instead of
# compiling. Reuses the same .env / SERVER_SECRET generation as `make up`.
.PHONY: up-release
up-release: .env
	docker compose -f docker-compose.prod.yml up

.PHONY: pull-release
pull-release:
	docker compose -f docker-compose.prod.yml pull

.PHONY: down
down:
	docker compose down

.PHONY: reset
reset:
	docker compose down -v

.PHONY: logs
logs:
	docker compose logs -f

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

.PHONY: clean
clean:
	-docker volume rm $(PROJECT)-cargo-target 2>/dev/null
	-docker volume rm $(PROJECT)-cargo-target-amd64 2>/dev/null
	-docker volume rm $(PROJECT)-cargo-registry 2>/dev/null
	-docker volume rm $(PROJECT)-cargo-git 2>/dev/null
	rm -f ./lark-server ./lark-edge
