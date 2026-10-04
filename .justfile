set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

export CARGO_TARGET_DIR := env_var_or_default("CARGO_TARGET_DIR", "target/dev")

default: help

help:
	@just --list

build:
	cargo build -p rbitcoin-node -p rbitcoin-cli

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	RUST_TEST_THREADS=1 cargo test --workspace

check: fmt-check clippy test

smoke:
	./scripts/ci-os-smoke.sh

coverage:
	./scripts/coverage.sh

deny:
	cargo deny check

ast-grep:
	./scripts/ast-grep.sh

core-init:
	./scripts/core-functional/init-submodule.sh

core-run:
	./scripts/core-functional/init-submodule.sh
	./scripts/core-functional/run.sh

core-list:
	./scripts/core-functional/run.sh --list

core-dry-run:
	./scripts/core-functional/run.sh --dry-run

core-nightly:
	./scripts/core-functional/nightly.sh

xcode-rust:
	cd xcode && make rust

xcode-rust-test:
	cd xcode && make rust
	cd xcode/rustylib && cargo test --lib

xcode-build:
	cd xcode && make rust
	cd xcode && IOS_DESTINATION='generic/platform=iOS' make app

xcode-check: xcode-rust-test xcode-build

pr-checks:
	./scripts/pr-checks-watch.sh --pr "${PR:?set PR=<number>}"

release-test:
	./scripts/release.test.sh

release-gate:
	./scripts/release-gate.sh

clean:
	cargo clean