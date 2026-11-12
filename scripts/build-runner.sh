#!/usr/bin/env bash
# Builds the runner (`patchweft-runner`) alone and prints its sha256: the one command a release
# builds it with, and the one scripts/check-runner-reproducible.sh checks.
#
#   scripts/build-runner.sh [ROOT]
#
# ROOT is a checkout (default: this script's). The runner is its own cargo workspace with its own
# lockfile (runner/Cargo.lock), built in release with the toolchain rust-toolchain.toml pins, and
# with the checkout and the cargo home remapped, so its bytes name neither path. A rebuild of
# unchanged runner sources gives the same bytes, and so the same profile ids.
# The binary is at ROOT/runner/target/release/patchweft-runner.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "${1:-$here/..}" && pwd -P)
cargo_home=$(cd "${CARGO_HOME:-$HOME/.cargo}" && pwd -P)
toolchain=$(sed -n 's/^channel *= *"\(.*\)"$/\1/p' "$root/rust-toolchain.toml")
[[ -n "$toolchain" ]] || { echo "no toolchain channel in $root/rust-toolchain.toml" >&2; exit 1; }
target="$root/runner/target"

# Nothing from the caller's environment reaches the compiler: only these flags.
unset CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR \
  RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOCFLAGS \
  CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_STRIP \
  CARGO_PROFILE_RELEASE_OPT_LEVEL CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_RELEASE_PANIC
# Panic locations name source files; remapped, they name neither the checkout nor the cargo home.
# Cargo leaves remap flags out of its metadata hashes, and hashes the shared runner/control by its
# path inside this workspace, so nothing path-dependent reaches the bytes.
export RUSTFLAGS="--remap-path-prefix=$root=/patchweft --remap-path-prefix=$cargo_home=/cargo"
export CARGO_INCREMENTAL=0

cargo "+$toolchain" build --release --offline --locked --quiet \
  --manifest-path "$root/runner/Cargo.toml" --target-dir "$target"
sha256sum "$target/release/patchweft-runner" | cut -d' ' -f1
