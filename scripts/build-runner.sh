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
#
# No cargo config file may reach the build (scripts/runner-cargo-config.sh): it refuses, naming
# the file, when one is in the runner's directory or an ancestor, or in the cargo home. The cargo
# home itself stays, since an offline build reads its registry. The release profile is passed
# explicitly too, matching runner/Cargo.toml, so nothing else can change it.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "${1:-$here/..}" && pwd -P)
# shellcheck source=scripts/runner-cargo-config.sh
source "$here/runner-cargo-config.sh"
refuse_cargo_config "$root/runner"
cargo_home=$(cd "${CARGO_HOME:-$HOME/.cargo}" && pwd -P)
toolchain=$(sed -n 's/^channel *= *"\(.*\)"$/\1/p' "$root/rust-toolchain.toml")
[[ -n "$toolchain" ]] || { echo "no toolchain channel in $root/rust-toolchain.toml" >&2; exit 1; }
target="$root/runner/target"

# Nothing from the caller's environment reaches the compiler: only these flags.
unset CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR \
  RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOCFLAGS \
  CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_STRIP \
  CARGO_PROFILE_RELEASE_OPT_LEVEL CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_RELEASE_PANIC \
  CARGO_PROFILE_RELEASE_INCREMENTAL
# Panic locations name source files; remapped, they name neither the checkout nor the cargo home.
# Cargo leaves remap flags out of its metadata hashes, and hashes the shared runner/control by its
# path inside this workspace, so nothing path-dependent reaches the bytes.
export RUSTFLAGS="--remap-path-prefix=$root=/patchweft --remap-path-prefix=$cargo_home=/cargo"
export CARGO_INCREMENTAL=0

# runner/Cargo.toml's [profile.release] with cargo's defaults for what it leaves out, pinned.
profile=(
  --config 'profile.release.opt-level=3'
  --config 'profile.release.debug=false'
  --config 'profile.release.lto=true'
  --config 'profile.release.codegen-units=1'
  --config 'profile.release.panic="unwind"'
  --config 'profile.release.strip=true'
  --config 'profile.release.incremental=false'
)
# From the runner's own directory: cargo finds config files from its working directory, and the
# check above covered that directory's ancestors.
cd "$root/runner"
cargo "+$toolchain" build --release --offline --locked --quiet "${profile[@]}" \
  --manifest-path "$root/runner/Cargo.toml" --target-dir "$target"
sha256sum "$target/release/patchweft-runner" | cut -d' ' -f1
