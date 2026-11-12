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
#
# Nor does the caller's environment: cargo runs under `env -i` with PATH, HOME, CARGO_HOME and
# RUSTUP_HOME alone, so no CARGO_BUILD_TARGET, CARGO_PROFILE_RELEASE_*, CARGO_TARGET_<triple>_*,
# RUSTC_WRAPPER or the like can change the bytes or where cargo puts them. CARGO_BUILD_JOBS is
# passed on as --jobs: parallelism never changes the bytes of one codegen unit under full LTO.
# The hash printed is of the executable cargo reports it built, which must be the one above.
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

# Nothing else from the caller's environment reaches cargo.
clean=(env -i "PATH=$PATH" "HOME=${HOME:?}" "CARGO_HOME=$cargo_home")
if [[ -n "${RUSTUP_HOME:-}" ]]; then
  clean+=("RUSTUP_HOME=$RUSTUP_HOME")
fi
# Panic locations name source files; remapped, they name neither the checkout nor the cargo home.
# Cargo leaves remap flags out of its metadata hashes, and hashes the shared runner/control by its
# path inside this workspace, so nothing path-dependent reaches the bytes.
clean+=("RUSTFLAGS=--remap-path-prefix=$root=/patchweft --remap-path-prefix=$cargo_home=/cargo"
  "CARGO_INCREMENTAL=0")
jobs=()
if [[ -n "${CARGO_BUILD_JOBS:-}" ]]; then
  [[ "$CARGO_BUILD_JOBS" =~ ^[1-9][0-9]*$ ]] ||
    { echo "CARGO_BUILD_JOBS must be a positive number, not $CARGO_BUILD_JOBS" >&2; exit 1; }
  jobs=(--jobs "$CARGO_BUILD_JOBS")
fi

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
built=$("${clean[@]}" cargo "+$toolchain" build --release --offline --locked --quiet \
  --message-format=json-render-diagnostics "${jobs[@]}" "${profile[@]}" \
  --manifest-path "$root/runner/Cargo.toml" --target-dir "$target" |
  sed -n 's/.*"executable":"\([^"]*\)".*/\1/p')
binary="$target/release/patchweft-runner"
if [[ "$built" != "$binary" ]]; then
  echo "cargo built ${built:-no executable}, not $binary: refusing to print a hash of" \
    "anything but what it just built" >&2
  exit 1
fi
sha256sum "$binary" | cut -d' ' -f1
