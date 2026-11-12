#!/usr/bin/env bash
# Copies the runner's sources into two different temporary directories, builds each with
# scripts/build-runner.sh under a different job count (1, then CARGO_BUILD_JOBS or 2 if that is
# unset or 1), and fails unless the two runners are byte-equal: neither the checkout's path nor
# build-runner.sh's --jobs may reach the bytes.
#
#   scripts/check-runner-reproducible.sh [ROOT]
#
# The runner's sources are runner/ (without its target; it holds the shared runner/control) and
# rust-toolchain.toml: everything its build reads from the checkout.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "${1:-$here/..}" && pwd -P)
# The same refusal as build-runner.sh, for the checkout itself and the cargo home, before copying:
# a config file here would not reach the copies, so both would agree and prove nothing.
# shellcheck source=scripts/runner-cargo-config.sh
source "$here/runner-cargo-config.sh"
refuse_cargo_config "$root/runner"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/runner-reproducible.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

second_jobs=${CARGO_BUILD_JOBS:-2}
[[ "$second_jobs" =~ ^[1-9][0-9]*$ ]] ||
  { echo "CARGO_BUILD_JOBS must be a positive number, not $second_jobs" >&2; exit 1; }
[[ "$second_jobs" != 1 ]] || second_jobs=2
declare -A jobs=([first]=1 [second-copy-at-another-depth/nested]=$second_jobs)

hashes=()
for copy in first second-copy-at-another-depth/nested; do
  tree="$scratch/$copy"
  mkdir -p "$tree"
  tar -C "$root" --exclude=./runner/target -cf - ./runner ./rust-toolchain.toml |
    tar -C "$tree" -xf -
  hash=$(CARGO_BUILD_JOBS=${jobs[$copy]} "$here/build-runner.sh" "$tree")
  echo "$tree (--jobs ${jobs[$copy]}): $hash"
  hashes+=("$hash")
done

if [[ "${hashes[0]}" != "${hashes[1]}" ]]; then
  cmp "$scratch/first/runner/target/release/patchweft-runner" \
    "$scratch/second-copy-at-another-depth/nested/runner/target/release/patchweft-runner" || true
  echo "runner builds differ: not reproducible" >&2
  exit 1
fi
echo "reproducible: ${hashes[0]}"
