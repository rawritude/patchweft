#!/usr/bin/env bash
# Copies the runner's sources into two different temporary directories, builds each with
# scripts/build-runner.sh, and fails unless the two runners are byte-equal.
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

hashes=()
for copy in first second-copy-at-another-depth/nested; do
  tree="$scratch/$copy"
  mkdir -p "$tree"
  tar -C "$root" --exclude=./runner/target -cf - ./runner ./rust-toolchain.toml |
    tar -C "$tree" -xf -
  hash=$("$here/build-runner.sh" "$tree")
  echo "$tree: $hash"
  hashes+=("$hash")
done

if [[ "${hashes[0]}" != "${hashes[1]}" ]]; then
  cmp "$scratch/first/runner/target/release/patchweft-runner" \
    "$scratch/second-copy-at-another-depth/nested/runner/target/release/patchweft-runner" || true
  echo "runner builds differ: not reproducible" >&2
  exit 1
fi
echo "reproducible: ${hashes[0]}"
