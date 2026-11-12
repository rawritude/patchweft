#!/usr/bin/env bash
# Checks that scripts/build-runner.sh and scripts/check-runner-reproducible.sh refuse to build
# when a cargo config file could reach the runner's build, and name it. Builds nothing: each case
# is refused before cargo runs.
#
#   scripts/test-runner-cargo-config.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/runner-cargo-config.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
failures=0

# A tree with the runner's layout; its sources are never read.
tree="$scratch/outer/tree"
mkdir -p "$tree/runner" "$scratch/home"
cp "$here/../rust-toolchain.toml" "$tree/"

expect_refused() {
  local name=$1 file=$2
  shift 2
  local output status=0
  output=$("$@" 2>&1) || status=$?
  if [[ $status -eq 0 ]] || ! grep -qF "refusing to build the runner: cargo config file $file" <<<"$output"; then
    echo "FAIL $name (exit $status): $output" >&2
    failures=$((failures + 1))
  elif [[ -e "$tree/runner/target" ]]; then
    echo "FAIL $name: a build started" >&2
    failures=$((failures + 1))
  else
    echo "ok   $name"
  fi
}

export CARGO_HOME="$scratch/home"
for config in "$tree/runner/.cargo/config.toml" "$tree/runner/.cargo/config" \
  "$tree/.cargo/config.toml" "$scratch/outer/.cargo/config"; do
  mkdir -p "$(dirname "$config")"
  echo '[build]' >"$config"
  expect_refused "build-runner.sh, $config" "$config" "$here/build-runner.sh" "$tree"
  expect_refused "check-runner-reproducible.sh, $config" "$config" \
    "$here/check-runner-reproducible.sh" "$tree"
  rm -f "$config"
done
for config in "$CARGO_HOME/config.toml" "$CARGO_HOME/config"; do
  echo '[build]' >"$config"
  expect_refused "build-runner.sh, cargo home $config" "$config" "$here/build-runner.sh" "$tree"
  rm -f "$config"
done

[[ $failures -eq 0 ]] || { echo "$failures failed" >&2; exit 1; }
echo "all refused"
