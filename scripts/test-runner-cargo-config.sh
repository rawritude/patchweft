#!/usr/bin/env bash
# Checks that scripts/build-runner.sh and scripts/check-runner-reproducible.sh refuse to build
# when a cargo config file could reach the runner's build, and name it; and that build-runner.sh
# hands cargo none of the caller's environment and prints the hash of what cargo just built, never
# a stale binary. Builds nothing: the config cases are refused before cargo runs, and the
# environment cases run a stand-in for cargo first on PATH.
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

# A stand-in for cargo: it records the environment and arguments it was given, and puts a new
# "runner" where real cargo would, under CARGO_BUILD_TARGET's triple when that reaches it, and
# reports it as cargo's JSON messages do.
mkdir -p "$scratch/bin"
cat >"$scratch/bin/cargo" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
here=$(dirname "$0")
env >"$here/env.log"
printf '%s\n' "$@" >"$here/args.log"
target=
while [[ $# -gt 0 ]]; do
  [[ "$1" == --target-dir ]] && target=$2
  shift
done
dir="$target/release"
[[ -n "${CARGO_BUILD_TARGET:-}" ]] && dir="$target/$CARGO_BUILD_TARGET/release"
mkdir -p "$dir"
echo "built $(date +%s%N) $$" >"$dir/patchweft-runner"
echo '{"reason":"compiler-artifact","executable":null}'
echo "{\"reason\":\"compiler-artifact\",\"executable\":\"$dir/patchweft-runner\"}"
FAKE
chmod +x "$scratch/bin/cargo"
binary="$tree/runner/target/release/patchweft-runner"

fail() {
  echo "FAIL $1" >&2
  failures=$((failures + 1))
}

# The environment the stale-runner case was found with, and others that change the bytes.
mkdir -p "$(dirname "$binary")"
echo stale >"$binary"
stale=$(sha256sum "$binary" | cut -d' ' -f1)
status=0
printed=$(PATH="$scratch/bin:$PATH" CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu \
  CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/bin/false \
  RUSTC_WRAPPER=/bin/false CARGO_ENCODED_RUSTFLAGS=-Cdebuginfo=2 CARGO_BUILD_JOBS=2 \
  "$here/build-runner.sh" "$tree" 2>&1) || status=$?
leaked=$(grep -E '^(CARGO_BUILD_TARGET|CARGO_PROFILE_|CARGO_TARGET_|RUSTC_WRAPPER|CARGO_ENCODED_RUSTFLAGS|CARGO_BUILD_JOBS)' \
  "$scratch/bin/env.log" || true)
if [[ $status -ne 0 ]]; then
  fail "environment: build-runner.sh failed ($status): $printed"
elif [[ -n "$leaked" ]]; then
  fail "environment: reached cargo: $leaked"
elif [[ "$printed" != "$(sha256sum "$binary" | cut -d' ' -f1)" || "$printed" == "$stale" ]]; then
  fail "environment: printed $printed, not the hash of what was just built (stale $stale)"
elif ! grep -qx -- --jobs "$scratch/bin/args.log" || ! grep -qx 2 "$scratch/bin/args.log"; then
  fail "environment: CARGO_BUILD_JOBS was not passed on as --jobs"
elif ! grep -q '^RUSTFLAGS=--remap-path-prefix=' "$scratch/bin/env.log"; then
  fail "environment: the remap flags did not reach cargo"
else
  echo "ok   environment: none of it reaches cargo, and the new build's hash is printed"
fi

# A build that lands anywhere else is refused, whatever sits at the expected path.
echo stale >"$binary"
cat >"$scratch/bin/cargo" <<'FAKE'
#!/usr/bin/env bash
target=
while [[ $# -gt 0 ]]; do
  [[ "$1" == --target-dir ]] && target=$2
  shift
done
mkdir -p "$target/elsewhere/release"
echo built >"$target/elsewhere/release/patchweft-runner"
echo "{\"reason\":\"compiler-artifact\",\"executable\":\"$target/elsewhere/release/patchweft-runner\"}"
FAKE
status=0
printed=$(PATH="$scratch/bin:$PATH" "$here/build-runner.sh" "$tree" 2>&1) || status=$?
if [[ $status -eq 0 ]] || ! grep -qF "not $binary" <<<"$printed"; then
  fail "elsewhere: exit $status, printed $printed"
else
  echo "ok   elsewhere: a build that is not the runner's path prints no hash"
fi

[[ $failures -eq 0 ]] || { echo "$failures failed" >&2; exit 1; }
echo "all refused"
