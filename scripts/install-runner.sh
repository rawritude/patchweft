#!/usr/bin/env bash
# Builds the runner with scripts/build-runner.sh and installs it beside a patchweft binary, where
# that binary looks for it:
#
#   scripts/install-runner.sh DIR    # DIR holds patchweft; installs DIR/runner/<sha256>/patchweft-runner
#
# DIR/runner holds only that one runner afterwards: a patchweft binary refuses a directory with
# more than one. Prints the installed runner's path.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
[[ $# -eq 1 ]] || { echo "usage: $0 DIR" >&2; exit 2; }
dir=$(cd "$1" && pwd -P)
sha=$("$here/build-runner.sh")
built="$here/../runner/target/release/patchweft-runner"
installed="$dir/runner/$sha/patchweft-runner"
if [[ "$(ls -A "$dir/runner" 2>/dev/null)" != "$sha" ]] ||
  [[ "$(sha256sum "$installed" 2>/dev/null | cut -d' ' -f1)" != "$sha" ]]; then
  staging=$(mktemp -d "$dir/.runner.XXXXXX")
  install -D -m 755 "$built" "$staging/$sha/patchweft-runner"
  [[ "$(sha256sum "$staging/$sha/patchweft-runner" | cut -d' ' -f1)" == "$sha" ]]
  rm -rf "$dir/runner"
  mv "$staging" "$dir/runner"
fi
echo "$installed"
