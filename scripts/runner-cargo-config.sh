# Sourced by scripts/build-runner.sh and scripts/check-runner-reproducible.sh, never run.
#
# refuse_cargo_config DIR: fails, naming the file, when any cargo config file could reach a build
# of the runner from DIR: `.cargo/config.toml` or `.cargo/config` in DIR or any of its ancestors,
# or `config.toml` or `config` in the cargo home ($CARGO_HOME, default ~/.cargo). Cargo reads all
# of them, and any could change the runner's bytes (rustflags, a linker, a profile override), so
# the build would no longer be the one a release names.
refuse_cargo_config() {
  local dir file cargo_home
  dir=$(cd "$1" && pwd -P)
  cargo_home=${CARGO_HOME:-$HOME/.cargo}
  local candidates=("$cargo_home/config.toml" "$cargo_home/config")
  while :; do
    candidates+=("$dir/.cargo/config.toml" "$dir/.cargo/config")
    [[ "$dir" == / ]] && break
    dir=$(dirname "$dir")
  done
  for file in "${candidates[@]}"; do
    if [[ -e "$file" || -L "$file" ]]; then
      echo "refusing to build the runner: cargo config file $file would reach its build;" \
        "move it aside (the runner is built from its sources, its Cargo.toml and these scripts alone)" >&2
      return 1
    fi
  done
}
