#!/usr/bin/env bash
set -euo pipefail

# Verify setup-node's result in the same shell environment used by build steps.
if [[ "$(node --version 2>/dev/null || true)" == v20.* ]]; then
  echo "Frontend Node: $(command -v node) ($(node --version))"
  exit 0
fi

node_version=20.20.2
case "$(uname -m)" in
  x86_64) node_arch=x64 ;;
  aarch64|arm64) node_arch=arm64 ;;
  *) echo "Unsupported Node architecture: $(uname -m)" >&2; exit 1 ;;
esac
archive="node-v${node_version}-linux-${node_arch}.tar.xz"
install_dir="${RUNNER_TEMP:?}/frontend-node-${node_version}"
mkdir -p "$install_dir"
downloaded=false
for base in "https://nodejs.org/dist/v${node_version}" "https://npmmirror.com/mirrors/node/v${node_version}"; do
  if curl --fail --location --retry 2 --connect-timeout 15 --max-time 180 "$base/SHASUMS256.txt" -o "$install_dir/SHASUMS256.txt" &&
     curl --fail --location --retry 2 --connect-timeout 15 --max-time 180 "$base/$archive" -o "$install_dir/$archive" &&
     (cd "$install_dir" && awk -v file="$archive" '$2 == file {print; found=1} END {if (!found) exit 1}' SHASUMS256.txt > archive.sha256 && sha256sum -c archive.sha256); then
    downloaded=true
    break
  fi
done
if [[ "$downloaded" != true ]]; then
  echo "Cannot download and verify Node ${node_version}" >&2
  exit 1
fi
tar -xJf "$install_dir/$archive" -C "$install_dir"
node_bin="$install_dir/node-v${node_version}-linux-${node_arch}/bin"
export PATH="$node_bin:$PATH"
hash -r
node --version
[[ "$(node --version)" == v20.* ]]
echo "$node_bin" >> "${GITHUB_PATH:?}"
echo "Frontend Node: $(command -v node) ($(node --version))"
