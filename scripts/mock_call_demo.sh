#!/usr/bin/env bash
# Run one loopback call without a SIP adapter.
#
# Starts voipswitchd in a temporary directory, creates a demo domain with two
# extensions, runs vs_mock_adapterd which registers both endpoints, places one
# inbound call and hangs up, then prints the resulting CDR.
#
# The mock adapter speaks the runtime IPC protocol only. It sends no SIP and no
# RTP, so media counters are expected to be zero.
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin_dir=${BIN_DIR:-$root/target/debug}

if [[ ! -x "$bin_dir/voipswitchd" || ! -x "$bin_dir/vs_mock_adapterd" ]]; then
  echo "==> building workspace"
  cargo build --manifest-path "$root/Cargo.toml" --workspace
fi

tmp=$(mktemp -d)
sock_dir="$tmp/sock"
mkdir -p "$sock_dir"
core_pid=""

cleanup() {
  if [[ -n "$core_pid" ]]; then
    kill "$core_pid" 2>/dev/null || true
    wait "$core_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

echo "==> starting voipswitchd (data: $tmp/data)"
"$bin_dir/voipswitchd" \
  --data-dir "$tmp/data" \
  --control-socket "$sock_dir/control.sock" \
  --runtime-socket "$sock_dir/runtime.sock" \
  >"$tmp/core.log" 2>&1 &
core_pid=$!

for _ in $(seq 1 100); do
  [[ -S "$sock_dir/control.sock" && -S "$sock_dir/runtime.sock" ]] && break
  sleep 0.1
done
if [[ ! -S "$sock_dir/runtime.sock" ]]; then
  echo "voipswitchd did not start:" >&2
  cat "$tmp/core.log" >&2
  exit 1
fi

cli=("$bin_dir/vs_cli" --socket "$sock_dir/control.sock" -x)

echo "==> configuring demo domain and extensions"
"${cli[@]}" "vc config insert domain domain_id=demo name=demo realm=demo.local password=demo-pass enabled=true" >/dev/null
"${cli[@]}" "vc config insert ext --domain demo number=1001 auth_user=1001 password=1001 enabled=true" >/dev/null
"${cli[@]}" "vc config insert ext --domain demo number=1002 auth_user=1002 password=1002 enabled=true" >/dev/null

echo "==> placing loopback call 1001 -> 1002"
"$bin_dir/vs_mock_adapterd" \
  --runtime-socket "$sock_dir/runtime.sock" \
  --auto-call 1001=1002 \
  --call-hold-ms "${CALL_HOLD_MS:-1200}" \
  --timeout-ms "${CALL_TIMEOUT_MS:-15000}"

echo "==> CDR"
"${cli[@]}" "show cdr --domain demo"
