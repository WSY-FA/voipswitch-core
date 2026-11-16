#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin_dir=$root/target/debug
mode=${1:-normal}
case "$mode" in normal|reject|cancel-late-answer|disconnect) ;; *) echo "unknown scenario: $mode" >&2; exit 2 ;; esac

# Always rebuild the executable targets so a stale cached demo binary cannot pass.
cargo build --manifest-path "$root/Cargo.toml" -p voipswitchd -p vs_cli -p vs_mock_adapterd

tmp=$(mktemp -d "${TMPDIR:-/tmp}/voipswitch-loopback-${mode}.XXXXXX")
sock_dir="$tmp/run/voipswitch"
mkdir -p "$sock_dir"
export XDG_RUNTIME_DIR="$tmp/run"
core_pid=
mock_pid=
keep=0
cleanup() {
  rc=$?
  [[ -n "$mock_pid" ]] && { kill "$mock_pid" 2>/dev/null || true; wait "$mock_pid" 2>/dev/null || true; }
  [[ -n "$core_pid" ]] && { kill "$core_pid" 2>/dev/null || true; wait "$core_pid" 2>/dev/null || true; }
  if (( rc != 0 )) || [[ ${KEEP_WORK_DIR:-0} == 1 ]]; then
    keep=1
    echo "loopback evidence: $tmp" >&2
    [[ -f "$tmp/core.log" ]] && tail -100 "$tmp/core.log" >&2 || true
    [[ -f "$tmp/mock.log" ]] && tail -100 "$tmp/mock.log" >&2 || true
  fi
  (( keep == 0 )) && rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT

"$bin_dir/voipswitchd" --data-dir "$tmp/data" --control-socket "$sock_dir/control.sock" \
  --runtime-socket "$sock_dir/runtime.sock" >"$tmp/core.log" 2>&1 &
core_pid=$!
for _ in $(seq 1 100); do
  [[ -S "$sock_dir/control.sock" && -S "$sock_dir/runtime.sock" ]] && break
  sleep .1
done
[[ -S "$sock_dir/runtime.sock" ]] || { echo "core startup timeout" >&2; exit 1; }
cli=("$bin_dir/vs_cli" --socket "$sock_dir/control.sock" --json -x)
run_cli() {
  local output
  output=$("${cli[@]}" "$1") || { echo "$output" >&2; return 1; }
  jq -e '.ok == true' <<<"$output" >/dev/null || { echo "$output" >&2; return 1; }
  printf '%s\n' "$output" >"$tmp/last-response.json"
  printf '%s\n' "$output"
}
run_cli "vc config insert domain domain_id=demo name=demo realm=demo.local password=demo-pass enabled=true" >/dev/null
run_cli "vc config insert ext --domain demo number=1001 auth_user=1001 password=1001 enabled=true" >/dev/null
run_cli "vc config insert ext --domain demo number=1002 auth_user=1002 password=1002 enabled=true" >/dev/null

mock_args=(--runtime-socket "$sock_dir/runtime.sock" --auto-call 1001=1002 --call-hold-ms "${CALL_HOLD_MS:-200}" --timeout-ms "${CALL_TIMEOUT_MS:-12000}")
[[ "$mode" == normal ]] || mock_args+=(--scenario "$mode")
"$bin_dir/vs_mock_adapterd" "${mock_args[@]}" >"$tmp/mock.log" 2>&1 &
mock_pid=$!
set +e
wait "$mock_pid"
mock_rc=$?
set -e
mock_pid=
[[ $mock_rc == 0 ]] || { echo "mock scenario exit=$mock_rc" >&2; exit 1; }
case "$mode" in
  cancel-late-answer)
    rg -q 'injected OutboundAnswered after CANCEL' "$tmp/mock.log" || { echo "late answer was not injected" >&2; exit 1; }
    rg -q 'late outbound answer received while call is terminating' "$tmp/core.log" || { echo "core did not process late answer" >&2; exit 1; }
    rg -q 'core requested cleanup of late answer' "$tmp/mock.log" || { echo "adapter did not receive late answer cleanup action" >&2; exit 1; }
    ;;
  disconnect)
    rg -q 'injected runtime disconnect after answer' "$tmp/mock.log" || { echo "runtime disconnect was not injected" >&2; exit 1; }
    rg -q 'runtime socket closed for disconnect scenario' "$tmp/mock.log" || { echo "runtime socket did not close" >&2; exit 1; }
    ;;
esac
if [[ "$mode" == disconnect ]]; then
  for _ in $(seq 1 50); do
    status=$(run_cli "show health-snapshot") || exit 1
    if jq -e '.result.data.payload.value.data.adapter_connection.clients == 0 and .result.data.payload.value.data.adapter_connection.ready == false' <<<"$status" >/dev/null; then break; fi
    sleep .1
  done
  jq -e '.result.data.payload.value.data.adapter_connection.clients == 0 and .result.data.payload.value.data.adapter_connection.ready == false' <<<"$status" >/dev/null || {
    echo "core still considers adapter connected" >&2; jq .result.data <<<"$status" >&2; exit 1;
  }
fi
if rg -q 'cleanup timeout forced removal' "$tmp/core.log"; then
  echo "core required forced cleanup timeout" >&2
  exit 1
fi

deadline=$((SECONDS + ${CDR_WAIT_SECONDS:-8}))
while :; do
  cdr=$(run_cli "show cdr --domain demo --page-size 20") || exit 1
  count=$(jq -er 'if .result.data.render == "object" and (.result.data.payload.value.data.records | type) == "array" then (.result.data.payload.value.data.records | length) else error("invalid CDR response") end' <<<"$cdr")
  [[ "$count" == 1 ]] && break
  (( SECONDS < deadline )) || { echo "expected exactly one CDR; observed $count" >&2; exit 1; }
  sleep .1
done
case "$mode" in
  normal)
    expected='.answered == true and .final_status == 200 and .hangup_cause == "normal_clearing"'
    ;;
  reject)
    expected='.answered == false and .final_status == 486 and .hangup_cause == "call_rejected"'
    ;;
  cancel-late-answer)
    expected='.answered == false and .final_status == null and .hangup_cause == "originator_cancel"'
    ;;
  disconnect)
    expected='(.answered == false and .final_status == 180 or .answered == true and .final_status == 200) and .hangup_cause == "internal_error"'
    ;;
esac
jq -e ".result.data.payload.value.data.records[0] | $expected" <<<"$cdr" >/dev/null || {
  echo "unexpected CDR outcome:" >&2; jq .result.data.payload.value.data.records <<<"$cdr" >&2; exit 1;
}
run_cli "show calls --domain demo" >"$tmp/calls.json"
jq -e '.result.data.render == "table" and (.result.data.payload.rows | type) == "array" and (.result.data.payload.rows | length) == 0' "$tmp/calls.json" >/dev/null || { cat "$tmp/calls.json" >&2; exit 1; }
run_cli "show sessions --domain demo" >"$tmp/sessions.json"
jq -e '.result.data.render == "table" and (.result.data.payload.rows | type) == "array" and (.result.data.payload.rows | length) == 0' "$tmp/sessions.json" >/dev/null || { cat "$tmp/sessions.json" >&2; exit 1; }
outcome=$(jq -r '.result.data.payload.value.data.records[0] | "answered=\(.answered) final_status=\(.final_status) hangup_cause=\(.hangup_cause)"' <<<"$cdr")
echo "PASS scenario=$mode cdr_count=$count $outcome calls=0 sessions=0"
