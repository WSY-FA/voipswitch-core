#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
for scenario in normal reject cancel-late-answer disconnect; do
  echo "==> loopback scenario: $scenario"
  bash "$root/scripts/mock_call_demo.sh" "$scenario"
done
