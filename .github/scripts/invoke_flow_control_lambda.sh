#!/usr/bin/env bash

set -euo pipefail

FLOW_CONTROL_ARN="${1:-}"
INTERVAL_SECONDS="${2:-300}"

if [[ -z "$FLOW_CONTROL_ARN" ]]; then
  echo "Usage: $0 <flow-control-lambda-arn> [interval-seconds]" >&2
  exit 1
fi

if ! [[ "$INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$INTERVAL_SECONDS" -lt 1 ]]; then
  echo "Interval must be a positive integer, got: $INTERVAL_SECONDS" >&2
  exit 1
fi

while true; do
  aws lambda invoke \
    --function-name "$FLOW_CONTROL_ARN" \
    --invocation-type Event \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    /tmp/flow-control-lambda-invoke-response.json >/dev/null

  sleep "$INTERVAL_SECONDS"
done
