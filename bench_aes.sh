#!/usr/bin/env bash
#
# Benchmark the `bench_aes` WASM action via the `wsk` CLI.
#
# Invokes bench_aes N times with the given num_iterations / message_length
# parameters and prints avg / min / max / p95 / p99 for the invoker-recorded
# action duration (the same `duration` field surfaced by user-events as
# `action-duration`).
#
# Modes:
#   sequential (default) -- one blocking invoke at a time; each call's duration
#                           is read from the blocking-invoke response. Measures
#                           clean per-call action duration with no concurrency.
#   --concurrent         -- fires all N non-blocking invokes back-to-back, then
#                           polls CouchDB for each activation's duration.
#                           Measures action duration under burst load.
#
# Requires: wsk (configured), jq, curl.

set -euo pipefail

ACTION_NAME="bench_aes"
RUNS=100
NUM_ITERATIONS=100
MESSAGE_LENGTH=100
MODE="sequential"

COUCHDB_URL="${COUCHDB_URL:-http://whisk_admin:some_passw0rd@127.0.0.1:5984}"
ACTIVATIONS_DB="${ACTIVATIONS_DB:-whisk_local_activations}"
POLL_TIMEOUT_SECS=120
POLL_INTERVAL_SECS=1

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  -n, --runs N              Number of invocations             (default: $RUNS)
  -i, --num-iterations N    bench_aes num_iterations param    (default: $NUM_ITERATIONS)
  -m, --message-length N    bench_aes message_length param    (default: $MESSAGE_LENGTH)
  -a, --action NAME         Action name                        (default: $ACTION_NAME)
      --concurrent          Fire all N invokes concurrently and collect via CouchDB
                            (sequential blocking invokes is the default)
  -h, --help                Show this help
EOF
}

while (( $# > 0 )); do
  case "$1" in
    -n|--runs)              RUNS="$2"; shift 2 ;;
    -i|--num-iterations)    NUM_ITERATIONS="$2"; shift 2 ;;
    -m|--message-length)    MESSAGE_LENGTH="$2"; shift 2 ;;
    -a|--action)            ACTION_NAME="$2"; shift 2 ;;
        --concurrent)       MODE="concurrent"; shift ;;
    -h|--help)              usage; exit 0 ;;
    *)                      echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

for bin in wsk jq curl; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: required binary '$bin' not found in PATH" >&2
    exit 1
  fi
done

echo "=== Benchmarking ${ACTION_NAME} ==="
echo "  runs:            $RUNS"
echo "  num_iterations:  $NUM_ITERATIONS"
echo "  message_length:  $MESSAGE_LENGTH"
echo "  mode:            $MODE"
echo

DURATIONS_FILE="$(mktemp -t bench_aes_durations.XXXXXX)"
trap 'rm -f "$DURATIONS_FILE"' EXIT

fail_count=0
ok_count=0

invoke_blocking_and_record() {
  # Runs a single blocking invoke, extracts `.duration` from the activation
  # record printed to stdout. Returns 0 iff we got a numeric duration and
  # statusCode 0.
  local out json duration status
  if ! out=$(wsk -i action invoke -b "$ACTION_NAME" \
        -p num_iterations "$NUM_ITERATIONS" \
        -p message_length "$MESSAGE_LENGTH" -r 2>/dev/null); then
    return 1
  fi

  # wsk prints a header line ("ok: invoked ...") followed by the activation
  # record JSON; strip everything before the first '{'.
  json=$(printf '%s\n' "$out" | sed -n '/^{/,$p')
  duration=$(printf '%s' "$json" | jq -r '.elapsed_ms // empty')
  status=$(printf '%s' "$json" | jq -r '.success // empty')
  if [[ -z "$duration" || -z "$status" || "$status" != "true" ]]; then
    return 1
  fi
  printf '%s\n' "$duration" >> "$DURATIONS_FILE"
  return 0
}

run_sequential() {
  for ((i = 1; i <= RUNS; i++)); do
    if invoke_blocking_and_record; then
      ok_count=$((ok_count + 1))
      printf '  [%4d/%d] ok\n' "$i" "$RUNS"
    else
      fail_count=$((fail_count + 1))
      printf '  [%4d/%d] FAILED\n' "$i" "$RUNS" >&2
    fi
  done
}

run_concurrent() {
  echo "--- firing $RUNS non-blocking invocations ---"
  local aids=()
  for ((i = 1; i <= RUNS; i++)); do
    local aid
    if ! aid=$(wsk -i action invoke "$ACTION_NAME" \
          -p num_iterations "$NUM_ITERATIONS" \
          -p message_length "$MESSAGE_LENGTH" 2>/dev/null \
        | grep -oE '[a-f0-9]{32}' | head -n1); then
      fail_count=$((fail_count + 1))
      continue
    fi
    if [[ -z "$aid" ]]; then
      fail_count=$((fail_count + 1))
      continue
    fi
    aids+=("$aid")
  done

  echo "  fired ${#aids[@]} invocations, $fail_count failed to submit"
  echo "--- polling CouchDB for durations (timeout ${POLL_TIMEOUT_SECS}s) ---"

  local deadline=$(( $(date +%s) + POLL_TIMEOUT_SECS ))
  local -A seen=()
  while (( ok_count + fail_count < ${#aids[@]} )); do
    if (( $(date +%s) >= deadline )); then
      echo "  polling deadline exceeded; $((${#aids[@]} - ok_count - fail_count)) activations unresolved" >&2
      break
    fi
    for aid in "${aids[@]}"; do
      [[ -n "${seen[$aid]:-}" ]] && continue
      local resp duration status
      resp=$(curl -sS -X POST -H "Content-Type: application/json" \
              -d "{\"selector\": {\"activationId\": \"$aid\"}, \"fields\": [\"duration\", \"response.statusCode\"]}" \
              "$COUCHDB_URL/$ACTIVATIONS_DB/_find" 2>/dev/null || true)
      duration=$(printf '%s' "$resp" | jq -r '.docs[0].duration // empty' 2>/dev/null || true)
      status=$(printf '%s' "$resp" | jq -r '.docs[0].response.statusCode // empty' 2>/dev/null || true)
      if [[ -n "$duration" && -n "$status" ]]; then
        seen[$aid]=1
        if [[ "$status" == "0" ]]; then
          printf '%s\n' "$duration" >> "$DURATIONS_FILE"
          ok_count=$((ok_count + 1))
        else
          fail_count=$((fail_count + 1))
        fi
      fi
    done
    (( ok_count + fail_count < ${#aids[@]} )) && sleep "$POLL_INTERVAL_SECS"
  done
}

case "$MODE" in
  sequential) run_sequential ;;
  concurrent) run_concurrent ;;
  *)          echo "ERROR: unknown mode '$MODE'" >&2; exit 1 ;;
esac

echo

if (( ok_count == 0 )); then
  echo "No successful activations; nothing to report." >&2
  exit 2
fi

# Compute avg / min / max / p95 / p99 via jq. Durations file is a plain list of
# integer ms values, one per line. We convert to a JSON array and feed jq.
DURATIONS_JSON=$(jq -Rn '[inputs | tonumber]' < "$DURATIONS_FILE")

jq -n --argjson d "$DURATIONS_JSON" --argjson failed "$fail_count" '
def percentile(arr; p):
  (arr | sort) as $s
  | ((($s | length) - 1) * p) as $idx
  | ($idx | floor) as $lo
  | ($idx | ceil)  as $hi
  | if $lo == $hi then $s[$lo]
    else $s[$lo] + ($s[$hi] - $s[$lo]) * ($idx - $lo)
    end;

($d | length) as $n |
($d | add / $n) as $avg |
($d | min) as $min |
($d | max) as $max |
percentile($d; 0.50) as $p50 |
percentile($d; 0.95) as $p95 |
percentile($d; 0.99) as $p99 |

"=== Results (n=\($n), failed=\($failed)) ===",
"  avg:  \(($avg * 100 | round / 100)) ms",
"  min:  \($min) ms",
"  max:  \($max) ms",
"  p50:  \(($p50 * 100 | round / 100)) ms",
"  p95:  \(($p95 * 100 | round / 100)) ms",
"  p99:  \(($p99 * 100 | round / 100)) ms"
'
