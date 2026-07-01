#!/usr/bin/env bash
set -euo pipefail

WASM_BIN="/home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-float.wasm"
CWASM_BIN="/home/vmraj2/faasrail/faasrail-benchmarks/bench-float.cwasm"
RUNS=1000
PARAM=1000

run_benchmark() {
    local label="$1" binary="$2" flag="$3"
    local times=()

    for ((i = 1; i <= RUNS; i++)); do
        local start end elapsed
        start=$(date +%s%N)
        wasmtime $flag "$binary" $PARAM > /dev/null
        end=$(date +%s%N)
        elapsed=$(( (end - start) / 1000 ))  # microseconds
        times+=("$elapsed")
    done

    IFS=$'\n'
    local sorted=($(printf '%s\n' "${times[@]}" | sort -n))
    unset IFS

    local n=${#sorted[@]}
    local sum=0 sq_sum=0
    for t in "${sorted[@]}"; do
        sum=$((sum + t))
        sq_sum=$((sq_sum + t * t))
    done

    local min=${sorted[0]}
    local max=${sorted[$((n - 1))]}

    local mid=$((n / 2))
    local median
    if ((n % 2 == 1)); then
        median=${sorted[$mid]}
    else
        median=$(( (sorted[mid - 1] + sorted[mid]) / 2 ))
    fi

    local avg=$((sum / n))
    local variance=$(( (sq_sum / n) - (avg * avg) ))
    # integer sqrt via Newton's method
    local stdev=0
    if ((variance > 0)); then
        stdev=$variance
        local prev=0
        while ((stdev != prev)); do
            prev=$stdev
            stdev=$(( (stdev + variance / stdev) / 2 ))
        done
    fi

    printf "\n=== %s (%d runs) ===\n" "$label" "$RUNS"
    printf "  avg:    %10d us  (%8.2f ms)\n" "$avg"    "$(echo "$avg / 1000" | bc -l)"
    printf "  median: %10d us  (%8.2f ms)\n" "$median" "$(echo "$median / 1000" | bc -l)"
    printf "  min:    %10d us  (%8.2f ms)\n" "$min"    "$(echo "$min / 1000" | bc -l)"
    printf "  max:    %10d us  (%8.2f ms)\n" "$max"    "$(echo "$max / 1000" | bc -l)"
    printf "  stdev:  %10d us  (%8.2f ms)\n" "$stdev"  "$(echo "$stdev / 1000" | bc -l)"
}

echo "Benchmarking wasmtime: $RUNS iterations, param=$PARAM"

run_benchmark "Normal WASM (.wasm)"   "$WASM_BIN"  ""
run_benchmark "Wasmtime Cache WASM (.wasm)"   "$WASM_BIN"  "-C cache-config=/etc/wasmtime/config.toml -C cache=y"
run_benchmark "Compiled WASM (.cwasm)" "$CWASM_BIN" "--allow-precompiled"
