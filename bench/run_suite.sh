#!/usr/bin/env bash
# Run the standard workload suite against one database and append CSV rows.
#
# Usage:
#   run_suite.sh <db-label> <config-label> <client-prefix...>
#
# The client prefix is invoked as:  <prefix> <mode> <ops> <threads> <preload>
# so it must already include the connection target (and creds, for skaidb).
#
# Examples:
#   CSV=results.csv ./run_suite.sh skaidb C4 \
#       ../target/release/examples/bench 10.0.0.1:7000 skaidb secret
#   CSV=results.csv MONGO_W=majority ./run_suite.sh mongo C4 \
#       python3 clients/mongo_bench.py 10.0.0.1:27017,10.0.0.2:27017
#
# Output CSV columns: db,config,workload,conns,throughput,p50,p99
set -u
db=$1; cfg=$2; shift 2
client=("$@")
CSV=${CSV:-bench-results.csv}
[ -f "$CSV" ] || echo "db,config,workload,conns,throughput,p50,p99" > "$CSV"

run_one() { # workload conns ops preload
  local wl=$1 conns=$2 ops=$3 preload=$4 out tp p50 p99
  out=$("${client[@]}" "$wl" "$ops" "$conns" "$preload" 2>&1)
  tp=$(grep -oP 'throughput\s*:\s*\K[0-9]+' <<<"$out")
  p50=$(grep -oP 'p50 \K[0-9.]+' <<<"$out")
  p99=$(grep -oP 'p99 \K[0-9.]+' <<<"$out")
  echo "$db,$cfg,$wl,$conns,${tp:-ERR},${p50:-},${p99:-}" >> "$CSV"
  printf "  %-8s %-16s %-6s c%-3s -> %6s ops/s (p99 %sms)\n" \
    "$db" "$cfg" "$wl" "$conns" "${tp:-ERR}" "${p99:-?}"
}

run_one write 1  1000 0
run_one write 16 4000 0
run_one read  16 4000 1000
run_one mixed 16 4000 1000
