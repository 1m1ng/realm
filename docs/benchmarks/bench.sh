#!/usr/bin/env bash
# Compare a realm binary against another one on the same machine.
#
#   ./bench.sh <baseline-binary> <candidate-binary> [rounds]
#
# Measures, through a relay each binary serves:
#   - tcp throughput           (iperf3)
#   - udp throughput           (iperf3 -u)
#   - new connection rate      (short-lived connections per second)
#   - request round-trip time  (ping/pong latency over one connection)
#
# Absolute numbers depend entirely on the machine; what matters is the
# difference between the two binaries measured back to back under the same
# conditions.

set -euo pipefail

BASELINE=${1:?usage: bench.sh <baseline-binary> <candidate-binary> [rounds]}
CANDIDATE=${2:?usage: bench.sh <baseline-binary> <candidate-binary> [rounds]}
ROUNDS=${3:-3}

HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

# backend ports
IPERF_PORT=15201
IPERF_UDP_PORT=15202
ECHO_PORT=15203
# relay ports
RELAY_TCP=15301
RELAY_UDP=15302
RELAY_ECHO=15303

start_backends() {
    iperf3 -s -p "$IPERF_PORT" --logfile "$WORK/iperf-tcp.log" &
    iperf3 -s -p "$IPERF_UDP_PORT" --logfile "$WORK/iperf-udp.log" &
    python3 "$HERE/bench_client.py" serve --port "$ECHO_PORT" &
    sleep 1
}

start_relay() {
    local binary=$1
    cat > "$WORK/relay.toml" <<EOF
[[endpoints]]
listen = "127.0.0.1:$RELAY_TCP"
remote = "127.0.0.1:$IPERF_PORT"

# iperf3 needs a tcp control connection on the same port even in udp mode,
# so this endpoint serves both data planes
[[endpoints]]
listen = "127.0.0.1:$RELAY_UDP"
remote = "127.0.0.1:$IPERF_UDP_PORT"
network = { use_udp = true }

[[endpoints]]
listen = "127.0.0.1:$RELAY_ECHO"
remote = "127.0.0.1:$ECHO_PORT"
EOF

    "$binary" -c "$WORK/relay.toml" > "$WORK/relay.log" 2>&1 &
    RELAY_PID=$!
    sleep 1
}

stop_relay() {
    kill "$RELAY_PID" 2>/dev/null || true
    wait "$RELAY_PID" 2>/dev/null || true
    sleep 0.5
}

measure() {
    local label=$1 binary=$2 round=$3

    start_relay "$binary"

    local tcp udp
    tcp=$(iperf3 -c 127.0.0.1 -p "$RELAY_TCP" -t 5 -J 2>/dev/null \
          | python3 -c 'import json,sys; print(json.load(sys.stdin)["end"]["sum_received"]["bits_per_second"])')
    echo "$label tcp_throughput_bps round=$round $tcp"

    udp=$(iperf3 -u -c 127.0.0.1 -p "$RELAY_UDP" -t 5 -b 4G -l 1200 -J 2>/dev/null \
          | python3 -c 'import json,sys; d=json.load(sys.stdin)["end"]["sum"]; print(d["bits_per_second"])')
    echo "$label udp_throughput_bps round=$round $udp"

    python3 "$HERE/bench_client.py" connrate --port "$RELAY_ECHO" \
        | sed "s/^/$label conn_rate_per_s round=$round /"

    python3 "$HERE/bench_client.py" rtt --port "$RELAY_ECHO" \
        | sed "s/^/$label rtt_us round=$round /"

    stop_relay
}

start_backends

echo "# realm benchmark, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "# baseline:  $BASELINE"
echo "# candidate: $CANDIDATE"
echo "# rounds:    $ROUNDS"

# interleaved on purpose: running one binary's rounds and then the other's
# measures the machine drifting as much as the code. Alternating makes both
# binaries see the same conditions, and the order flips every round so that
# neither one is always the warm-up.
for round in $(seq 1 "$ROUNDS"); do
    if [ $((round % 2)) -eq 1 ]; then
        measure baseline "$BASELINE" "$round"
        measure candidate "$CANDIDATE" "$round"
    else
        measure candidate "$CANDIDATE" "$round"
        measure baseline "$BASELINE" "$round"
    fi
done
