#!/usr/bin/env python3
"""Load generator for the realm benchmark.

Three modes:

  serve     -- an echo server for the relay to forward to
  connrate  -- how many short-lived connections per second get through
  rtt       -- request round-trip time over one established connection

Everything is deliberately simple and single-purpose: the goal is comparing
two realm binaries against each other, not producing an absolute number.
"""

import argparse
import socket
import socketserver
import statistics
import time


class EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        while True:
            data = self.request.recv(65536)
            if not data:
                return
            try:
                self.request.sendall(data)
            except OSError:
                return


class EchoServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def serve(port):
    with EchoServer(("127.0.0.1", port), EchoHandler) as server:
        server.serve_forever()


def conn_rate(port, count):
    """Connections per second, each one used for a single request."""
    payload = b"x" * 64
    started = time.perf_counter()
    done = 0

    for _ in range(count):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                sock.sendall(payload)
                if sock.recv(len(payload)):
                    done += 1
        except OSError:
            pass

    elapsed = time.perf_counter() - started
    print(f"{done / elapsed:.1f} (completed={done}/{count})")


def rtt(port, count):
    """Round-trip time over one connection, in microseconds."""
    payload = b"x" * 64
    samples = []

    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # warm up, so the first samples do not carry the connection setup
        for _ in range(200):
            sock.sendall(payload)
            sock.recv(len(payload))

        for _ in range(count):
            started = time.perf_counter()
            sock.sendall(payload)
            sock.recv(len(payload))
            samples.append((time.perf_counter() - started) * 1e6)

    samples.sort()
    p50 = statistics.median(samples)
    p99 = samples[int(len(samples) * 0.99)]
    print(f"p50={p50:.1f} p99={p99:.1f} mean={statistics.fmean(samples):.1f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["serve", "connrate", "rtt"])
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--count", type=int, default=0)
    args = parser.parse_args()

    if args.mode == "serve":
        serve(args.port)
    elif args.mode == "connrate":
        conn_rate(args.port, args.count or 2000)
    else:
        rtt(args.port, args.count or 20000)


if __name__ == "__main__":
    main()
