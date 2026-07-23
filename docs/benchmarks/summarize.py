#!/usr/bin/env python3
"""Summarize a `bench.sh` run.

Reads the raw output and prints, per metric, the median of each binary and the
relative difference — which is the only number that means anything, since the
absolute values depend on the machine.
"""

import re
import statistics
import sys

LINE = re.compile(r"^(baseline|candidate) (\w+) round=(\d+) (.*)$")
NUMBER = re.compile(r"[-+]?\d+\.?\d*(?:[eE][-+]?\d+)?")

# metrics where a smaller number is better
LOWER_IS_BETTER = {"rtt_us"}


def value_of(metric, rest):
    """The comparable number on a result line."""
    if metric == "rtt_us":
        # p50=37.4 p99=94.8 mean=41.9
        match = re.search(r"p50=([\d.]+)", rest)
        return float(match.group(1)) if match else None

    match = NUMBER.search(rest)
    return float(match.group(0)) if match else None


def main():
    samples = {}

    for line in sys.stdin:
        match = LINE.match(line.strip())
        if not match:
            continue
        label, metric, _round, rest = match.groups()
        value = value_of(metric, rest)
        if value is not None:
            samples.setdefault(metric, {}).setdefault(label, []).append(value)

    print(f"{'metric':<24} {'baseline':>14} {'candidate':>14} {'delta':>9}  verdict")
    print("-" * 80)

    for metric, by_label in samples.items():
        base = by_label.get("baseline", [])
        cand = by_label.get("candidate", [])
        if not base or not cand:
            continue

        base_med = statistics.median(base)
        cand_med = statistics.median(cand)
        delta = (cand_med - base_med) / base_med * 100

        # a regression is a candidate that is worse, in the metric's direction
        regression = delta if metric in LOWER_IS_BETTER else -delta
        spread = max(
            (max(base) - min(base)) / statistics.median(base) * 100,
            (max(cand) - min(cand)) / statistics.median(cand) * 100,
        )

        verdict = "within noise" if regression <= spread / 2 else "REGRESSION"

        print(
            f"{metric:<24} {base_med:>14.1f} {cand_med:>14.1f} {delta:>8.1f}%  "
            f"{verdict} (spread {spread:.0f}%)"
        )


if __name__ == "__main__":
    main()
