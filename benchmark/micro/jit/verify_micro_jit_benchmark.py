#!/usr/bin/env python3
#
# Verify artifacts produced by micro_jit_benchmark.py.

import argparse
import csv
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest
from micro_jit_manifest import ADMITTED_POLICIES as POLICIES, admitted_threshold_shapes


EXPECTED_SHAPES = {shape["shape"]: shape for shape in admitted_threshold_shapes()}


def read_csv(path: Path) -> list:
    if not path.exists():
        raise AssertionError(f"missing required benchmark artifact: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def row_float(row: dict, field: str) -> float:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0.0
    return float(value)


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0
    return int(value)


def verify_summary(rows: list) -> None:
    expected_keys = {(shape, policy) for shape in EXPECTED_SHAPES for policy in POLICIES}
    actual_keys = {(row["shape"], row["policy"]) for row in rows}
    if actual_keys != expected_keys:
        raise AssertionError(
            "summary.csv: shape/policy coverage mismatch: "
            f"missing={sorted(expected_keys - actual_keys)} unexpected={sorted(actual_keys - expected_keys)}"
        )
    by_shape = {}
    for row in rows:
        shape = EXPECTED_SHAPES[row["shape"]]
        if row["shape_key"] != shape["shape_key"]:
            raise AssertionError(f"summary.csv: wrong shape_key: {row}")
        if row["proof"] != shape["proof"]:
            raise AssertionError(f"summary.csv: wrong proof: {row}")
        if row_int(row, "run_count") < 5:
            raise AssertionError(f"summary.csv: expected at least five benchmark runs: {row}")
        if row_float(row, "median_s") <= 0 or row_float(row, "mean_s") <= 0:
            raise AssertionError(f"summary.csv: non-positive benchmark timing: {row}")
        by_shape.setdefault(row["shape"], {})[row["policy"]] = row

    for shape_name, policies in by_shape.items():
        off_median = row_float(policies["off"], "median_s")
        for policy in ("auto", "force"):
            policy_median = row_float(policies[policy], "median_s")
            expected_speedup = off_median / policy_median
            if abs(expected_speedup - row_float(policies[policy], "speedup_vs_off")) > 0.000001:
                raise AssertionError(f"summary.csv: {shape_name} {policy} speedup mismatch")
            expected_faster = "true" if policy_median < off_median else "false"
            if policies[policy]["faster_than_off"] != expected_faster:
                raise AssertionError(f"summary.csv: {shape_name} {policy} faster_than_off flag mismatch")


def verify_runs(runs: list, summary_rows: list) -> None:
    by_key = {}
    for row in runs:
        key = (row["shape"], row["policy"])
        by_key.setdefault(key, []).append(row_float(row, "timing_s"))
    for row in summary_rows:
        key = (row["shape"], row["policy"])
        timings = by_key.get(key, [])
        if len(timings) != row_int(row, "run_count"):
            raise AssertionError(f"runs.csv: run count mismatch for {key}")
        if abs(statistics.median(timings) - row_float(row, "median_s")) > 0.000000001:
            raise AssertionError(f"runs.csv: median mismatch for {key}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify micro_jit_benchmark.py artifacts")
    parser.add_argument("benchmark_dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    benchmark_dir = args.benchmark_dir.resolve()
    manifest = verify_trace_manifest(
        benchmark_dir,
        kind="micro_jit_benchmark",
        required_artifacts=["runs.csv", "summary.csv"],
    )
    rows = read_csv(benchmark_dir / "summary.csv")
    runs = read_csv(benchmark_dir / "runs.csv")
    verify_summary(rows)
    verify_runs(runs, rows)
    print(f"ok micro_benchmark_summary={len(rows)} runs={len(runs)} artifacts={len(manifest['artifacts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
