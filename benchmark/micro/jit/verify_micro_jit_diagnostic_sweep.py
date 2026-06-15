#!/usr/bin/env python3
#
# Verify artifacts produced by micro_jit_diagnostic_sweep.py.

import argparse
import csv
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest
from micro_jit_manifest import (
    DIAGNOSTIC_POLICIES as POLICIES,
    MIN_ADMITTED_SPEEDUP,
    diagnostic_expected_families,
)


EXPECTED_FAMILIES = diagnostic_expected_families()


def read_csv(path: Path) -> list:
    if not path.exists():
        raise AssertionError(f"missing required diagnostic sweep artifact: {path}")
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


def verify_summary(rows: list, manifest: dict) -> None:
    row_counts = sorted(int(value) for value in manifest["configuration"].get("row_counts", []))
    if not row_counts:
        raise AssertionError("trace_manifest.json: diagnostic sweep row_counts is empty")
    expected_keys = {
        (family, row_count, policy)
        for family in EXPECTED_FAMILIES
        for row_count in row_counts
        for policy in POLICIES
    }
    actual_keys = {(row["family"], row_int(row, "row_count"), row["policy"]) for row in rows}
    if actual_keys != expected_keys:
        raise AssertionError(
            "summary.csv: diagnostic sweep coverage mismatch: "
            f"missing={sorted(expected_keys - actual_keys)} unexpected={sorted(actual_keys - expected_keys)}"
        )
    for row in rows:
        family = EXPECTED_FAMILIES[row["family"]]
        if row["shape_key"] != family["shape_key"]:
            raise AssertionError(f"summary.csv: wrong sweep shape key: {row}")
        if row["candidate_shape"] != family["candidate_shape"]:
            raise AssertionError(f"summary.csv: wrong sweep candidate shape: {row}")
        if row_int(row, "run_count") < 5:
            raise AssertionError(f"summary.csv: expected at least five benchmark runs: {row}")
        if row_float(row, "median_s") <= 0 or row_float(row, "mean_s") <= 0:
            raise AssertionError(f"summary.csv: non-positive benchmark timing: {row}")

    by_family_count = {}
    for row in rows:
        by_family_count.setdefault((row["family"], row_int(row, "row_count")), {})[row["policy"]] = row
    for key, policies in by_family_count.items():
        off_median = row_float(policies["off"], "median_s")
        force_median = row_float(policies["force"], "median_s")
        force_speedup = row_float(policies["force"], "speedup_vs_off")
        if abs((off_median / force_median) - force_speedup) > 0.000001:
            raise AssertionError(f"summary.csv: speedup mismatch for {key}")


def verify_family_summary(rows: list, summary_rows: list) -> None:
    if {row["family"] for row in rows} != set(EXPECTED_FAMILIES):
        raise AssertionError(f"family_summary.csv: wrong family coverage: {rows}")
    force_rows_by_family = {}
    for row in summary_rows:
        if row["policy"] == "force":
            force_rows_by_family.setdefault(row["family"], []).append(row)
    for row in rows:
        family = row["family"]
        expected = EXPECTED_FAMILIES[family]
        if row["shape_key"] != expected["shape_key"]:
            raise AssertionError(f"family_summary.csv: wrong shape key: {row}")
        if row["candidate_shape"] != expected["candidate_shape"]:
            raise AssertionError(f"family_summary.csv: wrong candidate shape: {row}")
        if abs(row_float(row, "min_admitted_speedup") - MIN_ADMITTED_SPEEDUP) > 0.000001:
            raise AssertionError(f"family_summary.csv: wrong admission margin: {row}")
        force_rows = sorted(force_rows_by_family[family], key=lambda entry: row_int(entry, "row_count"))
        first_at_margin = ""
        for force_row in force_rows:
            if row_float(force_row, "speedup_vs_off") >= MIN_ADMITTED_SPEEDUP:
                first_at_margin = force_row["row_count"]
                break
        best_row = max(force_rows, key=lambda entry: row_float(entry, "speedup_vs_off"))
        if row.get("first_row_count_at_margin", "") != first_at_margin:
            raise AssertionError(f"family_summary.csv: first margin row mismatch: {row}")
        if row["best_row_count"] != best_row["row_count"]:
            raise AssertionError(f"family_summary.csv: best row count mismatch: {row}")
        if row["best_speedup_vs_off"] != best_row["speedup_vs_off"]:
            raise AssertionError(f"family_summary.csv: best speedup mismatch: {row}")


def verify_runs(runs: list, summary_rows: list) -> None:
    by_key = {}
    for row in runs:
        key = (row["family"], row_int(row, "row_count"), row["policy"])
        by_key.setdefault(key, []).append(row_float(row, "timing_s"))
    for row in summary_rows:
        key = (row["family"], row_int(row, "row_count"), row["policy"])
        timings = by_key.get(key, [])
        if len(timings) != row_int(row, "run_count"):
            raise AssertionError(f"runs.csv: run count mismatch for {key}")
        if abs(statistics.median(timings) - row_float(row, "median_s")) > 0.000000001:
            raise AssertionError(f"runs.csv: median mismatch for {key}")


def verify_benchmark_files(benchmark_dir: Path, summary_rows: list, manifest: dict) -> None:
    artifacts = set(manifest.get("artifacts", {}))
    for row in summary_rows:
        rel_path = row["benchmark_file"]
        if rel_path not in artifacts:
            raise AssertionError(f"trace_manifest.json: missing generated benchmark artifact {rel_path}")
        path = benchmark_dir / rel_path
        if not path.exists():
            raise AssertionError(f"missing generated benchmark file: {path}")
        text = path.read_text(encoding="utf-8")
        for needle in (
            "duckdb_jit_events()",
            f"candidate_shape='{row['candidate_shape']}'",
            f"admission_shape_key='{row['shape_key']}'",
        ):
            if row["policy"] == "force" and needle not in text:
                raise AssertionError(f"{path}: generated force benchmark missing {needle}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify micro_jit_diagnostic_sweep.py artifacts")
    parser.add_argument("sweep_dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sweep_dir = args.sweep_dir.resolve()
    manifest = verify_trace_manifest(
        sweep_dir,
        kind="micro_jit_diagnostic_sweep",
        required_artifacts=["runs.csv", "summary.csv", "family_summary.csv"],
    )
    summary_rows = read_csv(sweep_dir / "summary.csv")
    family_rows = read_csv(sweep_dir / "family_summary.csv")
    runs = read_csv(sweep_dir / "runs.csv")
    verify_summary(summary_rows, manifest)
    verify_family_summary(family_rows, summary_rows)
    verify_runs(runs, summary_rows)
    verify_benchmark_files(sweep_dir, summary_rows, manifest)
    print(
        "ok micro_diagnostic_sweep summary={summary} family={family} runs={runs} artifacts={artifacts}".format(
            summary=len(summary_rows),
            family=len(family_rows),
            runs=len(runs),
            artifacts=len(manifest["artifacts"]),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
