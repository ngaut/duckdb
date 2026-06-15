#!/usr/bin/env python3
#
# Verify artifacts produced by micro_jit_full_pipeline_selectivity_sweep.py.

import argparse
import csv
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest
from micro_jit_manifest import DIAGNOSTIC_POLICIES as POLICIES, MIN_ADMITTED_SPEEDUP


FAMILY = "native_full_pipeline_decimal_projection_ungrouped_sum"
SHAPE_KEY = "sljit:full-pipeline:fused-filter-projection-ungrouped-sum"
CANDIDATE_SHAPE = "scan-filter-scan-project-projection-sink"


def read_csv(path: Path) -> list:
    if not path.exists():
        raise AssertionError(f"missing required selectivity artifact: {path}")
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
    selectivity_values = sorted(int(value) for value in manifest["configuration"].get("selectivity_percent", []))
    if not row_counts:
        raise AssertionError("trace_manifest.json: row_counts is empty")
    if not selectivity_values:
        raise AssertionError("trace_manifest.json: selectivity_percent is empty")
    expected_keys = {
        (row_count, selectivity_percent, policy)
        for row_count in row_counts
        for selectivity_percent in selectivity_values
        for policy in POLICIES
    }
    actual_keys = {
        (row_int(row, "row_count"), row_int(row, "selectivity_percent"), row["policy"]) for row in rows
    }
    if actual_keys != expected_keys:
        raise AssertionError(
            "summary.csv: selectivity coverage mismatch: "
            f"missing={sorted(expected_keys - actual_keys)} unexpected={sorted(actual_keys - expected_keys)}"
        )
    by_shape = {}
    for row in rows:
        if row["family"] != FAMILY:
            raise AssertionError(f"summary.csv: wrong family: {row}")
        if row["shape_key"] != SHAPE_KEY:
            raise AssertionError(f"summary.csv: wrong shape key: {row}")
        if row["candidate_shape"] != CANDIDATE_SHAPE:
            raise AssertionError(f"summary.csv: wrong candidate shape: {row}")
        if row_int(row, "run_count") < 5:
            raise AssertionError(f"summary.csv: expected at least five benchmark runs: {row}")
        if row_int(row, "output_count") <= 0:
            raise AssertionError(f"summary.csv: non-positive output count: {row}")
        if row_float(row, "median_s") <= 0 or row_float(row, "mean_s") <= 0:
            raise AssertionError(f"summary.csv: non-positive benchmark timing: {row}")
        by_shape.setdefault((row_int(row, "row_count"), row_int(row, "selectivity_percent")), {})[row["policy"]] = row

    for key, policies in by_shape.items():
        off_median = row_float(policies["off"], "median_s")
        force_median = row_float(policies["force"], "median_s")
        force_speedup = row_float(policies["force"], "speedup_vs_off")
        if abs((off_median / force_median) - force_speedup) > 0.000001:
            raise AssertionError(f"summary.csv: speedup mismatch for {key}")
        expected_faster = "true" if force_median < off_median else "false"
        if policies["force"]["faster_than_off"] != expected_faster:
            raise AssertionError(f"summary.csv: faster_than_off mismatch for {key}")


def verify_selectivity_summary(rows: list, summary_rows: list) -> None:
    if len(rows) != 1:
        raise AssertionError(f"selectivity_summary.csv: expected one row, found {len(rows)}")
    row = rows[0]
    if row["family"] != FAMILY or row["shape_key"] != SHAPE_KEY or row["candidate_shape"] != CANDIDATE_SHAPE:
        raise AssertionError(f"selectivity_summary.csv: wrong shape identity: {row}")
    if abs(row_float(row, "min_admitted_speedup") - MIN_ADMITTED_SPEEDUP) > 0.000001:
        raise AssertionError(f"selectivity_summary.csv: wrong admission margin: {row}")
    force_rows = [entry for entry in summary_rows if entry["policy"] == "force"]
    best_row = max(force_rows, key=lambda entry: row_float(entry, "speedup_vs_off"))
    first_at_margin = ""
    for force_row in sorted(force_rows, key=lambda entry: row_int(entry, "selectivity_percent")):
        if row_float(force_row, "speedup_vs_off") >= MIN_ADMITTED_SPEEDUP:
            first_at_margin = force_row["selectivity_percent"]
            break
    if row["first_selectivity_percent_at_margin"] != first_at_margin:
        raise AssertionError(f"selectivity_summary.csv: first margin mismatch: {row}")
    if row["best_selectivity_percent"] != best_row["selectivity_percent"]:
        raise AssertionError(f"selectivity_summary.csv: best selectivity mismatch: {row}")
    if row["best_speedup_vs_off"] != best_row["speedup_vs_off"]:
        raise AssertionError(f"selectivity_summary.csv: best speedup mismatch: {row}")


def verify_runs(runs: list, summary_rows: list) -> None:
    by_key = {}
    for row in runs:
        key = (row_int(row, "row_count"), row_int(row, "selectivity_percent"), row["policy"])
        by_key.setdefault(key, []).append(row_float(row, "timing_s"))
    for row in summary_rows:
        key = (row_int(row, "row_count"), row_int(row, "selectivity_percent"), row["policy"])
        timings = by_key.get(key, [])
        if len(timings) != row_int(row, "run_count"):
            raise AssertionError(f"runs.csv: run count mismatch for {key}")
        if abs(statistics.median(timings) - row_float(row, "median_s")) > 0.000000001:
            raise AssertionError(f"runs.csv: median mismatch for {key}")


def verify_benchmark_files(sweep_dir: Path, summary_rows: list, manifest: dict) -> None:
    artifacts = set(manifest.get("artifacts", {}))
    for row in summary_rows:
        rel_path = row["benchmark_file"]
        if rel_path not in artifacts:
            raise AssertionError(f"trace_manifest.json: missing generated benchmark artifact {rel_path}")
        path = sweep_dir / rel_path
        if not path.exists():
            raise AssertionError(f"missing generated benchmark file: {path}")
        text = path.read_text(encoding="utf-8")
        if row["policy"] == "force":
            for needle in (
                "selected_source_execution='native-source'",
                f"candidate_shape='{CANDIDATE_SHAPE}'",
                f"admission_shape_key='{SHAPE_KEY}'",
                "candidate_sink_kind='ungrouped-aggregate-update'",
            ):
                if needle not in text:
                    raise AssertionError(f"{path}: generated force benchmark missing {needle}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify micro_jit_full_pipeline_selectivity_sweep.py artifacts")
    parser.add_argument("sweep_dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sweep_dir = args.sweep_dir.resolve()
    manifest = verify_trace_manifest(
        sweep_dir,
        kind="micro_jit_full_pipeline_selectivity_sweep",
        required_artifacts=["runs.csv", "summary.csv", "selectivity_summary.csv"],
    )
    summary_rows = read_csv(sweep_dir / "summary.csv")
    selectivity_rows = read_csv(sweep_dir / "selectivity_summary.csv")
    runs = read_csv(sweep_dir / "runs.csv")
    verify_summary(summary_rows, manifest)
    verify_selectivity_summary(selectivity_rows, summary_rows)
    verify_runs(runs, summary_rows)
    verify_benchmark_files(sweep_dir, summary_rows, manifest)
    print(
        "ok micro_full_pipeline_selectivity summary={summary} selectivity={selectivity} runs={runs} artifacts={artifacts}".format(
            summary=len(summary_rows),
            selectivity=len(selectivity_rows),
            runs=len(runs),
            artifacts=len(manifest["artifacts"]),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
