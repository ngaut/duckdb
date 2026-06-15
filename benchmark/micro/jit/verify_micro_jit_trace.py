#!/usr/bin/env python3
#
# Verify artifacts produced by micro_jit_trace.py.

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest
from micro_jit_manifest import ADMITTED_POLICIES as POLICIES, admitted_trace_shapes


EXPECTED_SHAPES = {
    shape["shape"]: {
        "shape_key": shape["shape_key"],
        "proof": shape["proof"],
        "candidate_shape": shape["expected_candidate_shape"],
        "min_cardinality": shape["min_cardinality"],
    }
    for shape in admitted_trace_shapes()
}


def read_csv(path: Path) -> list:
    if not path.exists():
        raise AssertionError(f"missing required trace artifact: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0
    return int(value)


def row_float(row: dict, field: str) -> float:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0.0
    return float(value)


def row_bool(row: dict, field: str) -> bool:
    return str(row.get(field, "")).lower() in ("1", "true", "t")


def verify_summary(trace_dir: Path, rows: list, manifest: dict) -> None:
    expected_keys = {(shape, policy) for shape in EXPECTED_SHAPES for policy in POLICIES}
    actual_keys = {(row["shape"], row["policy"]) for row in rows}
    if actual_keys != expected_keys:
        raise AssertionError(
            "summary.csv: shape/policy coverage mismatch: "
            f"missing={sorted(expected_keys - actual_keys)} unexpected={sorted(actual_keys - expected_keys)}"
        )
    for row in rows:
        shape = EXPECTED_SHAPES[row["shape"]]
        policy = row["policy"]
        if row["shape_key"] != shape["shape_key"]:
            raise AssertionError(f"summary.csv: wrong shape_key: {row}")
        if row["proof"] != shape["proof"]:
            raise AssertionError(f"summary.csv: wrong proof: {row}")
        if row["expected_candidate_shape"] != shape["candidate_shape"]:
            raise AssertionError(f"summary.csv: wrong candidate shape: {row}")
        if not row_bool(row, "validation_pass"):
            raise AssertionError(f"summary.csv: validation failed: {row}")
        if row_float(row, "total_time_s") <= 0:
            raise AssertionError(f"summary.csv: non-positive total time: {row}")
        if row_int(row, "zero_code_executable_compile_events") != 0:
            raise AssertionError(f"summary.csv: zero-code executable compile event: {row}")
        if policy == "off":
            if row_int(row, "compiled_regions") != 0 or row_int(row, "runtime_events") != 0:
                raise AssertionError(f"summary.csv: off policy compiled or ran kernels: {row}")
        elif policy == "auto":
            if row_int(row, "compiled_regions") != 0:
                raise AssertionError(f"summary.csv: auto policy compiled non-fused source-prefix region: {row}")
            if row_int(row, "runtime_events") != 0:
                raise AssertionError(f"summary.csv: auto policy ran non-fused source-prefix region: {row}")
            if row_int(row, "skipped_regions") <= 0:
                raise AssertionError(f"summary.csv: expected auto non-fused source-prefix skip event: {row}")
            if row_int(row, "admission_time_us") <= 0:
                raise AssertionError(f"summary.csv: expected auto admission timing: {row}")
        else:
            if row_int(row, "compiled_regions") <= 0:
                raise AssertionError(f"summary.csv: expected native compiled regions: {row}")
            if row_int(row, "executable_compile_events") <= 0:
                raise AssertionError(f"summary.csv: expected executable compile events: {row}")
            if row_int(row, "skipped_regions") <= 0:
                raise AssertionError(f"summary.csv: expected force non-fused source-boundary event: {row}")
            if row_int(row, "runtime_events") <= 0 or row_int(row, "runtime_input_rows") <= 0:
                raise AssertionError(f"summary.csv: expected runtime rows for compiled shape: {row}")
            if row_int(row, "codegen_time_us") <= 0:
                raise AssertionError(f"summary.csv: expected codegen timing: {row}")
        for field in (
            "event_summary_csv",
            "validation_csv",
            "events_csv",
            "counters_csv",
            "kernel_counters_csv",
            "database_file",
        ):
            if row[field] not in manifest["artifacts"]:
                raise AssertionError(f"trace_manifest.json: missing summary-owned artifact {row[field]}")
            if not (trace_dir / row[field]).exists():
                raise AssertionError(f"summary.csv: artifact does not exist: {row[field]}")


def verify_events(trace_dir: Path, rows: list) -> None:
    for row in rows:
        events = read_csv(trace_dir / row["events_csv"])
        if len(events) != row_int(row, "event_count"):
            raise AssertionError(f"{row['events_csv']}: event count mismatch")
        region_events = [event for event in events if event.get("target") == "region"]
        if row["policy"] == "off":
            if any(event.get("status") == "compiled" for event in region_events):
                raise AssertionError(f"{row['events_csv']}: off policy compiled a region")
            continue

        if row["policy"] in {"auto", "force"}:
            expected = EXPECTED_SHAPES[row["shape"]]
            source_boundary_events = [
                event
                for event in region_events
                if event.get("status") in {"skipped", "unsupported"}
                and event.get("candidate_shape") == expected["candidate_shape"]
                and event.get("candidate_scope") == "source_pipeline"
                and event.get("region_execution_form") == "none"
                and "source-fusion-gap:requires-native-source" in event.get("reason", "")
            ]
            if len(source_boundary_events) != 1:
                raise AssertionError(
                    f"{row['events_csv']}: expected one non-fused source-boundary {row['policy']} event, found {len(source_boundary_events)}"
                )
            event = source_boundary_events[0]
            if event.get("execution_mode") not in {"executor_fallback", "unsupported"}:
                raise AssertionError(f"{row['events_csv']}: source-prefix event has wrong execution mode: {event}")
            if event.get("region_execution_form") != "none":
                raise AssertionError(f"{row['events_csv']}: source-prefix event is not non-fused: {event}")
            if event.get("candidate_shape") != expected["candidate_shape"]:
                raise AssertionError(f"{row['events_csv']}: candidate shape mismatch: {event}")
            if event.get("admission_rule_present") == "true":
                if event.get("admission_shape_key") != expected["shape_key"]:
                    raise AssertionError(f"{row['events_csv']}: admission shape mismatch: {event}")
                if event.get("admission_proof") != expected["proof"]:
                    raise AssertionError(f"{row['events_csv']}: admission proof mismatch: {event}")
                if row_int(event, "admission_min_cardinality") != expected["min_cardinality"]:
                    raise AssertionError(f"{row['events_csv']}: admission threshold mismatch: {event}")
            reason = event.get("reason", "")
            if "execution-form=none" not in reason or "execution:unsupported" not in reason:
                raise AssertionError(f"{row['events_csv']}: source-boundary reason missing unsupported contract: {event}")
            if "helper-call:DuckDB source GetData helper boundary" not in reason:
                raise AssertionError(f"{row['events_csv']}: source helper reason missing: {event}")
            if row["policy"] == "auto":
                continue
            native_compiled = [
                event
                for event in region_events
                if event.get("phase") == "compile"
                and event.get("status") == "compiled"
                and event.get("execution_mode") == "native"
                and event.get("region_execution_form") == "fused"
            ]
            if not native_compiled:
                raise AssertionError(f"{row['events_csv']}: force did not compile a native fused region")
            continue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify micro_jit_trace.py artifacts")
    parser.add_argument("trace_dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trace_dir = args.trace_dir.resolve()
    manifest = verify_trace_manifest(trace_dir, kind="micro_jit_trace", required_artifacts=["summary.csv"])
    summary_rows = read_csv(trace_dir / "summary.csv")
    verify_summary(trace_dir, summary_rows, manifest)
    verify_events(trace_dir, summary_rows)
    print(f"ok micro_summary={len(summary_rows)} artifacts={len(manifest['artifacts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
