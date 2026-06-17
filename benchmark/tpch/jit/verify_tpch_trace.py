#!/usr/bin/env python3
#
# Verify TPC-H JIT trace artifacts produced by tpch_trace.py.

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest

from tpch_schema import DEFAULT_POLICIES, DEFAULT_QUERIES, configure_csv_field_size_limit

configure_csv_field_size_limit()

COMPILED_EXECUTION_MODES = {"native"}
EXECUTION_BODIES = {"none", "generated-machine-code", "native-operator-protocol"}
REGION_STATUSES = {"compiled", "skipped", "unsupported", "disabled", "executed"}
REGION_FORMS = {"", "none", "fused"}
BOOLEAN_VALUES = {"", "true", "false"}
VAGUE_REGION_LOWERING_REASON = "core region lowering rejected the pipeline graph"
CLASSIFIED_REGION_LOWERING_REASON = "core region lowering did not produce typed region IR"

SUMMARY_FILES = (
    "summary.csv",
    "query_gap_summary.csv",
    "region_decision_summary.csv",
    "kernel_runtime_summary.csv",
    "admission_evidence_summary.csv",
    "flow_step_summary.csv",
    "decision_counter_summary.csv",
    "operator_profile_summary.csv",
)

SUMMARY_REQUIRED_COLUMNS = (
    "query",
    "policy",
    "event_count",
    "compiled_regions",
    "non_region_events",
    "zero_code_native_compile_events",
    "runtime_events",
    "runtime_input_rows",
    "runtime_invocations",
    "runtime_time_us",
    "baseline_rows",
    "result_rows",
    "result_minus_baseline",
    "baseline_minus_result",
    "event_summary_csv",
    "correctness_csv",
    "events_csv",
    "counters_csv",
    "decision_counters_csv",
    "kernel_counters_csv",
    "profile_json",
    "profile_operator_count",
    "profile_query_time_us",
)

EVENT_REQUIRED_COLUMNS = (
    "event_id",
    "phase",
    "backend_name",
    "target",
    "status",
    "execution_mode",
    "region_execution_form",
    "execution_body",
    "policy_decision",
    "candidate_shape",
    "candidate_contract_abi",
    "admission_rule_present",
    "admission_min_cardinality",
    "admission_score",
    "admission_proof",
    "reason",
    "decision_time_us",
    "compile_time_us",
    "code_size",
    "input_rows",
    "output_rows",
    "invocation_count",
    "runtime_time_us",
    "ir",
)

REGION_SUMMARY_REQUIRED_COLUMNS = (
    "query",
    "policy",
    "status",
    "execution_mode",
    "region_execution_form",
    "execution_body",
    "candidate_shape",
    "candidate_contract_abi",
    "admission_rule_present",
    "admission_min_cardinality",
    "admission_proof",
    "count",
    "decision_time_us",
    "compile_time_us",
    "code_size",
    "example_reason",
)

KERNEL_REQUIRED_COLUMNS = (
    "query",
    "policy",
    "execution_mode",
    "region_execution_form",
    "execution_body",
    "candidate_shape",
    "candidate_contract_abi",
    "kernels",
    "reached_kernels",
    "row_processing_kernels",
    "unreached_kernels",
    "zero_input_kernels",
    "input_rows",
    "output_rows",
    "invocations",
    "runtime_time_us",
    "compile_time_us",
    "code_size",
)

FLOW_REQUIRED_COLUMNS = (
    "query",
    "policy",
    "target",
    "phase",
    "status",
    "execution_mode",
    "region_execution_form",
    "execution_body",
    "policy_decision",
    "candidate_shape",
    "candidate_contract_abi",
    "admission_rule_present",
    "admission_min_cardinality",
    "admission_proof",
    "event_count",
    "kernel_count",
    "input_rows",
    "output_rows",
    "invocations",
    "runtime_time_us",
    "decision_time_us",
    "compile_time_us",
    "code_size",
    "example_reason",
)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise AssertionError(f"missing required trace artifact: {path}")
    with path.open(newline="") as csv_file:
        return list(csv.DictReader(csv_file))


def require_columns(name: str, rows: list[dict], columns: tuple[str, ...]) -> None:
    if not rows:
        return
    missing = [column for column in columns if column not in rows[0]]
    if missing:
        raise AssertionError(f"{name}: missing columns {missing}")


def row_int(row: dict, field: str, default: int = 0) -> int:
    value = row.get(field, "")
    return default if value == "" else int(value)


def row_float(row: dict, field: str, default: float = 0.0) -> float:
    value = row.get(field, "")
    return default if value == "" else float(value)


def expected_queries(args: argparse.Namespace) -> list[str]:
    if args.queries is not None:
        return [f"{int(query_id):02d}" for query_id in args.queries]
    return list(DEFAULT_QUERIES[: args.expected_queries])


def expected_query_policy_keys(queries: list[str], policies: list[str]) -> set[tuple[str, str]]:
    return {(query, policy) for query in queries for policy in policies}


def require_non_negative(row: dict, artifact: str, fields: tuple[str, ...]) -> None:
    for field in fields:
        if row_int(row, field) < 0:
            raise AssertionError(f"{artifact}: negative {field}: {row}")


def verify_boolean_field(artifact: str, row: dict, field: str) -> None:
    if row.get(field, "") not in BOOLEAN_VALUES:
        raise AssertionError(f"{artifact}: invalid boolean field {field}: {row}")


def verify_summary(trace_dir: Path, manifest: dict, rows: list[dict], queries: list[str], policies: list[str]) -> None:
    require_columns("summary.csv", rows, SUMMARY_REQUIRED_COLUMNS)
    expected_keys = expected_query_policy_keys(queries, policies)
    actual_keys = {(row["query"], row["policy"]) for row in rows}
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        raise AssertionError(f"summary.csv: query/policy mismatch missing={missing} extra={extra}")

    manifest_artifacts = set(manifest.get("artifacts", []))
    artifact_fields = (
        "event_summary_csv",
        "correctness_csv",
        "events_csv",
        "counters_csv",
        "decision_counters_csv",
        "kernel_counters_csv",
        "profile_json",
    )
    for row in rows:
        require_non_negative(
            row,
            "summary.csv",
            (
                "event_count",
                "compiled_regions",
                "non_region_events",
                "zero_code_native_compile_events",
                "runtime_events",
                "runtime_input_rows",
                "runtime_invocations",
                "runtime_time_us",
                "baseline_rows",
                "result_rows",
                "profile_operator_count",
                "profile_query_time_us",
            ),
        )
        if row_int(row, "result_minus_baseline") != 0 or row_int(row, "baseline_minus_result") != 0:
            raise AssertionError(f"summary.csv: correctness diff for q{row['query']} {row['policy']}: {row}")
        if row_int(row, "non_region_events") != 0:
            raise AssertionError(f"summary.csv: non-region JIT event: {row}")
        if row["policy"] == "off" and (row_int(row, "compiled_regions") or row_int(row, "runtime_events")):
            raise AssertionError(f"summary.csv: off policy performed compiled/runtime JIT work: {row}")
        if row_int(row, "runtime_input_rows") > 0 and row_int(row, "runtime_invocations") <= 0:
            raise AssertionError(f"summary.csv: runtime rows without invocations: {row}")
        if row_int(row, "profile_operator_count") <= 0 or row_float(row, "profile_query_time_us") <= 0:
            raise AssertionError(f"summary.csv: missing physical profile data: {row}")

        for field in artifact_fields:
            artifact = row[field]
            if artifact not in manifest_artifacts:
                raise AssertionError(f"trace_manifest.json: missing summary-owned artifact {artifact}")
            if not (trace_dir / artifact).exists():
                raise AssertionError(f"summary.csv: missing {field} target {trace_dir / artifact}")

        events = read_csv(trace_dir / row["events_csv"])
        if len(events) != row_int(row, "event_count"):
            raise AssertionError(
                f"summary.csv: event_count mismatch for q{row['query']} {row['policy']}: "
                f"summary={row['event_count']} events_csv={len(events)}"
            )
        zero_code_native_compile_events = sum(
            1
            for event in events
            if event.get("phase") == "compile"
            and event.get("execution_mode") == "native"
            and row_int(event, "code_size") == 0
        )
        if zero_code_native_compile_events != row_int(row, "zero_code_native_compile_events"):
            raise AssertionError(
                f"summary.csv: zero-code native counter mismatch for q{row['query']} {row['policy']}: "
                f"summary={row['zero_code_native_compile_events']} events_csv={zero_code_native_compile_events}"
            )


def verify_admission_metadata(artifact: str, row: dict) -> None:
    verify_boolean_field(artifact, row, "admission_rule_present")
    rule_present = row.get("admission_rule_present", "")
    min_cardinality = row.get("admission_min_cardinality", "")
    proof = row.get("admission_proof", "")
    reason = row.get("reason", row.get("example_reason", ""))
    if rule_present == "true":
        if min_cardinality == "" or row_int(row, "admission_min_cardinality") <= 0:
            raise AssertionError(f"{artifact}: admission rule has no positive min cardinality: {row}")
        if proof == "":
            raise AssertionError(f"{artifact}: admission rule has no proof: {row}")
    elif rule_present == "false":
        if min_cardinality != "" or proof != "":
            raise AssertionError(f"{artifact}: absent admission rule exposes rule fields: {row}")
        if "admission_rule=missing" in reason and proof:
            raise AssertionError(f"{artifact}: missing-admission reason carries proof: {row}")


def verify_region_lowering_reason(artifact: str, row: dict) -> None:
    reason = row.get("reason", row.get("example_reason", ""))
    if VAGUE_REGION_LOWERING_REASON in reason:
        raise AssertionError(f"{artifact}: vague graph-lowering rejection leaked into trace: {row}")
    if CLASSIFIED_REGION_LOWERING_REASON in reason:
        if "graph_blocker=" not in reason:
            raise AssertionError(f"{artifact}: graph-lowering rejection lacks graph blocker: {row}")
        if "reason" in row and "graph_shape=" not in reason:
            raise AssertionError(f"{artifact}: graph-lowering event lacks graph shape: {row}")


def verify_execution_body(artifact: str, row: dict) -> None:
    body = row.get("execution_body", "")
    if body not in EXECUTION_BODIES:
        raise AssertionError(f"{artifact}: unknown execution body: {row}")

    status = row.get("status", "")
    phase = row.get("phase", "")
    mode = row.get("execution_mode", "")
    code_size = row_int(row, "code_size")
    if status == "compiled" or phase == "kernel_counter":
        if code_size > 0 and body != "generated-machine-code":
            raise AssertionError(f"{artifact}: generated-code compiled row has wrong execution body: {row}")
        if code_size <= 0 and body != "native-operator-protocol":
            raise AssertionError(f"{artifact}: zero-code compiled row is not a native operator protocol body: {row}")
    elif phase == "runtime" or status == "executed":
        if mode in COMPILED_EXECUTION_MODES and body == "none":
            raise AssertionError(f"{artifact}: runtime row lost compiled execution body: {row}")
    elif body != "none":
        raise AssertionError(f"{artifact}: non-compiled row exposes an execution body: {row}")


def verify_event_row(artifact: str, row: dict, require_ir: bool) -> None:
    if row.get("target") != "region":
        raise AssertionError(f"{artifact}: non-region JIT event: {row}")
    if row.get("status") not in REGION_STATUSES:
        raise AssertionError(f"{artifact}: unknown event status: {row}")
    if row.get("region_execution_form") not in REGION_FORMS:
        raise AssertionError(f"{artifact}: unknown region execution form: {row}")
    require_non_negative(
        row,
        artifact,
        (
            "decision_time_us",
            "compile_time_us",
            "code_size",
            "input_rows",
            "output_rows",
            "invocation_count",
            "runtime_time_us",
        ),
    )
    verify_admission_metadata(artifact, row)
    verify_region_lowering_reason(artifact, row)
    verify_execution_body(artifact, row)

    phase = row.get("phase", "")
    status = row.get("status", "")
    execution_mode = row.get("execution_mode", "")
    if status == "compiled":
        if execution_mode not in COMPILED_EXECUTION_MODES:
            raise AssertionError(f"{artifact}: compiled event has non-compiled execution mode: {row}")
        if row.get("region_execution_form") != "fused":
            raise AssertionError(f"{artifact}: compiled event is not a fused region: {row}")
        if row.get("candidate_contract_abi") != "full_pipeline":
            raise AssertionError(f"{artifact}: compiled event is not full-pipeline ABI: {row}")
        if row_int(row, "compile_time_us") <= 0:
            raise AssertionError(f"{artifact}: compiled event has no codegen evidence: {row}")
        if require_ir and row.get("ir", "") == "":
            raise AssertionError(f"{artifact}: compiled event has empty IR: {row}")
    if phase == "runtime" or status == "executed":
        if execution_mode not in COMPILED_EXECUTION_MODES:
            raise AssertionError(f"{artifact}: runtime event has non-compiled execution mode: {row}")
        if row_int(row, "invocation_count") <= 0:
            raise AssertionError(f"{artifact}: runtime event has no invocations: {row}")
    if row_int(row, "input_rows") > 0 and row_int(row, "invocation_count") <= 0:
        raise AssertionError(f"{artifact}: input rows without invocation count: {row}")


def verify_event_files(trace_dir: Path, summary_rows: list[dict], require_ir: bool) -> None:
    for summary in summary_rows:
        events_path = trace_dir / summary["events_csv"]
        events = read_csv(events_path)
        require_columns(summary["events_csv"], events, EVENT_REQUIRED_COLUMNS)
        for event in events:
            verify_event_row(summary["events_csv"], event, require_ir)


def verify_region_summary(rows: list[dict]) -> None:
    require_columns("region_decision_summary.csv", rows, REGION_SUMMARY_REQUIRED_COLUMNS)
    for row in rows:
        if row.get("status") not in REGION_STATUSES:
            raise AssertionError(f"region_decision_summary.csv: unknown status: {row}")
        if row.get("region_execution_form") not in REGION_FORMS:
            raise AssertionError(f"region_decision_summary.csv: unknown execution form: {row}")
        require_non_negative(
            row,
            "region_decision_summary.csv",
            ("count", "decision_time_us", "compile_time_us", "code_size"),
        )
        verify_admission_metadata("region_decision_summary.csv", row)
        verify_region_lowering_reason("region_decision_summary.csv", row)
        verify_execution_body("region_decision_summary.csv", row)
        if row.get("status") == "compiled":
            if row.get("execution_mode") not in COMPILED_EXECUTION_MODES:
                raise AssertionError(f"region_decision_summary.csv: compiled row has non-compiled mode: {row}")
            if row.get("region_execution_form") != "fused":
                raise AssertionError(f"region_decision_summary.csv: compiled row is not fused: {row}")
            if row.get("candidate_contract_abi") != "full_pipeline":
                raise AssertionError(f"region_decision_summary.csv: compiled row is not full-pipeline ABI: {row}")


def verify_kernel_runtime(rows: list[dict], require_runtime: bool) -> None:
    require_columns("kernel_runtime_summary.csv", rows, KERNEL_REQUIRED_COLUMNS)
    for row in rows:
        if row.get("execution_mode") not in COMPILED_EXECUTION_MODES:
            raise AssertionError(f"kernel_runtime_summary.csv: kernel row has non-compiled mode: {row}")
        if row.get("region_execution_form") != "fused":
            raise AssertionError(f"kernel_runtime_summary.csv: kernel row is not fused: {row}")
        if row.get("candidate_contract_abi") != "full_pipeline":
            raise AssertionError(f"kernel_runtime_summary.csv: kernel row is not full-pipeline ABI: {row}")
        verify_execution_body("kernel_runtime_summary.csv", {"status": "compiled", **row})
        require_non_negative(
            row,
            "kernel_runtime_summary.csv",
            (
                "kernels",
                "reached_kernels",
                "row_processing_kernels",
                "unreached_kernels",
                "zero_input_kernels",
                "input_rows",
                "output_rows",
                "invocations",
                "runtime_time_us",
                "compile_time_us",
                "code_size",
            ),
        )
        kernels = row_int(row, "kernels")
        reached = row_int(row, "reached_kernels")
        row_processing = row_int(row, "row_processing_kernels")
        unreached = row_int(row, "unreached_kernels")
        zero_input = row_int(row, "zero_input_kernels")
        if kernels <= 0 or reached > kernels or row_processing > reached:
            raise AssertionError(f"kernel_runtime_summary.csv: invalid kernel reach counts: {row}")
        if reached + unreached != kernels:
            raise AssertionError(f"kernel_runtime_summary.csv: reached/unreached mismatch: {row}")
        if reached > 0 and row_processing + zero_input != reached:
            raise AssertionError(f"kernel_runtime_summary.csv: row/zero-input mismatch: {row}")
        if require_runtime and reached > 0 and row_int(row, "invocations") <= 0:
            raise AssertionError(f"kernel_runtime_summary.csv: reached kernel has no invocations: {row}")


def verify_flow_summary(rows: list[dict]) -> None:
    require_columns("flow_step_summary.csv", rows, FLOW_REQUIRED_COLUMNS)
    for row in rows:
        if row.get("target") != "region":
            raise AssertionError(f"flow_step_summary.csv: non-region JIT target: {row}")
        if row.get("status") not in REGION_STATUSES:
            raise AssertionError(f"flow_step_summary.csv: unknown status: {row}")
        if row.get("region_execution_form") not in REGION_FORMS:
            raise AssertionError(f"flow_step_summary.csv: unknown execution form: {row}")
        require_non_negative(
            row,
            "flow_step_summary.csv",
            (
                "event_count",
                "kernel_count",
                "input_rows",
                "output_rows",
                "invocations",
                "runtime_time_us",
                "decision_time_us",
                "compile_time_us",
                "code_size",
            ),
        )
        verify_admission_metadata("flow_step_summary.csv", row)
        verify_region_lowering_reason("flow_step_summary.csv", row)
        verify_execution_body("flow_step_summary.csv", row)
        if row_int(row, "input_rows") > 0 and row_int(row, "invocations") <= 0:
            raise AssertionError(f"flow_step_summary.csv: input rows without invocations: {row}")


def verify_gap_summaries(query_gap_rows: list[dict], summary_rows: list[dict], queries: list[str], policies: list[str]) -> None:
    require_columns(
        "query_gap_summary.csv",
        query_gap_rows,
        ("query", "correctness_diff", "force_compiled_regions", "force_compiled_kernels", "root_cause"),
    )
    if sorted(row["query"] for row in query_gap_rows) != queries:
        raise AssertionError("query_gap_summary.csv: query coverage mismatch")
    for row in query_gap_rows:
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"query_gap_summary.csv: correctness diff is non-zero: {row}")
        if row_int(row, "force_compiled_regions") > 0 and row_int(row, "force_compiled_kernels") <= 0:
            raise AssertionError(f"query_gap_summary.csv: compiled force regions without kernels: {row}")
        if row.get("root_cause", "") == "":
            raise AssertionError(f"query_gap_summary.csv: missing root cause: {row}")

    expected_keys = expected_query_policy_keys(queries, policies)
    summary_keys = {(row["query"], row["policy"]) for row in summary_rows}
    if summary_keys != expected_keys:
        raise AssertionError("summary.csv: query/policy coverage changed after query-gap verification")


def verify_required_artifact_columns(trace_dir: Path) -> dict[str, list[dict]]:
    rows_by_name = {name: read_csv(trace_dir / name) for name in SUMMARY_FILES}
    require_columns("region_decision_summary.csv", rows_by_name["region_decision_summary.csv"], REGION_SUMMARY_REQUIRED_COLUMNS)
    require_columns("kernel_runtime_summary.csv", rows_by_name["kernel_runtime_summary.csv"], KERNEL_REQUIRED_COLUMNS)
    require_columns("flow_step_summary.csv", rows_by_name["flow_step_summary.csv"], FLOW_REQUIRED_COLUMNS)
    for name, rows in rows_by_name.items():
        if rows and ("query" in rows[0]):
            for row in rows:
                if "pipeline_shape" in row and row["pipeline_shape"] == "missing":
                    raise AssertionError(f"{name}: missing pipeline shape leaked into artifact: {row}")
                if "candidate_pipeline_shape" in row and row["candidate_pipeline_shape"] == "missing":
                    raise AssertionError(f"{name}: missing candidate pipeline shape leaked into artifact: {row}")
    return rows_by_name


def verify_runtime_requirement(summary_rows: list[dict], kernel_rows: list[dict], require_runtime: bool) -> None:
    if not require_runtime:
        return
    compiled_regions = sum(row_int(row, "compiled_regions") for row in summary_rows)
    runtime_events = sum(row_int(row, "runtime_events") for row in summary_rows)
    kernel_invocations = sum(row_int(row, "invocations") for row in kernel_rows)
    if compiled_regions > 0 and runtime_events <= 0:
        raise AssertionError("runtime tracing required but compiled regions produced no runtime events")
    if compiled_regions > 0 and kernel_invocations <= 0:
        raise AssertionError("runtime tracing required but kernel summary has no invocations")


def verify_ir_requirement(trace_dir: Path, summary_rows: list[dict], require_ir: bool) -> None:
    if not require_ir:
        return
    compiled_events = 0
    for summary in summary_rows:
        for event in read_csv(trace_dir / summary["events_csv"]):
            if event.get("status") == "compiled":
                compiled_events += 1
                if event.get("ir", "") == "":
                    raise AssertionError(f"{summary['events_csv']}: compiled event has empty IR: {event}")
    if compiled_events <= 0:
        raise AssertionError("IR tracing required but no compiled events were found")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify tpch_trace.py JIT trace artifacts")
    parser.add_argument("trace_dir", type=Path)
    parser.add_argument("--expected-queries", type=int, default=len(DEFAULT_QUERIES))
    parser.add_argument("--queries", nargs="+", default=None)
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES))
    parser.add_argument("--require-runtime", action="store_true")
    parser.add_argument("--require-ir", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trace_dir = args.trace_dir.resolve()
    if not trace_dir.exists():
        raise AssertionError(f"trace directory does not exist: {trace_dir}")

    for name in SUMMARY_FILES:
        if not (trace_dir / name).exists():
            raise AssertionError(f"missing required trace artifact: {trace_dir / name}")

    manifest = verify_trace_manifest(
        trace_dir,
        kind="tpch_jit_trace",
        required_artifacts=list(SUMMARY_FILES) + ["report.md"],
    )
    queries = expected_queries(args)
    manifest_configuration = manifest.get("configuration", {})
    if manifest_configuration.get("policies") != args.policies:
        raise AssertionError(
            f"trace_manifest.json: expected policies {args.policies}, found {manifest_configuration.get('policies')}"
        )
    if manifest_configuration.get("queries", []) != queries:
        raise AssertionError(
            f"trace_manifest.json: expected queries {queries}, found {manifest_configuration.get('queries', [])}"
        )

    rows_by_name = verify_required_artifact_columns(trace_dir)
    summary_rows = rows_by_name["summary.csv"]
    query_gap_rows = rows_by_name["query_gap_summary.csv"]
    region_rows = rows_by_name["region_decision_summary.csv"]
    kernel_rows = rows_by_name["kernel_runtime_summary.csv"]
    flow_rows = rows_by_name["flow_step_summary.csv"]

    require_runtime = args.require_runtime or bool(manifest_configuration.get("trace_runtime"))
    require_ir = args.require_ir or bool(manifest_configuration.get("dump_ir"))

    verify_summary(trace_dir, manifest, summary_rows, queries, args.policies)
    verify_event_files(trace_dir, summary_rows, require_ir)
    verify_gap_summaries(query_gap_rows, summary_rows, queries, args.policies)
    verify_region_summary(region_rows)
    verify_kernel_runtime(kernel_rows, require_runtime)
    verify_flow_summary(flow_rows)
    verify_runtime_requirement(summary_rows, kernel_rows, require_runtime)
    verify_ir_requirement(trace_dir, summary_rows, require_ir)

    print(
        "ok "
        f"summary={len(summary_rows)} "
        f"query_gap={len(query_gap_rows)} "
        f"region={len(region_rows)} "
        f"kernels={len(kernel_rows)} "
        f"flow_steps={len(flow_rows)} "
        f"require_runtime={require_runtime} "
        f"require_ir={require_ir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
