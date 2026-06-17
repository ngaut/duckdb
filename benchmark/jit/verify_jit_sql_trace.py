#!/usr/bin/env python3
#
# Verify artifacts produced by jit_sql_trace.py.

import argparse
import csv
import re
from pathlib import Path

from trace_manifest import verify_trace_manifest


TEST_CASE_RE = re.compile(r'TEST_CASE\("([^"]+)",\s*"([^"]*)"\)')
EXPECTED_CASES = {
    "region_native_filter_projection",
    "region_unsupported_unnest_boundary",
    "sql_equivalence_matrix",
}
KNOWN_REGION_EXECUTION_FORMS = {"none", "fused"}
COMPILED_EXECUTION_MODES = {"native"}
KNOWN_EXECUTION_BODIES = {"none", "generated-machine-code", "native-operator-protocol"}
KNOWN_CANDIDATE_ABIS = {
    "none",
    "full_pipeline",
}


CANDIDATE_TRAIT_REQUIRED_COLUMNS = (
    "candidate_signature_context",
    "candidate_signature_shape",
    "candidate_signature_feature_shape",
    "candidate_signature_context_feature_shape",
    "candidate_contract_shape",
    "candidate_contract_abi",
    "candidate_owns_source",
    "candidate_owns_transform",
    "candidate_owns_sink",
    "candidate_owns_state_scan",
    "candidate_has_source",
    "candidate_has_sink",
    "candidate_source_kind",
    "candidate_source_execution",
    "candidate_sink_kind",
    "candidate_expression_traits_known",
    "candidate_source_filter_count",
    "candidate_source_filter_expression_count",
    "candidate_source_filter_missing_count",
    "candidate_source_comparison_filter_count",
    "candidate_source_integer_comparison_filter_count",
    "candidate_source_non_integer_comparison_filter_count",
    "candidate_source_conjunction_filter_count",
    "candidate_source_projected_column_count",
    "candidate_source_returned_column_count",
    "candidate_arithmetic_projection_count",
    "candidate_integer_arithmetic_projection_count",
    "candidate_non_integer_arithmetic_projection_count",
    "candidate_reference_projection_count",
    "candidate_integer_comparison_filter_count",
    "candidate_non_integer_comparison_filter_count",
    "candidate_conjunction_filter_count",
)

MATRIX_REQUIRED_TEXT = (
    "native:bigint-add-constant",
    "native:bigint-subtract-constant",
    "native:bigint-multiply-constant",
    "native:bigint-compare-constant",
    "native:bigint-add-references",
    "native:bigint-multiply-references",
    "native:int32-to-int16-cast",
    "native:int32-coalesce",
    "native:constant-or-null",
    "contract<abi=full_pipeline",
    "logical=UUID",
    "logical=BLOB",
    "logical=INTERVAL",
    "logical=TIMESTAMP",
    "logical=DECIMAL",
    "logical=UBIGINT",
    "logical=DOUBLE",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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


def row_bool(row: dict, field: str) -> bool:
    value = str(row.get(field, "")).lower()
    return value in ("1", "true", "t")


def event_source_execution(row: dict) -> str:
    selected = row.get("selected_source_execution", "")
    if selected and selected != "none":
        return selected
    return row.get("candidate_source_execution", "")


def parse_pipeline_shape(shape: str) -> list:
    nodes = []
    if not shape:
        return nodes
    for segment in shape.split(";"):
        if segment == "pipeline":
            continue
        parts = segment.split(":", 3)
        if len(parts) != 4:
            continue
        nodes.append({"label": parts[0], "node_kind": parts[1], "operator_name": parts[2], "boundary": parts[3]})
    return nodes


def verify_candidate_abi(case_name: str, row: dict) -> None:
    abi = row.get("candidate_contract_abi", "")
    if abi == "":
        raise AssertionError(f"{case_name}: missing candidate_contract_abi in flow row: {row}")
    if abi not in KNOWN_CANDIDATE_ABIS:
        raise AssertionError(f"{case_name}: unknown candidate_contract_abi in flow row: {row}")


def verify_candidate_signature(case_name: str, row: dict) -> None:
    for field in (
        "candidate_signature_context",
        "candidate_signature_shape",
        "candidate_signature_feature_shape",
        "candidate_signature_context_feature_shape",
        "candidate_contract_shape",
    ):
        if row.get(field, "") in ("", "none"):
            raise AssertionError(f"{case_name}: missing candidate signature field {field}: {row}")


def verify_executable_candidate_abi(case_name: str, row: dict) -> None:
    verify_candidate_signature(case_name, row)
    verify_candidate_abi(case_name, row)
    abi = row.get("candidate_contract_abi", "")
    execution_mode = row.get("execution_mode", "")
    region_execution_form = row.get("region_execution_form", "")
    execution_body = row.get("execution_body", "")
    pipeline_shape = row.get("candidate_pipeline_shape", "")
    nodes = parse_pipeline_shape(pipeline_shape)
    has_source = any(node["node_kind"] == "source" for node in nodes)
    has_sink = any(node["node_kind"] == "sink" for node in nodes)
    if region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
        raise AssertionError(f"{case_name}: executable region has unknown execution form: {row}")
    if region_execution_form == "none":
        raise AssertionError(f"{case_name}: executable region did not declare an execution form: {row}")
    if execution_body == "none":
        raise AssertionError(f"{case_name}: executable region did not declare an execution body: {row}")
    if abi == "full_pipeline":
        if execution_mode not in COMPILED_EXECUTION_MODES:
            raise AssertionError(f"{case_name}: executable region has invalid execution mode: {row}")
        if pipeline_shape and not has_source:
            raise AssertionError(f"{case_name}: full-pipeline executable has no source boundary: {row}")
        if pipeline_shape and not has_sink:
            raise AssertionError(f"{case_name}: full-pipeline executable has no sink boundary: {row}")
        return
    raise AssertionError(f"{case_name}: unsupported executable candidate ABI: {row}")


def verify_execution_body(case_name: str, row: dict) -> None:
    body = row.get("execution_body", "")
    if body not in KNOWN_EXECUTION_BODIES:
        raise AssertionError(f"{case_name}: unknown execution body: {row}")
    status = row.get("status", "")
    phase = row.get("phase", "")
    mode = row.get("execution_mode", "")
    code_size = row_int(row, "code_size")
    if status == "compiled" or phase == "kernel_counter":
        if code_size > 0 and body != "generated-machine-code":
            raise AssertionError(f"{case_name}: generated-code row has wrong execution body: {row}")
        if code_size <= 0 and body != "native-operator-protocol":
            raise AssertionError(f"{case_name}: zero-code compiled row is not a native operator protocol body: {row}")
    elif phase == "runtime" or status == "executed":
        if mode in COMPILED_EXECUTION_MODES and body == "none":
            raise AssertionError(f"{case_name}: runtime row lost execution body: {row}")
    elif body != "none":
        raise AssertionError(f"{case_name}: non-compiled row exposes an execution body: {row}")


def verify_summary(trace_dir: Path, rows: list) -> None:
    cases = {row["case"] for row in rows}
    if cases != EXPECTED_CASES:
        raise AssertionError(f"summary.csv: expected cases {sorted(EXPECTED_CASES)}, found {sorted(cases)}")
    for row in rows:
        case_name = row["case"]
        if not row_bool(row, "validation_pass"):
            raise AssertionError(f"summary.csv: validation failed for {case_name}")
        if row_int(row, "event_count") <= 0:
            raise AssertionError(f"summary.csv: no JIT events recorded for {case_name}")
        if row_int(row, "non_region_events") != 0:
            raise AssertionError(f"summary.csv: non-region JIT target appeared for {case_name}")
        if row_bool(row, "expect_region_compiled") and row_int(row, "compiled_regions") <= 0:
            raise AssertionError(f"summary.csv: expected compiled region event for {case_name}")
        if row_bool(row, "expect_region_unsupported") and row_int(row, "unsupported_regions") <= 0:
            raise AssertionError(f"summary.csv: expected unsupported region event for {case_name}")
        if row_bool(row, "expect_runtime") and row_int(row, "runtime_events") <= 0:
            raise AssertionError(f"summary.csv: expected runtime event for {case_name}")
        for field in ("event_summary_csv", "validation_csv", "events_csv", "counters_csv", "kernel_counters_csv"):
            if not (trace_dir / row[field]).exists():
                raise AssertionError(f"summary.csv: missing {field} target for {case_name}: {row[field]}")
        events = read_csv(trace_dir / row["events_csv"])
        if len(events) != row_int(row, "event_count"):
            raise AssertionError(
                f"{row['events_csv']}: event_count mismatch for {case_name}: "
                f"summary={row['event_count']} actual={len(events)}"
            )
        zero_code_native_compile_events = sum(
            1
            for event in events
            if event.get("phase") == "compile"
            and event.get("execution_mode") == "native"
            and row_int(event, "code_size") == 0
        )
        if zero_code_native_compile_events != row_int(row, "zero_code_native_compile_events"):
            raise AssertionError(f"summary.csv: zero-code native counter mismatch for {case_name}")
        verify_events(case_name, events)
        verify_kernel_counters(case_name, read_csv(trace_dir / row["kernel_counters_csv"]), row)


def verify_events(case_name: str, rows: list) -> None:
    event_ids = []
    for row in rows:
        event_id = row_int(row, "event_id")
        event_ids.append(event_id)
        if row.get("backend_name") not in ("", "sljit"):
            raise AssertionError(f"{case_name}: unexpected backend name in event: {row}")
        if row.get("target") != "region":
            raise AssertionError(f"{case_name}: forbidden non-region JIT target in event: {row}")
        region_execution_form = row.get("region_execution_form", "")
        if row.get("target") == "region" and region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"{case_name}: region event has unknown execution form: {row}")
        verify_execution_body(case_name, row)
        if row.get("status") == "compiled" and row.get("execution_mode") in COMPILED_EXECUTION_MODES:
            if row.get("ir", "") == "":
                raise AssertionError(f"{case_name}: compiled generated event has no IR: {row}")
            if row.get("target") == "region" and region_execution_form == "none":
                raise AssertionError(f"{case_name}: compiled region event did not declare execution form: {row}")
            if row.get("target") == "region":
                verify_candidate_signature(case_name, row)
        if row.get("target") == "region" and row.get("status") in ("compiled", "skipped", "unsupported"):
            has_candidate = row.get("candidate_shape", "") not in ("", "none")
            if row.get("status") == "compiled" and not has_candidate:
                raise AssertionError(f"{case_name}: compiled region event missing candidate: {row}")
            if has_candidate:
                if row.get("candidate_pipeline_shape", "") == "":
                    raise AssertionError(f"{case_name}: region event missing pipeline shape: {row}")
                if row.get("candidate_context_pipeline_shape", "") == "":
                    raise AssertionError(f"{case_name}: region event missing context pipeline shape: {row}")
                for field in CANDIDATE_TRAIT_REQUIRED_COLUMNS:
                    if field not in row:
                        raise AssertionError(f"{case_name}: region event missing candidate trait column {field}: {row}")
                if row.get("status") == "compiled":
                    verify_executable_candidate_abi(case_name, row)
                elif row.get("status") in ("skipped", "unsupported"):
                    verify_candidate_abi(case_name, row)
    if event_ids != sorted(event_ids) or len(event_ids) != len(set(event_ids)):
        raise AssertionError(f"{case_name}: event IDs are not monotonic and unique")
    if case_name == "region_unsupported_unnest_boundary":
        verify_unsupported_unnest_boundary_events(rows)
    if case_name == "sql_equivalence_matrix":
        verify_matrix_events(rows)


def verify_unsupported_unnest_boundary_events(rows: list) -> None:
    has_source_boundary_unsupported = False
    has_unnest_boundary = False
    for row in rows:
        if (
            row.get("phase") == "decision"
            and row.get("target") == "region"
            and row.get("status") == "unsupported"
            and row.get("execution_mode") == "unsupported"
            and row.get("region_execution_form") == "none"
            and row.get("candidate_contract_abi") == "full_pipeline"
            and "source-contract-blocker:requires-source-contract" in row.get("reason", "")
            and "table-function-source-boundary" in row.get("reason", "")
        ):
            has_source_boundary_unsupported = True
        if (
            row.get("phase") == "decision"
            and row.get("target") == "region"
            and row.get("status") == "unsupported"
            and row.get("candidate_contract_abi") == "full_pipeline"
            and "UNNEST:boundary" in row.get("reason", "")
            and "DuckDB physical operator outside generated execution region" in row.get("reason", "")
        ):
            has_unnest_boundary = True
        if row.get("phase") == "runtime":
            raise AssertionError(f"region_unsupported_unnest_boundary: forbidden runtime JIT event: {row}")
        if row.get("status") == "compiled":
            raise AssertionError(f"region_unsupported_unnest_boundary: forbidden compiled JIT event: {row}")
    if not has_source_boundary_unsupported:
        raise AssertionError("region_unsupported_unnest_boundary: missing unsupported source-boundary source-contract evidence")
    if not has_unnest_boundary:
        raise AssertionError("region_unsupported_unnest_boundary: missing UNNEST unsupported-boundary evidence")


def verify_matrix_events(rows: list) -> None:
    text = "\n".join(
        "\n".join(
            str(row.get(field, ""))
            for field in (
                "target",
                "status",
                "execution_mode",
                "candidate_shape",
                "candidate_pipeline_shape",
                "candidate_context_pipeline_shape",
                "candidate_contract_abi",
                "reason",
                "ir",
            )
        )
        for row in rows
    )
    for needle in MATRIX_REQUIRED_TEXT:
        if needle not in text:
            raise AssertionError(f"sql_equivalence_matrix: missing required trace text: {needle}")
    compiled_region_shapes = {
        row.get("candidate_shape")
        for row in rows
        if row.get("target") == "region" and row.get("status") == "compiled"
    }
    for shape in ("projection-sink", "filter-projection-sink"):
        if shape not in compiled_region_shapes:
            raise AssertionError(f"sql_equivalence_matrix: missing compiled region shape: {shape}")
    has_unsupported_ctas_sink_boundary = any(
        row.get("target") == "region"
        and row.get("status") == "unsupported"
        and row.get("candidate_shape") == "filter-projection-sink"
        and row.get("region_execution_form") == "none"
        and "CREATE_TABLE_AS" in row.get("reason", "")
        and "missing-contract" in row.get("reason", "")
        for row in rows
    )
    if not has_unsupported_ctas_sink_boundary:
        raise AssertionError("sql_equivalence_matrix: missing unsupported CTAS sink-boundary evidence")


def verify_kernel_counters(case_name: str, rows: list, summary_row: dict) -> None:
    for row in rows:
        verify_execution_body(case_name, {"status": "compiled", **row})
    if row_bool(summary_row, "expect_runtime"):
        if not rows:
            raise AssertionError(f"{case_name}: expected kernel counters")
        if sum(row_int(row, "input_rows") for row in rows) <= 0:
            raise AssertionError(f"{case_name}: expected positive kernel counter input rows")
    if case_name == "region_unsupported_unnest_boundary":
        if rows:
            raise AssertionError(f"region_unsupported_unnest_boundary: unsupported boundary should not create kernels: {rows}")


def flow_rows_for_case(rows: list, case_name: str, include_kernel_counters: bool | None = None) -> list:
    result = [row for row in rows if row.get("case") == case_name]
    if include_kernel_counters is None:
        return result
    if include_kernel_counters:
        return [row for row in result if row.get("phase") == "kernel_counter"]
    return [row for row in result if row.get("phase") != "kernel_counter"]


def sum_flow(rows: list, field: str, **filters) -> int:
    total = 0
    for row in rows:
        if all(row.get(key, "") == value for key, value in filters.items()):
            total += row_int(row, field)
    return total


def verify_flow_step_summary(trace_dir: Path, rows: list, summary_rows: list, manifest: dict) -> None:
    if "flow_step_summary.csv" not in manifest["artifacts"]:
        raise AssertionError("trace_manifest.json: missing flow_step_summary.csv")
    if manifest.get("configuration", {}).get("flow_step_rows") != len(rows):
        raise AssertionError(
            "trace_manifest.json: flow_step_rows mismatch: "
            f"manifest={manifest.get('configuration', {}).get('flow_step_rows')} actual={len(rows)}"
        )
    if not rows:
        raise AssertionError("flow_step_summary.csv: expected rows")

    cases = {row.get("case", "") for row in rows}
    if cases != EXPECTED_CASES:
        raise AssertionError(f"flow_step_summary.csv: expected cases {sorted(EXPECTED_CASES)}, found {sorted(cases)}")

    summary_by_case = {row["case"]: row for row in summary_rows}
    for row in rows:
        case_name = row.get("case", "")
        phase = row.get("phase", "")
        target = row.get("target", "")
        status = row.get("status", "")
        execution_mode = row.get("execution_mode", "")
        region_execution_form = row.get("region_execution_form", "")
        candidate_shape = row.get("candidate_shape", "")
        if target != "region":
            raise AssertionError(f"{case_name}: forbidden non-region JIT target in flow row: {row}")
        if target == "region" and region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"{case_name}: flow row has unknown execution form: {row}")
        verify_execution_body(case_name, row)

        if phase == "kernel_counter":
            if row_int(row, "event_count") != 0:
                raise AssertionError(f"{case_name}: kernel counter flow row has event count: {row}")
            if row_int(row, "kernel_count") <= 0:
                raise AssertionError(f"{case_name}: kernel counter flow row has no kernels: {row}")
            if row_int(row, "kernel_count") != row_int(row, "reached_kernels") + row_int(row, "unreached_kernels"):
                raise AssertionError(f"{case_name}: kernel/reached/unreached mismatch: {row}")
            if row_int(row, "kernel_count") != row_int(row, "row_processing_kernels") + row_int(row, "zero_input_kernels"):
                raise AssertionError(f"{case_name}: kernel/row/zero-input mismatch: {row}")
            if row_int(row, "input_rows") > 0 and row_int(row, "row_processing_kernels") <= 0:
                raise AssertionError(f"{case_name}: positive input without row-processing kernel: {row}")
            if row_int(row, "invocations") > 0 and row_int(row, "reached_kernels") <= 0:
                raise AssertionError(f"{case_name}: positive invocations without reached kernel: {row}")
        else:
            if row_int(row, "event_count") <= 0:
                raise AssertionError(f"{case_name}: event flow row has no events: {row}")
            if row_int(row, "kernel_count") != 0:
                raise AssertionError(f"{case_name}: event flow row has kernel count: {row}")

        if target == "region" and candidate_shape != "none":
            is_executable = (
                status == "compiled"
                or phase == "kernel_counter"
                or (phase == "runtime" and execution_mode in COMPILED_EXECUTION_MODES)
            )
            if is_executable:
                verify_executable_candidate_abi(case_name, row)
            else:
                verify_candidate_abi(case_name, row)
    for case_name, summary_row in summary_by_case.items():
        event_rows = flow_rows_for_case(rows, case_name, include_kernel_counters=False)
        kernel_rows = flow_rows_for_case(rows, case_name, include_kernel_counters=True)

        if sum_flow(event_rows, "event_count") != row_int(summary_row, "event_count"):
            raise AssertionError(f"{case_name}: flow/event count mismatch")
        non_region_events = sum(row_int(row, "event_count") for row in event_rows if row.get("target", "") != "region")
        if non_region_events != row_int(summary_row, "non_region_events"):
            raise AssertionError(f"{case_name}: flow/non_region_events mismatch")
        expected_counts = (
            ("compiled_regions", {"target": "region", "status": "compiled"}),
            ("skipped_regions", {"target": "region", "status": "skipped"}),
            ("unsupported_regions", {"target": "region", "status": "unsupported"}),
            ("runtime_events", {"phase": "runtime"}),
        )
        for summary_field, filters in expected_counts:
            if sum_flow(event_rows, "event_count", **filters) != row_int(summary_row, summary_field):
                raise AssertionError(f"{case_name}: flow/{summary_field} mismatch")

        for summary_field, flow_field in (
            ("runtime_input_rows", "input_rows"),
            ("runtime_output_rows", "output_rows"),
            ("runtime_invocations", "invocations"),
            ("runtime_time_us", "runtime_time_us"),
            ("source_contract_output_rows", "source_contract_output_rows"),
            ("source_contract_invocations", "source_contract_invocations"),
            ("source_contract_runtime_time_us", "source_contract_runtime_time_us"),
            ("generated_body_runtime_time_us", "generated_body_runtime_time_us"),
        ):
            if sum_flow(event_rows, flow_field, phase="runtime") != row_int(summary_row, summary_field):
                raise AssertionError(f"{case_name}: flow/{summary_field} mismatch")

        for field in (
            "ir_lowering_time_us",
            "backend_analysis_time_us",
            "admission_time_us",
            "overlap_check_time_us",
            "codegen_time_us",
        ):
            if sum_flow(event_rows, field) != row_int(summary_row, field):
                raise AssertionError(f"{case_name}: flow/{field} mismatch")

        kernel_counter_rows = read_csv(trace_dir / summary_row["kernel_counters_csv"])
        if sum_flow(kernel_rows, "kernel_count") != len(kernel_counter_rows):
            raise AssertionError(f"{case_name}: flow/kernel counter count mismatch")
        for summary_field, flow_field in (
            ("input_rows", "input_rows"),
            ("output_rows", "output_rows"),
            ("invocation_count", "invocations"),
            ("runtime_time_us", "runtime_time_us"),
            ("source_contract_output_rows", "source_contract_output_rows"),
            ("source_contract_invocation_count", "source_contract_invocations"),
            ("source_contract_runtime_time_us", "source_contract_runtime_time_us"),
            ("generated_body_runtime_time_us", "generated_body_runtime_time_us"),
            ("compile_time_us", "compile_time_us"),
            ("code_size", "code_size"),
        ):
            actual = sum(row_int(row, summary_field) for row in kernel_counter_rows)
            observed = sum_flow(kernel_rows, flow_field)
            if actual != observed:
                raise AssertionError(
                    f"{case_name}: flow/kernel {summary_field} mismatch: expected={actual} observed={observed}"
                )


def parse_trace_cases(value: str) -> set:
    return {entry for entry in value.split(";") if entry}


def verify_test_surface_coverage(trace_dir: Path, rows: list, manifest: dict) -> None:
    if "test_surface_coverage.csv" not in manifest["artifacts"]:
        raise AssertionError("trace_manifest.json: missing test_surface_coverage.csv")
    if not rows:
        raise AssertionError("test_surface_coverage.csv: expected rows")

    root = repo_root()
    sql_test_files = {
        path.relative_to(root).as_posix()
        for path in sorted((root / "test" / "sql" / "jit").glob("*.test"))
    }
    covered_sql_files = {
        row["source_path"] for row in rows if row["test_kind"] == "sqllogictest"
    }
    if covered_sql_files != sql_test_files:
        raise AssertionError(
            "test_surface_coverage.csv: SQL JIT test coverage mismatch: "
            f"expected={sorted(sql_test_files)} actual={sorted(covered_sql_files)}"
        )

    api_test_path = root / "test" / "api" / "test_jit.cpp"
    api_test_names = {
        match.group(1) for match in TEST_CASE_RE.finditer(api_test_path.read_text(encoding="utf-8"))
    }
    expected_surface_rows = {
        (source_path, "sqllogictest", source_path) for source_path in sql_test_files
    } | {
        ("test/api/test_jit.cpp", "catch2", test_name) for test_name in api_test_names
    }
    actual_surface_rows = [
        (row["source_path"], row["test_kind"], row["test_name"]) for row in rows
    ]
    duplicate_surface_rows = sorted(
        {
            surface_row
            for surface_row in actual_surface_rows
            if actual_surface_rows.count(surface_row) > 1
        }
    )
    if duplicate_surface_rows:
        raise AssertionError(f"test_surface_coverage.csv: duplicate test surface rows {duplicate_surface_rows}")
    actual_surface_row_set = set(actual_surface_rows)
    if actual_surface_row_set != expected_surface_rows:
        raise AssertionError(
            "test_surface_coverage.csv: exact test surface mismatch: "
            f"missing={sorted(expected_surface_rows - actual_surface_row_set)} "
            f"unexpected={sorted(actual_surface_row_set - expected_surface_rows)}"
        )
    covered_api_names = {
        row["test_name"]
        for row in rows
        if row["source_path"] == "test/api/test_jit.cpp" and row["test_kind"] == "catch2"
    }
    if covered_api_names != api_test_names:
        raise AssertionError(
            "test_surface_coverage.csv: API JIT test coverage mismatch: "
            f"expected={sorted(api_test_names - covered_api_names)} missing, "
            f"unexpected={sorted(covered_api_names - api_test_names)}"
        )

    covered_trace_cases = set()
    for row in rows:
        source_path = root / row["source_path"]
        if not source_path.exists():
            raise AssertionError(f"test_surface_coverage.csv: missing source path {row['source_path']}")
        if row["verification_route"] == "":
            raise AssertionError(f"test_surface_coverage.csv: empty verification route: {row}")
        trace_cases = parse_trace_cases(row["trace_cases"])
        if trace_cases - EXPECTED_CASES:
            raise AssertionError(f"test_surface_coverage.csv: unknown trace cases in row: {row}")
        if "focused_sql_trace" in row["verification_route"] and not trace_cases:
            raise AssertionError(f"test_surface_coverage.csv: focused trace route without trace cases: {row}")
        if row["coverage_area"] == "":
            raise AssertionError(f"test_surface_coverage.csv: empty coverage area: {row}")
        covered_trace_cases.update(trace_cases)
    if covered_trace_cases != EXPECTED_CASES:
        raise AssertionError(
            "test_surface_coverage.csv: expected every focused trace case to map to tests, "
            f"found={sorted(covered_trace_cases)}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify jit_sql_trace.py artifacts")
    parser.add_argument("trace_dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trace_dir = args.trace_dir.resolve()
    manifest = verify_trace_manifest(
        trace_dir,
        kind="focused_sql_jit_trace",
        required_artifacts=["summary.csv", "test_surface_coverage.csv", "flow_step_summary.csv"],
    )
    summary_rows = read_csv(trace_dir / "summary.csv")
    for row in summary_rows:
        for field in ("event_summary_csv", "validation_csv", "events_csv", "counters_csv", "kernel_counters_csv"):
            if row[field] not in manifest["artifacts"]:
                raise AssertionError(f"trace_manifest.json: missing summary-owned artifact {row[field]}")
    manifest_cases = manifest.get("configuration", {}).get("cases", [])
    if set(manifest_cases) != EXPECTED_CASES:
        raise AssertionError(f"trace_manifest.json: unexpected cases {manifest_cases}")
    coverage_rows = read_csv(trace_dir / "test_surface_coverage.csv")
    verify_test_surface_coverage(trace_dir, coverage_rows, manifest)
    flow_rows = read_csv(trace_dir / "flow_step_summary.csv")
    verify_flow_step_summary(trace_dir, flow_rows, summary_rows, manifest)
    verify_summary(trace_dir, summary_rows)
    print(f"ok cases={len(summary_rows)} test_surface={len(coverage_rows)} flow_steps={len(flow_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
