#!/usr/bin/env python3
#
# Verify TPC-H JIT trace artifacts produced by tpch_trace.py.

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest

from tpch_schema import (
    AGGREGATE_OPERATORS,
    ADMISSION_EFFICIENCY_SUMMARY_FIELDS,
    ADMISSION_PROOF_GAP_SUMMARY_FIELDS,
    ADMISSION_SUMMARY_FIELDS,
    CANDIDATE_TRAIT_FIELDS,
    CAPABILITY_PRIORITY_SUMMARY_FIELDS,
    CAPABILITY_RUNTIME_FIELDS,
    CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES,
    DECISION_COUNTER_SUMMARY_FIELDS,
    FLOW_STEP_SUMMARY_FIELDS,
    FUSION_BLOCKER_SUMMARY_FIELDS,
    JOIN_OPERATORS,
    KNOWN_ADMISSION_PROOF_ROOT_CAUSES,
    KNOWN_ADMISSION_PROOF_STATUSES,
    KNOWN_CANDIDATE_ABIS,
    KNOWN_CANDIDATE_SCOPE_SUMMARY_VALUES,
    KNOWN_CANDIDATE_SCOPES,
    KNOWN_REGION_EXECUTION_FORMS,
    KNOWN_SOURCE_BOUNDARY_KINDS,
    MATERIALIZATION_OPERATORS,
    PROFILE_WRAPPER_OPERATORS,
    QUERY_CAPABILITY_PRIORITY_SUMMARY_FIELDS,
    REGION_DECISION_SUMMARY_FIELDS,
    SORT_OPERATORS,
    SOURCE_BOUNDARY_SUMMARY_FIELDS,
    SOURCE_FUSION_GAP_SUMMARY_FIELDS,
    SOURCE_NATIVE_RUNTIME_FIELDS,
    STAGE_FIELDS,
    TRACE_WRAPPER_OPERATORS,
    configure_csv_field_size_limit,
)

configure_csv_field_size_limit()

def wrapper_only_pipeline_shape(source: str, sink: str = "") -> str:
    result = "pipeline;source:source:" + source + ":source-missing-protocol"
    if sink:
        result += ";sink:sink:" + sink + ":sink"
    return result


def wrapper_only_region_source_marker(source: str) -> str:
    return "source:" + source + ":fallback"


WRAPPER_ONLY_PIPELINE_SHAPES = {
    wrapper_only_pipeline_shape("CREATE_TABLE_AS", "RESULT_COLLECTOR"),
    wrapper_only_pipeline_shape("RESULT_COLLECTOR"),
    wrapper_only_pipeline_shape("EXPLAIN_ANALYZE"),
}
WRAPPER_ONLY_REGION_SOURCE_MARKERS = tuple(
    wrapper_only_region_source_marker(source)
    for source in ("CREATE_TABLE_AS", "RESULT_COLLECTOR", "EXPLAIN_ANALYZE")
)
SUMMARY_FILES = (
    "summary.csv",
    "query_gap_summary.csv",
    "operator_gap_summary.csv",
    "capability_gap_summary.csv",
    "capability_priority_summary.csv",
    "query_capability_priority_summary.csv",
    "expression_fallback_summary.csv",
    "source_boundary_summary.csv",
    "source_boundary_priority_summary.csv",
    "source_fusion_gap_summary.csv",
    "fusion_blocker_summary.csv",
    "region_decision_summary.csv",
    "stage_pipeline_summary.csv",
    "kernel_runtime_summary.csv",
    "admission_efficiency_summary.csv",
    "admission_proof_gap_summary.csv",
    "pipeline_runtime_summary.csv",
    "flow_step_summary.csv",
    "decision_counter_summary.csv",
    "operator_profile_summary.csv",
)

REGION_DECISION_REQUIRED_COLUMNS = REGION_DECISION_SUMMARY_FIELDS
ADMISSION_EFFICIENCY_REQUIRED_COLUMNS = ADMISSION_EFFICIENCY_SUMMARY_FIELDS
ADMISSION_PROOF_GAP_REQUIRED_COLUMNS = ADMISSION_PROOF_GAP_SUMMARY_FIELDS
FLOW_STEP_REQUIRED_COLUMNS = FLOW_STEP_SUMMARY_FIELDS
DECISION_COUNTER_REQUIRED_COLUMNS = DECISION_COUNTER_SUMMARY_FIELDS
KNOWN_CAPABILITY_GAPS = {
    "scan_source_boundary",
    "join_operator_boundary",
    "aggregate_state_or_sink_boundary",
    "sort_topn_boundary",
    "materialization_boundary",
    "expression_fallback",
    "sink_boundary",
    "operator_fallback_boundary",
    "source_boundary",
    "source_executor_fallback",
    "source_missing_protocol",
    "other_boundary",
}
CAPABILITY_PRIORITY_REQUIRED_COLUMNS = CAPABILITY_PRIORITY_SUMMARY_FIELDS
QUERY_CAPABILITY_PRIORITY_REQUIRED_COLUMNS = QUERY_CAPABILITY_PRIORITY_SUMMARY_FIELDS
SOURCE_BOUNDARY_HASH_JOIN_REQUIRED_FIELDS = (
    "native_state_scan_contract_status",
    "native_state_scan_required_capability",
    "native_state_scan_protocol",
    "native_state_scan_blocker",
    "native_hash_join_probe_contract_status",
    "native_hash_join_probe_required_capability",
    "native_hash_join_probe_protocol",
    "native_hash_join_probe_blocker",
    "native_hash_join_build_contract_status",
    "native_hash_join_build_required_capability",
    "native_hash_join_build_protocol",
    "native_hash_join_build_blocker",
    "join_type",
    "condition_count",
    "equality_condition_count",
    "non_equality_condition_count",
    "null_equal_condition_count",
    "condition_types",
    "comparison_ops",
    "payload_columns",
    "payload_column_indices",
    "payload_types",
    "lhs_output_columns",
    "lhs_output_column_indices",
    "lhs_output_types",
    "rhs_output_columns",
    "rhs_output_types",
    "lhs_probe_columns",
    "lhs_probe_column_indices",
    "lhs_probe_types",
    "lhs_output_in_probe",
    "delim_types",
    "correlated_mark_counts_required",
    "residual_predicate",
    "residual_info",
    "filter_pushdown",
    "filter_pushdown_condition_count",
    "filter_pushdown_probe_count",
    "build_side_has_filter",
    "source_produces_rows",
    "regular_hash_table_layout_ready",
    "native_probe_shape_ready",
    "native_probe_shape_blocker",
    "native_probe_output_mode",
    "build_append_shape_ready",
    "build_append_shape_blocker",
    "hash_join_layout_column_count",
    "hash_join_layout_offsets",
    "hash_join_tuple_size",
    "hash_join_entry_size",
    "hash_join_pointer_offset",
    "hash_join_hash_column_index",
    "hash_join_found_match_column_present",
    "hash_join_found_match_column_index",
    "hash_join_native_protocol_blocker",
)
SOURCE_BOUNDARY_HASH_JOIN_NUMERIC_FIELDS = (
    "condition_count",
    "equality_condition_count",
    "non_equality_condition_count",
    "null_equal_condition_count",
    "payload_columns",
    "lhs_output_columns",
    "rhs_output_columns",
    "lhs_probe_columns",
    "lhs_output_in_probe",
    "delim_types",
    "filter_pushdown_condition_count",
    "filter_pushdown_probe_count",
    "hash_join_layout_column_count",
    "hash_join_tuple_size",
    "hash_join_entry_size",
    "hash_join_pointer_offset",
    "hash_join_hash_column_index",
    "hash_join_found_match_column_index",
)
SOURCE_BOUNDARY_AGGREGATE_REQUIRED_FIELDS = (
    "native_state_scan_contract_status",
    "native_state_scan_required_capability",
    "native_state_scan_protocol",
    "native_state_scan_blocker",
    "aggregate_operator_kind",
    "group_count",
    "group_types",
    "aggregate_count",
    "aggregate_functions",
    "aggregate_return_types",
    "aggregate_child_counts",
    "aggregate_types",
    "aggregate_filter_count",
    "aggregate_order_count",
    "payload_type_count",
    "payload_types",
    "grouping_set_count",
    "grouping_function_count",
    "radix_table_count",
    "distinct_aggregate_count",
    "distinct_table_count",
    "distinct_child_count",
    "input_group_type_count",
    "input_group_types",
    "non_distinct_filter_count",
    "distinct_filter_count",
)
SOURCE_BOUNDARY_AGGREGATE_NUMERIC_FIELDS = (
    "group_count",
    "aggregate_count",
    "aggregate_filter_count",
    "aggregate_order_count",
    "payload_type_count",
    "grouping_set_count",
    "grouping_function_count",
    "radix_table_count",
    "distinct_aggregate_count",
    "distinct_table_count",
    "distinct_child_count",
    "input_group_type_count",
    "non_distinct_filter_count",
    "distinct_filter_count",
)
SOURCE_BOUNDARY_REQUIRED_COLUMNS = SOURCE_BOUNDARY_SUMMARY_FIELDS
SOURCE_FUSION_GAP_REQUIRED_COLUMNS = SOURCE_FUSION_GAP_SUMMARY_FIELDS
FUSION_BLOCKER_REQUIRED_COLUMNS = FUSION_BLOCKER_SUMMARY_FIELDS


def read_csv(path: Path) -> list:
    if not path.exists():
        raise AssertionError(f"missing required trace artifact: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def verify_required_columns(name: str, rows: list, fields: tuple) -> None:
    if not rows:
        return
    missing = [field for field in fields if field not in rows[0]]
    if missing:
        raise AssertionError(f"{name}: missing required columns {missing}")


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


def event_source_execution(row: dict) -> str:
    selected = row.get("selected_source_execution", "")
    if selected and selected != "none":
        return selected
    return row.get("candidate_source_execution", "")


def reason_contains_proof(reason: str, proof: str) -> bool:
    if proof in reason:
        return True
    if not reason.endswith("..."):
        return False
    proof_marker = ";proof="
    proof_index = reason.find(proof_marker)
    if proof_index < 0:
        return False
    retained_proof = reason[proof_index + len(proof_marker) : -3]
    return retained_proof != "" and proof.startswith(retained_proof)


def verify_bool_field(artifact: str, row: dict, field: str) -> None:
    if row[field] not in {"true", "false"}:
        raise AssertionError(f"{artifact}: field {field} must be true or false: {row}")


def bracket_list_count(value: str) -> int:
    if value == "[]":
        return 0
    if not value.startswith("[") or not value.endswith("]"):
        raise ValueError(f"invalid bracket list: {value}")
    return len(value[1:-1].split("|"))


def hash_aggregate_missing_grouped_state_blocker(row: dict) -> str:
    if (
        row_int(row, "distinct_aggregate_count") != 0
        or row_int(row, "distinct_table_count") != 0
        or row_int(row, "distinct_child_count") != 0
        or row_int(row, "distinct_filter_count") != 0
    ):
        return "hash-aggregate-distinct-grouped-state-protocol-boundary"
    return "grouped-state-protocol-boundary"


def hash_aggregate_missing_lookup_blocker(row: dict) -> str:
    if (
        row_int(row, "distinct_aggregate_count") != 0
        or row_int(row, "distinct_table_count") != 0
        or row_int(row, "distinct_child_count") != 0
        or row_int(row, "distinct_filter_count") != 0
    ):
        return "hash-aggregate-distinct-lookup-protocol-boundary"
    return "hash-aggregate-lookup-boundary"


def verify_missing_grouped_state_contract(
    artifact: str, row: dict, expected_capability: str, expected_blocker: str = "grouped-state-protocol-boundary"
) -> None:
    expected = {
        "native_grouped_state_contract_status": "missing",
        "native_grouped_state_required_capability": expected_capability,
        "native_grouped_state_protocol": "v1",
        "native_grouped_state_blocker": expected_blocker,
    }
    for field, expected_value in expected.items():
        if row[field] != expected_value:
            raise AssertionError(
                f"{artifact}: grouped aggregate state contract field {field} mismatch: {row}"
            )


def verify_ready_grouped_state_contract(artifact: str, row: dict, expected_capability: str) -> None:
    expected = {
        "native_grouped_state_contract_status": "ready",
        "native_grouped_state_required_capability": expected_capability,
        "native_grouped_state_protocol": "v1",
        "native_grouped_state_blocker": "none",
    }
    for field, expected_value in expected.items():
        if row[field] != expected_value:
            raise AssertionError(
                f"{artifact}: grouped aggregate state contract field {field} mismatch: {row}"
            )


def hash_grouped_state_contract_should_be_ready(row: dict) -> bool:
    return (
        row_int(row, "grouping_set_count") == 1
        and row_int(row, "radix_table_count") == 1
        and row_int(row, "distinct_aggregate_count") == 0
        and row_int(row, "aggregate_filter_count") == 0
        and row_int(row, "aggregate_order_count") == 0
        and row_int(row, "non_distinct_filter_count") == row_int(row, "aggregate_count")
    )


def verify_grouped_state_layout_contract(artifact: str, row: dict) -> None:
    if row["grouped_state_layout_ready"] != "true":
        raise AssertionError(f"{artifact}: grouped aggregate state layout is not ready: {row}")
    aggregate_count = row_int(row, "aggregate_count")
    if row["grouped_state_offsets"] == "" or row["grouped_state_payload_sizes"] == "":
        raise AssertionError(f"{artifact}: grouped aggregate state layout fields are missing: {row}")
    if bracket_list_count(row["grouped_state_offsets"]) != aggregate_count:
        raise AssertionError(f"{artifact}: grouped aggregate state offset count does not match aggregates: {row}")
    if bracket_list_count(row["grouped_state_payload_sizes"]) != aggregate_count:
        raise AssertionError(f"{artifact}: grouped aggregate state payload size count does not match aggregates: {row}")


def verify_state_scan_contract(
    artifact: str,
    row: dict,
    expected_capability: str,
    expected_blocker: str,
    expected_status: str = "missing",
) -> None:
    expected = {
        "native_state_scan_contract_status": expected_status,
        "native_state_scan_required_capability": expected_capability,
        "native_state_scan_protocol": "v1",
        "native_state_scan_blocker": expected_blocker,
    }
    for field, expected_value in expected.items():
        if row[field] != expected_value:
            raise AssertionError(f"{artifact}: native state-scan contract field {field} mismatch: {row}")


def verify_native_operator_contract(
    artifact: str,
    row: dict,
    prefix: str,
    expected_status: str,
    expected_capability: str,
    expected_blocker: str,
) -> None:
    expected = {
        f"{prefix}_contract_status": expected_status,
        f"{prefix}_required_capability": expected_capability,
        f"{prefix}_protocol": "v1",
        f"{prefix}_blocker": expected_blocker,
    }
    for field, expected_value in expected.items():
        if row[field] != expected_value:
            raise AssertionError(f"{artifact}: native operator contract field {field} mismatch: {row}")


def verify_hash_join_native_contracts(artifact: str, row: dict) -> None:
    if row["regular_hash_table_layout_ready"] != "true":
        raise AssertionError(f"{artifact}: hash join regular layout is not ready: {row}")
    if row["hash_join_layout_offsets"] == "":
        raise AssertionError(f"{artifact}: hash join layout offsets are missing: {row}")
    for field in ("hash_join_found_match_column_present",):
        verify_bool_field(artifact, row, field)
    for ready_field, blocker_field in (
        ("native_probe_shape_ready", "native_probe_shape_blocker"),
        ("build_append_shape_ready", "build_append_shape_blocker"),
    ):
        verify_bool_field(artifact, row, ready_field)
        if row[ready_field] == "true" and row[blocker_field] != "none":
            raise AssertionError(f"{artifact}: ready hash-join shape has a blocker in {blocker_field}: {row}")
        if row[ready_field] == "false" and row[blocker_field] == "none":
            raise AssertionError(f"{artifact}: blocked hash-join shape lacks a blocker in {blocker_field}: {row}")

    common_blocker = row["hash_join_native_protocol_blocker"]
    if common_blocker == "hash-join-native-delimiter-state":
        raise AssertionError(f"{artifact}: stale delimiter-state hash join blocker: {row}")
    if row["join_type"] == "left" and row_int(row, "delim_types") > 0 and common_blocker != "none":
        raise AssertionError(f"{artifact}: left delimiter hash join should use the native protocol: {row}")
    if common_blocker == "none":
        verify_native_operator_contract(
            artifact,
            row,
            "native_hash_join_probe",
            "ready",
            "hash-join-native-probe",
            "none",
        )
        build_status = "ready"
        build_blocker = "none"
    else:
        verify_native_operator_contract(
            artifact,
            row,
            "native_hash_join_probe",
            "missing",
            "hash-join-native-probe",
            common_blocker,
        )
        build_status = "missing"
        build_blocker = common_blocker

    verify_native_operator_contract(
        artifact,
        row,
        "native_hash_join_build",
        build_status,
        "hash-join-native-build",
        build_blocker,
    )


def verify_empty_native_operator_contract(artifact: str, row: dict, prefix: str) -> None:
    for suffix in ("contract_status", "required_capability", "protocol", "blocker"):
        field = f"{prefix}_{suffix}"
        if row[field] != "":
            raise AssertionError(f"{artifact}: unexpected native operator contract field {field}: {row}")


def row_correctness_diff(row: dict) -> int:
    return row_int(row, "result_minus_baseline") + row_int(row, "baseline_minus_result")


def expected_query_ids(count: int) -> list:
    return [f"{query_id:02d}" for query_id in range(1, count + 1)]


def parse_pipeline_shape(pipeline_shape: str) -> list:
    nodes = []
    for segment in str(pipeline_shape).split(";"):
        if segment == "pipeline" or not segment:
            continue
        parts = segment.split(":", 3)
        if len(parts) != 4:
            continue
        nodes.append(
            {
                "role": parts[0],
                "node_kind": parts[1],
                "operator_name": parts[2],
                "boundary": parts[3],
            }
        )
    return nodes


def is_trace_wrapper_node(node: dict) -> bool:
    return node["operator_name"] in TRACE_WRAPPER_OPERATORS


def is_trace_wrapper_pipeline_shape(pipeline_shape: str) -> bool:
    return any(is_trace_wrapper_node(node) for node in parse_pipeline_shape(pipeline_shape))


def is_gap_pipeline_node(node: dict) -> bool:
    if is_trace_wrapper_node(node):
        return False
    if node["boundary"] in ("source-native", "operator-native", "sink-native"):
        return False
    return node["boundary"] != "none" or node["node_kind"] in ("operator", "source", "sink")


def classify_capability_gap(node: dict) -> str:
    operator_name = node["operator_name"]
    boundary = node["boundary"]
    if boundary == "scan" or operator_name == "TABLE_SCAN":
        return "scan_source_boundary"
    if operator_name in JOIN_OPERATORS:
        return "join_operator_boundary"
    if operator_name in AGGREGATE_OPERATORS:
        return "aggregate_state_or_sink_boundary"
    if operator_name in SORT_OPERATORS:
        return "sort_topn_boundary"
    if operator_name in MATERIALIZATION_OPERATORS:
        return "materialization_boundary"
    if boundary == "expression-fallback":
        return "expression_fallback"
    if boundary == "sink":
        return "sink_boundary"
    if boundary == "operator-fallback":
        return "operator_fallback_boundary"
    if boundary == "source-missing-protocol":
        return "source_missing_protocol"
    if boundary == "source-executor-fallback":
        return "source_executor_fallback"
    if boundary == "source-boundary":
        return "source_boundary"
    return "other_boundary"


def new_capability_runtime_entry() -> dict:
    return {field: 0 for field in CAPABILITY_RUNTIME_FIELDS}


def accumulate_capability_runtime_entry(entry: dict, kernel_row: dict) -> None:
    entry["runtime_input_rows"] += row_int(kernel_row, "input_rows")
    entry["runtime_output_rows"] += row_int(kernel_row, "output_rows")
    entry["runtime_invocations"] += row_int(kernel_row, "invocations")
    entry["runtime_time_us"] += row_int(kernel_row, "runtime_time_us")
    entry["source_native_output_rows"] += row_int(kernel_row, "source_native_output_rows")
    entry["source_native_invocations"] += row_int(kernel_row, "source_native_invocations")
    entry["source_native_runtime_time_us"] += row_int(kernel_row, "source_native_runtime_time_us")
    entry["generated_body_runtime_time_us"] += row_int(kernel_row, "generated_body_runtime_time_us")


def collect_capability_runtime_from_kernel_rows(kernel_rows: list) -> tuple:
    workload_summary = {}
    query_summary = {}
    for kernel_row in kernel_rows:
        pipeline_shape = kernel_row["candidate_pipeline_shape"]
        if is_trace_wrapper_pipeline_shape(pipeline_shape):
            continue
        seen_gaps = set()
        for node in parse_pipeline_shape(pipeline_shape):
            if not is_gap_pipeline_node(node):
                continue
            capability_gap = classify_capability_gap(node)
            if capability_gap in seen_gaps:
                continue
            seen_gaps.add(capability_gap)
            workload_key = (
                kernel_row["policy"],
                "compiled",
                kernel_row["execution_mode"],
                kernel_row["region_execution_form"],
                capability_gap,
            )
            query_key = (
                kernel_row["query"],
                kernel_row["policy"],
                "compiled",
                kernel_row["execution_mode"],
                kernel_row["region_execution_form"],
                capability_gap,
            )
            accumulate_capability_runtime_entry(
                workload_summary.setdefault(workload_key, new_capability_runtime_entry()), kernel_row
            )
            accumulate_capability_runtime_entry(
                query_summary.setdefault(query_key, new_capability_runtime_entry()), kernel_row
            )
    return workload_summary, query_summary


def assert_no_missing_pipeline_shape(name: str, rows: list) -> None:
    for row in rows:
        for field, value in row.items():
            if not field.endswith("pipeline_shape"):
                continue
            if row.get("candidate_shape", "none") in ("", "none"):
                continue
            if value == "":
                raise AssertionError(f"{name}: missing {field} for query {row.get('query', '')}")


def verify_candidate_scope(name: str, row: dict) -> None:
    scope = row.get("candidate_scope", "")
    abi = row.get("candidate_contract_abi", "")
    if scope == "":
        raise AssertionError(f"{name}: missing candidate_scope: {row}")
    if scope not in KNOWN_CANDIDATE_SCOPES:
        raise AssertionError(f"{name}: unknown candidate_scope {scope}: {row}")
    if abi and abi not in KNOWN_CANDIDATE_ABIS:
        raise AssertionError(f"{name}: unknown candidate_contract_abi {abi}: {row}")


def verify_candidate_scope_summary_value(name: str, row: dict, field: str, allow_no_candidate: bool = False) -> None:
    scope = row.get(field, "")
    known_scopes = KNOWN_CANDIDATE_SCOPE_SUMMARY_VALUES if allow_no_candidate else KNOWN_CANDIDATE_SCOPES
    if scope == "":
        raise AssertionError(f"{name}: missing {field}: {row}")
    if scope not in known_scopes:
        raise AssertionError(f"{name}: unknown {field} {scope}: {row}")


def verify_executable_candidate_scope(name: str, row: dict) -> None:
    verify_candidate_scope(name, row)
    scope = row.get("candidate_scope", "")
    abi = row.get("candidate_contract_abi", "")
    execution_mode = row.get("execution_mode", "")
    region_execution_form = row.get("region_execution_form", "")
    nodes = parse_pipeline_shape(row.get("candidate_pipeline_shape", ""))
    has_source = any(node["node_kind"] == "source" for node in nodes)
    has_sink = any(node["node_kind"] == "sink" for node in nodes)
    if region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
        raise AssertionError(f"{name}: executable region has unknown execution form: {row}")
    if region_execution_form == "none":
        raise AssertionError(f"{name}: executable region did not declare an execution form: {row}")
    if scope == "full_pipeline":
        if execution_mode != "native":
            raise AssertionError(f"{name}: full-pipeline executable has invalid execution mode: {row}")
        if not has_source:
            raise AssertionError(f"{name}: full-pipeline executable shape has no source node: {row}")
        if abi == "full_pipeline" and not has_sink:
            raise AssertionError(f"{name}: full-pipeline executable shape has no sink node: {row}")
        if abi == "state_scan" and has_sink:
            raise AssertionError(f"{name}: state-scan executable shape contains sink boundary: {row}")
        return
    raise AssertionError(f"{name}: unsupported executable candidate scope: {row}")


def verify_admission_metadata(name: str, row: dict) -> None:
    status = row.get("status", "")
    target = row.get("target", "region")
    phase = row.get("phase", "")
    policy = row.get("policy", "")
    candidate_shape = row.get("candidate_shape", "")
    reason = row.get("example_reason", "") or row.get("reason", "")
    shape_key = row.get("admission_shape_key", "")
    rule_present = row.get("admission_rule_present", "")
    min_cardinality = row.get("admission_min_cardinality", "")
    score = row.get("admission_score", "")
    proof = row.get("admission_proof", "")

    if phase == "kernel_counter" or target != "region" or candidate_shape in ("", "none"):
        return
    if status not in {"compiled", "skipped"}:
        return
    if shape_key == "":
        raise AssertionError(f"{name}: compiled/skipped region row missing admission shape key: {row}")
    if rule_present not in {"true", "false"}:
        raise AssertionError(f"{name}: admission_rule_present must be true or false: {row}")

    if rule_present == "true":
        if row_int(row, "admission_min_cardinality") <= 0:
            raise AssertionError(f"{name}: admission rule has no positive min cardinality: {row}")
        if score == "":
            raise AssertionError(f"{name}: admission rule has no score: {row}")
        if proof == "":
            raise AssertionError(f"{name}: admission rule has no proof: {row}")
    else:
        if min_cardinality != "" or score != "" or proof != "":
            raise AssertionError(f"{name}: absent admission rule still exposes rule fields: {row}")

    if (
        policy == "auto"
        and status == "skipped"
        and "jit_policy=auto skips region kernel without admitted performance proof" in reason
    ):
        if f"shape={shape_key}" not in reason:
            raise AssertionError(f"{name}: auto missing-proof skip reason does not name admission shape key: {row}")
        if rule_present == "false":
            if "admission_rule=missing" not in reason:
                raise AssertionError(f"{name}: auto missing-proof skip reason does not name missing admission rule: {row}")
        else:
            if f"min_cardinality={min_cardinality}" not in reason:
                raise AssertionError(f"{name}: auto below-threshold skip reason does not name min cardinality: {row}")
            if not reason_contains_proof(reason, proof):
                raise AssertionError(f"{name}: auto below-threshold skip reason does not name admission proof: {row}")

    if (
        policy == "auto"
        and status == "skipped"
        and "jit_policy=auto skips region before backend analysis" in reason
    ):
        if row_int(row, "backend_analysis_time_us") != 0:
            raise AssertionError(f"{name}: auto precheck skip performed backend analysis: {row}")
        if f"shape={shape_key}" not in reason:
            raise AssertionError(f"{name}: auto precheck skip reason does not name admission shape key: {row}")
        if rule_present == "false":
            if "admission_rule=missing" not in reason:
                raise AssertionError(f"{name}: auto precheck skip reason does not name missing admission rule: {row}")
        else:
            if f"min_cardinality={min_cardinality}" not in reason:
                raise AssertionError(f"{name}: auto below-threshold precheck reason does not name min cardinality: {row}")
            if not reason_contains_proof(reason, proof):
                raise AssertionError(f"{name}: auto below-threshold precheck reason does not name admission proof: {row}")

    if policy == "auto" and status == "compiled":
        if rule_present != "true":
            raise AssertionError(f"{name}: auto compiled region has no admission rule: {row}")
        if not reason_contains_proof(reason, proof):
            raise AssertionError(f"{name}: auto compiled reason does not include admission proof: {row}")


def verify_scope_summary_field(name: str, row: dict, field: str) -> None:
    value = row.get(field, "")
    if value == "":
        raise AssertionError(f"{name}: missing {field}: {row}")
    if not any(scope in value for scope in KNOWN_CANDIDATE_SCOPE_SUMMARY_VALUES):
        raise AssertionError(f"{name}: {field} does not contain a known candidate scope: {row}")


def verify_summary(trace_dir: Path, rows: list, policies: list, expected_queries: list) -> None:
    expected_rows = len(expected_queries) * len(policies)
    if len(rows) != expected_rows:
        raise AssertionError(f"summary.csv: expected {expected_rows} rows, found {len(rows)}")

    by_query = {}
    for row in rows:
        by_query.setdefault(row["query"], {})[row["policy"]] = row
        if row_correctness_diff(row) != 0:
            raise AssertionError(f"summary.csv: correctness diff for q{row['query']} policy {row['policy']}")
        if row_int(row, "zero_code_native_compile_events") != 0:
            raise AssertionError(f"summary.csv: zero-code native compile for q{row['query']} policy {row['policy']}")
        if row_int(row, "non_region_events") != 0:
            raise AssertionError(f"summary.csv: non-region JIT target appeared for q{row['query']} policy {row['policy']}")
        for csv_field in ("events_csv", "counters_csv", "decision_counters_csv", "kernel_counters_csv"):
            csv_path = trace_dir / row[csv_field]
            if not csv_path.exists():
                raise AssertionError(f"summary.csv: missing {csv_field} target {csv_path}")
        profile_json_name = row.get("profile_json", "")
        if not profile_json_name:
            raise AssertionError(f"summary.csv: missing profile_json field for q{row['query']} {row['policy']}")
        profile_json = trace_dir / profile_json_name
        if not profile_json.exists():
            raise AssertionError(f"summary.csv: missing profile_json target {profile_json}")
        if row_int(row, "profile_operator_count") <= 0:
            raise AssertionError(f"summary.csv: empty profile operator count for q{row['query']} {row['policy']}")
        if row_int(row, "profile_query_time_us") <= 0:
            raise AssertionError(f"summary.csv: non-positive profile query time for q{row['query']} {row['policy']}")
        events = read_csv(trace_dir / row["events_csv"])
        if len(events) != row_int(row, "event_count"):
            raise AssertionError(
                f"summary.csv: q{row['query']} {row['policy']} event_count={row['event_count']} "
                f"but {row['events_csv']} has {len(events)} rows"
            )

    actual_queries = sorted(by_query)
    if actual_queries != expected_queries:
        raise AssertionError(f"summary.csv: expected queries {expected_queries}, found {actual_queries}")
    for query_id, policy_rows in by_query.items():
        missing = [policy for policy in policies if policy not in policy_rows]
        if missing:
            raise AssertionError(f"summary.csv: q{query_id} missing policies {missing}")


def verify_query_gaps(rows: list, expected_queries: list, policies: list) -> None:
    if len(rows) != len(expected_queries):
        raise AssertionError(f"query_gap_summary.csv: expected {len(expected_queries)} rows, found {len(rows)}")
    actual_queries = sorted(row["query"] for row in rows)
    if actual_queries != expected_queries:
        raise AssertionError(f"query_gap_summary.csv: expected queries {expected_queries}, found {actual_queries}")
    has_force = "force" in policies
    for row in rows:
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} correctness diff is non-zero")
        if row_int(row, "force_compiled_regions") > 0 and row["force_compiled_pipeline_shapes"] == "":
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} compiled force regions without pipeline shapes")
        if row_int(row, "force_compiled_regions") > 0:
            verify_scope_summary_field("query_gap_summary.csv", row, "force_compiled_pipeline_scopes")
        if row_int(row, "auto_top_skip_count") > 0 and row["auto_top_skip_pipeline_shape"] == "":
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} auto skip without pipeline shape")
        if row_int(row, "auto_top_skip_count") > 0:
            verify_candidate_scope_summary_value(
                "query_gap_summary.csv",
                row,
                "auto_top_skip_scope",
                allow_no_candidate=row.get("auto_top_skip_shape", "") in ("", "none"),
            )
        auto_skip_has_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS
            for node in parse_pipeline_shape(row["auto_top_skip_pipeline_shape"])
        )
        if auto_skip_has_wrapper:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} trace wrapper leaked into auto skip")
        if row_int(row, "force_top_unsupported_count") > 0 and row["force_top_unsupported_pipeline_shape"] == "":
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} unsupported force row without pipeline shape")
        if row_int(row, "force_top_unsupported_count") > 0:
            verify_candidate_scope(
                "query_gap_summary.csv", {"candidate_scope": row.get("force_top_unsupported_scope", "")}
            )
        force_unsupported_has_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS
            for node in parse_pipeline_shape(row["force_top_unsupported_pipeline_shape"])
        )
        if force_unsupported_has_wrapper:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} trace wrapper leaked into force unsupported")
        if (
            "unsupported_operator_or_expression_boundaries" in row["root_cause"]
            and row_int(row, "force_relevant_unsupported_regions") <= 0
        ):
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} unsupported label without relevant unsupported")
        compiled_kernels = row_int(row, "force_compiled_kernels")
        reached_kernels = row_int(row, "force_reached_kernels")
        row_processing_kernels = row_int(row, "force_row_processing_kernels")
        unreached_kernels = row_int(row, "force_unreached_kernels")
        zero_input_kernels = row_int(row, "force_zero_input_kernels")
        if reached_kernels > compiled_kernels:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} reached kernels exceed compiled kernels")
        if row_processing_kernels > reached_kernels:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} row-processing kernels exceed reached kernels")
        if compiled_kernels != reached_kernels + unreached_kernels:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} compiled/reached/unreached mismatch")
        if compiled_kernels != row_processing_kernels + zero_input_kernels:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} compiled/row/zero-input mismatch")
        if row_int(row, "force_compiled_regions") > 0 and compiled_kernels <= 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} compiled regions without kernel rows")
        if row_int(row, "force_source_native_output_rows") > 0 and row_int(row, "force_source_native_invocations") <= 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} native source rows without invocations")
        if row_int(row, "force_source_native_runtime_time_us") < 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} negative native source runtime")
        if row_int(row, "force_generated_body_runtime_time_us") < 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} negative generated body runtime")
        if row_processing_kernels > 0 and "force_small_generated_regions" not in row["root_cause"]:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} runtime rows without generated-region label")
        if compiled_kernels > 0 and row_processing_kernels == 0:
            if "compiled_kernels_not_reached" not in row["root_cause"]:
                raise AssertionError(f"query_gap_summary.csv: q{row['query']} missing unreached-kernel label")
            if "force_small_generated_regions" in row["root_cause"]:
                raise AssertionError(f"query_gap_summary.csv: q{row['query']} unreachable kernels labeled as runtime region")
        if compiled_kernels > row_processing_kernels and row_processing_kernels > 0:
            if "partial_compiled_kernel_reach" not in row["root_cause"]:
                raise AssertionError(f"query_gap_summary.csv: q{row['query']} missing partial reach label")
        if not has_force:
            continue
        if row.get("force_top_profile_operator", "") == "":
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} missing top profile operator")
        if row["force_top_profile_operator"] in PROFILE_WRAPPER_OPERATORS:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} profile wrapper leaked into top profile operator")
        if row_int(row, "force_top_profile_time_us") <= 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} non-positive top profile time")
        if row_int(row, "force_profile_operator_time_us") <= 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} non-positive total profile time")
        if row_float(row, "force_scan_join_groupby_profile_percent") <= 0:
            raise AssertionError(f"query_gap_summary.csv: q{row['query']} missing scan/join/group-by profile share")
        if row["force_top_profile_operator"] in {"TABLE_SCAN", "HASH_JOIN", "HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"}:
            if "profile_dominated_by_scan_join_groupby" not in row["root_cause"]:
                raise AssertionError(f"query_gap_summary.csv: q{row['query']} missing profiler dominance label")


def admission_proof_gap_key(row: dict) -> tuple:
    return (
        row["admission_shape_key"],
        row["execution_mode"],
        row["region_execution_form"],
        row["candidate_shape"],
        row["candidate_scope"],
    )


def verify_admission_proof_gaps(rows: list, decision_counters: list, summary_rows: list) -> None:
    force_compiled = sum(
        row_int(row, "compiled_regions") for row in summary_rows if row["policy"] == "force"
    )
    if not rows:
        if force_compiled > 0:
            raise AssertionError("admission_proof_gap_summary.csv: force compiled regions but no proof-gap rows")
        return
    verify_required_columns("admission_proof_gap_summary.csv", rows, ADMISSION_PROOF_GAP_REQUIRED_COLUMNS)

    expected_keys = {
        admission_proof_gap_key(row)
        for row in decision_counters
        if row.get("target") == "region" and row.get("policy") == "force" and row.get("status") == "compiled"
    }
    actual_keys = set()
    for row in rows:
        key = admission_proof_gap_key(row)
        if key in actual_keys:
            raise AssertionError(f"admission_proof_gap_summary.csv: duplicate proof-gap row: {row}")
        actual_keys.add(key)
        if row_int(row, "force_compiled_regions") <= 0:
            raise AssertionError(f"admission_proof_gap_summary.csv: row has no force compiled regions: {row}")
        query_count = row_int(row, "query_count")
        classified_queries = (
            row_int(row, "force_winning_queries")
            + row_int(row, "force_losing_queries")
            + row_int(row, "force_equal_queries")
        )
        if query_count <= 0 or classified_queries != query_count:
            raise AssertionError(f"admission_proof_gap_summary.csv: query classification mismatch: {row}")
        if row["proof_status"] not in KNOWN_ADMISSION_PROOF_STATUSES:
            raise AssertionError(f"admission_proof_gap_summary.csv: unknown proof status: {row}")
        if row["root_cause"] not in KNOWN_ADMISSION_PROOF_ROOT_CAUSES:
            raise AssertionError(f"admission_proof_gap_summary.csv: unknown root cause: {row}")
        for field in (
            "min_force_speedup_vs_off",
            "median_force_speedup_vs_off",
            "max_force_speedup_vs_off",
        ):
            if row_float(row, field) <= 0:
                raise AssertionError(f"admission_proof_gap_summary.csv: non-positive speedup field {field}: {row}")
        if row_float(row, "min_force_speedup_vs_off") > row_float(row, "median_force_speedup_vs_off"):
            raise AssertionError(f"admission_proof_gap_summary.csv: min speedup exceeds median: {row}")
        if row_float(row, "median_force_speedup_vs_off") > row_float(row, "max_force_speedup_vs_off"):
            raise AssertionError(f"admission_proof_gap_summary.csv: median speedup exceeds max: {row}")
        if row["root_cause"] == "missing_measured_auto_admission_proof":
            if row["auto_rule_present"] != "false":
                raise AssertionError(f"admission_proof_gap_summary.csv: missing-proof row has auto rule: {row}")
            if row["proof_status"] != "positive_query_median":
                raise AssertionError(f"admission_proof_gap_summary.csv: missing-proof row is not positive: {row}")
            if row["admission_shape_key"] in CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES:
                raise AssertionError(f"admission_proof_gap_summary.csv: context-specific shape mislabeled: {row}")
        if row["root_cause"] == "context_specific_positive_without_generic_admission_proof":
            if row["auto_rule_present"] != "false":
                raise AssertionError(f"admission_proof_gap_summary.csv: context-specific row has auto rule: {row}")
            if row["proof_status"] != "positive_query_median":
                raise AssertionError(f"admission_proof_gap_summary.csv: context-specific row is not positive: {row}")
            if row["admission_shape_key"] not in CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES:
                raise AssertionError(f"admission_proof_gap_summary.csv: unknown context-specific shape: {row}")
            if row_int(row, "force_winning_queries") <= 0 or row_int(row, "force_losing_queries") != 0:
                raise AssertionError(
                    f"admission_proof_gap_summary.csv: context-specific positive query counts are invalid: {row}"
                )
        if row["root_cause"] == "auto_rule_admitted_and_compiled":
            if row["auto_rule_present"] != "true":
                raise AssertionError(f"admission_proof_gap_summary.csv: admitted row has no auto rule: {row}")
            if row_int(row, "auto_compiled_regions") <= 0:
                raise AssertionError(f"admission_proof_gap_summary.csv: admitted row did not compile in auto: {row}")
        if row["root_cause"] == "force_region_not_profitable" and row["proof_status"] != "negative_query_median":
            raise AssertionError(f"admission_proof_gap_summary.csv: force-not-profitable row has wrong status: {row}")

    if actual_keys != expected_keys:
        raise AssertionError(
            "admission_proof_gap_summary.csv: proof-gap keys do not match force compiled region keys; "
            f"missing={sorted(expected_keys - actual_keys)} extra={sorted(actual_keys - expected_keys)}"
        )


def verify_operator_gaps(rows: list) -> None:
    if not rows:
        raise AssertionError("operator_gap_summary.csv: expected at least one operator gap row")
    for row in rows:
        if row["operator_name"] in TRACE_WRAPPER_OPERATORS:
            raise AssertionError(f"operator_gap_summary.csv: trace wrapper leaked into operator gaps: {row}")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"operator_gap_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"operator_gap_summary.csv: non-positive region_events: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"operator_gap_summary.csv: non-positive query_count: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"operator_gap_summary.csv: missing example pipeline: {row}")
        verify_scope_summary_field("operator_gap_summary.csv", row, "candidate_scopes")
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"operator_gap_summary.csv: trace wrapper leaked into example pipeline: {row}")
        operators = {node["operator_name"] for node in parse_pipeline_shape(row["example_pipeline_shape"])}
        if row["operator_name"] not in operators:
            raise AssertionError(f"operator_gap_summary.csv: example pipeline does not contain operator: {row}")


def verify_capability_gaps(rows: list, operator_gap_rows: list) -> None:
    if not rows:
        raise AssertionError("capability_gap_summary.csv: expected at least one capability gap row")
    capability_occurrences = {}
    operator_occurrences = {}
    for row in operator_gap_rows:
        key = (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"])
        operator_occurrences[key] = operator_occurrences.get(key, 0) + row_int(row, "occurrences")
    for row in rows:
        if row["capability_gap"] not in KNOWN_CAPABILITY_GAPS:
            raise AssertionError(f"capability_gap_summary.csv: unknown capability gap {row['capability_gap']}")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"capability_gap_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"capability_gap_summary.csv: non-positive region_events: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"capability_gap_summary.csv: non-positive query_count: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing query_examples: {row}")
        if row["operators"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing operators: {row}")
        if row["boundaries"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing boundaries: {row}")
        if row["candidate_shapes"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing candidate_shapes: {row}")
        verify_scope_summary_field("capability_gap_summary.csv", row, "candidate_scopes")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing example pipeline: {row}")
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"capability_gap_summary.csv: trace wrapper leaked into capability gaps: {row}")
        if row["example_reason"] == "":
            raise AssertionError(f"capability_gap_summary.csv: missing example reason: {row}")
        key = (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"])
        capability_occurrences[key] = capability_occurrences.get(key, 0) + row_int(row, "occurrences")
    if capability_occurrences != operator_occurrences:
        raise AssertionError(
            "capability_gap_summary.csv: occurrence totals do not match operator gaps: "
            f"capability={capability_occurrences} operator={operator_occurrences}"
        )


def verify_capability_priorities(
    rows: list,
    capability_gap_rows: list,
    operator_profile_rows: list,
    kernel_rows: list,
) -> None:
    if not rows:
        raise AssertionError("capability_priority_summary.csv: expected at least one capability priority row")
    priority_by_key = {}
    expected_runtime_by_key, _ = collect_capability_runtime_from_kernel_rows(kernel_rows)
    gap_by_key = {
        (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"], row["capability_gap"]): row
        for row in capability_gap_rows
    }
    profile_time_by_policy = {}
    for row in operator_profile_rows:
        profile_time_by_policy[row["policy"]] = profile_time_by_policy.get(row["policy"], 0) + row_int(
            row, "operator_time_us"
        )
    for row in rows:
        key = (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"], row["capability_gap"])
        if key in priority_by_key:
            raise AssertionError(f"capability_priority_summary.csv: duplicate priority key: {row}")
        priority_by_key[key] = row
        if row["capability_gap"] not in KNOWN_CAPABILITY_GAPS:
            raise AssertionError(f"capability_priority_summary.csv: unknown capability gap {row['capability_gap']}")
        if row_int(row, "profile_time_us") < 0:
            raise AssertionError(f"capability_priority_summary.csv: negative profile time: {row}")
        if row_float(row, "profile_percent_of_policy") < 0:
            raise AssertionError(f"capability_priority_summary.csv: negative profile percent: {row}")
        if row_int(row, "profile_time_us") > profile_time_by_policy.get(row["policy"], 0):
            raise AssertionError(f"capability_priority_summary.csv: profile time exceeds policy total: {row}")
        if row_int(row, "profile_time_us") > 0 and row["profile_operators"] == "":
            raise AssertionError(f"capability_priority_summary.csv: profile time without operator evidence: {row}")
        for field in CAPABILITY_RUNTIME_FIELDS:
            if row_int(row, field) < 0:
                raise AssertionError(f"capability_priority_summary.csv: negative runtime field {field}: {row}")
            if row["status"] != "compiled" and row_int(row, field) != 0:
                raise AssertionError(
                    f"capability_priority_summary.csv: non-compiled row has runtime field {field}: {row}"
                )
        verify_scope_summary_field("capability_priority_summary.csv", row, "candidate_scopes")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"capability_priority_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"capability_priority_summary.csv: non-positive region_events: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"capability_priority_summary.csv: non-positive query_count: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"capability_priority_summary.csv: missing example pipeline: {row}")
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"capability_priority_summary.csv: trace wrapper leaked into priority gaps: {row}")
    if set(priority_by_key) != set(gap_by_key):
        raise AssertionError(
            "capability_priority_summary.csv: priority keys do not match capability gaps: "
            f"priority={sorted(priority_by_key)} gaps={sorted(gap_by_key)}"
        )
    for key, priority_row in priority_by_key.items():
        gap_row = gap_by_key[key]
        for field in ("occurrences", "region_events", "query_count", "max_estimated_cardinality"):
            if row_int(priority_row, field) != row_int(gap_row, field):
                raise AssertionError(
                    "capability_priority_summary.csv: priority row does not match gap row for "
                    f"{key} field={field}: priority={priority_row} gap={gap_row}"
                )
        if priority_row.get("candidate_scopes", "") != gap_row.get("candidate_scopes", ""):
            raise AssertionError(
                "capability_priority_summary.csv: candidate scopes do not match capability gaps for "
                f"{key}: priority={priority_row} gap={gap_row}"
            )
        expected_runtime = expected_runtime_by_key.get(key, {})
        for field in CAPABILITY_RUNTIME_FIELDS:
            if row_int(priority_row, field) != row_int(expected_runtime, field):
                raise AssertionError(
                    "capability_priority_summary.csv: runtime field does not match kernel summary for "
                    f"{key} field={field}: priority={priority_row} kernel={expected_runtime}"
                )


def verify_query_capability_priorities(
    rows: list,
    capability_priority_rows: list,
    operator_profile_rows: list,
    kernel_rows: list,
    expected_queries: list,
) -> None:
    if not rows:
        raise AssertionError("query_capability_priority_summary.csv: expected at least one query capability row")
    expected_query_set = set(expected_queries)
    _, expected_runtime_by_query_key = collect_capability_runtime_from_kernel_rows(kernel_rows)
    query_profile_time_by_key = {}
    for row in operator_profile_rows:
        key = (row["query"], row["policy"])
        query_profile_time_by_key[key] = query_profile_time_by_key.get(key, 0) + row_int(row, "operator_time_us")

    query_totals = {}
    seen_keys = set()
    for row in rows:
        if row["query"] not in expected_query_set:
            raise AssertionError(f"query_capability_priority_summary.csv: unexpected query {row['query']}")
        key = (
            row["query"],
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["capability_gap"],
        )
        if key in seen_keys:
            raise AssertionError(f"query_capability_priority_summary.csv: duplicate query priority key: {row}")
        seen_keys.add(key)
        if row["capability_gap"] not in KNOWN_CAPABILITY_GAPS:
            raise AssertionError(
                f"query_capability_priority_summary.csv: unknown capability gap {row['capability_gap']}"
            )
        if row_int(row, "profile_time_us") < 0:
            raise AssertionError(f"query_capability_priority_summary.csv: negative profile time: {row}")
        if row_float(row, "profile_percent_of_query_policy") < 0:
            raise AssertionError(f"query_capability_priority_summary.csv: negative profile percent: {row}")
        if row_int(row, "profile_time_us") > query_profile_time_by_key.get((row["query"], row["policy"]), 0):
            raise AssertionError(f"query_capability_priority_summary.csv: profile time exceeds query total: {row}")
        if row_int(row, "profile_time_us") > 0 and row["profile_operators"] == "":
            raise AssertionError(f"query_capability_priority_summary.csv: profile time without operator evidence: {row}")
        for field in CAPABILITY_RUNTIME_FIELDS:
            if row_int(row, field) < 0:
                raise AssertionError(f"query_capability_priority_summary.csv: negative runtime field {field}: {row}")
            if row["status"] != "compiled" and row_int(row, field) != 0:
                raise AssertionError(
                    f"query_capability_priority_summary.csv: non-compiled row has runtime field {field}: {row}"
                )
        expected_runtime = expected_runtime_by_query_key.get(key, {})
        for field in CAPABILITY_RUNTIME_FIELDS:
            if row_int(row, field) != row_int(expected_runtime, field):
                raise AssertionError(
                    "query_capability_priority_summary.csv: runtime field does not match kernel summary for "
                    f"{key} field={field}: query={row} kernel={expected_runtime}"
                )
        verify_scope_summary_field("query_capability_priority_summary.csv", row, "candidate_scopes")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"query_capability_priority_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"query_capability_priority_summary.csv: non-positive region_events: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"query_capability_priority_summary.csv: missing example pipeline: {row}")
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"query_capability_priority_summary.csv: trace wrapper leaked into query priority: {row}")

        aggregate_key = (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"], row["capability_gap"])
        entry = query_totals.setdefault(
            aggregate_key,
            {
                "profile_time_us": 0,
                "occurrences": 0,
                "region_events": 0,
                "query_count": set(),
                "max_estimated_cardinality": 0,
                **new_capability_runtime_entry(),
            },
        )
        entry["profile_time_us"] += row_int(row, "profile_time_us")
        entry["occurrences"] += row_int(row, "occurrences")
        entry["region_events"] += row_int(row, "region_events")
        entry["query_count"].add(row["query"])
        entry["max_estimated_cardinality"] = max(
            entry["max_estimated_cardinality"], row_int(row, "max_estimated_cardinality")
        )
        for field in CAPABILITY_RUNTIME_FIELDS:
            entry[field] += row_int(row, field)

    priority_by_key = {
        (row["policy"], row["status"], row["execution_mode"], row["region_execution_form"], row["capability_gap"]): row
        for row in capability_priority_rows
    }
    if set(query_totals) != set(priority_by_key):
        raise AssertionError(
            "query_capability_priority_summary.csv: query priority keys do not match workload priority: "
            f"query={sorted(query_totals)} priority={sorted(priority_by_key)}"
        )
    for key, query_total in query_totals.items():
        priority_row = priority_by_key[key]
        for field in (
            "profile_time_us",
            "occurrences",
            "region_events",
            "max_estimated_cardinality",
            *CAPABILITY_RUNTIME_FIELDS,
        ):
            if field in CAPABILITY_RUNTIME_FIELDS:
                if query_total[field] > row_int(priority_row, field):
                    raise AssertionError(
                        "query_capability_priority_summary.csv: query runtime exceeds workload priority for "
                        f"{key} field={field}: query={query_total} priority={priority_row}"
                    )
                continue
            if row_int(priority_row, field) != query_total[field]:
                raise AssertionError(
                    "query_capability_priority_summary.csv: query totals do not match workload priority for "
                    f"{key} field={field}: query={query_total} priority={priority_row}"
                )
        if row_int(priority_row, "query_count") != len(query_total["query_count"]):
            raise AssertionError(
                "query_capability_priority_summary.csv: query_count does not match workload priority for "
                f"{key}: query={query_total} priority={priority_row}"
            )


def verify_expression_fallback_summary(rows: list, expected_queries: list, require_full_lowering: bool) -> None:
    if not rows:
        if not require_full_lowering:
            return
        raise AssertionError("expression_fallback_summary.csv: expected at least one expression fallback row")
    expected_query_set = set(expected_queries)
    has_function_or_operator = False
    has_projection_expression = False
    seen_keys = set()
    for row in rows:
        key = (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["reason"],
            row["expression_class"],
            row["expression_type"],
            row["function_name"],
            row["return_type"],
        )
        if key in seen_keys:
            raise AssertionError(f"expression_fallback_summary.csv: duplicate fallback key: {row}")
        seen_keys.add(key)
        if row["policy"] == "off":
            raise AssertionError(f"expression_fallback_summary.csv: off policy should not have fallback rows: {row}")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"expression_fallback_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"expression_fallback_summary.csv: non-positive region_events: {row}")
        if row_int(row, "region_events") > row_int(row, "occurrences"):
            raise AssertionError(f"expression_fallback_summary.csv: events exceed occurrences: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"expression_fallback_summary.csv: non-positive query_count: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"expression_fallback_summary.csv: missing query examples: {row}")
        for query_example in row["query_examples"].split(","):
            query_id = query_example.strip().split("(")[0].removeprefix("q")
            if query_id and query_id not in expected_query_set:
                raise AssertionError(f"expression_fallback_summary.csv: unexpected query example {query_id}: {row}")
        for field in ("reason", "expression_class", "expression_type", "return_type"):
            if row[field] == "":
                raise AssertionError(f"expression_fallback_summary.csv: missing {field}: {row}")
        if row["reason"] == "function_or_operator_unsupported" and row["function_name"] == "":
            raise AssertionError(f"expression_fallback_summary.csv: function/operator row missing function name: {row}")
        verify_scope_summary_field("expression_fallback_summary.csv", row, "candidate_scopes")
        if row["candidate_shapes"] == "":
            raise AssertionError(f"expression_fallback_summary.csv: missing candidate shapes: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"expression_fallback_summary.csv: missing example pipeline: {row}")
        if row["example_reason"] == "":
            raise AssertionError(f"expression_fallback_summary.csv: missing example reason: {row}")
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"expression_fallback_summary.csv: trace wrapper leaked into fallback summary: {row}")
        if "core expression lowering unsupported;" not in row["example_reason"]:
            raise AssertionError(f"expression_fallback_summary.csv: example reason missing structured marker: {row}")
        if row["reason"] == "function_or_operator_unsupported":
            has_function_or_operator = True
        if "projection" in row["example_reason"].lower() or "projection" in row["candidate_shapes"].lower():
            has_projection_expression = True
    if not require_full_lowering:
        return
    if not has_function_or_operator:
        raise AssertionError("expression_fallback_summary.csv: missing function/operator fallback root cause")
    if not has_projection_expression:
        raise AssertionError("expression_fallback_summary.csv: missing projection expression fallback evidence")


def verify_source_boundary_summary(
    rows: list, expected_queries: list, require_full_lowering: bool, require_runtime: bool
) -> None:
    if not rows:
        if not require_full_lowering:
            return
        raise AssertionError("source_boundary_summary.csv: expected at least one source boundary row")
    expected_query_set = set(expected_queries)
    has_table_scan = False
    has_dynamic_filter = False
    has_pushed_filter = False
    has_stateful_source = False
    has_hash_aggregate_protocol = False
    has_perfect_hash_aggregate_protocol = False
    has_ungrouped_aggregate_protocol = False
    has_column_data_native_source = False
    has_compiled_source_boundary = False
    has_compiled_source_native = False
    has_compiled_source_native_runtime = False
    seen_keys = set()
    for row in rows:
        key = source_boundary_row_key(row)
        if key in seen_keys:
            raise AssertionError(f"source_boundary_summary.csv: duplicate source boundary key: {row}")
        seen_keys.add(key)
        if row["region_execution_form"] not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"source_boundary_summary.csv: unknown execution form: {row}")
        if row["policy"] == "off":
            raise AssertionError(f"source_boundary_summary.csv: off policy should not have source rows: {row}")
        if row["source_boundary_kind"] not in KNOWN_SOURCE_BOUNDARY_KINDS:
            raise AssertionError(f"source_boundary_summary.csv: unknown boundary kind: {row}")
        if row["source_execution"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing selected source execution: {row}")
        if row["source_operator"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing source operator: {row}")
        if row["boundary_operator"].endswith(")"):
            raise AssertionError(f"source_boundary_summary.csv: boundary operator contains wrapper punctuation: {row}")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"source_boundary_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"source_boundary_summary.csv: non-positive region_events: {row}")
        if row_int(row, "region_events") > row_int(row, "occurrences"):
            raise AssertionError(f"source_boundary_summary.csv: events exceed occurrences: {row}")
        for field in (
            *SOURCE_NATIVE_RUNTIME_FIELDS,
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"source_boundary_summary.csv: negative source runtime field {field}: {row}")
        if row["status"] == "compiled" and row["source_execution"] == "native-source":
            has_compiled_source_native = True
            if row_int(row, "source_native_output_rows") > 0 and row_int(row, "source_native_invocations") > 0:
                has_compiled_source_native_runtime = True
        elif row["status"] == "compiled" and row["source_execution"] != "native-source":
            has_compiled_source_boundary = True
            for field in SOURCE_NATIVE_RUNTIME_FIELDS:
                if row_int(row, field) != 0:
                    raise AssertionError(
                        f"source_boundary_summary.csv: source-boundary row has native-source field {field}: {row}"
                    )
        else:
            for field in (
                *SOURCE_NATIVE_RUNTIME_FIELDS,
                "generated_body_runtime_time_us",
            ):
                if row_int(row, field) != 0:
                    raise AssertionError(
                        f"source_boundary_summary.csv: non-compiled row has runtime field {field}: {row}"
                    )
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"source_boundary_summary.csv: non-positive query_count: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing query examples: {row}")
        for query_example in row["query_examples"].split(","):
            query_id = query_example.strip().split("(")[0].removeprefix("q")
            if query_id and query_id not in expected_query_set:
                raise AssertionError(f"source_boundary_summary.csv: unexpected query example {query_id}: {row}")
        if row["candidate_shapes"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing candidate shapes: {row}")
        verify_scope_summary_field("source_boundary_summary.csv", row, "candidate_scopes")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing example pipeline: {row}")
        if row["example_reason"] == "":
            raise AssertionError(f"source_boundary_summary.csv: missing example reason: {row}")
        source_nodes = [
            node for node in parse_pipeline_shape(row["example_pipeline_shape"]) if node["node_kind"] == "source"
        ]
        if not source_nodes:
            raise AssertionError(f"source_boundary_summary.csv: example pipeline has no source node: {row}")
        if row["source_operator"] not in {node["operator_name"] for node in source_nodes}:
            raise AssertionError(f"source_boundary_summary.csv: example pipeline does not contain source operator: {row}")
        if any(node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])):
            raise AssertionError(f"source_boundary_summary.csv: trace wrapper leaked into source summary: {row}")
        if row["source_operator"] == "TABLE_SCAN":
            has_table_scan = True
            for field in (
                "scan_function",
                "output_columns",
                "returned_columns",
                "column_ids",
                "native_source_input_columns",
                "native_source_input_types",
                "native_source_output_projection_map",
                "native_source_filter_column_map",
                "native_source_requires_unfiltered_input",
                "native_source_filter_prune_required",
                "native_source_filter_takeover_supported",
                "projected_columns",
                "projection_pushdown",
                "filter_pushdown",
                "filter_prune",
                "filter_count",
                "dynamic_filters",
                "in_out_function",
            ):
                if row[field] == "":
                    raise AssertionError(f"source_boundary_summary.csv: missing table-scan field {field}: {row}")
            for field in (
                "output_columns",
                "returned_columns",
                "column_ids",
                "native_source_input_columns",
                "projected_columns",
                "filter_count",
            ):
                if row_int(row, field) < 0:
                    raise AssertionError(f"source_boundary_summary.csv: negative table-scan field {field}: {row}")
            for field in (
                "native_source_requires_unfiltered_input",
                "native_source_filter_prune_required",
                "native_source_filter_takeover_supported",
                "projection_pushdown",
                "filter_pushdown",
                "filter_prune",
                "dynamic_filters",
                "in_out_function",
            ):
                verify_bool_field("source_boundary_summary.csv", row, field)
            if row["dynamic_filters"] == "true":
                has_dynamic_filter = True
            if row_int(row, "filter_count") > 0:
                has_pushed_filter = True
            expected_markers = {
                "table_scan_generated_source_filter": "generated native table scan filters",
                "table_scan_native_source": "native table scan source protocol",
                "table_scan_source_boundary": "DuckDB table scan source boundary",
                "duckdb_scan_source_boundary": "DuckDB scan source boundary",
                "duckdb_source_boundary": "DuckDB source boundary",
            }
            expected_marker = expected_markers.get(row["source_boundary_kind"], "")
            if expected_marker == "":
                raise AssertionError(f"source_boundary_summary.csv: table scan has unknown boundary kind: {row}")
            if expected_marker not in row["example_reason"]:
                raise AssertionError(f"source_boundary_summary.csv: example reason missing table-scan marker: {row}")
            for field in (
                "native_state_scan_contract_status",
                "native_state_scan_required_capability",
                "native_state_scan_protocol",
                "native_state_scan_blocker",
                "native_hash_join_probe_contract_status",
                "native_hash_join_probe_required_capability",
                "native_hash_join_probe_protocol",
                "native_hash_join_probe_blocker",
                "native_hash_join_build_contract_status",
                "native_hash_join_build_required_capability",
                "native_hash_join_build_protocol",
                "native_hash_join_build_blocker",
                "regular_hash_table_layout_ready",
                "native_probe_shape_ready",
                "native_probe_shape_blocker",
                "native_probe_output_mode",
                "build_append_shape_ready",
                "build_append_shape_blocker",
                "hash_join_layout_column_count",
                "hash_join_layout_offsets",
                "hash_join_tuple_size",
                "hash_join_entry_size",
                "hash_join_pointer_offset",
                "hash_join_hash_column_index",
                "hash_join_found_match_column_present",
                "hash_join_found_match_column_index",
                "hash_join_native_protocol_blocker",
                "native_hash_aggregate_lookup_contract_status",
                "native_hash_aggregate_lookup_required_capability",
                "native_hash_aggregate_lookup_protocol",
                "native_hash_aggregate_lookup_blocker",
            ):
                if row[field] != "":
                    raise AssertionError(f"source_boundary_summary.csv: table scan has state-scan field {field}: {row}")
        if row["source_boundary_kind"] in {
            "stateful_source_fallback",
            "stateful_native_state_scan",
            "stateful_native_source",
        }:
            has_stateful_source = True
            if row["boundary_operator"] == "":
                raise AssertionError(f"source_boundary_summary.csv: stateful source missing boundary operator: {row}")
            if row["source_boundary_kind"] == "stateful_native_source":
                if row["source_operator"] not in {"CTE_SCAN", "COLUMN_DATA_SCAN"}:
                    raise AssertionError(f"source_boundary_summary.csv: native-source row has wrong operator: {row}")
                if row["source_execution"] != "native-source":
                    raise AssertionError(f"source_boundary_summary.csv: native-source row has wrong execution: {row}")
                has_column_data_native_source = True
                for field in (
                    "scan_function",
                    "output_columns",
                    "returned_columns",
                ):
                    if row[field] == "":
                        raise AssertionError(f"source_boundary_summary.csv: missing native-source field {field}: {row}")
                if row_int(row, "output_columns") <= 0 or row_int(row, "returned_columns") <= 0:
                    raise AssertionError(f"source_boundary_summary.csv: native-source row has no columns: {row}")
                for field in (
                    "native_state_scan_contract_status",
                    "native_state_scan_required_capability",
                    "native_state_scan_protocol",
                    "native_state_scan_blocker",
                    "native_grouped_state_contract_status",
                    "native_grouped_state_required_capability",
                    "native_grouped_state_protocol",
                    "native_grouped_state_blocker",
                    "native_hash_join_probe_contract_status",
                    "native_hash_join_probe_required_capability",
                    "native_hash_join_probe_protocol",
                    "native_hash_join_probe_blocker",
                    "native_hash_join_build_contract_status",
                    "native_hash_join_build_required_capability",
                    "native_hash_join_build_protocol",
                    "native_hash_join_build_blocker",
                    "native_hash_aggregate_lookup_contract_status",
                    "native_hash_aggregate_lookup_required_capability",
                    "native_hash_aggregate_lookup_protocol",
                    "native_hash_aggregate_lookup_blocker",
                ):
                    if row[field] != "":
                        raise AssertionError(f"source_boundary_summary.csv: native-source row has state contract {field}: {row}")
                if (
                    "native stateful source protocol" not in row["example_reason"]
                    and "DuckDB column data native source protocol" not in row["example_reason"]
                ):
                    raise AssertionError(f"source_boundary_summary.csv: example reason missing native-source marker: {row}")
        if row["source_operator"] == "HASH_JOIN":
            for field in SOURCE_BOUNDARY_HASH_JOIN_REQUIRED_FIELDS:
                if row[field] == "":
                    raise AssertionError(f"source_boundary_summary.csv: missing hash-join field {field}: {row}")
            for field in SOURCE_BOUNDARY_HASH_JOIN_NUMERIC_FIELDS:
                if row_int(row, field) < 0:
                    raise AssertionError(f"source_boundary_summary.csv: negative hash-join field {field}: {row}")
            for field in (
                "residual_predicate",
                "residual_info",
                "filter_pushdown",
                "build_side_has_filter",
                "source_produces_rows",
                "correlated_mark_counts_required",
                "regular_hash_table_layout_ready",
                "native_probe_shape_ready",
                "build_append_shape_ready",
                "hash_join_found_match_column_present",
            ):
                verify_bool_field("source_boundary_summary.csv", row, field)
            if row_int(row, "condition_count") <= 0:
                raise AssertionError(f"source_boundary_summary.csv: hash join missing join conditions: {row}")
            if row_int(row, "equality_condition_count") + row_int(row, "non_equality_condition_count") != row_int(
                row, "condition_count"
            ):
                raise AssertionError(f"source_boundary_summary.csv: inconsistent hash-join condition counts: {row}")
            if row["source_produces_rows"] != "true":
                raise AssertionError(f"source_boundary_summary.csv: non-producing hash join source was not excluded: {row}")
            verify_state_scan_contract(
                "source_boundary_summary.csv",
                row,
                "hash-join-native-state-scan",
                "none",
                "ready",
            )
            verify_hash_join_native_contracts("source_boundary_summary.csv", row)
            verify_empty_native_operator_contract(
                "source_boundary_summary.csv", row, "native_hash_aggregate_lookup"
            )
            if (
                "DuckDB hash join native state scan protocol" not in row["example_reason"]
                and "native state scan source protocol" not in row["example_reason"]
            ):
                raise AssertionError(f"source_boundary_summary.csv: example reason missing hash-join marker: {row}")
        if row["source_operator"] in {"HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY", "UNGROUPED_AGGREGATE"}:
            if row["source_operator"] == "HASH_GROUP_BY":
                has_hash_aggregate_protocol = True
                if row["aggregate_operator_kind"] != "hash":
                    raise AssertionError(f"source_boundary_summary.csv: wrong hash aggregate kind: {row}")
                verify_state_scan_contract(
                    "source_boundary_summary.csv",
                    row,
                    "hash-aggregate-native-state-scan",
                    "none",
                    "ready",
                )
                if hash_grouped_state_contract_should_be_ready(row):
                    verify_ready_grouped_state_contract(
                        "source_boundary_summary.csv", row, "hash-aggregate-native-grouped-state"
                    )
                    verify_native_operator_contract(
                        "source_boundary_summary.csv",
                        row,
                        "native_hash_aggregate_lookup",
                        "ready",
                        "hash-aggregate-native-lookup",
                        "none",
                    )
                else:
                    verify_missing_grouped_state_contract(
                        "source_boundary_summary.csv",
                        row,
                        "hash-aggregate-native-grouped-state",
                        hash_aggregate_missing_grouped_state_blocker(row),
                    )
                    verify_native_operator_contract(
                        "source_boundary_summary.csv",
                        row,
                        "native_hash_aggregate_lookup",
                        "missing",
                        "hash-aggregate-native-lookup",
                        hash_aggregate_missing_lookup_blocker(row),
                    )
                verify_grouped_state_layout_contract("source_boundary_summary.csv", row)
            if row["source_operator"] == "PERFECT_HASH_GROUP_BY":
                has_perfect_hash_aggregate_protocol = True
                if row["aggregate_operator_kind"] != "perfect_hash":
                    raise AssertionError(f"source_boundary_summary.csv: wrong perfect hash aggregate kind: {row}")
                verify_state_scan_contract(
                    "source_boundary_summary.csv",
                    row,
                    "perfect-hash-aggregate-native-state-scan",
                    "none",
                    "ready",
                )
                verify_ready_grouped_state_contract(
                    "source_boundary_summary.csv", row, "perfect-hash-aggregate-native-grouped-state"
                )
                verify_grouped_state_layout_contract("source_boundary_summary.csv", row)
                verify_native_operator_contract(
                    "source_boundary_summary.csv",
                    row,
                    "native_hash_aggregate_lookup",
                    "ready",
                    "perfect-hash-aggregate-native-lookup",
                    "none",
                )
                for field in (
                    "perfect_required_bits_count",
                    "perfect_required_bits_total",
                    "perfect_required_bits",
                    "perfect_group_minima_count",
                ):
                    if row[field] == "":
                        raise AssertionError(f"source_boundary_summary.csv: missing perfect hash field {field}: {row}")
                for field in (
                    "perfect_required_bits_count",
                    "perfect_required_bits_total",
                    "perfect_group_minima_count",
                ):
                    if row_int(row, field) < 0:
                        raise AssertionError(f"source_boundary_summary.csv: negative perfect hash field {field}: {row}")
            if row["source_operator"] == "UNGROUPED_AGGREGATE":
                has_ungrouped_aggregate_protocol = True
                if row["aggregate_operator_kind"] != "ungrouped":
                    raise AssertionError(f"source_boundary_summary.csv: wrong ungrouped aggregate kind: {row}")
                verify_state_scan_contract(
                    "source_boundary_summary.csv",
                    row,
                    "ungrouped-aggregate-native-state-scan",
                    "none",
                    "ready",
                )
                for field in (
                    "native_grouped_state_contract_status",
                    "native_grouped_state_required_capability",
                    "native_grouped_state_protocol",
                    "native_grouped_state_blocker",
                    "native_hash_aggregate_lookup_contract_status",
                    "native_hash_aggregate_lookup_required_capability",
                    "native_hash_aggregate_lookup_protocol",
                    "native_hash_aggregate_lookup_blocker",
                    "grouped_state_layout_ready",
                    "grouped_state_offsets",
                    "grouped_state_payload_sizes",
                ):
                    if row[field] != "":
                        raise AssertionError(
                            f"source_boundary_summary.csv: ungrouped aggregate has grouped state field {field}: {row}"
                        )
            if row["source_operator"] != "HASH_JOIN":
                verify_empty_native_operator_contract(
                    "source_boundary_summary.csv", row, "native_hash_join_probe"
                )
                verify_empty_native_operator_contract(
                    "source_boundary_summary.csv", row, "native_hash_join_build"
                )
            for field in SOURCE_BOUNDARY_AGGREGATE_REQUIRED_FIELDS:
                if row[field] == "":
                    raise AssertionError(f"source_boundary_summary.csv: missing aggregate field {field}: {row}")
            for field in SOURCE_BOUNDARY_AGGREGATE_NUMERIC_FIELDS:
                if row_int(row, field) < 0:
                    raise AssertionError(f"source_boundary_summary.csv: negative aggregate field {field}: {row}")
            if row["source_operator"] == "UNGROUPED_AGGREGATE" and row_int(row, "aggregate_count") <= 0:
                raise AssertionError(f"source_boundary_summary.csv: ungrouped aggregate has no aggregates: {row}")
            if row_int(row, "group_count") <= 0 and row_int(row, "aggregate_count") <= 0:
                raise AssertionError(f"source_boundary_summary.csv: aggregate source has no grouping or aggregates: {row}")
            expected_aggregate_markers = (
                ("native state scan source protocol", "DuckDB hash aggregate native state scan protocol")
                if row["source_operator"] == "HASH_GROUP_BY"
                else (
                    ("native state scan source protocol",)
                    if row["source_boundary_kind"] == "stateful_native_state_scan"
                    else ("DuckDB aggregate source state protocol missing",)
                )
            )
            if not any(marker in row["example_reason"] for marker in expected_aggregate_markers):
                raise AssertionError(f"source_boundary_summary.csv: example reason missing aggregate marker: {row}")
    if not require_full_lowering:
        return
    if not has_table_scan:
        raise AssertionError("source_boundary_summary.csv: missing table scan source-boundary rows")
    if not has_dynamic_filter:
        raise AssertionError("source_boundary_summary.csv: missing dynamic-filter table scan rows")
    if not has_pushed_filter:
        raise AssertionError("source_boundary_summary.csv: missing pushed-filter table scan rows")
    if not has_stateful_source:
        raise AssertionError("source_boundary_summary.csv: missing stateful source rows")
    if not has_column_data_native_source:
        raise AssertionError("source_boundary_summary.csv: missing column-data native source rows")
    if not has_hash_aggregate_protocol:
        raise AssertionError("source_boundary_summary.csv: missing hash aggregate source protocol rows")
    if not has_perfect_hash_aggregate_protocol:
        raise AssertionError("source_boundary_summary.csv: missing perfect hash aggregate source protocol rows")
    if not has_ungrouped_aggregate_protocol:
        raise AssertionError("source_boundary_summary.csv: missing ungrouped aggregate source protocol rows")
    if require_runtime and has_compiled_source_native and not has_compiled_source_native_runtime:
        raise AssertionError("source_boundary_summary.csv: compiled native-source rows lack runtime source-native totals")


def source_boundary_row_key(row: dict) -> tuple:
    return (
        row["policy"],
        row["status"],
        row["execution_mode"],
        row["region_execution_form"],
        row["source_boundary_kind"],
        row["source_execution"],
        row["source_operator"],
        row["scan_function"],
        row["boundary_operator"],
        row["output_columns"],
        row["returned_columns"],
        row["column_ids"],
        row["native_source_input_columns"],
        row["native_source_input_types"],
        row["native_source_output_projection_map"],
        row["native_source_filter_column_map"],
        row["native_source_requires_unfiltered_input"],
        row["native_source_filter_prune_required"],
        row["native_source_filter_takeover_supported"],
        row["projected_columns"],
        row["projection_pushdown"],
        row["filter_pushdown"],
        row["filter_prune"],
        row["filter_count"],
        row["dynamic_filters"],
        row["in_out_function"],
        row["join_type"],
        row["condition_count"],
        row["equality_condition_count"],
        row["non_equality_condition_count"],
        row["null_equal_condition_count"],
        row["condition_types"],
        row["comparison_ops"],
        row["payload_columns"],
        row["payload_column_indices"],
        row["payload_types"],
        row["lhs_output_columns"],
        row["lhs_output_column_indices"],
        row["lhs_output_types"],
        row["rhs_output_columns"],
        row["rhs_output_types"],
        row["lhs_probe_columns"],
        row["lhs_probe_column_indices"],
        row["lhs_probe_types"],
        row["lhs_output_in_probe"],
        row["delim_types"],
        row["correlated_mark_counts_required"],
        row["residual_predicate"],
        row["residual_info"],
        row["filter_pushdown_condition_count"],
        row["filter_pushdown_probe_count"],
        row["build_side_has_filter"],
        row["aggregate_operator_kind"],
        row["group_count"],
        row["group_types"],
        row["aggregate_count"],
        row["aggregate_functions"],
        row["aggregate_return_types"],
        row["aggregate_child_counts"],
        row["aggregate_types"],
        row["aggregate_filter_count"],
        row["aggregate_order_count"],
        row["payload_type_count"],
        row["grouping_set_count"],
        row["grouping_function_count"],
        row["radix_table_count"],
        row["distinct_aggregate_count"],
        row["distinct_table_count"],
        row["distinct_child_count"],
        row["input_group_type_count"],
        row["input_group_types"],
        row["non_distinct_filter_count"],
        row["distinct_filter_count"],
        row["native_state_scan_contract_status"],
        row["native_state_scan_required_capability"],
        row["native_state_scan_protocol"],
        row["native_state_scan_blocker"],
        row["native_grouped_state_contract_status"],
        row["native_grouped_state_required_capability"],
        row["native_grouped_state_protocol"],
        row["native_grouped_state_blocker"],
        row["native_hash_join_probe_contract_status"],
        row["native_hash_join_probe_required_capability"],
        row["native_hash_join_probe_protocol"],
        row["native_hash_join_probe_blocker"],
        row["native_hash_join_build_contract_status"],
        row["native_hash_join_build_required_capability"],
        row["native_hash_join_build_protocol"],
        row["native_hash_join_build_blocker"],
        row["native_hash_aggregate_lookup_contract_status"],
        row["native_hash_aggregate_lookup_required_capability"],
        row["native_hash_aggregate_lookup_protocol"],
        row["native_hash_aggregate_lookup_blocker"],
        row["perfect_required_bits_count"],
        row["perfect_required_bits_total"],
        row["perfect_required_bits"],
        row["perfect_group_minima_count"],
    )


def verify_source_boundary_priorities(
    rows: list, source_boundary_rows: list, operator_profile_rows: list, require_full_tpch_coverage: bool
) -> None:
    if not rows:
        raise AssertionError("source_boundary_priority_summary.csv: expected at least one source priority row")
    source_by_key = {source_boundary_row_key(row): row for row in source_boundary_rows}
    priority_by_key = {}
    profile_time_by_policy = {}
    profile_time_by_policy_operator = {}
    for row in operator_profile_rows:
        profile_time_by_policy[row["policy"]] = profile_time_by_policy.get(row["policy"], 0) + row_int(
            row, "operator_time_us"
        )
        policy_operator_key = (row["policy"], row["operator_name"])
        profile_time_by_policy_operator[policy_operator_key] = profile_time_by_policy_operator.get(
            policy_operator_key, 0
        ) + row_int(row, "operator_time_us")
    allocated_time_by_policy = {}
    allocated_time_by_policy_operator = {}
    has_profile_time = False
    has_table_scan_profile = False
    has_stateful_profile = False
    for row in rows:
        key = source_boundary_row_key(row)
        if key in priority_by_key:
            raise AssertionError(f"source_boundary_priority_summary.csv: duplicate priority key: {row}")
        priority_by_key[key] = row
        if key not in source_by_key:
            raise AssertionError(f"source_boundary_priority_summary.csv: priority row has no source summary row: {row}")
        source_row = source_by_key[key]
        for field in (
            "occurrences",
            "region_events",
            "query_count",
            "max_estimated_cardinality",
            *SOURCE_NATIVE_RUNTIME_FIELDS,
        ):
            if row_int(row, field) != row_int(source_row, field):
                raise AssertionError(
                    f"source_boundary_priority_summary.csv: priority row does not match source summary for "
                    f"{field}: priority={row} source={source_row}"
                )
        for field in ("query_examples", "candidate_shapes", "candidate_scopes", "example_pipeline_shape"):
            if row.get(field, "") != source_row.get(field, ""):
                raise AssertionError(
                    f"source_boundary_priority_summary.csv: priority row does not match source summary for "
                    f"{field}: priority={row} source={source_row}"
                )
        if row_int(row, "profile_time_us") < 0:
            raise AssertionError(f"source_boundary_priority_summary.csv: negative profile time: {row}")
        if row_float(row, "profile_percent_of_policy") < 0:
            raise AssertionError(f"source_boundary_priority_summary.csv: negative profile percent: {row}")
        for field in (
            *SOURCE_NATIVE_RUNTIME_FIELDS,
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"source_boundary_priority_summary.csv: negative source runtime field {field}: {row}")
        if row_int(row, "profile_time_us") > profile_time_by_policy.get(row["policy"], 0):
            raise AssertionError(f"source_boundary_priority_summary.csv: profile time exceeds policy total: {row}")
        allocated_time_by_policy[row["policy"]] = allocated_time_by_policy.get(row["policy"], 0) + row_int(
            row, "profile_time_us"
        )
        policy_operator_key = (row["policy"], row["source_operator"])
        allocated_time_by_policy_operator[policy_operator_key] = allocated_time_by_policy_operator.get(
            policy_operator_key, 0
        ) + row_int(row, "profile_time_us")
        if row_int(row, "profile_time_us") > 0:
            has_profile_time = True
            if row["profile_operators"] == "":
                raise AssertionError(f"source_boundary_priority_summary.csv: profile time without operator evidence: {row}")
            if row["source_boundary_kind"] in {
                "table_scan_generated_source_filter",
                "table_scan_source_boundary",
                "table_scan_native_source",
            }:
                has_table_scan_profile = True
            if row["source_boundary_kind"] in {
                "stateful_source_fallback",
                "stateful_native_state_scan",
                "stateful_native_source",
            }:
                has_stateful_profile = True
        verify_scope_summary_field("source_boundary_priority_summary.csv", row, "candidate_scopes")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"source_boundary_priority_summary.csv: missing example pipeline: {row}")
        if any(node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])):
            raise AssertionError(f"source_boundary_priority_summary.csv: trace wrapper leaked into priority row: {row}")
    if set(priority_by_key) != set(source_by_key):
        raise AssertionError(
            "source_boundary_priority_summary.csv: priority keys do not match source summary keys: "
            f"priority={sorted(priority_by_key)} source={sorted(source_by_key)}"
        )
    for policy, allocated_time_us in allocated_time_by_policy.items():
        if allocated_time_us > profile_time_by_policy.get(policy, 0):
            raise AssertionError(
                "source_boundary_priority_summary.csv: allocated source profile time exceeds measured policy "
                f"profile time: policy={policy} allocated={allocated_time_us} measured={profile_time_by_policy.get(policy, 0)}"
            )
    for policy_operator_key, allocated_time_us in allocated_time_by_policy_operator.items():
        measured_time_us = profile_time_by_policy_operator.get(policy_operator_key, 0)
        if allocated_time_us > measured_time_us:
            raise AssertionError(
                "source_boundary_priority_summary.csv: allocated source profile time exceeds measured operator "
                f"profile time: key={policy_operator_key} allocated={allocated_time_us} measured={measured_time_us}"
            )
    if not has_profile_time:
        raise AssertionError("source_boundary_priority_summary.csv: no source priority row has profiler time")
    if not has_table_scan_profile:
        raise AssertionError("source_boundary_priority_summary.csv: no table scan source priority row has profiler time")
    if require_full_tpch_coverage and not has_stateful_profile:
        raise AssertionError("source_boundary_priority_summary.csv: no stateful source priority row has profiler time")


def verify_source_fusion_gaps(
    rows: list,
    source_boundary_rows: list,
    operator_profile_rows: list,
    expected_queries: list,
    policies: list,
    require_full_lowering: bool,
) -> None:
    if not rows:
        return
    expected_query_set = set(expected_queries)
    profile_time_by_policy = {}
    for row in operator_profile_rows:
        profile_time_by_policy[row["policy"]] = profile_time_by_policy.get(row["policy"], 0) + row_int(
            row, "operator_time_us"
        )
    source_boundary_keys = {
        (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["source_operator"],
            row["source_execution"],
            row["scan_function"],
        )
        for row in source_boundary_rows
    }
    seen_keys = set()
    has_force_evidence = False
    for row in rows:
        key = (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["source_fusion_gap"],
            row["source_operator"],
            row["source_execution"],
            row["native_source_status"],
            row["native_source_required_capability"],
            row["native_source_protocol"],
            row["native_source_blocker"],
            row["scan_function"],
        )
        if key in seen_keys:
            raise AssertionError(f"source_fusion_gap_summary.csv: duplicate key: {row}")
        seen_keys.add(key)
        if row["policy"] == "off":
            raise AssertionError(f"source_fusion_gap_summary.csv: off policy should not have gaps: {row}")
        if row["source_fusion_gap"] != "requires_native_source":
            raise AssertionError(f"source_fusion_gap_summary.csv: unknown source fusion gap: {row}")
        if row["source_execution"] in ("", "native-source"):
            raise AssertionError(f"source_fusion_gap_summary.csv: gap row must name non-native source execution: {row}")
        native_ready_handoff = (
            row["source_execution"] == "duckdb-source-boundary" and row["native_source_status"] == "ready"
        )
        if not native_ready_handoff and row["native_source_status"] != "blocked":
            raise AssertionError(f"source_fusion_gap_summary.csv: native source contract is not blocked: {row}")
        if row["native_source_required_capability"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing native source capability: {row}")
        if row["native_source_protocol"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing native source protocol version: {row}")
        if native_ready_handoff:
            if row["native_source_blocker"] != "none":
                raise AssertionError(f"source_fusion_gap_summary.csv: ready source handoff has blocker: {row}")
        elif row["native_source_blocker"] in ("", "none"):
            raise AssertionError(f"source_fusion_gap_summary.csv: missing native source blocker: {row}")
        if row["region_execution_form"] != "none":
            raise AssertionError(f"source_fusion_gap_summary.csv: source fusion gap has invalid execution form: {row}")
        if row["execution_mode"] not in {"executor_fallback", "unsupported"}:
            raise AssertionError(f"source_fusion_gap_summary.csv: unexpected execution mode: {row}")
        if row["status"] == "unsupported":
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"source_fusion_gap_summary.csv: unsupported source gap is not honest: {row}")
            has_force_evidence = has_force_evidence or row["policy"] == "force"
        if row["status"] == "compiled":
            raise AssertionError(f"source_fusion_gap_summary.csv: source fusion gap was compiled: {row}")
        if row["status"] == "skipped" and row["region_execution_form"] == "none":
            has_force_evidence = has_force_evidence or row["policy"] == "force"
        if row_int(row, "occurrences") <= 0 or row_int(row, "region_events") <= 0:
            raise AssertionError(f"source_fusion_gap_summary.csv: non-positive occurrences/events: {row}")
        if row_int(row, "region_events") > row_int(row, "occurrences"):
            raise AssertionError(f"source_fusion_gap_summary.csv: events exceed occurrences: {row}")
        for field in (
            "runtime_input_rows",
            "runtime_output_rows",
            "runtime_invocations",
            "runtime_time_us",
            *SOURCE_NATIVE_RUNTIME_FIELDS,
            "generated_body_runtime_time_us",
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"source_fusion_gap_summary.csv: negative runtime field {field}: {row}")
        if row["status"] == "compiled":
            if row_int(row, "runtime_invocations") <= 0 or row_int(row, "runtime_time_us") <= 0:
                raise AssertionError(f"source_fusion_gap_summary.csv: compiled gap lacks kernel runtime: {row}")
        for field in SOURCE_NATIVE_RUNTIME_FIELDS:
            if row_int(row, field) != 0:
                raise AssertionError(f"source_fusion_gap_summary.csv: blocked source gap has native runtime {field}: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"source_fusion_gap_summary.csv: missing query coverage: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing query examples: {row}")
        for query_example in row["query_examples"].split(","):
            query_id = query_example.strip().split("(")[0].removeprefix("q")
            if query_id and query_id not in expected_query_set:
                raise AssertionError(f"source_fusion_gap_summary.csv: unexpected query example {query_id}: {row}")
        verify_scope_summary_field("source_fusion_gap_summary.csv", row, "candidate_scopes")
        if "full_pipeline" not in row["candidate_scopes"]:
            raise AssertionError(f"source_fusion_gap_summary.csv: gap row is not a source/full pipeline: {row}")
        if row["candidate_shapes"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing candidate shapes: {row}")
        if row["admission_shape_keys"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing admission shape keys: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing example pipeline: {row}")
        source_nodes = [
            node for node in parse_pipeline_shape(row["example_pipeline_shape"]) if node["node_kind"] == "source"
        ]
        if not source_nodes:
            raise AssertionError(f"source_fusion_gap_summary.csv: example pipeline has no source node: {row}")
        if row["source_operator"] not in {node["operator_name"] for node in source_nodes}:
            raise AssertionError(f"source_fusion_gap_summary.csv: example pipeline has wrong source: {row}")
        if any(node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])):
            raise AssertionError(f"source_fusion_gap_summary.csv: trace wrapper leaked into source fusion row: {row}")
        if row["example_reason"] == "":
            raise AssertionError(f"source_fusion_gap_summary.csv: missing example reason: {row}")
        if (
            "source-fusion-gap:requires-native-source" not in row["example_reason"]
            and "source-fusion-gap:downstream-operator-resume-protocol-missing" not in row["example_reason"]
            and row["native_source_blocker"] != "hash-join-source-does-not-produce-rows-for-join-type"
            and "source-pushed filters require native-source filter split" not in row["example_reason"]
        ):
            raise AssertionError(f"source_fusion_gap_summary.csv: example reason missing source fusion root cause: {row}")
        for field in (
            "profile_time_us",
            "generated_body_runtime_time_us",
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"source_fusion_gap_summary.csv: negative field {field}: {row}")
        if row_int(row, "profile_time_us") > profile_time_by_policy.get(row["policy"], 0):
            raise AssertionError(f"source_fusion_gap_summary.csv: profile time exceeds policy total: {row}")
        if (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["source_operator"],
            row["source_execution"],
            row["scan_function"],
        ) not in source_boundary_keys:
            raise AssertionError(f"source_fusion_gap_summary.csv: row has no matching source boundary row: {row}")
        if row["source_operator"] == "TABLE_SCAN":
            if row["scan_function"] == "":
                raise AssertionError(f"source_fusion_gap_summary.csv: table scan source missing scan function: {row}")
            if row["native_source_required_capability"] != "duckdb-table-scan-native-source":
                raise AssertionError(f"source_fusion_gap_summary.csv: table scan source has wrong native capability: {row}")
            native_ready_handoff = (
                row["source_execution"] == "duckdb-source-boundary" and row["native_source_status"] == "ready"
            )
            if (
                row["source_fusion_gap"] == "requires_native_source"
                and not native_ready_handoff
                and row["native_source_blocker"] != "duckdb-table-scan-source-boundary"
            ):
                raise AssertionError(f"source_fusion_gap_summary.csv: table scan source has wrong native blocker: {row}")
    if require_full_lowering and "force" in policies and not has_force_evidence:
        raise AssertionError("source_fusion_gap_summary.csv: missing force source-fusion diagnostic")


def verify_fusion_blockers(
    rows: list,
    source_fusion_gap_rows: list,
    operator_profile_rows: list,
    expected_queries: list,
    policies: list,
    require_full_lowering: bool,
    manifest: dict,
) -> None:
    if manifest.get("configuration", {}).get("fusion_blocker_rows") != len(rows):
        raise AssertionError(
            "trace_manifest.json: fusion_blocker_rows mismatch: "
            f"manifest={manifest.get('configuration', {}).get('fusion_blocker_rows')} actual={len(rows)}"
        )
    if not rows:
        return
    expected_query_set = set(expected_queries)
    profile_time_by_policy = {}
    for row in operator_profile_rows:
        profile_time_by_policy[row["policy"]] = profile_time_by_policy.get(row["policy"], 0) + row_int(
            row, "operator_time_us"
        )
    seen_keys = set()
    blocker_classes = set()
    source_blocker_keys = set()
    has_table_scan_source_blocker = False
    has_sink_blocker = False
    for row in rows:
        key = (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["fusion_blocker"],
            row["source_kind"],
            row["source_execution"],
            row["native_source_required_capability"],
            row["native_source_blocker"],
            row["sink_kind"],
        )
        if key in seen_keys:
            raise AssertionError(f"fusion_blocker_summary.csv: duplicate key: {row}")
        seen_keys.add(key)
        if row["policy"] == "off":
            raise AssertionError(f"fusion_blocker_summary.csv: off policy should not have fusion blockers: {row}")
        if row["fusion_blocker"] == "" or row["blocker_class"] == "":
            raise AssertionError(f"fusion_blocker_summary.csv: missing blocker identity: {row}")
        if not row["fusion_blocker"].startswith(row["blocker_class"] + ":"):
            raise AssertionError(f"fusion_blocker_summary.csv: blocker class does not match blocker: {row}")
        if row["blocker_class"] not in {
            "source-fusion-gap",
            "sink-fusion-gap",
            "candidate-fusion-gap",
            "operator-fusion-gap",
        }:
            raise AssertionError(f"fusion_blocker_summary.csv: unknown blocker class: {row}")
        unsupported_sink_protocol_blocker = row["fusion_blocker"] in {
            "sink-fusion-gap:hash-join-build-protocol-missing",
            "sink-fusion-gap:hash-join-build-native-lowering",
        }
        unsupported_sink_resume_blocker = (
            row["fusion_blocker"] == "sink-fusion-gap:upstream-operator-resume-protocol-missing"
        )
        unsupported_source_resume_blocker = (
            row["fusion_blocker"] == "source-fusion-gap:downstream-operator-resume-protocol-missing"
        )
        if (
            row["blocker_class"] in {"source-fusion-gap", "sink-fusion-gap"}
            and not unsupported_sink_protocol_blocker
            and not unsupported_sink_resume_blocker
            and not unsupported_source_resume_blocker
        ):
            if row["status"] != "unsupported":
                raise AssertionError(f"fusion_blocker_summary.csv: source/sink blocker row is not unsupported: {row}")
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"fusion_blocker_summary.csv: source/sink blocker has bad form: {row}")
        if unsupported_source_resume_blocker:
            if row["status"] != "unsupported":
                raise AssertionError(f"fusion_blocker_summary.csv: source resume blocker should be unsupported: {row}")
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"fusion_blocker_summary.csv: source resume blocker has bad form: {row}")
        if unsupported_sink_protocol_blocker:
            if row["status"] != "unsupported":
                raise AssertionError(f"fusion_blocker_summary.csv: sink protocol blocker should be unsupported: {row}")
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"fusion_blocker_summary.csv: sink protocol blocker has bad form: {row}")
        if unsupported_sink_resume_blocker:
            if row["status"] != "unsupported":
                raise AssertionError(f"fusion_blocker_summary.csv: sink resume blocker should be unsupported: {row}")
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"fusion_blocker_summary.csv: sink resume blocker has bad form: {row}")
        if row["blocker_class"] == "candidate-fusion-gap":
            if row["fusion_blocker"] not in {
                "candidate-fusion-gap:executor-boundary",
                "candidate-fusion-gap:missing-protocol",
                "candidate-fusion-gap:source-boundary",
            }:
                raise AssertionError(f"fusion_blocker_summary.csv: unknown candidate blocker: {row}")
            if row["status"] == "compiled":
                raise AssertionError(f"fusion_blocker_summary.csv: candidate blocker row was compiled: {row}")
            elif row["status"] == "unsupported":
                if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                    raise AssertionError(f"fusion_blocker_summary.csv: unsupported candidate blocker has bad form: {row}")
            elif row["status"] == "skipped":
                if row["execution_mode"] != "executor_fallback":
                    raise AssertionError(f"fusion_blocker_summary.csv: skipped candidate blocker has bad form: {row}")
                if row["region_execution_form"] != "none":
                    raise AssertionError(f"fusion_blocker_summary.csv: skipped candidate blocker has bad form: {row}")
            else:
                raise AssertionError(f"fusion_blocker_summary.csv: unexpected candidate blocker status: {row}")
        if row["blocker_class"] == "operator-fusion-gap":
            if row["fusion_blocker"] not in {
                "operator-fusion-gap:multi-operator-resume-protocol-missing",
                "operator-fusion-gap:downstream-operator-continuation-protocol-missing",
                "operator-fusion-gap:downstream-operator-resume-protocol-missing",
                "operator-fusion-gap:upstream-operator-resume-protocol-missing",
                "operator-fusion-gap:hash-join-probe-native-lowering-missing",
                "operator-fusion-gap:hash-join-probe-protocol-missing",
            }:
                raise AssertionError(f"fusion_blocker_summary.csv: unknown operator blocker: {row}")
            if row["status"] != "unsupported":
                raise AssertionError(f"fusion_blocker_summary.csv: operator protocol blocker should be unsupported: {row}")
            if row["execution_mode"] != "unsupported" or row["region_execution_form"] != "none":
                raise AssertionError(f"fusion_blocker_summary.csv: operator protocol blocker has bad form: {row}")
        blocker_classes.add(row["blocker_class"])
        if row_int(row, "occurrences") <= 0 or row_int(row, "region_events") <= 0:
            raise AssertionError(f"fusion_blocker_summary.csv: non-positive occurrences/events: {row}")
        if row_int(row, "region_events") > row_int(row, "occurrences"):
            raise AssertionError(f"fusion_blocker_summary.csv: events exceed occurrences: {row}")
        if row_int(row, "query_count") <= 0 or row["query_examples"] == "":
            raise AssertionError(f"fusion_blocker_summary.csv: missing query coverage: {row}")
        for query_example in row["query_examples"].split(","):
            query_id = query_example.strip().split("(")[0].removeprefix("q")
            if query_id and query_id not in expected_query_set:
                raise AssertionError(f"fusion_blocker_summary.csv: unexpected query example {query_id}: {row}")
        verify_scope_summary_field("fusion_blocker_summary.csv", row, "candidate_scopes")
        if row["blocker_class"] == "source-fusion-gap":
            if "full_pipeline" not in row["candidate_scopes"]:
                raise AssertionError(f"fusion_blocker_summary.csv: source blocker row is not source/full pipeline: {row}")
        if row["blocker_class"] == "sink-fusion-gap":
            if "full_pipeline" not in row["candidate_scopes"]:
                raise AssertionError(f"fusion_blocker_summary.csv: sink blocker row is not full pipeline: {row}")
        if row["candidate_shapes"] == "":
            raise AssertionError(f"fusion_blocker_summary.csv: missing candidate shapes: {row}")
        if row["admission_shape_keys"] == "":
            raise AssertionError(f"fusion_blocker_summary.csv: missing admission shape keys: {row}")
        if row["example_pipeline_shape"] == "":
            raise AssertionError(f"fusion_blocker_summary.csv: missing example pipeline: {row}")
        if any(node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["example_pipeline_shape"])):
            raise AssertionError(f"fusion_blocker_summary.csv: trace wrapper leaked into blocker row: {row}")
        if row["example_reason"] == "" or f"fusion-blocker:{row['fusion_blocker']}" not in row["example_reason"]:
            raise AssertionError(f"fusion_blocker_summary.csv: example reason missing blocker text: {row}")
        for field in (
            "profile_time_us",
            "runtime_input_rows",
            "runtime_output_rows",
            "runtime_invocations",
            "runtime_time_us",
            *SOURCE_NATIVE_RUNTIME_FIELDS,
            "generated_body_runtime_time_us",
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"fusion_blocker_summary.csv: negative field {field}: {row}")
        if row_int(row, "profile_time_us") > profile_time_by_policy.get(row["policy"], 0):
            raise AssertionError(f"fusion_blocker_summary.csv: profile time exceeds policy total: {row}")
        if row["blocker_class"] == "source-fusion-gap":
            source_blocker_keys.add(
                (
                    row["policy"],
                    row["status"],
                    row["execution_mode"],
                    row["region_execution_form"],
                    row["source_kind"],
                    row["source_execution"],
                    row["native_source_required_capability"],
                    row["native_source_blocker"],
                )
            )
            downstream_operator_resume_blocker = (
                row["fusion_blocker"] == "source-fusion-gap:downstream-operator-resume-protocol-missing"
            )
            if row["source_execution"] == "" or (
                row["source_execution"] == "native-source" and not downstream_operator_resume_blocker
            ):
                raise AssertionError(f"fusion_blocker_summary.csv: source blocker lacks expected source boundary: {row}")
            if (
                downstream_operator_resume_blocker
                and row["source_execution"] == "native-source"
                and row["native_source_status"] != "ready"
            ):
                raise AssertionError(
                    f"fusion_blocker_summary.csv: downstream operator resume blocker lacks ready native source: {row}"
                )
            native_ready_handoff = (
                row["source_execution"] == "duckdb-source-boundary" and row["native_source_status"] == "ready"
            )
            if (
                row["fusion_blocker"] == "source-fusion-gap:requires-native-source"
                and not native_ready_handoff
                and row["native_source_status"] != "blocked"
            ):
                raise AssertionError(f"fusion_blocker_summary.csv: source blocker lacks blocked native contract: {row}")
            if row["native_source_required_capability"] == "":
                raise AssertionError(f"fusion_blocker_summary.csv: source blocker lacks native-source details: {row}")
            if row["source_kind"] == "duckdb-table-scan":
                has_table_scan_source_blocker = True
                if row["native_source_required_capability"] != "duckdb-table-scan-native-source":
                    raise AssertionError(f"fusion_blocker_summary.csv: table scan source has wrong capability: {row}")
                if (
                    row["fusion_blocker"] == "source-fusion-gap:requires-native-source"
                    and not native_ready_handoff
                    and row["native_source_blocker"] != "duckdb-table-scan-source-boundary"
                ):
                    raise AssertionError(f"fusion_blocker_summary.csv: table scan source has wrong blocker: {row}")
        if row["blocker_class"] == "sink-fusion-gap":
            has_sink_blocker = True
            if row["fusion_blocker"] not in {
                "sink-fusion-gap:requires-native-sink-or-operator-update",
                "sink-fusion-gap:hash-join-build-protocol-missing",
                "sink-fusion-gap:hash-join-build-native-lowering",
                "sink-fusion-gap:upstream-operator-resume-protocol-missing",
            }:
                raise AssertionError(f"fusion_blocker_summary.csv: unknown sink blocker: {row}")
            if row["sink_kind"] in ("", "none"):
                raise AssertionError(f"fusion_blocker_summary.csv: sink blocker lacks sink kind: {row}")
    source_fusion_keys = {
        (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            "duckdb-table-scan" if row["source_operator"] == "TABLE_SCAN" else row["source_operator"],
            row["source_execution"],
            row["native_source_required_capability"],
            row["native_source_blocker"],
        )
        for row in source_fusion_gap_rows
        if row["status"] == "skipped" and row["region_execution_form"] == "none"
    }
    if not source_blocker_keys.issuperset(source_fusion_keys):
        raise AssertionError(
            "fusion_blocker_summary.csv: source blocker rows do not cover source_fusion_gap_summary.csv: "
            f"blockers={sorted(source_blocker_keys)} source_fusion={sorted(source_fusion_keys)}"
        )

def verify_region_decisions(rows: list) -> None:
    if not rows:
        raise AssertionError("region_decision_summary.csv: expected rows")
    supported_statuses = {"compiled", "skipped", "unsupported"}
    for row in rows:
        if row["status"] not in supported_statuses:
            raise AssertionError(f"region_decision_summary.csv: unknown status {row['status']}")
        if row["region_execution_form"] not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"region_decision_summary.csv: unknown execution form: {row}")
        if row_int(row, "count") <= 0:
            raise AssertionError(f"region_decision_summary.csv: non-positive count: {row}")
        if row["candidate_pipeline_shape"] == "":
            raise AssertionError(f"region_decision_summary.csv: missing pipeline shape: {row}")
        if row.get("candidate_context_pipeline_shape", "") == "":
            raise AssertionError(f"region_decision_summary.csv: missing context pipeline shape: {row}")
        executable_nodes = parse_pipeline_shape(row["candidate_pipeline_shape"])
        if row.get("candidate_shape", "") in ("", "none"):
            verify_candidate_scope_summary_value(
                "region_decision_summary.csv", row, "candidate_scope", allow_no_candidate=True
            )
        elif row["status"] == "compiled":
            verify_executable_candidate_scope("region_decision_summary.csv", row)
        else:
            verify_candidate_scope("region_decision_summary.csv", row)
        if row["status"] != "compiled" and row["example_reason"] == "":
            raise AssertionError(f"region_decision_summary.csv: missing miss reason: {row}")
        verify_admission_metadata("region_decision_summary.csv", row)


def verify_decision_counter_summary(rows: list, policies: list, event_log_size: int,
                                    complete_auto_inventory_trace: bool) -> None:
    if not rows:
        raise AssertionError("decision_counter_summary.csv: expected rows")
    has_auto_region_skip = False
    has_shape_key = False
    has_below_threshold = False
    for row in rows:
        if row_int(row, "count") <= 0:
            raise AssertionError(f"decision_counter_summary.csv: non-positive count: {row}")
        region_execution_form = row.get("region_execution_form", "")
        if row.get("target") == "region" and region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"decision_counter_summary.csv: unknown region execution form: {row}")
        if row.get("target") == "region" and row.get("status") == "compiled" and region_execution_form == "none":
            raise AssertionError(f"decision_counter_summary.csv: compiled region counter has no execution form: {row}")
        if row.get("pipeline_shape", "") in WRAPPER_ONLY_PIPELINE_SHAPES:
            raise AssertionError(f"decision_counter_summary.csv: wrapper-only pipeline leaked into counters: {row}")
        if any(marker in row.get("example_reason", "") for marker in WRAPPER_ONLY_REGION_SOURCE_MARKERS):
            raise AssertionError(f"decision_counter_summary.csv: wrapper-only source leaked into counters: {row}")
        if row["target"] == "region" and row["policy"] == "auto" and row["status"] == "skipped":
            has_auto_region_skip = True
        if row.get("admission_shape_key", ""):
            has_shape_key = True
        if row.get("admission_rule_present") == "true":
            if row_int(row, "admission_min_cardinality") <= 0:
                raise AssertionError(f"decision_counter_summary.csv: rule row missing min cardinality: {row}")
            if not row.get("admission_proof", ""):
                raise AssertionError(f"decision_counter_summary.csv: rule row missing proof: {row}")
            if row.get("has_admission_score") != "true":
                raise AssertionError(f"decision_counter_summary.csv: rule row missing admission score: {row}")
            if row_int(row, "max_admission_score") < 0:
                has_below_threshold = True
    if "auto" in policies and complete_auto_inventory_trace and not has_auto_region_skip:
        raise AssertionError("decision_counter_summary.csv: auto policy produced no skipped region decision counters")
    if "auto" in policies and not has_shape_key:
        raise AssertionError("decision_counter_summary.csv: auto policy produced no admission shape keys")
    if event_log_size == 0 and "auto" in policies and not has_below_threshold:
        raise AssertionError("decision_counter_summary.csv: event-log-zero trace lacks below-threshold admission proof")


def verify_decision_counters_match_summary(summary_rows: list, decision_rows: list, event_log_size: int) -> None:
    by_query_policy = {(row["query"], row["policy"]): row for row in summary_rows}
    if event_log_size == 0:
        for row in summary_rows:
            if row_int(row, "event_count") != 0:
                raise AssertionError(f"summary.csv: event-log-zero trace retained events: {row}")

    decision_counts = {}
    for row in decision_rows:
        if row.get("target") != "region":
            continue
        status = row.get("status", "")
        if status not in {"compiled", "skipped", "unsupported"}:
            continue
        key = (row["query"], row["policy"], status)
        decision_counts[key] = decision_counts.get(key, 0) + row_int(row, "count")

    for key, summary_row in by_query_policy.items():
        for status, field in (
            ("compiled", "compiled_regions"),
            ("skipped", "skipped_regions"),
            ("unsupported", "unsupported_regions"),
        ):
            decision_count = decision_counts.get((key[0], key[1], status), 0)
            summary_count = row_int(summary_row, field)
            if decision_count != summary_count:
                raise AssertionError(
                    "decision_counter_summary.csv: region decision count mismatch for "
                    f"q{key[0]} {key[1]} {status}: counters={decision_count}, summary={summary_count}"
                )


def verify_stage_pipelines(rows: list, region_rows: list) -> None:
    if not rows:
        raise AssertionError("stage_pipeline_summary.csv: expected rows")
    region_keys = {}
    for region_row in region_rows:
        key = (
            region_row["policy"],
            region_row["status"],
            region_row["execution_mode"],
            region_row["region_execution_form"],
            region_row["candidate_shape"],
            region_row["candidate_pipeline_shape"],
            region_row.get("candidate_context_pipeline_shape", ""),
            region_row.get("candidate_scope", ""),
        )
        entry = region_keys.setdefault(key, {"count": 0})
        entry["count"] += row_int(region_row, "count")
    total_stage_time = 0
    for row in rows:
        key = (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_pipeline_shape"],
            row.get("candidate_context_pipeline_shape", ""),
            row.get("candidate_scope", ""),
        )
        if key not in region_keys:
            raise AssertionError(f"stage_pipeline_summary.csv: row has no matching region decision row: {row}")
        region_row = region_keys[key]
        if row_int(row, "count") != row_int(region_row, "count"):
            raise AssertionError(f"stage_pipeline_summary.csv: count mismatch with region decisions: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"stage_pipeline_summary.csv: non-positive query_count: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"stage_pipeline_summary.csv: missing query examples: {row}")
        if row["candidate_pipeline_shape"] == "":
            raise AssertionError(f"stage_pipeline_summary.csv: missing pipeline shape: {row}")
        if row.get("candidate_context_pipeline_shape", "") == "":
            raise AssertionError(f"stage_pipeline_summary.csv: missing context pipeline shape: {row}")
        if row.get("candidate_shape", "") in ("", "none"):
            verify_candidate_scope_summary_value(
                "stage_pipeline_summary.csv", row, "candidate_scope", allow_no_candidate=True
            )
        elif row["status"] == "compiled":
            verify_executable_candidate_scope("stage_pipeline_summary.csv", row)
        else:
            verify_candidate_scope("stage_pipeline_summary.csv", row)
        has_trace_wrapper = any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS for node in parse_pipeline_shape(row["candidate_pipeline_shape"])
        )
        if has_trace_wrapper:
            raise AssertionError(f"stage_pipeline_summary.csv: trace wrapper leaked into stage summary: {row}")
        stage_total = sum(row_int(row, field) for field in STAGE_FIELDS)
        total_stage_time += stage_total
        if row_int(row, "stage_total_time_us") != stage_total:
            raise AssertionError(f"stage_pipeline_summary.csv: stage total mismatch: {row}")
        if row["dominant_stage"] not in STAGE_FIELDS:
            raise AssertionError(f"stage_pipeline_summary.csv: invalid dominant_stage: {row}")
        if row_int(row, "dominant_stage_time_us") != row_int(row, row["dominant_stage"]):
            raise AssertionError(f"stage_pipeline_summary.csv: dominant stage time mismatch: {row}")
        if row["status"] == "compiled" and row_int(row, "codegen_time_us") <= 0:
            raise AssertionError(f"stage_pipeline_summary.csv: compiled row without codegen time: {row}")
        if row["status"] != "compiled" and row["example_reason"] == "":
            raise AssertionError(f"stage_pipeline_summary.csv: missing miss reason: {row}")
    if total_stage_time <= 0:
        raise AssertionError("stage_pipeline_summary.csv: total stage time is not positive")


def zero_code_native_protocol_key(row: dict) -> tuple:
    return (
        row.get("query", ""),
        row.get("policy", ""),
        row.get("execution_mode", ""),
        row.get("region_execution_form", ""),
        row.get("candidate_shape", ""),
        row.get("candidate_pipeline_shape", ""),
        row.get("candidate_context_pipeline_shape", ""),
        row.get("candidate_scope", ""),
    )


def is_zero_code_native_protocol_region(row: dict) -> bool:
    return (
        row.get("status", "") == "compiled"
        and row.get("execution_mode", "") == "native"
        and row_int(row, "code_size") <= 0
    )


def is_allowed_zero_code_native_protocol_row(row: dict, protocol_region_keys: set) -> bool:
    return (
        row.get("execution_mode", "") == "native"
        and row_int(row, "code_size") <= 0
        and zero_code_native_protocol_key(row) in protocol_region_keys
    )


def verify_kernel_runtime(rows: list, summary_rows: list, region_rows: list, require_runtime: bool) -> None:
    compiled_regions = sum(row_int(row, "compiled_regions") for row in summary_rows)
    runtime_rows = sum(row_int(row, "runtime_input_rows") for row in summary_rows)
    zero_code_protocol_region_keys = {
        zero_code_native_protocol_key(row) for row in region_rows if is_zero_code_native_protocol_region(row)
    }
    if require_runtime and compiled_regions > 0 and not rows:
        raise AssertionError("kernel_runtime_summary.csv: compiled regions exist but no kernel runtime rows were retained")
    if require_runtime and compiled_regions > 0 and runtime_rows <= 0:
        raise AssertionError("summary.csv: compiled regions exist but runtime_input_rows is zero")
    for row in rows:
        if (
            row["execution_mode"] == "native"
            and row_int(row, "code_size") <= 0
            and not is_allowed_zero_code_native_protocol_row(row, zero_code_protocol_region_keys)
        ):
            raise AssertionError(f"kernel_runtime_summary.csv: compiled kernel with non-positive code size: {row}")
        if row["candidate_pipeline_shape"] == "":
            raise AssertionError(f"kernel_runtime_summary.csv: missing pipeline shape: {row}")
        if row.get("candidate_context_pipeline_shape", "") == "":
            raise AssertionError(f"kernel_runtime_summary.csv: missing context pipeline shape: {row}")
        verify_executable_candidate_scope("kernel_runtime_summary.csv", row)
        if row_int(row, "kernels") <= 0:
            raise AssertionError(f"kernel_runtime_summary.csv: non-positive kernel count: {row}")
        reached_kernels = row_int(row, "reached_kernels")
        row_processing_kernels = row_int(row, "row_processing_kernels")
        unreached_kernels = row_int(row, "unreached_kernels")
        zero_input_kernels = row_int(row, "zero_input_kernels")
        kernels = row_int(row, "kernels")
        if kernels != reached_kernels + unreached_kernels:
            raise AssertionError(f"kernel_runtime_summary.csv: kernels do not equal reached+unreached: {row}")
        if kernels != row_processing_kernels + zero_input_kernels:
            raise AssertionError(f"kernel_runtime_summary.csv: kernels do not equal row+zero-input: {row}")
        if row_processing_kernels > reached_kernels:
            raise AssertionError(f"kernel_runtime_summary.csv: row-processing kernels exceed reached kernels: {row}")
        if row_int(row, "input_rows") > 0 and row_processing_kernels <= 0:
            raise AssertionError(f"kernel_runtime_summary.csv: input rows without row-processing kernel: {row}")
        if row_int(row, "invocations") > 0 and reached_kernels <= 0:
            raise AssertionError(f"kernel_runtime_summary.csv: invocations without reached kernel: {row}")
        if row_int(row, "source_native_output_rows") > 0 and row_int(row, "source_native_invocations") <= 0:
            raise AssertionError(f"kernel_runtime_summary.csv: native source rows without invocations: {row}")
        if row_int(row, "source_native_runtime_time_us") < 0:
            raise AssertionError(f"kernel_runtime_summary.csv: negative native source runtime: {row}")
        if row_int(row, "generated_body_runtime_time_us") < 0:
            raise AssertionError(f"kernel_runtime_summary.csv: negative generated body runtime: {row}")


def verify_admission_efficiency(rows: list, kernel_rows: list, summary_rows: list, policies: list) -> None:
    kernel_keys = {
        (
            row["query"],
            row["policy"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_pipeline_shape"],
            row.get("candidate_context_pipeline_shape", ""),
            row.get("candidate_scope", ""),
        )
        for row in kernel_rows
    }
    efficiency_keys = set()
    auto_compiled_regions = sum(
        row_int(row, "compiled_regions") for row in summary_rows if row.get("policy") == "auto"
    )
    if kernel_rows and not rows:
        raise AssertionError(
            "admission_efficiency_summary.csv: kernel runtime rows exist but efficiency rows are empty"
        )
    for row in rows:
        key = (
            row["query"],
            row["policy"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_pipeline_shape"],
            row.get("candidate_context_pipeline_shape", ""),
            row.get("candidate_scope", ""),
        )
        if key in efficiency_keys:
            raise AssertionError(f"admission_efficiency_summary.csv: duplicate efficiency key: {row}")
        efficiency_keys.add(key)
        if key not in kernel_keys:
            raise AssertionError(f"admission_efficiency_summary.csv: row has no kernel runtime summary: {row}")
        if row.get("status") != "compiled":
            raise AssertionError(f"admission_efficiency_summary.csv: non-compiled efficiency row: {row}")
        if row["policy"] not in policies:
            raise AssertionError(f"admission_efficiency_summary.csv: unknown policy: {row}")
        generated_percent = row_float(row, "generated_body_runtime_percent")
        if generated_percent < 0 or generated_percent > 100:
            raise AssertionError(f"admission_efficiency_summary.csv: invalid generated body percent: {row}")
        native_percent = row_float(row, "source_native_runtime_percent")
        if native_percent < 0 or native_percent > 100:
            raise AssertionError(f"admission_efficiency_summary.csv: invalid native source percent: {row}")
        for field in (
            "runtime_time_us",
            "source_native_runtime_time_us",
            "generated_body_runtime_time_us",
        ):
            if row_int(row, field) < 0:
                raise AssertionError(f"admission_efficiency_summary.csv: negative {field}: {row}")
        if row["efficiency_class"] not in {
            "native_source_dominant",
            "generated_body_dominant",
            "mixed_runtime",
            "not_reached",
            "unmeasured",
        }:
            raise AssertionError(f"admission_efficiency_summary.csv: unknown efficiency class: {row}")
        if row["efficiency_class"] == "generated_body_dominant" and generated_percent < 50:
            raise AssertionError(f"admission_efficiency_summary.csv: generated-dominant row below threshold: {row}")
        if row["efficiency_class"] == "native_source_dominant" and native_percent < 50:
            raise AssertionError(
                f"admission_efficiency_summary.csv: native-source-dominant row below threshold: {row}"
            )
        if row["root_cause"] == "source_filter_loop_not_generated":
            if row["efficiency_class"] != "native_source_dominant":
                raise AssertionError(
                    f"admission_efficiency_summary.csv: source-filter root cause is not native-source dominant: {row}"
                )
            if "scan-filter" not in row["candidate_shape"]:
                raise AssertionError(
                    f"admission_efficiency_summary.csv: source-filter root cause has no source filter shape: {row}"
                )
            if generated_percent >= 20:
                raise AssertionError(
                    f"admission_efficiency_summary.csv: source-filter root cause has dominant generated body: {row}"
                )
    if efficiency_keys != kernel_keys:
        missing = sorted(kernel_keys - efficiency_keys)
        extra = sorted(efficiency_keys - kernel_keys)
        raise AssertionError(
            "admission_efficiency_summary.csv: efficiency keys do not match kernel runtime keys; "
            f"missing={missing[:3]} extra={extra[:3]}"
        )
    if auto_compiled_regions > 0 and not any(row["policy"] == "auto" for row in rows):
        raise AssertionError("admission_efficiency_summary.csv: auto compiled regions lack efficiency rows")


def verify_pipeline_runtime(
    rows: list,
    stage_rows: list,
    kernel_rows: list,
    operator_profile_rows: list,
    require_runtime: bool,
) -> None:
    if not rows:
        raise AssertionError("pipeline_runtime_summary.csv: expected rows")
    stage_by_key = {}
    for stage_row in stage_rows:
        key = (
            stage_row["policy"],
            stage_row["status"],
            stage_row["execution_mode"],
            stage_row["region_execution_form"],
            stage_row["candidate_shape"],
            stage_row["candidate_pipeline_shape"],
            stage_row.get("candidate_context_pipeline_shape", ""),
            stage_row.get("candidate_scope", ""),
        )
        stage_by_key[key] = stage_row

    kernel_totals = {}
    for kernel_row in kernel_rows:
        key = (
            kernel_row["policy"],
            kernel_row["execution_mode"],
            kernel_row["region_execution_form"],
            kernel_row["candidate_shape"],
            kernel_row["candidate_pipeline_shape"],
            kernel_row.get("candidate_context_pipeline_shape", ""),
            kernel_row.get("candidate_scope", ""),
        )
        entry = kernel_totals.setdefault(
            key,
            {
                "compiled_kernels": 0,
                "reached_kernels": 0,
                "row_processing_kernels": 0,
                "unreached_kernels": 0,
                "zero_input_kernels": 0,
                "runtime_input_rows": 0,
                "runtime_output_rows": 0,
                "runtime_invocations": 0,
                "runtime_time_us": 0,
                "source_native_output_rows": 0,
                "source_native_invocations": 0,
                "source_native_runtime_time_us": 0,
                "generated_body_runtime_time_us": 0,
            },
        )
        entry["compiled_kernels"] += row_int(kernel_row, "kernels")
        entry["reached_kernels"] += row_int(kernel_row, "reached_kernels")
        entry["row_processing_kernels"] += row_int(kernel_row, "row_processing_kernels")
        entry["unreached_kernels"] += row_int(kernel_row, "unreached_kernels")
        entry["zero_input_kernels"] += row_int(kernel_row, "zero_input_kernels")
        entry["runtime_input_rows"] += row_int(kernel_row, "input_rows")
        entry["runtime_output_rows"] += row_int(kernel_row, "output_rows")
        entry["runtime_invocations"] += row_int(kernel_row, "invocations")
        entry["runtime_time_us"] += row_int(kernel_row, "runtime_time_us")
        entry["source_native_output_rows"] += row_int(kernel_row, "source_native_output_rows")
        entry["source_native_invocations"] += row_int(kernel_row, "source_native_invocations")
        entry["source_native_runtime_time_us"] += row_int(kernel_row, "source_native_runtime_time_us")
        entry["generated_body_runtime_time_us"] += row_int(kernel_row, "generated_body_runtime_time_us")

    profile_time_by_policy = {}
    for profile_row in operator_profile_rows:
        profile_time_by_policy[profile_row["policy"]] = profile_time_by_policy.get(profile_row["policy"], 0) + row_int(
            profile_row, "operator_time_us"
        )

    seen_keys = set()
    for row in rows:
        key = (
            row["policy"],
            row["status"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_pipeline_shape"],
            row.get("candidate_context_pipeline_shape", ""),
            row.get("candidate_scope", ""),
        )
        if key in seen_keys:
            raise AssertionError(f"pipeline_runtime_summary.csv: duplicate pipeline key: {row}")
        seen_keys.add(key)
        if key not in stage_by_key:
            raise AssertionError(f"pipeline_runtime_summary.csv: row has no matching stage row: {row}")
        if row_int(row, "region_events") <= 0:
            raise AssertionError(f"pipeline_runtime_summary.csv: non-positive region_events: {row}")
        if row_int(row, "query_count") <= 0:
            raise AssertionError(f"pipeline_runtime_summary.csv: non-positive query_count: {row}")
        if row["query_examples"] == "":
            raise AssertionError(f"pipeline_runtime_summary.csv: missing query examples: {row}")
        if row["candidate_pipeline_shape"] == "":
            raise AssertionError(f"pipeline_runtime_summary.csv: missing executable pipeline shape: {row}")
        if row.get("candidate_context_pipeline_shape", "") == "":
            raise AssertionError(f"pipeline_runtime_summary.csv: missing context pipeline shape: {row}")
        if row.get("candidate_shape", "") in ("", "none"):
            verify_candidate_scope_summary_value(
                "pipeline_runtime_summary.csv", row, "candidate_scope", allow_no_candidate=True
            )
        elif row["status"] == "compiled":
            verify_executable_candidate_scope("pipeline_runtime_summary.csv", row)
        else:
            verify_candidate_scope("pipeline_runtime_summary.csv", row)
        if any(
            node["operator_name"] in TRACE_WRAPPER_OPERATORS
            for node in parse_pipeline_shape(row["candidate_pipeline_shape"])
        ):
            raise AssertionError(f"pipeline_runtime_summary.csv: trace wrapper leaked into pipeline row: {row}")

        stage_row = stage_by_key[key]
        stage_field_pairs = (
            ("region_events", "count"),
            ("query_count", "query_count"),
            ("max_estimated_cardinality", "max_estimated_cardinality"),
            ("decision_time_us", "decision_time_us"),
            ("compile_time_us", "compile_time_us"),
            ("code_size", "code_size"),
        )
        for row_field, stage_field in stage_field_pairs:
            if row_int(row, row_field) != row_int(stage_row, stage_field):
                raise AssertionError(
                    "pipeline_runtime_summary.csv: field does not match stage summary for "
                    f"{key} field={row_field}: pipeline={row} stage={stage_row}"
                )
        stage_total = sum(row_int(row, field) for field in STAGE_FIELDS)
        if row_int(row, "stage_total_time_us") != stage_total:
            raise AssertionError(f"pipeline_runtime_summary.csv: stage total mismatch: {row}")
        if row["dominant_stage"] not in STAGE_FIELDS:
            raise AssertionError(f"pipeline_runtime_summary.csv: invalid dominant stage: {row}")
        if row_int(row, "dominant_stage_time_us") != row_int(row, row["dominant_stage"]):
            raise AssertionError(f"pipeline_runtime_summary.csv: dominant stage time mismatch: {row}")
        if row_int(row, "profile_time_us") < 0:
            raise AssertionError(f"pipeline_runtime_summary.csv: negative profile time: {row}")
        if row_float(row, "profile_percent_of_policy") < 0:
            raise AssertionError(f"pipeline_runtime_summary.csv: negative profile percent: {row}")
        if row_int(row, "profile_time_us") > profile_time_by_policy.get(row["policy"], 0):
            raise AssertionError(f"pipeline_runtime_summary.csv: profile time exceeds policy total: {row}")
        if row_int(row, "profile_time_us") > 0 and row["profile_operators"] == "":
            raise AssertionError(f"pipeline_runtime_summary.csv: profile time without operator evidence: {row}")

        compiled_kernels = row_int(row, "compiled_kernels")
        reached_kernels = row_int(row, "reached_kernels")
        row_processing_kernels = row_int(row, "row_processing_kernels")
        unreached_kernels = row_int(row, "unreached_kernels")
        zero_input_kernels = row_int(row, "zero_input_kernels")
        if compiled_kernels != reached_kernels + unreached_kernels:
            raise AssertionError(f"pipeline_runtime_summary.csv: compiled/reached/unreached mismatch: {row}")
        if compiled_kernels != row_processing_kernels + zero_input_kernels:
            raise AssertionError(f"pipeline_runtime_summary.csv: compiled/row/zero-input mismatch: {row}")
        if row_processing_kernels > reached_kernels:
            raise AssertionError(f"pipeline_runtime_summary.csv: row-processing kernels exceed reached kernels: {row}")
        if row["status"] != "compiled":
            for field in (
                "compiled_kernels",
                "reached_kernels",
                "row_processing_kernels",
                "unreached_kernels",
                "zero_input_kernels",
                "runtime_input_rows",
                "runtime_output_rows",
                "runtime_invocations",
                "runtime_time_us",
                *SOURCE_NATIVE_RUNTIME_FIELDS,
                "generated_body_runtime_time_us",
            ):
                if row_int(row, field) != 0:
                    raise AssertionError(f"pipeline_runtime_summary.csv: non-compiled row has runtime field {field}: {row}")
            continue

        runtime_key = (
            row["policy"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_pipeline_shape"],
            row.get("candidate_context_pipeline_shape", ""),
            row.get("candidate_scope", ""),
        )
        if require_runtime and runtime_key not in kernel_totals:
            raise AssertionError(f"pipeline_runtime_summary.csv: compiled row has no retained kernel total: {row}")
        runtime_total = kernel_totals.get(runtime_key, {})
        for field in (
            "compiled_kernels",
            "reached_kernels",
            "row_processing_kernels",
            "unreached_kernels",
            "zero_input_kernels",
            "runtime_input_rows",
            "runtime_output_rows",
            "runtime_invocations",
            "runtime_time_us",
            "generated_body_runtime_time_us",
        ):
            if row_int(row, field) != row_int(runtime_total, field):
                raise AssertionError(
                    "pipeline_runtime_summary.csv: runtime field does not match kernel summary for "
                    f"{runtime_key} field={field}: pipeline={row} kernel={runtime_total}"
                )

    if set(stage_by_key) != seen_keys:
        raise AssertionError(
            "pipeline_runtime_summary.csv: pipeline keys do not match stage summary keys: "
            f"pipeline={sorted(seen_keys)} stage={sorted(stage_by_key)}"
        )


def flow_rows_for_query_policy(rows: list, query: str, policy: str, include_kernel_counters: bool) -> list:
    result = []
    for row in rows:
        if row.get("query", "") != query or row.get("policy", "") != policy:
            continue
        if include_kernel_counters != (row.get("phase", "") == "kernel_counter"):
            continue
        result.append(row)
    return result


def sum_flow(rows: list, field: str, **filters) -> int:
    total = 0
    for row in rows:
        if all(row.get(key, "") == value for key, value in filters.items()):
            total += row_int(row, field)
    return total


def verify_flow_step_summary(rows: list, summary_rows: list, manifest: dict, expected_queries: list, policies: list,
                             complete_auto_inventory_trace: bool) -> None:
    if "flow_step_summary.csv" not in manifest["artifacts"]:
        raise AssertionError("trace_manifest.json: missing flow_step_summary.csv")
    if manifest.get("configuration", {}).get("flow_step_rows") != len(rows):
        raise AssertionError(
            "trace_manifest.json: flow_step_rows mismatch: "
            f"manifest={manifest.get('configuration', {}).get('flow_step_rows')} actual={len(rows)}"
        )
    if not rows:
        raise AssertionError("flow_step_summary.csv: expected rows")

    summary_by_query_policy = {(row["query"], row["policy"]): row for row in summary_rows}

    if complete_auto_inventory_trace:
        expected_query_policy = {(query, policy) for query in expected_queries for policy in policies}
    else:
        expected_query_policy = {
            (row["query"], row["policy"])
            for row in summary_rows
            if (
                row_int(row, "event_count") > 0
                or row_int(row, "compiled_regions") > 0
                or row_int(row, "runtime_events") > 0
            )
        }
    actual_query_policy = {(row.get("query", ""), row.get("policy", "")) for row in rows}
    missing = expected_query_policy - actual_query_policy
    if missing:
        raise AssertionError(f"flow_step_summary.csv: missing query/policy rows {sorted(missing)}")

    for row in rows:
        phase = row.get("phase", "")
        target = row.get("target", "")
        status = row.get("status", "")
        execution_mode = row.get("execution_mode", "")
        region_execution_form = row.get("region_execution_form", "")
        candidate_shape = row.get("candidate_shape", "")
        if target != "region":
            raise AssertionError(f"flow_step_summary.csv: forbidden non-region JIT target: {row}")
        if target == "region" and region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"flow_step_summary.csv: unknown execution form: {row}")

        if phase == "kernel_counter":
            if row_int(row, "event_count") != 0:
                raise AssertionError(f"flow_step_summary.csv: kernel counter row has event count: {row}")
            if row_int(row, "kernel_count") <= 0:
                raise AssertionError(f"flow_step_summary.csv: kernel counter row has no kernels: {row}")
            if row_int(row, "kernel_count") != row_int(row, "reached_kernels") + row_int(row, "unreached_kernels"):
                raise AssertionError(f"flow_step_summary.csv: kernel/reached/unreached mismatch: {row}")
            if row_int(row, "kernel_count") != row_int(row, "row_processing_kernels") + row_int(row, "zero_input_kernels"):
                raise AssertionError(f"flow_step_summary.csv: kernel/row/zero-input mismatch: {row}")
            if row_int(row, "input_rows") > 0 and row_int(row, "row_processing_kernels") <= 0:
                raise AssertionError(f"flow_step_summary.csv: positive input without row-processing kernel: {row}")
            if row_int(row, "invocations") > 0 and row_int(row, "reached_kernels") <= 0:
                raise AssertionError(f"flow_step_summary.csv: positive invocations without reached kernel: {row}")
        else:
            if row_int(row, "event_count") <= 0:
                raise AssertionError(f"flow_step_summary.csv: event row has no events: {row}")
            if row_int(row, "kernel_count") != 0:
                raise AssertionError(f"flow_step_summary.csv: event row has kernel count: {row}")

        if target == "region" and candidate_shape not in ("", "none"):
            is_executable = (
                status == "compiled"
                or phase == "kernel_counter"
                or (phase == "runtime" and execution_mode == "native")
            )
            if is_executable:
                verify_executable_candidate_scope("flow_step_summary.csv", row)
            else:
                verify_candidate_scope("flow_step_summary.csv", row)
            if row.get("candidate_pipeline_shape", "") == "":
                raise AssertionError(f"flow_step_summary.csv: missing executable pipeline shape: {row}")
            if row.get("candidate_context_pipeline_shape", "") == "":
                raise AssertionError(f"flow_step_summary.csv: missing context pipeline shape: {row}")
        if target == "region" and status == "compiled" and execution_mode == "native":
            summary_row = summary_by_query_policy.get((row.get("query", ""), row.get("policy", "")), {})
            zero_code_native_compile_events = row_int(summary_row, "zero_code_native_compile_events")
            if row_int(row, "code_size") <= 0 and zero_code_native_compile_events != 0:
                raise AssertionError(f"flow_step_summary.csv: compiled flow row has no code: {row}")
        for runtime_field in (
            "runtime_time_us",
            "source_native_runtime_time_us",
            "generated_body_runtime_time_us",
        ):
            if row_int(row, runtime_field) < 0:
                raise AssertionError(f"flow_step_summary.csv: negative runtime field {runtime_field}: {row}")
        if row_int(row, "source_native_output_rows") > 0 and row_int(row, "source_native_invocations") <= 0:
            raise AssertionError(f"flow_step_summary.csv: native source rows without invocations: {row}")
        verify_admission_metadata("flow_step_summary.csv", row)

    for summary_row in summary_rows:
        query = summary_row["query"]
        policy = summary_row["policy"]
        event_rows = flow_rows_for_query_policy(rows, query, policy, include_kernel_counters=False)
        kernel_rows = flow_rows_for_query_policy(rows, query, policy, include_kernel_counters=True)

        if sum_flow(event_rows, "event_count") != row_int(summary_row, "event_count"):
            raise AssertionError(f"flow_step_summary.csv: event count mismatch for q{query} {policy}")
        non_region_events = sum(row_int(row, "event_count") for row in event_rows if row.get("target", "") != "region")
        if non_region_events != row_int(summary_row, "non_region_events"):
            raise AssertionError(f"flow_step_summary.csv: non_region_events mismatch for q{query} {policy}")
        expected_counts = (
            ("compiled_regions", {"target": "region", "status": "compiled"}),
            ("skipped_regions", {"target": "region", "status": "skipped"}),
            ("unsupported_regions", {"target": "region", "status": "unsupported"}),
            ("runtime_events", {"phase": "runtime"}),
        )
        for summary_field, filters in expected_counts:
            flow_count = sum_flow(event_rows, "event_count", **filters)
            summary_count = row_int(summary_row, summary_field)
            if summary_field in {"compiled_regions", "skipped_regions", "unsupported_regions"}:
                if flow_count > summary_count:
                    raise AssertionError(f"flow_step_summary.csv: {summary_field} exceeds summary for q{query} {policy}")
                continue
            if flow_count != summary_count:
                raise AssertionError(f"flow_step_summary.csv: {summary_field} mismatch for q{query} {policy}")

        for field in (
            "runtime_input_rows",
            "runtime_output_rows",
            "runtime_invocations",
            "runtime_time_us",
            "generated_body_runtime_time_us",
            "ir_lowering_time_us",
            "backend_analysis_time_us",
            "admission_time_us",
            "overlap_check_time_us",
            "codegen_time_us",
        ):
            flow_field = {
                "runtime_input_rows": "input_rows",
                "runtime_output_rows": "output_rows",
                "runtime_invocations": "invocations",
            }.get(field, field)
            phase_filter = (
                {"phase": "runtime"}
                if field.startswith("runtime_")
                or field.startswith("source_boundary_")
                or field.startswith("generated_body_")
                else {}
            )
            if sum_flow(event_rows, flow_field, **phase_filter) != row_int(summary_row, field):
                raise AssertionError(f"flow_step_summary.csv: {field} mismatch for q{query} {policy}")

        if row_int(summary_row, "compiled_regions") > 0 and sum_flow(kernel_rows, "kernel_count") <= 0:
            raise AssertionError(f"flow_step_summary.csv: compiled regions without kernel rows for q{query} {policy}")


def verify_operator_profile(rows: list, summary_rows: list, expected_queries: list, policies: list) -> None:
    if not rows:
        raise AssertionError("operator_profile_summary.csv: expected rows")
    expected_keys = {(row["query"], row["policy"]) for row in summary_rows}
    actual_queries = sorted({row["query"] for row in summary_rows})
    if actual_queries != expected_queries:
        raise AssertionError(f"operator_profile_summary.csv: expected queries {expected_queries}, found {actual_queries}")
    seen_keys = set()
    for row in rows:
        if row["operator_name"] in PROFILE_WRAPPER_OPERATORS:
            raise AssertionError(f"operator_profile_summary.csv: profile wrapper leaked into operator profile: {row}")
        if row_int(row, "occurrences") <= 0:
            raise AssertionError(f"operator_profile_summary.csv: non-positive occurrences: {row}")
        if row_int(row, "query_time_us") <= 0:
            raise AssertionError(f"operator_profile_summary.csv: non-positive query time: {row}")
        if row_int(row, "output_rows") < 0:
            raise AssertionError(f"operator_profile_summary.csv: negative output rows: {row}")
        seen_keys.add((row["query"], row["policy"]))
    if len(expected_keys) != len(expected_queries) * len(policies):
        raise AssertionError("operator_profile_summary.csv: summary query/policy key count mismatch")
    missing = sorted(expected_keys - seen_keys)
    if missing:
        raise AssertionError(f"operator_profile_summary.csv: missing profile rows for query/policies {missing[:5]}")


def verify_ir(trace_dir: Path, summary_rows: list, require_ir: bool) -> None:
    if not require_ir:
        return
    for row in summary_rows:
        events = read_csv(trace_dir / row["events_csv"])
        for event in events:
            if event.get("status") == "compiled" and event.get("execution_mode") == "native":
                if event.get("ir", "") == "":
                    raise AssertionError(
                        f"{row['events_csv']}: compiled {event.get('target')} event {event.get('event_id')} has empty IR"
                    )
                if event.get("target") == "region":
                    region_execution_form = event.get("region_execution_form", "")
                    if region_execution_form not in KNOWN_REGION_EXECUTION_FORMS or region_execution_form == "none":
                        raise AssertionError(
                            f"{row['events_csv']}: compiled region event {event.get('event_id')} has invalid form"
                        )


def verify_source_boundary_features(
    trace_dir: Path,
    summary_rows: list,
    require_lowered_event_features: bool,
    require_ir: bool,
    require_full_tpch_coverage: bool,
) -> None:
    if not require_lowered_event_features:
        return
    required_scan_reason_features = (
        "function=",
        "output_columns=",
        "returned_columns=",
        "column_ids=",
        "native_source_input_columns=",
        "native_source_input_types=",
        "native_source_output_projection_map=",
        "native_source_filter_column_map=",
        "native_source_requires_unfiltered_input=",
        "native_source_filter_prune_required=",
        "native_source_filter_takeover_supported=",
        "projection_pushdown=",
        "projected_columns=",
        "filter_pushdown=",
        "filter_prune=",
        "filter_count=",
        "dynamic_filters=",
        "in_out_function=",
    )
    required_scan_helper_reason_markers = (
        "DuckDB table scan source boundary",
        "DuckDB source boundary",
    )
    required_scan_helper_reason_features = ()
    required_scan_native_runtime_reason_features = ("native table scan source protocol",)
    required_scan_native_reason_features = (
        "native_source_contract<status=ready",
        "blocker=none",
    )
    required_scan_ir_features = (
        "source<kind=duckdb-table-scan",
        "table_scan_protocol<",
        "column_id_bindings=",
        "projection_ids=",
        "native_source_input_columns=",
        "native_source_input_types=",
        "native_source_output_projection_map=",
        "native_source_filter_column_map=",
        "native_source_requires_unfiltered_input=",
        "native_source_filter_prune_required=",
        "native_source_filter_takeover_supported=",
    )
    required_scan_helper_ir_features = ("execution=duckdb-source-boundary",)
    required_scan_native_ir_features = ("execution=native-source",)
    required_hash_join_features = (
        "operator=HASH_JOIN",
        "join_type=",
        "condition_count=",
        "equality_condition_count=",
        "non_equality_condition_count=",
        "null_equal_condition_count=",
        "condition_types=",
        "comparison_ops=",
        "payload_columns=",
        "payload_column_indices=",
        "payload_types=",
        "lhs_output_columns=",
        "lhs_output_column_indices=",
        "lhs_output_types=",
        "rhs_output_columns=",
        "rhs_output_types=",
        "lhs_probe_columns=",
        "lhs_probe_column_indices=",
        "lhs_probe_types=",
        "lhs_output_in_probe=",
        "delim_types=",
        "residual_predicate=",
        "residual_info=",
        "filter_pushdown=",
        "filter_pushdown_condition_count=",
        "filter_pushdown_probe_count=",
        "build_side_has_filter=",
        "source_produces_rows=",
    )
    required_hash_join_ir_features = ("hash_join_protocol<",)
    required_hash_join_native_state_scan_features = (
        "execution=native-source",
        "source_produces_rows=true",
        "native_state_scan_contract_status=ready",
        "native_state_scan_required_capability=hash-join-native-state-scan",
        "native_state_scan_blocker=none",
    )
    required_aggregate_features = (
        "DuckDB hash aggregate native state scan protocol",
        "aggregate_operator_kind=",
        "group_count=",
        "group_types=",
        "aggregate_count=",
        "aggregate_functions=",
        "aggregate_return_types=",
        "aggregate_child_counts=",
        "aggregate_types=",
        "aggregate_filter_count=",
        "aggregate_order_count=",
        "payload_type_count=",
        "payload_types=",
        "grouping_set_count=",
        "grouping_function_count=",
        "radix_table_count=",
        "distinct_aggregate_count=",
        "distinct_table_count=",
        "distinct_child_count=",
        "input_group_type_count=",
        "input_group_types=",
        "non_distinct_filter_count=",
        "distinct_filter_count=",
    )
    required_aggregate_ir_features = ("aggregate_protocol<",)
    table_scan_events = 0
    dynamic_filter_events = 0
    filter_pushdown_events = 0
    hash_aggregate_events = 0
    perfect_hash_aggregate_events = 0
    ungrouped_aggregate_events = 0
    for row in summary_rows:
        if row["policy"] == "off":
            continue
        for event in read_csv(trace_dir / row["events_csv"]):
            if event.get("target", "") != "region":
                continue
            if event.get("phase", "") != "decision":
                continue
            pipeline_shape = event.get("candidate_pipeline_shape", "")
            trace_text = event.get("reason", "") + " " + event.get("ir", "")
            if event.get("status") == "skipped" and not any(
                marker in trace_text
                for marker in (
                    "DuckDB table scan source boundary",
                    "DuckDB hash join native state scan protocol",
                    "DuckDB hash join state scan source does not produce rows",
                    "DuckDB hash aggregate native state scan protocol",
                    "DuckDB aggregate source state protocol missing",
                    "native state scan source protocol",
                )
            ):
                continue
            if "source:source:HASH_JOIN:" in pipeline_shape:
                if "source_produces_rows=false" in trace_text:
                    raise AssertionError(
                        f"{row['events_csv']}: non-producing hash-join source event was not excluded: {event}"
                    )
                hash_join_features = required_hash_join_features
                if require_ir:
                    hash_join_features += required_hash_join_ir_features
                hash_join_features += required_hash_join_native_state_scan_features
                for feature in hash_join_features:
                    if feature not in trace_text:
                        raise AssertionError(
                            f"{row['events_csv']}: hash-join source boundary event {event.get('event_id')} "
                            f"does not expose {feature}: {event}"
                        )
            aggregate_source_markers = {
                "HASH_GROUP_BY": "aggregate_operator_kind=hash",
                "PERFECT_HASH_GROUP_BY": "aggregate_operator_kind=perfect_hash",
                "UNGROUPED_AGGREGATE": "aggregate_operator_kind=ungrouped",
            }
            for operator_name, kind_marker in aggregate_source_markers.items():
                if f"source:source:{operator_name}:" not in pipeline_shape:
                    continue
                if operator_name == "HASH_GROUP_BY":
                    hash_aggregate_events += 1
                if operator_name == "PERFECT_HASH_GROUP_BY":
                    perfect_hash_aggregate_events += 1
                if operator_name == "UNGROUPED_AGGREGATE":
                    ungrouped_aggregate_events += 1
                aggregate_features = tuple(
                    feature
                    for feature in required_aggregate_features
                    if feature != "DuckDB hash aggregate native state scan protocol"
                )
                if operator_name in {"HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY", "UNGROUPED_AGGREGATE"}:
                    if (
                        "native state scan source protocol" not in trace_text
                        and "native_state_scan_contract_status=ready" not in trace_text
                    ):
                        raise AssertionError(
                            f"{row['events_csv']}: aggregate source boundary event {event.get('event_id')} "
                            f"does not expose native state scan runtime or ready protocol: {event}"
                        )
                    if operator_name == "HASH_GROUP_BY" and "hash-aggregate-native-state-scan" not in trace_text:
                        raise AssertionError(
                            f"{row['events_csv']}: hash aggregate source boundary event {event.get('event_id')} "
                            f"does not expose hash aggregate native state scan capability: {event}"
                        )
                aggregate_features += (f"operator={operator_name}", kind_marker)
                if require_ir:
                    aggregate_features += required_aggregate_ir_features
                for feature in aggregate_features:
                    if feature not in trace_text:
                        raise AssertionError(
                            f"{row['events_csv']}: aggregate source boundary event {event.get('event_id')} "
                            f"does not expose {feature}: {event}"
                        )
            if (
                "source:source:TABLE_SCAN:scan" not in pipeline_shape
                and "source:source:TABLE_SCAN:source-native" not in pipeline_shape
            ):
                continue
            table_scan_events += 1
            source_execution = event_source_execution(event)
            is_native_scan = source_execution == "native-source"
            selected_native_scan = event.get("selected_source_execution", "") == "native-source"
            scan_features = required_scan_reason_features
            scan_features += required_scan_native_reason_features if is_native_scan else required_scan_helper_reason_features
            if selected_native_scan:
                scan_features += required_scan_native_runtime_reason_features
            if require_ir:
                scan_features += required_scan_ir_features
                scan_features += required_scan_native_ir_features if is_native_scan else required_scan_helper_ir_features
            if not is_native_scan and not any(marker in trace_text for marker in required_scan_helper_reason_markers):
                raise AssertionError(
                    f"{row['events_csv']}: table-scan source boundary event {event.get('event_id')} "
                    f"does not expose a source boundary marker: {event}"
                )
            for feature in scan_features:
                if feature not in trace_text:
                    raise AssertionError(
                        f"{row['events_csv']}: table-scan source boundary event {event.get('event_id')} "
                        f"does not expose {feature}: {event}"
                    )
            if "dynamic_filters=true" in trace_text:
                dynamic_filter_events += 1
            if "filter_count=1" in trace_text:
                filter_pushdown_events += 1
    if not require_full_tpch_coverage:
        return
    if table_scan_events <= 0:
        raise AssertionError("TPC-H trace did not expose any table-scan source boundary feature events")
    if dynamic_filter_events <= 0:
        raise AssertionError("TPC-H trace did not expose any dynamic-filter table-scan source boundary")
    if filter_pushdown_events <= 0:
        raise AssertionError("TPC-H trace did not expose any filtered table-scan source boundary")
    if hash_aggregate_events <= 0:
        raise AssertionError("TPC-H trace did not expose any hash aggregate source protocol feature events")
    if perfect_hash_aggregate_events <= 0:
        raise AssertionError("TPC-H trace did not expose any perfect hash aggregate source protocol feature events")
    if ungrouped_aggregate_events <= 0:
        raise AssertionError("TPC-H trace did not expose any ungrouped aggregate source protocol feature events")


def verify_expression_fallback_reasons(trace_dir: Path, summary_rows: list, require_full_lowering: bool) -> None:
    if not require_full_lowering:
        return
    expression_fallback_events = 0
    projection_fallback_events = 0
    filter_fallback_events = 0
    for row in summary_rows:
        if row["policy"] == "off":
            continue
        for event in read_csv(trace_dir / row["events_csv"]):
            if event.get("target", "") != "region" or event.get("phase", "") != "decision":
                continue
            trace_text = event.get("reason", "") + " " + event.get("ir", "")
            has_projection_fallback = "core projection expression lowering unsupported" in trace_text
            has_filter_fallback = "core filter expression lowering unsupported" in trace_text
            if not has_projection_fallback and not has_filter_fallback:
                continue
            expression_fallback_events += 1
            projection_fallback_events += 1 if has_projection_fallback else 0
            filter_fallback_events += 1 if has_filter_fallback else 0
            for feature in (
                "core expression lowering unsupported;",
                "reason=",
                "class=",
                "type=",
                "return=",
            ):
                if feature not in trace_text:
                    raise AssertionError(
                        f"{row['events_csv']}: expression fallback event {event.get('event_id')} "
                        f"does not expose {feature}: {event}"
                    )
            if has_projection_fallback and "expression_index=" not in trace_text:
                raise AssertionError(
                    f"{row['events_csv']}: projection fallback event {event.get('event_id')} "
                    f"does not expose expression_index: {event}"
                )
    if expression_fallback_events <= 0:
        raise AssertionError("TPC-H trace did not expose any structured expression fallback reason")
    if projection_fallback_events <= 0:
        raise AssertionError("TPC-H trace did not expose any structured projection fallback reason")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify tpch_trace.py JIT trace artifacts")
    parser.add_argument("trace_dir", type=Path)
    parser.add_argument("--expected-queries", type=int, default=22)
    parser.add_argument("--queries", nargs="+", default=None)
    parser.add_argument("--policies", nargs="+", default=["off", "auto", "force"])
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
    expected_queries = (
        [f"{int(query_id):02d}" for query_id in args.queries]
        if args.queries is not None
        else expected_query_ids(args.expected_queries)
    )
    manifest_configuration = manifest.get("configuration", {})
    if manifest_configuration.get("policies") != args.policies:
        raise AssertionError(
            f"trace_manifest.json: expected policies {args.policies}, found {manifest_configuration.get('policies')}"
        )
    if manifest_configuration.get("queries", []) != expected_queries:
        raise AssertionError(
            f"trace_manifest.json: expected queries {expected_queries}, found {manifest_configuration.get('queries', [])}"
        )

    summary_rows = read_csv(trace_dir / "summary.csv")
    summary_owned_fields = (
        "event_summary_csv",
        "correctness_csv",
        "events_csv",
        "counters_csv",
        "decision_counters_csv",
        "kernel_counters_csv",
        "profile_json",
    )
    for row in summary_rows:
        for field in summary_owned_fields:
            if row[field] not in manifest["artifacts"]:
                raise AssertionError(f"trace_manifest.json: missing summary-owned artifact {row[field]}")
    query_gap_rows = read_csv(trace_dir / "query_gap_summary.csv")
    operator_gap_rows = read_csv(trace_dir / "operator_gap_summary.csv")
    capability_gap_rows = read_csv(trace_dir / "capability_gap_summary.csv")
    capability_priority_rows = read_csv(trace_dir / "capability_priority_summary.csv")
    query_capability_priority_rows = read_csv(trace_dir / "query_capability_priority_summary.csv")
    expression_fallback_rows = read_csv(trace_dir / "expression_fallback_summary.csv")
    source_boundary_rows = read_csv(trace_dir / "source_boundary_summary.csv")
    source_boundary_priority_rows = read_csv(trace_dir / "source_boundary_priority_summary.csv")
    source_fusion_gap_rows = read_csv(trace_dir / "source_fusion_gap_summary.csv")
    fusion_blocker_rows = read_csv(trace_dir / "fusion_blocker_summary.csv")
    region_rows = read_csv(trace_dir / "region_decision_summary.csv")
    stage_rows = read_csv(trace_dir / "stage_pipeline_summary.csv")
    kernel_rows = read_csv(trace_dir / "kernel_runtime_summary.csv")
    admission_efficiency_rows = read_csv(trace_dir / "admission_efficiency_summary.csv")
    admission_proof_gap_rows = read_csv(trace_dir / "admission_proof_gap_summary.csv")
    pipeline_runtime_rows = read_csv(trace_dir / "pipeline_runtime_summary.csv")
    flow_step_rows = read_csv(trace_dir / "flow_step_summary.csv")
    decision_counter_rows = read_csv(trace_dir / "decision_counter_summary.csv")
    operator_profile_rows = read_csv(trace_dir / "operator_profile_summary.csv")

    verify_summary(trace_dir, summary_rows, args.policies, expected_queries)
    verify_required_columns(
        "decision_counter_summary.csv",
        decision_counter_rows,
        DECISION_COUNTER_REQUIRED_COLUMNS,
    )
    event_log_size = int(manifest_configuration.get("event_log_size", 0))
    complete_auto_inventory_trace = bool(manifest_configuration.get("dump_ir", False))
    verify_decision_counter_summary(decision_counter_rows, args.policies, event_log_size, complete_auto_inventory_trace)
    verify_decision_counters_match_summary(summary_rows, decision_counter_rows, event_log_size)
    verify_admission_proof_gaps(admission_proof_gap_rows, decision_counter_rows, summary_rows)
    if event_log_size == 0:
        verify_operator_profile(operator_profile_rows, summary_rows, expected_queries, args.policies)
        print(
            "ok "
            f"summary={len(summary_rows)} "
            f"admission_proof_gap={len(admission_proof_gap_rows)} "
            f"decision_counter={len(decision_counter_rows)} "
            f"profile={len(operator_profile_rows)} "
            "event_log_size=0"
        )
        return 0
    verify_query_gaps(query_gap_rows, expected_queries, args.policies)
    verify_operator_gaps(operator_gap_rows)
    verify_capability_gaps(capability_gap_rows, operator_gap_rows)
    verify_required_columns(
        "capability_priority_summary.csv",
        capability_priority_rows,
        CAPABILITY_PRIORITY_REQUIRED_COLUMNS,
    )
    verify_required_columns(
        "query_capability_priority_summary.csv",
        query_capability_priority_rows,
        QUERY_CAPABILITY_PRIORITY_REQUIRED_COLUMNS,
    )
    verify_capability_priorities(capability_priority_rows, capability_gap_rows, operator_profile_rows, kernel_rows)
    verify_query_capability_priorities(
        query_capability_priority_rows,
        capability_priority_rows,
        operator_profile_rows,
        kernel_rows,
        expected_queries,
    )
    require_lowered_event_features = args.require_ir or bool(manifest_configuration.get("dump_ir")) or (
        "force" in args.policies
    )
    require_full_tpch_lowering = require_lowered_event_features and expected_queries == expected_query_ids(22)
    verify_expression_fallback_summary(expression_fallback_rows, expected_queries, require_full_tpch_lowering)
    verify_required_columns("source_boundary_summary.csv", source_boundary_rows, SOURCE_BOUNDARY_REQUIRED_COLUMNS)
    verify_required_columns(
        "source_boundary_priority_summary.csv",
        source_boundary_priority_rows,
        SOURCE_BOUNDARY_REQUIRED_COLUMNS,
    )
    require_runtime_counters = args.require_runtime or bool(manifest_configuration.get("trace_runtime"))
    verify_source_boundary_summary(
        source_boundary_rows, expected_queries, require_full_tpch_lowering, require_runtime_counters
    )
    verify_source_boundary_priorities(
        source_boundary_priority_rows, source_boundary_rows, operator_profile_rows, require_full_tpch_lowering
    )
    verify_required_columns(
        "source_fusion_gap_summary.csv",
        source_fusion_gap_rows,
        SOURCE_FUSION_GAP_REQUIRED_COLUMNS,
    )
    verify_source_fusion_gaps(
        source_fusion_gap_rows,
        source_boundary_rows,
        operator_profile_rows,
        expected_queries,
        args.policies,
        require_full_tpch_lowering,
    )
    verify_required_columns(
        "fusion_blocker_summary.csv",
        fusion_blocker_rows,
        FUSION_BLOCKER_REQUIRED_COLUMNS,
    )
    verify_fusion_blockers(
        fusion_blocker_rows,
        source_fusion_gap_rows,
        operator_profile_rows,
        expected_queries,
        args.policies,
        require_full_tpch_lowering,
        manifest,
    )
    verify_required_columns("region_decision_summary.csv", region_rows, REGION_DECISION_REQUIRED_COLUMNS)
    verify_required_columns("flow_step_summary.csv", flow_step_rows, FLOW_STEP_REQUIRED_COLUMNS)
    verify_region_decisions(region_rows)
    verify_stage_pipelines(stage_rows, region_rows)
    verify_kernel_runtime(kernel_rows, summary_rows, region_rows, args.require_runtime)
    verify_required_columns(
        "admission_efficiency_summary.csv",
        admission_efficiency_rows,
        ADMISSION_EFFICIENCY_REQUIRED_COLUMNS,
    )
    verify_admission_efficiency(admission_efficiency_rows, kernel_rows, summary_rows, args.policies)
    verify_pipeline_runtime(
        pipeline_runtime_rows,
        stage_rows,
        kernel_rows,
        operator_profile_rows,
        args.require_runtime,
    )
    verify_flow_step_summary(flow_step_rows, summary_rows, manifest, expected_queries, args.policies,
                             complete_auto_inventory_trace)
    verify_operator_profile(operator_profile_rows, summary_rows, expected_queries, args.policies)
    require_ir_text = args.require_ir or bool(manifest_configuration.get("dump_ir"))
    verify_source_boundary_features(
        trace_dir,
        summary_rows,
        require_lowered_event_features,
        require_ir_text,
        require_full_tpch_lowering,
    )
    verify_expression_fallback_reasons(trace_dir, summary_rows, require_full_tpch_lowering)
    verify_ir(trace_dir, summary_rows, args.require_ir)
    for name, rows in (
        ("query_gap_summary.csv", query_gap_rows),
        ("capability_gap_summary.csv", capability_gap_rows),
        ("capability_priority_summary.csv", capability_priority_rows),
        ("query_capability_priority_summary.csv", query_capability_priority_rows),
        ("expression_fallback_summary.csv", expression_fallback_rows),
        ("source_boundary_summary.csv", source_boundary_rows),
        ("source_boundary_priority_summary.csv", source_boundary_priority_rows),
        ("source_fusion_gap_summary.csv", source_fusion_gap_rows),
        ("fusion_blocker_summary.csv", fusion_blocker_rows),
        ("region_decision_summary.csv", region_rows),
        ("stage_pipeline_summary.csv", stage_rows),
        ("kernel_runtime_summary.csv", kernel_rows),
        ("admission_efficiency_summary.csv", admission_efficiency_rows),
        ("pipeline_runtime_summary.csv", pipeline_runtime_rows),
        ("flow_step_summary.csv", flow_step_rows),
    ):
        assert_no_missing_pipeline_shape(name, rows)

    print(
        "ok summary={summary} query_gap={query_gap} operator_gap={operator_gap} "
        "capability_gap={capability_gap} capability_priority={capability_priority} "
        "query_capability_priority={query_capability_priority} "
        "expression_fallback={expression_fallback} "
        "source_boundary={source_boundary} "
        "source_boundary_priority={source_boundary_priority} "
        "source_fusion_gap={source_fusion_gap} "
        "fusion_blocker={fusion_blocker} "
        "region={region} stage={stage} kernels={kernels} admission_efficiency={admission_efficiency} "
        "admission_proof_gap={admission_proof_gap} "
        "pipeline_runtime={pipeline_runtime} "
        "flow_steps={flow_steps} decision_counter={decision_counter} profile={profile}".format(
            summary=len(summary_rows),
            query_gap=len(query_gap_rows),
            operator_gap=len(operator_gap_rows),
            capability_gap=len(capability_gap_rows),
            capability_priority=len(capability_priority_rows),
            query_capability_priority=len(query_capability_priority_rows),
            expression_fallback=len(expression_fallback_rows),
            source_boundary=len(source_boundary_rows),
            source_boundary_priority=len(source_boundary_priority_rows),
            source_fusion_gap=len(source_fusion_gap_rows),
            fusion_blocker=len(fusion_blocker_rows),
            region=len(region_rows),
            stage=len(stage_rows),
            kernels=len(kernel_rows),
            admission_efficiency=len(admission_efficiency_rows),
            admission_proof_gap=len(admission_proof_gap_rows),
            pipeline_runtime=len(pipeline_runtime_rows),
            flow_steps=len(flow_step_rows),
            decision_counter=len(decision_counter_rows),
            profile=len(operator_profile_rows),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
