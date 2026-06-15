#!/usr/bin/env python3
#
# Trace DuckDB JIT behavior across TPC-H queries.
#
# This script is intentionally outside the benchmark runner: it combines
# correctness checks, EXPLAIN ANALYZE timing, compile/admission events, and
# runtime counters in one reproducible trace artifact.

import argparse
import collections
import csv
import datetime
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import (
    TRACE_MANIFEST_NAME,
    default_trace_output_directory,
    prepare_trace_output_directory,
    write_trace_manifest,
)

from tpch_schema import (
    AGGREGATE_OPERATORS,
    ADMISSION_EFFICIENCY_SUMMARY_FIELDS,
    ADMISSION_PROOF_GAP_SUMMARY_FIELDS,
    CANDIDATE_TRAIT_FIELDS,
    CAPABILITY_GAP_SUMMARY_FIELDS,
    CAPABILITY_PRIORITY_SUMMARY_FIELDS,
    CAPABILITY_RUNTIME_FIELDS,
    CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES,
    DECISION_COUNTER_SUMMARY_FIELDS,
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    EXPRESSION_FALLBACK_SUMMARY_FIELDS,
    FLOW_STEP_SUMMARY_FIELDS,
    FUSION_BLOCKER_SUMMARY_FIELDS,
    JOIN_OPERATORS,
    KERNEL_RUNTIME_SUMMARY_FIELDS,
    MATERIALIZATION_OPERATORS,
    OPERATOR_GAP_SUMMARY_FIELDS,
    OPERATOR_PROFILE_SUMMARY_FIELDS,
    PIPELINE_RUNTIME_SUMMARY_FIELDS,
    PROFILE_HEAVY_OPERATORS,
    PROFILE_WRAPPER_OPERATORS,
    QUERY_CAPABILITY_PRIORITY_SUMMARY_FIELDS,
    QUERY_GAP_SUMMARY_FIELDS,
    REGION_DECISION_STATUSES,
    REGION_DECISION_SUMMARY_FIELDS,
    SORT_OPERATORS,
    SOURCE_BOUNDARY_PRIORITY_SUMMARY_FIELDS,
    SOURCE_BOUNDARY_SUMMARY_FIELDS,
    SOURCE_FUSION_GAP_SUMMARY_FIELDS,
    SOURCE_NATIVE_RUNTIME_FIELDS,
    STAGE_PIPELINE_SUMMARY_FIELDS,
    STAGE_FIELDS,
    TPCH_TABLES,
    TRACE_WRAPPER_OPERATORS,
    configure_csv_field_size_limit,
)

configure_csv_field_size_limit()

TOTAL_TIME_RE = re.compile(r"Total Time:\s*([0-9.]+)s")
EXPRESSION_FALLBACK_MARKER = "core expression lowering unsupported;"
EXPRESSION_FALLBACK_FIELD_RE = re.compile(r"(reason|class|type|return|function)=([^;\)]+)")
SOURCE_BOUNDARY_MARKERS = (
    ("table_scan_generated_source_filter", "generated source-prefix table scan filters"),
    ("table_scan_native_source", "native table scan source protocol"),
    ("table_scan_source_boundary", "DuckDB table scan source boundary"),
    ("duckdb_scan_source_boundary", "DuckDB scan source boundary"),
    ("stateful_native_state_scan", "native state scan source protocol"),
    ("stateful_native_state_scan", "DuckDB hash join native state scan protocol"),
    ("stateful_native_state_scan", "DuckDB hash aggregate native state scan protocol"),
    ("stateful_native_source", "native stateful source protocol"),
    ("stateful_native_source", "DuckDB column data native source protocol"),
    ("stateful_source_fallback", "DuckDB hash join state scan source does not produce rows"),
    ("stateful_source_fallback", "DuckDB aggregate source state protocol missing"),
    ("stateful_source_fallback", "DuckDB stateful source operator fallback boundary"),
    ("duckdb_source_boundary", "DuckDB source boundary"),
)
SOURCE_BOUNDARY_FIELD_NAMES = (
    r"function|operator|fields|output_columns|returned_columns|column_ids|column_id_bindings|projection_ids|"
    r"projected_columns|projection_pushdown|"
    r"source_prefix_input_columns|source_prefix_input_types|source_prefix_output_projection_map|"
    r"source_prefix_filter_column_map|source_prefix_requires_unfiltered_input|source_prefix_filter_prune_required|"
    r"source_prefix_filter_split_supported|"
    r"filter_pushdown|filter_prune|filter_count|dynamic_filters|in_out_function|join_type|condition_count|"
    r"equality_condition_count|non_equality_condition_count|null_equal_condition_count|condition_types|"
    r"comparison_ops|payload_columns|payload_column_indices|payload_types|lhs_output_columns|lhs_output_column_indices|lhs_output_types|"
    r"rhs_output_columns|rhs_output_types|lhs_probe_columns|lhs_probe_column_indices|lhs_probe_types|"
    r"lhs_output_in_probe|delim_types|correlated_mark_counts_required|residual_predicate|"
    r"residual_info|filter_pushdown_condition_count|filter_pushdown_probe_count|build_side_has_filter|"
    r"source_produces_rows|"
    r"regular_hash_table_layout_ready|native_probe_shape_ready|native_probe_shape_blocker|"
    r"native_probe_output_mode|"
    r"build_append_shape_ready|build_append_shape_blocker|hash_join_layout_column_count|hash_join_layout_offsets|"
    r"hash_join_tuple_size|hash_join_entry_size|hash_join_pointer_offset|hash_join_hash_column_index|"
    r"hash_join_found_match_column_present|hash_join_found_match_column_index|hash_join_native_protocol_blocker|"
    r"aggregate_operator_kind|group_count|group_types|aggregate_count|aggregate_functions|"
    r"aggregate_return_types|aggregate_child_counts|aggregate_types|aggregate_filter_count|aggregate_order_count|"
    r"payload_type_count|grouping_set_count|grouping_function_count|radix_table_count|distinct_aggregate_count|"
    r"distinct_table_count|distinct_child_count|input_group_type_count|input_group_types|non_distinct_filter_count|"
    r"distinct_filter_count|native_state_scan_contract_status|native_state_scan_required_capability|"
    r"native_state_scan_protocol|native_state_scan_blocker|"
    r"native_grouped_state_contract_status|native_grouped_state_required_capability|"
    r"native_grouped_state_protocol|native_grouped_state_blocker|"
    r"native_hash_join_probe_contract_status|native_hash_join_probe_required_capability|"
    r"native_hash_join_probe_protocol|native_hash_join_probe_blocker|"
    r"native_hash_join_build_contract_status|native_hash_join_build_required_capability|"
    r"native_hash_join_build_protocol|native_hash_join_build_blocker|"
    r"native_hash_aggregate_lookup_contract_status|native_hash_aggregate_lookup_required_capability|"
    r"native_hash_aggregate_lookup_protocol|native_hash_aggregate_lookup_blocker|"
    r"perfect_required_bits_count|perfect_required_bits_total|"
    r"perfect_required_bits|"
    r"perfect_group_minima_count|grouped_state_layout_ready|grouped_state_offsets|grouped_state_payload_sizes"
)
SOURCE_BOUNDARY_FIELD_RE = re.compile(
    r"(" + SOURCE_BOUNDARY_FIELD_NAMES + r")=(.*?)(?=(?:[;,|](?:" + SOURCE_BOUNDARY_FIELD_NAMES + r")=)|[>);]|$)"
)
SOURCE_BOUNDARY_EXECUTION_RE = re.compile(r"source<kind=[^,>]+,execution=([^,>]+)")
REGION_LOWERING_COUNT_RE = re.compile(
    r"region-lowering:native=(\d+),fallback=(\d+),execution-form=([^;]+)"
)
FUSION_BLOCKER_RE = re.compile(r"(?:^|;)fusion-blocker:([^;]+)")
NATIVE_SOURCE_CONTRACT_RE = re.compile(
    r"native_source_contract<status=([^,>]+),required_capability=([^,>]+),protocol=([^,>]+),blocker=([^>]+)>"
)
class TraceConfigurationError(RuntimeError):
    pass


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def read_query(root: Path, query_id: str) -> str:
    query_path = root / "extension" / "tpch" / "dbgen" / "queries" / f"q{query_id}.sql"
    query = query_path.read_text(encoding="utf-8").strip()
    while query.endswith(";"):
        query = query[:-1].strip()
    if not query:
        raise RuntimeError(f"empty query file: {query_path}")
    return query


def run_duckdb(duckdb: Path, db_path: Path, sql: str, label: str) -> subprocess.CompletedProcess:
    script = f".bail on\n{sql}\n"
    result = subprocess.run(
        [str(duckdb), str(db_path)],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"DuckDB command failed during {label} with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def run_copy_query(duckdb: Path, db_path: Path, select_sql: str, output_path: Path, label: str) -> None:
    run_duckdb(
        duckdb,
        db_path,
        f"COPY ({select_sql}) TO {sql_quote(output_path)} (HEADER, DELIMITER ',');",
        label,
    )


def copy_statement(select_sql: str, output_path: Path) -> str:
    return f"COPY ({select_sql}) TO {sql_quote(output_path)} (HEADER, DELIMITER ',');"


def read_single_csv_row(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise RuntimeError(f"expected one row in {path}, found {len(rows)}")
    return rows[0]


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0
    return int(value)


def read_region_decision_totals(path: Path) -> dict:
    totals = {
        "compiled_regions": 0,
        "skipped_regions": 0,
        "unsupported_regions": 0,
    }
    if not path.exists():
        return totals
    status_to_field = {
        "compiled": "compiled_regions",
        "skipped": "skipped_regions",
        "unsupported": "unsupported_regions",
    }
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("target") != "region":
                continue
            field = status_to_field.get(row.get("status", ""))
            if field is None:
                continue
            totals[field] += row_int(row, "count")
    return totals


def row_float(row: dict, field: str) -> float:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0.0
    return float(value)


def row_correctness_diff(row: dict) -> int:
    return row_int(row, "result_minus_baseline") + row_int(row, "baseline_minus_result")


def row_stage_time_us(row: dict) -> int:
    return sum(row_int(row, field) for field in STAGE_FIELDS)


def seconds_to_us(value) -> int:
    if value == "" or value is None:
        return 0
    return int(round(float(value) * 1000000))


def format_percent(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0000"
    return f"{(float(numerator) * 100.0 / float(denominator)):.4f}"


def format_ratio(value: float) -> str:
    return f"{value:.6f}"


def join_unique(values) -> str:
    return ";".join(sorted(value for value in values if value))


def compact_reason(reason: str, limit: int = 360) -> str:
    if len(reason) <= limit:
        return reason
    return reason[:limit] + "...[truncated]"


def read_profile_json(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"missing profiler output: {path}")
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def iter_profile_nodes(profile: dict):
    def walk(node: dict, depth: int):
        yield node, depth
        for child in node.get("children", []) or []:
            yield from walk(child, depth + 1)

    for root in profile.get("operator", []) or []:
        yield from walk(root, 0)


def profile_query_time_us(profile: dict) -> int:
    return seconds_to_us((profile.get("query") or {}).get("total_time", 0))


def profile_cpu_time_us(profile: dict) -> int:
    return seconds_to_us((profile.get("query") or {}).get("cpu_time", 0))


def profile_operator_time_us(profile: dict) -> int:
    total = 0
    for node, _ in iter_profile_nodes(profile):
        if str(node.get("type", "")) in PROFILE_WRAPPER_OPERATORS:
            continue
        total += seconds_to_us(node.get("timing", 0))
    return total


def profile_operator_count(profile: dict) -> int:
    result = 0
    for node, _ in iter_profile_nodes(profile):
        if str(node.get("type", "")) not in PROFILE_WRAPPER_OPERATORS:
            result += 1
    return result


def format_profile_extra_info(extra_info: dict) -> str:
    if not extra_info:
        return ""
    return json.dumps(extra_info, sort_keys=True, separators=(",", ":"))


def truncate_text(value: str, limit: int = 120) -> str:
    value = " ".join(str(value).split())
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def fusion_blocker_example_text(reason: str, blocker: str) -> str:
    marker = f"fusion-blocker:{blocker}"
    marker_index = reason.find(marker)
    if marker_index < 0:
        return truncate_text(reason, 512)
    prefix_parts = [
        part
        for part in reason[:marker_index].split(";")
        if part.startswith("region-lowering:") or part.startswith("selected-source-execution=")
    ]
    next_marker_index = reason.find(";fusion-blocker:", marker_index + len(marker))
    segment_end = next_marker_index if next_marker_index >= 0 else len(reason)
    segment = reason[marker_index:segment_end]
    return truncate_text(";".join([*prefix_parts, segment]), 512)


def trace_summary_reason(value: str) -> str:
    text = str(value)
    if "jit_policy=auto" in text:
        return truncate_text(text, 1024)
    return truncate_text(text)


def compact_pipeline_shape(pipeline_shape: str) -> str:
    operators = []
    for node in parse_pipeline_shape(pipeline_shape):
        operator_name = node["operator_name"]
        if not operators or operators[-1] != operator_name:
            operators.append(operator_name)
    return " -> ".join(operators) if operators else str(pipeline_shape)


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


def attribution_pipeline_shape(region_entry: dict) -> str:
    if region_entry.get("status") == "unsupported":
        return region_entry.get("candidate_context_pipeline_shape", "") or region_entry.get("candidate_pipeline_shape", "")
    return region_entry.get("candidate_pipeline_shape", "")


def iter_expression_fallback_details(trace_text: str):
    matches = list(re.finditer(re.escape(EXPRESSION_FALLBACK_MARKER), trace_text))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(trace_text)
        segment = trace_text[match.start() : end]
        fields = {}
        for field_match in EXPRESSION_FALLBACK_FIELD_RE.finditer(segment):
            fields.setdefault(field_match.group(1), field_match.group(2).strip())
        reason = fields.get("reason", "")
        if not reason:
            continue
        yield {
            "reason": reason,
            "expression_class": fields.get("class", ""),
            "expression_type": fields.get("type", ""),
            "function_name": fields.get("function", ""),
            "return_type": fields.get("return", ""),
        }


def expression_fallback_example_text(event_reason: str, trace_text: str) -> str:
    marker_index = event_reason.find(EXPRESSION_FALLBACK_MARKER)
    if marker_index >= 0:
        return truncate_text(event_reason[marker_index:], 240)
    marker_index = trace_text.find(EXPRESSION_FALLBACK_MARKER)
    if marker_index >= 0:
        return truncate_text(trace_text[marker_index:], 240)
    return truncate_text(event_reason, 240)


def source_boundary_segment(trace_text: str, marker_index: int) -> str:
    start = marker_index
    source_contract_start = trace_text.rfind("(source<", 0, marker_index)
    if source_contract_start >= 0:
        prior_boundary = max(
            trace_text.rfind(";source:", 0, marker_index),
            trace_text.rfind(";op", 0, marker_index),
            trace_text.rfind(";sink:", 0, marker_index),
        )
        if source_contract_start > prior_boundary:
            start = source_contract_start + 1
    end = len(trace_text)
    for token in (";execution:", ";source:", ";op0:", ";op1:", ";op2:", ";sink:", " duckdb.region"):
        token_index = trace_text.find(token, marker_index + 1)
        if token_index >= 0:
            end = min(end, token_index)
    return trace_text[start:end]


def clean_source_boundary_field_value(value: str) -> str:
    value = value.strip()
    while value.endswith(")") and value.count("(") < value.count(")"):
        value = value[:-1].strip()
    while value.endswith("]") and value.count("[") < value.count("]"):
        value = value[:-1].strip()
    return value


def remove_source_boundary_nested_block(segment: str, token: str, close_char: str) -> str:
    result = []
    offset = 0
    open_char = token[-1]
    while True:
        token_index = segment.find(token, offset)
        if token_index < 0:
            result.append(segment[offset:])
            return "".join(result)
        result.append(segment[offset:token_index])
        depth = 0
        index = token_index + len(token) - 1
        while index < len(segment):
            if segment[index] == open_char:
                depth += 1
            elif segment[index] == close_char:
                depth -= 1
                if depth == 0:
                    index += 1
                    break
            index += 1
        offset = index


def normalize_source_boundary_segment_for_fields(segment: str) -> str:
    # The source/sink fields list is the machine-readable contract. Nested IR payloads
    # reuse generic names such as function= and group_count= for human-readable detail;
    # remove them before mapping boundary columns.
    for token, close_char in (
        (",hash_join_protocol<", ">"),
        (",hash_join_keys=[", "]"),
        (",aggregate_protocol<", ">"),
        (",aggregates=[", "]"),
        (",groups=[", "]"),
    ):
        segment = remove_source_boundary_nested_block(segment, token, close_char)
    return segment


def extract_source_boundary_details(trace_text: str) -> Optional[dict]:
    marker_index = -1
    source_boundary_kind = ""
    source_marker = ""
    for candidate_kind, candidate_marker in SOURCE_BOUNDARY_MARKERS:
        candidate_index = trace_text.find(candidate_marker)
        if candidate_index < 0:
            continue
        marker_index = candidate_index
        source_boundary_kind = candidate_kind
        source_marker = candidate_marker
        break
    if marker_index < 0:
        return None

    fields = {}
    raw_segment = source_boundary_segment(trace_text, marker_index)
    execution_match = SOURCE_BOUNDARY_EXECUTION_RE.search(raw_segment)
    segment = normalize_source_boundary_segment_for_fields(raw_segment)
    for field_match in SOURCE_BOUNDARY_FIELD_RE.finditer(segment):
        fields[field_match.group(1)] = clean_source_boundary_field_value(field_match.group(2))
    return {
        "source_boundary_kind": source_boundary_kind,
        "source_marker": source_marker,
        "source_execution": execution_match.group(1) if execution_match else "",
        "scan_function": fields.get("function", ""),
        "boundary_operator": fields.get("operator", ""),
        "output_columns": fields.get("output_columns", ""),
        "returned_columns": fields.get("returned_columns", ""),
        "column_ids": fields.get("column_ids", ""),
        "source_prefix_input_columns": fields.get("source_prefix_input_columns", ""),
        "source_prefix_input_types": fields.get("source_prefix_input_types", ""),
        "source_prefix_output_projection_map": fields.get("source_prefix_output_projection_map", ""),
        "source_prefix_filter_column_map": fields.get("source_prefix_filter_column_map", ""),
        "source_prefix_requires_unfiltered_input": fields.get("source_prefix_requires_unfiltered_input", ""),
        "source_prefix_filter_prune_required": fields.get("source_prefix_filter_prune_required", ""),
        "source_prefix_filter_split_supported": fields.get("source_prefix_filter_split_supported", ""),
        "projected_columns": fields.get("projected_columns", ""),
        "projection_pushdown": fields.get("projection_pushdown", ""),
        "filter_pushdown": fields.get("filter_pushdown", ""),
        "filter_prune": fields.get("filter_prune", ""),
        "filter_count": fields.get("filter_count", ""),
        "dynamic_filters": fields.get("dynamic_filters", ""),
        "in_out_function": fields.get("in_out_function", ""),
        "join_type": fields.get("join_type", ""),
        "condition_count": fields.get("condition_count", ""),
        "equality_condition_count": fields.get("equality_condition_count", ""),
        "non_equality_condition_count": fields.get("non_equality_condition_count", ""),
        "null_equal_condition_count": fields.get("null_equal_condition_count", ""),
        "condition_types": fields.get("condition_types", ""),
        "comparison_ops": fields.get("comparison_ops", ""),
        "payload_columns": fields.get("payload_columns", ""),
        "payload_column_indices": fields.get("payload_column_indices", ""),
        "payload_types": fields.get("payload_types", ""),
        "lhs_output_columns": fields.get("lhs_output_columns", ""),
        "lhs_output_column_indices": fields.get("lhs_output_column_indices", ""),
        "lhs_output_types": fields.get("lhs_output_types", ""),
        "rhs_output_columns": fields.get("rhs_output_columns", ""),
        "rhs_output_types": fields.get("rhs_output_types", ""),
        "lhs_probe_columns": fields.get("lhs_probe_columns", ""),
        "lhs_probe_column_indices": fields.get("lhs_probe_column_indices", ""),
        "lhs_probe_types": fields.get("lhs_probe_types", ""),
        "lhs_output_in_probe": fields.get("lhs_output_in_probe", ""),
        "delim_types": fields.get("delim_types", ""),
        "correlated_mark_counts_required": fields.get("correlated_mark_counts_required", ""),
        "residual_predicate": fields.get("residual_predicate", ""),
        "residual_info": fields.get("residual_info", ""),
        "filter_pushdown_condition_count": fields.get("filter_pushdown_condition_count", ""),
        "filter_pushdown_probe_count": fields.get("filter_pushdown_probe_count", ""),
        "build_side_has_filter": fields.get("build_side_has_filter", ""),
        "source_produces_rows": fields.get("source_produces_rows", ""),
        "regular_hash_table_layout_ready": fields.get("regular_hash_table_layout_ready", ""),
        "native_probe_shape_ready": fields.get("native_probe_shape_ready", ""),
        "native_probe_shape_blocker": fields.get("native_probe_shape_blocker", ""),
        "native_probe_output_mode": fields.get("native_probe_output_mode", ""),
        "build_append_shape_ready": fields.get("build_append_shape_ready", ""),
        "build_append_shape_blocker": fields.get("build_append_shape_blocker", ""),
        "hash_join_layout_column_count": fields.get("hash_join_layout_column_count", ""),
        "hash_join_layout_offsets": fields.get("hash_join_layout_offsets", ""),
        "hash_join_tuple_size": fields.get("hash_join_tuple_size", ""),
        "hash_join_entry_size": fields.get("hash_join_entry_size", ""),
        "hash_join_pointer_offset": fields.get("hash_join_pointer_offset", ""),
        "hash_join_hash_column_index": fields.get("hash_join_hash_column_index", ""),
        "hash_join_found_match_column_present": fields.get("hash_join_found_match_column_present", ""),
        "hash_join_found_match_column_index": fields.get("hash_join_found_match_column_index", ""),
        "hash_join_native_protocol_blocker": fields.get("hash_join_native_protocol_blocker", ""),
        "aggregate_operator_kind": fields.get("aggregate_operator_kind", ""),
        "group_count": fields.get("group_count", ""),
        "group_types": fields.get("group_types", ""),
        "aggregate_count": fields.get("aggregate_count", ""),
        "aggregate_functions": fields.get("aggregate_functions", ""),
        "aggregate_return_types": fields.get("aggregate_return_types", ""),
        "aggregate_child_counts": fields.get("aggregate_child_counts", ""),
        "aggregate_types": fields.get("aggregate_types", ""),
        "aggregate_filter_count": fields.get("aggregate_filter_count", ""),
        "aggregate_order_count": fields.get("aggregate_order_count", ""),
        "payload_type_count": fields.get("payload_type_count", ""),
        "grouping_set_count": fields.get("grouping_set_count", ""),
        "grouping_function_count": fields.get("grouping_function_count", ""),
        "radix_table_count": fields.get("radix_table_count", ""),
        "distinct_aggregate_count": fields.get("distinct_aggregate_count", ""),
        "distinct_table_count": fields.get("distinct_table_count", ""),
        "distinct_child_count": fields.get("distinct_child_count", ""),
        "input_group_type_count": fields.get("input_group_type_count", ""),
        "input_group_types": fields.get("input_group_types", ""),
        "non_distinct_filter_count": fields.get("non_distinct_filter_count", ""),
        "distinct_filter_count": fields.get("distinct_filter_count", ""),
        "native_state_scan_contract_status": fields.get("native_state_scan_contract_status", ""),
        "native_state_scan_required_capability": fields.get("native_state_scan_required_capability", ""),
        "native_state_scan_protocol": fields.get("native_state_scan_protocol", ""),
        "native_state_scan_blocker": fields.get("native_state_scan_blocker", ""),
        "native_grouped_state_contract_status": fields.get("native_grouped_state_contract_status", ""),
        "native_grouped_state_required_capability": fields.get("native_grouped_state_required_capability", ""),
        "native_grouped_state_protocol": fields.get("native_grouped_state_protocol", ""),
        "native_grouped_state_blocker": fields.get("native_grouped_state_blocker", ""),
        "native_hash_join_probe_contract_status": fields.get("native_hash_join_probe_contract_status", ""),
        "native_hash_join_probe_required_capability": fields.get("native_hash_join_probe_required_capability", ""),
        "native_hash_join_probe_protocol": fields.get("native_hash_join_probe_protocol", ""),
        "native_hash_join_probe_blocker": fields.get("native_hash_join_probe_blocker", ""),
        "native_hash_join_build_contract_status": fields.get("native_hash_join_build_contract_status", ""),
        "native_hash_join_build_required_capability": fields.get("native_hash_join_build_required_capability", ""),
        "native_hash_join_build_protocol": fields.get("native_hash_join_build_protocol", ""),
        "native_hash_join_build_blocker": fields.get("native_hash_join_build_blocker", ""),
        "native_hash_aggregate_lookup_contract_status": fields.get("native_hash_aggregate_lookup_contract_status", ""),
        "native_hash_aggregate_lookup_required_capability": fields.get(
            "native_hash_aggregate_lookup_required_capability", ""
        ),
        "native_hash_aggregate_lookup_protocol": fields.get("native_hash_aggregate_lookup_protocol", ""),
        "native_hash_aggregate_lookup_blocker": fields.get("native_hash_aggregate_lookup_blocker", ""),
        "perfect_required_bits_count": fields.get("perfect_required_bits_count", ""),
        "perfect_required_bits_total": fields.get("perfect_required_bits_total", ""),
        "perfect_required_bits": fields.get("perfect_required_bits", ""),
        "perfect_group_minima_count": fields.get("perfect_group_minima_count", ""),
        "grouped_state_layout_ready": fields.get("grouped_state_layout_ready", ""),
        "grouped_state_offsets": fields.get("grouped_state_offsets", ""),
        "grouped_state_payload_sizes": fields.get("grouped_state_payload_sizes", ""),
    }


def source_boundary_example_text(event_reason: str, trace_text: str, source_marker: str) -> str:
    marker_index = event_reason.find(source_marker)
    if marker_index >= 0:
        return truncate_text(event_reason[marker_index:], 260)
    marker_index = trace_text.find(source_marker)
    if marker_index >= 0:
        segment = source_boundary_segment(trace_text, marker_index)
        segment_marker_index = segment.find(source_marker)
        if segment_marker_index > 140:
            segment = segment[:140] + "...|" + segment[segment_marker_index:]
        return truncate_text(segment, 260)
    return truncate_text(event_reason, 260)


def region_lowering_native_count(trace_text: str) -> int:
    match = REGION_LOWERING_COUNT_RE.search(trace_text)
    if not match:
        return 0
    return int(match.group(1))


def extract_native_source_contract(trace_text: str) -> dict:
    match = NATIVE_SOURCE_CONTRACT_RE.search(trace_text)
    if not match:
        return {
            "native_source_status": "",
            "native_source_required_capability": "",
            "native_source_protocol": "",
            "native_source_blocker": "",
        }
    return {
        "native_source_status": match.group(1),
        "native_source_required_capability": match.group(2),
        "native_source_protocol": match.group(3),
        "native_source_blocker": match.group(4),
    }


def is_source_fusion_gap_event(event: dict, trace_text: str) -> bool:
    if event.get("candidate_scope", "") not in ("source_pipeline", "full_pipeline"):
        return False
    if event_source_execution(event) == "native-source":
        return False
    if "source-fusion-gap:requires-native-source" in trace_text:
        return True
    if "source-pushed filters require source-prefix filter split" in trace_text:
        return True
    if event.get("region_execution_form", "") != "none":
        return False
    return region_lowering_native_count(trace_text) > 0


def source_fusion_gap_kind(trace_text: str) -> str:
    return "requires_native_source"


def event_source_execution(event: dict) -> str:
    selected = event.get("selected_source_execution", "")
    if selected and selected != "none":
        return selected
    return event.get("candidate_source_execution", "")


def source_boundary_execution(event: dict, details: dict) -> str:
    execution = details.get("source_execution", "")
    if execution and execution != "none":
        return execution
    return event_source_execution(event)


def source_fusion_gap_example_text(event_reason: str, trace_text: str, source_marker: str) -> str:
    blockers = (
        "fusion-blocker:source-fusion-gap:requires-native-source",
    )
    for blocker in blockers:
        marker_index = event_reason.find(blocker)
        if marker_index >= 0:
            return truncate_text(event_reason[marker_index:], 260)
        marker_index = trace_text.find(blocker)
        if marker_index >= 0:
            return truncate_text(source_boundary_segment(trace_text, marker_index), 260)
    source_filter_gap = "source-pushed filters require source-prefix filter split"
    marker_index = event_reason.find(source_filter_gap)
    if marker_index >= 0:
        return truncate_text(event_reason[marker_index:], 260)
    marker_index = trace_text.find(source_filter_gap)
    if marker_index >= 0:
        return truncate_text(source_boundary_segment(trace_text, marker_index), 260)
    return source_boundary_example_text(event_reason, trace_text, source_marker)


def iter_source_boundary_event_entries(out_dir: Path, rows: list):
    for summary_row in rows:
        policy = summary_row["policy"]
        if policy == "off":
            continue
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("phase") not in ("decision", "compile"):
                    continue
                event_reason = event.get("reason", "")
                event_ir = event.get("ir", "")
                trace_text = event_reason
                details = extract_source_boundary_details(trace_text)
                if not details:
                    trace_text = event_reason + " " + event_ir
                    details = extract_source_boundary_details(trace_text)
                if not details:
                    continue
                pipeline_shape = attribution_pipeline_shape(
                    {
                        "status": event.get("status", ""),
                        "candidate_pipeline_shape": event.get("candidate_pipeline_shape", ""),
                        "candidate_context_pipeline_shape": event.get("candidate_context_pipeline_shape", ""),
                    }
                )
                if is_trace_wrapper_pipeline_shape(pipeline_shape):
                    continue
                for source_node in parse_pipeline_shape(pipeline_shape):
                    if source_node["node_kind"] != "source":
                        continue
                    yield {
                        "query": summary_row["query"],
                        "policy": policy,
                        "event": event,
                        "trace_text": trace_text,
                        "details": details,
                        "source_execution": source_boundary_execution(event, details),
                        "source_node": source_node,
                        "pipeline_shape": pipeline_shape,
                    }


def source_boundary_key(policy: str, event: dict, details: dict, source_node: dict) -> tuple:
    return (
        policy,
        event.get("status", ""),
        event.get("execution_mode", ""),
        event.get("region_execution_form", ""),
        details["source_boundary_kind"],
        source_boundary_execution(event, details),
        source_node["operator_name"],
        details["scan_function"],
        details["boundary_operator"],
        details["output_columns"],
        details["returned_columns"],
        details["column_ids"],
        details["source_prefix_input_columns"],
        details["source_prefix_input_types"],
        details["source_prefix_output_projection_map"],
        details["source_prefix_filter_column_map"],
        details["source_prefix_requires_unfiltered_input"],
        details["source_prefix_filter_prune_required"],
        details["source_prefix_filter_split_supported"],
        details["projected_columns"],
        details["projection_pushdown"],
        details["filter_pushdown"],
        details["filter_prune"],
        details["filter_count"],
        details["dynamic_filters"],
        details["in_out_function"],
        details["join_type"],
        details["condition_count"],
        details["equality_condition_count"],
        details["non_equality_condition_count"],
        details["null_equal_condition_count"],
        details["condition_types"],
        details["comparison_ops"],
        details["payload_columns"],
        details["payload_column_indices"],
        details["payload_types"],
        details["lhs_output_columns"],
        details["lhs_output_column_indices"],
        details["lhs_output_types"],
        details["rhs_output_columns"],
        details["rhs_output_types"],
        details["lhs_probe_columns"],
        details["lhs_probe_column_indices"],
        details["lhs_probe_types"],
        details["lhs_output_in_probe"],
        details["delim_types"],
        details["correlated_mark_counts_required"],
        details["residual_predicate"],
        details["residual_info"],
        details["filter_pushdown_condition_count"],
        details["filter_pushdown_probe_count"],
        details["build_side_has_filter"],
        details["source_produces_rows"],
        details["regular_hash_table_layout_ready"],
        details["native_probe_shape_ready"],
        details["native_probe_shape_blocker"],
        details["native_probe_output_mode"],
        details["build_append_shape_ready"],
        details["build_append_shape_blocker"],
        details["hash_join_layout_column_count"],
        details["hash_join_layout_offsets"],
        details["hash_join_tuple_size"],
        details["hash_join_entry_size"],
        details["hash_join_pointer_offset"],
        details["hash_join_hash_column_index"],
        details["hash_join_found_match_column_present"],
        details["hash_join_found_match_column_index"],
        details["hash_join_native_protocol_blocker"],
        details["aggregate_operator_kind"],
        details["group_count"],
        details["group_types"],
        details["aggregate_count"],
        details["aggregate_functions"],
        details["aggregate_return_types"],
        details["aggregate_child_counts"],
        details["aggregate_types"],
        details["aggregate_filter_count"],
        details["aggregate_order_count"],
        details["payload_type_count"],
        details["grouping_set_count"],
        details["grouping_function_count"],
        details["radix_table_count"],
        details["distinct_aggregate_count"],
        details["distinct_table_count"],
        details["distinct_child_count"],
        details["input_group_type_count"],
        details["input_group_types"],
        details["non_distinct_filter_count"],
        details["distinct_filter_count"],
        details["native_state_scan_contract_status"],
        details["native_state_scan_required_capability"],
        details["native_state_scan_protocol"],
        details["native_state_scan_blocker"],
        details["native_grouped_state_contract_status"],
        details["native_grouped_state_required_capability"],
        details["native_grouped_state_protocol"],
        details["native_grouped_state_blocker"],
        details["native_hash_join_probe_contract_status"],
        details["native_hash_join_probe_required_capability"],
        details["native_hash_join_probe_protocol"],
        details["native_hash_join_probe_blocker"],
        details["native_hash_join_build_contract_status"],
        details["native_hash_join_build_required_capability"],
        details["native_hash_join_build_protocol"],
        details["native_hash_join_build_blocker"],
        details["native_hash_aggregate_lookup_contract_status"],
        details["native_hash_aggregate_lookup_required_capability"],
        details["native_hash_aggregate_lookup_protocol"],
        details["native_hash_aggregate_lookup_blocker"],
        details["perfect_required_bits_count"],
        details["perfect_required_bits_total"],
        details["perfect_required_bits"],
        details["perfect_group_minima_count"],
        details["grouped_state_layout_ready"],
        details["grouped_state_offsets"],
        details["grouped_state_payload_sizes"],
    )


def source_runtime_candidate_key(query: str, policy: str, event: dict) -> tuple:
    return (
        query,
        policy,
        event.get("candidate_id", ""),
        event.get("candidate_shape", ""),
        event.get("candidate_pipeline_shape", ""),
        event.get("candidate_context_pipeline_shape", ""),
        event.get("candidate_scope", ""),
    )


def collect_source_native_runtime_by_candidate(out_dir: Path, rows: list) -> dict:
    runtime_by_candidate = collections.defaultdict(collections.Counter)
    executed_runtime_by_candidate = collections.defaultdict(collections.Counter)
    boundary_runtime_keys = set()
    for summary_row in rows:
        policy = summary_row["policy"]
        if policy == "off":
            continue
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("phase") != "runtime":
                    continue
                if event.get("candidate_id", "") == "":
                    continue
                key = source_runtime_candidate_key(summary_row["query"], policy, event)
                if event.get("status") == "source_native":
                    boundary_runtime_keys.add(key)
                    runtime_by_candidate[key]["source_native_output_rows"] += row_int(
                        event, "source_native_output_rows"
                    )
                    runtime_by_candidate[key]["source_native_invocations"] += row_int(
                        event, "source_native_invocation_count"
                    )
                    runtime_by_candidate[key]["source_native_runtime_time_us"] += row_int(
                        event, "source_native_runtime_time_us"
                    )
                elif event.get("status") == "executed":
                    executed_runtime_by_candidate[key]["source_native_output_rows"] += row_int(
                        event, "source_native_output_rows"
                    )
                    executed_runtime_by_candidate[key]["source_native_invocations"] += row_int(
                        event, "source_native_invocation_count"
                    )
                    executed_runtime_by_candidate[key]["source_native_runtime_time_us"] += row_int(
                        event, "source_native_runtime_time_us"
                    )
    for key, executed_runtime in executed_runtime_by_candidate.items():
        if key in boundary_runtime_keys:
            continue
        runtime_by_candidate[key].update(executed_runtime)
    return runtime_by_candidate


def new_source_boundary_summary_entry(policy: str, event: dict, details: dict, source_node: dict) -> dict:
    return {
        "policy": policy,
        "status": event.get("status", ""),
        "execution_mode": event.get("execution_mode", ""),
        "region_execution_form": event.get("region_execution_form", ""),
        "source_boundary_kind": details["source_boundary_kind"],
        "source_execution": source_boundary_execution(event, details),
        "source_operator": source_node["operator_name"],
        "scan_function": details["scan_function"],
        "boundary_operator": details["boundary_operator"],
        "output_columns": details["output_columns"],
        "returned_columns": details["returned_columns"],
        "column_ids": details["column_ids"],
        "source_prefix_input_columns": details["source_prefix_input_columns"],
        "source_prefix_input_types": details["source_prefix_input_types"],
        "source_prefix_output_projection_map": details["source_prefix_output_projection_map"],
        "source_prefix_filter_column_map": details["source_prefix_filter_column_map"],
        "source_prefix_requires_unfiltered_input": details["source_prefix_requires_unfiltered_input"],
        "source_prefix_filter_prune_required": details["source_prefix_filter_prune_required"],
        "source_prefix_filter_split_supported": details["source_prefix_filter_split_supported"],
        "projected_columns": details["projected_columns"],
        "projection_pushdown": details["projection_pushdown"],
        "filter_pushdown": details["filter_pushdown"],
        "filter_prune": details["filter_prune"],
        "filter_count": details["filter_count"],
        "dynamic_filters": details["dynamic_filters"],
        "in_out_function": details["in_out_function"],
        "join_type": details["join_type"],
        "condition_count": details["condition_count"],
        "equality_condition_count": details["equality_condition_count"],
        "non_equality_condition_count": details["non_equality_condition_count"],
        "null_equal_condition_count": details["null_equal_condition_count"],
        "condition_types": details["condition_types"],
        "comparison_ops": details["comparison_ops"],
        "payload_columns": details["payload_columns"],
        "payload_column_indices": details["payload_column_indices"],
        "payload_types": details["payload_types"],
        "lhs_output_columns": details["lhs_output_columns"],
        "lhs_output_column_indices": details["lhs_output_column_indices"],
        "lhs_output_types": details["lhs_output_types"],
        "rhs_output_columns": details["rhs_output_columns"],
        "rhs_output_types": details["rhs_output_types"],
        "lhs_probe_columns": details["lhs_probe_columns"],
        "lhs_probe_column_indices": details["lhs_probe_column_indices"],
        "lhs_probe_types": details["lhs_probe_types"],
        "lhs_output_in_probe": details["lhs_output_in_probe"],
        "delim_types": details["delim_types"],
        "correlated_mark_counts_required": details["correlated_mark_counts_required"],
        "residual_predicate": details["residual_predicate"],
        "residual_info": details["residual_info"],
        "filter_pushdown_condition_count": details["filter_pushdown_condition_count"],
        "filter_pushdown_probe_count": details["filter_pushdown_probe_count"],
        "build_side_has_filter": details["build_side_has_filter"],
        "source_produces_rows": details["source_produces_rows"],
        "regular_hash_table_layout_ready": details["regular_hash_table_layout_ready"],
        "native_probe_shape_ready": details["native_probe_shape_ready"],
        "native_probe_shape_blocker": details["native_probe_shape_blocker"],
        "native_probe_output_mode": details["native_probe_output_mode"],
        "build_append_shape_ready": details["build_append_shape_ready"],
        "build_append_shape_blocker": details["build_append_shape_blocker"],
        "hash_join_layout_column_count": details["hash_join_layout_column_count"],
        "hash_join_layout_offsets": details["hash_join_layout_offsets"],
        "hash_join_tuple_size": details["hash_join_tuple_size"],
        "hash_join_entry_size": details["hash_join_entry_size"],
        "hash_join_pointer_offset": details["hash_join_pointer_offset"],
        "hash_join_hash_column_index": details["hash_join_hash_column_index"],
        "hash_join_found_match_column_present": details["hash_join_found_match_column_present"],
        "hash_join_found_match_column_index": details["hash_join_found_match_column_index"],
        "hash_join_native_protocol_blocker": details["hash_join_native_protocol_blocker"],
        "aggregate_operator_kind": details["aggregate_operator_kind"],
        "group_count": details["group_count"],
        "group_types": details["group_types"],
        "aggregate_count": details["aggregate_count"],
        "aggregate_functions": details["aggregate_functions"],
        "aggregate_return_types": details["aggregate_return_types"],
        "aggregate_child_counts": details["aggregate_child_counts"],
        "aggregate_types": details["aggregate_types"],
        "aggregate_filter_count": details["aggregate_filter_count"],
        "aggregate_order_count": details["aggregate_order_count"],
        "payload_type_count": details["payload_type_count"],
        "grouping_set_count": details["grouping_set_count"],
        "grouping_function_count": details["grouping_function_count"],
        "radix_table_count": details["radix_table_count"],
        "distinct_aggregate_count": details["distinct_aggregate_count"],
        "distinct_table_count": details["distinct_table_count"],
        "distinct_child_count": details["distinct_child_count"],
        "input_group_type_count": details["input_group_type_count"],
        "input_group_types": details["input_group_types"],
        "non_distinct_filter_count": details["non_distinct_filter_count"],
        "distinct_filter_count": details["distinct_filter_count"],
        "native_state_scan_contract_status": details["native_state_scan_contract_status"],
        "native_state_scan_required_capability": details["native_state_scan_required_capability"],
        "native_state_scan_protocol": details["native_state_scan_protocol"],
        "native_state_scan_blocker": details["native_state_scan_blocker"],
        "native_grouped_state_contract_status": details["native_grouped_state_contract_status"],
        "native_grouped_state_required_capability": details["native_grouped_state_required_capability"],
        "native_grouped_state_protocol": details["native_grouped_state_protocol"],
        "native_grouped_state_blocker": details["native_grouped_state_blocker"],
        "native_hash_join_probe_contract_status": details["native_hash_join_probe_contract_status"],
        "native_hash_join_probe_required_capability": details["native_hash_join_probe_required_capability"],
        "native_hash_join_probe_protocol": details["native_hash_join_probe_protocol"],
        "native_hash_join_probe_blocker": details["native_hash_join_probe_blocker"],
        "native_hash_join_build_contract_status": details["native_hash_join_build_contract_status"],
        "native_hash_join_build_required_capability": details["native_hash_join_build_required_capability"],
        "native_hash_join_build_protocol": details["native_hash_join_build_protocol"],
        "native_hash_join_build_blocker": details["native_hash_join_build_blocker"],
        "native_hash_aggregate_lookup_contract_status": details["native_hash_aggregate_lookup_contract_status"],
        "native_hash_aggregate_lookup_required_capability": details[
            "native_hash_aggregate_lookup_required_capability"
        ],
        "native_hash_aggregate_lookup_protocol": details["native_hash_aggregate_lookup_protocol"],
        "native_hash_aggregate_lookup_blocker": details["native_hash_aggregate_lookup_blocker"],
        "perfect_required_bits_count": details["perfect_required_bits_count"],
        "perfect_required_bits_total": details["perfect_required_bits_total"],
        "perfect_required_bits": details["perfect_required_bits"],
        "perfect_group_minima_count": details["perfect_group_minima_count"],
        "grouped_state_layout_ready": details["grouped_state_layout_ready"],
        "grouped_state_offsets": details["grouped_state_offsets"],
        "grouped_state_payload_sizes": details["grouped_state_payload_sizes"],
        "source_native_output_rows": 0,
        "source_native_invocations": 0,
        "source_native_runtime_time_us": 0,
        "occurrences": 0,
        "region_events": 0,
        "event_keys": set(),
        "runtime_keys": set(),
        "queries": collections.Counter(),
        "candidate_shapes": collections.Counter(),
        "candidate_scopes": collections.Counter(),
        "max_estimated_cardinality": 0,
        "example_pipeline_shape": "",
        "example_reason": "",
    }


def accumulate_source_boundary_entry(entry: dict, source_entry: dict) -> None:
    event = source_entry["event"]
    policy = source_entry["policy"]
    event_identity = (source_entry["query"], policy, event.get("event_id", ""))
    entry["occurrences"] += 1
    if event_identity not in entry["event_keys"]:
        entry["region_events"] += 1
        entry["event_keys"].add(event_identity)
    entry["queries"][source_entry["query"]] += 1
    entry["candidate_shapes"][event.get("candidate_shape", "") or "none"] += 1
    entry["candidate_scopes"][event.get("candidate_scope", "") or "none"] += 1
    entry["max_estimated_cardinality"] = max(
        entry["max_estimated_cardinality"], row_int(event, "candidate_estimated_cardinality")
    )
    runtime = source_entry.get("runtime")
    runtime_identity = (source_entry["query"], policy, event.get("candidate_id", ""))
    if runtime and runtime_identity not in entry["runtime_keys"]:
        entry["runtime_keys"].add(runtime_identity)
        entry["source_native_output_rows"] += runtime["source_native_output_rows"]
        entry["source_native_invocations"] += runtime["source_native_invocations"]
        entry["source_native_runtime_time_us"] += runtime["source_native_runtime_time_us"]
    if not entry["example_pipeline_shape"]:
        entry["example_pipeline_shape"] = source_entry["pipeline_shape"]
    if not entry["example_reason"]:
        entry["example_reason"] = source_boundary_example_text(
            event.get("reason", ""), source_entry["trace_text"], source_entry["details"]["source_marker"]
        )


def finalize_source_boundary_summary_entry(entry: dict) -> dict:
    entry = dict(entry)
    entry["query_count"] = len(entry["queries"])
    entry["query_examples"] = format_query_examples(entry["queries"])
    entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
    entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
    del entry["event_keys"]
    del entry["runtime_keys"]
    del entry["queries"]
    return entry


def source_boundary_feature_text(entry: dict) -> str:
    if entry.get("join_type", ""):
        return (
            "join={join};conds={conds};eq={eq};neq={neq};null_eq={null_eq};payload={payload};"
            "payload_idx={payload_idx};lhs={lhs};lhs_idx={lhs_idx};rhs={rhs};probe={probe};probe_idx={probe_idx};"
            "pushdown={pushdown};fp_probes={fp_probes};"
            "state_scan={state_scan};native_probe={native_probe};native_build={native_build}"
        ).format(
            join=entry["join_type"],
            conds=entry["condition_count"],
            eq=entry["equality_condition_count"],
            neq=entry["non_equality_condition_count"],
            null_eq=entry["null_equal_condition_count"],
            payload=entry["payload_columns"],
            payload_idx=entry["payload_column_indices"],
            lhs=entry["lhs_output_columns"],
            lhs_idx=entry["lhs_output_column_indices"],
            rhs=entry["rhs_output_columns"],
            probe=entry["lhs_probe_columns"],
            probe_idx=entry["lhs_probe_column_indices"],
            pushdown=entry["filter_pushdown"],
            fp_probes=entry["filter_pushdown_probe_count"],
            state_scan=entry["native_state_scan_contract_status"],
            native_probe=entry["native_hash_join_probe_contract_status"],
            native_build=entry["native_hash_join_build_contract_status"],
        )
    if entry.get("aggregate_operator_kind", ""):
        return (
            "agg={kind};groups={groups};aggs={aggs};funcs={funcs};payload={payload};distinct={distinct};"
            "filters={filters};orders={orders};radix={radix};grouping_sets={grouping_sets};"
            "state_scan={state_scan};grouped_state={grouped_state};lookup={lookup}"
        ).format(
            kind=entry["aggregate_operator_kind"],
            groups=entry["group_count"],
            aggs=entry["aggregate_count"],
            funcs=truncate_text(entry["aggregate_functions"], 50),
            payload=entry["payload_type_count"],
            distinct=entry["distinct_aggregate_count"],
            filters=entry["aggregate_filter_count"],
            orders=entry["aggregate_order_count"],
            radix=entry["radix_table_count"],
            grouping_sets=entry["grouping_set_count"],
            state_scan=entry["native_state_scan_contract_status"],
            grouped_state=entry["native_grouped_state_contract_status"],
            lookup=entry["native_hash_aggregate_lookup_contract_status"],
        )
    return "cols={cols};projected={projected};filters={filters};dynamic={dynamic};in_out={in_out}".format(
        cols=entry["column_ids"] or entry["output_columns"],
        projected=entry["projected_columns"],
        filters=entry["filter_count"],
        dynamic=entry["dynamic_filters"],
        in_out=entry["in_out_function"],
    )


def collect_source_boundary_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    source_native_runtime_by_candidate = collect_source_native_runtime_by_candidate(out_dir, rows)
    for source_entry in iter_source_boundary_event_entries(out_dir, rows):
        runtime_key = source_runtime_candidate_key(source_entry["query"], source_entry["policy"], source_entry["event"])
        if source_entry["event"].get("status") == "compiled":
            source_entry["runtime"] = source_native_runtime_by_candidate.get(runtime_key)
        else:
            source_entry["runtime"] = None
        key = source_boundary_key(
            source_entry["policy"], source_entry["event"], source_entry["details"], source_entry["source_node"]
        )
        entry = summary.setdefault(
            key,
            new_source_boundary_summary_entry(
                source_entry["policy"], source_entry["event"], source_entry["details"], source_entry["source_node"]
            ),
        )
        accumulate_source_boundary_entry(entry, source_entry)

    result = []
    for entry in summary.values():
        result.append(finalize_source_boundary_summary_entry(entry))
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            -entry["occurrences"],
            -entry["region_events"],
            entry["source_boundary_kind"],
            entry["source_execution"],
            entry["source_operator"],
            entry["scan_function"],
            entry["dynamic_filters"],
            entry["filter_count"],
        )
    )
    return result


def collect_expression_fallback_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        policy = summary_row["policy"]
        if policy == "off":
            continue
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("phase") != "decision":
                    continue
                trace_text = event.get("reason", "") + " " + event.get("ir", "")
                details = list(iter_expression_fallback_details(trace_text))
                if not details:
                    continue
                pipeline_shape = attribution_pipeline_shape(
                    {
                        "status": event.get("status", ""),
                        "candidate_pipeline_shape": event.get("candidate_pipeline_shape", ""),
                        "candidate_context_pipeline_shape": event.get("candidate_context_pipeline_shape", ""),
                    }
                )
                if is_trace_wrapper_pipeline_shape(pipeline_shape):
                    continue
                event_identity = (summary_row["query"], policy, event.get("event_id", ""))
                for detail in details:
                    key = (
                        policy,
                        event.get("status", ""),
                        event.get("execution_mode", ""),
                        event.get("region_execution_form", ""),
                        detail["reason"],
                        detail["expression_class"],
                        detail["expression_type"],
                        detail["function_name"],
                        detail["return_type"],
                    )
                    entry = summary.setdefault(
                        key,
                        {
                            "policy": policy,
                            "status": event.get("status", ""),
                            "execution_mode": event.get("execution_mode", ""),
                            "region_execution_form": event.get("region_execution_form", ""),
                            "reason": detail["reason"],
                            "expression_class": detail["expression_class"],
                            "expression_type": detail["expression_type"],
                            "function_name": detail["function_name"],
                            "return_type": detail["return_type"],
                            "occurrences": 0,
                            "region_events": 0,
                            "event_keys": set(),
                            "queries": collections.Counter(),
                            "candidate_shapes": collections.Counter(),
                            "candidate_scopes": collections.Counter(),
                            "example_pipeline_shape": "",
                            "example_reason": "",
                        },
                    )
                    entry["occurrences"] += 1
                    if event_identity not in entry["event_keys"]:
                        entry["region_events"] += 1
                        entry["event_keys"].add(event_identity)
                    entry["queries"][summary_row["query"]] += 1
                    entry["candidate_shapes"][event.get("candidate_shape", "") or "none"] += 1
                    entry["candidate_scopes"][event.get("candidate_scope", "") or "none"] += 1
                    if not entry["example_pipeline_shape"]:
                        entry["example_pipeline_shape"] = pipeline_shape
                    if not entry["example_reason"]:
                        entry["example_reason"] = expression_fallback_example_text(event.get("reason", ""), trace_text)

    result = []
    for entry in summary.values():
        entry = dict(entry)
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        del entry["event_keys"]
        del entry["queries"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["occurrences"],
            -entry["region_events"],
            entry["reason"],
            entry["function_name"],
            entry["expression_type"],
        )
    )
    return result


def collect_region_decision_breakdown(out_dir: Path, rows: list) -> list:
    breakdown = {}
    for summary_row in rows:
        policy = summary_row["policy"]
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("status") not in REGION_DECISION_STATUSES:
                    continue
                key = (
                    policy,
                    event.get("status", ""),
                    event.get("execution_mode", ""),
                    event.get("region_execution_form", "") or "none",
                    event.get("candidate_shape", "") or "none",
                    event.get("candidate_scope", "") or "none",
                )
                entry = breakdown.setdefault(
                    key,
                    {
                        "policy": policy,
                        "status": event.get("status", ""),
                        "execution_mode": event.get("execution_mode", ""),
                        "region_execution_form": event.get("region_execution_form", "") or "none",
                        "candidate_shape": event.get("candidate_shape", "") or "none",
                        "candidate_scope": event.get("candidate_scope", "") or "none",
                        "count": 0,
                        "queries": collections.Counter(),
                        "example_reason": "",
                    },
                )
                entry["count"] += 1
                entry["queries"][summary_row["query"]] += 1
                if not entry["example_reason"]:
                    entry["example_reason"] = trace_summary_reason(event.get("reason", ""))
    result = list(breakdown.values())
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["count"],
            entry["candidate_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def collect_kernel_runtime_breakdown(out_dir: Path, rows: list) -> list:
    breakdown = {}
    for summary_row in rows:
        policy = summary_row["policy"]
        counters_csv = summary_row.get("kernel_counters_csv")
        if not counters_csv:
            continue
        counters_path = out_dir / counters_csv
        if not counters_path.exists():
            continue
        with counters_path.open(newline="", encoding="utf-8") as handle:
            for counter in csv.DictReader(handle):
                if counter.get("target") != "region":
                    continue
                key = (
                    policy,
                    counter.get("execution_mode", ""),
                    counter.get("region_execution_form", "") or "none",
                    counter.get("candidate_shape", "") or "none",
                    counter.get("candidate_scope", "") or "none",
                )
                entry = breakdown.setdefault(
                    key,
                    {
                        "policy": policy,
                        "execution_mode": counter.get("execution_mode", ""),
                        "region_execution_form": counter.get("region_execution_form", "") or "none",
                        "candidate_shape": counter.get("candidate_shape", "") or "none",
                        "candidate_scope": counter.get("candidate_scope", "") or "none",
                        "kernels": 0,
                        "reached_kernels": 0,
                        "row_processing_kernels": 0,
                        "unreached_kernels": 0,
                        "zero_input_kernels": 0,
                        "input_rows": 0,
                        "output_rows": 0,
                        "invocations": 0,
                        "runtime_time_us": 0,
                        "queries": collections.Counter(),
                    },
                )
                entry["kernels"] += 1
                input_rows = row_int(counter, "input_rows")
                invocation_count = row_int(counter, "invocation_count")
                if invocation_count > 0:
                    entry["reached_kernels"] += 1
                else:
                    entry["unreached_kernels"] += 1
                if input_rows > 0:
                    entry["row_processing_kernels"] += 1
                else:
                    entry["zero_input_kernels"] += 1
                entry["input_rows"] += input_rows
                entry["output_rows"] += row_int(counter, "output_rows")
                entry["invocations"] += invocation_count
                entry["runtime_time_us"] += row_int(counter, "runtime_time_us")
                entry["queries"][summary_row["query"]] += 1
    result = list(breakdown.values())
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["input_rows"],
            entry["candidate_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def flow_step_key(query: str, policy: str, row: dict, phase: str, status: str, policy_decision: str) -> tuple:
    return (
        query,
        policy,
        row.get("target", "") or "none",
        phase or "none",
        status or "none",
        row.get("execution_mode", "") or "none",
        row.get("region_execution_form", "") or "none",
        policy_decision or "none",
        row.get("candidate_shape", "") or "none",
        row.get("candidate_scope", "") or "none",
        row.get("candidate_contract_abi", "") or "none",
        row.get("candidate_pipeline_shape", "") or "none",
        row.get("candidate_context_pipeline_shape", "") or "none",
        row.get("admission_shape_key", "") or "none",
        row.get("admission_rule_present", "") or "none",
        row.get("admission_min_cardinality", "") or "none",
        row.get("admission_score", "") or "none",
        row.get("admission_proof", "") or "none",
    )


def new_flow_step_entry(key: tuple) -> dict:
    return {
        "query": key[0],
        "policy": key[1],
        "target": key[2],
        "phase": key[3],
        "status": key[4],
        "execution_mode": key[5],
        "region_execution_form": key[6],
        "policy_decision": key[7],
        "candidate_shape": key[8],
        "candidate_scope": key[9],
        "candidate_contract_abi": key[10],
        "candidate_pipeline_shape": key[11],
        "candidate_context_pipeline_shape": key[12],
        "admission_shape_key": "" if key[13] == "none" else key[13],
        "admission_rule_present": "" if key[14] == "none" else key[14],
        "admission_min_cardinality": "" if key[15] == "none" else key[15],
        "admission_score": "" if key[16] == "none" else key[16],
        "admission_proof": "" if key[17] == "none" else key[17],
        "event_count": 0,
        "kernel_count": 0,
        "reached_kernels": 0,
        "row_processing_kernels": 0,
        "unreached_kernels": 0,
        "zero_input_kernels": 0,
        "input_rows": 0,
        "output_rows": 0,
        "invocations": 0,
        "runtime_time_us": 0,
        "source_native_output_rows": 0,
        "source_native_invocations": 0,
        "source_native_runtime_time_us": 0,
        "generated_body_runtime_time_us": 0,
        "declined_invocations": 0,
        "declined_runtime_time_us": 0,
        "fallback_input_rows": 0,
        "fallback_output_rows": 0,
        "fallback_invocations": 0,
        "fallback_runtime_time_us": 0,
        "decision_time_us": 0,
        "compile_time_us": 0,
        "code_size": 0,
        "ir_lowering_time_us": 0,
        "backend_analysis_time_us": 0,
        "admission_time_us": 0,
        "overlap_check_time_us": 0,
        "codegen_time_us": 0,
        "example_reason": "",
    }


def collect_flow_step_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        query = summary_row["query"]
        policy = summary_row["policy"]
        events_csv = summary_row.get("events_csv")
        if events_csv:
            events_path = out_dir / events_csv
            if events_path.exists():
                with events_path.open(newline="", encoding="utf-8") as handle:
                    for event in csv.DictReader(handle):
                        key = flow_step_key(
                            query,
                            policy,
                            event,
                            event.get("phase", "") or "none",
                            event.get("status", "") or "none",
                            event.get("policy_decision", "") or "none",
                        )
                        entry = summary.setdefault(key, new_flow_step_entry(key))
                        entry["event_count"] += 1
                        entry["input_rows"] += row_int(event, "input_rows")
                        entry["output_rows"] += row_int(event, "output_rows")
                        entry["invocations"] += row_int(event, "invocation_count")
                        entry["runtime_time_us"] += row_int(event, "runtime_time_us")
                        entry["source_native_output_rows"] += row_int(event, "source_native_output_rows")
                        entry["source_native_invocations"] += row_int(event, "source_native_invocation_count")
                        entry["source_native_runtime_time_us"] += row_int(event, "source_native_runtime_time_us")
                        entry["generated_body_runtime_time_us"] += row_int(event, "generated_body_runtime_time_us")
                        entry["decision_time_us"] += row_int(event, "decision_time_us")
                        entry["compile_time_us"] += row_int(event, "compile_time_us")
                        entry["code_size"] += row_int(event, "code_size")
                        for field in STAGE_FIELDS:
                            entry[field] += row_int(event, field)
                        if not entry["example_reason"]:
                            entry["example_reason"] = trace_summary_reason(event.get("reason", ""))

        kernel_counters_csv = summary_row.get("kernel_counters_csv")
        if not kernel_counters_csv:
            continue
        kernel_counters_path = out_dir / kernel_counters_csv
        if not kernel_counters_path.exists():
            continue
        with kernel_counters_path.open(newline="", encoding="utf-8") as handle:
            for counter in csv.DictReader(handle):
                key = flow_step_key(
                    query,
                    policy,
                    counter,
                    "kernel_counter",
                    counter.get("last_runtime_status", "") or "compiled",
                    "kernel_counter",
                )
                entry = summary.setdefault(key, new_flow_step_entry(key))
                input_rows = row_int(counter, "input_rows")
                invocation_count = row_int(counter, "invocation_count")
                entry["kernel_count"] += 1
                if invocation_count > 0:
                    entry["reached_kernels"] += 1
                else:
                    entry["unreached_kernels"] += 1
                if input_rows > 0:
                    entry["row_processing_kernels"] += 1
                else:
                    entry["zero_input_kernels"] += 1
                entry["input_rows"] += input_rows
                entry["output_rows"] += row_int(counter, "output_rows")
                entry["invocations"] += invocation_count
                entry["runtime_time_us"] += row_int(counter, "runtime_time_us")
                entry["source_native_output_rows"] += row_int(counter, "source_native_output_rows")
                entry["source_native_invocations"] += row_int(counter, "source_native_invocation_count")
                entry["source_native_runtime_time_us"] += row_int(counter, "source_native_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(counter, "generated_body_runtime_time_us")
                entry["declined_invocations"] += row_int(counter, "declined_invocation_count")
                entry["declined_runtime_time_us"] += row_int(counter, "declined_runtime_time_us")
                entry["fallback_input_rows"] += row_int(counter, "fallback_input_rows")
                entry["fallback_output_rows"] += row_int(counter, "fallback_output_rows")
                entry["fallback_invocations"] += row_int(counter, "fallback_invocation_count")
                entry["fallback_runtime_time_us"] += row_int(counter, "fallback_runtime_time_us")
                entry["compile_time_us"] += row_int(counter, "compile_time_us")
                entry["code_size"] += row_int(counter, "code_size")
                if not entry["example_reason"]:
                    entry["example_reason"] = truncate_text(counter.get("compile_reason", ""))

    result = list(summary.values())
    result.sort(
        key=lambda entry: (
            entry["query"],
            entry["policy"],
            entry["target"],
            entry["phase"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_scope"],
            entry["candidate_contract_abi"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["admission_shape_key"],
            entry["admission_rule_present"],
            entry["admission_min_cardinality"],
            entry["admission_score"],
            entry["admission_proof"],
        )
    )
    return result


def collect_operator_profile_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        profile_json = summary_row.get("profile_json")
        if not profile_json:
            continue
        profile_path = out_dir / profile_json
        if not profile_path.exists():
            continue
        profile = read_profile_json(profile_path)
        query_time_us = profile_query_time_us(profile)
        cpu_time_us = profile_cpu_time_us(profile)
        for node, _ in iter_profile_nodes(profile):
            operator_name = str(node.get("type", ""))
            if operator_name in PROFILE_WRAPPER_OPERATORS:
                continue
            key = (summary_row["query"], summary_row["policy"], operator_name)
            entry = summary.setdefault(
                key,
                {
                    "query": summary_row["query"],
                    "policy": summary_row["policy"],
                    "operator_name": operator_name,
                    "occurrences": 0,
                    "operator_time_us": 0,
                    "max_operator_time_us": 0,
                    "output_rows": 0,
                    "intermediate_size_bytes": 0,
                    "query_time_us": query_time_us,
                    "cpu_time_us": cpu_time_us,
                    "extra_infos": collections.Counter(),
                },
            )
            operator_time_us = seconds_to_us(node.get("timing", 0))
            entry["occurrences"] += 1
            entry["operator_time_us"] += operator_time_us
            entry["max_operator_time_us"] = max(entry["max_operator_time_us"], operator_time_us)
            entry["output_rows"] += int(node.get("intermediate_rows") or 0)
            entry["intermediate_size_bytes"] += int(node.get("intermediate_size_bytes") or 0)
            extra_info = format_profile_extra_info(node.get("extra_info") or {})
            if extra_info:
                entry["extra_infos"][extra_info] += 1

    result = []
    for entry in summary.values():
        examples = [
            extra_info
            for extra_info, _ in sorted(entry["extra_infos"].items(), key=lambda item: (-item[1], item[0]))[:3]
        ]
        result.append(
            {
                "query": entry["query"],
                "policy": entry["policy"],
                "operator_name": entry["operator_name"],
                "occurrences": entry["occurrences"],
                "operator_time_us": entry["operator_time_us"],
                "max_operator_time_us": entry["max_operator_time_us"],
                "output_rows": entry["output_rows"],
                "intermediate_size_bytes": entry["intermediate_size_bytes"],
                "query_time_us": entry["query_time_us"],
                "cpu_time_us": entry["cpu_time_us"],
                "percent_query_time": format_percent(entry["operator_time_us"], entry["query_time_us"]),
                "extra_info_examples": " || ".join(examples),
            }
        )
    result.sort(
        key=lambda entry: (entry["query"], entry["policy"], -entry["operator_time_us"], entry["operator_name"])
    )
    return result


def collect_region_decision_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("status") not in REGION_DECISION_STATUSES:
                    continue
                key = (
                    summary_row["query"],
                    summary_row["policy"],
                    event.get("status", ""),
                    event.get("execution_mode", ""),
                    event.get("region_execution_form", "") or "none",
                    event.get("candidate_shape", "") or "none",
                    event.get("candidate_pipeline_shape", "") or "none",
                    event.get("candidate_context_pipeline_shape", "") or "none",
                    event.get("candidate_scope", "") or "none",
                    *(event.get(field, "") or "none" for field in CANDIDATE_TRAIT_FIELDS),
                    event.get("admission_shape_key", "") or "none",
                    event.get("admission_rule_present", "") or "none",
                    event.get("admission_min_cardinality", "") or "none",
                    event.get("admission_score", "") or "none",
                    event.get("admission_proof", "") or "none",
                )
                entry = summary.setdefault(
                    key,
                    {
                        "query": summary_row["query"],
                        "policy": summary_row["policy"],
                        "status": event.get("status", ""),
                        "execution_mode": event.get("execution_mode", ""),
                        "region_execution_form": event.get("region_execution_form", "") or "none",
                        "candidate_shape": event.get("candidate_shape", "") or "none",
                        "candidate_pipeline_shape": event.get("candidate_pipeline_shape", "") or "none",
                        "candidate_context_pipeline_shape": event.get("candidate_context_pipeline_shape", "") or "none",
                        "candidate_scope": event.get("candidate_scope", "") or "none",
                        **{field: event.get(field, "") or "" for field in CANDIDATE_TRAIT_FIELDS},
                        "admission_shape_key": event.get("admission_shape_key", "") or "",
                        "admission_rule_present": event.get("admission_rule_present", "") or "",
                        "admission_min_cardinality": event.get("admission_min_cardinality", "") or "",
                        "admission_score": event.get("admission_score", "") or "",
                        "admission_proof": event.get("admission_proof", "") or "",
                        "count": 0,
                        "max_estimated_cardinality": 0,
                        "decision_time_us": 0,
                        "compile_time_us": 0,
                        "code_size": 0,
                        "ir_lowering_time_us": 0,
                        "backend_analysis_time_us": 0,
                        "admission_time_us": 0,
                        "overlap_check_time_us": 0,
                        "codegen_time_us": 0,
                        "example_reason": "",
                    },
                )
                entry["count"] += 1
                entry["max_estimated_cardinality"] = max(
                    entry["max_estimated_cardinality"], row_int(event, "candidate_estimated_cardinality")
                )
                entry["decision_time_us"] += row_int(event, "decision_time_us")
                entry["compile_time_us"] += row_int(event, "compile_time_us")
                entry["code_size"] += row_int(event, "code_size")
                for field in STAGE_FIELDS:
                    entry[field] += row_int(event, field)
                if not entry["example_reason"]:
                    entry["example_reason"] = trace_summary_reason(event.get("reason", ""))
    result = list(summary.values())
    result.sort(
        key=lambda entry: (
            entry["query"],
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
            *(entry[field] for field in CANDIDATE_TRAIT_FIELDS),
            entry["admission_shape_key"],
            entry["admission_rule_present"],
            entry["admission_min_cardinality"],
            entry["admission_score"],
            entry["admission_proof"],
        )
    )
    return result


def collect_stage_pipeline_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for entry in collect_region_decision_summary(out_dir, rows):
        if is_trace_wrapper_region(entry):
            continue
        key = (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
        stage_entry = summary.setdefault(
            key,
            {
                "policy": entry["policy"],
                "status": entry["status"],
                "execution_mode": entry["execution_mode"],
                "region_execution_form": entry["region_execution_form"],
                "candidate_shape": entry["candidate_shape"],
                "candidate_pipeline_shape": entry["candidate_pipeline_shape"],
                "candidate_context_pipeline_shape": entry["candidate_context_pipeline_shape"],
                "candidate_scope": entry["candidate_scope"],
                "count": 0,
                "queries": collections.Counter(),
                "max_estimated_cardinality": 0,
                "decision_time_us": 0,
                "compile_time_us": 0,
                "stage_total_time_us": 0,
                "dominant_stage": "",
                "dominant_stage_time_us": 0,
                "ir_lowering_time_us": 0,
                "backend_analysis_time_us": 0,
                "admission_time_us": 0,
                "overlap_check_time_us": 0,
                "codegen_time_us": 0,
                "code_size": 0,
                "example_reason": "",
            },
        )
        count = row_int(entry, "count")
        stage_entry["count"] += count
        stage_entry["queries"][entry["query"]] += count
        stage_entry["max_estimated_cardinality"] = max(
            stage_entry["max_estimated_cardinality"], row_int(entry, "max_estimated_cardinality")
        )
        stage_entry["decision_time_us"] += row_int(entry, "decision_time_us")
        stage_entry["compile_time_us"] += row_int(entry, "compile_time_us")
        stage_entry["code_size"] += row_int(entry, "code_size")
        for field in STAGE_FIELDS:
            stage_entry[field] += row_int(entry, field)
        if not stage_entry["example_reason"]:
            stage_entry["example_reason"] = entry.get("example_reason", "")

    result = []
    for entry in summary.values():
        entry = dict(entry)
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        del entry["queries"]
        entry["stage_total_time_us"] = sum(row_int(entry, field) for field in STAGE_FIELDS)
        dominant_stage = max(STAGE_FIELDS, key=lambda field: row_int(entry, field))
        entry["dominant_stage"] = dominant_stage
        entry["dominant_stage_time_us"] = row_int(entry, dominant_stage)
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["stage_total_time_us"],
            -entry["count"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def collect_kernel_runtime_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        counters_csv = summary_row.get("kernel_counters_csv")
        if not counters_csv:
            continue
        counters_path = out_dir / counters_csv
        if not counters_path.exists():
            continue
        with counters_path.open(newline="", encoding="utf-8") as handle:
            for counter in csv.DictReader(handle):
                if counter.get("target") != "region":
                    continue
                key = (
                    summary_row["query"],
                    summary_row["policy"],
                    counter.get("execution_mode", ""),
                    counter.get("region_execution_form", "") or "none",
                    counter.get("candidate_shape", "") or "none",
                    counter.get("candidate_pipeline_shape", "") or "none",
                    counter.get("candidate_context_pipeline_shape", "") or "none",
                    counter.get("candidate_scope", "") or "none",
                )
                entry = summary.setdefault(
                    key,
                    {
                        "query": summary_row["query"],
                        "policy": summary_row["policy"],
                        "execution_mode": counter.get("execution_mode", ""),
                        "region_execution_form": counter.get("region_execution_form", "") or "none",
                        "candidate_shape": counter.get("candidate_shape", "") or "none",
                        "candidate_pipeline_shape": counter.get("candidate_pipeline_shape", "") or "none",
                        "candidate_context_pipeline_shape": counter.get("candidate_context_pipeline_shape", "") or "none",
                        "candidate_scope": counter.get("candidate_scope", "") or "none",
                        "candidate_intervals": set(),
                        "kernels": 0,
                        "reached_kernels": 0,
                        "row_processing_kernels": 0,
                        "unreached_kernels": 0,
                        "zero_input_kernels": 0,
                        "max_estimated_cardinality": 0,
                        "input_rows": 0,
                        "output_rows": 0,
                        "invocations": 0,
                        "runtime_time_us": 0,
                        "source_native_output_rows": 0,
                        "source_native_invocations": 0,
                        "source_native_runtime_time_us": 0,
                        "generated_body_runtime_time_us": 0,
                        "compile_time_us": 0,
                        "code_size": 0,
                    },
                )
                start = counter.get("candidate_start_operator_index", "")
                end = counter.get("candidate_end_operator_index", "")
                if start != "" and end != "":
                    entry["candidate_intervals"].add(f"{start}-{end}")
                entry["kernels"] += 1
                input_rows = row_int(counter, "input_rows")
                invocation_count = row_int(counter, "invocation_count")
                if invocation_count > 0:
                    entry["reached_kernels"] += 1
                else:
                    entry["unreached_kernels"] += 1
                if input_rows > 0:
                    entry["row_processing_kernels"] += 1
                else:
                    entry["zero_input_kernels"] += 1
                entry["max_estimated_cardinality"] = max(
                    entry["max_estimated_cardinality"], row_int(counter, "candidate_estimated_cardinality")
                )
                entry["input_rows"] += input_rows
                entry["output_rows"] += row_int(counter, "output_rows")
                entry["invocations"] += invocation_count
                entry["runtime_time_us"] += row_int(counter, "runtime_time_us")
                entry["source_native_output_rows"] += row_int(counter, "source_native_output_rows")
                entry["source_native_invocations"] += row_int(counter, "source_native_invocation_count")
                entry["source_native_runtime_time_us"] += row_int(counter, "source_native_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(counter, "generated_body_runtime_time_us")
                entry["compile_time_us"] += row_int(counter, "compile_time_us")
                entry["code_size"] += row_int(counter, "code_size")
    result = []
    for entry in summary.values():
        entry = dict(entry)
        entry["candidate_intervals"] = ",".join(sorted(entry["candidate_intervals"]))
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["query"],
            entry["policy"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def admission_efficiency_key(row: dict) -> tuple:
    return (
        row["query"],
        row["policy"],
        row.get("execution_mode", ""),
        row.get("region_execution_form", "") or "none",
        row.get("candidate_shape", "") or "none",
        row.get("candidate_pipeline_shape", "") or "none",
        row.get("candidate_context_pipeline_shape", "") or "none",
        row.get("candidate_scope", "") or "none",
    )


def classify_admission_efficiency(row: dict, region: dict, native_runtime: int, generated_runtime: int) -> tuple:
    runtime_time = row_int(row, "runtime_time_us")
    component_time = native_runtime + generated_runtime
    if row_int(row, "reached_kernels") <= 0 or row_int(row, "invocations") <= 0:
        return "not_reached", "compiled_kernel_not_reached"
    if runtime_time <= 0 and component_time <= 0:
        return "unmeasured", "compiled_kernel_without_runtime_measurement"
    denominator = max(runtime_time, component_time)
    if native_runtime * 100 >= denominator * 50:
        if row_int(region, "candidate_source_filter_count") > 0 and generated_runtime * 100 < denominator * 20:
            return "native_source_dominant", "source_filter_loop_not_generated"
        return "native_source_dominant", "native_source_owns_runtime"
    if generated_runtime * 100 >= denominator * 50:
        return "generated_body_dominant", "generated_body_owns_runtime"
    return "mixed_runtime", "mixed_runtime_ownership"


def collect_admission_efficiency_summary(out_dir: Path, rows: list) -> list:
    region_by_key = {}
    for region in collect_region_decision_summary(out_dir, rows):
        if region.get("status") != "compiled":
            continue
        key = admission_efficiency_key(region)
        existing = region_by_key.get(key)
        if existing is None or row_int(region, "count") > row_int(existing, "count"):
            region_by_key[key] = region

    result = []
    for kernel in collect_kernel_runtime_summary(out_dir, rows):
        region = region_by_key.get(admission_efficiency_key(kernel), {})
        source_native_runtime = row_int(kernel, "source_native_runtime_time_us")
        generated_body_runtime = row_int(kernel, "generated_body_runtime_time_us")
        runtime_time = row_int(kernel, "runtime_time_us")
        component_time = source_native_runtime + generated_body_runtime
        denominator = max(runtime_time, component_time)
        efficiency_class, root_cause = classify_admission_efficiency(
            kernel, region, source_native_runtime, generated_body_runtime
        )
        result.append(
            {
                "query": kernel["query"],
                "policy": kernel["policy"],
                "status": region.get("status", "compiled"),
                "execution_mode": kernel["execution_mode"],
                "region_execution_form": kernel["region_execution_form"],
                "candidate_shape": kernel["candidate_shape"],
                "candidate_pipeline_shape": kernel["candidate_pipeline_shape"],
                "candidate_context_pipeline_shape": kernel["candidate_context_pipeline_shape"],
                "candidate_scope": kernel["candidate_scope"],
                "candidate_contract_abi": region.get("candidate_contract_abi", ""),
                "admission_shape_key": region.get("admission_shape_key", ""),
                "admission_rule_present": region.get("admission_rule_present", ""),
                "admission_min_cardinality": region.get("admission_min_cardinality", ""),
                "admission_score": region.get("admission_score", ""),
                "admission_proof": region.get("admission_proof", ""),
                "kernels": kernel["kernels"],
                "reached_kernels": kernel["reached_kernels"],
                "row_processing_kernels": kernel["row_processing_kernels"],
                "input_rows": kernel["input_rows"],
                "output_rows": kernel["output_rows"],
                "invocations": kernel["invocations"],
                "runtime_time_us": kernel["runtime_time_us"],
                "source_native_runtime_time_us": source_native_runtime,
                "source_native_runtime_percent": format_percent(source_native_runtime, denominator),
                "generated_body_runtime_time_us": generated_body_runtime,
                "generated_body_runtime_percent": format_percent(generated_body_runtime, denominator),
                "compile_time_us": kernel["compile_time_us"],
                "code_size": kernel["code_size"],
                "efficiency_class": efficiency_class,
                "root_cause": root_cause,
            }
        )
    result.sort(
        key=lambda entry: (
            entry["query"],
            entry["policy"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def classify_admission_proof_gap(
    winning_queries: int,
    losing_queries: int,
    equal_queries: int,
    auto_rule_present: bool,
    auto_compiled_regions: int,
    admission_shape_key: str,
) -> tuple:
    if winning_queries > 0 and losing_queries == 0:
        proof_status = "positive_query_median"
    elif winning_queries > 0 and losing_queries > 0:
        proof_status = "mixed_query_median"
    elif winning_queries == 0 and losing_queries == 0 and equal_queries > 0:
        proof_status = "neutral_query_median"
    else:
        proof_status = "negative_query_median"

    if auto_rule_present:
        if auto_compiled_regions > 0:
            root_cause = "auto_rule_admitted_and_compiled"
        elif proof_status == "positive_query_median":
            root_cause = "auto_rule_present_but_not_selected_or_not_compiled"
        else:
            root_cause = "auto_rule_requires_cost_model_refinement"
    elif (
        proof_status == "positive_query_median"
        and admission_shape_key in CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES
    ):
        root_cause = "context_specific_positive_without_generic_admission_proof"
    elif proof_status == "positive_query_median":
        root_cause = "missing_measured_auto_admission_proof"
    elif proof_status == "mixed_query_median":
        root_cause = "shape_profitability_depends_on_context"
    elif proof_status == "neutral_query_median":
        root_cause = "neutral_query_level_evidence"
    else:
        root_cause = "force_region_not_profitable"
    return proof_status, root_cause


def admission_proof_gap_key(row: dict) -> tuple:
    return (
        row["admission_shape_key"],
        row["execution_mode"],
        row["region_execution_form"],
        row["candidate_shape"],
        row["candidate_scope"],
    )


def collect_admission_proof_gap_summary(out_dir: Path, rows: list) -> list:
    speedup_by_query_policy = {}
    grouped_summary = {(row["query"], row["policy"]): row for row in rows}
    for query_id in sorted({row["query"] for row in rows}):
        off = grouped_summary.get((query_id, "off"), {})
        off_time = row_float(off, "total_time_s")
        if off_time <= 0:
            continue
        for policy in ("auto", "force"):
            policy_row = grouped_summary.get((query_id, policy), {})
            policy_time = row_float(policy_row, "total_time_s")
            if policy_time > 0:
                speedup_by_query_policy[(query_id, policy)] = off_time / policy_time

    grouped = collections.defaultdict(
        lambda: {
            "queries": set(),
            "query_speedups": {},
            "force_compiled_regions": 0,
            "auto_compiled_regions": 0,
            "auto_skipped_regions": 0,
            "auto_rule_present": False,
            "max_estimated_cardinality": 0,
            "compile_time_us": 0,
            "code_size": 0,
            **{field: 0 for field in STAGE_FIELDS},
            "example_reason": "",
        }
    )
    decision_counter_rows = collect_decision_counter_summary(out_dir, rows)

    force_compiled_keys = set()
    for row in decision_counter_rows:
        if row["target"] == "region" and row["policy"] == "force" and row["status"] == "compiled":
            force_compiled_keys.add(admission_proof_gap_key(row))

    for row in decision_counter_rows:
        if row["target"] != "region":
            continue
        key = admission_proof_gap_key(row)
        if key not in force_compiled_keys:
            continue
        entry = grouped[key]
        count = row_int(row, "count")
        if row.get("admission_rule_present") == "true":
            entry["auto_rule_present"] = True
        entry["max_estimated_cardinality"] = max(
            entry["max_estimated_cardinality"], row_int(row, "max_estimated_cardinality")
        )
        if not entry["example_reason"]:
            entry["example_reason"] = compact_reason(row.get("example_reason", ""))
        if row["policy"] == "force" and row["status"] == "compiled":
            entry["queries"].add(row["query"])
            entry["query_speedups"][row["query"]] = speedup_by_query_policy.get((row["query"], "force"), 0.0)
            entry["force_compiled_regions"] += count
            entry["compile_time_us"] += row_int(row, "compile_time_us")
            entry["code_size"] += row_int(row, "code_size")
            for field in STAGE_FIELDS:
                entry[field] += row_int(row, field)
        elif row["policy"] == "auto" and row["status"] == "compiled":
            entry["auto_compiled_regions"] += count
        elif row["policy"] == "auto" and row["status"] == "skipped":
            entry["auto_skipped_regions"] += count

    result = []
    for key, entry in grouped.items():
        if entry["force_compiled_regions"] <= 0:
            continue
        admission_shape_key, execution_mode, region_execution_form, candidate_shape, candidate_scope = key
        speedups = list(entry["query_speedups"].values())
        winning_queries = sum(1 for speedup in speedups if speedup > 1.000001)
        losing_queries = sum(1 for speedup in speedups if speedup < 0.999999)
        equal_queries = len(speedups) - winning_queries - losing_queries
        proof_status, root_cause = classify_admission_proof_gap(
            winning_queries,
            losing_queries,
            equal_queries,
            entry["auto_rule_present"],
            entry["auto_compiled_regions"],
            admission_shape_key,
        )
        stage_time_us = sum(entry[field] for field in STAGE_FIELDS)
        result.append(
            {
                "admission_shape_key": admission_shape_key,
                "execution_mode": execution_mode,
                "region_execution_form": region_execution_form,
                "candidate_shape": candidate_shape,
                "candidate_scope": candidate_scope,
                "query_count": len(entry["queries"]),
                "query_examples": join_unique(entry["queries"]),
                "force_compiled_regions": entry["force_compiled_regions"],
                "force_winning_queries": winning_queries,
                "force_losing_queries": losing_queries,
                "force_equal_queries": equal_queries,
                "min_force_speedup_vs_off": format_ratio(min(speedups) if speedups else 0.0),
                "median_force_speedup_vs_off": format_ratio(statistics.median(speedups) if speedups else 0.0),
                "max_force_speedup_vs_off": format_ratio(max(speedups) if speedups else 0.0),
                "auto_rule_present": str(entry["auto_rule_present"]).lower(),
                "auto_compiled_regions": entry["auto_compiled_regions"],
                "auto_skipped_regions": entry["auto_skipped_regions"],
                "max_estimated_cardinality": entry["max_estimated_cardinality"],
                "compile_time_us": entry["compile_time_us"],
                "code_size": entry["code_size"],
                "stage_time_us": stage_time_us,
                **{field: entry[field] for field in STAGE_FIELDS},
                "proof_status": proof_status,
                "root_cause": root_cause,
                "example_reason": entry["example_reason"],
            }
        )

    status_order = {
        "positive_query_median": 0,
        "mixed_query_median": 1,
        "neutral_query_median": 2,
        "negative_query_median": 3,
    }
    return sorted(
        result,
        key=lambda row: (
            status_order.get(row["proof_status"], 100),
            -row_float(row, "median_force_speedup_vs_off"),
            row["admission_shape_key"],
            row["candidate_shape"],
            row["candidate_scope"],
        ),
    )


def select_top_region_entry(entries: list) -> dict:
    if not entries:
        return {}
    return sorted(
        entries,
        key=lambda entry: (
            -row_int(entry, "count"),
            -row_int(entry, "max_estimated_cardinality"),
            entry.get("candidate_shape", ""),
            entry.get("candidate_pipeline_shape", ""),
        ),
    )[0]


def select_top_profile_entry(entries: list) -> dict:
    if not entries:
        return {}
    return sorted(
        entries,
        key=lambda entry: (
            -row_int(entry, "operator_time_us"),
            entry.get("operator_name", ""),
        ),
    )[0]


def is_trace_wrapper_region(entry: dict) -> bool:
    return is_trace_wrapper_pipeline_shape(attribution_pipeline_shape(entry))


def relevant_region_entries(entries: list) -> list:
    return [entry for entry in entries if not is_trace_wrapper_region(entry)]


def summarize_compiled_pipeline_shapes(kernel_entries: list, limit: int = 3) -> str:
    if not kernel_entries:
        return ""
    ordered = sorted(
        kernel_entries,
        key=lambda entry: (
            -row_int(entry, "input_rows"),
            entry.get("candidate_shape", ""),
            entry.get("candidate_pipeline_shape", ""),
        ),
    )
    result = []
    for entry in ordered[:limit]:
        result.append(
            "{shape}:{pipeline}".format(
                shape=entry.get("candidate_shape", "") or "none",
                pipeline=entry.get("candidate_pipeline_shape", "") or "none",
            )
        )
    if len(ordered) > limit:
        result.append(f"+{len(ordered) - limit} more")
    return " | ".join(result)


def summarize_compiled_pipeline_scopes(kernel_entries: list) -> str:
    scope_counter = collections.Counter()
    for entry in kernel_entries:
        scope_counter[entry.get("candidate_scope", "") or "none"] += row_int(entry, "kernels")
    return format_counter_examples(scope_counter)


def sum_profile_time_us(entries: list, operator_names: Optional[tuple] = None) -> int:
    if operator_names is None:
        return sum(row_int(entry, "operator_time_us") for entry in entries)
    operator_set = set(operator_names)
    return sum(row_int(entry, "operator_time_us") for entry in entries if entry.get("operator_name") in operator_set)


def classify_query_gap(
    auto_row: dict,
    force_row: dict,
    force_relevant_unsupported_count: int,
    force_kernels: list,
    force_profile_entries: list,
) -> str:
    reasons = []
    compiled_kernels = sum(row_int(entry, "kernels") for entry in force_kernels)
    row_processing_kernels = sum(row_int(entry, "row_processing_kernels") for entry in force_kernels)
    if row_int(auto_row, "skipped_regions") > 0:
        reasons.append("auto_no_admission_rule")
    if compiled_kernels > 0 and row_processing_kernels == 0:
        reasons.append("compiled_kernels_not_reached")
    elif compiled_kernels > 0 and row_processing_kernels < compiled_kernels:
        reasons.append("partial_compiled_kernel_reach")
    if row_processing_kernels > 0:
        reasons.append("force_small_generated_regions")
    if force_relevant_unsupported_count > 0:
        reasons.append("unsupported_operator_or_expression_boundaries")
    force_top_profile = select_top_profile_entry(force_profile_entries)
    if force_top_profile.get("operator_name") in PROFILE_HEAVY_OPERATORS:
        reasons.append("profile_dominated_by_scan_join_groupby")
    if row_processing_kernels > 0:
        force_profile_time_us = sum_profile_time_us(force_profile_entries)
        projection_time_us = sum_profile_time_us(force_profile_entries, ("PROJECTION",))
        projection_percent = (projection_time_us * 100.0 / force_profile_time_us) if force_profile_time_us > 0 else 0.0
        if projection_percent < 10.0:
            reasons.append("compiled_projection_not_runtime_dominant")
    if not reasons:
        reasons.append("no_region_activity")
    return ";".join(reasons)


def collect_query_gap_summary(out_dir: Path, rows: list) -> list:
    by_query_policy = {}
    for row in rows:
        by_query_policy[(row["query"], row["policy"])] = row

    region_by_query_policy_status = collections.defaultdict(list)
    for entry in collect_region_decision_summary(out_dir, rows):
        region_by_query_policy_status[(entry["query"], entry["policy"], entry["status"])].append(entry)

    kernel_by_query_policy = collections.defaultdict(list)
    for entry in collect_kernel_runtime_summary(out_dir, rows):
        kernel_by_query_policy[(entry["query"], entry["policy"])].append(entry)

    profile_by_query_policy = collections.defaultdict(list)
    for entry in collect_operator_profile_summary(out_dir, rows):
        profile_by_query_policy[(entry["query"], entry["policy"])].append(entry)

    result = []
    for query_id in sorted({row["query"] for row in rows}):
        off = by_query_policy.get((query_id, "off"), {})
        auto = by_query_policy.get((query_id, "auto"), {})
        force = by_query_policy.get((query_id, "force"), {})
        auto_skip_entries = relevant_region_entries(region_by_query_policy_status[(query_id, "auto", "skipped")])
        auto_skip = select_top_region_entry(auto_skip_entries)
        force_unsupported_entries = region_by_query_policy_status[(query_id, "force", "unsupported")]
        force_relevant_unsupported_entries = relevant_region_entries(force_unsupported_entries)
        force_unsupported = select_top_region_entry(force_relevant_unsupported_entries)
        force_relevant_unsupported_count = sum(row_int(entry, "count") for entry in force_relevant_unsupported_entries)
        force_kernels = kernel_by_query_policy[(query_id, "force")]
        force_compiled_kernels = sum(row_int(entry, "kernels") for entry in force_kernels)
        force_reached_kernels = sum(row_int(entry, "reached_kernels") for entry in force_kernels)
        force_row_processing_kernels = sum(row_int(entry, "row_processing_kernels") for entry in force_kernels)
        force_unreached_kernels = sum(row_int(entry, "unreached_kernels") for entry in force_kernels)
        force_zero_input_kernels = sum(row_int(entry, "zero_input_kernels") for entry in force_kernels)
        force_profile_entries = profile_by_query_policy[(query_id, "force")]
        force_top_profile = select_top_profile_entry(force_profile_entries)
        force_profile_time_us = sum_profile_time_us(force_profile_entries)
        force_scan_join_groupby_time_us = sum_profile_time_us(force_profile_entries, PROFILE_HEAVY_OPERATORS)
        force_projection_time_us = sum_profile_time_us(force_profile_entries, ("PROJECTION",))
        off_s = row_float(off, "total_time_s")
        auto_s = row_float(auto, "total_time_s")
        force_s = row_float(force, "total_time_s")
        result.append(
            {
                "query": query_id,
                "off_s": f"{off_s:.6f}" if off else "",
                "auto_s": f"{auto_s:.6f}" if auto else "",
                "force_s": f"{force_s:.6f}" if force else "",
                "auto_relative_to_off": f"{auto_s / off_s:.4f}" if off_s > 0 and auto else "",
                "force_relative_to_off": f"{force_s / off_s:.4f}" if off_s > 0 and force else "",
                "correctness_diff": row_correctness_diff(auto) + row_correctness_diff(force),
                "auto_compiled_regions": row_int(auto, "compiled_regions"),
                "auto_skipped_regions": row_int(auto, "skipped_regions"),
                "auto_unsupported_regions": row_int(auto, "unsupported_regions"),
                "force_compiled_regions": row_int(force, "compiled_regions"),
                "force_skipped_regions": row_int(force, "skipped_regions"),
                "force_unsupported_regions": row_int(force, "unsupported_regions"),
                "force_relevant_unsupported_regions": force_relevant_unsupported_count,
                "force_compiled_kernels": force_compiled_kernels,
                "force_reached_kernels": force_reached_kernels,
                "force_row_processing_kernels": force_row_processing_kernels,
                "force_unreached_kernels": force_unreached_kernels,
                "force_zero_input_kernels": force_zero_input_kernels,
                "force_runtime_input_rows": sum(row_int(entry, "input_rows") for entry in force_kernels),
                "force_runtime_output_rows": sum(row_int(entry, "output_rows") for entry in force_kernels),
                "force_runtime_invocations": sum(row_int(entry, "invocations") for entry in force_kernels),
                "force_runtime_time_us": sum(row_int(entry, "runtime_time_us") for entry in force_kernels),
                "force_source_native_output_rows": sum(
                    row_int(entry, "source_native_output_rows") for entry in force_kernels
                ),
                "force_source_native_invocations": sum(
                    row_int(entry, "source_native_invocations") for entry in force_kernels
                ),
                "force_source_native_runtime_time_us": sum(
                    row_int(entry, "source_native_runtime_time_us") for entry in force_kernels
                ),
                "force_generated_body_runtime_time_us": sum(
                    row_int(entry, "generated_body_runtime_time_us") for entry in force_kernels
                ),
                "force_top_profile_operator": force_top_profile.get("operator_name", ""),
                "force_top_profile_time_us": row_int(force_top_profile, "operator_time_us"),
                "force_top_profile_percent": force_top_profile.get("percent_query_time", ""),
                "force_profile_operator_time_us": force_profile_time_us,
                "force_scan_join_groupby_profile_percent": format_percent(
                    force_scan_join_groupby_time_us, force_profile_time_us
                ),
                "force_projection_profile_percent": format_percent(force_projection_time_us, force_profile_time_us),
                "force_compiled_pipeline_shapes": summarize_compiled_pipeline_shapes(force_kernels),
                "force_compiled_pipeline_scopes": summarize_compiled_pipeline_scopes(force_kernels),
                "auto_top_skip_shape": auto_skip.get("candidate_shape", ""),
                "auto_top_skip_pipeline_shape": attribution_pipeline_shape(auto_skip),
                "auto_top_skip_scope": auto_skip.get("candidate_scope", ""),
                "auto_top_skip_count": row_int(auto_skip, "count"),
                "auto_top_skip_reason": auto_skip.get("example_reason", ""),
                "force_top_unsupported_shape": force_unsupported.get("candidate_shape", ""),
                "force_top_unsupported_pipeline_shape": attribution_pipeline_shape(force_unsupported),
                "force_top_unsupported_scope": force_unsupported.get("candidate_scope", ""),
                "force_top_unsupported_count": row_int(force_unsupported, "count"),
                "force_top_unsupported_reason": force_unsupported.get("example_reason", ""),
                "root_cause": classify_query_gap(
                    auto,
                    force,
                    force_relevant_unsupported_count,
                    force_kernels,
                    force_profile_entries,
                ),
            }
        )
    return result


def format_counter_examples(value_counter: collections.Counter, limit: int = 5) -> str:
    examples = []
    for value, count in sorted(value_counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        if count == 1:
            examples.append(str(value))
        else:
            examples.append(f"{value}({count})")
    return ", ".join(examples)


def collect_operator_gap_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for region_entry in collect_region_decision_summary(out_dir, rows):
        if region_entry["status"] not in REGION_DECISION_STATUSES:
            continue
        pipeline_shape = attribution_pipeline_shape(region_entry)
        if is_trace_wrapper_pipeline_shape(pipeline_shape):
            continue
        count = row_int(region_entry, "count")
        seen_event_keys = set()
        for node in parse_pipeline_shape(pipeline_shape):
            if not is_gap_pipeline_node(node):
                continue
            key = (
                region_entry["policy"],
                region_entry["status"],
                region_entry["execution_mode"],
                region_entry["region_execution_form"],
                node["operator_name"],
                node["node_kind"],
                node["boundary"],
            )
            entry = summary.setdefault(
                key,
                {
                    "policy": region_entry["policy"],
                    "status": region_entry["status"],
                    "execution_mode": region_entry["execution_mode"],
                    "region_execution_form": region_entry["region_execution_form"],
                    "operator_name": node["operator_name"],
                    "node_kind": node["node_kind"],
                    "boundary": node["boundary"],
                    "occurrences": 0,
                    "region_events": 0,
                    "queries": collections.Counter(),
                    "candidate_shapes": collections.Counter(),
                    "candidate_scopes": collections.Counter(),
                    "max_estimated_cardinality": 0,
                    "example_pipeline_shape": "",
                    "example_reason": "",
                },
            )
            entry["occurrences"] += count
            if key not in seen_event_keys:
                entry["region_events"] += count
                seen_event_keys.add(key)
            entry["queries"][region_entry["query"]] += count
            entry["candidate_shapes"][region_entry["candidate_shape"]] += count
            entry["candidate_scopes"][region_entry["candidate_scope"]] += count
            entry["max_estimated_cardinality"] = max(
                entry["max_estimated_cardinality"], row_int(region_entry, "max_estimated_cardinality")
            )
            if not entry["example_pipeline_shape"]:
                entry["example_pipeline_shape"] = pipeline_shape
            if not entry["example_reason"]:
                entry["example_reason"] = region_entry["example_reason"]

    result = []
    for entry in summary.values():
        entry = dict(entry)
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        del entry["queries"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["occurrences"],
            -entry["region_events"],
            entry["operator_name"],
            entry["boundary"],
        )
    )
    return result


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


def collect_capability_gap_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for region_entry in collect_region_decision_summary(out_dir, rows):
        if region_entry["status"] not in REGION_DECISION_STATUSES:
            continue
        pipeline_shape = attribution_pipeline_shape(region_entry)
        if is_trace_wrapper_pipeline_shape(pipeline_shape):
            continue
        count = row_int(region_entry, "count")
        seen_event_keys = set()
        for node in parse_pipeline_shape(pipeline_shape):
            if not is_gap_pipeline_node(node):
                continue
            capability_gap = classify_capability_gap(node)
            key = (
                region_entry["policy"],
                region_entry["status"],
                region_entry["execution_mode"],
                region_entry["region_execution_form"],
                capability_gap,
            )
            entry = summary.setdefault(
                key,
                {
                    "policy": region_entry["policy"],
                    "status": region_entry["status"],
                    "execution_mode": region_entry["execution_mode"],
                    "region_execution_form": region_entry["region_execution_form"],
                    "capability_gap": capability_gap,
                    "occurrences": 0,
                    "region_events": 0,
                    "queries": collections.Counter(),
                    "operators": collections.Counter(),
                    "boundaries": collections.Counter(),
                    "candidate_shapes": collections.Counter(),
                    "candidate_scopes": collections.Counter(),
                    "max_estimated_cardinality": 0,
                    "example_pipeline_shape": "",
                    "example_reason": "",
                },
            )
            entry["occurrences"] += count
            if key not in seen_event_keys:
                entry["region_events"] += count
                seen_event_keys.add(key)
            entry["queries"][region_entry["query"]] += count
            entry["operators"][node["operator_name"]] += count
            entry["boundaries"][node["boundary"]] += count
            entry["candidate_shapes"][region_entry["candidate_shape"]] += count
            entry["candidate_scopes"][region_entry["candidate_scope"]] += count
            entry["max_estimated_cardinality"] = max(
                entry["max_estimated_cardinality"], row_int(region_entry, "max_estimated_cardinality")
            )
            if not entry["example_pipeline_shape"]:
                entry["example_pipeline_shape"] = pipeline_shape
            if not entry["example_reason"]:
                entry["example_reason"] = region_entry["example_reason"]

    result = []
    for entry in summary.values():
        entry = dict(entry)
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["operators"] = format_counter_examples(entry["operators"])
        entry["boundaries"] = format_counter_examples(entry["boundaries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        del entry["queries"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -entry["occurrences"],
            -entry["region_events"],
            entry["capability_gap"],
        )
    )
    return result


def format_profile_operator_examples(profile_keys: set, profile_lookup: dict, limit: int = 5) -> str:
    profile_by_operator = collections.Counter()
    for profile_key in profile_keys:
        operator_name = profile_key[2]
        profile_by_operator[operator_name] += row_int(profile_lookup[profile_key], "operator_time_us")
    examples = []
    for operator_name, time_us in sorted(profile_by_operator.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        examples.append(f"{operator_name}({time_us}us)")
    return ", ".join(examples)


def format_profile_operator_allocations(profile_allocations: dict, limit: int = 5) -> str:
    profile_by_operator = collections.Counter()
    for profile_key, allocated_time_us in profile_allocations.items():
        profile_by_operator[profile_key[2]] += allocated_time_us
    examples = []
    for operator_name, time_us in sorted(profile_by_operator.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        examples.append(f"{operator_name}({time_us}us)")
    return ", ".join(examples)


def new_capability_runtime_entry() -> dict:
    return {field: 0 for field in CAPABILITY_RUNTIME_FIELDS}


def accumulate_capability_runtime_entry(entry: dict, kernel_entry: dict) -> None:
    entry["runtime_input_rows"] += row_int(kernel_entry, "input_rows")
    entry["runtime_output_rows"] += row_int(kernel_entry, "output_rows")
    entry["runtime_invocations"] += row_int(kernel_entry, "invocations")
    entry["runtime_time_us"] += row_int(kernel_entry, "runtime_time_us")
    entry["source_native_output_rows"] += row_int(kernel_entry, "source_native_output_rows")
    entry["source_native_invocations"] += row_int(kernel_entry, "source_native_invocations")
    entry["source_native_runtime_time_us"] += row_int(kernel_entry, "source_native_runtime_time_us")
    entry["generated_body_runtime_time_us"] += row_int(kernel_entry, "generated_body_runtime_time_us")


def collect_capability_runtime_summary(out_dir: Path, rows: list) -> tuple:
    workload_summary = {}
    query_summary = {}
    for kernel_entry in collect_kernel_runtime_summary(out_dir, rows):
        pipeline_shape = kernel_entry["candidate_pipeline_shape"]
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
                kernel_entry["policy"],
                "compiled",
                kernel_entry["execution_mode"],
                kernel_entry["region_execution_form"],
                capability_gap,
            )
            query_key = (
                kernel_entry["query"],
                kernel_entry["policy"],
                "compiled",
                kernel_entry["execution_mode"],
                kernel_entry["region_execution_form"],
                capability_gap,
            )
            accumulate_capability_runtime_entry(
                workload_summary.setdefault(workload_key, new_capability_runtime_entry()), kernel_entry
            )
            accumulate_capability_runtime_entry(
                query_summary.setdefault(query_key, new_capability_runtime_entry()), kernel_entry
            )
    return workload_summary, query_summary


def source_boundary_priority_sort_key(entry: dict) -> tuple:
    return (
        entry["policy"],
        entry["status"],
        entry["execution_mode"],
        entry["region_execution_form"],
        entry["source_boundary_kind"],
        entry["source_execution"],
        entry["source_operator"],
        entry["scan_function"],
        entry["boundary_operator"],
        entry["output_columns"],
        entry["returned_columns"],
        entry["column_ids"],
        entry["source_prefix_input_columns"],
        entry["source_prefix_input_types"],
        entry["source_prefix_output_projection_map"],
        entry["source_prefix_filter_column_map"],
        entry["source_prefix_requires_unfiltered_input"],
        entry["source_prefix_filter_prune_required"],
        entry["source_prefix_filter_split_supported"],
        entry["projected_columns"],
        entry["projection_pushdown"],
        entry["filter_pushdown"],
        entry["filter_prune"],
        entry["filter_count"],
        entry["dynamic_filters"],
        entry["in_out_function"],
        entry["join_type"],
        entry["condition_count"],
        entry["equality_condition_count"],
        entry["non_equality_condition_count"],
        entry["null_equal_condition_count"],
        entry["condition_types"],
        entry["comparison_ops"],
        entry["payload_columns"],
        entry["payload_column_indices"],
        entry["payload_types"],
        entry["lhs_output_columns"],
        entry["lhs_output_column_indices"],
        entry["lhs_output_types"],
        entry["rhs_output_columns"],
        entry["rhs_output_types"],
        entry["lhs_probe_columns"],
        entry["lhs_probe_column_indices"],
        entry["lhs_probe_types"],
        entry["lhs_output_in_probe"],
        entry["delim_types"],
        entry["correlated_mark_counts_required"],
        entry["residual_predicate"],
        entry["residual_info"],
        entry["filter_pushdown_condition_count"],
        entry["filter_pushdown_probe_count"],
        entry["build_side_has_filter"],
        entry["source_produces_rows"],
        entry["regular_hash_table_layout_ready"],
        entry["native_probe_shape_ready"],
        entry["native_probe_shape_blocker"],
        entry["native_probe_output_mode"],
        entry["build_append_shape_ready"],
        entry["build_append_shape_blocker"],
        entry["hash_join_layout_column_count"],
        entry["hash_join_layout_offsets"],
        entry["hash_join_tuple_size"],
        entry["hash_join_entry_size"],
        entry["hash_join_pointer_offset"],
        entry["hash_join_hash_column_index"],
        entry["hash_join_found_match_column_present"],
        entry["hash_join_found_match_column_index"],
        entry["hash_join_native_protocol_blocker"],
        entry["aggregate_operator_kind"],
        entry["group_count"],
        entry["group_types"],
        entry["aggregate_count"],
        entry["aggregate_functions"],
        entry["aggregate_return_types"],
        entry["aggregate_child_counts"],
        entry["aggregate_types"],
        entry["aggregate_filter_count"],
        entry["aggregate_order_count"],
        entry["payload_type_count"],
        entry["grouping_set_count"],
        entry["grouping_function_count"],
        entry["radix_table_count"],
        entry["distinct_aggregate_count"],
        entry["distinct_table_count"],
        entry["distinct_child_count"],
        entry["input_group_type_count"],
        entry["input_group_types"],
        entry["non_distinct_filter_count"],
        entry["distinct_filter_count"],
        entry["native_state_scan_contract_status"],
        entry["native_state_scan_required_capability"],
        entry["native_state_scan_protocol"],
        entry["native_state_scan_blocker"],
        entry["native_grouped_state_contract_status"],
        entry["native_grouped_state_required_capability"],
        entry["native_grouped_state_protocol"],
        entry["native_grouped_state_blocker"],
        entry["native_hash_join_probe_contract_status"],
        entry["native_hash_join_probe_required_capability"],
        entry["native_hash_join_probe_protocol"],
        entry["native_hash_join_probe_blocker"],
        entry["native_hash_join_build_contract_status"],
        entry["native_hash_join_build_required_capability"],
        entry["native_hash_join_build_protocol"],
        entry["native_hash_join_build_blocker"],
        entry["native_hash_aggregate_lookup_contract_status"],
        entry["native_hash_aggregate_lookup_required_capability"],
        entry["native_hash_aggregate_lookup_protocol"],
        entry["native_hash_aggregate_lookup_blocker"],
        entry["perfect_required_bits_count"],
        entry["perfect_required_bits_total"],
        entry["perfect_required_bits"],
        entry["perfect_group_minima_count"],
        entry["grouped_state_layout_ready"],
        entry["grouped_state_offsets"],
        entry["grouped_state_payload_sizes"],
    )


def allocate_source_boundary_profile_time(summary: dict, profile_lookup: dict) -> None:
    profile_key_entries = collections.defaultdict(list)
    for entry in summary.values():
        for profile_key, occurrence_count in entry["profile_key_occurrences"].items():
            profile_key_entries[profile_key].append((entry, occurrence_count))

    for profile_key, entries in profile_key_entries.items():
        if profile_key not in profile_lookup:
            continue
        total_occurrences = sum(occurrence_count for _, occurrence_count in entries)
        if total_occurrences <= 0:
            continue
        profile_time_us = row_int(profile_lookup[profile_key], "operator_time_us")
        allocations = []
        allocated_time_us = 0
        for entry, occurrence_count in entries:
            numerator = profile_time_us * occurrence_count
            base_time_us = numerator // total_occurrences
            remainder = numerator % total_occurrences
            allocations.append((entry, base_time_us, remainder))
            allocated_time_us += base_time_us
        remainder_time_us = profile_time_us - allocated_time_us
        allocations.sort(key=lambda item: (-item[2], source_boundary_priority_sort_key(item[0])))
        for allocation_idx, (entry, base_time_us, _) in enumerate(allocations):
            extra_time_us = 1 if allocation_idx < remainder_time_us else 0
            entry["profile_allocations"][profile_key] = base_time_us + extra_time_us


def collect_capability_priority_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    runtime_by_key, _ = collect_capability_runtime_summary(out_dir, rows)
    profile_total_by_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_policy[entry["policy"]] += row_int(entry, "operator_time_us")

    summary = {}
    for region_entry in collect_region_decision_summary(out_dir, rows):
        if region_entry["status"] not in REGION_DECISION_STATUSES:
            continue
        pipeline_shape = attribution_pipeline_shape(region_entry)
        if is_trace_wrapper_pipeline_shape(pipeline_shape):
            continue
        count = row_int(region_entry, "count")
        seen_event_keys = set()
        for node in parse_pipeline_shape(pipeline_shape):
            if not is_gap_pipeline_node(node):
                continue
            capability_gap = classify_capability_gap(node)
            key = (
                region_entry["policy"],
                region_entry["status"],
                region_entry["execution_mode"],
                region_entry["region_execution_form"],
                capability_gap,
            )
            entry = summary.setdefault(
                key,
                {
                    "policy": region_entry["policy"],
                    "status": region_entry["status"],
                    "execution_mode": region_entry["execution_mode"],
                    "region_execution_form": region_entry["region_execution_form"],
                    "capability_gap": capability_gap,
                    "occurrences": 0,
                    "region_events": 0,
                    "queries": collections.Counter(),
                    "operators": collections.Counter(),
                    "boundaries": collections.Counter(),
                    "candidate_shapes": collections.Counter(),
                    "candidate_scopes": collections.Counter(),
                    "profile_keys": set(),
                    "max_estimated_cardinality": 0,
                    "example_pipeline_shape": "",
                    "example_reason": "",
                },
            )
            entry["occurrences"] += count
            if key not in seen_event_keys:
                entry["region_events"] += count
                seen_event_keys.add(key)
            entry["queries"][region_entry["query"]] += count
            entry["operators"][node["operator_name"]] += count
            entry["boundaries"][node["boundary"]] += count
            entry["candidate_shapes"][region_entry["candidate_shape"]] += count
            entry["candidate_scopes"][region_entry["candidate_scope"]] += count
            profile_key = (region_entry["query"], region_entry["policy"], node["operator_name"])
            if profile_key in profile_lookup:
                entry["profile_keys"].add(profile_key)
            entry["max_estimated_cardinality"] = max(
                entry["max_estimated_cardinality"], row_int(region_entry, "max_estimated_cardinality")
            )
            if not entry["example_pipeline_shape"]:
                entry["example_pipeline_shape"] = pipeline_shape
            if not entry["example_reason"]:
                entry["example_reason"] = region_entry["example_reason"]

    result = []
    for entry in summary.values():
        entry = dict(entry)
        profile_time_us = sum(row_int(profile_lookup[profile_key], "operator_time_us") for profile_key in entry["profile_keys"])
        policy_profile_time_us = profile_total_by_policy[entry["policy"]]
        runtime_key = (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["capability_gap"],
        )
        runtime_entry = runtime_by_key.get(runtime_key, {})
        entry["profile_time_us"] = profile_time_us
        entry["profile_percent_of_policy"] = format_percent(profile_time_us, policy_profile_time_us)
        for field in CAPABILITY_RUNTIME_FIELDS:
            entry[field] = row_int(runtime_entry, field) if entry["status"] == "compiled" else 0
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["operators"] = format_counter_examples(entry["operators"])
        entry["profile_operators"] = format_profile_operator_examples(entry["profile_keys"], profile_lookup)
        entry["boundaries"] = format_counter_examples(entry["boundaries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        del entry["queries"]
        del entry["profile_keys"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -row_int(entry, "profile_time_us"),
            -entry["occurrences"],
            entry["capability_gap"],
        )
    )
    return result


def collect_source_boundary_priority_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    profile_total_by_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_policy[entry["policy"]] += row_int(entry, "operator_time_us")

    summary = {}
    source_native_runtime_by_candidate = collect_source_native_runtime_by_candidate(out_dir, rows)
    for source_entry in iter_source_boundary_event_entries(out_dir, rows):
        runtime_key = source_runtime_candidate_key(source_entry["query"], source_entry["policy"], source_entry["event"])
        if source_entry["event"].get("status") == "compiled":
            source_entry["runtime"] = source_native_runtime_by_candidate.get(runtime_key)
        else:
            source_entry["runtime"] = None
        key = source_boundary_key(
            source_entry["policy"], source_entry["event"], source_entry["details"], source_entry["source_node"]
        )
        entry = summary.setdefault(
            key,
            new_source_boundary_summary_entry(
                source_entry["policy"], source_entry["event"], source_entry["details"], source_entry["source_node"]
            ),
        )
        if "profile_key_occurrences" not in entry:
            entry["profile_key_occurrences"] = collections.Counter()
        if "profile_allocations" not in entry:
            entry["profile_allocations"] = {}
        accumulate_source_boundary_entry(entry, source_entry)
        profile_key = (source_entry["query"], source_entry["policy"], source_entry["source_node"]["operator_name"])
        if profile_key in profile_lookup:
            entry["profile_key_occurrences"][profile_key] += 1

    allocate_source_boundary_profile_time(summary, profile_lookup)
    result = []
    for entry in summary.values():
        profile_time_us = sum(entry["profile_allocations"].values())
        policy_profile_time_us = profile_total_by_policy[entry["policy"]]
        profile_operators = format_profile_operator_allocations(entry["profile_allocations"])
        profile_key_count = len(entry["profile_allocations"])
        del entry["profile_key_occurrences"]
        del entry["profile_allocations"]
        entry = finalize_source_boundary_summary_entry(entry)
        entry["profile_time_us"] = profile_time_us
        entry["profile_percent_of_policy"] = format_percent(profile_time_us, policy_profile_time_us)
        entry["profile_operators"] = profile_operators
        entry["profile_key_count"] = profile_key_count
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -row_int(entry, "profile_time_us"),
            -row_int(entry, "occurrences"),
            entry["source_boundary_kind"],
            entry["source_execution"],
            entry["source_operator"],
            entry["scan_function"],
        )
    )
    for entry in result:
        del entry["profile_key_count"]
    return result


def collect_source_fusion_gap_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    profile_total_by_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_policy[entry["policy"]] += row_int(entry, "operator_time_us")

    summary = {}
    runtime_by_candidate = collect_region_runtime_by_candidate(out_dir, rows)
    for source_entry in iter_source_boundary_event_entries(out_dir, rows):
        event = source_entry["event"]
        trace_text = source_entry["trace_text"]
        if not is_source_fusion_gap_event(event, trace_text):
            continue
        source_node = source_entry["source_node"]
        details = source_entry["details"]
        native_source_contract = extract_native_source_contract(trace_text)
        source_execution = event_source_execution(event)
        source_gap = source_fusion_gap_kind(trace_text)
        key = (
            source_entry["policy"],
            event.get("status", ""),
            event.get("execution_mode", ""),
            event.get("region_execution_form", ""),
            source_gap,
            source_node["operator_name"],
            source_execution,
            native_source_contract["native_source_status"],
            native_source_contract["native_source_required_capability"],
            native_source_contract["native_source_protocol"],
            native_source_contract["native_source_blocker"],
            details["scan_function"],
        )
        entry = summary.setdefault(
            key,
            {
                "policy": source_entry["policy"],
                "status": event.get("status", ""),
                "execution_mode": event.get("execution_mode", ""),
                "region_execution_form": event.get("region_execution_form", ""),
                "source_fusion_gap": source_gap,
                "source_operator": source_node["operator_name"],
                "source_execution": source_execution,
                "native_source_status": native_source_contract["native_source_status"],
                "native_source_required_capability": native_source_contract["native_source_required_capability"],
                "native_source_protocol": native_source_contract["native_source_protocol"],
                "native_source_blocker": native_source_contract["native_source_blocker"],
                "scan_function": details["scan_function"],
                "candidate_shapes": collections.Counter(),
                "candidate_scopes": collections.Counter(),
                "admission_shape_keys": collections.Counter(),
                "profile_keys": set(),
                "runtime_input_rows": 0,
                "runtime_output_rows": 0,
                "runtime_invocations": 0,
                "runtime_time_us": 0,
                "source_native_output_rows": 0,
                "source_native_invocations": 0,
                "source_native_runtime_time_us": 0,
                "generated_body_runtime_time_us": 0,
                "occurrences": 0,
                "region_events": 0,
                "event_keys": set(),
                "runtime_keys": set(),
                "queries": collections.Counter(),
                "max_estimated_cardinality": 0,
                "example_pipeline_shape": "",
                "example_reason": "",
            },
        )
        event_identity = (source_entry["query"], source_entry["policy"], event.get("event_id", ""))
        entry["occurrences"] += 1
        if event_identity not in entry["event_keys"]:
            entry["region_events"] += 1
            entry["event_keys"].add(event_identity)
        entry["queries"][source_entry["query"]] += 1
        entry["candidate_shapes"][event.get("candidate_shape", "") or "none"] += 1
        entry["candidate_scopes"][event.get("candidate_scope", "") or "none"] += 1
        entry["admission_shape_keys"][event.get("admission_shape_key", "") or "none"] += 1
        entry["max_estimated_cardinality"] = max(
            entry["max_estimated_cardinality"], row_int(event, "candidate_estimated_cardinality")
        )
        runtime_key = fusion_blocker_runtime_key(source_entry["query"], source_entry["policy"], event)
        runtime = runtime_by_candidate.get(runtime_key)
        if runtime and runtime_key not in entry["runtime_keys"]:
            entry["runtime_keys"].add(runtime_key)
            entry["runtime_input_rows"] += row_int(runtime, "input_rows")
            entry["runtime_output_rows"] += row_int(runtime, "output_rows")
            entry["runtime_invocations"] += row_int(runtime, "invocations")
            entry["runtime_time_us"] += row_int(runtime, "runtime_time_us")
            entry["source_native_output_rows"] += row_int(runtime, "source_native_output_rows")
            entry["source_native_invocations"] += row_int(runtime, "source_native_invocations")
            entry["source_native_runtime_time_us"] += row_int(runtime, "source_native_runtime_time_us")
            entry["generated_body_runtime_time_us"] += row_int(runtime, "generated_body_runtime_time_us")
        profile_key = (source_entry["query"], source_entry["policy"], source_node["operator_name"])
        if profile_key in profile_lookup:
            entry["profile_keys"].add(profile_key)
        if not entry["example_pipeline_shape"]:
            entry["example_pipeline_shape"] = source_entry["pipeline_shape"]
        if not entry["example_reason"]:
            entry["example_reason"] = source_fusion_gap_example_text(
                event.get("reason", ""), trace_text, source_entry["details"]["source_marker"]
            )

    result = []
    for entry in summary.values():
        entry = dict(entry)
        profile_time_us = sum(row_int(profile_lookup[profile_key], "operator_time_us") for profile_key in entry["profile_keys"])
        policy_profile_time_us = profile_total_by_policy[entry["policy"]]
        entry["profile_time_us"] = profile_time_us
        entry["profile_percent_of_policy"] = format_percent(profile_time_us, policy_profile_time_us)
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        entry["admission_shape_keys"] = format_counter_examples(entry["admission_shape_keys"])
        del entry["profile_keys"]
        del entry["event_keys"]
        del entry["runtime_keys"]
        del entry["queries"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -row_int(entry, "profile_time_us"),
            -row_int(entry, "occurrences"),
            entry["source_operator"],
            entry["native_source_required_capability"],
            entry["native_source_blocker"],
            entry["scan_function"],
        )
    )
    return result


def iter_region_decision_event_entries(out_dir: Path, rows: list):
    for summary_row in rows:
        policy = summary_row["policy"]
        if policy == "off":
            continue
        events_csv = summary_row.get("events_csv")
        if not events_csv:
            continue
        events_path = out_dir / events_csv
        if not events_path.exists():
            continue
        with events_path.open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                if event.get("target") != "region" or event.get("phase") not in ("decision", "compile"):
                    continue
                pipeline_shape = attribution_pipeline_shape(
                    {
                        "status": event.get("status", ""),
                        "candidate_pipeline_shape": event.get("candidate_pipeline_shape", ""),
                        "candidate_context_pipeline_shape": event.get("candidate_context_pipeline_shape", ""),
                    }
                )
                if is_trace_wrapper_pipeline_shape(pipeline_shape):
                    continue
                yield {
                    "query": summary_row["query"],
                    "policy": policy,
                    "event": event,
                    "pipeline_shape": pipeline_shape,
                    "trace_text": event.get("reason", "") + " " + event.get("ir", ""),
                }


def fusion_blocker_runtime_key(query: str, policy: str, event: dict) -> tuple:
    return (
        query,
        policy,
        event.get("execution_mode", ""),
        event.get("region_execution_form", "") or "none",
        event.get("candidate_shape", "") or "none",
        event.get("candidate_pipeline_shape", "") or "none",
        event.get("candidate_context_pipeline_shape", "") or "none",
        event.get("candidate_scope", "") or "none",
    )


def collect_region_runtime_by_candidate(out_dir: Path, rows: list) -> dict:
    return {
        (
            entry["query"],
            entry["policy"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        ): entry
        for entry in collect_kernel_runtime_summary(out_dir, rows)
    }


def collect_fusion_blocker_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    profile_total_by_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_policy[entry["policy"]] += row_int(entry, "operator_time_us")
    runtime_by_candidate = collect_region_runtime_by_candidate(out_dir, rows)

    summary = {}
    for region_entry in iter_region_decision_event_entries(out_dir, rows):
        event = region_entry["event"]
        reason = event.get("reason", "")
        trace_text = region_entry["trace_text"]
        blockers = [match.group(1) for match in FUSION_BLOCKER_RE.finditer(reason)]
        if not blockers:
            continue
        native_source_contract = extract_native_source_contract(trace_text)
        for blocker in blockers:
            blocker_class = blocker.split(":", 1)[0]
            source_execution = event_source_execution(event)
            key = (
                region_entry["policy"],
                event.get("status", ""),
                event.get("execution_mode", ""),
                event.get("region_execution_form", "") or "none",
                blocker,
                blocker_class,
                event.get("candidate_source_kind", ""),
                source_execution,
                native_source_contract["native_source_status"],
                native_source_contract["native_source_required_capability"],
                native_source_contract["native_source_protocol"],
                native_source_contract["native_source_blocker"],
                event.get("candidate_sink_kind", ""),
            )
            entry = summary.setdefault(
                key,
                {
                    "policy": region_entry["policy"],
                    "status": event.get("status", ""),
                    "execution_mode": event.get("execution_mode", ""),
                    "region_execution_form": event.get("region_execution_form", "") or "none",
                    "fusion_blocker": blocker,
                    "blocker_class": blocker_class,
                    "source_kind": event.get("candidate_source_kind", ""),
                    "source_execution": source_execution,
                    "native_source_status": native_source_contract["native_source_status"],
                    "native_source_required_capability": native_source_contract["native_source_required_capability"],
                    "native_source_protocol": native_source_contract["native_source_protocol"],
                    "native_source_blocker": native_source_contract["native_source_blocker"],
                    "sink_kind": event.get("candidate_sink_kind", ""),
                    "candidate_shapes": collections.Counter(),
                    "candidate_scopes": collections.Counter(),
                    "admission_shape_keys": collections.Counter(),
                    "profile_keys": set(),
                    "runtime_keys": set(),
                    "event_keys": set(),
                    "queries": collections.Counter(),
                    "max_estimated_cardinality": 0,
                    "example_pipeline_shape": "",
                    "example_reason": "",
                },
            )
            event_identity = (region_entry["query"], region_entry["policy"], event.get("event_id", ""), blocker)
            entry["occurrences"] = entry.get("occurrences", 0) + 1
            if event_identity not in entry["event_keys"]:
                entry["event_keys"].add(event_identity)
            entry["queries"][region_entry["query"]] += 1
            entry["candidate_shapes"][event.get("candidate_shape", "") or "none"] += 1
            entry["candidate_scopes"][event.get("candidate_scope", "") or "none"] += 1
            entry["admission_shape_keys"][event.get("admission_shape_key", "") or "none"] += 1
            entry["max_estimated_cardinality"] = max(
                entry["max_estimated_cardinality"], row_int(event, "candidate_estimated_cardinality")
            )
            for node in parse_pipeline_shape(region_entry["pipeline_shape"]):
                if is_trace_wrapper_node(node):
                    continue
                profile_key = (region_entry["query"], region_entry["policy"], node["operator_name"])
                if profile_key in profile_lookup:
                    entry["profile_keys"].add(profile_key)
            runtime_key = fusion_blocker_runtime_key(region_entry["query"], region_entry["policy"], event)
            if runtime_key in runtime_by_candidate:
                entry["runtime_keys"].add(runtime_key)
            if not entry["example_pipeline_shape"]:
                entry["example_pipeline_shape"] = region_entry["pipeline_shape"]
            if not entry["example_reason"]:
                entry["example_reason"] = fusion_blocker_example_text(reason, blocker)

    result = []
    for entry in summary.values():
        entry = dict(entry)
        profile_time_us = sum(row_int(profile_lookup[profile_key], "operator_time_us") for profile_key in entry["profile_keys"])
        policy_profile_time_us = profile_total_by_policy[entry["policy"]]
        runtime_rows = [runtime_by_candidate[runtime_key] for runtime_key in entry["runtime_keys"]]
        entry["profile_time_us"] = profile_time_us
        entry["profile_percent_of_policy"] = format_percent(profile_time_us, policy_profile_time_us)
        entry["profile_operators"] = format_profile_operator_examples(entry["profile_keys"], profile_lookup)
        entry["runtime_input_rows"] = sum(row_int(runtime, "input_rows") for runtime in runtime_rows)
        entry["runtime_output_rows"] = sum(row_int(runtime, "output_rows") for runtime in runtime_rows)
        entry["runtime_invocations"] = sum(row_int(runtime, "invocations") for runtime in runtime_rows)
        entry["runtime_time_us"] = sum(row_int(runtime, "runtime_time_us") for runtime in runtime_rows)
        entry["source_native_output_rows"] = sum(row_int(runtime, "source_native_output_rows") for runtime in runtime_rows)
        entry["source_native_invocations"] = sum(row_int(runtime, "source_native_invocations") for runtime in runtime_rows)
        entry["source_native_runtime_time_us"] = sum(
            row_int(runtime, "source_native_runtime_time_us") for runtime in runtime_rows
        )
        entry["generated_body_runtime_time_us"] = sum(
            row_int(runtime, "generated_body_runtime_time_us") for runtime in runtime_rows
        )
        entry["region_events"] = len(entry["event_keys"])
        entry["query_count"] = len(entry["queries"])
        entry["query_examples"] = format_query_examples(entry["queries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        entry["admission_shape_keys"] = format_counter_examples(entry["admission_shape_keys"])
        del entry["profile_keys"]
        del entry["runtime_keys"]
        del entry["event_keys"]
        del entry["queries"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["blocker_class"],
            -row_int(entry, "profile_time_us"),
            -row_int(entry, "occurrences"),
            entry["fusion_blocker"],
        )
    )
    return result


def collect_query_capability_priority_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    _, runtime_by_query_key = collect_capability_runtime_summary(out_dir, rows)
    profile_total_by_query_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_query_policy[(entry["query"], entry["policy"])] += row_int(entry, "operator_time_us")

    summary = {}
    for region_entry in collect_region_decision_summary(out_dir, rows):
        if region_entry["status"] not in REGION_DECISION_STATUSES:
            continue
        pipeline_shape = attribution_pipeline_shape(region_entry)
        if is_trace_wrapper_pipeline_shape(pipeline_shape):
            continue
        count = row_int(region_entry, "count")
        seen_event_keys = set()
        for node in parse_pipeline_shape(pipeline_shape):
            if not is_gap_pipeline_node(node):
                continue
            capability_gap = classify_capability_gap(node)
            key = (
                region_entry["query"],
                region_entry["policy"],
                region_entry["status"],
                region_entry["execution_mode"],
                region_entry["region_execution_form"],
                capability_gap,
            )
            entry = summary.setdefault(
                key,
                {
                    "query": region_entry["query"],
                    "policy": region_entry["policy"],
                    "status": region_entry["status"],
                    "execution_mode": region_entry["execution_mode"],
                    "region_execution_form": region_entry["region_execution_form"],
                    "capability_gap": capability_gap,
                    "occurrences": 0,
                    "region_events": 0,
                    "operators": collections.Counter(),
                    "boundaries": collections.Counter(),
                    "candidate_shapes": collections.Counter(),
                    "candidate_scopes": collections.Counter(),
                    "profile_keys": set(),
                    "max_estimated_cardinality": 0,
                    "example_pipeline_shape": "",
                    "example_reason": "",
                },
            )
            entry["occurrences"] += count
            if key not in seen_event_keys:
                entry["region_events"] += count
                seen_event_keys.add(key)
            entry["operators"][node["operator_name"]] += count
            entry["boundaries"][node["boundary"]] += count
            entry["candidate_shapes"][region_entry["candidate_shape"]] += count
            entry["candidate_scopes"][region_entry["candidate_scope"]] += count
            profile_key = (region_entry["query"], region_entry["policy"], node["operator_name"])
            if profile_key in profile_lookup:
                entry["profile_keys"].add(profile_key)
            entry["max_estimated_cardinality"] = max(
                entry["max_estimated_cardinality"], row_int(region_entry, "max_estimated_cardinality")
            )
            if not entry["example_pipeline_shape"]:
                entry["example_pipeline_shape"] = pipeline_shape
            if not entry["example_reason"]:
                entry["example_reason"] = region_entry["example_reason"]

    result = []
    for entry in summary.values():
        entry = dict(entry)
        profile_time_us = sum(row_int(profile_lookup[profile_key], "operator_time_us") for profile_key in entry["profile_keys"])
        query_policy_profile_time_us = profile_total_by_query_policy[(entry["query"], entry["policy"])]
        runtime_key = (
            entry["query"],
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["capability_gap"],
        )
        runtime_entry = runtime_by_query_key.get(runtime_key, {})
        entry["profile_time_us"] = profile_time_us
        entry["profile_percent_of_query_policy"] = format_percent(profile_time_us, query_policy_profile_time_us)
        for field in CAPABILITY_RUNTIME_FIELDS:
            entry[field] = row_int(runtime_entry, field) if entry["status"] == "compiled" else 0
        entry["operators"] = format_counter_examples(entry["operators"])
        entry["profile_operators"] = format_profile_operator_examples(entry["profile_keys"], profile_lookup)
        entry["boundaries"] = format_counter_examples(entry["boundaries"])
        entry["candidate_shapes"] = format_counter_examples(entry["candidate_shapes"])
        entry["candidate_scopes"] = format_counter_examples(entry["candidate_scopes"])
        del entry["profile_keys"]
        result.append(entry)
    result.sort(
        key=lambda entry: (
            entry["query"],
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -row_int(entry, "profile_time_us"),
            -entry["occurrences"],
            entry["capability_gap"],
        )
    )
    return result


def collect_pipeline_runtime_summary(out_dir: Path, rows: list) -> list:
    profile_rows = collect_operator_profile_summary(out_dir, rows)
    profile_lookup = {
        (entry["query"], entry["policy"], entry["operator_name"]): entry
        for entry in profile_rows
    }
    profile_total_by_policy = collections.Counter()
    for entry in profile_rows:
        profile_total_by_policy[entry["policy"]] += row_int(entry, "operator_time_us")

    runtime_by_key = {}
    for kernel_entry in collect_kernel_runtime_summary(out_dir, rows):
        key = (
            kernel_entry["policy"],
            kernel_entry["execution_mode"],
            kernel_entry["region_execution_form"],
            kernel_entry["candidate_shape"],
            kernel_entry["candidate_pipeline_shape"],
            kernel_entry["candidate_context_pipeline_shape"],
            kernel_entry["candidate_scope"],
        )
        entry = runtime_by_key.setdefault(
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
        entry["compiled_kernels"] += row_int(kernel_entry, "kernels")
        entry["reached_kernels"] += row_int(kernel_entry, "reached_kernels")
        entry["row_processing_kernels"] += row_int(kernel_entry, "row_processing_kernels")
        entry["unreached_kernels"] += row_int(kernel_entry, "unreached_kernels")
        entry["zero_input_kernels"] += row_int(kernel_entry, "zero_input_kernels")
        entry["runtime_input_rows"] += row_int(kernel_entry, "input_rows")
        entry["runtime_output_rows"] += row_int(kernel_entry, "output_rows")
        entry["runtime_invocations"] += row_int(kernel_entry, "invocations")
        entry["runtime_time_us"] += row_int(kernel_entry, "runtime_time_us")
        entry["source_native_output_rows"] += row_int(kernel_entry, "source_native_output_rows")
        entry["source_native_invocations"] += row_int(kernel_entry, "source_native_invocations")
        entry["source_native_runtime_time_us"] += row_int(kernel_entry, "source_native_runtime_time_us")
        entry["generated_body_runtime_time_us"] += row_int(kernel_entry, "generated_body_runtime_time_us")

    summary = {}
    for region_entry in collect_region_decision_summary(out_dir, rows):
        if region_entry["status"] not in REGION_DECISION_STATUSES:
            continue
        if is_trace_wrapper_region(region_entry):
            continue
        key = (
            region_entry["policy"],
            region_entry["status"],
            region_entry["execution_mode"],
            region_entry["region_execution_form"],
            region_entry["candidate_shape"],
            region_entry["candidate_pipeline_shape"],
            region_entry["candidate_context_pipeline_shape"],
            region_entry["candidate_scope"],
        )
        entry = summary.setdefault(
            key,
            {
                "policy": region_entry["policy"],
                "status": region_entry["status"],
                "execution_mode": region_entry["execution_mode"],
                "region_execution_form": region_entry["region_execution_form"],
                "candidate_shape": region_entry["candidate_shape"],
                "candidate_pipeline_shape": region_entry["candidate_pipeline_shape"],
                "candidate_context_pipeline_shape": region_entry["candidate_context_pipeline_shape"],
                "candidate_scope": region_entry["candidate_scope"],
                "region_events": 0,
                "queries": collections.Counter(),
                "max_estimated_cardinality": 0,
                "decision_time_us": 0,
                "compile_time_us": 0,
                "code_size": 0,
                "profile_keys": set(),
                "capability_gaps": collections.Counter(),
                "example_reason": "",
            },
        )
        count = row_int(region_entry, "count")
        entry["region_events"] += count
        entry["queries"][region_entry["query"]] += count
        entry["max_estimated_cardinality"] = max(
            entry["max_estimated_cardinality"], row_int(region_entry, "max_estimated_cardinality")
        )
        entry["decision_time_us"] += row_int(region_entry, "decision_time_us")
        entry["compile_time_us"] += row_int(region_entry, "compile_time_us")
        entry["code_size"] += row_int(region_entry, "code_size")
        for field in STAGE_FIELDS:
            entry[field] = entry.get(field, 0) + row_int(region_entry, field)
        if not entry["example_reason"]:
            entry["example_reason"] = region_entry["example_reason"]

        for node in parse_pipeline_shape(attribution_pipeline_shape(region_entry)):
            if is_trace_wrapper_node(node):
                continue
            profile_key = (region_entry["query"], region_entry["policy"], node["operator_name"])
            if profile_key in profile_lookup:
                entry["profile_keys"].add(profile_key)
            if is_gap_pipeline_node(node):
                capability_gap = classify_capability_gap(node)
                entry["capability_gaps"][capability_gap] += count

    result = []
    for entry in summary.values():
        entry = dict(entry)
        stage_total = sum(row_int(entry, field) for field in STAGE_FIELDS)
        dominant_stage = max(STAGE_FIELDS, key=lambda field: row_int(entry, field))
        runtime_key = (
            entry["policy"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
        runtime_entry = runtime_by_key.get(runtime_key, {})
        profile_time_us = sum(row_int(profile_lookup[profile_key], "operator_time_us") for profile_key in entry["profile_keys"])
        policy_profile_time_us = profile_total_by_policy[entry["policy"]]
        result.append(
            {
                "policy": entry["policy"],
                "status": entry["status"],
                "execution_mode": entry["execution_mode"],
                "region_execution_form": entry["region_execution_form"],
                "candidate_shape": entry["candidate_shape"],
                "candidate_pipeline_shape": entry["candidate_pipeline_shape"],
                "candidate_context_pipeline_shape": entry["candidate_context_pipeline_shape"],
                "candidate_scope": entry["candidate_scope"],
                "region_events": entry["region_events"],
                "query_count": len(entry["queries"]),
                "query_examples": format_query_examples(entry["queries"]),
                "max_estimated_cardinality": entry["max_estimated_cardinality"],
                "decision_time_us": entry["decision_time_us"],
                "compile_time_us": entry["compile_time_us"],
                "stage_total_time_us": stage_total,
                "dominant_stage": dominant_stage,
                "dominant_stage_time_us": row_int(entry, dominant_stage),
                "ir_lowering_time_us": row_int(entry, "ir_lowering_time_us"),
                "backend_analysis_time_us": row_int(entry, "backend_analysis_time_us"),
                "admission_time_us": row_int(entry, "admission_time_us"),
                "overlap_check_time_us": row_int(entry, "overlap_check_time_us"),
                "codegen_time_us": row_int(entry, "codegen_time_us"),
                "code_size": entry["code_size"],
                "compiled_kernels": row_int(runtime_entry, "compiled_kernels") if entry["status"] == "compiled" else 0,
                "reached_kernels": row_int(runtime_entry, "reached_kernels") if entry["status"] == "compiled" else 0,
                "row_processing_kernels": row_int(runtime_entry, "row_processing_kernels")
                if entry["status"] == "compiled"
                else 0,
                "unreached_kernels": row_int(runtime_entry, "unreached_kernels") if entry["status"] == "compiled" else 0,
                "zero_input_kernels": row_int(runtime_entry, "zero_input_kernels") if entry["status"] == "compiled" else 0,
                "runtime_input_rows": row_int(runtime_entry, "runtime_input_rows")
                if entry["status"] == "compiled"
                else 0,
                "runtime_output_rows": row_int(runtime_entry, "runtime_output_rows")
                if entry["status"] == "compiled"
                else 0,
                "runtime_invocations": row_int(runtime_entry, "runtime_invocations")
                if entry["status"] == "compiled"
                else 0,
                "runtime_time_us": row_int(runtime_entry, "runtime_time_us") if entry["status"] == "compiled" else 0,
                "source_native_output_rows": row_int(runtime_entry, "source_native_output_rows")
                if entry["status"] == "compiled"
                else 0,
                "source_native_invocations": row_int(runtime_entry, "source_native_invocations")
                if entry["status"] == "compiled"
                else 0,
                "source_native_runtime_time_us": row_int(runtime_entry, "source_native_runtime_time_us")
                if entry["status"] == "compiled"
                else 0,
                "generated_body_runtime_time_us": row_int(runtime_entry, "generated_body_runtime_time_us")
                if entry["status"] == "compiled"
                else 0,
                "profile_time_us": profile_time_us,
                "profile_percent_of_policy": format_percent(profile_time_us, policy_profile_time_us),
                "profile_operators": format_profile_operator_examples(entry["profile_keys"], profile_lookup),
                "capability_gaps": format_counter_examples(entry["capability_gaps"]),
                "example_reason": entry["example_reason"],
            }
        )
    result.sort(
        key=lambda entry: (
            entry["policy"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            -row_int(entry, "profile_time_us"),
            -row_int(entry, "runtime_input_rows"),
            -row_int(entry, "region_events"),
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            entry["candidate_scope"],
        )
    )
    return result


def format_query_examples(query_counter: collections.Counter, limit: int = 5) -> str:
    examples = []
    for query_id, count in sorted(query_counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        if count == 1:
            examples.append(f"q{query_id}")
        else:
            examples.append(f"q{query_id}({count})")
    return ", ".join(examples)


def explain_total_time_seconds(output: str) -> float:
    match = TOTAL_TIME_RE.search(output)
    if not match:
        raise RuntimeError(f"could not find EXPLAIN ANALYZE total time in output:\n{output}")
    return float(match.group(1))


def setting_sql(args: argparse.Namespace, policy: str) -> str:
    dump_ir = "true" if args.dump_ir else "false"
    trace_runtime = "true" if args.trace_runtime else "false"
    jit_verify = "true" if args.jit_verify else "false"
    return f"""
LOAD {args.jit_extension};
SET threads={args.threads};
SET enable_jit=true;
SET jit_backend={sql_quote(args.backend)};
SET jit_policy={sql_quote(policy)};
SET jit_verify={jit_verify};
SET jit_dump_ir={dump_ir};
SET jit_trace_runtime={trace_runtime};
SET jit_trace_decisions=true;
SET jit_event_log_size={args.event_log_size};
"""


def create_tpch_database(args: argparse.Namespace, db_path: Path) -> None:
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
LOAD tpch;
CALL dbgen(sf={args.scale_factor});
""",
        "TPC-H dbgen",
    )


def validate_tpch_database(args: argparse.Namespace, db_path: Path) -> None:
    checks = "\n".join(f"SELECT 1 FROM {table_name} LIMIT 0;" for table_name in TPCH_TABLES)
    run_duckdb(args.duckdb, db_path, checks, "TPC-H schema validation")


def prepare_tpch_database(args: argparse.Namespace) -> tuple[Path, Optional[Path]]:
    if args.use_existing_db:
        if args.db is None:
            raise TraceConfigurationError(
                "--use-existing-db requires --db so the trace harness knows which database to reuse"
            )
        db_path = args.db.resolve()
        if not db_path.exists():
            raise TraceConfigurationError(f"--use-existing-db database does not exist: {db_path}")
        try:
            validate_tpch_database(args, db_path)
        except RuntimeError as exc:
            raise TraceConfigurationError(
                f"--use-existing-db database is not a valid TPC-H database: {db_path}\n{exc}"
            ) from None
        return db_path, None

    if args.db is not None:
        db_path = args.db.resolve()
        if db_path.exists():
            raise TraceConfigurationError(
                f"--db already exists: {db_path}; pass --use-existing-db to reuse it or choose a new path"
            )
        db_path.parent.mkdir(parents=True, exist_ok=True)
        create_tpch_database(args, db_path)
        validate_tpch_database(args, db_path)
        return db_path, None

    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_jit_tpch_trace_"))
    db_path = temp_dir / "tpch.duckdb"
    create_tpch_database(args, db_path)
    validate_tpch_database(args, db_path)
    return db_path, temp_dir


def create_baseline(args: argparse.Namespace, db_path: Path, query_id: str, query_sql: str) -> None:
    table_name = f"__jit_trace_baseline_q{query_id}"
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, "off")}
CREATE OR REPLACE TABLE {table_name} AS
{query_sql};
""",
        f"baseline q{query_id}",
    )


def run_policy_trace(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    query_id: str,
    query_sql: str,
    policy: str,
) -> dict:
    baseline_table = f"__jit_trace_baseline_q{query_id}"
    result_table = f"__jit_trace_result_q{query_id}_{policy}"
    event_summary_path = out_dir / f"q{query_id}_{policy}_event_summary.csv"
    correctness_path = out_dir / f"q{query_id}_{policy}_correctness.csv"
    events_path = out_dir / f"q{query_id}_{policy}_events.csv"
    counters_path = out_dir / f"q{query_id}_{policy}_counters.csv"
    decision_counters_path = out_dir / f"q{query_id}_{policy}_decision_counters.csv"
    kernel_counters_path = out_dir / f"q{query_id}_{policy}_kernel_counters.csv"
    profile_json_path = out_dir / f"q{query_id}_{policy}_profile.json"

    event_summary_select = """
SELECT
    count(*) AS event_count,
    count(*) FILTER (
        WHERE phase='compile' AND target='region' AND status='compiled'
    ) AS compiled_regions,
    count(*) FILTER (
        WHERE target <> 'region'
    ) AS non_region_events,
    count(*) FILTER (
        WHERE target='region' AND status='skipped'
    ) AS skipped_regions,
    count(*) FILTER (
        WHERE target='region' AND status='unsupported'
    ) AS unsupported_regions,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native' AND code_size=0
          AND reason NOT LIKE '%kernel=native-operator-loop%'
    ) AS zero_code_native_compile_events,
    count(*) FILTER (
        WHERE phase='runtime'
    ) AS runtime_events,
    coalesce(sum(input_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_input_rows,
    coalesce(sum(output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_output_rows,
    coalesce(sum(invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_invocations,
    coalesce(sum(runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_time_us,
    coalesce(sum(source_native_output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_output_rows,
    coalesce(sum(source_native_invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_invocations,
    coalesce(sum(source_native_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_runtime_time_us,
    coalesce(sum(generated_body_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS generated_body_runtime_time_us,
    coalesce(sum(fused_prepare_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS fused_prepare_runtime_time_us,
    coalesce(sum(fused_group_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS fused_group_runtime_time_us,
    coalesce(sum(fused_state_bind_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS fused_state_bind_runtime_time_us,
    coalesce(sum(fused_update_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS fused_update_runtime_time_us,
    coalesce(sum(fused_finish_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS fused_finish_runtime_time_us,
    coalesce(sum(ir_lowering_time_us), 0) AS ir_lowering_time_us,
    coalesce(sum(backend_analysis_time_us), 0) AS backend_analysis_time_us,
    coalesce(sum(admission_time_us), 0) AS admission_time_us,
    coalesce(sum(overlap_check_time_us), 0) AS overlap_check_time_us,
    coalesce(sum(codegen_time_us), 0) AS codegen_time_us,
    coalesce(max(code_size) FILTER (
        WHERE phase='compile'
    ), 0) AS max_compile_code_size,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native'
    ) AS native_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode<>'native'
    ) AS non_native_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='executor_fallback'
    ) AS executor_fallback_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='unsupported'
    ) AS unsupported_compile_events
FROM duckdb_jit_events()
"""

    events_select = """
SELECT
    event_id,
    phase,
    backend_name,
    target,
    status,
    execution_mode,
    region_execution_form,
    selected_source_execution,
    policy_decision,
    candidate_id,
    candidate_shape,
    candidate_pipeline_shape,
    candidate_context_pipeline_shape,
    candidate_scope,
    candidate_contract_abi,
    candidate_contract_first_node,
    candidate_contract_node_count,
    candidate_contract_start_operator_index,
    candidate_contract_end_operator_index,
    candidate_owns_source,
    candidate_owns_transform,
    candidate_owns_sink,
    candidate_owns_state_scan,
    candidate_node_count,
    candidate_start_operator_index,
    candidate_end_operator_index,
    candidate_estimated_cardinality,
    candidate_has_source,
    candidate_has_sink,
    candidate_source_kind,
    candidate_source_execution,
    candidate_sink_kind,
    candidate_has_table_scan_source,
    candidate_has_stateful_source,
    candidate_expression_traits_known,
    candidate_source_filter_count,
    candidate_source_filter_expression_count,
    candidate_source_filter_fallback_count,
    candidate_source_comparison_filter_count,
    candidate_source_integer_comparison_filter_count,
    candidate_source_non_integer_comparison_filter_count,
    candidate_source_conjunction_filter_count,
    candidate_source_projected_column_count,
    candidate_source_returned_column_count,
    candidate_filter_count,
    candidate_projection_count,
    candidate_operator_count,
    candidate_core_expression_operator_count,
    candidate_arithmetic_projection_count,
    candidate_integer_arithmetic_projection_count,
    candidate_non_integer_arithmetic_projection_count,
    candidate_reference_projection_count,
    candidate_comparison_filter_count,
    candidate_integer_comparison_filter_count,
    candidate_non_integer_comparison_filter_count,
    candidate_conjunction_filter_count,
    candidate_expression_fallback_count,
    candidate_operator_fallback_count,
    candidate_scan_boundary_count,
    candidate_sink_boundary_count,
    candidate_source_ownership,
    candidate_state_scan_ownership,
    candidate_transform_ownership,
    candidate_sink_ownership,
    candidate_executor_boundary_free,
    candidate_native_fusion_ready,
    candidate_generated_operator_count,
    candidate_source_boundary_count,
    candidate_executor_boundary_count,
    candidate_missing_protocol_count,
    candidate_required_capabilities,
    candidate_fusion_blockers,
    admission_shape_key,
    admission_rule_present,
    admission_min_cardinality,
    admission_score,
    admission_proof,
    reason,
    decision_time_us,
    compile_time_us,
    code_size,
    kernel_id,
    input_rows,
    output_rows,
    invocation_count,
    runtime_time_us,
    runtime_result,
    source_native_output_rows,
    source_native_invocation_count,
    source_native_runtime_time_us,
    generated_body_runtime_time_us,
    fused_prepare_runtime_time_us,
    fused_group_runtime_time_us,
    fused_state_bind_runtime_time_us,
    fused_update_runtime_time_us,
    fused_finish_runtime_time_us,
    generated_body_flat_input_rows,
    generated_body_flat_invocation_count,
    generated_body_shared_selection_input_rows,
    generated_body_shared_selection_invocation_count,
    generated_body_selection_input_rows,
    generated_body_selection_invocation_count,
    native_operator_loop_input_rows,
    native_operator_loop_invocation_count,
    ir_lowering_time_us,
    backend_analysis_time_us,
    admission_time_us,
    overlap_check_time_us,
    codegen_time_us,
    ir
FROM duckdb_jit_events()
ORDER BY event_id
"""

    explain = run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, policy)}
SELECT * FROM duckdb_jit_clear_events();
SELECT * FROM duckdb_jit_clear_counters();
EXPLAIN ANALYZE
{query_sql};
{copy_statement(event_summary_select, event_summary_path)}
{copy_statement(events_select, events_path)}
{copy_statement("SELECT * FROM duckdb_jit_counters() ORDER BY backend_name, target, status, execution_mode, region_execution_form, policy_decision", counters_path)}
{copy_statement("SELECT * FROM duckdb_jit_decision_counters() ORDER BY backend_name, target, phase, status, execution_mode, region_execution_form, policy_decision, candidate_scope, candidate_shape, admission_shape_key", decision_counters_path)}
{copy_statement("SELECT * FROM duckdb_jit_kernel_counters() ORDER BY kernel_id", kernel_counters_path)}
""",
        f"explain/events q{query_id} {policy}",
    )
    total_time_s = explain_total_time_seconds(explain.stdout)

    if profile_json_path.exists():
        profile_json_path.unlink()
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, policy)}
PRAGMA enable_profiling=json;
PRAGMA profiling_mode=detailed;
PRAGMA profiling_output={sql_quote(profile_json_path)};
CREATE OR REPLACE TABLE {result_table} AS
{query_sql};
""",
        f"result q{query_id} {policy}",
    )
    profile = read_profile_json(profile_json_path)
    run_copy_query(
        args.duckdb,
        db_path,
        f"""
SELECT
    (SELECT count(*) FROM {baseline_table}) AS baseline_rows,
    (SELECT count(*) FROM {result_table}) AS result_rows,
    (SELECT count(*) FROM (
        SELECT * FROM {result_table}
        EXCEPT ALL
        SELECT * FROM {baseline_table}
    )) AS result_minus_baseline,
    (SELECT count(*) FROM (
        SELECT * FROM {baseline_table}
        EXCEPT ALL
        SELECT * FROM {result_table}
    )) AS baseline_minus_result
""",
        correctness_path,
        f"correctness q{query_id} {policy}",
    )
    run_duckdb(args.duckdb, db_path, f"DROP TABLE IF EXISTS {result_table};", f"drop result q{query_id} {policy}")

    event_summary = read_single_csv_row(event_summary_path)
    event_summary.update(read_region_decision_totals(decision_counters_path))
    correctness = read_single_csv_row(correctness_path)
    return {
        "query": query_id,
        "policy": policy,
        "total_time_s": f"{total_time_s:.6f}",
        **event_summary,
        **correctness,
        "event_summary_csv": event_summary_path.name,
        "correctness_csv": correctness_path.name,
        "events_csv": events_path.name,
        "counters_csv": counters_path.name,
        "decision_counters_csv": decision_counters_path.name,
        "kernel_counters_csv": kernel_counters_path.name,
        "profile_json": profile_json_path.name,
        "profile_operator_count": profile_operator_count(profile),
        "profile_operator_time_us": profile_operator_time_us(profile),
        "profile_query_time_us": profile_query_time_us(profile),
        "profile_cpu_time_us": profile_cpu_time_us(profile),
    }


def write_summary(out_dir: Path, rows: list) -> None:
    if not rows:
        return
    summary_path = out_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_csv_rows(path: Path, fieldnames: tuple, rows: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def collect_decision_counter_summary(out_dir: Path, rows: list) -> list:
    result = []
    for summary_row in rows:
        decision_counters_csv = summary_row.get("decision_counters_csv")
        if not decision_counters_csv:
            continue
        decision_counters_path = out_dir / decision_counters_csv
        if not decision_counters_path.exists():
            continue
        with decision_counters_path.open(newline="", encoding="utf-8") as handle:
            for counter in csv.DictReader(handle):
                entry = {
                    "query": summary_row["query"],
                    "policy": summary_row["policy"],
                }
                for field in DECISION_COUNTER_SUMMARY_FIELDS:
                    if field in entry:
                        continue
                    entry[field] = counter.get(field, "")
                result.append(entry)
    return result


def write_aggregate_csvs(out_dir: Path, rows: list) -> None:
    write_csv_rows(
        out_dir / "query_gap_summary.csv",
        QUERY_GAP_SUMMARY_FIELDS,
        collect_query_gap_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "operator_gap_summary.csv",
        OPERATOR_GAP_SUMMARY_FIELDS,
        collect_operator_gap_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "capability_gap_summary.csv",
        CAPABILITY_GAP_SUMMARY_FIELDS,
        collect_capability_gap_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "capability_priority_summary.csv",
        CAPABILITY_PRIORITY_SUMMARY_FIELDS,
        collect_capability_priority_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "query_capability_priority_summary.csv",
        QUERY_CAPABILITY_PRIORITY_SUMMARY_FIELDS,
        collect_query_capability_priority_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "expression_fallback_summary.csv",
        EXPRESSION_FALLBACK_SUMMARY_FIELDS,
        collect_expression_fallback_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "source_boundary_summary.csv",
        SOURCE_BOUNDARY_SUMMARY_FIELDS,
        collect_source_boundary_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "source_boundary_priority_summary.csv",
        SOURCE_BOUNDARY_PRIORITY_SUMMARY_FIELDS,
        collect_source_boundary_priority_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "source_fusion_gap_summary.csv",
        SOURCE_FUSION_GAP_SUMMARY_FIELDS,
        collect_source_fusion_gap_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "fusion_blocker_summary.csv",
        FUSION_BLOCKER_SUMMARY_FIELDS,
        collect_fusion_blocker_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "region_decision_summary.csv",
        REGION_DECISION_SUMMARY_FIELDS,
        collect_region_decision_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "stage_pipeline_summary.csv",
        STAGE_PIPELINE_SUMMARY_FIELDS,
        collect_stage_pipeline_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "kernel_runtime_summary.csv",
        KERNEL_RUNTIME_SUMMARY_FIELDS,
        collect_kernel_runtime_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "admission_efficiency_summary.csv",
        ADMISSION_EFFICIENCY_SUMMARY_FIELDS,
        collect_admission_efficiency_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "admission_proof_gap_summary.csv",
        ADMISSION_PROOF_GAP_SUMMARY_FIELDS,
        collect_admission_proof_gap_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "pipeline_runtime_summary.csv",
        PIPELINE_RUNTIME_SUMMARY_FIELDS,
        collect_pipeline_runtime_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "flow_step_summary.csv",
        FLOW_STEP_SUMMARY_FIELDS,
        collect_flow_step_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "decision_counter_summary.csv",
        DECISION_COUNTER_SUMMARY_FIELDS,
        collect_decision_counter_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "operator_profile_summary.csv",
        OPERATOR_PROFILE_SUMMARY_FIELDS,
        collect_operator_profile_summary(out_dir, rows),
    )


def write_manifest(
    args: argparse.Namespace,
    out_dir: Path,
    rows: list,
    db_path: Path,
    temp_dir: Optional[Path],
) -> None:
    artifact_names = [
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
        "report.md",
    ]
    for row in rows:
        artifact_names.extend(
            [
                row["event_summary_csv"],
                row["correctness_csv"],
                row["events_csv"],
                row["counters_csv"],
                row["decision_counters_csv"],
                row["kernel_counters_csv"],
                row["profile_json"],
            ]
        )
    if args.use_existing_db:
        db_mode = "explicit_existing"
    elif args.db is not None:
        db_mode = "explicit_new"
    else:
        db_mode = "temporary"
    write_trace_manifest(
        out_dir,
        kind="tpch_jit_trace",
        generator="benchmark/tpch/jit/tpch_trace.py",
        configuration={
            "duckdb": str(args.duckdb),
            "backend": args.backend,
            "jit_extension": args.jit_extension,
            "threads": args.threads,
            "event_log_size": args.event_log_size,
            "trace_runtime": args.trace_runtime,
            "dump_ir": args.dump_ir,
            "jit_verify": args.jit_verify,
            "scale_factor": args.scale_factor,
            "queries": [f"{int(query_id):02d}" for query_id in args.queries],
            "policies": list(args.policies),
            "db_path": str(db_path),
            "db_mode": db_mode,
            "keep_db": args.keep_db,
            "temporary_database_directory": str(temp_dir) if temp_dir is not None else "",
            "fusion_blocker_rows": len(collect_fusion_blocker_summary(out_dir, rows)),
            "flow_step_rows": len(collect_flow_step_summary(out_dir, rows)),
            "admission_efficiency_rows": len(collect_admission_efficiency_summary(out_dir, rows)),
            "admission_proof_gap_rows": len(collect_admission_proof_gap_summary(out_dir, rows)),
            "micro_diagnostic_dir": str(args.micro_diagnostic_dir) if args.micro_diagnostic_dir else "",
        },
        artifact_names=artifact_names,
    )


def read_micro_benchmark_summary(micro_benchmark_dir: Optional[Path]) -> list:
    if not micro_benchmark_dir:
        return []
    summary_path = micro_benchmark_dir / "summary.csv"
    if not summary_path.exists():
        return []
    with summary_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_micro_diagnostic_summary(micro_diagnostic_dir: Optional[Path]) -> list:
    if not micro_diagnostic_dir:
        return []
    summary_path = micro_diagnostic_dir / "summary.csv"
    if not summary_path.exists():
        return []
    with summary_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def append_micro_admission_snapshot(lines: list, args: argparse.Namespace) -> None:
    micro_rows = read_micro_benchmark_summary(args.micro_benchmark_dir)
    if not micro_rows:
        return
    lines.append("## Micro Admission Proof Snapshot")
    lines.append("")
    lines.append(
        "Micro benchmark-runner verification passed for the admitted threshold shapes. "
        "Each admitted threshold benchmark result query processed 1,000,000 runtime input rows "
        "before this TPC-H comparison used the corresponding admission proof."
    )
    lines.append("")
    lines.append(
        "| shape | policy | shape_key | proof | run_count | median_s | speedup_vs_off | faster_than_off |"
    )
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | --- |")
    for row in micro_rows:
        lines.append(
            "| {shape} | {policy} | {shape_key} | {proof} | {run_count} | {median_s} | {speedup} | {faster} |".format(
                shape=row["shape"],
                policy=row["policy"],
                shape_key=row["shape_key"],
                proof=row["proof"],
                run_count=row["run_count"],
                median_s=row["median_s"],
                speedup=row["speedup_vs_off"],
                faster=row["faster_than_off"],
            )
        )
    lines.append("")


def append_micro_diagnostic_snapshot(lines: list, args: argparse.Namespace) -> None:
    diagnostic_rows = read_micro_diagnostic_summary(args.micro_diagnostic_dir)
    if not diagnostic_rows:
        return
    lines.append("## Micro Diagnostic Rejection Snapshot")
    lines.append("")
    lines.append(
        "Diagnostic benchmark-runner verification passed for unadmitted shapes. Threshold rows below the "
        "admission margin remain diagnostic; if a threshold shape crosses that margin, it must be promoted "
        "with a real proof or reclassified."
    )
    lines.append("")
    lines.append("| shape | family | size | policy | shape_key | run_count | median_s | speedup_vs_off | faster_than_off |")
    lines.append("| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |")
    for row in diagnostic_rows:
        lines.append(
            "| {shape} | {family} | {size} | {policy} | {shape_key} | {run_count} | {median_s} | {speedup} | {faster} |".format(
                shape=row["shape"],
                family=row["family"],
                size=row["size"],
                policy=row["policy"],
                shape_key=row["shape_key"],
                run_count=row["run_count"],
                median_s=row["median_s"],
                speedup=row["speedup_vs_off"],
                faster=row["faster_than_off"],
            )
        )
    lines.append("")


def write_report(args: argparse.Namespace, out_dir: Path, rows: list) -> None:
    by_query = {}
    for row in rows:
        by_query.setdefault(row["query"], {})[row["policy"]] = row
    by_policy = {}
    for row in rows:
        by_policy.setdefault(row["policy"], []).append(row)

    lines = []
    lines.append("# TPC-H JIT Trace Report")
    lines.append("")
    lines.append(f"- generated_at: {datetime.datetime.now(datetime.timezone.utc).isoformat()}")
    lines.append(f"- duckdb: {args.duckdb}")
    lines.append(f"- scale_factor: {args.scale_factor}")
    lines.append(f"- backend: {args.backend}")
    lines.append(f"- threads: {args.threads}")
    lines.append(f"- jit_verify: {str(args.jit_verify).lower()}")
    lines.append(f"- trace_runtime: {str(args.trace_runtime).lower()}")
    lines.append(f"- dump_ir: {str(args.dump_ir).lower()}")
    if args.micro_benchmark_dir:
        lines.append(f"- micro_benchmark_dir: {args.micro_benchmark_dir}")
    if args.micro_diagnostic_dir:
        lines.append(f"- micro_diagnostic_dir: {args.micro_diagnostic_dir}")
    lines.append("")
    append_micro_admission_snapshot(lines, args)
    append_micro_diagnostic_snapshot(lines, args)
    lines.append("## Query Summary")
    lines.append("")
    lines.append(
        "| query | off_s | auto_s | force_s | auto_diff | force_diff | auto_regions | force_regions | "
        "auto_skipped | auto_unsupported | force_native | auto_non_region | force_non_region | auto_stage_us | force_stage_us |"
    )
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for query_id in sorted(by_query):
        off = by_query[query_id].get("off", {})
        auto = by_query[query_id].get("auto", {})
        force = by_query[query_id].get("force", {})
        auto_diff = row_correctness_diff(auto)
        force_diff = row_correctness_diff(force)
        auto_stage_us = row_stage_time_us(auto)
        force_stage_us = row_stage_time_us(force)
        lines.append(
            "| {query} | {off_s} | {auto_s} | {force_s} | {auto_diff} | {force_diff} | "
            "{auto_compiled} | {force_compiled} | {auto_skipped} | {auto_unsupported} | "
            "{force_native} | {auto_non_region} | {force_non_region} | "
            "{auto_stage_us} | {force_stage_us} |".format(
                query=query_id,
                off_s=off.get("total_time_s", ""),
                auto_s=auto.get("total_time_s", ""),
                force_s=force.get("total_time_s", ""),
                auto_diff=auto_diff,
                force_diff=force_diff,
                auto_compiled=auto.get("compiled_regions", ""),
                force_compiled=force.get("compiled_regions", ""),
                auto_skipped=auto.get("skipped_regions", ""),
                auto_unsupported=auto.get("unsupported_regions", ""),
                force_native=force.get("native_compile_events", ""),
                auto_non_region=auto.get("non_region_events", ""),
                force_non_region=force.get("non_region_events", ""),
                auto_stage_us=auto_stage_us,
                force_stage_us=force_stage_us,
            )
        )
    lines.append("")
    lines.append("## Policy Totals")
    lines.append("")
    lines.append(
        "| policy | total_s | relative_to_off | correctness_diff | compiled_regions | skipped_regions | "
        "unsupported_regions | non_region_events | native_compiles | zero_code_native | runtime_events | "
        "runtime_rows | stage_us | codegen_us |"
    )
    lines.append(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    off_total = sum(row_float(row, "total_time_s") for row in by_policy.get("off", []))
    for policy in args.policies:
        policy_rows = by_policy.get(policy, [])
        if not policy_rows:
            continue
        total_time_s = sum(row_float(row, "total_time_s") for row in policy_rows)
        relative_to_off = total_time_s / off_total if off_total > 0 else 0.0
        lines.append(
            "| {policy} | {total:.6f} | {relative:.4f} | {correctness} | {compiled_regions} | "
            "{skipped_regions} | {unsupported_regions} | {non_region_events} | {native_compiles} | "
            "{zero_code_native} | {runtime_events} | {runtime_rows} | {stage_us} | {codegen_us} |".format(
                policy=policy,
                total=total_time_s,
                relative=relative_to_off,
                correctness=sum(row_correctness_diff(row) for row in policy_rows),
                compiled_regions=sum(row_int(row, "compiled_regions") for row in policy_rows),
                skipped_regions=sum(row_int(row, "skipped_regions") for row in policy_rows),
                unsupported_regions=sum(row_int(row, "unsupported_regions") for row in policy_rows),
                non_region_events=sum(row_int(row, "non_region_events") for row in policy_rows),
                native_compiles=sum(row_int(row, "native_compile_events") for row in policy_rows),
                zero_code_native=sum(row_int(row, "zero_code_native_compile_events") for row in policy_rows),
                runtime_events=sum(row_int(row, "runtime_events") for row in policy_rows),
                runtime_rows=sum(row_int(row, "runtime_input_rows") for row in policy_rows),
                stage_us=sum(row_stage_time_us(row) for row in policy_rows),
                codegen_us=sum(row_int(row, "codegen_time_us") for row in policy_rows),
            )
        )
    lines.append("")
    admission_efficiency_summary = collect_admission_efficiency_summary(out_dir, rows)
    if admission_efficiency_summary:
        lines.append("## Admission Efficiency Summary")
        lines.append("")
        lines.append(
            "| query | policy | mode | form | efficiency | root_cause | runtime_us | generated_pct | "
            "native_source_pct | admission_shape | pipeline |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- |")
        top_admission_efficiency = sorted(
            admission_efficiency_summary,
            key=lambda entry: (
                -row_int(entry, "runtime_time_us"),
                entry["query"],
                entry["policy"],
            ),
        )[:20]
        for entry in top_admission_efficiency:
            lines.append(
                "| {query} | {policy} | {mode} | {form} | {efficiency} | {root_cause} | {runtime_us} | "
                "{generated_pct} | {native_pct} | {shape_key} | {pipeline} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    efficiency=entry["efficiency_class"],
                    root_cause=entry["root_cause"],
                    runtime_us=entry["runtime_time_us"],
                    generated_pct=entry["generated_body_runtime_percent"],
                    native_pct=entry["source_native_runtime_percent"],
                    shape_key=truncate_text(entry["admission_shape_key"], 72).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["candidate_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    admission_proof_gap_summary = collect_admission_proof_gap_summary(out_dir, rows)
    if admission_proof_gap_summary:
        lines.append("## Admission Proof Gap Summary")
        lines.append("")
        lines.append(
            "| shape_key | mode | form | scope | queries | force_regions | median_speedup | "
            "proof_status | root_cause |"
        )
        lines.append("| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |")
        for entry in admission_proof_gap_summary[:20]:
            lines.append(
                "| {shape_key} | {mode} | {form} | {scope} | {queries} | {force_regions} | {speedup} | "
                "{proof_status} | {root_cause} |".format(
                    shape_key=truncate_text(entry["admission_shape_key"], 72).replace("|", "\\|"),
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    scope=entry["candidate_scope"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    force_regions=entry["force_compiled_regions"],
                    speedup=entry["median_force_speedup_vs_off"],
                    proof_status=entry["proof_status"],
                    root_cause=entry["root_cause"],
                )
            )
        lines.append("")
    query_gap_summary = collect_query_gap_summary(out_dir, rows)
    if query_gap_summary:
        lines.append("## Query Gap Summary")
        lines.append("")
        lines.append(
            "| query | root_cause | force_kernels | reached | row_kernels | unreached | runtime_rows | "
            "runtime_us | compiled_pipeline_shapes | top_force_profile | scan_join_groupby_pct | projection_pct | "
            "top_auto_skip | top_force_unsupported |"
        )
        lines.append(
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |"
        )
        for entry in query_gap_summary:
            compiled_pipeline_shapes = truncate_text(entry["force_compiled_pipeline_shapes"], 90)
            top_auto_skip = ""
            if row_int(entry, "auto_top_skip_count") > 0:
                top_auto_skip = truncate_text(
                    "{count}x {shape}@{pipeline}: {reason}".format(
                        count=entry["auto_top_skip_count"],
                        shape=entry["auto_top_skip_shape"] or "none",
                        pipeline=compact_pipeline_shape(entry["auto_top_skip_pipeline_shape"]),
                        reason=entry["auto_top_skip_reason"],
                    ),
                    90,
                )
            top_force_unsupported = ""
            if row_int(entry, "force_top_unsupported_count") > 0:
                top_force_unsupported = truncate_text(
                    "{count}x {shape}@{pipeline}: {reason}".format(
                        count=entry["force_top_unsupported_count"],
                        shape=entry["force_top_unsupported_shape"] or "none",
                        pipeline=compact_pipeline_shape(entry["force_top_unsupported_pipeline_shape"]),
                        reason=entry["force_top_unsupported_reason"],
                    ),
                    90,
                )
            lines.append(
                "| {query} | {root_cause} | {kernels} | {reached} | {row_kernels} | {unreached} | "
                "{runtime_rows} | {runtime_us} | {compiled} | {profile} | {heavy_pct} | {projection_pct} | "
                "{skip} | {unsupported} |".format(
                    query=entry["query"],
                    root_cause=entry["root_cause"].replace("|", "\\|"),
                    kernels=entry["force_compiled_kernels"],
                    reached=entry["force_reached_kernels"],
                    row_kernels=entry["force_row_processing_kernels"],
                    unreached=entry["force_unreached_kernels"],
                    runtime_rows=entry["force_runtime_input_rows"],
                    runtime_us=entry["force_runtime_time_us"],
                    compiled=compiled_pipeline_shapes.replace("|", "\\|"),
                    profile="{operator}:{time_us}us".format(
                        operator=entry["force_top_profile_operator"],
                        time_us=entry["force_top_profile_time_us"],
                    ),
                    heavy_pct=entry["force_scan_join_groupby_profile_percent"],
                    projection_pct=entry["force_projection_profile_percent"],
                    skip=top_auto_skip.replace("|", "\\|"),
                    unsupported=top_force_unsupported.replace("|", "\\|"),
                )
            )
        lines.append("")
    query_capability_priority_summary = collect_query_capability_priority_summary(out_dir, rows)
    if query_capability_priority_summary:
        lines.append("## Query Capability Priority Summary")
        lines.append("")
        lines.append(
            "| query | status | capability_gap | profile_us | profile_pct | runtime_rows | runtime_us | "
            "source_rows | source_us | occurrences | profile_operators | candidate_scopes | example_pipeline |"
        )
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |")
        top_query_priority = {}
        for entry in query_capability_priority_summary:
            if entry["policy"] != "force":
                continue
            existing = top_query_priority.get(entry["query"])
            if existing is None or row_int(entry, "profile_time_us") > row_int(existing, "profile_time_us"):
                top_query_priority[entry["query"]] = entry
        for query_id in sorted(top_query_priority):
            entry = top_query_priority[query_id]
            lines.append(
                "| {query} | {status} | {capability_gap} | {profile_time_us} | {profile_percent} | "
                "{runtime_rows} | {runtime_us} | {source_rows} | {source_us} | {occurrences} | "
                "{profile_operators} | {candidate_scopes} | {pipeline} |".format(
                    query=entry["query"],
                    status=entry["status"],
                    capability_gap=entry["capability_gap"],
                    profile_time_us=entry["profile_time_us"],
                    profile_percent=entry["profile_percent_of_query_policy"],
                    runtime_rows=entry["runtime_input_rows"],
                    runtime_us=entry["runtime_time_us"],
                    source_rows=entry["source_native_output_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    profile_operators=truncate_text(entry["profile_operators"], 80).replace("|", "\\|"),
                    candidate_scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    stage_pipeline_summary = collect_stage_pipeline_summary(out_dir, rows)
    if stage_pipeline_summary:
        lines.append("## Stage Pipeline Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | shape | scope | count | queries | stage_us | dominant_stage | "
            "decision_us | compile_us | codegen_us | pipeline |"
        )
        lines.append("| --- | --- | --- | --- | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |")
        top_stage_pipelines = sorted(
            stage_pipeline_summary,
            key=lambda entry: (
                -row_int(entry, "stage_total_time_us"),
                -row_int(entry, "count"),
                entry["policy"],
                entry["status"],
                entry["candidate_shape"],
            ),
        )[:20]
        for entry in top_stage_pipelines:
            lines.append(
                "| {policy} | {status} | {mode} | {shape} | {scope} | {count} | {queries} | {stage_us} | "
                "{dominant_stage}:{dominant_stage_time_us} | {decision_us} | {compile_us} | {codegen_us} | "
                "{pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    shape=entry["candidate_shape"],
                    scope=entry["candidate_scope"],
                    count=entry["count"],
                    queries=entry["query_examples"],
                    stage_us=entry["stage_total_time_us"],
                    dominant_stage=entry["dominant_stage"],
                    dominant_stage_time_us=entry["dominant_stage_time_us"],
                    decision_us=entry["decision_time_us"],
                    compile_us=entry["compile_time_us"],
                    codegen_us=entry["codegen_time_us"],
                    pipeline=truncate_text(compact_pipeline_shape(entry["candidate_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    capability_gap_summary = collect_capability_gap_summary(out_dir, rows)
    if capability_gap_summary:
        lines.append("## Capability Gap Summary")
        lines.append("")
        lines.append(
            "| policy | status | capability_gap | occurrences | region_events | queries | operators | boundaries | candidate_shapes | candidate_scopes | example_pipeline |"
        )
        lines.append("| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- |")
        top_capability_gaps = sorted(
            capability_gap_summary,
            key=lambda entry: (-row_int(entry, "occurrences"), entry["policy"], entry["status"], entry["capability_gap"]),
        )[:25]
        for entry in top_capability_gaps:
            lines.append(
                "| {policy} | {status} | {capability_gap} | {occurrences} | {region_events} | "
                "{queries} | {operators} | {boundaries} | {candidate_shapes} | {candidate_scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    capability_gap=entry["capability_gap"],
                    occurrences=entry["occurrences"],
                    region_events=entry["region_events"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    operators=truncate_text(entry["operators"], 60).replace("|", "\\|"),
                    boundaries=truncate_text(entry["boundaries"], 60).replace("|", "\\|"),
                    candidate_shapes=truncate_text(entry["candidate_shapes"], 60).replace("|", "\\|"),
                    candidate_scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    capability_priority_summary = collect_capability_priority_summary(out_dir, rows)
    if capability_priority_summary:
        lines.append("## Capability Priority Summary")
        lines.append("")
        lines.append(
            "| policy | status | capability_gap | profile_us | profile_pct | runtime_rows | runtime_us | "
            "source_rows | source_us | occurrences | queries | profile_operators | candidate_scopes | example_pipeline |"
        )
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |")
        top_capability_priorities = sorted(
            capability_priority_summary,
            key=lambda entry: (
                -row_int(entry, "profile_time_us"),
                -row_int(entry, "occurrences"),
                entry["policy"],
                entry["status"],
                entry["capability_gap"],
            ),
        )[:25]
        for entry in top_capability_priorities:
            lines.append(
                "| {policy} | {status} | {capability_gap} | {profile_time_us} | {profile_percent} | "
                "{runtime_rows} | {runtime_us} | {source_rows} | {source_us} | {occurrences} | "
                "{queries} | {profile_operators} | {candidate_scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    capability_gap=entry["capability_gap"],
                    profile_time_us=entry["profile_time_us"],
                    profile_percent=entry["profile_percent_of_policy"],
                    runtime_rows=entry["runtime_input_rows"],
                    runtime_us=entry["runtime_time_us"],
                    source_rows=entry["source_native_output_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    profile_operators=truncate_text(entry["profile_operators"], 80).replace("|", "\\|"),
                    candidate_scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    source_boundary_summary = collect_source_boundary_summary(out_dir, rows)
    if source_boundary_summary:
        lines.append("## Source Boundary Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | kind | source_exec | source | function | boundary_operator | columns | projected | "
            "filters | dynamic | in_out | source_rows | source_us | occurrences | events | queries | "
            "candidate_scopes | example_pipeline |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |"
        )
        top_source_boundaries = sorted(
            source_boundary_summary,
            key=lambda entry: (
                -row_int(entry, "occurrences"),
                -row_int(entry, "max_estimated_cardinality"),
                entry["policy"],
                entry["status"],
                entry["source_boundary_kind"],
                entry["source_execution"],
                entry["source_operator"],
            ),
        )[:25]
        for entry in top_source_boundaries:
            lines.append(
                "| {policy} | {status} | {mode} | {kind} | {source_exec} | {source} | {function} | {boundary_operator} | "
                "{columns} | {projected} | {filters} | {dynamic} | {in_out} | {source_rows} | {source_us} | "
                "{occurrences} | {events} | {queries} | {scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    kind=entry["source_boundary_kind"],
                    source_exec=entry["source_execution"],
                    source=entry["source_operator"],
                    function=entry["scan_function"],
                    boundary_operator=entry["boundary_operator"],
                    columns=entry["column_ids"] or entry["output_columns"],
                    projected=entry["projected_columns"],
                    filters=entry["filter_count"],
                    dynamic=entry["dynamic_filters"],
                    in_out=entry["in_out_function"],
                    source_rows=entry["source_native_output_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    events=entry["region_events"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    source_boundary_priority_summary = collect_source_boundary_priority_summary(out_dir, rows)
    if source_boundary_priority_summary:
        lines.append("## Source Boundary Priority Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | kind | source_exec | source | function | profile_us | profile_pct | "
            "source_rows | source_us | occurrences | queries | profile_operators | features | candidate_scopes | "
            "example_pipeline |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |")
        top_source_priorities = sorted(
            source_boundary_priority_summary,
            key=lambda entry: (
                -row_int(entry, "profile_time_us"),
                -row_int(entry, "occurrences"),
                entry["policy"],
                entry["status"],
                entry["source_boundary_kind"],
                entry["source_execution"],
                entry["source_operator"],
            ),
        )[:25]
        for entry in top_source_priorities:
            features = source_boundary_feature_text(entry)
            lines.append(
                "| {policy} | {status} | {mode} | {kind} | {source_exec} | {source} | {function} | {profile_us} | "
                "{profile_pct} | {source_rows} | {source_us} | {occurrences} | {queries} | {profile_operators} | "
                "{features} | {scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    kind=entry["source_boundary_kind"],
                    source_exec=entry["source_execution"],
                    source=entry["source_operator"],
                    function=entry["scan_function"],
                    profile_us=entry["profile_time_us"],
                    profile_pct=entry["profile_percent_of_policy"],
                    source_rows=entry["source_native_output_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    profile_operators=truncate_text(entry["profile_operators"], 80).replace("|", "\\|"),
                    features=features.replace("|", "\\|"),
                    scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    source_fusion_gap_summary = collect_source_fusion_gap_summary(out_dir, rows)
    if source_fusion_gap_summary:
        lines.append("## Source Fusion Gap Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | form | gap | source | execution | native_capability | native_blocker | function | profile_us | profile_pct | "
            "source_rows | source_us | occurrences | queries | candidate_shapes | admission_shapes | example_pipeline |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |"
        )
        top_source_fusion_gaps = sorted(
            source_fusion_gap_summary,
            key=lambda entry: (
                -row_int(entry, "profile_time_us"),
                -row_int(entry, "occurrences"),
                entry["policy"],
                entry["status"],
                entry["source_operator"],
                entry["scan_function"],
            ),
        )[:25]
        for entry in top_source_fusion_gaps:
            lines.append(
                "| {policy} | {status} | {mode} | {form} | {gap} | {source} | {execution} | {capability} | "
                "{blocker} | {function} | "
                "{profile_us} | {profile_pct} | {source_rows} | {source_us} | {occurrences} | {queries} | "
                "{shapes} | {admission_shapes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    gap=entry["source_fusion_gap"],
                    source=entry["source_operator"],
                    execution=entry["source_execution"],
                    capability=entry["native_source_required_capability"],
                    blocker=entry["native_source_blocker"],
                    function=entry["scan_function"],
                    profile_us=entry["profile_time_us"],
                    profile_pct=entry["profile_percent_of_policy"],
                    source_rows=entry["source_native_output_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    shapes=truncate_text(entry["candidate_shapes"], 60).replace("|", "\\|"),
                    admission_shapes=truncate_text(entry["admission_shape_keys"], 70).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    fusion_blocker_summary = collect_fusion_blocker_summary(out_dir, rows)
    if fusion_blocker_summary:
        lines.append("## Fusion Blocker Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | form | blocker | source | sink | native_capability | native_blocker | "
            "profile_us | runtime_rows | source_us | body_us | occurrences | queries | shapes | pipeline |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |"
        )
        top_fusion_blockers = sorted(
            fusion_blocker_summary,
            key=lambda entry: (
                -row_int(entry, "profile_time_us"),
                -row_int(entry, "runtime_input_rows"),
                -row_int(entry, "occurrences"),
                entry["policy"],
                entry["fusion_blocker"],
            ),
        )[:25]
        for entry in top_fusion_blockers:
            lines.append(
                "| {policy} | {status} | {mode} | {form} | {blocker} | {source} | {sink} | {capability} | "
                "{native_blocker} | {profile_us} | {runtime_rows} | {source_us} | {body_us} | "
                "{occurrences} | {queries} | {shapes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    blocker=entry["fusion_blocker"],
                    source=entry["source_kind"] or "none",
                    sink=entry["sink_kind"] or "none",
                    capability=entry["native_source_required_capability"] or "none",
                    native_blocker=entry["native_source_blocker"] or "none",
                    profile_us=entry["profile_time_us"],
                    runtime_rows=entry["runtime_input_rows"],
                    source_us=entry["source_native_runtime_time_us"],
                    body_us=entry["generated_body_runtime_time_us"],
                    occurrences=entry["occurrences"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    shapes=truncate_text(entry["candidate_shapes"], 70).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    expression_fallback_summary = collect_expression_fallback_summary(out_dir, rows)
    if expression_fallback_summary:
        lines.append("## Expression Fallback Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | reason | class | type | function | return | occurrences | "
            "events | queries | candidate_scopes | example_pipeline |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- | --- |")
        top_expression_fallbacks = sorted(
            expression_fallback_summary,
            key=lambda entry: (
                -row_int(entry, "occurrences"),
                -row_int(entry, "region_events"),
                entry["policy"],
                entry["status"],
                entry["reason"],
                entry["function_name"],
            ),
        )[:25]
        for entry in top_expression_fallbacks:
            lines.append(
                "| {policy} | {status} | {mode} | {reason} | {klass} | {expr_type} | {function} | "
                "{return_type} | {occurrences} | {events} | {queries} | {scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    reason=entry["reason"],
                    klass=entry["expression_class"],
                    expr_type=entry["expression_type"],
                    function=entry["function_name"],
                    return_type=entry["return_type"],
                    occurrences=entry["occurrences"],
                    events=entry["region_events"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    operator_gap_summary = collect_operator_gap_summary(out_dir, rows)
    if operator_gap_summary:
        lines.append("## Operator Gap Summary")
        lines.append("")
        lines.append(
            "| policy | status | operator | kind | boundary | occurrences | region_events | queries | candidate_shapes | candidate_scopes | example_pipeline |"
        )
        lines.append("| --- | --- | --- | --- | --- | ---: | ---: | --- | --- | --- | --- |")
        top_operator_gaps = sorted(
            operator_gap_summary,
            key=lambda entry: (-row_int(entry, "occurrences"), entry["policy"], entry["status"], entry["operator_name"]),
        )[:25]
        for entry in top_operator_gaps:
            lines.append(
                "| {policy} | {status} | {operator_name} | {node_kind} | {boundary} | {occurrences} | "
                "{region_events} | {queries} | {candidate_shapes} | {candidate_scopes} | {pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    operator_name=entry["operator_name"],
                    node_kind=entry["node_kind"],
                    boundary=entry["boundary"],
                    occurrences=entry["occurrences"],
                    region_events=entry["region_events"],
                    queries=entry["query_examples"].replace("|", "\\|"),
                    candidate_shapes=truncate_text(entry["candidate_shapes"], 60).replace("|", "\\|"),
                    candidate_scopes=truncate_text(entry["candidate_scopes"], 60).replace("|", "\\|"),
                    pipeline=truncate_text(compact_pipeline_shape(entry["example_pipeline_shape"]), 90).replace(
                        "|", "\\|"
                    ),
                )
            )
        lines.append("")
    operator_profile_summary = collect_operator_profile_summary(out_dir, rows)
    if operator_profile_summary:
        lines.append("## Operator Profile Summary")
        lines.append("")
        lines.append(
            "| query | policy | operator | time_us | pct_query | rows | occurrences | extra_info |"
        )
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | --- |")
        top_profile_entries = sorted(
            operator_profile_summary,
            key=lambda entry: (-row_int(entry, "operator_time_us"), entry["query"], entry["policy"], entry["operator_name"]),
        )[:25]
        for entry in top_profile_entries:
            lines.append(
                "| {query} | {policy} | {operator_name} | {time_us} | {percent} | {rows} | {occurrences} | {extra} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    operator_name=entry["operator_name"],
                    time_us=entry["operator_time_us"],
                    percent=entry["percent_query_time"],
                    rows=entry["output_rows"],
                    occurrences=entry["occurrences"],
                    extra=truncate_text(entry["extra_info_examples"], 90).replace("|", "\\|"),
                )
            )
        lines.append("")
    decision_breakdown = collect_region_decision_breakdown(out_dir, rows)
    if decision_breakdown:
        lines.append("## Region Decision Breakdown")
        lines.append("")
        lines.append(
            "| policy | status | execution_mode | candidate_shape | candidate_scope | count | query_examples | example_reason |"
        )
        lines.append("| --- | --- | --- | --- | --- | ---: | --- | --- |")
        for entry in decision_breakdown:
            lines.append(
                "| {policy} | {status} | {execution_mode} | {candidate_shape} | {candidate_scope} | {count} | {queries} | {reason} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    execution_mode=entry["execution_mode"],
                    candidate_shape=entry["candidate_shape"],
                    candidate_scope=entry["candidate_scope"],
                    count=entry["count"],
                    queries=format_query_examples(entry["queries"]),
                    reason=entry["example_reason"].replace("|", "\\|"),
                )
            )
        lines.append("")
    kernel_breakdown = collect_kernel_runtime_breakdown(out_dir, rows)
    if kernel_breakdown:
        lines.append("## Runtime Kernel Breakdown")
        lines.append("")
        lines.append(
            "| policy | execution_mode | candidate_shape | candidate_scope | kernels | reached | row_kernels | unreached | input_rows | "
            "output_rows | invocations | runtime_us | query_examples |"
        )
        lines.append(
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
        )
        for entry in kernel_breakdown:
            lines.append(
                "| {policy} | {execution_mode} | {candidate_shape} | {candidate_scope} | {kernels} | {reached} | {row_kernels} | "
                "{unreached} | {input_rows} | {output_rows} | {invocations} | {runtime_time_us} | {queries} |".format(
                    policy=entry["policy"],
                    execution_mode=entry["execution_mode"],
                    candidate_shape=entry["candidate_shape"],
                    candidate_scope=entry["candidate_scope"],
                    kernels=entry["kernels"],
                    reached=entry["reached_kernels"],
                    row_kernels=entry["row_processing_kernels"],
                    unreached=entry["unreached_kernels"],
                    input_rows=entry["input_rows"],
                    output_rows=entry["output_rows"],
                    invocations=entry["invocations"],
                    runtime_time_us=entry["runtime_time_us"],
                    queries=format_query_examples(entry["queries"]),
                )
            )
        lines.append("")
    pipeline_runtime_summary = collect_pipeline_runtime_summary(out_dir, rows)
    if pipeline_runtime_summary:
        lines.append("## Pipeline Runtime Summary")
        lines.append("")
        lines.append(
            "| policy | status | mode | candidate_shape | candidate_scope | events | kernels | row_kernels | runtime_rows | "
            "runtime_us | profile_us | profile_pct | capability_gaps | query_examples | compact_pipeline |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |"
        )
        top_pipeline_entries = sorted(
            pipeline_runtime_summary,
            key=lambda entry: (
                -row_int(entry, "profile_time_us"),
                -row_int(entry, "runtime_input_rows"),
                -row_int(entry, "region_events"),
                entry["policy"],
                entry["status"],
                entry["candidate_shape"],
            ),
        )[:25]
        for entry in top_pipeline_entries:
            lines.append(
                "| {policy} | {status} | {mode} | {shape} | {scope} | {events} | {kernels} | {row_kernels} | "
                "{runtime_rows} | {runtime_us} | {profile_us} | {profile_pct} | {gaps} | {queries} | "
                "{pipeline} |".format(
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    shape=entry["candidate_shape"],
                    scope=entry["candidate_scope"],
                    events=entry["region_events"],
                    kernels=entry["compiled_kernels"],
                    row_kernels=entry["row_processing_kernels"],
                    runtime_rows=entry["runtime_input_rows"],
                    runtime_us=entry["runtime_time_us"],
                    profile_us=entry["profile_time_us"],
                    profile_pct=entry["profile_percent_of_policy"],
                    gaps=truncate_text(entry["capability_gaps"], 80).replace("|", "\\|"),
                    queries=entry["query_examples"].replace("|", "\\|"),
                    pipeline=truncate_text(
                        compact_pipeline_shape(entry["candidate_pipeline_shape"]), 90
                    ).replace("|", "\\|"),
                )
            )
    lines.append("")
    lines.append(
        "Detailed per-query event, counter, and kernel-counter CSV files are in this directory. "
        "Stable aggregate CSVs are available as `query_gap_summary.csv`, `operator_gap_summary.csv`, "
        "`capability_gap_summary.csv`, `capability_priority_summary.csv`, `region_decision_summary.csv`, "
        "`query_capability_priority_summary.csv`, `expression_fallback_summary.csv`, "
        "`source_boundary_summary.csv`, `source_boundary_priority_summary.csv`, "
        "`source_fusion_gap_summary.csv`, `fusion_blocker_summary.csv`, "
        "`stage_pipeline_summary.csv`, `kernel_runtime_summary.csv`, "
        "`admission_proof_gap_summary.csv`, `pipeline_runtime_summary.csv`, "
        "`flow_step_summary.csv`, and `operator_profile_summary.csv`."
    )
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Trace DuckDB JIT behavior on TPC-H queries")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "release" / "duckdb")
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="explicit database path; created when new, or reused with --use-existing-db",
    )
    parser.add_argument("--use-existing-db", action="store_true", help="reuse and validate the explicit --db path")
    parser.add_argument("--keep-db", action="store_true", help="keep the temporary database created when --db is omitted")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--scale-factor", type=float, default=1)
    parser.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--micro-benchmark-dir",
        type=Path,
        default=None,
        help="optional micro_jit_benchmark.py output directory to embed admission proof snapshot in report.md",
    )
    parser.add_argument(
        "--micro-diagnostic-dir",
        type=Path,
        default=None,
        help="optional micro_jit_diagnostic_benchmark.py output directory to embed rejection snapshot in report.md",
    )
    parser.add_argument("--event-log-size", type=int, default=200000)
    parser.add_argument("--trace-runtime", action="store_true")
    parser.add_argument("--dump-ir", action="store_true")
    parser.add_argument(
        "--jit-verify",
        action="store_true",
        help="run compiled JIT kernels against DuckDB reference execution during the trace",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    args.duckdb = args.duckdb.resolve()
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")

    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("tpch_trace")
    out_dir = prepare_trace_output_directory(args.out_dir)

    db_path, temp_dir = prepare_tpch_database(args)

    rows = []
    try:
        for query_id in args.queries:
            query_id = f"{int(query_id):02d}"
            query_sql = read_query(root, query_id)
            create_baseline(args, db_path, query_id, query_sql)
            for policy in args.policies:
                rows.append(run_policy_trace(args, db_path, out_dir, query_id, query_sql, policy))
        write_summary(out_dir, rows)
        write_aggregate_csvs(out_dir, rows)
        write_report(args, out_dir, rows)
        write_manifest(args, out_dir, rows, db_path, temp_dir)
        print(f"trace output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        print(f"query gaps: {out_dir / 'query_gap_summary.csv'}")
        print(f"operator gaps: {out_dir / 'operator_gap_summary.csv'}")
        print(f"capability gaps: {out_dir / 'capability_gap_summary.csv'}")
        print(f"capability priorities: {out_dir / 'capability_priority_summary.csv'}")
        print(f"query capability priorities: {out_dir / 'query_capability_priority_summary.csv'}")
        print(f"expression fallbacks: {out_dir / 'expression_fallback_summary.csv'}")
        print(f"source boundaries: {out_dir / 'source_boundary_summary.csv'}")
        print(f"source boundary priorities: {out_dir / 'source_boundary_priority_summary.csv'}")
        print(f"source fusion gaps: {out_dir / 'source_fusion_gap_summary.csv'}")
        print(f"fusion blockers: {out_dir / 'fusion_blocker_summary.csv'}")
        print(f"region decisions: {out_dir / 'region_decision_summary.csv'}")
        print(f"stage pipelines: {out_dir / 'stage_pipeline_summary.csv'}")
        print(f"kernel runtime: {out_dir / 'kernel_runtime_summary.csv'}")
        print(f"admission efficiency: {out_dir / 'admission_efficiency_summary.csv'}")
        print(f"admission proof gaps: {out_dir / 'admission_proof_gap_summary.csv'}")
        print(f"pipeline runtime: {out_dir / 'pipeline_runtime_summary.csv'}")
        print(f"flow steps: {out_dir / 'flow_step_summary.csv'}")
        print(f"operator profile: {out_dir / 'operator_profile_summary.csv'}")
        print(f"report: {out_dir / 'report.md'}")
        print(f"manifest: {out_dir / TRACE_MANIFEST_NAME}")
    finally:
        if temp_dir is not None and not args.keep_db:
            shutil.rmtree(temp_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TraceConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
