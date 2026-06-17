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
    ADMISSION_EVIDENCE_SUMMARY_FIELDS,
    CANDIDATE_SIGNATURE_FIELDS,
    CANDIDATE_TRAIT_FIELDS,
    DECISION_COUNTER_SUMMARY_FIELDS,
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    FLOW_STEP_SUMMARY_FIELDS,
    KERNEL_RUNTIME_SUMMARY_FIELDS,
    OPERATOR_PROFILE_SUMMARY_FIELDS,
    PROFILE_HEAVY_OPERATORS,
    PROFILE_WRAPPER_OPERATORS,
    QUERY_GAP_SUMMARY_FIELDS,
    REGION_DECISION_STATUSES,
    REGION_DECISION_SUMMARY_FIELDS,
    STAGE_FIELDS,
    TPCH_TABLES,
    TRACE_WRAPPER_OPERATORS,
    configure_csv_field_size_limit,
)

configure_csv_field_size_limit()

TOTAL_TIME_RE = re.compile(r"Total Time:\s*([0-9.]+)s")
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


def append_metric_text(current: str, value: str) -> str:
    if not value:
        return current
    if current:
        return f"{current};{value}"
    return value


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


def candidate_signature_key(row: dict) -> tuple:
    return tuple(row.get(field, "") or "none" for field in CANDIDATE_SIGNATURE_FIELDS)


def candidate_signature_entry(row: dict) -> dict:
    return {field: row.get(field, "") or "none" for field in CANDIDATE_SIGNATURE_FIELDS}


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


def trace_summary_reason(value: str) -> str:
    text = str(value)
    if "execution_region_policy=auto" in text:
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
                "label": parts[0],
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


def attribution_pipeline_shape(region_entry: dict) -> str:
    if region_entry.get("status") == "unsupported":
        return region_entry.get("candidate_context_pipeline_shape", "") or region_entry.get("candidate_pipeline_shape", "")
    return region_entry.get("candidate_pipeline_shape", "")


def flow_step_key(query: str, policy: str, row: dict, phase: str, status: str, policy_decision: str) -> tuple:
    return (
        query,
        policy,
        row.get("target", "") or "none",
        phase or "none",
        status or "none",
        row.get("execution_mode", "") or "none",
        row.get("region_execution_form", "") or "none",
        row.get("execution_body", "") or "none",
        policy_decision or "none",
        row.get("candidate_shape", "") or "none",
        row.get("candidate_contract_abi", "") or "none",
        row.get("candidate_pipeline_shape", "") or "none",
        row.get("candidate_context_pipeline_shape", "") or "none",
        *candidate_signature_key(row),
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
        "execution_body": key[7],
        "policy_decision": key[8],
        "candidate_shape": key[9],
        "candidate_contract_abi": key[10],
        "candidate_pipeline_shape": key[11],
        "candidate_context_pipeline_shape": key[12],
        "candidate_signature_context": key[13],
        "candidate_signature_shape": key[14],
        "candidate_signature_feature_shape": key[15],
        "candidate_signature_context_feature_shape": key[16],
        "candidate_contract_shape": key[17],
        "admission_shape_key": "" if key[18] == "none" else key[18],
        "admission_rule_present": "" if key[19] == "none" else key[19],
        "admission_min_cardinality": "" if key[20] == "none" else key[20],
        "admission_score": "" if key[21] == "none" else key[21],
        "admission_proof": "" if key[22] == "none" else key[22],
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
        "source_contract_output_rows": 0,
        "source_contract_invocations": 0,
        "source_contract_runtime_time_us": 0,
        "generated_body_runtime_time_us": 0,
        "generated_stage_runtime_breakdown": "",
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
                        entry["source_contract_output_rows"] += row_int(event, "source_contract_output_rows")
                        entry["source_contract_invocations"] += row_int(event, "source_contract_invocation_count")
                        entry["source_contract_runtime_time_us"] += row_int(event, "source_contract_runtime_time_us")
                        entry["generated_body_runtime_time_us"] += row_int(event, "generated_body_runtime_time_us")
                        entry["generated_stage_runtime_breakdown"] = append_metric_text(
                            entry["generated_stage_runtime_breakdown"],
                            event.get("generated_stage_runtime_breakdown", ""),
                        )
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
                entry["source_contract_output_rows"] += row_int(counter, "source_contract_output_rows")
                entry["source_contract_invocations"] += row_int(counter, "source_contract_invocation_count")
                entry["source_contract_runtime_time_us"] += row_int(counter, "source_contract_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(counter, "generated_body_runtime_time_us")
                entry["generated_stage_runtime_breakdown"] = append_metric_text(
                    entry["generated_stage_runtime_breakdown"],
                    counter.get("generated_stage_runtime_breakdown", ""),
                )
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
            entry["candidate_contract_abi"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            *(entry[field] for field in CANDIDATE_SIGNATURE_FIELDS),
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
                if event.get("phase") == "runtime":
                    continue
                key = (
                    summary_row["query"],
                    summary_row["policy"],
                    event.get("status", ""),
                    event.get("execution_mode", ""),
                    event.get("region_execution_form", "") or "none",
                    event.get("execution_body", "") or "none",
                    event.get("candidate_shape", "") or "none",
                    event.get("candidate_pipeline_shape", "") or "none",
                    event.get("candidate_context_pipeline_shape", "") or "none",
                    *candidate_signature_key(event),
                    event.get("candidate_contract_abi", "") or "none",
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
                        "execution_body": event.get("execution_body", "") or "none",
                        "candidate_shape": event.get("candidate_shape", "") or "none",
                        "candidate_pipeline_shape": event.get("candidate_pipeline_shape", "") or "none",
                        "candidate_context_pipeline_shape": event.get("candidate_context_pipeline_shape", "") or "none",
                        **candidate_signature_entry(event),
                        "candidate_contract_abi": event.get("candidate_contract_abi", "") or "none",
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
            entry["execution_body"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            *(entry[field] for field in CANDIDATE_SIGNATURE_FIELDS),
            entry["candidate_contract_abi"],
            *(entry[field] for field in CANDIDATE_TRAIT_FIELDS),
            entry["admission_shape_key"],
            entry["admission_rule_present"],
            entry["admission_min_cardinality"],
            entry["admission_score"],
            entry["admission_proof"],
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
                    counter.get("execution_body", "") or "none",
                    counter.get("candidate_shape", "") or "none",
                    counter.get("candidate_pipeline_shape", "") or "none",
                    counter.get("candidate_context_pipeline_shape", "") or "none",
                    *candidate_signature_key(counter),
                    counter.get("candidate_contract_abi", "") or "none",
                )
                entry = summary.setdefault(
                    key,
                    {
                        "query": summary_row["query"],
                        "policy": summary_row["policy"],
                        "execution_mode": counter.get("execution_mode", ""),
                        "region_execution_form": counter.get("region_execution_form", "") or "none",
                        "execution_body": counter.get("execution_body", "") or "none",
                        "candidate_shape": counter.get("candidate_shape", "") or "none",
                        "candidate_pipeline_shape": counter.get("candidate_pipeline_shape", "") or "none",
                        "candidate_context_pipeline_shape": counter.get("candidate_context_pipeline_shape", "") or "none",
                        **candidate_signature_entry(counter),
                        "candidate_contract_abi": counter.get("candidate_contract_abi", "") or "none",
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
                        "source_contract_output_rows": 0,
                        "source_contract_invocations": 0,
                        "source_contract_runtime_time_us": 0,
                        "generated_body_runtime_time_us": 0,
                        "generated_stage_runtime_breakdown": "",
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
                entry["source_contract_output_rows"] += row_int(counter, "source_contract_output_rows")
                entry["source_contract_invocations"] += row_int(counter, "source_contract_invocation_count")
                entry["source_contract_runtime_time_us"] += row_int(counter, "source_contract_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(counter, "generated_body_runtime_time_us")
                entry["generated_stage_runtime_breakdown"] = append_metric_text(
                    entry["generated_stage_runtime_breakdown"],
                    counter.get("generated_stage_runtime_breakdown", ""),
                )
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
            entry["execution_body"],
            entry["candidate_shape"],
            entry["candidate_pipeline_shape"],
            entry["candidate_context_pipeline_shape"],
            *(entry[field] for field in CANDIDATE_SIGNATURE_FIELDS),
            entry["candidate_contract_abi"],
        )
    )
    return result


def classify_admission_evidence(
    winning_queries: int,
    losing_queries: int,
    equal_queries: int,
    auto_rule_present: bool,
    auto_compiled_regions: int,
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
    elif proof_status == "positive_query_median":
        root_cause = "positive_shape_without_auto_admission"
    elif proof_status == "mixed_query_median":
        root_cause = "shape_profitability_depends_on_context"
    elif proof_status == "neutral_query_median":
        root_cause = "neutral_query_level_evidence"
    else:
        root_cause = "force_region_not_profitable"
    return proof_status, root_cause


def admission_evidence_key(row: dict) -> tuple:
    return (
        row["admission_shape_key"],
        row["execution_mode"],
        row["region_execution_form"],
        row["execution_body"],
        row["candidate_shape"],
        *candidate_signature_key(row),
        row["candidate_contract_abi"],
    )


def collect_admission_evidence_summary(out_dir: Path, rows: list) -> list:
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
            force_compiled_keys.add(admission_evidence_key(row))

    for row in decision_counter_rows:
        if row["target"] != "region":
            continue
        key = admission_evidence_key(row)
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
        (
            admission_shape_key,
            execution_mode,
            region_execution_form,
            execution_body,
            candidate_shape,
            candidate_signature_context,
            candidate_signature_shape,
            candidate_signature_feature_shape,
            candidate_signature_context_feature_shape,
            candidate_contract_shape,
            candidate_contract_abi,
        ) = key
        speedups = list(entry["query_speedups"].values())
        winning_queries = sum(1 for speedup in speedups if speedup > 1.000001)
        losing_queries = sum(1 for speedup in speedups if speedup < 0.999999)
        equal_queries = len(speedups) - winning_queries - losing_queries
        proof_status, root_cause = classify_admission_evidence(
            winning_queries,
            losing_queries,
            equal_queries,
            entry["auto_rule_present"],
            entry["auto_compiled_regions"],
        )
        stage_time_us = sum(entry[field] for field in STAGE_FIELDS)
        result.append(
            {
                "admission_shape_key": admission_shape_key,
                "execution_mode": execution_mode,
                "region_execution_form": region_execution_form,
                "execution_body": execution_body,
                "candidate_shape": candidate_shape,
                "candidate_signature_context": candidate_signature_context,
                "candidate_signature_shape": candidate_signature_shape,
                "candidate_signature_feature_shape": candidate_signature_feature_shape,
                "candidate_signature_context_feature_shape": candidate_signature_context_feature_shape,
                "candidate_contract_shape": candidate_contract_shape,
                "candidate_contract_abi": candidate_contract_abi,
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
            *(row[field] for field in CANDIDATE_SIGNATURE_FIELDS),
            row["candidate_contract_abi"],
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


def summarize_compiled_pipeline_abis(kernel_entries: list) -> str:
    abi_counter = collections.Counter()
    for entry in kernel_entries:
        abi_counter[entry.get("candidate_contract_abi", "") or "none"] += row_int(entry, "kernels")
    return format_counter_examples(abi_counter)


def sum_profile_time_us(entries: list, operator_names: Optional[tuple] = None) -> int:
	if operator_names is None:
		return sum(row_int(entry, "operator_time_us") for entry in entries)
	operator_set = set(operator_names)
	return sum(row_int(entry, "operator_time_us") for entry in entries if entry.get("operator_name") in operator_set)


def classify_auto_skip_reasons(auto_skip_entries: list) -> list:
    if not auto_skip_entries:
        return []
    reasons = []
    if any(
        entry.get("admission_rule_present") == "true" and row_int(entry, "admission_score") < 0
        for entry in auto_skip_entries
    ):
        reasons.append("auto_regions_below_admission_threshold")
    if any(
        (
            entry.get("admission_rule_present") == "false"
            or "admission_rule=missing" in entry.get("example_reason", "")
        )
        for entry in auto_skip_entries
    ):
        reasons.append("auto_missing_admission_rule")
    if not reasons:
        reasons.append("auto_region_not_admitted")
    return reasons


def classify_query_gap(
    auto_row: dict,
    auto_skip_entries: list,
    force_row: dict,
    force_relevant_unsupported_count: int,
    force_kernels: list,
    force_profile_entries: list,
) -> str:
    reasons = []
    compiled_kernels = sum(row_int(entry, "kernels") for entry in force_kernels)
    row_processing_kernels = sum(row_int(entry, "row_processing_kernels") for entry in force_kernels)
    if row_int(auto_row, "skipped_regions") > 0:
        reasons.extend(classify_auto_skip_reasons(auto_skip_entries))
    if compiled_kernels > 0 and row_processing_kernels == 0:
        reasons.append("compiled_kernels_not_reached")
    elif compiled_kernels > 0 and row_processing_kernels < compiled_kernels:
        reasons.append("compiled_kernel_reach_gap")
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
                "force_source_contract_output_rows": sum(
                    row_int(entry, "source_contract_output_rows") for entry in force_kernels
                ),
                "force_source_contract_invocations": sum(
                    row_int(entry, "source_contract_invocations") for entry in force_kernels
                ),
                "force_source_contract_runtime_time_us": sum(
                    row_int(entry, "source_contract_runtime_time_us") for entry in force_kernels
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
                "force_compiled_pipeline_abis": summarize_compiled_pipeline_abis(force_kernels),
                "auto_top_skip_shape": auto_skip.get("candidate_shape", ""),
                "auto_top_skip_pipeline_shape": attribution_pipeline_shape(auto_skip),
                "auto_top_skip_abi": auto_skip.get("candidate_contract_abi", "") or "none",
                "auto_top_skip_count": row_int(auto_skip, "count"),
                "auto_top_skip_reason": auto_skip.get("example_reason", ""),
                "force_top_unsupported_shape": force_unsupported.get("candidate_shape", ""),
                "force_top_unsupported_pipeline_shape": attribution_pipeline_shape(force_unsupported),
                "force_top_unsupported_abi": force_unsupported.get("candidate_contract_abi", "") or "none",
                "force_top_unsupported_count": row_int(force_unsupported, "count"),
                "force_top_unsupported_reason": force_unsupported.get("example_reason", ""),
				"root_cause": classify_query_gap(
					auto,
					auto_skip_entries,
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


def admission_profile_sql(admission_rules: Optional[list]) -> str:
    if not admission_rules:
        return "SELECT * FROM duckdb_jit_clear_admission_rules();"
    statements = ["SELECT * FROM duckdb_jit_clear_admission_rules();"]
    for rule in admission_rules:
        statements.append(
            "SELECT * FROM duckdb_jit_add_admission_rule("
            f"{sql_quote(rule['backend_name'])}, "
            f"{sql_quote(rule['target'])}, "
            f"{sql_quote(rule['admission_shape_key'])}, "
            f"{int(rule['admission_min_cardinality'])}::UBIGINT, "
            f"{sql_quote(rule['admission_proof'])}"
            ");"
        )
    return "\n".join(statements)


def setting_sql(args: argparse.Namespace, policy: str, admission_rules: Optional[list] = None) -> str:
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
{admission_profile_sql(admission_rules)}
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
    admission_rules: Optional[list] = None,
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
    coalesce(sum(source_contract_output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_contract_output_rows,
    coalesce(sum(source_contract_invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_contract_invocations,
    coalesce(sum(source_contract_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_contract_runtime_time_us,
    coalesce(sum(generated_body_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS generated_body_runtime_time_us,
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
        WHERE phase='compile' AND execution_mode != 'native'
    ) AS non_native_compile_events,
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
    execution_body,
    selected_source_execution,
    policy_decision,
    candidate_id,
    candidate_shape,
    candidate_pipeline_shape,
    candidate_context_pipeline_shape,
    candidate_signature_context,
    candidate_signature_shape,
    candidate_signature_feature_shape,
    candidate_signature_context_feature_shape,
    candidate_contract_shape,
    candidate_signature_ir,
    candidate_contract_abi,
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
    candidate_expression_traits_known,
    candidate_source_filter_count,
    candidate_source_filter_expression_count,
    candidate_source_filter_missing_count,
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
    candidate_expression_missing_count,
    candidate_operator_missing_count,
    candidate_source_ownership,
    candidate_state_scan_ownership,
    candidate_transform_ownership,
    candidate_sink_ownership,
    candidate_generated_operator_count,
    candidate_source_boundary_count,
    candidate_missing_contract_count,
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
    source_contract_output_rows,
    source_contract_invocation_count,
    source_contract_runtime_time_us,
    generated_body_runtime_time_us,
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
{setting_sql(args, policy, admission_rules)}
SELECT * FROM duckdb_jit_clear_events();
SELECT * FROM duckdb_jit_clear_counters();
EXPLAIN ANALYZE
{query_sql};
{copy_statement(event_summary_select, event_summary_path)}
{copy_statement(events_select, events_path)}
{copy_statement("SELECT * FROM duckdb_jit_counters() ORDER BY backend_name, target, status, execution_mode, region_execution_form, execution_body, policy_decision", counters_path)}
{copy_statement("SELECT * FROM duckdb_jit_decision_counters() ORDER BY backend_name, target, phase, status, execution_mode, region_execution_form, execution_body, policy_decision, candidate_contract_abi, candidate_shape, admission_shape_key", decision_counters_path)}
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
{setting_sql(args, policy, admission_rules)}
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


def merge_admission_profile_rule(admission_profile: list, rule: dict) -> None:
    for entry in admission_profile:
        if (
            entry["backend_name"] == rule["backend_name"]
            and entry["target"] == rule["target"]
            and entry["admission_shape_key"] == rule["admission_shape_key"]
        ):
            entry["admission_min_cardinality"] = max(
                int(entry["admission_min_cardinality"]), int(rule["admission_min_cardinality"])
            )
            entry["admission_proof"] = rule["admission_proof"]
            return
    admission_profile.append(rule)


def canonical_feature_shape(value: str) -> str:
    features = sorted({feature for feature in value.split("+") if feature})
    return "+".join(features)


def build_candidate_admission_profile_key(row: dict) -> str:
    backend_name = row.get("backend_name", "")
    context = row.get("candidate_signature_context", "")
    shape = row.get("candidate_signature_shape", "")
    if not backend_name or not context or not shape:
        return ""
    result = f"{backend_name}:{context}:{shape}"
    feature_shape = canonical_feature_shape(row.get("candidate_signature_feature_shape", ""))
    if feature_shape:
        result += f":{feature_shape}"
    context_feature_shape = canonical_feature_shape(row.get("candidate_signature_context_feature_shape", ""))
    if context_feature_shape:
        result += f":context:{context_feature_shape}"
    contract_shape = row.get("candidate_contract_shape", "")
    if contract_shape:
        result += f":contract:{contract_shape}"
    return result


def make_admission_profile_rule(row: dict, admission_key: str, min_cardinality: int, proof: str) -> dict:
    return {
        "backend_name": row.get("backend_name", ""),
        "target": "region",
        "admission_shape_key": admission_key,
        "admission_min_cardinality": min_cardinality,
        "admission_proof": proof,
    }


def collect_force_admission_profile_rules(args: argparse.Namespace, out_dir: Path, off_row: dict, force_row: dict) -> list:
    off_time = row_float(off_row, "total_time_s")
    force_time = row_float(force_row, "total_time_s")
    if off_time <= 0 or force_time <= 0:
        return []
    speedup = off_time / force_time
    if speedup < args.auto_profile_min_speedup:
        return []

    decision_counters_path = out_dir / force_row["decision_counters_csv"]
    if not decision_counters_path.exists():
        return []
    result = []
    with decision_counters_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("target") != "region" or row.get("status") != "compiled":
                continue
            admission_key = row.get("admission_shape_key", "")
            if not admission_key:
                continue
            min_cardinality = row_int(row, "max_estimated_cardinality")
            if min_cardinality <= 0:
                continue
            proof = (
                "measured-auto-admission:force-vs-off"
                f";query={force_row['query']};speedup={speedup:.6f}"
            )
            if not row.get("backend_name"):
                row["backend_name"] = args.backend
            candidate_key = build_candidate_admission_profile_key(row)
            if candidate_key:
                result.append(make_admission_profile_rule(row, candidate_key, min_cardinality, proof + ";key=inventory"))
            result.append(make_admission_profile_rule(row, admission_key, min_cardinality, proof + ";key=lowered"))
    return result


AGGREGATE_CSVS = (
    ("query_gap_summary.csv", "query gaps", QUERY_GAP_SUMMARY_FIELDS, collect_query_gap_summary),
    ("region_decision_summary.csv", "region decisions", REGION_DECISION_SUMMARY_FIELDS, collect_region_decision_summary),
    ("kernel_runtime_summary.csv", "kernel runtime", KERNEL_RUNTIME_SUMMARY_FIELDS, collect_kernel_runtime_summary),
    (
        "admission_evidence_summary.csv",
        "admission evidence",
        ADMISSION_EVIDENCE_SUMMARY_FIELDS,
        collect_admission_evidence_summary,
    ),
    ("flow_step_summary.csv", "flow steps", FLOW_STEP_SUMMARY_FIELDS, collect_flow_step_summary),
    ("decision_counter_summary.csv", "decision counters", DECISION_COUNTER_SUMMARY_FIELDS, collect_decision_counter_summary),
    ("operator_profile_summary.csv", "operator profile", OPERATOR_PROFILE_SUMMARY_FIELDS, collect_operator_profile_summary),
)


def collect_aggregate_csvs(out_dir: Path, rows: list) -> dict:
    return {name: collector(out_dir, rows) for name, _, _, collector in AGGREGATE_CSVS}


def write_aggregate_csvs(out_dir: Path, rows: list) -> dict:
    aggregate_rows = collect_aggregate_csvs(out_dir, rows)
    for name, _, fieldnames, _ in AGGREGATE_CSVS:
        write_csv_rows(out_dir / name, fieldnames, aggregate_rows[name])
    return aggregate_rows


def write_manifest(
    args: argparse.Namespace,
    out_dir: Path,
    rows: list,
    aggregate_rows: dict,
    db_path: Path,
    temp_dir: Optional[Path],
) -> None:
    artifact_names = ["summary.csv", *(name for name, _, _, _ in AGGREGATE_CSVS), "report.md"]
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
            "calibrate_auto_from_force": args.calibrate_auto_from_force,
            "auto_profile_min_speedup": args.auto_profile_min_speedup,
            "scale_factor": args.scale_factor,
            "queries": [f"{int(query_id):02d}" for query_id in args.queries],
            "policies": list(args.policies),
            "db_path": str(db_path),
            "db_mode": db_mode,
            "keep_db": args.keep_db,
            "temporary_database_directory": str(temp_dir) if temp_dir is not None else "",
            "query_gap_rows": len(aggregate_rows["query_gap_summary.csv"]),
            "region_decision_rows": len(aggregate_rows["region_decision_summary.csv"]),
            "kernel_runtime_rows": len(aggregate_rows["kernel_runtime_summary.csv"]),
            "flow_step_rows": len(aggregate_rows["flow_step_summary.csv"]),
            "admission_evidence_rows": len(aggregate_rows["admission_evidence_summary.csv"]),
        },
        artifact_names=artifact_names,
    )


def write_report(args: argparse.Namespace, out_dir: Path, rows: list, aggregate_rows: dict) -> None:
    by_query = {}
    by_policy = {}
    for row in rows:
        by_query.setdefault(row["query"], {})[row["policy"]] = row
        by_policy.setdefault(row["policy"], []).append(row)

    def md(value, limit: int = 90) -> str:
        return truncate_text(value, limit).replace("|", "\\|")

    lines = [
        "# TPC-H JIT Trace Report",
        "",
        f"- generated_at: {datetime.datetime.now(datetime.timezone.utc).isoformat()}",
        f"- duckdb: {args.duckdb}",
        f"- scale_factor: {args.scale_factor}",
        f"- backend: {args.backend}",
        f"- threads: {args.threads}",
        f"- jit_verify: {str(args.jit_verify).lower()}",
        f"- trace_runtime: {str(args.trace_runtime).lower()}",
        f"- dump_ir: {str(args.dump_ir).lower()}",
        f"- calibrate_auto_from_force: {str(args.calibrate_auto_from_force).lower()}",
        f"- auto_profile_min_speedup: {args.auto_profile_min_speedup}",
        "",
        "## Query Summary",
        "",
        "| query | off_s | auto_s | force_s | correctness_diff | auto_regions | force_regions | force_kernels | force_runtime_rows | root_cause |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    query_gap_by_id = {entry["query"]: entry for entry in aggregate_rows["query_gap_summary.csv"]}
    for query_id in sorted(by_query):
        off = by_query[query_id].get("off", {})
        auto = by_query[query_id].get("auto", {})
        force = by_query[query_id].get("force", {})
        gap = query_gap_by_id.get(query_id, {})
        lines.append(
            "| {query} | {off_s} | {auto_s} | {force_s} | {correctness} | {auto_regions} | {force_regions} | "
            "{force_kernels} | {runtime_rows} | {root_cause} |".format(
                query=query_id,
                off_s=off.get("total_time_s", ""),
                auto_s=auto.get("total_time_s", ""),
                force_s=force.get("total_time_s", ""),
                correctness=row_correctness_diff(auto) + row_correctness_diff(force),
                auto_regions=auto.get("compiled_regions", ""),
                force_regions=force.get("compiled_regions", ""),
                force_kernels=gap.get("force_compiled_kernels", ""),
                runtime_rows=gap.get("force_runtime_input_rows", ""),
                root_cause=md(gap.get("root_cause", ""), 120),
            )
        )

    lines.extend([
        "",
        "## Policy Totals",
        "",
        "| policy | total_s | relative_to_off | correctness_diff | compiled_regions | skipped_regions | unsupported_regions | runtime_events | runtime_rows | stage_us | codegen_us |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    off_total = sum(row_float(row, "total_time_s") for row in by_policy.get("off", []))
    for policy in args.policies:
        policy_rows = by_policy.get(policy, [])
        if not policy_rows:
            continue
        total_time_s = sum(row_float(row, "total_time_s") for row in policy_rows)
        relative_to_off = total_time_s / off_total if off_total > 0 else 0.0
        lines.append(
            "| {policy} | {total:.6f} | {relative:.4f} | {correctness} | {compiled_regions} | "
            "{skipped_regions} | {unsupported_regions} | {runtime_events} | {runtime_rows} | {stage_us} | {codegen_us} |".format(
                policy=policy,
                total=total_time_s,
                relative=relative_to_off,
                correctness=sum(row_correctness_diff(row) for row in policy_rows),
                compiled_regions=sum(row_int(row, "compiled_regions") for row in policy_rows),
                skipped_regions=sum(row_int(row, "skipped_regions") for row in policy_rows),
                unsupported_regions=sum(row_int(row, "unsupported_regions") for row in policy_rows),
                runtime_events=sum(row_int(row, "runtime_events") for row in policy_rows),
                runtime_rows=sum(row_int(row, "runtime_input_rows") for row in policy_rows),
                stage_us=sum(row_stage_time_us(row) for row in policy_rows),
                codegen_us=sum(row_int(row, "codegen_time_us") for row in policy_rows),
            )
        )

    admission_evidence = aggregate_rows["admission_evidence_summary.csv"]
    if admission_evidence:
        lines.extend([
            "",
            "## Admission Evidence",
            "",
            "| shape_key | mode | form | body | ABI | queries | force_regions | median_speedup | proof_status | root_cause |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
        ])
        for entry in admission_evidence[:20]:
            lines.append(
                "| {shape_key} | {mode} | {form} | {body} | {abi} | {queries} | {force_regions} | {speedup} | "
                "{proof_status} | {root_cause} |".format(
                    shape_key=md(entry["admission_shape_key"], 72),
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    body=entry["execution_body"],
                    abi=entry["candidate_contract_abi"],
                    queries=md(entry["query_examples"], 80),
                    force_regions=entry["force_compiled_regions"],
                    speedup=entry["median_force_speedup_vs_off"],
                    proof_status=entry["proof_status"],
                    root_cause=md(entry["root_cause"], 80),
                )
            )

    region_rows = sorted(
        aggregate_rows["region_decision_summary.csv"],
        key=lambda entry: (-row_int(entry, "count"), entry["query"], entry["policy"], entry["status"]),
    )[:25]
    if region_rows:
        lines.extend([
            "",
            "## Region Decisions",
            "",
            "| query | policy | status | mode | form | body | shape | ABI | count | est_cardinality | pipeline | reason |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
        ])
        for entry in region_rows:
            lines.append(
                "| {query} | {policy} | {status} | {mode} | {form} | {body} | {shape} | {abi} | {count} | "
                "{est} | {pipeline} | {reason} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    body=entry["execution_body"],
                    shape=md(entry["candidate_shape"], 50),
                    abi=entry["candidate_contract_abi"],
                    count=entry["count"],
                    est=entry["max_estimated_cardinality"],
                    pipeline=md(compact_pipeline_shape(entry["candidate_pipeline_shape"]), 90),
                    reason=md(entry["example_reason"], 120),
                )
            )

    kernel_rows = sorted(
        aggregate_rows["kernel_runtime_summary.csv"],
        key=lambda entry: (-row_int(entry, "input_rows"), -row_int(entry, "runtime_time_us"), entry["query"]),
    )[:25]
    if kernel_rows:
        lines.extend([
            "",
            "## Kernel Runtime",
            "",
            "| query | policy | mode | form | body | shape | kernels | reached | row_kernels | input_rows | output_rows | invocations | runtime_us | source_us | body_us |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for entry in kernel_rows:
            lines.append(
                "| {query} | {policy} | {mode} | {form} | {body} | {shape} | {kernels} | {reached} | {row_kernels} | "
                "{input_rows} | {output_rows} | {invocations} | {runtime_us} | {source_us} | {body_us} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    body=entry["execution_body"],
                    shape=md(entry["candidate_shape"], 60),
                    kernels=entry["kernels"],
                    reached=entry["reached_kernels"],
                    row_kernels=entry["row_processing_kernels"],
                    input_rows=entry["input_rows"],
                    output_rows=entry["output_rows"],
                    invocations=entry["invocations"],
                    runtime_us=entry["runtime_time_us"],
                    source_us=entry["source_contract_runtime_time_us"],
                    body_us=entry["generated_body_runtime_time_us"],
                )
            )

    operator_rows = sorted(
        aggregate_rows["operator_profile_summary.csv"],
        key=lambda entry: (-row_int(entry, "operator_time_us"), entry["query"], entry["policy"]),
    )[:25]
    if operator_rows:
        lines.extend([
            "",
            "## Operator Profile",
            "",
            "| query | policy | operator | time_us | pct_query | rows | occurrences | extra_info |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ])
        for entry in operator_rows:
            lines.append(
                "| {query} | {policy} | {operator} | {time_us} | {percent} | {rows} | {occurrences} | {extra} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    operator=entry["operator_name"],
                    time_us=entry["operator_time_us"],
                    percent=entry["percent_query_time"],
                    rows=entry["output_rows"],
                    occurrences=entry["occurrences"],
                    extra=md(entry["extra_info_examples"], 90),
                )
            )

    flow_rows = sorted(
        aggregate_rows["flow_step_summary.csv"],
        key=lambda entry: (-row_int(entry, "event_count") - row_int(entry, "kernel_count"), entry["query"], entry["policy"]),
    )[:25]
    if flow_rows:
        lines.extend([
            "",
            "## Flow Steps",
            "",
            "| query | policy | target | phase | status | mode | form | body | events | kernels | runtime_rows | compile_us | code_size | shape |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ])
        for entry in flow_rows:
            lines.append(
                "| {query} | {policy} | {target} | {phase} | {status} | {mode} | {form} | {body} | {events} | "
                "{kernels} | {runtime_rows} | {compile_us} | {code_size} | {shape} |".format(
                    query=entry["query"],
                    policy=entry["policy"],
                    target=entry["target"],
                    phase=entry["phase"],
                    status=entry["status"],
                    mode=entry["execution_mode"],
                    form=entry["region_execution_form"],
                    body=entry["execution_body"],
                    events=entry["event_count"],
                    kernels=entry["kernel_count"],
                    runtime_rows=entry["input_rows"],
                    compile_us=entry["compile_time_us"],
                    code_size=entry["code_size"],
                    shape=md(entry["candidate_shape"], 70),
                )
            )

    artifact_names = "`, `".join(name for name, _, _, _ in AGGREGATE_CSVS)
    lines.extend([
        "",
        "Detailed per-query event, counter, kernel-counter, and profile files are in this directory.",
        f"Stable aggregate CSVs: `{artifact_names}`.",
    ])
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
    parser.add_argument(
        "--calibrate-auto-from-force",
        action="store_true",
        help="Promote force-winning compiled region shapes into an imported admission profile before later auto runs.",
    )
    parser.add_argument(
        "--auto-profile-min-speedup",
        type=float,
        default=1.0,
        help="Minimum force/off speedup required before a compiled region shape is added to the auto admission profile.",
    )
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--event-log-size", type=int, default=200000)
    parser.add_argument("--trace-runtime", action="store_true")
    parser.add_argument("--dump-ir", action="store_true")
    parser.add_argument(
        "--jit-verify",
        action="store_true",
        help="run compiled-region kernels against DuckDB reference execution during the trace",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    args.duckdb = args.duckdb.resolve()
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")
    if args.calibrate_auto_from_force:
        policy_positions = {policy: idx for idx, policy in enumerate(args.policies)}
        if not (
            "off" in policy_positions
            and "force" in policy_positions
            and "auto" in policy_positions
            and policy_positions["off"] < policy_positions["force"] < policy_positions["auto"]
        ):
            raise TraceConfigurationError("--calibrate-auto-from-force requires --policies off force auto")
        if args.auto_profile_min_speedup < 1:
            raise TraceConfigurationError("--auto-profile-min-speedup must be >= 1 for measured auto admission")

    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("tpch_trace")
    out_dir = prepare_trace_output_directory(args.out_dir)

    db_path, temp_dir = prepare_tpch_database(args)

    rows = []
    admission_profile = []
    try:
        for query_id in args.queries:
            query_id = f"{int(query_id):02d}"
            query_sql = read_query(root, query_id)
            create_baseline(args, db_path, query_id, query_sql)
            query_rows = {}
            for policy in args.policies:
                policy_profile = admission_profile if args.calibrate_auto_from_force and policy == "auto" else None
                row = run_policy_trace(args, db_path, out_dir, query_id, query_sql, policy, policy_profile)
                rows.append(row)
                query_rows[policy] = row
                if args.calibrate_auto_from_force and policy == "force" and "off" in query_rows:
                    for rule in collect_force_admission_profile_rules(args, out_dir, query_rows["off"], row):
                        merge_admission_profile_rule(admission_profile, rule)
        write_summary(out_dir, rows)
        aggregate_rows = write_aggregate_csvs(out_dir, rows)
        write_report(args, out_dir, rows, aggregate_rows)
        write_manifest(args, out_dir, rows, aggregate_rows, db_path, temp_dir)
        print(f"trace output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        for name, label, _, _ in AGGREGATE_CSVS:
            print(f"{label}: {out_dir / name}")
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
