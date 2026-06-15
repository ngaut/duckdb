#!/usr/bin/env python3
#
# Production-style timing harness for DuckDB JIT on TPC-H.
#
# tpch_trace.py is the diagnostic tool: it emits compile events, runtime trace
# facts, IR, capability gaps, and profile attribution. This harness answers a
# narrower performance question with repeated runs and event retention disabled
# by default: does a policy run faster on the actual query path?

import argparse
import collections
import csv
import json
import statistics
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import default_trace_output_directory, prepare_trace_output_directory, write_trace_manifest

from tpch_schema import (
    BENCHMARK_ADMISSION_PROOF_GAP_FIELDS as ADMISSION_PROOF_GAP_FIELDS,
    BENCHMARK_CORRECTNESS_SUMMARY_FIELDS as CORRECTNESS_SUMMARY_FIELDS,
    BENCHMARK_POLICY_SUMMARY_FIELDS as POLICY_SUMMARY_FIELDS,
    BENCHMARK_RUN_FIELDS as RUN_FIELDS,
    BENCHMARK_SUMMARY_FIELDS as SUMMARY_FIELDS,
    CANDIDATE_TRAIT_FIELDS,
    CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES,
    DECISION_COUNTER_SUMMARY_FIELDS,
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    OPERATOR_PROFILE_SUMMARY_FIELDS,
    STAGE_FIELDS,
    configure_csv_field_size_limit,
)

configure_csv_field_size_limit()

from tpch_trace import (
    TraceConfigurationError,
    collect_operator_profile_summary,
    copy_statement,
    create_baseline,
    prepare_tpch_database,
    profile_cpu_time_us,
    profile_operator_count,
    profile_operator_time_us,
    profile_query_time_us,
    read_profile_json,
    read_query,
    read_single_csv_row,
    repo_root,
    row_int,
    run_duckdb,
    setting_sql,
    write_csv_rows,
)


def seconds(value_us: int) -> float:
    return float(value_us) / 1000000.0


def format_seconds(value_us: int) -> str:
    return f"{seconds(value_us):.9f}"


def median_int(values: list) -> int:
    if not values:
        return 0
    return int(round(statistics.median(values)))


def mean_int(values: list) -> int:
    if not values:
        return 0
    return int(round(statistics.mean(values)))


def read_csv_rows(path: Path) -> list:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sum_decision_counter(
    rows: list, *, target: str, status: Optional[str] = None, execution_mode: Optional[str] = None
) -> int:
    total = 0
    for row in rows:
        if row.get("target") != target:
            continue
        if status is not None and row.get("status") != status:
            continue
        if execution_mode is not None and row.get("execution_mode") != execution_mode:
            continue
        total += row_int(row, "count")
    return total


def write_summary(out_dir: Path, rows: list) -> None:
    csv_rows = [{field: row[field] for field in RUN_FIELDS} for row in rows]
    write_csv_rows(out_dir / "runs.csv", RUN_FIELDS, csv_rows)
    summary_rows = collect_summary(rows)
    policy_rows = collect_policy_summary(summary_rows)
    correctness_rows = collect_correctness_summary(rows)
    decision_counter_rows = collect_decision_counter_summary(rows)
    write_csv_rows(out_dir / "summary.csv", SUMMARY_FIELDS, summary_rows)
    write_csv_rows(out_dir / "query_summary.csv", SUMMARY_FIELDS, summary_rows)
    write_csv_rows(out_dir / "policy_summary.csv", POLICY_SUMMARY_FIELDS, policy_rows)
    write_csv_rows(out_dir / "correctness_summary.csv", CORRECTNESS_SUMMARY_FIELDS, correctness_rows)
    write_csv_rows(
        out_dir / "operator_profile_summary.csv",
        OPERATOR_PROFILE_SUMMARY_FIELDS,
        collect_operator_profile_summary(out_dir, rows),
    )
    write_csv_rows(
        out_dir / "decision_counter_summary.csv",
        DECISION_COUNTER_SUMMARY_FIELDS,
        decision_counter_rows,
    )
    write_csv_rows(
        out_dir / "admission_proof_gap_summary.csv",
        ADMISSION_PROOF_GAP_FIELDS,
        collect_admission_proof_gap_summary(summary_rows, decision_counter_rows),
    )


def collect_summary(rows: list) -> list:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["policy"])].append(row)

    off_medians = {}
    for (query_id, policy), group in grouped.items():
        if policy == "off":
            off_medians[query_id] = median_int([row_int(row, "profile_query_time_us") for row in group])

    result = []
    policy_order = {policy: idx for idx, policy in enumerate(DEFAULT_POLICIES)}
    for (query_id, policy), group in sorted(
        grouped.items(), key=lambda item: (item[0][0], policy_order.get(item[0][1], 100))
    ):
        timings = [row_int(row, "profile_query_time_us") for row in group]
        median_us = median_int(timings)
        off_median = off_medians.get(query_id, 0)
        speedup = float(off_median) / float(median_us) if off_median > 0 and median_us > 0 else 0.0
        result.append(
            {
                "query": query_id,
                "policy": policy,
                "run_count": len(group),
                "min_s": format_seconds(min(timings) if timings else 0),
                "median_s": format_seconds(median_us),
                "mean_s": format_seconds(mean_int(timings)),
                "max_s": format_seconds(max(timings) if timings else 0),
                "speedup_vs_off_median": f"{speedup:.6f}",
                "faster_than_off_median": str(speedup >= 1.0).lower(),
                "correctness_diff": sum(row_int(row, "correctness_diff") for row in group),
                "compiled_regions": sum(row_int(row, "compiled_regions") for row in group),
                "skipped_regions": sum(row_int(row, "skipped_regions") for row in group),
                "unsupported_regions": sum(row_int(row, "unsupported_regions") for row in group),
                "decision_count": sum(row_int(row, "decision_count") for row in group),
                **{field: sum(row_int(row, field) for row in group) for field in STAGE_FIELDS},
                "timings_s": ";".join(format_seconds(value) for value in timings),
            }
        )
    return result


def collect_policy_summary(summary_rows: list) -> list:
    grouped = collections.defaultdict(list)
    for row in summary_rows:
        grouped[row["policy"]].append(row)
    off_total = sum(seconds_to_us(row["median_s"]) for row in grouped.get("off", []))

    result = []
    policy_order = {policy: idx for idx, policy in enumerate(DEFAULT_POLICIES)}
    for policy, group in sorted(grouped.items(), key=lambda item: policy_order.get(item[0], 100)):
        total_us = sum(seconds_to_us(row["median_s"]) for row in group)
        relative = float(total_us) / float(off_total) if off_total > 0 else 0.0
        result.append(
            {
                "policy": policy,
                "query_count": len(group),
                "run_count": sum(row_int(row, "run_count") for row in group),
                "total_median_s": format_seconds(total_us),
                "relative_to_off": f"{relative:.6f}",
                "faster_queries": sum(1 for row in group if float(row["speedup_vs_off_median"]) > 1.000001),
                "slower_queries": sum(1 for row in group if float(row["speedup_vs_off_median"]) < 0.999999),
                "equal_queries": sum(1 for row in group if 0.999999 <= float(row["speedup_vs_off_median"]) <= 1.000001),
                "correctness_diff": sum(row_int(row, "correctness_diff") for row in group),
                "compiled_regions": sum(row_int(row, "compiled_regions") for row in group),
                "skipped_regions": sum(row_int(row, "skipped_regions") for row in group),
                "unsupported_regions": sum(row_int(row, "unsupported_regions") for row in group),
                "decision_count": sum(row_int(row, "decision_count") for row in group),
                **{field: sum(row_int(row, field) for row in group) for field in STAGE_FIELDS},
            }
        )
    return result


def seconds_to_us(value: str) -> int:
    return int(round(float(value) * 1000000))


def collect_correctness_summary(rows: list) -> list:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["policy"])].append(row)
    result = []
    for (query_id, policy), group in sorted(grouped.items()):
        result_rows = [row_int(row, "result_rows") for row in group]
        result.append(
            {
                "query": query_id,
                "policy": policy,
                "run_count": len(group),
                "baseline_rows": max(row_int(row, "baseline_rows") for row in group),
                "max_result_rows": max(result_rows) if result_rows else 0,
                "min_result_rows": min(result_rows) if result_rows else 0,
                "correctness_diff": sum(row_int(row, "correctness_diff") for row in group),
            }
        )
    return result


def collect_decision_counter_summary(rows: list) -> list:
    result = []
    for row in rows:
        counters_path = Path(row["decision_counters_csv"])
        if not counters_path.is_absolute():
            counters_path = row["_out_dir"] / counters_path
        for counter in read_csv_rows(counters_path):
            result.append(
                {
                    "query": row["query"],
                    "policy": row["policy"],
                    "backend_name": counter.get("backend_name", ""),
                    "target": counter.get("target", ""),
                    "phase": counter.get("phase", ""),
                    "status": counter.get("status", ""),
                    "execution_mode": counter.get("execution_mode", ""),
                    "region_execution_form": counter.get("region_execution_form", ""),
                    "policy_decision": counter.get("policy_decision", ""),
                    "pipeline_shape": counter.get("pipeline_shape", ""),
                    "pipeline_estimated_cardinality": counter.get("pipeline_estimated_cardinality", "0"),
                    "candidate_shape": counter.get("candidate_shape", ""),
                    "candidate_scope": counter.get("candidate_scope", ""),
                    "admission_shape_key": counter.get("admission_shape_key", ""),
                    "admission_rule_present": counter.get("admission_rule_present", ""),
                    "admission_min_cardinality": counter.get("admission_min_cardinality", "0"),
                    "admission_proof": counter.get("admission_proof", ""),
                    "has_admission_score": counter.get("has_admission_score", "false"),
                    "min_admission_score": counter.get("min_admission_score", "0"),
                    "max_admission_score": counter.get("max_admission_score", "0"),
                    **{field: counter.get(field, "") for field in CANDIDATE_TRAIT_FIELDS},
                    "count": counter.get("count", "0"),
                    "max_estimated_cardinality": counter.get("max_estimated_cardinality", "0"),
                    "decision_time_us": counter.get("decision_time_us", "0"),
                    "compile_time_us": counter.get("compile_time_us", "0"),
                    "code_size": counter.get("code_size", "0"),
                    **{field: counter.get(field, "0") for field in STAGE_FIELDS},
                    "example_reason": counter.get("example_reason", ""),
                }
            )
    return result


def format_ratio(value: float) -> str:
    return f"{value:.6f}"


def join_unique(values) -> str:
    return ";".join(sorted(value for value in values if value))


def compact_reason(reason: str, limit: int = 360) -> str:
    if len(reason) <= limit:
        return reason
    return reason[:limit] + "...[truncated]"


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


def collect_admission_proof_gap_summary(summary_rows: list, decision_counter_rows: list) -> list:
    speedup_by_query_policy = {
        (row["query"], row["policy"]): float(row["speedup_vs_off_median"]) for row in summary_rows
    }
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

    force_compiled_keys = set()
    for row in decision_counter_rows:
        if row["target"] != "region":
            continue
        key = (
            row["admission_shape_key"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_scope"],
        )
        if row["policy"] == "force" and row["status"] == "compiled":
            force_compiled_keys.add(key)

    for row in decision_counter_rows:
        if row["target"] != "region":
            continue
        key = (
            row["admission_shape_key"],
            row["execution_mode"],
            row["region_execution_form"],
            row["candidate_shape"],
            row["candidate_scope"],
        )
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
    return sorted(
        result,
        key=lambda row: (
            row["root_cause"] != "missing_measured_auto_admission_proof",
            -row_int(row, "force_winning_queries"),
            -row_int(row, "force_compiled_regions"),
            row["admission_shape_key"],
        ),
    )


def run_policy_benchmark(
    args: argparse.Namespace, db_path: Path, out_dir: Path, query_id: str, query_sql: str, policy: str, repeat: int
) -> dict:
    baseline_table = f"__jit_trace_baseline_q{query_id}"
    result_table = f"__jit_benchmark_result_q{query_id}_{policy}_{repeat}"
    counter_table = f"__jit_benchmark_counter_q{query_id}_{policy}_{repeat}"
    profile_json_path = out_dir / f"q{query_id}_{policy}_r{repeat}_profile.json"
    correctness_path = out_dir / f"q{query_id}_{policy}_r{repeat}_correctness.csv"
    decision_counters_path = out_dir / f"q{query_id}_{policy}_r{repeat}_decision_counters.csv"

    if profile_json_path.exists():
        profile_json_path.unlink()
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, policy)}
SET jit_trace_decisions=false;
SELECT * FROM duckdb_jit_clear_events();
SELECT * FROM duckdb_jit_clear_counters();
PRAGMA enable_profiling=json;
PRAGMA profiling_mode=detailed;
PRAGMA profiling_output={sql_quote(profile_json_path)};
CREATE OR REPLACE TABLE {result_table} AS
{query_sql};
PRAGMA disable_profiling;
{copy_statement(f'''
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
	''', correctness_path)}
DROP TABLE IF EXISTS {result_table};
""",
        f"benchmark q{query_id} {policy} repeat {repeat}",
    )
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, policy)}
SET jit_trace_decisions=true;
DROP TABLE IF EXISTS {counter_table};
SELECT * FROM duckdb_jit_clear_events();
SELECT * FROM duckdb_jit_clear_counters();
CREATE OR REPLACE TABLE {counter_table} AS
{query_sql};
{copy_statement(
    'SELECT * FROM duckdb_jit_decision_counters() ORDER BY backend_name, target, phase, status, '
    'execution_mode, region_execution_form, policy_decision, candidate_scope, candidate_shape, admission_shape_key',
    decision_counters_path,
)}
DROP TABLE IF EXISTS {counter_table};
""",
        f"counter q{query_id} {policy} repeat {repeat}",
    )
    profile = read_profile_json(profile_json_path)
    correctness = read_single_csv_row(correctness_path)
    decision_counter_rows = read_csv_rows(decision_counters_path)
    correctness_diff = row_int(correctness, "result_minus_baseline") + row_int(correctness, "baseline_minus_result")
    return {
        "query": query_id,
        "policy": policy,
        "repeat": repeat,
        "profile_query_time_us": profile_query_time_us(profile),
        "profile_cpu_time_us": profile_cpu_time_us(profile),
        "profile_operator_time_us": profile_operator_time_us(profile),
        "profile_operator_count": profile_operator_count(profile),
        **correctness,
        "correctness_diff": correctness_diff,
        "compiled_regions": sum_decision_counter(decision_counter_rows, target="region", status="compiled"),
        "skipped_regions": sum_decision_counter(decision_counter_rows, target="region", status="skipped"),
        "unsupported_regions": sum_decision_counter(decision_counter_rows, target="region", status="unsupported"),
        "native_region_decisions": sum_decision_counter(
            decision_counter_rows, target="region", execution_mode="native"
        ),
        "non_native_region_decisions": sum(
            row.get("target") == "region" and row.get("execution_mode") != "native"
            for row in decision_counter_rows
        ),
        "executor_fallback_region_decisions": sum_decision_counter(
            decision_counter_rows, target="region", execution_mode="executor_fallback"
        ),
        "unsupported_region_decisions": sum_decision_counter(
            decision_counter_rows, target="region", execution_mode="unsupported"
        ),
        "decision_count": sum(row_int(row, "count") for row in decision_counter_rows),
        **{field: sum(row_int(row, field) for row in decision_counter_rows) for field in STAGE_FIELDS},
        "profile_json": profile_json_path.name,
        "correctness_csv": correctness_path.name,
        "decision_counters_csv": decision_counters_path.name,
        "_out_dir": out_dir,
    }


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_report(args: argparse.Namespace, out_dir: Path) -> None:
    policy_rows = read_csv_rows(out_dir / "policy_summary.csv")
    query_rows = read_csv_rows(out_dir / "summary.csv")
    admission_gap_rows = read_csv_rows(out_dir / "admission_proof_gap_summary.csv")
    lines = [
        "# TPC-H JIT Benchmark Report",
        "",
        f"- duckdb: {args.duckdb}",
        f"- scale_factor: {args.scale_factor}",
        f"- backend: {args.backend}",
        f"- threads: {args.threads}",
        f"- repeats: {args.repeats}",
        f"- event_log_size: {args.event_log_size}",
        f"- jit_verify: {str(args.jit_verify).lower()}",
        "",
        "## Policy Summary",
        "",
        "| policy | total_median_s | relative_to_off | faster_queries | slower_queries | correctness_diff | "
        "compiled_regions | skipped_regions | unsupported_regions |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in policy_rows:
        lines.append(
            "| {policy} | {total_median_s} | {relative_to_off} | {faster_queries} | {slower_queries} | "
            "{correctness_diff} | {compiled_regions} | {skipped_regions} | {unsupported_regions} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Query Summary",
            "",
            "| query | policy | median_s | speedup_vs_off | correctness_diff | compiled_regions | skipped_regions | "
            "unsupported_regions |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in query_rows:
        lines.append(
            "| {query} | {policy} | {median_s} | {speedup_vs_off_median} | {correctness_diff} | "
            "{compiled_regions} | {skipped_regions} | {unsupported_regions} |".format(**row)
        )
    if admission_gap_rows:
        lines.extend(
            [
                "",
                "## Admission Proof Gaps",
                "",
                "| shape_key | mode | form | scope | queries | force_regions | median_speedup | proof_status | root_cause |",
                "| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
            ]
        )
        for row in admission_gap_rows:
            lines.append(
                "| {admission_shape_key} | {execution_mode} | {region_execution_form} | {candidate_scope} | "
                "{query_examples} | {force_compiled_regions} | {median_force_speedup_vs_off} | "
                "{proof_status} | {root_cause} |".format(**row)
            )
    lines.append("")
    lines.append(
        "This harness keeps diagnostic tracing separate from production timing. Use `tpch_trace.py` when you need IR, "
        "runtime events, capability gaps, and per-stage flow facts."
    )
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_manifest(args: argparse.Namespace, out_dir: Path, db_path: Path, temp_dir: Optional[Path]) -> None:
    artifact_names = [
        "runs.csv",
        "summary.csv",
        "query_summary.csv",
        "policy_summary.csv",
        "correctness_summary.csv",
        "operator_profile_summary.csv",
        "decision_counter_summary.csv",
        "admission_proof_gap_summary.csv",
        "report.md",
    ]
    for row in read_csv_rows(out_dir / "runs.csv"):
        artifact_names.append(row["profile_json"])
        artifact_names.append(row["correctness_csv"])
        artifact_names.append(row["decision_counters_csv"])
    write_trace_manifest(
        out_dir,
        kind="tpch_jit_benchmark",
        generator="benchmark/tpch/jit/tpch_benchmark.py",
        configuration={
            "duckdb": str(args.duckdb),
            "scale_factor": args.scale_factor,
            "queries": [f"{int(query_id):02d}" for query_id in args.queries],
            "policies": list(args.policies),
            "backend": args.backend,
            "jit_extension": args.jit_extension,
            "threads": args.threads,
            "repeats": args.repeats,
            "event_log_size": args.event_log_size,
            "jit_verify": args.jit_verify,
            "db_path": str(db_path),
            "temporary_db": temp_dir is not None,
            "kept_db": bool(args.keep_db),
        },
        artifact_names=artifact_names,
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB JIT policies on TPC-H with repeated production timings"
    )
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "release" / "duckdb")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--use-existing-db", action="store_true")
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--scale-factor", type=float, default=1)
    parser.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--jit-verify", action="store_true")
    parser.set_defaults(trace_runtime=False, dump_ir=False)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    args.duckdb = args.duckdb.resolve()
    if args.repeats <= 0:
        raise TraceConfigurationError("--repeats must be positive")
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("tpch_benchmark")
    out_dir = prepare_trace_output_directory(args.out_dir)
    db_path, temp_dir = prepare_tpch_database(args)

    rows = []
    try:
        for query_id in args.queries:
            query_id = f"{int(query_id):02d}"
            query_sql = read_query(root, query_id)
            create_baseline(args, db_path, query_id, query_sql)
            for repeat in range(1, args.repeats + 1):
                for policy in args.policies:
                    rows.append(run_policy_benchmark(args, db_path, out_dir, query_id, query_sql, policy, repeat))
        write_summary(out_dir, rows)
        write_report(args, out_dir)
        write_manifest(args, out_dir, db_path, temp_dir)
        print(f"benchmark output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        print(f"policy summary: {out_dir / 'policy_summary.csv'}")
        print(f"report: {out_dir / 'report.md'}")
    finally:
        if temp_dir is not None and not args.keep_db:
            import shutil

            shutil.rmtree(temp_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
