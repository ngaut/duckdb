#!/usr/bin/env python3
#
# Small shared IO helpers for DuckDB execution-region benchmarks.

import csv
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

REGION_SUMMARY_FIELDS = (
    "compiled_regions",
    "compile_errors",
    "runtime_regions",
    "runtime_events",
    "unsupported_decisions",
    "skipped_decisions",
    "unavailable_decisions",
    "disabled_decisions",
    "decision_time_us",
    "compile_time_us",
    "pipeline_cbo_time_us",
    "graph_build_time_us",
    "candidate_cbo_time_us",
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "codegen_time_us",
    "executable_build_time_us",
    "machine_codegen_time_us",
    "kernel_build_time_us",
    "lazy_codegen_time_us",
    "lazy_machine_codegen_time_us",
    "lazy_code_size",
    "code_size",
    "runtime_time_us",
    "source_runtime_time_us",
    "sink_next_batch_runtime_time_us",
    "generated_runtime_time_us",
)

PROFILE_EVENT_FIELDS = (
    "entry_type",
    "selected_runner",
    "runner_cost_profile",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_generated_stage_count",
    "runner_cost_generated_backend_stage_count",
    "runner_cost_generated_grouped_aggregate_stage_count",
    "runner_cost_native_grouped_state_address_lookup_count",
    "runner_cost_grouped_aggregate_estimated_cardinality",
    "runner_cost_costed_batches",
    "runner_cost_source_contract_input_rows",
    "runner_cost_source_contract_input_batches",
    "runner_cost_source_contract_output_cardinality_unknown",
    "runner_cost_materialization_elision_count",
    "runner_cost_selected_hash_join_filter_materialization_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_hash_join_build_sink_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_grouped_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_input_scope",
    "runner_cost_generated_work_class",
    "runner_cost_native_protocol_class",
    "runner_cost_admission_class",
    "runner_cost_selection_reason",
    "runner_cost_required_runtime_proofs",
    "runner_cost_generated_expression_work",
    "runner_cost_generated_stage_work",
    "runner_cost_generated_backend_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_selected_hash_join_filter_materialization_penalty",
    "runner_cost_source_contract_scan_penalty",
    "runner_cost_full_pipeline_work",
    "runner_cost_stateful_protocol_penalty",
    "runner_cost_startup_cost",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner",
    "blocker",
    "pipeline_cbo_time_us",
    "graph_build_time_us",
    "candidate_cbo_time_us",
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "codegen_time_us",
    "executable_build_time_us",
    "machine_codegen_time_us",
    "kernel_build_time_us",
    "lazy_codegen_time_us",
    "lazy_machine_codegen_time_us",
    "lazy_code_size",
    "hash_join_probe_layout",
    "jit_runtime_path_counts",
    "jit_runtime_proof_counts",
    "jit_runtime_delegation_counts",
    "selected_source_execution",
    "selected_uses_scan_filters",
    "candidate_uses_scan_filters",
    "source_stage_runtime_breakdown",
    "generated_stage_runtime_breakdown",
)

SHELL_TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9]+(?:\.[0-9]+)?)")


@dataclass(frozen=True)
class TimedMaterializedAttemptSpec:
    setup_sql: str
    table_name: str
    query_sql: str
    artifact_path: Path
    label: str
    pre_sql: str = ""
    validation_sql: str = ""
    cleanup_sql: str = ""
    collect_counters: bool = False


@dataclass(frozen=True)
class TimedMaterializedAttemptGroup:
    label: str
    preparation_sql: str
    attempts: tuple[TimedMaterializedAttemptSpec, ...]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def normalize_query_ids(query_ids: list[str]) -> list[str]:
    return [f"{int(query_id):02d}" for query_id in query_ids]


def make_output_dir(path: Path | None, prefix: str) -> Path:
    out_dir = path.resolve() if path else Path(tempfile.mkdtemp(prefix=f"duckdb_jit_{prefix}_"))
    if out_dir.exists() and any(out_dir.iterdir()):
        raise RuntimeError(f"benchmark output directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_bool(value: bool) -> str:
    return "true" if value else "false"


def duckdb_shell_quote(value) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def jit_cbo_setting_sql(settings: list[str]) -> list[str]:
    statements = []
    for setting in settings:
        name, separator, value = setting.partition("=")
        if not separator or not name.startswith("jit_cbo_") or not value:
            raise ValueError(f"invalid --jit-cbo-setting {setting!r}, expected jit_cbo_name=value")
        statements.append(f"SET {name}={value};")
    return statements


def discard_query_sql(select_sql: str) -> str:
    return f"CREATE OR REPLACE TEMP TABLE __jit_benchmark_discard AS {select_sql};\nDROP TABLE __jit_benchmark_discard;"


def jit_setup_sql(
    args,
    policy: str,
    *,
    jit_verify=False,
    trace_runtime=False,
    trace_decisions=False,
    event_log_size=None,
    reset_events=False,
    reset_counters=False,
) -> str:
    jit_enabled = policy != "off"
    event_log_size = args.event_log_size if event_log_size is None else event_log_size
    statements = [
        f"LOAD {args.jit_extension};",
        f"SET threads={args.threads};",
        f"SET enable_jit={sql_bool(jit_enabled)};",
        f"SET jit_backend={sql_quote(args.backend)};",
        f"SET jit_policy={sql_quote(policy)};",
        f"SET jit_verify={sql_bool(jit_verify or getattr(args, 'jit_verify', False))};",
        "SET jit_dump_ir=false;",
        f"SET jit_trace_runtime={sql_bool(trace_runtime)};",
        f"SET jit_trace_decisions={sql_bool(trace_decisions)};",
        f"SET jit_event_log_size={event_log_size};",
    ]
    statements.extend(jit_cbo_setting_sql(getattr(args, "jit_cbo_setting", [])))
    if reset_events:
        statements.append(discard_query_sql("SELECT * FROM duckdb_jit_clear_events()"))
    if reset_counters:
        statements.append(discard_query_sql("SELECT * FROM duckdb_jit_clear_counters()"))
    return "\n".join(statements) + "\n"


def run_duckdb(duckdb: Path, db_path: Path, sql: str, label: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [str(duckdb), str(db_path)],
        input=f".bail on\n{sql}\n",
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


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise AssertionError(f"missing required benchmark artifact: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def require_fields(row: dict, fields: tuple[str, ...]) -> dict:
    missing = [field for field in fields if field not in row]
    if missing:
        raise KeyError(f"missing required fields: {', '.join(missing)}")
    return {field: row[field] for field in fields}


def read_profile_json(path: Path) -> dict | list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    return 0 if value in ("", None) else int(value)


def row_float(row: dict, field: str) -> float:
    value = row.get(field, "")
    return 0.0 if value in ("", None) else float(value)


def row_bool(row: dict, field: str) -> bool:
    value = row.get(field, False)
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def profile_query_time_us(profile: dict) -> int:
    value = (profile.get("query") or {}).get("total_time", 0)
    return 0 if value in ("", None) else int(round(float(value) * 1_000_000))


def counter_region_summary(counter_rows: list[dict]) -> dict:
    summary = {field: 0 for field in REGION_SUMMARY_FIELDS}
    for row in counter_rows:
        status = row.get("status", "")
        count = row_int(row, "count")
        if status == "compiled":
            summary["compiled_regions"] += count
        elif status == "error":
            summary["compile_errors"] += count
        elif status == "unsupported":
            summary["unsupported_decisions"] += count
        elif status == "skipped":
            summary["skipped_decisions"] += count
        elif status == "unavailable":
            summary["unavailable_decisions"] += count
        elif status == "disabled":
            summary["disabled_decisions"] += count
        elif status in ("executed", "source_contract"):
            summary["runtime_events"] += count
            if row_int(row, "invocation_count") > 0 or row_int(row, "source_contract_invocation_count") > 0:
                summary["runtime_regions"] += count
        summary["decision_time_us"] += row_int(row, "decision_time_us")
        summary["compile_time_us"] += row_int(row, "compile_time_us")
        summary["pipeline_cbo_time_us"] += row_int(row, "pipeline_cbo_time_us")
        summary["graph_build_time_us"] += row_int(row, "graph_build_time_us")
        summary["candidate_cbo_time_us"] += row_int(row, "candidate_cbo_time_us")
        summary["ir_lowering_time_us"] += row_int(row, "ir_lowering_time_us")
        summary["backend_analysis_time_us"] += row_int(row, "backend_analysis_time_us")
        summary["codegen_time_us"] += row_int(row, "codegen_time_us")
        summary["executable_build_time_us"] += row_int(row, "executable_build_time_us")
        summary["machine_codegen_time_us"] += row_int(row, "machine_codegen_time_us")
        summary["kernel_build_time_us"] += row_int(row, "kernel_build_time_us")
        summary["lazy_codegen_time_us"] += row_int(row, "lazy_codegen_time_us")
        summary["lazy_machine_codegen_time_us"] += row_int(row, "lazy_machine_codegen_time_us")
        summary["lazy_code_size"] += row_int(row, "lazy_code_size")
        summary["code_size"] += row_int(row, "code_size")
        summary["runtime_time_us"] += row_int(row, "runtime_time_us")
        summary["source_runtime_time_us"] += row_int(row, "source_contract_runtime_time_us")
        summary["sink_next_batch_runtime_time_us"] += row_int(row, "sink_next_batch_runtime_time_us")
        summary["generated_runtime_time_us"] += row_int(row, "generated_body_runtime_time_us")
    return summary


def materialize_query(args, db_path: Path, setup_sql: str, table_name: str, query_sql: str, label: str) -> None:
    run_duckdb(
        args.duckdb,
        db_path,
        f"{setup_sql}\nCREATE OR REPLACE TABLE {table_name} AS\n{query_sql};",
        label,
    )


def profile_materialized_attempt(
    args,
    db_path: Path,
    setup_sql: str,
    table_name: str,
    query_sql: str,
    profile_path: Path,
    label: str,
    *,
    pre_sql: str = "",
    validation_sql: str = "",
    cleanup_sql: str = "",
    collect_counters: bool = False,
) -> dict:
    if profile_path.exists():
        profile_path.unlink()
    validation_path = profile_path.with_name(f"{profile_path.stem}_validation.json")
    if validation_path.exists():
        validation_path.unlink()
    counters_path = profile_path.with_name(f"{profile_path.stem}_counters.json")
    if counters_path.exists():
        counters_path.unlink()

    statements = [setup_sql]
    if pre_sql.strip():
        statements.append(pre_sql)
    statements.extend(
        [
            "PRAGMA enable_profiling=json;",
            "PRAGMA profiling_mode=detailed;",
            f"PRAGMA profiling_output={sql_quote(profile_path)};",
            f"CREATE OR REPLACE TABLE {table_name} AS\n{query_sql};",
            "PRAGMA disable_profiling;",
        ]
    )
    if collect_counters:
        statements.append(".mode json")
        statements.append(f".once {sql_quote(counters_path)}")
        statements.append("SELECT * FROM duckdb_jit_counters();")
    if validation_sql.strip():
        statements.append(".mode json")
        statements.append(f".once {sql_quote(validation_path)}")
        statements.append(validation_sql)
    if cleanup_sql.strip():
        statements.append(cleanup_sql)

    run_duckdb(args.duckdb, db_path, "\n".join(statements), label)
    if not profile_path.exists():
        raise RuntimeError(f"profile JSON was not written: {profile_path}")

    validation_rows = []
    if validation_sql.strip():
        if not validation_path.exists():
            raise RuntimeError(f"validation JSON was not written during {label}: {validation_path}")
        validation_rows = read_profile_json(validation_path)
        validation_path.unlink()

    counter_rows = []
    if collect_counters:
        if not counters_path.exists():
            raise RuntimeError(f"counter JSON was not written during {label}: {counters_path}")
        counter_rows = read_profile_json(counters_path)
        counters_path.unlink()

    return {
        "profile": read_profile_json(profile_path),
        "validation": validation_rows,
        "counters": counter_rows,
    }


def parse_shell_timers_us(output: str, labels: list[str]) -> list[int]:
    matches = SHELL_TIMER_RE.findall(output)
    if len(matches) != len(labels):
        joined_labels = ", ".join(labels)
        raise RuntimeError(f"expected {len(labels)} shell timers during {joined_labels}, found {len(matches)}")
    return [int(round(float(match) * 1_000_000)) for match in matches]


def parse_shell_timer_us(output: str, label: str) -> int:
    return parse_shell_timers_us(output, [label])[0]


def timed_materialized_artifact_paths(
    attempt: TimedMaterializedAttemptSpec,
) -> tuple[Path, Path]:
    validation_path = attempt.artifact_path.with_name(f"{attempt.artifact_path.stem}_validation.json")
    counters_path = attempt.artifact_path.with_name(f"{attempt.artifact_path.stem}_counters.json")
    return validation_path, counters_path


def prepare_timed_materialized_artifacts(attempt: TimedMaterializedAttemptSpec) -> None:
    for path in timed_materialized_artifact_paths(attempt):
        if path.exists():
            path.unlink()


def timed_materialized_statements(attempt: TimedMaterializedAttemptSpec) -> list[str]:
    validation_path, counters_path = timed_materialized_artifact_paths(attempt)
    statements = [attempt.setup_sql]
    if attempt.pre_sql.strip():
        statements.append(attempt.pre_sql)
    statements.extend(
        [
            ".timer on",
            f"CREATE OR REPLACE TABLE {attempt.table_name} AS\n{attempt.query_sql};",
            ".timer off",
        ]
    )
    if attempt.collect_counters:
        statements.append(".mode json")
        statements.append(f".once {sql_quote(counters_path)}")
        statements.append("SELECT * FROM duckdb_jit_counters();")
    if attempt.validation_sql.strip():
        statements.append(".mode json")
        statements.append(f".once {sql_quote(validation_path)}")
        statements.append(attempt.validation_sql)
    if attempt.cleanup_sql.strip():
        statements.append(attempt.cleanup_sql)
    return statements


def read_timed_materialized_attempt(attempt: TimedMaterializedAttemptSpec, query_time_us: int) -> dict:
    validation_path, counters_path = timed_materialized_artifact_paths(attempt)
    validation_rows = []
    if attempt.validation_sql.strip():
        if not validation_path.exists():
            raise RuntimeError(f"validation JSON was not written during {attempt.label}: {validation_path}")
        validation_rows = read_profile_json(validation_path)
        validation_path.unlink()

    counter_rows = []
    if attempt.collect_counters:
        if not counters_path.exists():
            raise RuntimeError(f"counter JSON was not written during {attempt.label}: {counters_path}")
        counter_rows = read_profile_json(counters_path)
        counters_path.unlink()

    return {
        "query_time_us": query_time_us,
        "validation": validation_rows,
        "counters": counter_rows,
    }


def timed_materialized_attempt_groups(
    args,
    db_path: Path,
    groups: list[TimedMaterializedAttemptGroup],
) -> list[dict]:
    if not groups:
        return []
    empty_groups = [group.label for group in groups if not group.attempts]
    if empty_groups:
        raise ValueError(f"timed workload groups must contain attempts: {', '.join(empty_groups)}")

    statements = []
    all_attempts = []
    quoted_db_path = duckdb_shell_quote(db_path.resolve())
    for group in groups:
        if group.preparation_sql.strip():
            statements.append(f".open {quoted_db_path}")
            statements.append(group.preparation_sql)
            # Closing after preparation checkpoints the workload before any
            # query timer starts, matching the former preparation process.
            statements.append(".open :memory:")
        for attempt in group.attempts:
            prepare_timed_materialized_artifacts(attempt)
            statements.append(f".open {quoted_db_path}")
            statements.extend(timed_materialized_statements(attempt))
            # Reopening the checkpointed database before every sample preserves
            # fresh buffer-manager and connection state without another OS exec.
            statements.append(".open :memory:")
            all_attempts.append(attempt)

    group_range = groups[0].label if len(groups) == 1 else f"{groups[0].label} through {groups[-1].label}"
    batch_label = f"timed workload groups ({group_range})"
    result = run_duckdb(args.duckdb, Path(":memory:"), "\n".join(statements), batch_label)
    query_times = parse_shell_timers_us(
        result.stdout + "\n" + result.stderr,
        [attempt.label for attempt in all_attempts],
    )
    return [
        read_timed_materialized_attempt(attempt, query_time_us)
        for attempt, query_time_us in zip(all_attempts, query_times)
    ]


def timed_materialized_attempt(
    args,
    db_path: Path,
    setup_sql: str,
    table_name: str,
    query_sql: str,
    artifact_path: Path,
    label: str,
    *,
    pre_sql: str = "",
    validation_sql: str = "",
    cleanup_sql: str = "",
    collect_counters: bool = False,
) -> dict:
    attempt = TimedMaterializedAttemptSpec(
        setup_sql=setup_sql,
        table_name=table_name,
        query_sql=query_sql,
        artifact_path=artifact_path,
        label=label,
        pre_sql=pre_sql,
        validation_sql=validation_sql,
        cleanup_sql=cleanup_sql,
        collect_counters=collect_counters,
    )
    prepare_timed_materialized_artifacts(attempt)
    result = run_duckdb(args.duckdb, db_path, "\n".join(timed_materialized_statements(attempt)), label)
    query_time_us = parse_shell_timer_us(result.stdout + "\n" + result.stderr, label)
    return read_timed_materialized_attempt(attempt, query_time_us)


def correctness_sql(baseline_table: str, result_table: str) -> str:
    return f"""
SELECT
    (SELECT count(*) FROM {baseline_table})::BIGINT AS baseline_rows,
    (SELECT count(*) FROM {result_table})::BIGINT AS result_rows,
    (SELECT count(*) FROM (SELECT * FROM {result_table} EXCEPT ALL SELECT * FROM {baseline_table}))::BIGINT
        AS result_minus_baseline,
    (SELECT count(*) FROM (SELECT * FROM {baseline_table} EXCEPT ALL SELECT * FROM {result_table}))::BIGINT
        AS baseline_minus_result;
"""


def correctness_from_rows(rows, result_table: str) -> dict:
    if not isinstance(rows, list) or len(rows) != 1:
        raise RuntimeError(f"unexpected correctness JSON for {result_table}: {rows}")
    row = rows[0]
    row["correctness_diff"] = int(row["result_minus_baseline"]) + int(row["baseline_minus_result"])
    return row


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_columns(rows: list[dict], columns: tuple[str, ...], label: str = "summary.csv") -> None:
    require(bool(rows), f"{label}: expected rows")
    missing = [column for column in columns if column not in rows[0]]
    require(not missing, f"{label}: missing required columns {missing}")


def verify_profile(trace_dir: Path, row: dict, *, require_regions: bool = True) -> dict:
    profile_path = trace_dir / row["profile_json"]
    require(profile_path.exists(), f"missing profile JSON: {profile_path}")
    profile = read_profile_json(profile_path)
    require(
        row_float(profile.get("query") or {}, "total_time") > 0,
        f"{profile_path.name}: missing query timing",
    )
    require("operator" in profile, f"{profile_path.name}: missing operator tree")
    regions = profile.get("execution_regions") or {}
    if require_regions:
        require(bool(regions), f"{profile_path.name}: missing execution_regions diagnostics")
        require(
            "events" in regions,
            f"{profile_path.name}: missing execution_regions.events",
        )
        events = regions.get("events") or []
        if events:
            missing = [name for name in PROFILE_EVENT_FIELDS if name not in events[0]]
            require(
                not missing,
                f"{profile_path.name}: execution_regions.events missing fields {missing}",
            )
    return profile
