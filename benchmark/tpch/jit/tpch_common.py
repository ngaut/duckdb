#!/usr/bin/env python3
#
# TPC-H-specific helpers for the execution-region benchmark harness.

import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import REGION_SUMMARY_FIELDS, normalize_query_ids, run_duckdb

DEFAULT_QUERIES = tuple(f"{query_id:02d}" for query_id in range(1, 23))
DEFAULT_POLICIES = ("off", "auto")
TPCH_TABLES = ("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem")

SUMMARY_FIELDS = (
    "query",
    "policy",
    "run_count",
    "min_s",
    "median_s",
    "mean_s",
    "max_s",
    "speedup_vs_off_median",
    "correctness_diff",
    *REGION_SUMMARY_FIELDS,
)

RUN_FIELDS = (
    "query",
    "policy",
    "repeat",
    "timing_mode",
    "query_time_us",
    "correctness_diff",
    *REGION_SUMMARY_FIELDS,
    "profile_json",
)

COUNTER_BASE_FIELDS = (
    "query",
    "policy",
    "repeat",
    "backend_name",
    "status",
    "execution_mode",
    "selected_runner",
    "blocker",
    "count",
    "decision_time_us",
    "compile_time_us",
    "code_size",
)

COUNTER_RUNTIME_FIELDS = (
    "input_rows",
    "output_rows",
    "invocation_count",
    "runtime_time_us",
    "source_contract_output_rows",
    "source_contract_invocation_count",
    "source_contract_runtime_time_us",
    "source_stage_runtime_breakdown",
    "source_stage_count_breakdown",
    "sink_next_batch_invocation_count",
    "sink_next_batch_runtime_time_us",
    "generated_body_runtime_time_us",
    "generated_stage_runtime_breakdown",
    "generated_stage_count_breakdown",
)

COUNTER_STAGE_TIMING_FIELDS = (
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "codegen_time_us",
    "pipeline_cbo_time_us",
    "graph_build_time_us",
    "candidate_cbo_time_us",
    "executable_build_time_us",
    "machine_codegen_time_us",
    "kernel_build_time_us",
)

COUNTER_JIT_RUNTIME_FIELDS = (
    "lazy_codegen_time_us",
    "lazy_machine_codegen_time_us",
    "lazy_code_size",
    "hash_join_probe_layout",
    "jit_runtime_path_counts",
    "jit_materialization_boundary_counts",
)

COUNTER_RUNNER_COST_PROFILE_FIELDS = (
    "runner_cost_profile",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_generated_backend_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_materialization_source_append_count",
    "runner_cost_unfused_mark_filter_aggregate_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_hash_join_build_sink_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_grouped_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_input_scope",
    "runner_cost_admission_class",
    "runner_cost_selection_reason",
)

COUNTER_RUNNER_COST_WORK_FIELDS = (
    "runner_cost_generated_expression_work",
    "runner_cost_generated_stage_work",
    "runner_cost_generated_backend_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_materialization_source_append_penalty",
    "runner_cost_unfused_mark_filter_aggregate_penalty",
    "runner_cost_full_pipeline_work",
    "runner_cost_stateful_protocol_penalty",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_startup_cost",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_compiled_vectorized_runner_benefit",
    "runner_cost_compiled_vectorized_startup_cost",
    "runner_cost_compiled_vectorized_required_benefit",
    "runner_cost_compiled_vectorized_net_benefit",
    "runner_cost_gpu_runner_benefit",
    "runner_cost_gpu_transfer_cost",
    "runner_cost_gpu_startup_cost",
    "runner_cost_gpu_required_benefit",
    "runner_cost_gpu_net_benefit",
)

COUNTER_RUNNER_SELECTION_FIELDS = (
    "runner_cost_selected_accelerated_runner_count",
    "runner_cost_selected_compiled_vectorized_runner_count",
    "runner_cost_selected_gpu_runner_count",
)

COUNTER_FIELDS = (
    *COUNTER_BASE_FIELDS,
    *COUNTER_RUNTIME_FIELDS,
    *COUNTER_STAGE_TIMING_FIELDS,
    *COUNTER_JIT_RUNTIME_FIELDS,
    *COUNTER_RUNNER_COST_PROFILE_FIELDS,
    *COUNTER_RUNNER_COST_WORK_FIELDS,
    *COUNTER_RUNNER_SELECTION_FIELDS,
)

RUNNER_COST_COMPONENT_FIELDS = (
    "runner_cost_generated_expression_work",
    "runner_cost_generated_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_materialization_source_append_penalty",
    "runner_cost_unfused_mark_filter_aggregate_penalty",
    "runner_cost_full_pipeline_work",
    "runner_cost_stateful_protocol_penalty",
)

PERFORMANCE_GAP_FIELDS = (
    "query",
    "off_median_s",
    "auto_median_s",
    "auto_speedup_vs_off",
    "auto_compiled_regions",
    "auto_unsupported_decisions",
    "auto_skipped_decisions",
    "auto_decision_time_us",
    "auto_compile_time_us",
    "auto_pipeline_cbo_time_us",
    "auto_graph_build_time_us",
    "auto_candidate_cbo_time_us",
    "auto_ir_lowering_time_us",
    "auto_backend_analysis_time_us",
    "auto_codegen_time_us",
    "auto_executable_build_time_us",
    "auto_machine_codegen_time_us",
    "auto_kernel_build_time_us",
    "auto_lazy_codegen_time_us",
    "auto_lazy_machine_codegen_time_us",
    "auto_lazy_code_size",
    "auto_primary_blocker",
    "auto_primary_blocker_count",
    "auto_runner_cost_benefit",
    "auto_runner_cost_startup_cost",
    "auto_runner_cost_required_benefit",
    "auto_runner_cost_net_benefit",
    "auto_runner_cost_selected_accelerated_runner_count",
)


class TPCHConfigurationError(RuntimeError):
    pass


def normalize_tpch_query_ids(query_ids: list[str]) -> list[str]:
    if any(query_id.lower() == "all" for query_id in query_ids):
        if len(query_ids) != 1 or query_ids[0].lower() != "all":
            raise TPCHConfigurationError("--queries all cannot be combined with explicit query ids")
        return list(DEFAULT_QUERIES)
    try:
        normalized = normalize_query_ids(query_ids)
    except ValueError as exc:
        raise TPCHConfigurationError("--queries must be integers or all") from exc
    unknown = sorted(set(normalized) - set(DEFAULT_QUERIES))
    if unknown:
        raise TPCHConfigurationError(f"unknown TPC-H query id(s): {', '.join(unknown)}")
    return normalized


def read_query(root: Path, query_id: str) -> str:
    path = root / "extension" / "tpch" / "dbgen" / "queries" / f"q{int(query_id):02d}.sql"
    query = path.read_text(encoding="utf-8").strip()
    while query.endswith(";"):
        query = query[:-1].strip()
    if not query:
        raise RuntimeError(f"empty TPC-H query file: {path}")
    return query


def create_tpch_database(args, db_path: Path) -> None:
    run_duckdb(args.duckdb, db_path, f"LOAD tpch;\nCALL dbgen(sf={args.scale_factor});", "TPC-H dbgen")


def validate_tpch_database(args, db_path: Path) -> None:
    checks = "\n".join(f"SELECT 1 FROM {table_name} LIMIT 0;" for table_name in TPCH_TABLES)
    run_duckdb(args.duckdb, db_path, checks, "TPC-H schema validation")


def prepare_tpch_database(args) -> tuple[Path, Optional[Path]]:
    if args.use_existing_db:
        if args.db is None:
            raise TPCHConfigurationError("--use-existing-db requires --db")
        db_path = args.db.resolve()
        if not db_path.exists():
            raise TPCHConfigurationError(f"database does not exist: {db_path}")
        validate_tpch_database(args, db_path)
        return db_path, None

    if args.db is not None:
        db_path = args.db.resolve()
        if db_path.exists():
            raise TPCHConfigurationError(f"--db already exists: {db_path}; use --use-existing-db to reuse it")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        create_tpch_database(args, db_path)
        validate_tpch_database(args, db_path)
        return db_path, None

    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_jit_tpch_benchmark_"))
    db_path = temp_dir / "tpch.duckdb"
    create_tpch_database(args, db_path)
    validate_tpch_database(args, db_path)
    return db_path, temp_dir


def cleanup_tpch_database(temp_dir: Optional[Path]) -> None:
    if temp_dir is not None:
        shutil.rmtree(temp_dir, ignore_errors=True)
