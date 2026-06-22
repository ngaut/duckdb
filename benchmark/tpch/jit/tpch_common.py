#!/usr/bin/env python3
#
# TPC-H-specific helpers for the execution-region benchmark harness.

import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import REGION_SUMMARY_FIELDS, run_duckdb


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

COUNTER_FIELDS = (
    "query",
    "policy",
    "repeat",
    "backend_name",
    "target",
    "status",
    "execution_mode",
    "region_execution_form",
    "execution_body",
    "selected_runner",
    "requested_policy",
    "runner_cost_profile",
    "blocker",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_startup_cost",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner_count",
    "count",
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
    "hash_join_probe_layout",
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
