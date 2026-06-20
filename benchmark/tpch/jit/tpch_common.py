#!/usr/bin/env python3
#
# TPC-H-specific helpers for the execution-region benchmark harness.

import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import run_duckdb


DEFAULT_QUERIES = tuple(f"{query_id:02d}" for query_id in range(1, 23))
DEFAULT_POLICIES = ("off", "auto", "force")
TPCH_TABLES = ("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem")


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
