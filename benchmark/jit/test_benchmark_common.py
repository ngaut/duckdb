#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_common import (  # noqa: E402
    TimedMaterializedAttemptGroup,
    TimedMaterializedAttemptSpec,
    duckdb_shell_quote,
    parse_shell_timers_us,
    timed_materialized_attempt_groups,
    timed_materialized_artifact_paths,
)


class TestTimedMaterializedAttemptBatch(unittest.TestCase):
    def test_reopens_database_for_every_sample_in_one_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db_path = root / 'database with "quotes".duckdb'
            attempts = [
                TimedMaterializedAttemptSpec(
                    setup_sql="LOAD jit_sljit;",
                    table_name=f"result_{index}",
                    query_sql=f"SELECT {index} AS value",
                    artifact_path=root / f"attempt_{index}.json",
                    label=f"attempt {index}",
                    validation_sql=f"SELECT {index} AS value",
                    cleanup_sql=f"DROP TABLE result_{index};",
                    collect_counters=True,
                )
                for index in (1, 2)
            ]

            def fake_run_duckdb(duckdb: Path, initial_db: Path, sql: str, label: str):
                self.assertEqual(duckdb, Path("duckdb"))
                self.assertEqual(initial_db, Path(":memory:"))
                self.assertEqual(sql.count(f".open {duckdb_shell_quote(db_path.resolve())}"), 3)
                self.assertEqual(sql.count(".open :memory:"), 3)
                self.assertLess(
                    sql.index("CREATE TABLE fixture"),
                    sql.index("CREATE OR REPLACE TABLE result_1"),
                )
                self.assertIn("generic probe", label)
                for index, attempt in enumerate(attempts, start=1):
                    validation_path, counters_path = timed_materialized_artifact_paths(attempt)
                    validation_path.write_text(json.dumps([{"value": index}]), encoding="utf-8")
                    counters_path.write_text(
                        json.dumps([{"status": "compiled", "count": index}]),
                        encoding="utf-8",
                    )
                return subprocess.CompletedProcess(
                    [],
                    0,
                    stdout="Run Time (s): real 0.125\nRun Time (s): real 0.250\n",
                    stderr="",
                )

            args = SimpleNamespace(duckdb=Path("duckdb"))
            group = TimedMaterializedAttemptGroup(
                label="generic probe",
                preparation_sql="CREATE TABLE fixture(value INTEGER);",
                attempts=tuple(attempts),
            )
            with patch("benchmark_common.run_duckdb", side_effect=fake_run_duckdb) as run_duckdb:
                results = timed_materialized_attempt_groups(args, db_path, [group])

            run_duckdb.assert_called_once()
            self.assertEqual([result["query_time_us"] for result in results], [125000, 250000])
            self.assertEqual(results[0]["validation"], [{"value": 1}])
            self.assertEqual(results[1]["counters"], [{"status": "compiled", "count": 2}])
            for attempt in attempts:
                validation_path, counters_path = timed_materialized_artifact_paths(attempt)
                self.assertFalse(validation_path.exists())
                self.assertFalse(counters_path.exists())

    def test_timer_count_is_an_exact_batch_contract(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "expected 2 shell timers"):
            parse_shell_timers_us("Run Time (s): real 0.125", ["first", "second"])

    def test_rejects_a_group_without_timed_attempts(self) -> None:
        group = TimedMaterializedAttemptGroup(label="empty", preparation_sql="SELECT 42;", attempts=())
        with self.assertRaisesRegex(ValueError, "must contain attempts: empty"):
            timed_materialized_attempt_groups(SimpleNamespace(duckdb=Path("duckdb")), Path("db"), [group])


if __name__ == "__main__":
    unittest.main()
