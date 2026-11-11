import re
import sys
import json
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from unittest import mock

import jit_differential_fuzz
from jit_differential_fuzz import DERIVED_COLUMNS, EDGES, SCHEMA, QueryGenerator, RunResult, classify


class QueryGeneratorTest(unittest.TestCase):
    def test_a_seed_reproduces_its_queries(self) -> None:
        generator_a, generator_b = QueryGenerator(7), QueryGenerator(7)
        self.assertEqual([generator_a.query() for _ in range(50)], [generator_b.query() for _ in range(50)])

    def test_floating_point_columns_are_derived_and_never_summed(self) -> None:
        floats = {name for table, columns in SCHEMA.items() for name, kind in columns.items() if kind == "float"}
        self.assertTrue(floats)
        for table, columns in SCHEMA.items():
            for name, kind in columns.items():
                if kind == "float":
                    self.assertIn(name, DERIVED_COLUMNS[table])
        generator = QueryGenerator(5)
        queries = [generator.query() for _ in range(1000)]
        self.assertTrue(any(name in sql for sql in queries for name in floats))
        for sql in queries:
            # a floating-point sum rounds differently in a different order
            self.assertIsNone(re.search(r"sum\((t\d+\.)?(" + "|".join(floats) + r")\b", sql), sql)

    def test_every_derived_kind_is_generated(self) -> None:
        # TPC-H stores only BIGINT/INTEGER keys, DECIMAL(15,2), DATE and strings; the derived columns give every other
        # type the JIT has native paths for a column, and each must reach generated queries
        derived = {name: SCHEMA[table][name] for table, columns in DERIVED_COLUMNS.items() for name in columns}
        self.assertEqual({"float", "small", "wide", "dec", "bool", "ts"}, set(derived.values()))
        generator = QueryGenerator(11)
        queries = [generator.query() for _ in range(1000)]
        for name in derived:
            self.assertTrue(any(re.search(rf"\b{name}\b", sql) for sql in queries), name)

    def test_columns_of_one_table_are_compared(self) -> None:
        # a comparison between two columns of one table stays a filter above the scan
        generator = QueryGenerator(4)
        queries = [generator.query() for _ in range(300)]
        comparisons = [m for sql in queries for m in re.finditer(r"\b(t\d+)\.(\w+) (?:<|>|<=|>=|=|<>) \1\.(\w+)", sql)]
        self.assertTrue(comparisons)
        self.assertTrue(any(SCHEMA["lineitem"].get(m.group(2)) == "date" for m in comparisons))

    def test_edges_name_schema_columns(self) -> None:
        for left, left_columns, right, right_columns in EDGES:
            for column in left_columns:
                self.assertIn(column, SCHEMA[left])
            for column in right_columns:
                self.assertIn(column, SCHEMA[right])

    def test_join_tree_renders_every_hop(self) -> None:
        generator = QueryGenerator(11)
        kinds = set()
        for _ in range(300):
            tree = generator.join_tree()
            self.assertEqual(len(tree.kinds), len(tree.tables) - 1)
            self.assertEqual(tree.from_clause().count(" ON "), len(tree.tables) - 1)
            kinds.update(tree.kinds)
        self.assertEqual(kinds, {"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"})

    def test_generated_queries_are_deterministic_by_construction(self) -> None:
        generator = QueryGenerator(3)
        for _ in range(500):
            sql = generator.query()
            self.assertEqual(sql.count("("), sql.count(")"), sql)
            self.assertNotIn("avg(", sql)
            for match in re.finditer(r"LIMIT \d+", sql):
                own_select = sql[sql.rfind("SELECT", 0, match.start()) : match.start()]
                # a LIMIT either follows a total ORDER BY of its own SELECT, or only feeds a count
                counted = own_select.startswith("SELECT * FROM c ") and "count(*) FROM (" + own_select in sql
                self.assertTrue("ORDER BY" in own_select or counted, sql)

    def test_predicates_appear_as_values(self) -> None:
        marker = "(TRUE AND FALSE)"
        generator = QueryGenerator(7)
        generator.boolean_expression = lambda columns: marker
        queries = [generator.query() for _ in range(600)]
        self.assertTrue(
            any(marker in sql and "GROUP BY" in sql and sql.index("GROUP BY") < sql.rindex(marker) for sql in queries)
        )
        self.assertTrue(any(f"count({marker})" in sql for sql in queries))
        self.assertTrue(any(f"SELECT {marker}" in sql or f"SELECT DISTINCT {marker}" in sql for sql in queries))

        generator = QueryGenerator(7)
        tree = generator.join_tree()
        expressions = [generator.boolean_expression(tree.columns()) for _ in range(200)]
        self.assertTrue(any(" AND " in e or " OR " in e for e in expressions))
        for expression in expressions:
            self.assertEqual(expression.count("("), expression.count(")"), expression)

    def test_order_sensitive_families_scan_one_table(self) -> None:
        generator = QueryGenerator(9)
        seen = 0
        for _ in range(400):
            sql = generator.query()
            if sql.startswith("CREATE TABLE memory.main.r") or (
                not sql.startswith("SELECT count(*) AS n") and not sql.startswith("WITH")
            ):
                self.assertNotIn(" JOIN ", sql, sql)
                seen += 1
        self.assertGreater(seen, 0)


class DatabaseTest(unittest.TestCase):
    def test_every_referenced_column_gets_nulls(self) -> None:
        sql = jit_differential_fuzz.database_sql(0.1)
        self.assertTrue(sql.startswith("CALL dbgen(sf=0.1);"))
        for table, columns in SCHEMA.items():
            self.assertIn(f"CREATE OR REPLACE TABLE {table} AS SELECT * REPLACE (", sql)
            for column in columns:
                self.assertIn(f"THEN NULL ELSE {column} END AS {column}", sql)

    def test_derived_columns_exist_before_they_get_nulls(self) -> None:
        sql = jit_differential_fuzz.database_sql(0.1)
        for table, columns in DERIVED_COLUMNS.items():
            derive = sql.index(f"CREATE OR REPLACE TABLE {table} AS SELECT *, ")
            nulls = sql.index(f"CREATE OR REPLACE TABLE {table} AS SELECT * REPLACE (")
            self.assertLess(derive, nulls)
            for column, expression in columns.items():
                self.assertIn(f"{expression} AS {column}", sql)
                self.assertIn(column, SCHEMA[table])


class ClassifyTest(unittest.TestCase):
    def test_verdicts(self) -> None:
        off = RunResult(0, "n,h\n1,2", 0.1)
        self.assertEqual(classify(off, RunResult(0, "n,h\n1,2", 0.1)), "ok")
        self.assertEqual(classify(off, RunResult(0, "n,h\n1,3", 0.1)), "mismatch")
        self.assertEqual(classify(off, RunResult(1, "INTERNAL Error: boom", 0.1)), "crash")
        self.assertEqual(classify(off, RunResult(1, "Invalid Input Error: x", 0.1)), "error")
        self.assertEqual(classify(off, RunResult(-1, "TIMEOUT", 0.1)), "timeout")
        self.assertEqual(classify(off, RunResult(1, "Out of Memory Error: failed to pin block", 0.1)), "oom")

    def test_errors_compare_by_signature(self) -> None:
        off = RunResult(1, "Out of Range Error: Overflow in multiplication of INT32 (2147484 * 1000)!", 0.1)
        same = RunResult(1, "Out of Range Error: Overflow in multiplication of INT32 (5999999 * 1000)!", 0.1)
        self.assertEqual(classify(off, same), "ok")
        self.assertEqual(classify(off, RunResult(0, "n,h\n1,2", 0.1)), "mismatch")
        self.assertEqual(classify(off, RunResult(1, "Conversion Error: Could not convert 'x' to INT32", 0.1)), "error")
        reworded = RunResult(1, "Out of Range Error: Overflow in multiplication of DECIMAL", 0.1)
        self.assertEqual(classify(off, reworded), "ok")


class CampaignTest(unittest.TestCase):
    def run_campaign(self, runs, queries=1, replay=None):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            argv = ["--queries", str(queries), "--workers", "1", "--report", str(report)]
            if replay is not None:
                earlier = Path(directory) / "replay.json"
                earlier.write_text(json.dumps(replay))
                argv += ["--replay", str(earlier), "--threads", "1"]
            with (
                mock.patch.object(jit_differential_fuzz, "create_database"),
                mock.patch.object(jit_differential_fuzz.QueryGenerator, "query", return_value="SELECT 1"),
                mock.patch.object(jit_differential_fuzz, "run_query", side_effect=runs),
            ):
                status = jit_differential_fuzz.main(argv)
            return status, json.loads(report.read_text())

    def test_unusable_native_reference_is_inconclusive_and_replayable(self):
        for result in (
            RunResult(-1, "TIMEOUT", 0),
            RunResult(1, "Out of Memory Error: x", 0),
            RunResult(-11, "", 0),
            RunResult(1, "unrecognized process failure", 0),
        ):
            with self.subTest(result=result):
                status, report = self.run_campaign([result])
                self.assertEqual(status, 2)
                self.assertEqual(report["summary"], {"attempted": 1, "compared": 0, "inconclusive": 1, "findings": 0})
                self.assertEqual(report["records"][0]["sql"], "SELECT 1")
                self.assertTrue(report["records"][0]["reference_failure"])
                ok = RunResult(0, "1", 0)
                status, replay = self.run_campaign([ok] * 3, replay=report)
                self.assertEqual(status, 0)
                self.assertEqual(replay["summary"]["attempted"], 1)

    def test_jit_oom_and_partial_coverage_cannot_pass(self):
        ok = RunResult(0, "1", 0)
        for runs in ([ok, RunResult(1, "Out of Memory Error: x", 0), ok], [RunResult(-1, "TIMEOUT", 0), ok, ok, ok]):
            status, report = self.run_campaign(runs, queries=1 if len(runs) == 3 else 2)
            self.assertEqual(status, 2)
            self.assertEqual(report["summary"]["inconclusive"], 1)
            self.assertEqual(len(report["records"]), 1)

    def test_mismatch_takes_precedence_over_inconclusive(self):
        ok = RunResult(0, "1", 0)
        status, report = self.run_campaign([ok, RunResult(0, "2", 0), RunResult(1, "Out of Memory Error: x", 0)])
        self.assertEqual(status, 1)
        self.assertEqual(report["summary"]["findings"], 1)

    def test_results_and_expected_errors_both_count_as_comparisons(self):
        for result in (RunResult(0, "1", 0), RunResult(1, "Out of Range Error: x", 0)):
            status, report = self.run_campaign([result] * 3)
            self.assertEqual(status, 0)
            self.assertEqual(report["summary"], {"attempted": 1, "compared": 1, "inconclusive": 0, "findings": 0})
            self.assertEqual(report["records"], [])

    def test_empty_campaigns_are_rejected(self):
        with self.assertRaises(SystemExit):
            self.run_campaign([], queries=0)
        with self.assertRaises(SystemExit):
            self.run_campaign([], replay={"records": []})

    def test_native_internal_error_is_preserved_before_its_stack_trace(self):
        error = "INTERNAL Error: assertion failed"
        status, report = self.run_campaign([RunResult(1, error + "\n" + "stack frame\n" * 100, 0)])
        self.assertEqual(status, 2)
        self.assertEqual(report["records"][0]["off"], error)
        self.assertEqual(report["records"][0]["off_returncode"], 1)


if __name__ == "__main__":
    unittest.main()
