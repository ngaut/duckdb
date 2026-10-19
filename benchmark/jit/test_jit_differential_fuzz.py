import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import copy
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

    def test_columns_the_database_lacks_are_never_generated(self) -> None:
        schema = copy.deepcopy(SCHEMA)
        present = {(table, name) for table, columns in schema.items() for name in columns}
        present -= {(table, name) for table, columns in DERIVED_COLUMNS.items() for name in columns}
        with mock.patch.object(jit_differential_fuzz, "SCHEMA", schema):
            jit_differential_fuzz.restrict_schema(present)
            self.assertEqual({(table, name) for table, columns in schema.items() for name in columns}, present)
            generator = QueryGenerator(9)
            queries = [generator.query() for _ in range(300)]
        for sql in queries:
            for columns in DERIVED_COLUMNS.values():
                for name in columns:
                    self.assertNotIn(name, sql)

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


if __name__ == "__main__":
    unittest.main()
