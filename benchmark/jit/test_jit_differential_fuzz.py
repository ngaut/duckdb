import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from jit_differential_fuzz import EDGES, SCHEMA, QueryGenerator, RunResult, classify


class QueryGeneratorTest(unittest.TestCase):
    def test_a_seed_reproduces_its_queries(self) -> None:
        generator_a, generator_b = QueryGenerator(7), QueryGenerator(7)
        self.assertEqual([generator_a.query() for _ in range(50)], [generator_b.query() for _ in range(50)])

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


if __name__ == "__main__":
    unittest.main()
