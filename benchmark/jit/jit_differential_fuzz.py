#!/usr/bin/env python3
"""Differential fuzzing of the execution-region JIT against jit_policy='off'.

Generates seeded random queries over the TPC-H schema -- foreign-key chains of inner and outer joins (some on
expression keys, some on the string forms TPC-H also spells them in, some with non-equality residuals), random filters
(IS NULL among them, and IN lists long enough to plan as MARK joins), DISTINCT, GROUP BY (ROLLUP,
CUBE, GROUPING SETS, HAVING, ORDER BY ALL LIMIT) over up to five expression keys (CASE and predicates over several
columns used as values among them), FILTER aggregates,
partitioned and ordered window functions, materialized CTEs with one or two consumers, UNION ALL and set operations,
IN (one or two columns), EXISTS and correlated count subqueries -- and runs each one with the JIT off, at the default
cost model and with compilation forced. Every run is a fresh shell
process, so a crash that invalidates the database cannot poison the next query. Results are compared through an
order-independent fingerprint (row count and the sum of row hashes); generated queries use exact types only and give
every LIMIT a total order, so any difference is a JIT bug.

    python3 benchmark/jit/jit_differential_fuzz.py --database tpch_sf1.duckdb --queries 300 --seed 1 --report r.json
    python3 benchmark/jit/jit_differential_fuzz.py --database tpch_sf1.duckdb --replay r.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import random
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

FORCED_SETTINGS = (
    "SET jit_cbo_generated_stage_benefit=4096; SET jit_cbo_materialization_elision_benefit=4096; "
    "SET jit_cbo_native_operator_stage_benefit=4096; SET jit_cbo_full_pipeline_benefit=4096; "
    "SET jit_cbo_source_contract_scan_filter_penalty=0; SET jit_cbo_startup_base_cost=0; "
    "SET jit_cbo_startup_margin_basis_points=0;"
)
CONFIGS = {
    "off": "SET jit_policy='off';",
    "default": "",
    "forced": FORCED_SETTINGS,
}

# column kinds: key (integer key), int, dec (DECIMAL), float (FLOAT or DOUBLE), date, enum (low-cardinality string),
# text (free text)
SCHEMA: dict[str, dict[str, str]] = {
    "lineitem": {
        "l_orderkey": "key",
        "l_partkey": "key",
        "l_suppkey": "key",
        "l_linenumber": "int",
        "l_quantity": "dec",
        "l_extendedprice": "dec",
        "l_discount": "dec",
        "l_tax": "dec",
        "l_returnflag": "enum",
        "l_linestatus": "enum",
        "l_shipdate": "date",
        "l_commitdate": "date",
        "l_receiptdate": "date",
        "l_shipinstruct": "enum",
        "l_shipmode": "enum",
        "l_comment": "text",
        "l_ratio": "float",
    },
    "orders": {
        "o_orderkey": "key",
        "o_custkey": "key",
        "o_orderstatus": "enum",
        "o_totalprice": "dec",
        "o_orderdate": "date",
        "o_orderpriority": "enum",
        "o_clerk": "text",
        "o_shippriority": "int",
        "o_comment": "text",
    },
    "customer": {
        "c_custkey": "key",
        "c_name": "text",
        "c_nationkey": "key",
        "c_phone": "text",
        "c_acctbal": "dec",
        "c_mktsegment": "enum",
        "c_comment": "text",
    },
    "part": {
        "p_partkey": "key",
        "p_name": "text",
        "p_mfgr": "enum",
        "p_brand": "enum",
        "p_type": "text",
        "p_size": "int",
        "p_container": "text",
        "p_retailprice": "dec",
        "p_comment": "text",
    },
    "partsupp": {
        "ps_partkey": "key",
        "ps_suppkey": "key",
        "ps_availqty": "int",
        "ps_supplycost": "dec",
        "ps_comment": "text",
        "ps_weight": "float",
    },
    "supplier": {
        "s_suppkey": "key",
        "s_name": "text",
        "s_nationkey": "key",
        "s_phone": "text",
        "s_acctbal": "dec",
        "s_comment": "text",
        "s_score": "float",
    },
    "nation": {"n_nationkey": "key", "n_name": "enum", "n_regionkey": "key", "n_comment": "text"},
    "region": {"r_regionkey": "key", "r_name": "enum", "r_comment": "text"},
}

# TPC-H has no floating-point column: the fuzz database derives these from TPC-H columns, mixing in both NaN signs, both
# infinities and both zeros, which DuckDB orders and groups differently from IEEE (every NaN is one value, above
# infinity; -0.0 is 0.0). s_score reaches joins from the build side
DERIVED_COLUMNS: dict[str, dict[str, str]] = {
    "lineitem": {
        "l_ratio": "CASE l_orderkey % 13 WHEN 0 THEN 'nan'::DOUBLE WHEN 1 THEN -('nan'::DOUBLE) WHEN 2 THEN 'inf'::DOUBLE "
        "WHEN 3 THEN '-inf'::DOUBLE WHEN 4 THEN -0.0::DOUBLE WHEN 5 THEN 0.0::DOUBLE "
        "ELSE CAST(l_extendedprice AS DOUBLE) / CAST(l_quantity AS DOUBLE) END",
    },
    "partsupp": {
        "ps_weight": "CASE ps_partkey % 7 WHEN 0 THEN 'nan'::FLOAT WHEN 1 THEN -0.0::FLOAT WHEN 2 THEN 0.0::FLOAT "
        "WHEN 3 THEN 'inf'::FLOAT ELSE CAST(ps_supplycost AS FLOAT) / 4 END",
    },
    "supplier": {
        "s_score": "CASE s_suppkey % 6 WHEN 0 THEN 'nan'::DOUBLE WHEN 1 THEN -0.0::DOUBLE WHEN 2 THEN 0.0::DOUBLE "
        "WHEN 3 THEN '-inf'::DOUBLE ELSE CAST(s_acctbal AS DOUBLE) END",
    },
}

# foreign-key edges: (table, columns, referenced table, columns)
EDGES = [
    ("lineitem", ("l_orderkey",), "orders", ("o_orderkey",)),
    ("lineitem", ("l_partkey",), "part", ("p_partkey",)),
    ("lineitem", ("l_suppkey",), "supplier", ("s_suppkey",)),
    ("lineitem", ("l_partkey", "l_suppkey"), "partsupp", ("ps_partkey", "ps_suppkey")),
    ("orders", ("o_custkey",), "customer", ("c_custkey",)),
    ("customer", ("c_nationkey",), "nation", ("n_nationkey",)),
    ("supplier", ("s_nationkey",), "nation", ("n_nationkey",)),
    ("nation", ("n_regionkey",), "region", ("r_regionkey",)),
    ("partsupp", ("ps_partkey",), "part", ("p_partkey",)),
    ("partsupp", ("ps_suppkey",), "supplier", ("s_suppkey",)),
]

# Foreign keys TPC-H also spells as strings: a join on them probes VARCHAR keys, inlined (phone country codes) and out of
# line (names), each side written as (left table expression, right table expression)
STRING_KEYS = {
    ("orders", "customer"): ("'Customer#' || lpad(CAST({a}.o_custkey AS VARCHAR), 9, '0')", "{a}.c_name"),
    ("lineitem", "supplier"): ("'Supplier#' || lpad(CAST({a}.l_suppkey AS VARCHAR), 9, '0')", "{a}.s_name"),
    ("partsupp", "supplier"): ("'Supplier#' || lpad(CAST({a}.ps_suppkey AS VARCHAR), 9, '0')", "{a}.s_name"),
    ("customer", "nation"): ("substring({a}.c_phone, 1, 2)", "CAST({a}.n_nationkey + 10 AS VARCHAR)"),
    ("supplier", "nation"): ("substring({a}.s_phone, 1, 2)", "CAST({a}.n_nationkey + 10 AS VARCHAR)"),
}

ENUM_VALUES = {
    "l_returnflag": ["A", "N", "R"],
    "l_linestatus": ["F", "O"],
    "l_shipinstruct": ["COLLECT COD", "DELIVER IN PERSON", "NONE", "TAKE BACK RETURN"],
    "l_shipmode": ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"],
    "o_orderstatus": ["F", "O", "P"],
    "o_orderpriority": ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"],
    "c_mktsegment": ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"],
    "p_mfgr": [f"Manufacturer#{i}" for i in range(1, 6)],
    "p_brand": [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)],
    "n_name": ["ALGERIA", "BRAZIL", "CANADA", "FRANCE", "GERMANY", "INDIA", "JAPAN", "PERU", "RUSSIA", "UNITED STATES"],
    "r_name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
}
TEXT_PATTERNS = ["%ly%", "%special%", "%the%", "%a%", "%1%", "SM%", "%BRASS", "Customer#0000%", "Clerk#00000%"]
DEC_BOUNDS = {
    "l_quantity": (1, 50),
    "l_extendedprice": (900, 100000),
    "l_discount": (0, 0.1),
    "l_tax": (0, 0.08),
    "o_totalprice": (1000, 500000),
    "c_acctbal": (-999, 9999),
    "p_retailprice": (900, 2100),
    "ps_supplycost": (1, 1000),
    "s_acctbal": (-999, 9999),
}
INT_BOUNDS = {"l_linenumber": (1, 7), "o_shippriority": (0, 0), "p_size": (1, 50), "ps_availqty": (1, 9999)}


@dataclass
class Column:
    alias: str
    name: str
    kind: str

    @property
    def ref(self) -> str:
        return f"{self.alias}.{self.name}"


@dataclass
class JoinTree:
    tables: list[tuple[str, str]] = field(default_factory=list)  # (alias, table)
    conditions: list[str] = field(default_factory=list)
    kinds: list[str] = field(default_factory=list)  # JOIN, LEFT JOIN, RIGHT JOIN or FULL JOIN

    def columns(self) -> list[Column]:
        return [Column(alias, name, kind) for alias, table in self.tables for name, kind in SCHEMA[table].items()]

    def from_clause(self) -> str:
        first_alias, first_table = self.tables[0]
        sql = f"{first_table} {first_alias}"
        for (alias, table), condition, kind in zip(self.tables[1:], self.conditions, self.kinds):
            sql += f" {kind} {table} {alias} ON {condition}"
        return sql


class QueryGenerator:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.alias_counter = 0
        # an IN list of six or more constants plans as a MARK join, which leaves insertion order undefined
        self.mark_in_lists = True

    def alias(self) -> str:
        self.alias_counter += 1
        return f"t{self.alias_counter}"

    def join_tree(self, max_hops: int = 3) -> JoinTree:
        """Joins follow foreign keys to the tables they reference, so an inner join keeps the row count of the table
        that holds the key and an outer join adds at most the other table's rows; only the start table may also join
        one table that references it (one fan-out)."""
        start = self.rng.choice(["lineitem", "lineitem", "orders", "partsupp", "customer", "supplier", "part"])
        tree = JoinTree([(self.alias(), start)])
        start_alias = tree.tables[0][0]
        fanned_out = False
        for _ in range(self.rng.randint(0, max_hops)):
            candidates = []
            for alias, table in tree.tables:
                for left, left_cols, right, right_cols in EDGES:
                    if left == table:
                        candidates.append((alias, left_cols, right, right_cols, False))
                    elif right == table and alias == start_alias and not fanned_out:
                        candidates.append((alias, right_cols, left, left_cols, True))
            present = {table for _, table in tree.tables}
            candidates = [c for c in candidates if c[2] not in present or c[2] == "nation"]
            if not candidates:
                break
            alias, own_cols, other, other_cols, fan_out = self.rng.choice(candidates)
            fanned_out = fanned_out or fan_out
            other_alias = self.alias()
            own_table = next(table for table_alias, table in tree.tables if table_alias == alias)
            string_key = STRING_KEYS.get((other, own_table) if fan_out else (own_table, other))
            if string_key and self.rng.random() < 0.3:
                own_key, other_key = (string_key[1], string_key[0]) if fan_out else string_key
                condition = f"{own_key.format(a=alias)} = {other_key.format(a=other_alias)}"
            else:
                # an expression key keeps the foreign-key cardinality but takes the hash join's key-expression path
                key = "{} * 2" if self.rng.random() < 0.15 else "{}"
                condition = " AND ".join(
                    f"{key.format(f'{alias}.{a}')} = {key.format(f'{other_alias}.{b}')}"
                    for a, b in zip(own_cols, other_cols)
                )
            residual = self.join_residual(alias, own_table, other_alias, other)
            if residual:
                condition += f" AND {residual}"
            tree.tables.append((other_alias, other))
            tree.conditions.append(condition)
            tree.kinds.append(self.rng.choices(["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"], [14, 4, 1, 1])[0])
        return tree

    def join_residual(self, alias: str, table: str, other_alias: str, other: str) -> str | None:
        """A non-equality condition between two columns of the same kind, probed as a join residual."""
        if self.rng.random() >= 0.2:
            return None
        pairs = [
            (name, other_name)
            for name, kind in SCHEMA[table].items()
            for other_name, other_kind in SCHEMA[other].items()
            if kind == other_kind and kind in ("int", "dec", "float", "date")
        ]
        if not pairs:
            return None
        name, other_name = self.rng.choice(pairs)
        operator = self.rng.choice(["<", ">", "<=", ">=", "<>"])
        return f"{alias}.{name} {operator} {other_alias}.{other_name}"

    def predicate(self, column: Column) -> str:
        r, ref = self.rng, column.ref
        if r.random() < 0.08:
            return f"{ref} IS {r.choice(['', 'NOT '])}NULL"
        if column.kind in ("key", "int", "dec") and r.random() < 0.1:
            # DuckDB ranks NaN above infinity and equal to itself; IEEE comparisons leave it unordered
            operator = r.choice(["=", "<>", "<", ">", "<=", ">="])
            constant = r.choice(["0", "1.5", "'nan'::DOUBLE", "'inf'::DOUBLE", "-0.0::DOUBLE"])
            value = self.float_value(ref)
            return f"{value} {operator} {constant}" if r.random() < 0.5 else f"{constant} {operator} {value}"
        if column.kind in ("key", "int", "dec") and r.random() < 0.05:
            # DISTINCT FROM compares NULL as a value
            negate = "NOT " if r.random() < 0.5 else ""
            value = r.choice(["NULL", str(r.randint(0, 50))])
            return f"{ref} IS {negate}DISTINCT FROM {value}"
        if column.kind == "key":
            return r.choice([f"{ref} % {r.randint(2, 13)} = {r.randint(0, 1)}", f"{ref} < {r.randint(1, 25) * 4000}"])
        if column.kind == "int":
            low, high = INT_BOUNDS.get(column.name, (0, 100))
            return r.choice([f"{ref} > {r.randint(low, high)}", f"{ref} BETWEEN {low} AND {r.randint(low, high)}"])
        if column.kind == "dec":
            low, high = DEC_BOUNDS.get(column.name, (0, 1000))
            value = round(r.uniform(low, high), 2)
            # a DOUBLE comparison is exact per value; DOUBLE never reaches a sum, whose rounding depends on order
            return r.choice(
                [f"{ref} < {value}", f"{ref} >= {value}", f"{ref} * 2 > {value}", f"CAST({ref} AS DOUBLE) < {value}"]
            )
        if column.kind == "float":
            operator = r.choice(["=", "<>", "<", ">", "<=", ">="])
            constant = r.choice(
                ["0", "1.5", "1000.25", "'nan'::DOUBLE", "'inf'::DOUBLE", "'-inf'::DOUBLE", "-0.0::DOUBLE"]
            )
            return f"{ref} {operator} {constant}" if r.random() < 0.5 else f"{constant} {operator} {ref}"
        if column.kind == "date":
            day = f"DATE '199{r.randint(2, 8)}-{r.randint(1, 12):02d}-{r.randint(1, 28):02d}'"
            return r.choice([f"{ref} < {day}", f"{ref} >= {day}"])
        if column.kind == "enum":
            values = ENUM_VALUES.get(column.name, ["X"])
            # six or more constants plan as a MARK join against the constant list
            count = r.randint(6, 8) if self.mark_in_lists and len(values) >= 6 and r.random() < 0.3 else r.randint(1, 3)
            picked = r.sample(values, min(len(values), count))
            quoted = ", ".join(f"'{v}'" for v in picked)
            return r.choice([f"{ref} = '{picked[0]}'", f"{ref} IN ({quoted})", f"{ref} <> '{picked[0]}'"])
        if self.mark_in_lists and column.name in ("c_phone", "s_phone") and r.random() < 0.5:
            codes = ", ".join(f"'{code}'" for code in r.sample(range(10, 35), 7))
            return f"substring({ref}, 1, 2) {'NOT ' if r.random() < 0.2 else ''}IN ({codes})"
        return f"{ref} {'NOT ' if r.random() < 0.2 else ''}LIKE '{r.choice(TEXT_PATTERNS)}'"

    def predicates(self, tree: JoinTree, low: int = 0, high: int = 3) -> list[str]:
        columns = tree.columns()
        result = []
        for _ in range(self.rng.randint(low, high)):
            predicate = self.predicate(self.rng.choice(columns))
            # NOT keeps the rows a predicate rejects, but not the ones it leaves NULL
            result.append(f"NOT ({predicate})" if self.rng.random() < 0.1 else predicate)
        return result

    def expression(self, column: Column, floats: bool = True) -> str:
        r, ref = self.rng, column.ref
        if r.random() < 0.5:
            return ref
        if floats and column.kind in ("key", "int", "dec") and r.random() < 0.15:
            return self.float_value(ref)
        if r.random() < 0.15:
            return f"CASE WHEN {self.predicate(column)} THEN {ref} END"
        if r.random() < 0.05:
            # branches are tried in order: a row the first predicate takes never reaches the second
            otherwise = r.choice(["NULL", ref])
            return f"CASE WHEN {self.predicate(column)} THEN {ref} WHEN {self.predicate(column)} THEN {ref} ELSE {otherwise} END"
        if r.random() < 0.15:
            return self.null_handling(column)
        if column.kind in ("key", "int"):
            casts = [f"CAST({ref} AS INTEGER)", f"CAST({ref} % 30000 AS SMALLINT)", f"CAST({ref} AS HUGEINT) * 3"]
            if r.random() < 0.05:
                # overflows on the larger keys: off and the JIT must raise the same error
                casts.append(f"CAST({ref} AS INTEGER) * 1000")
            if r.random() < 0.05:
                # a divisor that is zero for some rows: off raises a division by zero, unless a filter removed them
                casts.append(f"{ref} // ({ref} % {r.randint(3, 7)})")
            # TPC-H integers are never negative: subtracting makes negative results, which 64-bit consumers of a
            # 32-bit result (sums, range checks) must see sign-extended
            return r.choice(
                [
                    f"{ref} % {r.randint(2, 50)}",
                    f"{ref} + {r.randint(1, 9)}",
                    f"({ref} * 3) % 1000",
                    f"CAST({ref} AS INTEGER) - {r.randint(10, 100000)}",
                    f"{ref} // {r.randint(2, 9)}",
                    f"(CAST({ref} AS INTEGER) - {r.randint(10, 100000)}) // {r.randint(2, 9)}",
                    f"-{ref}",
                    *casts,
                ]
            )
        if column.kind == "dec":
            return r.choice(
                [f"{ref} * 2", f"{ref} + 1", f"CAST({ref} AS INTEGER)", f"CAST({ref} AS BIGINT)", f"-{ref}"]
            )
        if column.kind == "float":
            return r.choice([f"{ref} * 2", f"{ref} + 1", f"-{ref}", f"abs({ref})", f"{ref} / 4"])
        if column.kind == "date":
            # TPC-H dates are near day 8000 of the epoch: subtracting more reaches negative day counts
            return r.choice(
                [
                    f"year({ref})",
                    f"month({ref})",
                    f"{ref} + 7",
                    f"{ref} - 9000",
                    f"date_diff('day', DATE '1992-01-01', {ref})",
                ]
            )
        return r.choice(
            [
                f"substr({ref}, 1, 3)",
                f"length({ref})",
                f"upper(substr({ref}, 1, 2))",
                f"concat(substr({ref}, 1, 2), '-')",
            ]
        )

    def boolean_expression(self, columns: list[Column]) -> str:
        """A predicate used as a value -- a projection, group key or aggregate argument. Its terms read several columns
        in an order that need not match the input's, so the slots a compiled predicate reads its sources through differ
        from the columns; AND, OR and NOT keep their three-valued results"""
        r = self.rng
        terms = [self.predicate(r.choice(columns)) for _ in range(r.randint(1, 3))]
        combined = f" {r.choice(['AND', 'OR'])} ".join(f"({term})" for term in terms)
        return f"NOT ({combined})" if r.random() < 0.2 else f"({combined})"

    def value(self, columns: list[Column]) -> str:
        if self.rng.random() < 0.12:
            return self.boolean_expression(columns)
        return self.expression(self.rng.choice(columns))

    def float_value(self, ref: str) -> str:
        """A FLOAT or DOUBLE value, so groups, DISTINCT and joins see floating-point keys: DuckDB treats -0.0 as 0.0
        and every NaN as the same value, which neither the bits nor a C++ comparison do. The fingerprint hash
        normalizes both, so whichever representative a group keeps hashes the same"""
        r = self.rng
        modulus = r.randint(2, 7)
        return r.choice(
            [
                f"CAST({ref} AS DOUBLE)",
                f"CAST({ref} AS FLOAT)",
                f"CAST({ref} % {modulus} AS DOUBLE) / 2",
                f"{ref} / {modulus}",
                f"{ref} // {modulus} * 1.5",
                f"CASE {ref} % 4 WHEN 0 THEN -0.0::DOUBLE WHEN 1 THEN 0.0::DOUBLE WHEN 2 THEN 'nan'::DOUBLE "
                f"ELSE -('nan'::DOUBLE) END",
                f"CASE {ref} % 3 WHEN 0 THEN -0.0::FLOAT WHEN 1 THEN 'nan'::FLOAT ELSE CAST({ref} % {modulus} AS FLOAT) END",
            ]
        )

    def null_handling(self, column: Column) -> str:
        r, ref = self.rng, column.ref
        if column.kind in ("key", "int"):
            return r.choice([f"coalesce({ref}, -1)", f"nullif({ref} % 7, 0)"])
        if column.kind in ("dec", "float"):
            return r.choice([f"coalesce({ref}, 0)", f"nullif({ref}, 0)"])
        if column.kind == "date":
            return f"coalesce({ref}, DATE '1990-01-01')"
        return r.choice([f"coalesce({ref}, 'none')", f"nullif(substr({ref}, 1, 1), 'A')"])

    def aggregate(self, columns: list[Column]) -> str:
        r = self.rng
        numeric = [c for c in columns if c.kind in ("key", "int", "dec")]
        decimals = [c for c in columns if c.kind == "dec"]
        choice = r.randint(0, 6)
        if choice == 0 and r.random() < 0.3:
            # counts the rows the predicate does not leave NULL
            return f"count({self.boolean_expression(columns)})"
        if choice == 0 or not numeric:
            return "count(*)"
        if choice == 1:
            return f"sum({r.choice(numeric).ref})"
        if choice == 2 and len(decimals) >= 2:
            a, b = r.sample(decimals, 2)
            return f"sum({a.ref} * (1 - {b.ref}))"
        if choice == 3:
            return f"min({self.value(columns)})"
        if choice == 4:
            return f"max({self.value(columns)})"
        if choice == 5:
            return f"count(DISTINCT {self.value(columns)})"
        if r.random() < 0.3:
            return f"sum({r.choice(numeric).ref}) FILTER (WHERE {self.predicate(r.choice(columns))})"
        # a floating-point sum rounds differently in a different order
        return f"sum({self.expression(r.choice(numeric), floats=False)})"

    def core_select(self, tree: JoinTree, shape: str, where: list[str]) -> tuple[str, int]:
        r, columns = self.rng, tree.columns()
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""
        if shape == "agg":
            groups = [self.value(columns) for _ in range(r.randint(0, 5))]
            aggregates = [self.aggregate(columns) for _ in range(r.randint(1, 3))]
            group_sql = ""
            if groups:
                group_list = ", ".join(groups)
                grouping = r.choices(["plain", "rollup", "cube", "sets"], [17, 2, 1, 1])[0]
                if grouping == "rollup":
                    group_sql = f" GROUP BY ROLLUP ({group_list})"
                elif grouping == "cube" and len(groups) <= 3:
                    group_sql = f" GROUP BY CUBE ({group_list})"
                elif grouping == "sets":
                    group_sql = f" GROUP BY GROUPING SETS (({group_list}), ({groups[0]}), ())"
                else:
                    group_sql = f" GROUP BY {group_list}"
                if r.random() < 0.2:
                    group_sql += f" HAVING count(*) > {r.choice([1, 2, 5])}"
            sql = f"SELECT {', '.join(groups + aggregates)} FROM {tree.from_clause()}{where_sql}{group_sql}"
            if r.random() < 0.15:
                # every output column orders the rows, so the rows a LIMIT keeps are determined
                sql += f" ORDER BY ALL LIMIT {r.choice([1, 10, 100])}"
            return sql, len(groups) + len(aggregates)
        if shape == "window":
            # a frame-less window aggregate over a partition does not depend on the order of the partition's rows
            numeric = [c for c in columns if c.kind in ("key", "int", "dec")]
            expressions = [self.expression(r.choice(columns)) for _ in range(r.randint(1, 3))]
            window = r.choice(["count(*)", f"sum({r.choice(numeric).ref})", f"max({r.choice(numeric).ref})"])
            over = f"PARTITION BY {expressions[0]}"
            if r.random() < 0.4:
                # ordered: the default frame ends at the current row's last peer, so ties get one value
                window = r.choice([window, "rank()", "dense_rank()"])
                over += f" ORDER BY {expressions[-1]}"
            sql = f"SELECT {', '.join(expressions)}, {window} OVER ({over}) FROM {tree.from_clause()}{where_sql}"
            return sql, len(expressions) + 1
        expressions = [self.value(columns) for _ in range(r.randint(1, 4))]
        distinct = "DISTINCT " if shape == "distinct" else ""
        sql = f"SELECT {distinct}{', '.join(expressions)} FROM {tree.from_clause()}{where_sql}"
        if shape == "topn":
            order = ", ".join(str(i + 1) for i in range(len(expressions)))
            sql += f" ORDER BY {order} LIMIT {r.choice([1, 10, 100, 5000])}"
        return sql, len(expressions)

    def subquery_predicate(self, tree: JoinTree) -> str:
        r = self.rng
        keys = [c for c in tree.columns() if c.kind == "key"]
        column = r.choice(keys)
        inner = self.join_tree(max_hops=1)
        inner_keys = [c for c in inner.columns() if c.kind == "key"]
        inner_key = r.choice(inner_keys)
        inner_where = self.predicates(inner, 1, 2)
        negate = "NOT " if r.random() < 0.3 else ""
        choice = r.randint(0, 4)
        if choice <= 1:
            return (
                f"{column.ref} {negate}IN (SELECT {inner_key.ref} FROM {inner.from_clause()} "
                f"WHERE {' AND '.join(inner_where)})"
            )
        if choice == 2:
            other_column, other_inner_key = r.choice(keys), r.choice(inner_keys)
            return (
                f"({column.ref}, {other_column.ref}) {negate}IN (SELECT {inner_key.ref}, {other_inner_key.ref} "
                f"FROM {inner.from_clause()} WHERE {' AND '.join(inner_where)})"
            )
        if choice == 3:
            # a correlated count: a group that finds no rows counts zero
            return (
                f"(SELECT count(*) FROM {inner.from_clause()} WHERE {inner_key.ref} = {column.ref} "
                f"AND {' AND '.join(inner_where)}) {r.choice(['=', '>', '<'])} {r.choice([0, 1, 3])}"
            )
        return (
            f"{negate}EXISTS (SELECT 1 FROM {inner.from_clause()} WHERE {inner_key.ref} = {column.ref} "
            f"AND {' AND '.join(inner_where)})"
        )

    def query(self) -> str:
        r = self.rng
        self.alias_counter = 0
        tree = self.join_tree()
        where = self.predicates(tree)
        if r.random() < 0.2:
            where.append(self.subquery_predicate(tree))
        family = r.choice(["plain", "plain", "plain", "cte", "cte", "union", "setop", "ordered", "ctas"])
        if family in ("ordered", "ctas"):
            return self.ordered_query(family)
        if family == "plain":
            sql, width = self.core_select(
                tree, r.choice(["agg", "agg", "distinct", "project", "topn", "window"]), where
            )
            return fingerprint(sql, width)
        if family == "setop":
            sql, width = self.core_select(tree, "project", where)
            other_where = self.predicates(tree, 1, 2)
            other = sql.split(" WHERE ")[0] + f" WHERE {' AND '.join(other_where)}"
            operation = r.choice(["UNION", "INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL"])
            return fingerprint(f"{sql} {operation} {other}", width)
        if family == "union":
            sql, width = self.core_select(tree, "project", where)
            other_where = self.predicates(tree, 1, 2)
            other = sql.split(" WHERE ")[0] + f" WHERE {' AND '.join(other_where)}"
            columns = ", ".join(f"c{i}" for i in range(width))
            union = f"SELECT {columns}, count(*) FROM ({sql} UNION ALL {other}) AS u({columns}) GROUP BY {columns}"
            return fingerprint(union, width + 1)
        # a materialized CTE: one consumer is fed by the producer's pushes, two consumers read a buffer
        sql, width = self.core_select(tree, "project", where)
        columns = ", ".join(f"c{i}" for i in range(width))
        cte = f"WITH c({columns}) AS MATERIALIZED ({sql}) "
        consumer = r.randint(0, 4)
        if consumer == 0:
            body = f"SELECT c0, count(*) FROM c GROUP BY c0"
            return cte + fingerprint(body, 2)
        if consumer == 1:
            body = f"SELECT count(*) FROM (SELECT * FROM c LIMIT {r.choice([1, 5, 1000])})"
            return cte + fingerprint(body, 1)
        if consumer == 2:
            body = f"SELECT {columns} FROM c ORDER BY {', '.join(str(i + 1) for i in range(width))} LIMIT 50"
            return cte + fingerprint(body, width)
        if consumer == 3:
            body = f"SELECT {columns} FROM c UNION ALL SELECT {columns} FROM c WHERE c0 IS NOT NULL"
            return cte + fingerprint(body, width)
        body = f"SELECT count(*), count(DISTINCT c0) FROM c WHERE c0 IN (SELECT c0 FROM c)"
        return cte + fingerprint(body, 2)

    def ordered_query(self, family: str) -> str:
        """Insertion order is only defined for a single scanned table (a probe row's matches come in hash-chain order
        even without the JIT), so the order-sensitive families scan one table: a filtered stream compared row by row,
        or a CTAS whose fingerprint includes rowid."""
        r = self.rng
        self.alias_counter = 0
        table = r.choice(["lineitem", "lineitem", "orders", "partsupp", "customer", "part"])
        tree = JoinTree([(self.alias(), table)])
        columns = tree.columns()
        self.mark_in_lists = False
        expressions = [self.expression(r.choice(columns)) for _ in range(r.randint(1, 4))]
        select = ", ".join(f"{e} AS c{i}" for i, e in enumerate(expressions))
        where = self.predicates(tree, 0, 2)
        self.mark_in_lists = True
        if family == "ordered":
            key = next(c for c in columns if c.kind == "key")
            where.append(f"{key.ref} % {r.randint(97, 1009)} = {r.randint(0, 3)}")
            return f"SELECT {select} FROM {tree.from_clause()} WHERE {' AND '.join(where)}"
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""
        columns_sql = ", ".join(f"c{i}" for i in range(len(expressions)))
        return (
            f"CREATE TABLE memory.main.r AS SELECT {select} FROM {tree.from_clause()}{where_sql};\n"
            f"SELECT count(*) AS n, sum(hash(rowid, {columns_sql})) AS h FROM memory.main.r"
        )


def database_columns(duckdb: Path, database: Path) -> set[tuple[str, str]]:
    result = subprocess.run(
        [str(duckdb), "-readonly", "-noheader", "-csv", str(database)],
        input="SELECT table_name, column_name FROM duckdb_columns();\n",
        text=True,
        capture_output=True,
        check=True,
    )
    return {tuple(line.split(",", 1)) for line in result.stdout.splitlines() if line}


def restrict_schema(columns: set[tuple[str, str]]) -> None:
    """Columns the database lacks, such as the derived columns in a plain TPC-H database, are never generated"""
    for table, table_columns in SCHEMA.items():
        for name in [name for name in table_columns if (table, name) not in columns]:
            del table_columns[name]


def fingerprint(sql: str, width: int) -> str:
    columns = ", ".join(f"f{i}" for i in range(width))
    return f"SELECT count(*) AS n, sum(hash({columns})) AS h FROM ({sql}) AS q({columns})"


@dataclass
class RunResult:
    returncode: int
    output: str
    seconds: float


def run_query(duckdb: Path, database: Path, settings: str, threads: int, sql: str, timeout: int) -> RunResult:
    # an in-memory session reads the database through a read-only attachment, so a CTAS can write to memory
    script = (
        f"ATTACH '{database}' AS tpch (READ_ONLY);\nUSE tpch;\nSET threads={threads};\n{settings}\n.mode csv\n"
        f"{sql};\n"
    )
    start = time.monotonic()
    try:
        result = subprocess.run([str(duckdb)], input=script, capture_output=True, text=True, timeout=timeout)
        output = (result.stdout + result.stderr).strip()
        return RunResult(result.returncode, output, time.monotonic() - start)
    except subprocess.TimeoutExpired:
        return RunResult(-1, "TIMEOUT", time.monotonic() - start)


def error_signature(output: str) -> str | None:
    """The class of the first error (Out of Range Error, Conversion Error, ...). The rest of the message may name the
    row that tripped it first, which depends on the schedule, and the generated code words some messages differently;
    what must match is that both runs fail, and how."""
    for line in output.splitlines():
        if "Error:" in line:
            return line.split("Error:", 1)[0].strip() + " Error"
    return None


def classify(off: RunResult, run: RunResult) -> str:
    if run.output == "TIMEOUT":
        return "timeout"
    if "INTERNAL Error" in run.output or "FATAL" in run.output or run.returncode < 0:
        return "crash"
    off_error = error_signature(off.output)
    if off_error is not None:
        # a query off rejects must fail the same way with the JIT; succeeding means a check was skipped
        run_error = error_signature(run.output)
        if run_error == off_error:
            return "ok"
        if run_error is not None and "Out of Memory Error" in run.output:
            return "oom"
        return "error" if run_error is not None else "mismatch"
    if run.output != off.output:
        if "Out of Memory Error" in run.output:
            # near a memory limit either run may fail to fit; a result that differs is what marks a JIT bug
            return "oom"
        return "error" if "Error" in run.output else "mismatch"
    return "ok"


def check(args: argparse.Namespace, index: int, sql: str, threads: int) -> dict:
    off = run_query(args.duckdb, args.database, args.settings + CONFIGS["off"], threads, sql, args.timeout)
    record = {"index": index, "threads": threads, "sql": sql, "off": off.output[-300:], "results": {}}
    if (
        off.output == "TIMEOUT"
        or off.returncode < 0
        or any(marker in off.output for marker in ("INTERNAL Error", "FATAL", "Out of Memory Error"))
    ):
        record["invalid"] = True
        return record
    for name in ("default", "forced"):
        run = run_query(args.duckdb, args.database, args.settings + CONFIGS[name], threads, sql, args.timeout)
        verdict = classify(off, run)
        record["results"][name] = verdict
        if verdict != "ok":
            errors = [line for line in run.output.splitlines() if "Error" in line]
            record.setdefault("outputs", {})[name] = errors[0] if errors else run.output[-600:]
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--duckdb", type=Path, default=ROOT / "build" / "reldebug" / "duckdb")
    parser.add_argument("--database", type=Path, required=True, help="TPC-H database (CALL dbgen(sf=1))")
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--report", type=Path, help="JSON report of every non-ok query")
    parser.add_argument(
        "--settings",
        default="",
        help="SQL every run executes first, e.g. \"SET memory_limit='300MB';\" to make it spill",
    )
    parser.add_argument("--replay", type=Path, help="re-check an earlier report's queries at one and --threads threads")
    args = parser.parse_args(argv)

    restrict_schema(database_columns(args.duckdb, args.database))
    if args.replay:
        earlier = json.loads(args.replay.read_text(encoding="utf-8"))
        jobs = [(record["index"], record["sql"], threads) for record in earlier for threads in (1, args.threads)]
    else:
        generator = QueryGenerator(args.seed)
        jobs = [(index, generator.query(), 1 if index % 4 == 0 else args.threads) for index in range(args.queries)]
    findings, invalid, ooms, done = [], 0, 0, 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(check, args, index, sql, threads) for index, sql, threads in jobs]
        for future in concurrent.futures.as_completed(futures):
            record = future.result()
            done += 1
            if record.get("invalid"):
                invalid += 1
                continue
            ooms += sum(verdict == "oom" for verdict in record["results"].values())
            bad = {name: verdict for name, verdict in record["results"].items() if verdict not in ("ok", "oom")}
            if bad:
                findings.append(record)
                print(f"[{done}/{len(jobs)}] q{record['index']} t{record['threads']} {bad}", flush=True)
    findings.sort(key=lambda record: (record["index"], record["threads"]))
    print(
        f"checked {len(jobs) - invalid} queries ({invalid} invalid, {ooms} JIT runs out of memory), "
        f"{len(findings)} with findings"
    )
    if args.report:
        args.report.write_text(json.dumps(findings, indent=2) + "\n", encoding="utf-8")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
