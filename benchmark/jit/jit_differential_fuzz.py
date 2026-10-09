#!/usr/bin/env python3
"""Differential fuzzing of the execution-region JIT against jit_policy='off'.

Generates seeded random queries over the TPC-H schema -- foreign-key chains of inner and outer joins, some with
non-equality residuals, random filters (IS NULL among them), DISTINCT, GROUP BY (ROLLUP, HAVING) over up to five
expression keys (CASE among them), FILTER aggregates, partitioned window aggregates, materialized CTEs with one or two
consumers, UNION ALL and IN/EXISTS subqueries -- and
runs each one with the JIT off, at the default cost model and with compilation forced. Every run is a fresh shell
process, so a crash that invalidates the database cannot poison the next query. Results are compared through an
order-independent fingerprint (row count and the sum of row hashes); generated queries use exact types only and give
every LIMIT a total order, so any difference is a JIT bug.

    python3 benchmark/jit/jit_differential_fuzz.py --database tpch_sf1.duckdb --queries 300 --seed 1
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

# column kinds: key (integer key), int, dec (DECIMAL), date, enum (low-cardinality string), text (free text)
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
    },
    "supplier": {
        "s_suppkey": "key",
        "s_name": "text",
        "s_nationkey": "key",
        "s_phone": "text",
        "s_acctbal": "dec",
        "s_comment": "text",
    },
    "nation": {"n_nationkey": "key", "n_name": "enum", "n_regionkey": "key", "n_comment": "text"},
    "region": {"r_regionkey": "key", "r_name": "enum", "r_comment": "text"},
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
            condition = " AND ".join(f"{alias}.{a} = {other_alias}.{b}" for a, b in zip(own_cols, other_cols))
            own_table = next(table for table_alias, table in tree.tables if table_alias == alias)
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
            if kind == other_kind and kind in ("int", "dec", "date")
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
        if column.kind == "key":
            return r.choice([f"{ref} % {r.randint(2, 13)} = {r.randint(0, 1)}", f"{ref} < {r.randint(1, 25) * 4000}"])
        if column.kind == "int":
            low, high = INT_BOUNDS.get(column.name, (0, 100))
            return r.choice([f"{ref} > {r.randint(low, high)}", f"{ref} BETWEEN {low} AND {r.randint(low, high)}"])
        if column.kind == "dec":
            low, high = DEC_BOUNDS.get(column.name, (0, 1000))
            value = round(r.uniform(low, high), 2)
            return r.choice([f"{ref} < {value}", f"{ref} >= {value}", f"{ref} * 2 > {value}"])
        if column.kind == "date":
            day = f"DATE '199{r.randint(2, 8)}-{r.randint(1, 12):02d}-{r.randint(1, 28):02d}'"
            return r.choice([f"{ref} < {day}", f"{ref} >= {day}"])
        if column.kind == "enum":
            values = ENUM_VALUES.get(column.name, ["X"])
            picked = r.sample(values, min(len(values), r.randint(1, 3)))
            quoted = ", ".join(f"'{v}'" for v in picked)
            return r.choice([f"{ref} = '{picked[0]}'", f"{ref} IN ({quoted})", f"{ref} <> '{picked[0]}'"])
        return f"{ref} {'NOT ' if r.random() < 0.2 else ''}LIKE '{r.choice(TEXT_PATTERNS)}'"

    def predicates(self, tree: JoinTree, low: int = 0, high: int = 3) -> list[str]:
        columns = tree.columns()
        return [self.predicate(self.rng.choice(columns)) for _ in range(self.rng.randint(low, high))]

    def expression(self, column: Column) -> str:
        r, ref = self.rng, column.ref
        if r.random() < 0.5:
            return ref
        if r.random() < 0.15:
            return f"CASE WHEN {self.predicate(column)} THEN {ref} END"
        if column.kind in ("key", "int"):
            return r.choice([f"{ref} % {r.randint(2, 50)}", f"{ref} + {r.randint(1, 9)}", f"({ref} * 3) % 1000"])
        if column.kind == "dec":
            return r.choice([f"{ref} * 2", f"{ref} + 1", f"CAST({ref} AS INTEGER)"])
        if column.kind == "date":
            return r.choice([f"year({ref})", f"month({ref})", f"{ref} + 7"])
        return r.choice([f"substr({ref}, 1, 3)", f"length({ref})", f"upper(substr({ref}, 1, 2))"])

    def aggregate(self, columns: list[Column]) -> str:
        r = self.rng
        numeric = [c for c in columns if c.kind in ("key", "int", "dec")]
        decimals = [c for c in columns if c.kind == "dec"]
        choice = r.randint(0, 6)
        if choice == 0 or not numeric:
            return "count(*)"
        if choice == 1:
            return f"sum({r.choice(numeric).ref})"
        if choice == 2 and len(decimals) >= 2:
            a, b = r.sample(decimals, 2)
            return f"sum({a.ref} * (1 - {b.ref}))"
        if choice == 3:
            return f"min({self.expression(r.choice(columns))})"
        if choice == 4:
            return f"max({self.expression(r.choice(columns))})"
        if choice == 5:
            return f"count(DISTINCT {self.expression(r.choice(columns))})"
        if r.random() < 0.3:
            return f"sum({r.choice(numeric).ref}) FILTER (WHERE {self.predicate(r.choice(columns))})"
        return f"sum({self.expression(r.choice(numeric))})"

    def core_select(self, tree: JoinTree, shape: str, where: list[str]) -> tuple[str, int]:
        r, columns = self.rng, tree.columns()
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""
        if shape == "agg":
            groups = [self.expression(r.choice(columns)) for _ in range(r.randint(0, 5))]
            aggregates = [self.aggregate(columns) for _ in range(r.randint(1, 3))]
            group_sql = ""
            if groups:
                group_list = ", ".join(groups)
                group_sql = f" GROUP BY ROLLUP ({group_list})" if r.random() < 0.1 else f" GROUP BY {group_list}"
                if r.random() < 0.2:
                    group_sql += f" HAVING count(*) > {r.choice([1, 2, 5])}"
            return f"SELECT {', '.join(groups + aggregates)} FROM {tree.from_clause()}{where_sql}{group_sql}", len(
                groups
            ) + len(aggregates)
        if shape == "window":
            # a frame-less window aggregate over a partition does not depend on the order of the partition's rows
            numeric = [c for c in columns if c.kind in ("key", "int", "dec")]
            expressions = [self.expression(r.choice(columns)) for _ in range(r.randint(1, 3))]
            window = r.choice(["count(*)", f"sum({r.choice(numeric).ref})", f"max({r.choice(numeric).ref})"])
            sql = (
                f"SELECT {', '.join(expressions)}, {window} OVER (PARTITION BY {expressions[0]}) "
                f"FROM {tree.from_clause()}{where_sql}"
            )
            return sql, len(expressions) + 1
        expressions = [self.expression(r.choice(columns)) for _ in range(r.randint(1, 4))]
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
        if r.random() < 0.5:
            negate = "NOT " if r.random() < 0.3 else ""
            return (
                f"{column.ref} {negate}IN (SELECT {inner_key.ref} FROM {inner.from_clause()} "
                f"WHERE {' AND '.join(inner_where)})"
            )
        negate = "NOT " if r.random() < 0.3 else ""
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
        family = r.choice(["plain", "plain", "plain", "cte", "cte", "union", "ordered", "ctas"])
        if family in ("ordered", "ctas"):
            return self.ordered_query(family)
        if family == "plain":
            sql, width = self.core_select(
                tree, r.choice(["agg", "agg", "distinct", "project", "topn", "window"]), where
            )
            return fingerprint(sql, width)
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
        expressions = [self.expression(r.choice(columns)) for _ in range(r.randint(1, 4))]
        select = ", ".join(f"{e} AS c{i}" for i, e in enumerate(expressions))
        where = self.predicates(tree, 0, 2)
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


def classify(off: RunResult, run: RunResult) -> str:
    if run.output == "TIMEOUT":
        return "timeout"
    if "INTERNAL Error" in run.output or "FATAL" in run.output or run.returncode < 0:
        return "crash"
    if run.output != off.output:
        return "error" if "Error" in run.output else "mismatch"
    return "ok"


def check(args: argparse.Namespace, index: int, sql: str) -> dict:
    threads = 1 if index % 4 == 0 else args.threads
    off = run_query(args.duckdb, args.database, CONFIGS["off"], threads, sql, args.timeout)
    record = {"index": index, "threads": threads, "sql": sql, "off": off.output[-300:], "results": {}}
    if off.returncode != 0 or "Error" in off.output:
        record["invalid"] = True
        return record
    for name in ("default", "forced"):
        run = run_query(args.duckdb, args.database, CONFIGS[name], threads, sql, args.timeout)
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
    args = parser.parse_args(argv)

    generator = QueryGenerator(args.seed)
    queries = [generator.query() for _ in range(args.queries)]
    findings, invalid, done = [], 0, 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(check, args, index, sql) for index, sql in enumerate(queries)]
        for future in concurrent.futures.as_completed(futures):
            record = future.result()
            done += 1
            if record.get("invalid"):
                invalid += 1
                continue
            bad = {name: verdict for name, verdict in record["results"].items() if verdict != "ok"}
            if bad:
                findings.append(record)
                print(f"[{done}/{len(queries)}] q{record['index']} t{record['threads']} {bad}", flush=True)
    findings.sort(key=lambda record: record["index"])
    print(f"checked {len(queries) - invalid} queries ({invalid} invalid), {len(findings)} with findings")
    if args.report:
        args.report.write_text(json.dumps(findings, indent=2) + "\n", encoding="utf-8")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
