#!/usr/bin/env python3
#
# Shared metadata for SLJIT microbenchmark admission proofs.

ADMITTED_POLICIES = ("off", "auto", "force")
DIAGNOSTIC_POLICIES = ("off", "force")

LARGE_ROW_COUNT = 20000000
MIN_ADMITTED_SPEEDUP = 1.05
ADDEND = 7
DEFAULT_SWEEP_ROW_COUNTS = (100000, 500000, 1000000, 5000000, 10000000, 20000000)


def projection_expected(row_count: int) -> int:
    return row_count * (row_count - 1) // 2 + ADDEND * row_count


def filter_bound(row_count: int) -> int:
    return row_count // 2


def filter_expected(row_count: int) -> int:
    return row_count - filter_bound(row_count) - 1


def generic_expected(row_count: int) -> int:
    lower = filter_bound(row_count) + 1
    upper = row_count - 1
    count = filter_expected(row_count)
    return (lower + upper) * count + ADDEND * count


def decimal_projection_ungrouped_sum_expected(row_count: int) -> str:
    total = sum(((row_idx % 100) + 1) * ((row_idx % 7) + 1) for row_idx in range(filter_bound(row_count) + 1, row_count))
    return f"{total}.0000"


ADMITTED_FAMILIES = {
    "native_filter_projection": {
        "shape_constant": "SLJIT_SOURCE_PREFIX_FILTER_PROJECTION_SHAPE",
        "shape_key": "sljit:source-prefix:filter-projection",
        "proof": "benchmark/micro/jit/native_filter_projection",
        "template": "benchmark/micro/jit/native_filter_projection.benchmark.in",
        "min_cardinality": 1000000,
        "threshold_row_count": 1000000,
        "candidate_shape": "filter-projection-projection",
        "expected_result": "375002749993",
        "view_sql": """
CREATE OR REPLACE TABLE jit_micro_filter_projection AS
SELECT i::BIGINT AS i
FROM range(1000000) tbl(i);
""",
        "query": """
SELECT sum(j)
FROM (
    SELECT i + 7 AS j
    FROM jit_micro_filter_projection
    WHERE i > 500000
) t
""",
    },
    "native_projection_chain": {
        "shape_constant": "SLJIT_SOURCE_PREFIX_PROJECTION_CHAIN_SHAPE",
        "shape_key": "sljit:source-prefix:projection-chain",
        "proof": "benchmark/micro/jit/native_projection_chain",
        "template": "benchmark/micro/jit/native_projection_chain.benchmark.in",
        "min_cardinality": 1000000,
        "threshold_row_count": 1000000,
        "candidate_shape": "projection-projection-projection",
        "expected_result": "500009500000",
        "view_sql": """
CREATE OR REPLACE TABLE jit_micro_projection_chain AS
SELECT i::BIGINT AS i
FROM range(1000000) tbl(i);
""",
        "query": """
SELECT sum(k)
FROM (
    SELECT j + 3 AS k
    FROM (
        SELECT i + 7 AS j
        FROM jit_micro_projection_chain
    ) t0
) t1
""",
    },
}


DIAGNOSTIC_FAMILIES = {
    "native_projection": {
        "shape_key": "sljit:source-prefix:projection",
        "template": "benchmark/micro/jit/native_projection.benchmark.in",
        "candidate_shape": "projection-projection",
        "include_in_sweep": True,
        "query": lambda row_count: (
            f"SELECT sum(j) FROM (SELECT i + {ADDEND} AS j FROM jit_micro)"
        ),
        "expected": projection_expected,
    },
    "native_filter": {
        "shape_key": "sljit:source-prefix:filter",
        "template": "benchmark/micro/jit/native_filter.benchmark.in",
        "candidate_shape": "filter",
        "include_in_sweep": True,
        "query": lambda row_count: (
            f"SELECT count(*) FROM jit_micro WHERE i > {filter_bound(row_count)}"
        ),
        "expected": filter_expected,
    },
    "native_filter_projection_generic": {
        "shape_key": "sljit:source-prefix:filter-projection-projection",
        "template": "benchmark/micro/jit/native_filter_projection_generic.benchmark.in",
        "candidate_shape": "filter-projection-projection",
        "include_in_sweep": True,
        "query": lambda row_count: (
            "SELECT sum(a + b) FROM ("
            f"SELECT i AS a, i + {ADDEND} AS b "
            f"FROM jit_micro WHERE i > {filter_bound(row_count)}) t"
        ),
        "expected": generic_expected,
    },
    "native_full_pipeline_decimal_projection_ungrouped_sum": {
        "shape_key": "sljit:full-pipeline:filter-projection-ungrouped-aggregate-update",
        "template": "benchmark/micro/jit/native_full_pipeline_decimal_projection_ungrouped_sum.benchmark.in",
        "candidate_shape": "scan-filter-scan-project-projection-sink",
        "include_in_sweep": False,
        "expected": decimal_projection_ungrouped_sum_expected,
    },
}


def benchmark_path(family: str, policy: str, size: str) -> str:
    if size == "threshold":
        return f"benchmark/micro/jit/{family}_threshold_{policy}.benchmark"
    if size == "large":
        return f"benchmark/micro/jit/{family}_{policy}.benchmark"
    raise ValueError(f"unknown benchmark size: {size}")


def admitted_threshold_shapes() -> tuple:
    return tuple(
        {
            "shape": f"{family}_threshold",
            "family": family,
            "size": "threshold",
            "shape_key": entry["shape_key"],
            "proof": entry["proof"],
            "benchmarks": {
                policy: benchmark_path(family, policy, "threshold") for policy in ADMITTED_POLICIES
            },
        }
        for family, entry in ADMITTED_FAMILIES.items()
    )


def admitted_trace_shapes() -> tuple:
    return tuple(
        {
            "shape": f"{family}_threshold",
            "family": family,
            "size": "threshold",
            "shape_key": entry["shape_key"],
            "proof": entry["proof"],
            "row_count": entry["threshold_row_count"],
            "expected_result": entry["expected_result"],
            "expected_candidate_shape": entry["candidate_shape"],
            "view_sql": entry["view_sql"],
            "query": entry["query"],
            "min_cardinality": entry["min_cardinality"],
        }
        for family, entry in ADMITTED_FAMILIES.items()
    )


def diagnostic_benchmark_shapes() -> tuple:
    shapes = []
    for family, entry in DIAGNOSTIC_FAMILIES.items():
        for size in ("threshold", "large"):
            shapes.append(
                {
                    "shape": f"{family}_{size}",
                    "family": family,
                    "size": size,
                    "shape_key": entry["shape_key"],
                    "benchmarks": {
                        policy: benchmark_path(family, policy, size) for policy in DIAGNOSTIC_POLICIES
                    },
                }
            )
    return tuple(shapes)


def diagnostic_sweep_shapes() -> tuple:
    return tuple(
        {
            "family": family,
            "shape_key": entry["shape_key"],
            "candidate_shape": entry["candidate_shape"],
            "query": entry["query"],
            "expected": entry["expected"],
        }
        for family, entry in DIAGNOSTIC_FAMILIES.items()
        if entry.get("include_in_sweep", False)
    )


def diagnostic_expected_families() -> dict:
    return {
        family: {
            "shape_key": entry["shape_key"],
            "candidate_shape": entry["candidate_shape"],
        }
        for family, entry in DIAGNOSTIC_FAMILIES.items()
        if entry.get("include_in_sweep", False)
    }
