#!/usr/bin/env python3
#
# Generic production workload gate for execution-region JIT.

from __future__ import annotations

import argparse
import collections
import csv
import statistics
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_common import (  # noqa: E402
    REGION_SUMMARY_FIELDS,
    counter_region_summary,
    correctness_from_rows,
    correctness_sql,
    jit_setup_sql,
    make_output_dir,
    repo_root,
    row_int,
    run_duckdb,
    timed_materialized_attempt,
    write_csv,
)

# Read-only query variants share this immutable fixture through `setup_id`.
# Preparation runs in a separate process so every timed sample reopens the same
# checkpointed database state.
GROUPED_SELECTIVE_MULTI_AGGREGATE_SETUP_SQL = (
    "CREATE OR REPLACE TABLE __jit_generic_selective_groups("
    "group_flag VARCHAR NOT NULL, group_status VARCHAR NOT NULL, event_date DATE NOT NULL, "
    "quantity DECIMAL(15,2) NOT NULL, price DECIMAL(15,2) NOT NULL, "
    "discount DECIMAL(15,2) NOT NULL, tax DECIMAL(15,2) NOT NULL); "
    "INSERT INTO __jit_generic_selective_groups "
    "SELECT CASE i % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END, "
    "CASE i % 2 WHEN 0 THEN 'F' ELSE 'O' END, "
    "DATE '2024-01-01' + CASE i % 12 "
    "WHEN 8 THEN 4 WHEN 9 THEN 5 WHEN 10 THEN 0 WHEN 11 THEN 1 "
    "ELSE CAST(i % 12 AS INTEGER) END, "
    "CAST(1 + i % 50 AS DECIMAL(15,2)), CAST(100 + i % 1000 AS DECIMAL(15,2)), "
    "CAST(i % 10 AS DECIMAL(15,2)), CAST(i % 8 AS DECIMAL(15,2)) "
    "FROM range(8000000) tbl(i);"
)

GROUPED_SORTED_RUNS_SETUP_SQL = (
    "CREATE OR REPLACE TABLE __jit_generic_sorted_runs AS "
    "SELECT ((i // 3) - 500000)::BIGINT AS group_id, "
    "((i // 3) * 3 - 1500000)::BIGINT AS sparse_group_id, "
    "500000::BIGINT AS group_offset, (i % 97)::INTEGER AS value, "
    "CASE WHEN i % 11 = 0 THEN NULL ELSE (i % 97)::INTEGER END AS nullable_value "
    "FROM range(6000000) tbl(i);"
)

GROUPED_WIDE_SORTED_RUN_LANE_COUNT = 16
GROUPED_WIDE_SORTED_RUN_INNER_AGGREGATES = ", ".join(
    f"sum(value + {lane_idx}) AS lane_{lane_idx}" for lane_idx in range(GROUPED_WIDE_SORTED_RUN_LANE_COUNT)
)
GROUPED_WIDE_SORTED_RUN_OUTER_AGGREGATES = ", ".join(
    f"sum(lane_{lane_idx}) AS lane_{lane_idx}" for lane_idx in range(GROUPED_WIDE_SORTED_RUN_LANE_COUNT)
)

JOIN_STRING_COMPLEMENTARY_SETUP_SQL = (
    "CREATE OR REPLACE TABLE __jit_generic_orders AS "
    "SELECT i::BIGINT AS order_id, "
    "CASE i % 5 WHEN 0 THEN '1-URGENT' WHEN 1 THEN '2-HIGH' "
    "WHEN 2 THEN '3-MEDIUM' ELSE '4-NOT SPECIFIED' END AS priority "
    "FROM range(1500000) tbl(i); "
    "CREATE OR REPLACE TABLE __jit_generic_shipments AS "
    "SELECT (i % 1500000)::BIGINT AS order_id, "
    "CASE i % 3 WHEN 0 THEN 'MAIL' WHEN 1 THEN 'SHIP' ELSE 'RAIL' END AS ship_mode, "
    "'group-' || CAST(14000042 + i % 9 AS VARCHAR) AS route_group "
    "FROM range(6000000) tbl(i);"
)


GENERIC_WORKLOADS = (
    {
        "name": "range_expr",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM range(50000000) tbl(i)",
        "minimum_auto_speedup": 1.20,
        "requires_compiled_auto": True,
    },
    {
        "name": "range_filter",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM range(50000000) tbl(i) WHERE i % 7 = 3",
        "minimum_auto_speedup": 1.20,
        "requires_compiled_auto": True,
    },
    {
        "name": "range_case",
        "sql": (
            "SELECT sum(CASE WHEN i % 11 < 3 THEN i * 31 + i % 97 "
            "ELSE i * 7 - i % 29 END) AS value FROM range(50000000) tbl(i)"
        ),
        "minimum_auto_speedup": 1.50,
        "requires_compiled_auto": True,
    },
    {
        "name": "range_multi_aggregate",
        "sql": (
            "SELECT sum(i * 31 + i % 97) AS total, "
            "sum(CASE WHEN i % 7 = 3 THEN i ELSE 0 END) AS selected, "
            "count(*) AS row_count FROM range(50000000) tbl(i)"
        ),
        "minimum_auto_speedup": 1.50,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_expr",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input",
        "minimum_auto_speedup": 1.10,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_filter",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(40000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input WHERE i % 7 = 3",
        # Ten alternating T4 production pairs prove 2.016x after the source
        # contract takes ownership of this proven-safe modulo predicate. Keep
        # margin for storage-scan variance while preventing a return to the
        # old vectorized-filter boundary.
        "minimum_auto_speedup": 1.08,
        "minimum_auto_speedup_by_threads": {4: 1.85},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_mixed_numeric_string_filter",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_mixed_filter("
            "id BIGINT NOT NULL, event_date DATE NOT NULL, amount INTEGER NOT NULL, status VARCHAR); "
            "INSERT INTO __jit_generic_mixed_filter "
            "SELECT i, DATE '2024-01-01' + CAST(i % 730 AS INTEGER), CAST(i % 1000 AS INTEGER), "
            "CASE WHEN i % 29 = 0 THEN NULL WHEN i % 7 = 0 THEN 'ACTIVE' "
            "WHEN i % 11 = 0 THEN 'PENDING' ELSE 'ARCHIVED' END "
            "FROM range(8000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(id * 31 + amount) AS value FROM __jit_generic_mixed_filter "
            "WHERE event_date >= DATE '2024-04-01' AND event_date < DATE '2024-10-01' "
            "AND amount >= 200 AND status IN ('ACTIVE', 'PENDING')"
        ),
        # Production promotion: 1.338x at T1 and 1.286x at T4. Preserve the
        # demonstrated gain with thread-specific margins instead of a flaky
        # machine-wide absolute timing threshold.
        "minimum_auto_speedup": 1.15,
        "minimum_auto_speedup_by_threads": {1: 1.25, 4: 1.20},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_like_fragments",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_like_fragments(id BIGINT NOT NULL, comment VARCHAR NOT NULL); "
            "INSERT INTO __jit_generic_like_fragments "
            "SELECT i, CASE WHEN i % 100 = 0 "
            "THEN 'ordinary package with special shipping requests included ' || CAST(i AS VARCHAR) "
            "ELSE 'ordinary shipping package comment with several common words ' || CAST(i AS VARCHAR) END "
            "FROM range(4000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(id * 31) AS value FROM __jit_generic_like_fragments "
            "WHERE comment NOT LIKE '%special%requests%'"
        ),
        # Fixed-shape lowering for generic two-fragment percent-only patterns
        # promotes at 1.236x (T1) and 1.219x (T4). Keep enough host-noise
        # margin while preventing the matcher from falling back to its old
        # dynamic fragment loop.
        "minimum_auto_speedup": 1.15,
        "minimum_auto_speedup_by_threads": {1: 1.18, 4: 1.16},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_low_cardinality_like",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_low_cardinality_like("
            "id BIGINT NOT NULL, comment VARCHAR NOT NULL); "
            "INSERT INTO __jit_generic_low_cardinality_like "
            "SELECT i, CASE WHEN i % 100 = 0 "
            "THEN 'ordinary package with special shipping requests included' "
            "ELSE 'ordinary shipping package comment with several common words' END "
            # This intentionally remains a DuckDB vectorized/dictionary filter.
            # Keep the policy-overhead sample long enough that its fixed CBO
            # decision cost is below the five-percent slowdown budget.
            "FROM range(16000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(id * 31) AS value FROM __jit_generic_low_cardinality_like "
            "WHERE comment NOT LIKE '%special%requests%'"
        ),
        "minimum_auto_speedup": 0.0,
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": False,
    },
    {
        "name": "scan_compare_columns",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_compare_input AS "
            "SELECT i::INTEGER AS a, "
            "(i + CASE WHEN i % 17 = 0 THEN -1 ELSE 1 END)::INTEGER AS b "
            "FROM range(30000000) tbl(i);"
        ),
        "sql": ("SELECT sum(a) AS value FROM __jit_generic_compare_input " "WHERE a < b"),
        "minimum_auto_speedup": 1.25,
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_nullable",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_nullable_input AS "
            "SELECT CASE WHEN i % 13 = 0 THEN NULL ELSE i END AS value "
            "FROM range(20000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(coalesce(value, 7) * 31 + coalesce(value % 97, 0)) AS value "
            "FROM __jit_generic_nullable_input"
        ),
        "minimum_auto_speedup": 1.0,
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "scan_nullable_multi",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_nullable_multi AS "
            "SELECT CASE WHEN i % 13 = 0 THEN NULL ELSE i END AS a, "
            "CASE WHEN i % 17 = 0 THEN NULL ELSE i * 3 END AS b, "
            "i AS fallback FROM range(20000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(coalesce(a, fallback) * 31 + "
            "coalesce(b % 97, fallback % 97)) AS value "
            "FROM __jit_generic_nullable_multi"
        ),
        "minimum_auto_speedup": 1.30,
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_scan",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT i % 1000 AS key, sum(i * 31) AS value FROM __jit_generic_input GROUP BY key ORDER BY key",
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 1.15, 4: 1.05},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_dense_multi_aggregate",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_dense_multi AS " "SELECT i::BIGINT AS i FROM range(20000000) tbl(i);"
        ),
        "sql": (
            "SELECT (i % 8)::SMALLINT AS key, "
            "sum(i * 31 + i % 97) AS total, "
            "sum(CASE WHEN i % 7 = 3 THEN i ELSE 0 END) AS selected, "
            "count(*) AS row_count "
            "FROM __jit_generic_dense_multi GROUP BY key ORDER BY key"
        ),
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 1.80, 4: 1.60},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_non_null_string_multi_aggregate",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_non_null_string_groups("
            "group_flag VARCHAR NOT NULL, group_status VARCHAR NOT NULL, "
            "quantity DECIMAL(15,2) NOT NULL, price DECIMAL(15,2) NOT NULL, "
            "discount DECIMAL(15,2) NOT NULL, tax DECIMAL(15,2) NOT NULL); "
            "INSERT INTO __jit_generic_non_null_string_groups "
            "SELECT CASE i % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END, "
            "CASE i % 2 WHEN 0 THEN 'F' ELSE 'O' END, "
            "CAST(1 + i % 50 AS DECIMAL(15,2)), CAST(100 + i % 1000 AS DECIMAL(15,2)), "
            "CAST(i % 10 AS DECIMAL(15,2)), CAST(i % 8 AS DECIMAL(15,2)) "
            "FROM range(8000000) tbl(i);"
        ),
        "sql": (
            "SELECT group_flag, group_status, sum(quantity), sum(price), "
            "sum(price * (1.00::DECIMAL(15,2) - discount)), "
            "sum(price * (1.00::DECIMAL(15,2) - discount) * (1.00::DECIMAL(15,2) + tax)), "
            "sum(discount), count(*) "
            "FROM __jit_generic_non_null_string_groups "
            "WHERE quantity > 10.00::DECIMAL(15,2) "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        "minimum_auto_speedup": 1.15,
        "minimum_auto_speedup_by_threads": {1: 1.16, 4: 1.13},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_selective_multi_aggregate",
        "setup_id": "grouped_selective_multi_aggregate_input",
        "setup_sql": GROUPED_SELECTIVE_MULTI_AGGREGATE_SETUP_SQL,
        "sql": (
            "SELECT group_flag, group_status, sum(quantity), sum(price), "
            "sum(price * (1.00::DECIMAL(15,2) - discount)), "
            "sum(price * (1.00::DECIMAL(15,2) - discount) * (1.00::DECIMAL(15,2) + tax)), "
            "sum(discount), count(*) "
            "FROM __jit_generic_selective_groups WHERE event_date <= DATE '2024-01-04' "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        "minimum_auto_speedup": 1.12,
        "minimum_auto_speedup_by_threads": {1: 1.22, 4: 1.17},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_selective_conjunction_multi_aggregate",
        "setup_id": "grouped_selective_multi_aggregate_input",
        "setup_sql": GROUPED_SELECTIVE_MULTI_AGGREGATE_SETUP_SQL,
        "sql": (
            "SELECT group_flag, group_status, sum(quantity), sum(price), "
            "sum(price * (1.00::DECIMAL(15,2) - discount)), "
            "sum(price * (1.00::DECIMAL(15,2) - discount) * (1.00::DECIMAL(15,2) + tax)), "
            "sum(discount), count(*) "
            "FROM __jit_generic_selective_groups "
            "WHERE event_date <= DATE '2024-01-04' AND tax <= 3.00::DECIMAL(15,2) "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        "minimum_auto_speedup": 1.00,
        "minimum_auto_speedup_by_threads": {1: 1.31, 4: 1.25},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_selective_three_way_conjunction_multi_aggregate",
        "setup_id": "grouped_selective_multi_aggregate_input",
        "setup_sql": GROUPED_SELECTIVE_MULTI_AGGREGATE_SETUP_SQL,
        "sql": (
            "SELECT group_flag, group_status, sum(quantity), sum(price), "
            "sum(price * (1.00::DECIMAL(15,2) - discount)), "
            "sum(price * (1.00::DECIMAL(15,2) - discount) * (1.00::DECIMAL(15,2) + tax)), "
            "sum(discount), count(*) "
            "FROM __jit_generic_selective_groups "
            "WHERE event_date <= DATE '2024-01-04' "
            "AND tax <= 3.00::DECIMAL(15,2) "
            "AND quantity <= 25.00::DECIMAL(15,2) "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        "minimum_auto_speedup": 1.00,
        "minimum_auto_speedup_by_threads": {1: 1.25, 4: 1.20},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_sorted_runs",
        "setup_id": "grouped_sorted_runs_input",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT group_id, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 3.00, 4: 1.75},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_materialized_sorted_runs",
        "setup_id": "grouped_sorted_runs_input",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT group_id + group_offset AS grouped_value, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY group_id + group_offset"
            ") grouped"
        ),
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 2.25, 4: 1.45},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_sparse_sorted_runs",
        "setup_id": "grouped_sorted_runs_input",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT sparse_group_id, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY sparse_group_id"
            ") grouped"
        ),
        # More than one interval per vector crosses the bounded uniqueness-summary
        # budget. Coalescing remains a proof representation change, not a reason to
        # rehash a locally monotonic stream during finalization.
        "minimum_auto_speedup": 0.0,
        # Ten alternating production runs prove 2.600x at T1 and 1.134x at T4.
        # The parallel floor leaves margin for the shared two-million-row state scan.
        "minimum_auto_speedup_by_threads": {1: 2.35, 4: 1.08},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_nullable_sorted_runs",
        "setup_id": "grouped_sorted_runs_input",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value, sum(group_count) AS value_count FROM ("
            "SELECT group_id, sum(nullable_value) AS group_sum, "
            "count(nullable_value) AS group_count "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 1.70, 4: 1.45},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_wide_sorted_runs",
        "setup_id": "grouped_sorted_runs_input",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            f"SELECT {GROUPED_WIDE_SORTED_RUN_OUTER_AGGREGATES} FROM ("
            f"SELECT group_id, {GROUPED_WIDE_SORTED_RUN_INNER_AGGREGATES} "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        # Wide affine reductions share one source run and expand directly into
        # final aggregate states. Code size stays bounded independently of the
        # number of output lanes.
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 2.25, 4: 1.90},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "grouped_distinct",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_distinct_input AS "
            "SELECT (i % 127)::INTEGER AS group_id, (i % 1009)::INTEGER AS value_id "
            "FROM range(2000000) tbl(i); "
            "CREATE OR REPLACE TABLE __jit_generic_distinct_blocked AS "
            "SELECT (i * 17 % 1009)::INTEGER AS value_id FROM range(29) tbl(i);"
        ),
        "sql": (
            "SELECT group_id, count(DISTINCT value_id) AS value_count "
            "FROM __jit_generic_distinct_input "
            "WHERE value_id NOT IN (SELECT value_id FROM __jit_generic_distinct_blocked) "
            "GROUP BY group_id ORDER BY group_id"
        ),
        "minimum_auto_speedup": 0.0,
        "minimum_auto_speedup_by_threads": {1: 1.03},
        "requires_compiled_auto": True,
    },
    {
        "name": "join_range",
        "sql": (
            "SELECT sum(a.i * b.i) AS value FROM range(2000000) a(i) "
            "JOIN range(2000000) b(i) ON a.i = b.i WHERE a.i % 7 = 3"
        ),
        "minimum_auto_speedup": 0.0,
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": False,
    },
    {
        "name": "join_exact_filter_build",
        "setup_sql": (
            "CREATE OR REPLACE TABLE __jit_generic_exact_fact AS "
            "SELECT (i % 500000)::INTEGER AS filter_key, i::BIGINT AS join_key, "
            "(i % 97)::BIGINT AS payload FROM range(8000000) tbl(i); "
            "CREATE OR REPLACE TABLE __jit_generic_exact_filter AS "
            "SELECT ((i * 251) % 500000)::INTEGER AS filter_key FROM range(2000) tbl(i); "
            "CREATE OR REPLACE TABLE __jit_generic_exact_probe AS "
            "SELECT i::BIGINT AS join_key FROM range(8000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(selected.payload) AS value FROM __jit_generic_exact_probe probe "
            "JOIN (SELECT fact.join_key, fact.payload "
            "      FROM __jit_generic_exact_fact fact "
            "      JOIN __jit_generic_exact_filter filter USING (filter_key)) selected "
            "USING (join_key)"
        ),
        "minimum_auto_speedup": 1.08,
        "minimum_auto_speedup_by_threads": {1: 1.15},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
    {
        "name": "join_string_complementary_grouped_sum",
        "setup_id": "join_string_complementary_input",
        "setup_sql": JOIN_STRING_COMPLEMENTARY_SETUP_SQL,
        "sql": (
            "SELECT shipment.ship_mode, "
            "sum(CASE WHEN orders.priority = '1-URGENT' OR orders.priority = '2-HIGH' "
            "THEN 1 ELSE 0 END) AS high_priority_count, "
            "sum(CASE WHEN orders.priority <> '1-URGENT' AND orders.priority <> '2-HIGH' "
            "THEN 1 ELSE 0 END) AS low_priority_count "
            "FROM __jit_generic_shipments shipment "
            "JOIN __jit_generic_orders orders USING (order_id) "
            "WHERE shipment.ship_mode IN ('MAIL', 'SHIP') "
            "GROUP BY shipment.ship_mode ORDER BY shipment.ship_mode"
        ),
        # Promotion receipts: 1.332x at T1 and 1.250x at T4. Fixed-width RHS
        # values bind their immutable packed storage once; an external-string
        # classifier remains reserved for equality that cannot use that layout.
        # With direct perfect-hash output, an incremental source derives the
        # build index as key-minus-minimum, while all-valid incremental inline
        # groups accumulate at their ordinal without rebinding selections.
        "minimum_auto_speedup": 1.21,
        "minimum_auto_speedup_by_threads": {1: 1.31, 4: 1.24},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
        # This is a physical storage contract, not a query-specific shortcut:
        # wide values are compared as their immutable packed representation.
        "required_runtime_paths": (
            "hash_join_probe.perfect_probe.direct_aggregate_consumer.compressed_uhugeint_predicate=",
            "hash_join_probe.perfect_probe.direct_aggregate_consumer.inline_string_identity_known_groups=",
            "hash_join_probe.perfect_probe.direct_aggregate_consumer.derived_build_index.contiguous_source=",
        ),
    },
    {
        "name": "join_string_complementary_medium_groups",
        "setup_id": "join_string_complementary_input",
        "setup_sql": JOIN_STRING_COMPLEMENTARY_SETUP_SQL,
        "sql": (
            "SELECT shipment.route_group, "
            "sum(CASE WHEN orders.priority = '1-URGENT' OR orders.priority = '2-HIGH' "
            "THEN 1 ELSE 0 END) AS high_priority_count, "
            "sum(CASE WHEN orders.priority <> '1-URGENT' AND orders.priority <> '2-HIGH' "
            "THEN 1 ELSE 0 END) AS low_priority_count "
            "FROM __jit_generic_shipments shipment "
            "JOIN __jit_generic_orders orders USING (order_id) "
            "GROUP BY shipment.route_group ORDER BY shipment.route_group"
        ),
        # Nine recurring groups cross the direct-tier boundary and prove that
        # the runtime-observed ninth key switches the exact accumulator to hash
        # lookup even when persisted NDV statistics underestimate the domain.
        # Wide-key staging promotes at 1.179x (T1) and 1.141x (T4).
        "minimum_auto_speedup": 1.08,
        "minimum_auto_speedup_by_threads": {1: 1.12, 4: 1.08},
        "max_auto_slowdown": 1.05,
        "requires_compiled_auto": True,
    },
)

RUN_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "query_time_us",
    "correctness_diff",
    "jit_runtime_path_counts",
    "jit_runtime_delegation_counts",
    "generated_stage_runtime_breakdown",
    *REGION_SUMMARY_FIELDS,
)

SUMMARY_FIELDS = (
    "workload",
    "policy",
    "timing_mode",
    "performance_checks",
    "run_count",
    "median_s",
    "speedup_vs_off_median",
    "correctness_diff",
    "compiled_regions",
    "runtime_regions",
    "runtime_events",
    "compile_errors",
    "minimum_auto_speedup",
    "max_auto_slowdown",
)


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Benchmark generic DuckDB execution-region JIT workloads")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "reldebug" / "duckdb")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--repeats",
        type=int,
        choices=(5, 10),
        default=5,
        help="order-alternating production pairs: 5 for a candidate, 10 for an explicit promotion",
    )
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--workloads", nargs="+", default=None)
    parser.add_argument("--trace-runtime", action="store_true")
    parser.add_argument(
        "--jit-cbo-setting",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="repeatable JIT CBO setting override",
    )
    return parser.parse_args()


def make_args(args: argparse.Namespace) -> SimpleNamespace:
    return SimpleNamespace(
        duckdb=args.duckdb.resolve(),
        backend=args.backend,
        jit_extension=args.jit_extension,
        threads=args.threads,
        event_log_size=args.event_log_size,
        trace_runtime=args.trace_runtime,
        jit_verify=False,
        jit_cbo_setting=args.jit_cbo_setting,
    )


def median_us(values: list[int]) -> int:
    return int(round(statistics.median(values))) if values else 0


def policy_order(repeat: int) -> tuple[str, str]:
    if repeat <= 0:
        raise ValueError("repeat must be positive")
    # Each pair observes the same database state transition, but reversing the
    # first policy on alternating repeats prevents cache and frequency warmup
    # from consistently favoring JIT as the second query.
    return ("off", "auto") if repeat % 2 else ("auto", "off")


def prepare_workload(
    args: SimpleNamespace,
    db_path: Path,
    workload: dict,
    expected_table: str,
    prepare_setup: bool,
) -> None:
    sql = jit_setup_sql(
        args,
        "off",
        trace_runtime=False,
        trace_decisions=False,
        event_log_size=0,
    )
    if prepare_setup:
        sql += workload.get("setup_sql", "")
    sql += f"\nCREATE OR REPLACE TABLE {expected_table} AS\n{workload['sql']};"
    run_duckdb(
        args.duckdb,
        db_path,
        sql,
        f"generic workload {workload['name']} fixture preparation",
    )


def run_workload(
    args: SimpleNamespace,
    db_path: Path,
    out_dir: Path,
    workload: dict,
    policy: str,
    repeat: int,
    expected_table: str,
) -> dict:
    workload_name = workload["name"]
    result_table = f"__jit_generic_result_{workload_name}_{policy}_{repeat}"
    setup_sql = jit_setup_sql(
        args,
        policy,
        trace_runtime=args.trace_runtime,
        trace_decisions=args.trace_runtime,
        reset_events=True,
        reset_counters=True,
    )
    artifact_path = out_dir / f"{workload_name}_{policy}_{repeat}.json"
    attempt = timed_materialized_attempt(
        args,
        db_path,
        setup_sql,
        result_table,
        workload["sql"],
        artifact_path,
        f"generic workload {workload_name} {policy} repeat {repeat}",
        validation_sql=correctness_sql(expected_table, result_table),
        cleanup_sql=f"DROP TABLE IF EXISTS {result_table};",
        collect_counters=True,
    )
    counters = attempt["counters"]
    region_metrics = counter_region_summary(counters)
    correctness = correctness_from_rows(attempt["validation"], result_table)
    return {
        "workload": workload_name,
        "policy": policy,
        "repeat": repeat,
        "query_time_us": attempt["query_time_us"],
        "correctness_diff": correctness["correctness_diff"],
        "jit_runtime_path_counts": ";".join(
            str(row.get("jit_runtime_path_counts", "")) for row in counters if row.get("jit_runtime_path_counts")
        ),
        "jit_runtime_delegation_counts": ";".join(
            str(row.get("jit_runtime_delegation_counts", ""))
            for row in counters
            if row.get("jit_runtime_delegation_counts")
        ),
        "generated_stage_runtime_breakdown": ";".join(
            str(row.get("generated_stage_runtime_breakdown", ""))
            for row in counters
            if row.get("generated_stage_runtime_breakdown")
        ),
        **region_metrics,
    }


def minimum_auto_speedup(workload: dict, threads: int) -> float:
    return float(
        workload.get("minimum_auto_speedup_by_threads", {}).get(threads, workload.get("minimum_auto_speedup", 0.0))
    )


def summarize(rows: list[dict], workloads: tuple[dict, ...], threads: int, trace_runtime: bool) -> list[dict]:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["workload"], row["policy"])].append(row)
    off_medians = {
        workload: median_us([row_int(row, "query_time_us") for row in workload_rows])
        for (workload, policy), workload_rows in grouped.items()
        if policy == "off"
    }
    result = []
    for workload in workloads:
        name = workload["name"]
        for policy in ("off", "auto"):
            workload_rows = grouped[(name, policy)]
            timing = median_us([row_int(row, "query_time_us") for row in workload_rows])
            off = off_medians.get(name, 0)
            result.append(
                {
                    "workload": name,
                    "policy": policy,
                    "timing_mode": "profile" if trace_runtime else "production",
                    "performance_checks": not trace_runtime,
                    "run_count": len(workload_rows),
                    "median_s": f"{timing / 1_000_000.0:.9f}",
                    "speedup_vs_off_median": f"{(float(off) / timing) if off and timing else 0.0:.6f}",
                    "correctness_diff": sum(row_int(row, "correctness_diff") for row in workload_rows),
                    "compiled_regions": sum(row_int(row, "compiled_regions") for row in workload_rows),
                    "runtime_regions": sum(row_int(row, "runtime_regions") for row in workload_rows),
                    "runtime_events": sum(row_int(row, "runtime_events") for row in workload_rows),
                    "compile_errors": sum(row_int(row, "compile_errors") for row in workload_rows),
                    "minimum_auto_speedup": minimum_auto_speedup(workload, threads),
                    "max_auto_slowdown": workload.get("max_auto_slowdown", 1.05),
                }
            )
    return result


def verification_failures(
    summary: list[dict],
    runs: list[dict],
    workloads: tuple[dict, ...],
    threads: int,
    trace_runtime: bool,
) -> list[str]:
    by_workload = {(row["workload"], row["policy"]): row for row in summary}
    failures = []
    for workload in workloads:
        name = workload["name"]
        off = by_workload[(name, "off")]
        auto = by_workload[(name, "auto")]
        if int(off["correctness_diff"]) != 0 or int(auto["correctness_diff"]) != 0:
            failures.append(f"{name}: correctness mismatch")
        if int(auto["compile_errors"]) != 0:
            failures.append(f"{name}: JIT compile errors")
        if not trace_runtime:
            speedup = float(auto["speedup_vs_off_median"])
            minimum_speedup = minimum_auto_speedup(workload, threads)
            if speedup < minimum_speedup:
                failures.append(f"{name}: auto speedup {speedup:.3f} below required {minimum_speedup:.3f}")
            max_slowdown = float(workload.get("max_auto_slowdown", 1.05))
            if speedup > 0 and speedup < 1.0 / max_slowdown:
                failures.append(f"{name}: auto slowdown {1.0 / speedup:.3f} exceeds {max_slowdown:.3f}")
        if workload.get("requires_compiled_auto") and int(auto["compiled_regions"]) == 0:
            failures.append(f"{name}: auto did not compile a region")
        if trace_runtime and workload.get("requires_compiled_auto") and int(auto["runtime_events"]) == 0:
            failures.append(f"{name}: traced auto run did not execute a compiled region")
        if trace_runtime:
            auto_runs = [run for run in runs if run["workload"] == name and run["policy"] == "auto"]
            for required_path in workload.get("required_runtime_paths", ()):
                missing_repeats = [
                    str(run["repeat"]) for run in auto_runs if required_path not in run["jit_runtime_path_counts"]
                ]
                if missing_repeats:
                    failures.append(
                        f"{name}: required runtime path {required_path!r} missing from auto repeats "
                        + ", ".join(missing_repeats)
                    )
    return failures


def main() -> int:
    args = parse_args()
    if args.threads <= 0:
        raise ValueError("--threads must be positive")
    if not args.duckdb.exists():
        raise FileNotFoundError(args.duckdb)
    out_dir = make_output_dir(args.out_dir, "generic_benchmark")
    db_path = out_dir / "generic.duckdb"
    runtime_args = make_args(args)
    rows = []
    prepared_setups: dict[str, str] = {}
    known_workloads = {workload["name"]: workload for workload in GENERIC_WORKLOADS}
    if args.workloads:
        unknown_workloads = [name for name in args.workloads if name not in known_workloads]
        if unknown_workloads:
            raise ValueError(f"unknown workloads: {', '.join(unknown_workloads)}")
        workloads = tuple(known_workloads[name] for name in args.workloads)
    else:
        workloads = GENERIC_WORKLOADS
    try:
        for workload in workloads:
            expected_table = f"__jit_generic_expected_{workload['name']}"
            setup_id = workload.get("setup_id", workload["name"])
            setup_sql = workload.get("setup_sql", "")
            if setup_id in prepared_setups and prepared_setups[setup_id] != setup_sql:
                raise ValueError(f"workloads sharing setup_id {setup_id!r} must use identical setup_sql")
            materialize_setup = setup_id not in prepared_setups
            prepare_workload(
                runtime_args,
                db_path,
                workload,
                expected_table,
                prepare_setup=materialize_setup,
            )
            prepared_setups[setup_id] = setup_sql
            for repeat in range(1, args.repeats + 1):
                for policy in policy_order(repeat):
                    rows.append(
                        run_workload(
                            runtime_args,
                            db_path,
                            out_dir,
                            workload,
                            policy,
                            repeat,
                            expected_table,
                        )
                    )
        summary = summarize(rows, workloads, args.threads, args.trace_runtime)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, rows)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summary)
        failures = verification_failures(summary, rows, workloads, args.threads, args.trace_runtime)
        if failures:
            gate = "runtime proof" if args.trace_runtime else "performance"
            raise RuntimeError(f"generic JIT {gate} gate failed: " + "; ".join(failures))
        gate = "runtime proof" if args.trace_runtime else "production benchmark"
        print(f"generic JIT {gate} passed: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        return 0
    finally:
        if db_path.exists():
            db_path.unlink()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
