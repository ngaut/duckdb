#!/usr/bin/env python3
#
# The workloads of the JIT performance gate and their checked-in thresholds.
#
# A workload is one query. Per thread count it is qualified for, `floor` is the least speedup of the JIT over the
# native engine, measured as the native engine's instructions over the JIT's, `ceiling` the most instructions the JIT
# may retire and `compiled` whether the JIT compiles a region for it. A workload with `setup_sql` reads the tables
# that SQL creates; workloads that share a fixture are adjacent and use the same SQL.

from __future__ import annotations

from pathlib import Path

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
        "floor": {1: 2.73, 4: 2.73},
        "ceiling": {1: 1_696_300_000, 4: 1_696_200_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_filter",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM range(50000000) tbl(i) WHERE i % 7 = 3",
        "floor": {1: 2.01, 4: 2.01},
        "ceiling": {1: 1_089_700_000, 4: 1_091_700_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_case",
        "sql": (
            "SELECT sum(CASE WHEN i % 11 < 3 THEN i * 31 + i % 97 "
            "ELSE i * 7 - i % 29 END) AS value FROM range(50000000) tbl(i)"
        ),
        "floor": {1: 3.06, 4: 3.06},
        "ceiling": {1: 2_310_100_000, 4: 2_311_500_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_multi_aggregate",
        "sql": (
            "SELECT sum(i * 31 + i % 97) AS total, "
            "sum(CASE WHEN i % 7 = 3 THEN i ELSE 0 END) AS selected, "
            "count(*) AS row_count FROM range(50000000) tbl(i)"
        ),
        "floor": {1: 2.79, 4: 2.80},
        "ceiling": {1: 2_564_500_000, 4: 2_565_400_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "join_range",
        "sql": (
            "SELECT sum(a.i * b.i) AS value FROM range(2000000) a(i) "
            "JOIN range(2000000) b(i) ON a.i = b.i WHERE a.i % 7 = 3"
        ),
        "floor": {1: 1.17, 4: 1.18},
        "ceiling": {1: 170_800_000, 4: 177_500_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "scan_expr",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input",
        "floor": {1: 0.99, 4: 0.99},
        "ceiling": {1: 518_700_000, 4: 521_200_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_scan",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT i % 1000 AS key, sum(i * 31) AS value FROM __jit_generic_input GROUP BY key ORDER BY key",
        "floor": {1: 1.12, 4: 1.12},
        "ceiling": {1: 926_600_000, 4: 930_300_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "scan_filter",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(40000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input WHERE i % 7 = 3",
        # The source contract owns this exception-free modulo predicate, so the
        # filter runs fused with the aggregate instead of as a vectorized filter.
        "floor": {1: 1.78, 4: 1.78},
        "ceiling": {1: 772_900_000, 4: 775_400_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.39, 4: 1.46},
        "ceiling": {1: 272_200_000, 4: 281_900_000},
        "compiled": {1: True, 4: True},
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
        # Multi-fragment LIKE patterns: exact-candidate verification, selection
        # compaction and paired vector scans with an overlapping tail.
        "floor": {1: 1.36, 4: 1.36},
        "ceiling": {1: 1_878_500_000, 4: 1_882_300_000},
        "compiled": {1: True, 4: True},
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
            "FROM range(16000000) tbl(i);"
        ),
        "sql": (
            "SELECT sum(id * 31) AS value FROM __jit_generic_low_cardinality_like "
            "WHERE comment NOT LIKE '%special%requests%'"
        ),
        # The native dictionary filter evaluates the pattern once per distinct comment, which leaves a compiled
        # filter nothing to save: the floor bounds what compiling this shape may cost.
        "floor": {1: 0.83, 4: 0.83},
        "ceiling": {1: 681_700_000, 4: 682_400_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.55, 4: 1.54},
        "ceiling": {1: 426_300_000, 4: 428_900_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 2.35, 4: 2.34},
        "ceiling": {1: 1_006_000_000, 4: 1_014_800_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.91, 4: 1.90},
        "ceiling": {1: 1_406_900_000, 4: 1_417_800_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.17, 4: 1.16},
        "ceiling": {1: 1_783_400_000, 4: 1_788_700_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.57, 4: 1.56},
        "ceiling": {1: 871_300_000, 4: 878_600_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_non_null_string_reference_aggregate",
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
            "SELECT group_flag, group_status, sum(quantity), sum(price), sum(discount), sum(tax), count(*) "
            "FROM __jit_generic_non_null_string_groups "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        # A generated reference-only string perfect-hash update would replace an
        # efficient vectorized primitive without removing expression work, so
        # the CBO keeps this shape vectorized.
        "floor": {1: 0.94, 4: 0.94},
        "ceiling": {1: 868_700_000, 4: 871_200_000},
        "compiled": {1: False, 4: False},
    },
    {
        "name": "grouped_selective_multi_aggregate",
        "setup_sql": GROUPED_SELECTIVE_MULTI_AGGREGATE_SETUP_SQL,
        "sql": (
            "SELECT group_flag, group_status, sum(quantity), sum(price), "
            "sum(price * (1.00::DECIMAL(15,2) - discount)), "
            "sum(price * (1.00::DECIMAL(15,2) - discount) * (1.00::DECIMAL(15,2) + tax)), "
            "sum(discount), count(*) "
            "FROM __jit_generic_selective_groups WHERE event_date <= DATE '2024-01-04' "
            "GROUP BY group_flag, group_status ORDER BY group_flag, group_status"
        ),
        "floor": {1: 1.72, 4: 1.70},
        "ceiling": {1: 741_400_000, 4: 749_500_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_selective_conjunction_multi_aggregate",
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
        "floor": {1: 2.08, 4: 2.06},
        "ceiling": {1: 583_700_000, 4: 591_200_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_selective_three_way_conjunction_multi_aggregate",
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
        "floor": {1: 1.97, 4: 1.95},
        "ceiling": {1: 534_800_000, 4: 541_700_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_sorted_runs",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT group_id, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        "floor": {1: 1.75, 4: 2.51},
        "ceiling": {1: 444_100_000, 4: 455_800_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_affine_sorted_runs",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT group_id + group_offset AS grouped_value, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY group_id + group_offset"
            ") grouped"
        ),
        "floor": {1: 1.45, 4: 2.09},
        "ceiling": {1: 542_800_000, 4: 556_800_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_sparse_sorted_runs",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value FROM ("
            "SELECT sparse_group_id, sum(value) AS group_sum "
            "FROM __jit_generic_sorted_runs GROUP BY sparse_group_id"
            ") grouped"
        ),
        # More than one interval per vector crosses the bounded uniqueness-summary
        # budget. Coalescing is a proof representation change, not a reason to
        # rehash a locally monotonic stream during finalization.
        # Producer-owned monotonic summaries and progression-boundary publication
        # keep parallel finalization from rehashing.
        "floor": {1: 1.74, 4: 2.50},
        "ceiling": {1: 445_400_000, 4: 458_400_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_nullable_sorted_runs",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            "SELECT sum(group_sum) AS value, sum(group_count) AS value_count FROM ("
            "SELECT group_id, sum(nullable_value) AS group_sum, "
            "count(nullable_value) AS group_count "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        "floor": {1: 1.32, 4: 1.84},
        "ceiling": {1: 700_600_000, 4: 708_900_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_wide_sorted_runs",
        "setup_sql": GROUPED_SORTED_RUNS_SETUP_SQL,
        "sql": (
            f"SELECT {GROUPED_WIDE_SORTED_RUN_OUTER_AGGREGATES} FROM ("
            f"SELECT group_id, {GROUPED_WIDE_SORTED_RUN_INNER_AGGREGATES} "
            "FROM __jit_generic_sorted_runs GROUP BY group_id"
            ") grouped"
        ),
        # Wide affine reductions share one source run and expand directly into
        # final aggregate states. Canonical machine-word batches classify once
        # and use a raw-pointer progression writer, while wide values keep the
        # exact general path. Append-only ownership skips pointer-table resize
        # and capacity-abandon work once uniqueness is proven.
        "floor": {1: 4.33, 4: 4.82},
        "ceiling": {1: 891_600_000, 4: 904_000_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 1.16, 4: 1.71},
        "ceiling": {1: 260_800_000, 4: 389_700_000},
        "compiled": {1: True, 4: True},
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
        # The outer regular probe reduces dictionary-backed RHS values directly
        # into the ungrouped aggregate state: no match selection, RHS gather,
        # projection, or payload vector is published. The finalized filter view
        # is bound once and holds no owning materialization state.
        "floor": {1: 1.68, 4: 1.62},
        "ceiling": {1: 93_000_000, 4: 100_000_000},
        "compiled": {1: True, 4: True},
        "required_runtime_paths": (
            "hash_join_probe.regular_probe.all_valid.flat.single_key.no_chain." "direct_ungrouped_aggregate_consumer=",
            "aggregate_update.join_output_probe_consumer_ungrouped_aggregate.dictionary_source=",
        ),
    },
    {
        "name": "join_string_complementary_grouped_sum",
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
        # Fixed-width RHS values bind their immutable packed storage once; an
        # external-string classifier serves equality that cannot use that layout.
        # With direct perfect-hash output, an incremental source derives the
        # build index as key-minus-minimum, while all-valid incremental inline
        # groups accumulate at their ordinal without rebinding selections.
        "floor": {1: 1.78, 4: 1.75},
        "ceiling": {1: 754_000_000, 4: 776_400_000},
        "compiled": {1: True, 4: True},
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
        "floor": {1: 2.28, 4: 2.30},
        "ceiling": {1: 717_400_000, 4: 739_000_000},
        "compiled": {1: True, 4: True},
    },
)

# Columns as real tables store them: NULLs, dictionary-compressed strings, narrow integers, decimals and doubles, a
# small dimension table to join, and randomly ordered grouping keys.
ENCODINGS_SETUP_SQL = (
    "CREATE OR REPLACE TABLE __jit_encodings_facts AS "
    "SELECT i AS id, i % 97 AS g, (i * 7919) % 1000 AS k, "
    "CASE WHEN i % 13 = 0 THEN NULL ELSE i % 10007 END AS n, "
    "(i % 100000)::DECIMAL(15,2) AS price, ((i % 50)::DECIMAL(4,2)) / 100 AS disc, "
    "DATE '1992-01-01' + (i % 2500)::INTEGER AS dt, "
    "['AIR','MAIL','SHIP','RAIL','TRUCK','FOB','REG AIR'][(i % 7) + 1] AS mode, "
    "'cust#' || (i % 50000)::VARCHAR AS name, "
    "(i % 1000)::DOUBLE / 3 AS dbl, (i % 127)::TINYINT AS ti, (i % 30000)::SMALLINT AS si, "
    "hash(i) % 65536 AS rk, hash(i * 3) % 64 AS ra, (hash(i * 7) % 1000)::INTEGER AS rv "
    "FROM range(8000000) tbl(i); "
    "CREATE OR REPLACE TABLE __jit_encodings_dim AS "
    "SELECT i AS k, 'dim' || i::VARCHAR AS label, CASE WHEN i % 5 = 0 THEN NULL ELSE i % 37 END AS nk, "
    "i % 3 AS cat FROM range(1000) tbl(i);"
)

# name -> (sql, whether the JIT compiles it per thread count it is qualified for). Workloads that sum doubles are
# qualified on one thread only: the native engine adds doubles in a thread-dependent order, which rounding the result
# cannot fully hide.
ENCODINGS_QUERIES = {
    "decimal_double_product": (
        "SELECT round(sum(price * (1 - disc)), -3) FROM __jit_encodings_facts WHERE disc > 0.05",
        {1: True},
    ),
    "decimal_grouped": (
        "SELECT g, round(sum(price * (1 - disc)), -2), round(avg(price), 4) FROM __jit_encodings_facts GROUP BY g",
        {1: True},
    ),
    "date_range": (
        "SELECT sum(price) FROM __jit_encodings_facts WHERE dt >= DATE '1994-01-01' AND dt < DATE '1995-01-01'",
        {1: True, 4: True},
    ),
    "date_year_groups": (
        "SELECT year(dt), count(*) FROM __jit_encodings_facts GROUP BY ALL",
        {1: True, 4: True},
    ),
    "dictionary_case": (
        "SELECT sum(CASE WHEN mode = 'AIR' THEN price ELSE 0 END), sum(price) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "dictionary_contains": (
        "SELECT count(*) FROM __jit_encodings_facts WHERE name LIKE '%99%'",
        {1: False, 4: False},
    ),
    "dictionary_groups": (
        "SELECT mode, sum(id) FROM __jit_encodings_facts GROUP BY mode",
        {1: True, 4: True},
    ),
    "dictionary_groups_filtered": (
        "SELECT mode, count(*) FROM __jit_encodings_facts WHERE k < 500 GROUP BY mode",
        {1: True, 4: True},
    ),
    "nullable_groups": (
        "SELECT g, sum(n), count(n) FROM __jit_encodings_facts GROUP BY g",
        {1: True, 4: True},
    ),
    "nullable_min_max": (
        "SELECT min(n), max(n) FROM __jit_encodings_facts WHERE k > 100",
        {1: True, 4: True},
    ),
    "narrow_integers": (
        "SELECT sum(ti::INTEGER * si) FROM __jit_encodings_facts WHERE ti > 10",
        {1: True, 4: True},
    ),
    "double_filter": (
        "SELECT round(sum(dbl * 2), -2) FROM __jit_encodings_facts WHERE dbl > 100.5",
        {1: True},
    ),
    "double_range": (
        "SELECT sum(id) FROM __jit_encodings_facts WHERE disc BETWEEN 0.05 AND 0.07",
        {1: True, 4: True},
    ),
    "dimension_join": (
        "SELECT sum(f.id) FROM __jit_encodings_facts f JOIN __jit_encodings_dim d ON f.k = d.k WHERE d.cat = 1",
        {1: True, 4: True},
    ),
    "dimension_string_groups": (
        "SELECT d.label, sum(f.price) FROM __jit_encodings_facts f JOIN __jit_encodings_dim d ON f.k = d.k GROUP BY d.label",
        {1: True, 4: True},
    ),
    "semi_join": (
        "SELECT count(*) FROM __jit_encodings_facts WHERE k IN (SELECT k FROM __jit_encodings_dim WHERE cat = 2)",
        {1: False, 4: False},
    ),
    "anti_join": (
        "SELECT count(*) FROM __jit_encodings_facts WHERE k NOT IN (SELECT k FROM __jit_encodings_dim WHERE cat = 2)",
        {1: True, 4: True},
    ),
    "integer_in_list": (
        "SELECT sum(id) FROM __jit_encodings_facts WHERE g IN (1, 5, 17, 33, 90)",
        {1: True, 4: True},
    ),
    "dictionary_or_nullable": (
        "SELECT count(*) FROM __jit_encodings_facts WHERE mode = 'AIR' OR n < 50",
        {1: True, 4: True},
    ),
    "nullable_coalesce": (
        "SELECT sum(coalesce(n, 0) + k) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "modulo_groups": (
        "SELECT k % 10, sum(id) FROM __jit_encodings_facts WHERE g < 50 GROUP BY 1",
        {1: True, 4: True},
    ),
    "distinct_count": (
        "SELECT count(DISTINCT g) FROM __jit_encodings_facts WHERE k < 900",
        {1: True, 4: True},
    ),
    "filtered_top_n": (
        "SELECT id, price FROM __jit_encodings_facts WHERE k = 7 ORDER BY price DESC, id LIMIT 10",
        {1: False, 4: False},
    ),
    "dictionary_prefix": (
        "SELECT count(*) FROM __jit_encodings_facts WHERE name LIKE 'cust#12%'",
        {1: False, 4: False},
    ),
    "integer_case": (
        "SELECT sum(CASE WHEN k < 300 THEN id ELSE -id END) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "double_case": (
        "SELECT round(sum(CASE WHEN k < 300 THEN dbl ELSE -dbl END), -2) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "case_null_sums": (
        "SELECT sum(CASE WHEN k < 300 THEN price END), sum(CASE WHEN k > 600 THEN id END) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "case_null_groups": (
        "SELECT g, sum(CASE WHEN k < 300 THEN price END) FROM __jit_encodings_facts GROUP BY g",
        {1: True, 4: True},
    ),
    "case_null_counts": (
        "SELECT g, count(CASE WHEN k < 300 THEN 1 END) FROM __jit_encodings_facts GROUP BY g",
        {1: True, 4: True},
    ),
    "case_null_count_sums": (
        "SELECT count(CASE WHEN k < 300 THEN 1 END), sum(CASE WHEN k > 600 THEN id END), count(*) "
        "FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    "aggregate_filter_groups": (
        "SELECT g, sum(id) FILTER (WHERE k < 300), count(*) FILTER (WHERE k > 600) FROM __jit_encodings_facts "
        "GROUP BY g",
        {1: True, 4: True},
    ),
    "aggregate_filter_sums": (
        "SELECT sum(id) FILTER (WHERE k < 300), count(*) FILTER (WHERE k > 600), count(*) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    # rv is random: a branch on its filter mispredicts every other row
    "aggregate_filter_random_groups": (
        "SELECT g, sum(id) FILTER (WHERE rv < 500), count(*) FILTER (WHERE rv >= 750) FROM __jit_encodings_facts "
        "GROUP BY g",
        {1: True, 4: True},
    ),
    "aggregate_filter_random_sums": (
        "SELECT sum(id) FILTER (WHERE rv < 500), count(*) FILTER (WHERE rv >= 750), count(*) FROM __jit_encodings_facts",
        {1: True, 4: True},
    ),
    # a perfect hash aggregate over an expression key of 16 bits
    "expression_key_groups": (
        "SELECT id % 50000 AS gg, sum(id) FROM __jit_encodings_facts GROUP BY gg",
        {1: True, 4: True},
    ),
    # random conditions for the FILTERs of a perfect hash aggregate over an expression key of 16 bits
    "expression_key_filter_counts": (
        "SELECT id % 50000 AS gg, count(*) FILTER (WHERE rv < 500), count(*) FILTER (WHERE rv >= 750) "
        "FROM __jit_encodings_facts GROUP BY gg",
        {1: True, 4: True},
    ),
    "narrow_between": (
        "SELECT sum(id) FROM __jit_encodings_facts WHERE si BETWEEN 100 AND 2000",
        {1: True, 4: True},
    ),
    "nullable_key_join": (
        "SELECT count(*) FROM __jit_encodings_facts f JOIN __jit_encodings_dim d ON f.g = d.nk",
        {1: False, 4: False},
    ),
    "random_key_groups": (
        "SELECT rk, sum(id) FROM __jit_encodings_facts GROUP BY rk",
        {1: True, 4: True},
    ),
    "random_key_counts": (
        "SELECT rk, count(*) FROM __jit_encodings_facts GROUP BY rk",
        {1: True, 4: True},
    ),
    "random_multi_key_groups": (
        "SELECT ra, rk, sum(id) FROM __jit_encodings_facts GROUP BY ra, rk",
        {1: True, 4: True},
    ),
}

# name -> (floor, ceiling) per thread count
ENCODINGS_THRESHOLDS = {
    "decimal_double_product": ({1: 2.63}, {1: 319_800_000}),
    "decimal_grouped": ({1: 1.27}, {1: 846_200_000}),
    "date_range": ({1: 2.13, 4: 2.10}, {1: 113_800_000, 4: 116_300_000}),
    "date_year_groups": ({1: 0.95, 4: 0.95}, {1: 447_600_000, 4: 453_300_000}),
    "dictionary_case": ({1: 1.22, 4: 1.21}, {1: 248_800_000, 4: 252_200_000}),
    "dictionary_contains": ({1: 0.94, 4: 0.94}, {1: 2_693_400_000, 4: 2_693_000_000}),
    "dictionary_groups": ({1: 0.94, 4: 0.93}, {1: 530_000_000, 4: 533_900_000}),
    "dictionary_groups_filtered": ({1: 1.13, 4: 1.13}, {1: 437_800_000, 4: 441_900_000}),
    "nullable_groups": ({1: 1.09, 4: 1.09}, {1: 427_000_000, 4: 432_900_000}),
    "nullable_min_max": ({1: 1.66, 4: 1.66}, {1: 412_500_000, 4: 415_100_000}),
    "narrow_integers": ({1: 2.35, 4: 2.31}, {1: 204_600_000, 4: 208_300_000}),
    "double_filter": ({1: 1.06}, {1: 289_500_000}),
    "double_range": ({1: 1.32, 4: 1.30}, {1: 207_500_000, 4: 212_000_000}),
    "dimension_join": ({1: 0.95, 4: 0.95}, {1: 387_100_000, 4: 390_800_000}),
    "dimension_string_groups": ({1: 0.94, 4: 0.93}, {1: 585_300_000, 4: 592_800_000}),
    "semi_join": ({1: 0.94, 4: 0.94}, {1: 398_200_000, 4: 400_200_000}),
    "anti_join": ({1: 1.69, 4: 1.67}, {1: 275_100_000, 4: 280_700_000}),
    "integer_in_list": ({1: 1.73, 4: 1.70}, {1: 236_100_000, 4: 242_300_000}),
    "dictionary_or_nullable": ({1: 1.55, 4: 1.54}, {1: 313_100_000, 4: 317_300_000}),
    "nullable_coalesce": ({1: 1.51, 4: 1.50}, {1: 311_300_000, 4: 315_700_000}),
    "modulo_groups": ({1: 1.30, 4: 1.29}, {1: 420_700_000, 4: 425_600_000}),
    "distinct_count": ({1: 1.37, 4: 1.36}, {1: 396_200_000, 4: 399_900_000}),
    "filtered_top_n": ({1: 0.94, 4: 0.94}, {1: 273_700_000, 4: 276_300_000}),
    "dictionary_prefix": ({1: 0.94, 4: 0.94}, {1: 2_652_400_000, 4: 2_654_000_000}),
    "integer_case": ({1: 1.88, 4: 1.84}, {1: 194_100_000, 4: 199_000_000}),
    "double_case": ({1: 1.49, 4: 1.48}, {1: 331_600_000, 4: 335_800_000}),
    "case_null_sums": ({1: 2.87, 4: 2.78}, {1: 217_900_000, 4: 225_600_000}),
    "case_null_groups": ({1: 1.62, 4: 1.60}, {1: 342_300_000, 4: 348_500_000}),
    "case_null_counts": ({1: 1.56, 4: 1.54}, {1: 316_700_000, 4: 321_400_000}),
    "case_null_count_sums": ({1: 3.02, 4: 2.91}, {1: 199_900_000, 4: 207_700_000}),
    "aggregate_filter_groups": ({1: 1.10, 4: 1.10}, {1: 458_600_000, 4: 462_400_000}),
    "aggregate_filter_sums": ({1: 1.06, 4: 1.05}, {1: 226_300_000, 4: 228_900_000}),
    "aggregate_filter_random_groups": ({1: 1.10, 4: 1.10}, {1: 458_500_000, 4: 461_600_000}),
    "aggregate_filter_random_sums": ({1: 0.90, 4: 0.89}, {1: 225_200_000, 4: 227_800_000}),
    "expression_key_groups": ({1: 1.25, 4: 1.23}, {1: 296_400_000, 4: 315_500_000}),
    "expression_key_filter_counts": ({1: 1.28, 4: 1.26}, {1: 429_900_000, 4: 451_800_000}),
    "narrow_between": ({1: 2.69, 4: 2.54}, {1: 55_000_000, 4: 59_000_000}),
    "nullable_key_join": ({1: 0.94, 4: 0.94}, {1: 2_840_000_000, 4: 2_839_400_000}),
    "random_key_groups": ({1: 1.23, 4: 1.19}, {1: 261_900_000, 4: 292_300_000}),
    "random_key_counts": ({1: 1.37, 4: 1.31}, {1: 208_700_000, 4: 234_100_000}),
    "random_multi_key_groups": ({1: 0.94, 4: 0.94}, {1: 1_763_900_000, 4: 3_304_700_000}),
}

ENCODINGS_WORKLOADS = tuple(
    {
        "name": name,
        "setup_sql": ENCODINGS_SETUP_SQL,
        "sql": sql,
        "floor": ENCODINGS_THRESHOLDS[name][0],
        "ceiling": ENCODINGS_THRESHOLDS[name][1],
        "compiled": compiled,
    }
    for name, (sql, compiled) in ENCODINGS_QUERIES.items()
)

# TPC-H at scale factor 10: query -> (floor, ceiling) on one thread
TPCH_THRESHOLDS = {
    "01": (1.67, 7_468_200_000),
    "02": (1.02, 451_700_000),
    "03": (1.19, 4_929_400_000),
    "04": (1.20, 3_630_400_000),
    "05": (1.07, 3_821_300_000),
    "06": (1.34, 2_439_000_000),
    "07": (1.04, 5_751_300_000),
    "08": (1.20, 3_265_000_000),
    "09": (0.79, 12_457_000_000),
    "10": (1.01, 6_552_600_000),
    "11": (1.00, 816_700_000),
    "12": (1.27, 4_699_700_000),
    "13": (1.33, 8_868_000_000),
    "14": (1.07, 2_823_500_000),
    "15": (1.35, 1_929_400_000),
    "16": (1.02, 1_747_800_000),
    "17": (1.15, 2_950_300_000),
    "18": (1.52, 6_678_200_000),
    "19": (1.68, 2_581_500_000),
    "20": (1.37, 3_986_700_000),
    "21": (1.03, 9_828_700_000),
    "22": (1.11, 2_366_500_000),
}
TPCH_QUERY_DIRECTORY = Path(__file__).resolve().parents[2] / "extension" / "tpch" / "dbgen" / "queries"

TPCH_WORKLOADS = tuple(
    {
        "name": f"q{query}",
        "setup_sql": "CALL dbgen(sf=10);",
        "sql": (TPCH_QUERY_DIRECTORY / f"q{query}.sql").read_text(encoding="utf-8").strip().rstrip(";"),
        "floor": {1: floor},
        "ceiling": {1: ceiling},
        "compiled": {1: True},
    }
    for query, (floor, ceiling) in TPCH_THRESHOLDS.items()
)
