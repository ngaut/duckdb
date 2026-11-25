#!/usr/bin/env python3
#
# The workloads of the JIT performance gate and their checked-in thresholds.
#
# A workload is one query. Per thread count it is qualified for, `floor` is the least speedup of the JIT over the
# native engine, measured as the native engine's cycles over the JIT's, `ceiling` the most instructions the JIT may
# retire and `compiled` whether the JIT compiles a region for it. A workload with `setup_sql` reads the tables
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
        "floor": {1: 3.58, 4: 3.58},
        "ceiling": {1: 1_695_500_000, 4: 1_695_000_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_filter",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM range(50000000) tbl(i) WHERE i % 7 = 3",
        "floor": {1: 2.68, 4: 2.66},
        "ceiling": {1: 1_088_600_000, 4: 1_089_600_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_case",
        "sql": (
            "SELECT sum(CASE WHEN i % 11 < 3 THEN i * 31 + i % 97 "
            "ELSE i * 7 - i % 29 END) AS value FROM range(50000000) tbl(i)"
        ),
        "floor": {1: 4.03, 4: 4.04},
        "ceiling": {1: 2_307_000_000, 4: 2_307_900_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "range_multi_aggregate",
        "sql": (
            "SELECT sum(i * 31 + i % 97) AS total, "
            "sum(CASE WHEN i % 7 = 3 THEN i ELSE 0 END) AS selected, "
            "count(*) AS row_count FROM range(50000000) tbl(i)"
        ),
        "floor": {1: 3.98, 4: 3.97},
        "ceiling": {1: 2_558_400_000, 4: 2_557_800_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "join_range",
        "sql": (
            "SELECT sum(a.i * b.i) AS value FROM range(2000000) a(i) "
            "JOIN range(2000000) b(i) ON a.i = b.i WHERE a.i % 7 = 3"
        ),
        "floor": {1: 1.10, 4: 1.11},
        "ceiling": {1: 170_500_000, 4: 175_500_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "scan_expr",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input",
        "floor": {1: 1.27, 4: 1.32},
        "ceiling": {1: 518_500_000, 4: 520_300_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "grouped_scan",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(20000000) tbl(i);",
        "sql": "SELECT i % 1000 AS key, sum(i * 31) AS value FROM __jit_generic_input GROUP BY key ORDER BY key",
        "floor": {1: 1.47, 4: 1.47},
        "ceiling": {1: 926_200_000, 4: 928_700_000},
        "compiled": {1: True, 4: True},
    },
    {
        "name": "scan_filter",
        "setup_sql": "CREATE OR REPLACE TABLE __jit_generic_input AS SELECT i FROM range(40000000) tbl(i);",
        "sql": "SELECT sum(i * 31 + (i % 97)) AS value FROM __jit_generic_input WHERE i % 7 = 3",
        # The source contract owns this exception-free modulo predicate, so the
        # filter runs fused with the aggregate instead of as a vectorized filter.
        "floor": {1: 2.02, 4: 2.02},
        "ceiling": {1: 772_700_000, 4: 775_300_000},
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
        "floor": {1: 1.29, 4: 1.38},
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
        "floor": {1: 1.55, 4: 1.52},
        "ceiling": {1: 1_877_100_000, 4: 1_878_800_000},
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
        "floor": {1: 0.96, 4: 0.96},
        "ceiling": {1: 665_200_000, 4: 666_300_000},
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
        "floor": {1: 1.37, 4: 1.38},
        "ceiling": {1: 425_700_000, 4: 428_300_000},
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
        "floor": {1: 2.99, 4: 2.96},
        "ceiling": {1: 1_003_400_000, 4: 1_012_700_000},
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
        "floor": {1: 2.17, 4: 2.12},
        "ceiling": {1: 1_404_300_000, 4: 1_412_500_000},
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
        "floor": {1: 1.80, 4: 1.72},
        "ceiling": {1: 1_783_100_000, 4: 1_787_100_000},
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
        "floor": {1: 1.72, 4: 1.69},
        "ceiling": {1: 866_900_000, 4: 875_800_000},
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
        "floor": {1: 0.96, 4: 0.96},
        "ceiling": {1: 865_500_000, 4: 868_200_000},
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
        "floor": {1: 1.84, 4: 1.78},
        "ceiling": {1: 737_900_000, 4: 745_500_000},
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
        "floor": {1: 1.73, 4: 1.69},
        "ceiling": {1: 580_800_000, 4: 588_300_000},
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
        "floor": {1: 1.73, 4: 1.70},
        "ceiling": {1: 531_800_000, 4: 538_900_000},
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
        "floor": {1: 3.36, 4: 3.85},
        "ceiling": {1: 443_000_000, 4: 453_200_000},
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
        "floor": {1: 2.87, 4: 3.26},
        "ceiling": {1: 541_300_000, 4: 554_000_000},
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
        "floor": {1: 3.41, 4: 3.80},
        "ceiling": {1: 444_300_000, 4: 454_100_000},
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
        "floor": {1: 2.34, 4: 2.81},
        "ceiling": {1: 699_000_000, 4: 708_500_000},
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
        "floor": {1: 6.96, 4: 6.65},
        "ceiling": {1: 890_300_000, 4: 898_500_000},
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
        "floor": {1: 1.42, 4: 1.93},
        "ceiling": {1: 260_200_000, 4: 388_000_000},
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
        "floor": {1: 1.35, 4: 1.31},
        "ceiling": {1: 92_500_000, 4: 98_800_000},
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
        "floor": {1: 1.48, 4: 1.43},
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
        "floor": {1: 1.73, 4: 1.80},
        "ceiling": {1: 715_500_000, 4: 735_900_000},
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
    "decimal_double_product": ({1: 2.30}, {1: 319_500_000}),
    "decimal_grouped": ({1: 1.17}, {1: 845_300_000}),
    "date_range": ({1: 1.55, 4: 1.48}, {1: 113_300_000, 4: 115_700_000}),
    "date_year_groups": ({1: 1.53, 4: 1.50}, {1: 447_400_000, 4: 452_600_000}),
    "dictionary_case": ({1: 0.99, 4: 0.99}, {1: 248_700_000, 4: 251_600_000}),
    "dictionary_contains": ({1: 0.90, 4: 0.89}, {1: 2_689_000_000, 4: 2_690_800_000}),
    "dictionary_groups": ({1: 0.89, 4: 0.87}, {1: 529_300_000, 4: 532_900_000}),
    "dictionary_groups_filtered": ({1: 1.12, 4: 1.10}, {1: 437_100_000, 4: 441_500_000}),
    "nullable_groups": ({1: 1.15, 4: 1.16}, {1: 426_200_000, 4: 430_900_000}),
    "nullable_min_max": ({1: 1.33, 4: 1.32}, {1: 412_300_000, 4: 414_000_000}),
    "narrow_integers": ({1: 1.97, 4: 1.90}, {1: 204_000_000, 4: 207_700_000}),
    "double_filter": ({1: 1.13}, {1: 286_500_000}),
    "double_range": ({1: 1.04, 4: 1.02}, {1: 207_100_000, 4: 211_400_000}),
    "dimension_join": ({1: 0.89, 4: 0.89}, {1: 383_800_000, 4: 387_700_000}),
    "dimension_string_groups": ({1: 0.86, 4: 0.87}, {1: 585_200_000, 4: 591_500_000}),
    "semi_join": ({1: 0.89, 4: 0.88}, {1: 397_700_000, 4: 399_700_000}),
    "anti_join": ({1: 1.38, 4: 1.37}, {1: 274_300_000, 4: 280_200_000}),
    "integer_in_list": ({1: 1.29, 4: 1.27}, {1: 236_100_000, 4: 241_600_000}),
    "dictionary_or_nullable": ({1: 0.95, 4: 0.99}, {1: 312_000_000, 4: 316_900_000}),
    "nullable_coalesce": ({1: 1.50, 4: 1.50}, {1: 310_200_000, 4: 314_600_000}),
    "modulo_groups": ({1: 1.16, 4: 1.14}, {1: 420_600_000, 4: 425_300_000}),
    "distinct_count": ({1: 1.28, 4: 1.23}, {1: 395_400_000, 4: 398_800_000}),
    "filtered_top_n": ({1: 0.84, 4: 0.89}, {1: 273_600_000, 4: 276_200_000}),
    "dictionary_prefix": ({1: 0.88, 4: 0.89}, {1: 2_650_000_000, 4: 2_651_900_000}),
    "integer_case": ({1: 1.50, 4: 1.49}, {1: 194_100_000, 4: 198_400_000}),
    "double_case": ({1: 1.55, 4: 1.51}, {1: 328_700_000, 4: 333_000_000}),
    "case_null_sums": ({1: 1.96, 4: 1.90}, {1: 217_200_000, 4: 225_100_000}),
    "case_null_groups": ({1: 1.71, 4: 1.67}, {1: 342_200_000, 4: 347_700_000}),
    "case_null_counts": ({1: 1.93, 4: 1.87}, {1: 315_500_000, 4: 320_400_000}),
    "case_null_count_sums": ({1: 2.19, 4: 2.10}, {1: 199_500_000, 4: 207_100_000}),
    "aggregate_filter_groups": ({1: 1.30, 4: 1.30}, {1: 458_000_000, 4: 461_500_000}),
    "aggregate_filter_sums": ({1: 1.06, 4: 1.08}, {1: 225_800_000, 4: 228_400_000}),
    "aggregate_filter_random_groups": ({1: 1.32, 4: 1.33}, {1: 457_400_000, 4: 460_900_000}),
    "aggregate_filter_random_sums": ({1: 0.93, 4: 0.93}, {1: 225_100_000, 4: 227_100_000}),
    "expression_key_groups": ({1: 1.56, 4: 1.51}, {1: 295_900_000, 4: 314_300_000}),
    "expression_key_filter_counts": ({1: 1.69, 4: 1.63}, {1: 429_900_000, 4: 450_000_000}),
    "narrow_between": ({1: 1.32, 4: 1.25}, {1: 55_000_000, 4: 58_000_000}),
    "nullable_key_join": ({1: 0.89, 4: 0.89}, {1: 2_837_300_000, 4: 2_834_200_000}),
    "random_key_groups": ({1: 1.37, 4: 1.32}, {1: 261_000_000, 4: 290_300_000}),
    "random_key_counts": ({1: 1.57, 4: 1.49}, {1: 207_600_000, 4: 232_200_000}),
    "random_multi_key_groups": ({1: 0.89, 4: 0.89}, {1: 1_755_200_000, 4: 3_295_400_000}),
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
    "01": (1.89, 7_468_200_000),
    "02": (0.98, 448_800_000),
    "03": (1.47, 4_738_100_000),
    "04": (1.23, 3_630_400_000),
    "05": (1.17, 3_500_100_000),
    "06": (2.49, 2_413_600_000),
    "07": (1.35, 4_759_400_000),
    "08": (1.45, 3_249_900_000),
    "09": (1.44, 12_033_500_000),
    "10": (0.98, 5_588_900_000),
    "11": (0.98, 738_300_000),
    "12": (1.15, 4_680_300_000),
    "13": (1.88, 8_852_500_000),
    "14": (1.27, 2_817_800_000),
    "15": (1.91, 1_929_400_000),
    "16": (0.98, 1_747_800_000),
    "17": (1.11, 2_948_400_000),
    "18": (2.46, 6_655_500_000),
    "19": (2.81, 2_581_500_000),
    "20": (1.28, 3_986_700_000),
    "21": (0.98, 9_363_200_000),
    "22": (1.07, 2_365_800_000),
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
