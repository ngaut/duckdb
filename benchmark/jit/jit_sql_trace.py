#!/usr/bin/env python3
#
# Trace focused DuckDB JIT SQL coverage cases.
#
# TPC-H traces explain workload behavior. This script explains whether the
# focused SQL coverage used by the JIT tests still flows through the expected
# region, scalar-IR, fallback, event, counter, and runtime paths.

import argparse
import csv
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from trace_manifest import (
    TRACE_MANIFEST_NAME,
    default_trace_output_directory,
    prepare_trace_output_directory,
    write_trace_manifest,
)


TEST_CASE_RE = re.compile(r'TEST_CASE\("([^"]+)",\s*"([^"]*)"\)')
TEST_SURFACE_COVERAGE_FIELDS = (
    "source_path",
    "test_kind",
    "test_name",
    "verification_route",
    "trace_cases",
    "coverage_area",
)
FLOW_STEP_SUMMARY_FIELDS = (
    "case",
    "target",
    "phase",
    "status",
    "execution_mode",
    "region_execution_form",
    "policy_decision",
    "candidate_shape",
    "candidate_scope",
    "candidate_contract_abi",
    "event_count",
    "kernel_count",
    "reached_kernels",
    "row_processing_kernels",
    "unreached_kernels",
    "zero_input_kernels",
    "input_rows",
    "output_rows",
    "invocations",
    "runtime_time_us",
    "source_native_output_rows",
    "source_native_invocations",
    "source_native_runtime_time_us",
    "generated_body_runtime_time_us",
    "declined_invocations",
    "declined_runtime_time_us",
    "fallback_input_rows",
    "fallback_output_rows",
    "fallback_invocations",
    "fallback_runtime_time_us",
    "decision_time_us",
    "compile_time_us",
    "code_size",
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "admission_time_us",
    "overlap_check_time_us",
    "codegen_time_us",
    "example_reason",
)

CASES = (
    {
        "case": "region_native_filter_projection",
        "expect_region_compiled": True,
        "expect_region_unsupported": True,
        "expect_runtime": True,
        "settings": "",
        "sql": """
CREATE TABLE jit_native_region AS
SELECT x + 10 AS y
FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT), (NULL::BIGINT), (5::BIGINT)) AS t(x)
WHERE x > 2;
""",
        "validation": """
SELECT
    (SELECT count(*) FROM jit_native_region) = 2
    AND (SELECT sum(y) FROM jit_native_region) = 28 AS validation_pass
""",
    },
    {
        "case": "region_unsupported_join_fallback",
        "expect_region_compiled": False,
        "expect_region_unsupported": True,
        "expect_runtime": False,
        "settings": "",
        "sql": """
CREATE TABLE jit_left AS
SELECT *
FROM (VALUES (1), (2), (3)) AS t(i);
CREATE TABLE jit_right AS
SELECT *
FROM (VALUES (1, 'one'), (3, 'three')) AS t(i, name);

CREATE TABLE jit_join_result AS
SELECT l.i, r.name
FROM jit_left l
JOIN jit_right r USING (i);
""",
        "validation": """
SELECT
    (SELECT count(*) FROM jit_join_result) = 2
    AND (SELECT count(*) FROM jit_join_result WHERE i = 1 AND name = 'one') = 1
    AND (SELECT count(*) FROM jit_join_result WHERE i = 3 AND name = 'three') = 1 AS validation_pass
""",
    },
    {
        "case": "region_resume_state_fallback",
        "expect_region_compiled": False,
        "expect_region_unsupported": True,
        "expect_runtime": False,
        "settings": "",
        "sql": """
CREATE TABLE jit_resume_state_trace AS
SELECT count(*) AS c
FROM (
  SELECT i + 1 AS j, [1,2,3,4,5,6,7,8,9,10] AS xs
  FROM range(4096) tbl(i)
  WHERE i > 0
) t, UNNEST(xs) u(x);
""",
        "validation": """
SELECT (SELECT c FROM jit_resume_state_trace) = 40950 AS validation_pass
""",
    },
    {
        "case": "sql_equivalence_matrix",
        "expect_region_compiled": True,
        "expect_region_unsupported": True,
        "expect_runtime": True,
        "settings": "",
        "sql": """
CREATE TABLE jit_matrix_equiv AS
SELECT *
FROM (VALUES
    (1, 10, '1'),
    (2, NULL, '2'),
    (NULL, 30, NULL),
    (4, 40, 'bad')
) AS t(i, j, s);

CREATE TABLE jit_matrix_i64 AS
SELECT *
FROM (VALUES
    (1::BIGINT),
    (2::BIGINT),
    (NULL::BIGINT),
    (4::BIGINT)
) AS t(i);

CREATE TABLE jit_matrix_refs AS
SELECT *
FROM (VALUES
    (1, 1::BIGINT, 2::BIGINT),
    (2, 5::BIGINT, 3::BIGINT),
    (3, NULL::BIGINT, 4::BIGINT),
    (4, 9::BIGINT, NULL::BIGINT),
    (5, 7::BIGINT, 7::BIGINT),
    (6, -4::BIGINT, -2::BIGINT)
) AS t(id, a, b);

CREATE TABLE jit_matrix_types AS
SELECT *
FROM (VALUES
    (1::UBIGINT, 1.5::DOUBLE, '10', DATE '2024-01-01',
     12.34::DECIMAL(9,2), '00112233-4455-6677-8899-aabbccddeeff'::UUID,
     'abc'::BLOB, '0011'::BIT),
    (2::UBIGINT, 2.0::DOUBLE, '30', DATE '2024-03-01',
     -56.78::DECIMAL(9,2), '00112233-4455-6677-8899-aabbccddee00'::UUID,
     'abd'::BLOB, '0100'::BIT),
    (NULL::UBIGINT, NULL::DOUBLE, NULL, NULL::DATE,
     NULL::DECIMAL(9,2), NULL::UUID, NULL::BLOB, NULL::BIT)
) AS t(u, d, s, dt, dec_value, uuid_value, blob_value, bit_value);

CREATE TABLE jit_matrix_temporal AS
SELECT *
FROM (VALUES
    (TIME '01:00:00', TIMESTAMP '2024-01-01 10:00:00',
     TIMESTAMPTZ '2024-01-01 10:00:00+00', '01:00:00+00'::TIMETZ),
    (TIME '03:30:00', TIMESTAMP '2024-02-01 00:00:00',
     TIMESTAMPTZ '2024-02-01 00:00:00+00', '23:00:00+00'::TIMETZ),
    (NULL::TIME, NULL::TIMESTAMP, NULL::TIMESTAMPTZ, NULL::TIMETZ)
) AS t(tm, ts, tstz, tmtz);

CREATE TABLE jit_matrix_interval AS
SELECT *
FROM (VALUES
    (INTERVAL '1 day'),
    (INTERVAL '2 days'),
    (NULL::INTERVAL),
    (INTERVAL '4 days')
) AS t(iv);

CREATE TABLE jit_matrix_scalar AS
SELECT i,
       COALESCE(j, -1) AS j2,
       i + 3 AS plus3,
       (i < j)::INTEGER AS cmp,
       (i IS NULL)::INTEGER AS is_missing,
       (i IS NOT NULL AND j >= 10)::INTEGER AS bool_guard,
       CAST(i AS SMALLINT) AS small_i,
       CASE WHEN i IS NULL THEN 0 WHEN i > 2 THEN i * 10 ELSE i + 10 END AS case_value,
       (i BETWEEN 1 AND 3)::INTEGER AS between_value,
       (j NOT BETWEEN 20 AND 35)::INTEGER AS not_between_value,
       constant_or_null('ok', i, j) AS cor_value
FROM jit_matrix_equiv;

CREATE TABLE jit_matrix_i64_arith AS
SELECT i, i + 5 AS add5, i - 5 AS sub5, 10 - i AS rsub, i * 3 AS mul3, -i AS neg_i
FROM jit_matrix_i64;

CREATE TABLE jit_matrix_ref_ops AS
SELECT id, a < b AS lt, a = b AS eq, a >= b AS ge, a + b AS add_ab, a - b AS sub_ab, a * b AS mul_ab
FROM jit_matrix_refs;

CREATE TABLE jit_matrix_region_i64 AS
SELECT x + 10 AS y
FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT), (NULL::BIGINT), (5::BIGINT)) AS t(x)
WHERE x > 2;

CREATE TABLE jit_matrix_region_i64_sub AS
SELECT x - 5 AS y
FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT), (NULL::BIGINT), (5::BIGINT)) AS t(x)
WHERE x > 2;

CREATE TABLE jit_matrix_region_i64_mul AS
SELECT x * 3 AS y
FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT), (NULL::BIGINT), (5::BIGINT)) AS t(x)
WHERE x > 2;

CREATE TABLE jit_matrix_region_i64_cor AS
SELECT constant_or_null(42::BIGINT, x) AS y
FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT), (NULL::BIGINT), (5::BIGINT)) AS t(x)
WHERE x > 2;

CREATE TABLE jit_matrix_region_i32 AS
SELECT x + 10::INTEGER AS y
FROM (VALUES (1::INTEGER), (2::INTEGER), (3::INTEGER), (NULL::INTEGER), (5::INTEGER)) AS t(x)
WHERE x > 2::INTEGER;

CREATE TABLE jit_matrix_region_null AS
SELECT x IS NULL AS missing
FROM (VALUES (1::BIGINT, 'a'), (NULL::BIGINT, 'b'), (3::BIGINT, NULL), (NULL::BIGINT, NULL)) AS t(x, s)
WHERE s IS NOT NULL;

CREATE TABLE jit_matrix_region_cast AS
SELECT CAST(x AS SMALLINT) AS y
FROM (VALUES (1::INTEGER), (127::INTEGER), (-129::INTEGER), (NULL::INTEGER)) AS t(x)
WHERE x IS NOT NULL;

CREATE TABLE jit_matrix_region_coalesce AS
SELECT COALESCE(j, 0) AS y
FROM jit_matrix_equiv
WHERE i IS NOT NULL;

CREATE TABLE jit_matrix_region_ref AS
SELECT a + b AS s
FROM jit_matrix_refs
WHERE a < b;

CREATE TABLE jit_matrix_complex AS
SELECT u,
       CAST(u + 2::UBIGINT AS BIGINT) AS u_add,
       CAST(d * 2.0 AS INTEGER) AS d_twice,
       (s < '20')::INTEGER AS s_lt,
       (dt < DATE '2024-02-01')::INTEGER AS dt_lt,
       COALESCE(dec_value, 99.99::DECIMAL(9,2))::VARCHAR AS dec_coalesce,
       (uuid_value = '00112233-4455-6677-8899-aabbccddeeff'::UUID)::INTEGER AS uuid_eq,
       (blob_value = 'abc'::BLOB)::INTEGER AS blob_eq,
       (bit_value < '0100'::BIT)::INTEGER AS bit_lt
FROM jit_matrix_types;

CREATE TABLE jit_matrix_decimal AS
SELECT (dec_value + 0.66::DECIMAL(9,2))::VARCHAR AS dec_add,
       (dec_value - 1.11::DECIMAL(9,2))::VARCHAR AS dec_sub
FROM jit_matrix_types;

CREATE TABLE jit_matrix_temporal_result AS
SELECT (tm < TIME '02:00:00')::INTEGER AS tm_lt,
       (ts >= TIMESTAMP '2024-02-01 00:00:00')::INTEGER AS ts_ge,
       (tstz = TIMESTAMPTZ '2024-01-01 10:00:00+00')::INTEGER AS tstz_eq,
       (tmtz IS DISTINCT FROM '01:00:00+00'::TIMETZ)::INTEGER AS tmtz_distinct,
       (tmtz IS NOT DISTINCT FROM NULL::TIMETZ)::INTEGER AS tmtz_null
FROM jit_matrix_temporal;

CREATE TABLE jit_matrix_interval_result AS
SELECT (iv < INTERVAL '3 days')::INTEGER AS iv_lt,
       (iv IS DISTINCT FROM INTERVAL '2 days')::INTEGER AS iv_distinct,
       (iv IN (INTERVAL '1 day', NULL))::INTEGER AS iv_in
FROM jit_matrix_interval;
""",
        "validation": """
SELECT
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_scalar
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1, 10, 4, 1, 0, 1, 1::SMALLINT, 11, 1, 1, 'ok'),
            (2, -1, 5, NULL, 0, NULL, 2::SMALLINT, 12, 1, NULL, NULL),
            (NULL, 30, NULL, NULL, 1, 0, NULL::SMALLINT, 0, NULL, 0, NULL),
            (4, 40, 7, 1, 0, 1, 4::SMALLINT, 40, 0, 1, 'ok')
        ) AS expected(i, j2, plus3, cmp, is_missing, bool_guard, small_i,
                      case_value, between_value, not_between_value, cor_value)
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT *
        FROM (VALUES
            (1, 10, 4, 1, 0, 1, 1::SMALLINT, 11, 1, 1, 'ok'),
            (2, -1, 5, NULL, 0, NULL, 2::SMALLINT, 12, 1, NULL, NULL),
            (NULL, 30, NULL, NULL, 1, 0, NULL::SMALLINT, 0, NULL, 0, NULL),
            (4, 40, 7, 1, 0, 1, 4::SMALLINT, 40, 0, 1, 'ok')
        ) AS expected(i, j2, plus3, cmp, is_missing, bool_guard, small_i,
                      case_value, between_value, not_between_value, cor_value)
        EXCEPT ALL
        SELECT * FROM jit_matrix_scalar
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_i64_arith
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1::BIGINT, 6::BIGINT, -4::BIGINT, 9::BIGINT, 3::BIGINT, -1::BIGINT),
            (2::BIGINT, 7::BIGINT, -3::BIGINT, 8::BIGINT, 6::BIGINT, -2::BIGINT),
            (NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT),
            (4::BIGINT, 9::BIGINT, -1::BIGINT, 6::BIGINT, 12::BIGINT, -4::BIGINT)
        ) AS expected(i, add5, sub5, rsub, mul3, neg_i)
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_ref_ops
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1, true, false, false, 3::BIGINT, -1::BIGINT, 2::BIGINT),
            (2, false, false, true, 8::BIGINT, 2::BIGINT, 15::BIGINT),
            (3, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT),
            (4, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT),
            (5, false, true, true, 14::BIGINT, 0::BIGINT, 49::BIGINT),
            (6, true, false, false, -6::BIGINT, -2::BIGINT, 8::BIGINT)
        ) AS expected(id, lt, eq, ge, add_ab, sub_ab, mul_ab)
    )) = 0
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_i64) = '[13, 15]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_i64_sub) = '[-2, 0]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_i64_mul) = '[9, 15]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_i64_cor) = '[42, 42]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_i32) = '[13, 15]'
    AND (SELECT list(missing ORDER BY missing)::VARCHAR FROM jit_matrix_region_null) = '[false, true]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_cast) = '[-129, 1, 127]'
    AND (SELECT list(y ORDER BY y)::VARCHAR FROM jit_matrix_region_coalesce) = '[0, 10, 40]'
    AND (SELECT list(s ORDER BY s)::VARCHAR FROM jit_matrix_region_ref) = '[-6, 3]'
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_complex
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1::UBIGINT, 3::BIGINT, 3, 1, 1, '12.34', 1, 1, 1),
            (2::UBIGINT, 4::BIGINT, 4, 0, 0, '-56.78', 0, 0, 0),
            (NULL::UBIGINT, NULL::BIGINT, NULL, NULL, NULL, '99.99', NULL, NULL, NULL)
        ) AS expected(u, u_add, d_twice, s_lt, dt_lt, dec_coalesce, uuid_eq, blob_eq, bit_lt)
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_decimal
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            ('13.00', '11.23'),
            ('-56.12', '-57.89'),
            (NULL, NULL)
        ) AS expected(dec_add, dec_sub)
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_temporal_result
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1, 0, 1, 0, 0),
            (0, 1, 0, 1, 0),
            (NULL, NULL, NULL, 1, 1)
        ) AS expected(tm_lt, ts_ge, tstz_eq, tmtz_distinct, tmtz_null)
    )) = 0
    AND
    (SELECT count(*) FROM (
        SELECT * FROM jit_matrix_interval_result
        EXCEPT ALL
        SELECT *
        FROM (VALUES
            (1, 1, 1),
            (1, 0, NULL),
            (0, 1, NULL),
            (NULL, 1, NULL)
        ) AS expected(iv_lt, iv_distinct, iv_in)
    )) = 0 AS validation_pass
""",
    },
)


SQL_TEST_COVERAGE = {
    "test/sql/jit/test_jit_framework.test": {
        "trace_cases": (
            "region_native_filter_projection",
            "region_unsupported_join_fallback",
            "region_resume_state_fallback",
            "sql_equivalence_matrix",
        ),
        "coverage_area": "settings, backend registration, scalar IR, events, counters, runtime trace, and fallback diagnostics",
    },
}


def classify_api_test_case(test_name: str) -> tuple[str, tuple, str]:
    lower_name = test_name.lower()
    trace_cases = set()
    areas = []

    if "sljit native" in lower_name or "between" in lower_name or "dump ir" in lower_name:
        trace_cases.add("sql_equivalence_matrix")
        areas.append("native SLJIT scalar IR and region semantics")
    if "runtime trace" in lower_name or "kernel counter" in lower_name or "compiled coverage" in lower_name:
        trace_cases.add("region_native_filter_projection")
        areas.append("runtime/kernel trace linkage")
    if "auto policy" in lower_name or "auto-rejected" in lower_name:
        trace_cases.add("region_native_filter_projection")
        areas.append("policy admission and skip evidence")
    if "fallback" in lower_name or "declining" in lower_name or "unsupported" in lower_name:
        trace_cases.add("region_unsupported_join_fallback")
        areas.append("fallback/decline honesty")
    if trace_cases:
        route = "api_unit_test_suite;focused_sql_trace"
    else:
        route = "api_unit_test_suite"
        areas.append("core API/ABI contract")
    return route, tuple(sorted(trace_cases)), "; ".join(dict.fromkeys(areas))


def collect_test_surface_coverage(root: Path) -> list:
    rows = []
    for path in sorted((root / "test" / "sql" / "jit").glob("*.test")):
        rel_path = path.relative_to(root).as_posix()
        if rel_path not in SQL_TEST_COVERAGE:
            raise RuntimeError(f"missing focused trace coverage mapping for SQL JIT test: {rel_path}")
        metadata = SQL_TEST_COVERAGE[rel_path]
        rows.append(
            {
                "source_path": rel_path,
                "test_kind": "sqllogictest",
                "test_name": rel_path,
                "verification_route": "sqllogictest_suite;focused_sql_trace",
                "trace_cases": ";".join(metadata["trace_cases"]),
                "coverage_area": metadata["coverage_area"],
            }
        )

    api_test_path = root / "test" / "api" / "test_jit.cpp"
    for match in TEST_CASE_RE.finditer(api_test_path.read_text(encoding="utf-8")):
        test_name = match.group(1)
        route, trace_cases, coverage_area = classify_api_test_case(test_name)
        rows.append(
            {
                "source_path": "test/api/test_jit.cpp",
                "test_kind": "catch2",
                "test_name": test_name,
                "verification_route": route,
                "trace_cases": ";".join(trace_cases),
                "coverage_area": coverage_area,
            }
        )
    return rows


def write_csv_rows(path: Path, fieldnames: tuple, rows: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    if value == "" or value is None:
        return 0
    return int(value)


def truncate_text(value: str, limit: int = 120) -> str:
    value = str(value)
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def flow_key(case_name: str, row: dict, phase: str, status: str, policy_decision: str) -> tuple:
    return (
        case_name,
        row.get("target", "") or "none",
        phase or "none",
        status or "none",
        row.get("execution_mode", "") or "none",
        row.get("region_execution_form", "") or "none",
        policy_decision or "none",
        row.get("candidate_shape", "") or "none",
        row.get("candidate_scope", "") or "none",
        row.get("candidate_contract_abi", "") or "none",
    )


def new_flow_entry(key: tuple) -> dict:
    return {
        "case": key[0],
        "target": key[1],
        "phase": key[2],
        "status": key[3],
        "execution_mode": key[4],
        "region_execution_form": key[5],
        "policy_decision": key[6],
        "candidate_shape": key[7],
        "candidate_scope": key[8],
        "candidate_contract_abi": key[9],
        "event_count": 0,
        "kernel_count": 0,
        "reached_kernels": 0,
        "row_processing_kernels": 0,
        "unreached_kernels": 0,
        "zero_input_kernels": 0,
        "input_rows": 0,
        "output_rows": 0,
        "invocations": 0,
        "runtime_time_us": 0,
        "source_native_output_rows": 0,
        "source_native_invocations": 0,
        "source_native_runtime_time_us": 0,
        "generated_body_runtime_time_us": 0,
        "declined_invocations": 0,
        "declined_runtime_time_us": 0,
        "fallback_input_rows": 0,
        "fallback_output_rows": 0,
        "fallback_invocations": 0,
        "fallback_runtime_time_us": 0,
        "decision_time_us": 0,
        "compile_time_us": 0,
        "code_size": 0,
        "ir_lowering_time_us": 0,
        "backend_analysis_time_us": 0,
        "admission_time_us": 0,
        "overlap_check_time_us": 0,
        "codegen_time_us": 0,
        "example_reason": "",
    }


def collect_flow_step_summary(out_dir: Path, rows: list) -> list:
    summary = {}
    for summary_row in rows:
        case_name = summary_row["case"]
        with (out_dir / summary_row["events_csv"]).open(newline="", encoding="utf-8") as handle:
            for event in csv.DictReader(handle):
                key = flow_key(
                    case_name,
                    event,
                    event.get("phase", "") or "none",
                    event.get("status", "") or "none",
                    event.get("policy_decision", "") or "none",
                )
                entry = summary.setdefault(key, new_flow_entry(key))
                entry["event_count"] += 1
                entry["input_rows"] += row_int(event, "input_rows")
                entry["output_rows"] += row_int(event, "output_rows")
                entry["invocations"] += row_int(event, "invocation_count")
                entry["runtime_time_us"] += row_int(event, "runtime_time_us")
                entry["source_native_output_rows"] += row_int(event, "source_native_output_rows")
                entry["source_native_invocations"] += row_int(event, "source_native_invocation_count")
                entry["source_native_runtime_time_us"] += row_int(event, "source_native_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(event, "generated_body_runtime_time_us")
                entry["decision_time_us"] += row_int(event, "decision_time_us")
                entry["compile_time_us"] += row_int(event, "compile_time_us")
                entry["code_size"] += row_int(event, "code_size")
                entry["ir_lowering_time_us"] += row_int(event, "ir_lowering_time_us")
                entry["backend_analysis_time_us"] += row_int(event, "backend_analysis_time_us")
                entry["admission_time_us"] += row_int(event, "admission_time_us")
                entry["overlap_check_time_us"] += row_int(event, "overlap_check_time_us")
                entry["codegen_time_us"] += row_int(event, "codegen_time_us")
                if not entry["example_reason"]:
                    entry["example_reason"] = truncate_text(event.get("reason", ""))

        with (out_dir / summary_row["kernel_counters_csv"]).open(newline="", encoding="utf-8") as handle:
            for counter in csv.DictReader(handle):
                key = flow_key(
                    case_name,
                    counter,
                    "kernel_counter",
                    counter.get("last_runtime_status", "") or "compiled",
                    "kernel_counter",
                )
                entry = summary.setdefault(key, new_flow_entry(key))
                input_rows = row_int(counter, "input_rows")
                invocation_count = row_int(counter, "invocation_count")
                entry["kernel_count"] += 1
                if invocation_count > 0:
                    entry["reached_kernels"] += 1
                else:
                    entry["unreached_kernels"] += 1
                if input_rows > 0:
                    entry["row_processing_kernels"] += 1
                else:
                    entry["zero_input_kernels"] += 1
                entry["input_rows"] += input_rows
                entry["output_rows"] += row_int(counter, "output_rows")
                entry["invocations"] += invocation_count
                entry["runtime_time_us"] += row_int(counter, "runtime_time_us")
                entry["source_native_output_rows"] += row_int(counter, "source_native_output_rows")
                entry["source_native_invocations"] += row_int(counter, "source_native_invocation_count")
                entry["source_native_runtime_time_us"] += row_int(counter, "source_native_runtime_time_us")
                entry["generated_body_runtime_time_us"] += row_int(counter, "generated_body_runtime_time_us")
                entry["declined_invocations"] += row_int(counter, "declined_invocation_count")
                entry["declined_runtime_time_us"] += row_int(counter, "declined_runtime_time_us")
                entry["fallback_input_rows"] += row_int(counter, "fallback_input_rows")
                entry["fallback_output_rows"] += row_int(counter, "fallback_output_rows")
                entry["fallback_invocations"] += row_int(counter, "fallback_invocation_count")
                entry["fallback_runtime_time_us"] += row_int(counter, "fallback_runtime_time_us")
                entry["compile_time_us"] += row_int(counter, "compile_time_us")
                entry["code_size"] += row_int(counter, "code_size")
                if not entry["example_reason"]:
                    entry["example_reason"] = truncate_text(counter.get("compile_reason", ""))

    result = list(summary.values())
    result.sort(
        key=lambda entry: (
            entry["case"],
            entry["target"],
            entry["phase"],
            entry["status"],
            entry["execution_mode"],
            entry["region_execution_form"],
            entry["candidate_shape"],
            entry["candidate_scope"],
            entry["candidate_contract_abi"],
        )
    )
    return result


EVENT_SUMMARY_SELECT = """
SELECT
    count(*) AS event_count,
    count(*) FILTER (
        WHERE target <> 'region'
    ) AS non_region_events,
    count(*) FILTER (
        WHERE target='region' AND status='compiled'
    ) AS compiled_regions,
    count(*) FILTER (
        WHERE target='region' AND status='skipped'
    ) AS skipped_regions,
    count(*) FILTER (
        WHERE target='region' AND status='unsupported'
    ) AS unsupported_regions,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native'
    ) AS native_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='unsupported'
    ) AS unsupported_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='executor_fallback'
    ) AS executor_fallback_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native' AND code_size=0
    ) AS zero_code_native_compile_events,
    count(*) FILTER (
        WHERE phase='runtime'
    ) AS runtime_events,
    coalesce(sum(input_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_input_rows,
    coalesce(sum(output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_output_rows,
    coalesce(sum(invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_invocations,
    coalesce(sum(runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_time_us,
    coalesce(sum(source_native_output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_output_rows,
    coalesce(sum(source_native_invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_invocations,
    coalesce(sum(source_native_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS source_native_runtime_time_us,
    coalesce(sum(generated_body_runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS generated_body_runtime_time_us,
    coalesce(sum(ir_lowering_time_us), 0) AS ir_lowering_time_us,
    coalesce(sum(backend_analysis_time_us), 0) AS backend_analysis_time_us,
    coalesce(sum(admission_time_us), 0) AS admission_time_us,
    coalesce(sum(overlap_check_time_us), 0) AS overlap_check_time_us,
    coalesce(sum(codegen_time_us), 0) AS codegen_time_us
FROM duckdb_jit_events()
"""


EVENTS_SELECT = """
SELECT
    event_id,
    phase,
    backend_name,
    target,
    status,
    execution_mode,
    region_execution_form,
    selected_source_execution,
    policy_decision,
    candidate_id,
    candidate_shape,
    candidate_pipeline_shape,
    candidate_context_pipeline_shape,
    candidate_scope,
    candidate_contract_abi,
    candidate_contract_first_node,
    candidate_contract_node_count,
    candidate_contract_start_operator_index,
    candidate_contract_end_operator_index,
    candidate_owns_source,
    candidate_owns_transform,
    candidate_owns_sink,
    candidate_owns_state_scan,
    candidate_node_count,
    candidate_start_operator_index,
    candidate_end_operator_index,
    candidate_estimated_cardinality,
    candidate_has_source,
    candidate_has_sink,
    candidate_source_kind,
    candidate_source_execution,
    candidate_sink_kind,
    candidate_has_table_scan_source,
    candidate_has_stateful_source,
    candidate_expression_traits_known,
    candidate_source_filter_count,
    candidate_source_filter_expression_count,
    candidate_source_filter_fallback_count,
    candidate_source_comparison_filter_count,
    candidate_source_integer_comparison_filter_count,
    candidate_source_non_integer_comparison_filter_count,
    candidate_source_conjunction_filter_count,
    candidate_source_projected_column_count,
    candidate_source_returned_column_count,
    candidate_filter_count,
    candidate_projection_count,
    candidate_operator_count,
    candidate_core_expression_operator_count,
    candidate_arithmetic_projection_count,
    candidate_integer_arithmetic_projection_count,
    candidate_non_integer_arithmetic_projection_count,
    candidate_reference_projection_count,
    candidate_comparison_filter_count,
    candidate_integer_comparison_filter_count,
    candidate_non_integer_comparison_filter_count,
    candidate_conjunction_filter_count,
    candidate_expression_fallback_count,
    candidate_operator_fallback_count,
    candidate_scan_boundary_count,
    candidate_sink_boundary_count,
    candidate_source_ownership,
    candidate_state_scan_ownership,
    candidate_transform_ownership,
    candidate_sink_ownership,
    candidate_executor_boundary_free,
    candidate_native_fusion_ready,
    candidate_generated_operator_count,
    candidate_typed_helper_boundary_count,
    candidate_executor_boundary_count,
    candidate_missing_protocol_count,
    candidate_required_capabilities,
    candidate_fusion_blockers,
    admission_shape_key,
    admission_rule_present,
    admission_min_cardinality,
    admission_score,
    admission_proof,
    reason,
    decision_time_us,
    compile_time_us,
    code_size,
    kernel_id,
    input_rows,
    output_rows,
    invocation_count,
    runtime_time_us,
    runtime_result,
    source_native_output_rows,
    source_native_invocation_count,
    source_native_runtime_time_us,
    generated_body_runtime_time_us,
    generated_body_flat_input_rows,
    generated_body_flat_invocation_count,
    generated_body_shared_selection_input_rows,
    generated_body_shared_selection_invocation_count,
    generated_body_selection_input_rows,
    generated_body_selection_invocation_count,
    generated_body_generic_input_rows,
    generated_body_generic_invocation_count,
    ir_lowering_time_us,
    backend_analysis_time_us,
    admission_time_us,
    overlap_check_time_us,
    codegen_time_us,
    ir
FROM duckdb_jit_events()
ORDER BY event_id
"""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def run_duckdb(duckdb: Path, db_path: Path, sql: str, label: str) -> subprocess.CompletedProcess:
    script = f".bail on\n{sql}\n"
    result = subprocess.run(
        [str(duckdb), str(db_path)],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"DuckDB command failed during {label} with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def copy_statement(select_sql: str, output_path: Path) -> str:
    return f"COPY ({select_sql}) TO {sql_quote(output_path)} (HEADER, DELIMITER ',');"


def read_single_csv_row(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise RuntimeError(f"expected one row in {path}, found {len(rows)}")
    return rows[0]


def setting_sql(args: argparse.Namespace) -> str:
    return f"""
LOAD {args.jit_extension};
SET threads={args.threads};
SET enable_jit=true;
SET jit_backend={sql_quote(args.backend)};
SET jit_policy='force';
SET jit_verify=true;
SET jit_dump_ir=true;
SET jit_trace_runtime=true;
SET jit_trace_decisions=true;
SET jit_event_log_size={args.event_log_size};
"""


def run_case(args: argparse.Namespace, db_path: Path, out_dir: Path, case: dict) -> dict:
    case_name = case["case"]
    event_summary_path = out_dir / f"{case_name}_event_summary.csv"
    validation_path = out_dir / f"{case_name}_validation.csv"
    events_path = out_dir / f"{case_name}_events.csv"
    counters_path = out_dir / f"{case_name}_counters.csv"
    kernel_counters_path = out_dir / f"{case_name}_kernel_counters.csv"
    run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args)}
{case.get("settings", "")}
SELECT * FROM duckdb_jit_clear_events();
{case["sql"]}
{copy_statement(EVENT_SUMMARY_SELECT, event_summary_path)}
{copy_statement(EVENTS_SELECT, events_path)}
{copy_statement("SELECT * FROM duckdb_jit_counters() ORDER BY backend_name, target, status, execution_mode, region_execution_form, policy_decision", counters_path)}
{copy_statement("SELECT * FROM duckdb_jit_kernel_counters() ORDER BY kernel_id", kernel_counters_path)}
SET enable_jit=false;
{copy_statement(case["validation"], validation_path)}
""",
        f"JIT SQL trace case {case_name}",
    )
    event_summary = read_single_csv_row(event_summary_path)
    validation = read_single_csv_row(validation_path)
    return {
        "case": case_name,
        "validation_pass": validation.get("validation_pass", "false"),
        "expect_region_compiled": int(case["expect_region_compiled"]),
        "expect_region_unsupported": int(case["expect_region_unsupported"]),
        "expect_runtime": int(case["expect_runtime"]),
        **event_summary,
        "event_summary_csv": event_summary_path.name,
        "validation_csv": validation_path.name,
        "events_csv": events_path.name,
        "counters_csv": counters_path.name,
        "kernel_counters_csv": kernel_counters_path.name,
    }


def write_summary(out_dir: Path, rows: list) -> None:
    write_csv_rows(out_dir / "summary.csv", tuple(rows[0].keys()), rows)


def write_test_surface_coverage(root: Path, out_dir: Path) -> list:
    rows = collect_test_surface_coverage(root)
    write_csv_rows(out_dir / "test_surface_coverage.csv", TEST_SURFACE_COVERAGE_FIELDS, rows)
    return rows


def write_flow_step_summary(out_dir: Path, rows: list) -> list:
    flow_rows = collect_flow_step_summary(out_dir, rows)
    write_csv_rows(out_dir / "flow_step_summary.csv", FLOW_STEP_SUMMARY_FIELDS, flow_rows)
    return flow_rows


def write_manifest(
    args: argparse.Namespace,
    out_dir: Path,
    rows: list,
    test_surface_rows: list,
    flow_step_rows: list,
    db_path: Path,
    temp_dir: Optional[Path],
) -> None:
    artifact_names = ["summary.csv", "test_surface_coverage.csv", "flow_step_summary.csv"]
    for row in rows:
        artifact_names.extend(
            [
                row["event_summary_csv"],
                row["validation_csv"],
                row["events_csv"],
                row["counters_csv"],
                row["kernel_counters_csv"],
            ]
        )
    write_trace_manifest(
        out_dir,
        kind="focused_sql_jit_trace",
        generator="benchmark/jit/jit_sql_trace.py",
        configuration={
            "duckdb": str(args.duckdb),
            "backend": args.backend,
            "jit_extension": args.jit_extension,
            "threads": args.threads,
            "event_log_size": args.event_log_size,
            "cases": [case["case"] for case in CASES],
            "test_surface_rows": len(test_surface_rows),
            "flow_step_rows": len(flow_step_rows),
            "db_path": str(db_path),
            "db_mode": "explicit_new" if args.db is not None else "temporary",
            "keep_db": args.keep_db,
            "temporary_database_directory": str(temp_dir) if temp_dir is not None else "",
        },
        artifact_names=artifact_names,
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Trace focused DuckDB JIT SQL coverage cases")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "release" / "duckdb")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--event-log-size", type=int, default=100000)
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--db", type=Path, default=None)
    return parser.parse_args()


def prepare_database(args: argparse.Namespace) -> tuple[Path, Optional[Path]]:
    if args.db is not None:
        db_path = args.db.resolve()
        if db_path.exists():
            raise RuntimeError(f"--db already exists: {db_path}; choose a new path")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path, None
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_jit_sql_trace_"))
    return temp_dir / "jit_sql_trace.duckdb", temp_dir


def main() -> int:
    args = parse_args()
    root = repo_root()
    args.duckdb = args.duckdb.resolve()
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("sql_trace")
    out_dir = prepare_trace_output_directory(args.out_dir)

    db_path, temp_dir = prepare_database(args)
    try:
        rows = [run_case(args, db_path, out_dir, case) for case in CASES]
        write_summary(out_dir, rows)
        flow_step_rows = write_flow_step_summary(out_dir, rows)
        test_surface_rows = write_test_surface_coverage(root, out_dir)
        write_manifest(args, out_dir, rows, test_surface_rows, flow_step_rows, db_path, temp_dir)
        print(f"trace output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        print(f"flow steps: {out_dir / 'flow_step_summary.csv'}")
        print(f"manifest: {out_dir / TRACE_MANIFEST_NAME}")
    finally:
        if temp_dir is not None and not args.keep_db:
            shutil.rmtree(temp_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
