#!/usr/bin/env python3
#
# Verify TPC-H JIT benchmark artifacts produced by tpch_benchmark.py.

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import verify_trace_manifest

from tpch_schema import (
    BENCHMARK_ADMISSION_EVIDENCE_FIELDS,
    BENCHMARK_POLICY_SUMMARY_FIELDS,
    BENCHMARK_RUN_FIELDS,
    BENCHMARK_SUMMARY_FIELDS,
    CANDIDATE_SIGNATURE_FIELDS,
    CANDIDATE_TRAIT_FIELDS,
    KNOWN_ADMISSION_EVIDENCE_ROOT_CAUSES,
    KNOWN_ADMISSION_EVIDENCE_STATUSES,
    KNOWN_REGION_EXECUTION_FORMS,
    configure_csv_field_size_limit,
)

configure_csv_field_size_limit()

KNOWN_EXECUTION_BODIES = {"none", "generated-machine-code", "native-operator-protocol"}

REQUIRED_ARTIFACTS = (
    "runs.csv",
    "summary.csv",
    "query_summary.csv",
    "policy_summary.csv",
    "correctness_summary.csv",
    "operator_profile_summary.csv",
    "decision_counter_summary.csv",
    "admission_evidence_summary.csv",
    "report.md",
)

RUN_REQUIRED_COLUMNS = BENCHMARK_RUN_FIELDS
SUMMARY_REQUIRED_COLUMNS = BENCHMARK_SUMMARY_FIELDS
POLICY_REQUIRED_COLUMNS = BENCHMARK_POLICY_SUMMARY_FIELDS
CANDIDATE_TRAIT_REQUIRED_COLUMNS = (
    "target",
    "status",
    "region_execution_form",
    "execution_body",
    "pipeline_shape",
    "pipeline_estimated_cardinality",
    "example_reason",
    *CANDIDATE_TRAIT_FIELDS,
)
ADMISSION_EVIDENCE_REQUIRED_COLUMNS = BENCHMARK_ADMISSION_EVIDENCE_FIELDS
AUTO_POLICY_RELATIVE_TOLERANCE = 1.005

def wrapper_only_pipeline_shape(source: str, sink: str = "") -> str:
    result = "pipeline;source:source:" + source + ":source-missing-contract"
    if sink:
        result += ";sink:sink:" + sink + ":sink"
    return result


def wrapper_only_region_source_marker(source: str) -> str:
    return "source:" + source + ":boundary"


WRAPPER_ONLY_PIPELINE_SHAPES = {
    wrapper_only_pipeline_shape("CREATE_TABLE_AS", "RESULT_COLLECTOR"),
    wrapper_only_pipeline_shape("RESULT_COLLECTOR"),
    wrapper_only_pipeline_shape("EXPLAIN_ANALYZE"),
}
WRAPPER_ONLY_REGION_SOURCE_MARKERS = tuple(
    wrapper_only_region_source_marker(source)
    for source in ("CREATE_TABLE_AS", "RESULT_COLLECTOR", "EXPLAIN_ANALYZE")
)


def read_csv(path: Path) -> list:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def row_int(row: dict, field: str) -> int:
    value = row.get(field, "")
    if value in ("", None):
        return 0
    return int(value)


def row_float(row: dict, field: str) -> float:
    value = row.get(field, "")
    if value in ("", None):
        return 0.0
    return float(value)


def require_columns(name: str, rows: list, required: tuple) -> None:
    if not rows:
        raise AssertionError(f"{name}: expected at least one row")
    missing = [field for field in required if field not in rows[0]]
    if missing:
        raise AssertionError(f"{name}: missing required columns {missing}")


def expected_query_ids(count: int) -> list:
    return [f"{query_id:02d}" for query_id in range(1, count + 1)]


def verify_runs(trace_dir: Path, rows: list, expected_queries: list, policies: list, repeats: int) -> None:
    require_columns("runs.csv", rows, RUN_REQUIRED_COLUMNS)
    expected_rows = len(expected_queries) * len(policies) * repeats
    if len(rows) != expected_rows:
        raise AssertionError(f"runs.csv: expected {expected_rows} rows, found {len(rows)}")

    seen = set()
    for row in rows:
        key = (row["query"], row["policy"], row["repeat"])
        if key in seen:
            raise AssertionError(f"runs.csv: duplicate run row {key}")
        seen.add(key)
        if row["query"] not in expected_queries:
            raise AssertionError(f"runs.csv: unexpected query {row['query']}")
        if row["policy"] not in policies:
            raise AssertionError(f"runs.csv: unexpected policy {row['policy']}")
        repeat = row_int(row, "repeat")
        if repeat < 1 or repeat > repeats:
            raise AssertionError(f"runs.csv: repeat out of range: {row}")
        if row_int(row, "profile_query_time_us") <= 0:
            raise AssertionError(f"runs.csv: non-positive query timing: {row}")
        if row_int(row, "profile_operator_count") <= 0:
            raise AssertionError(f"runs.csv: missing operator profile coverage: {row}")
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"runs.csv: non-zero correctness diff: {row}")
        for artifact_field in ("profile_json", "correctness_csv", "decision_counters_csv"):
            artifact = trace_dir / row[artifact_field]
            if not artifact.exists():
                raise AssertionError(f"runs.csv: missing per-run artifact {artifact}")


def verify_summary(rows: list, expected_queries: list, policies: list, repeats: int) -> None:
    require_columns("summary.csv", rows, SUMMARY_REQUIRED_COLUMNS)
    expected_rows = len(expected_queries) * len(policies)
    if len(rows) != expected_rows:
        raise AssertionError(f"summary.csv: expected {expected_rows} rows, found {len(rows)}")
    seen = set()
    off_by_query = {}
    for row in rows:
        key = (row["query"], row["policy"])
        if key in seen:
            raise AssertionError(f"summary.csv: duplicate row {key}")
        seen.add(key)
        if row_int(row, "run_count") != repeats:
            raise AssertionError(f"summary.csv: wrong run count: {row}")
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"summary.csv: non-zero correctness diff: {row}")
        if row_float(row, "median_s") <= 0:
            raise AssertionError(f"summary.csv: non-positive median timing: {row}")
        if row_float(row, "speedup_vs_off_median") <= 0:
            raise AssertionError(f"summary.csv: non-positive speedup: {row}")
        faster_than_off = row.get("faster_than_off_median", "")
        expected_faster_than_off = "true" if row_float(row, "speedup_vs_off_median") >= 1.0 else "false"
        if faster_than_off != expected_faster_than_off:
            raise AssertionError(f"summary.csv: faster_than_off_median mismatch: {row}")
        if row["policy"] == "off":
            if row_int(row, "evidence_compiled_regions") != 0 or row_int(row, "evidence_decision_count") != 0:
                raise AssertionError(f"summary.csv: off policy produced JIT evidence work: {row}")
        if row["policy"] == "auto" and row_int(row, "evidence_compiled_regions") > 0:
            if row_float(row, "speedup_vs_off_median") < 1.0:
                raise AssertionError(
                    f"summary.csv: auto evidence compiled regions without positive production speedup: {row}"
                )
        timings = [value for value in row.get("timings_s", "").split(";") if value]
        if len(timings) != repeats:
            raise AssertionError(f"summary.csv: timings_s does not match repeats: {row}")
        if row["policy"] == "off":
            off_by_query[row["query"]] = row_float(row, "median_s")
    for query_id in expected_queries:
        if query_id not in off_by_query:
            raise AssertionError(f"summary.csv: missing off baseline for q{query_id}")


def verify_policy_summary(rows: list, expected_queries: list, policies: list, repeats: int) -> None:
    require_columns("policy_summary.csv", rows, POLICY_REQUIRED_COLUMNS)
    if sorted(row["policy"] for row in rows) != sorted(policies):
        raise AssertionError(
            f"policy_summary.csv: expected policies {policies}, found {[row['policy'] for row in rows]}"
        )
    for row in rows:
        if row_int(row, "query_count") != len(expected_queries):
            raise AssertionError(f"policy_summary.csv: wrong query count: {row}")
        if row_int(row, "run_count") != len(expected_queries) * repeats:
            raise AssertionError(f"policy_summary.csv: wrong run count: {row}")
        if row["policy"] == "auto" and row_int(row, "evidence_decision_count") <= 0:
            raise AssertionError(f"policy_summary.csv: auto policy produced no compiled-region decision counters: {row}")
        if row["policy"] == "off":
            if row_int(row, "evidence_compiled_regions") != 0 or row_int(row, "evidence_decision_count") != 0:
                raise AssertionError(f"policy_summary.csv: off policy produced JIT evidence work: {row}")
        if row["policy"] == "auto" and row_int(row, "evidence_compiled_regions") > 0:
            if row_float(row, "relative_to_off") > AUTO_POLICY_RELATIVE_TOLERANCE:
                raise AssertionError(f"policy_summary.csv: auto compiled regions but lost to off policy: {row}")
        if row["policy"] == "force" and row_int(row, "evidence_compiled_regions") <= 0:
            raise AssertionError(f"policy_summary.csv: force policy did not compile any traced regions: {row}")
        if row_float(row, "total_median_s") <= 0:
            raise AssertionError(f"policy_summary.csv: non-positive total median: {row}")
        if row_float(row, "relative_to_off") <= 0:
            raise AssertionError(f"policy_summary.csv: non-positive relative time: {row}")
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"policy_summary.csv: non-zero correctness diff: {row}")
        classified = row_int(row, "faster_queries") + row_int(row, "slower_queries") + row_int(row, "equal_queries")
        if classified != len(expected_queries):
            raise AssertionError(f"policy_summary.csv: query speed classification mismatch: {row}")


def verify_correctness(rows: list, expected_queries: list, policies: list, repeats: int) -> None:
    require_columns(
        "correctness_summary.csv", rows, ("query", "policy", "run_count", "baseline_rows", "correctness_diff")
    )
    if len(rows) != len(expected_queries) * len(policies):
        raise AssertionError("correctness_summary.csv: wrong row count")
    for row in rows:
        if row_int(row, "run_count") != repeats:
            raise AssertionError(f"correctness_summary.csv: wrong run count: {row}")
        if row_int(row, "baseline_rows") < 0:
            raise AssertionError(f"correctness_summary.csv: invalid baseline rows: {row}")
        if row_int(row, "correctness_diff") != 0:
            raise AssertionError(f"correctness_summary.csv: non-zero correctness diff: {row}")


def admission_evidence_key(row: dict) -> tuple:
    return (
        row["admission_shape_key"],
        row["execution_mode"],
        row["region_execution_form"],
        row["execution_body"],
        row["candidate_shape"],
        *(row[field] for field in CANDIDATE_SIGNATURE_FIELDS),
        row["candidate_contract_abi"],
    )


def verify_admission_evidences(rows: list, decision_counters: list, policy_summary: list) -> None:
    force_compiled = sum(
        row_int(row, "evidence_compiled_regions") for row in policy_summary if row["policy"] == "force"
    )
    if not rows:
        if force_compiled > 0:
            raise AssertionError("admission_evidence_summary.csv: force compiled regions but no admission evidence rows")
        return
    require_columns("admission_evidence_summary.csv", rows, ADMISSION_EVIDENCE_REQUIRED_COLUMNS)

    expected_keys = {
        admission_evidence_key(row)
        for row in decision_counters
        if row.get("target") == "region" and row.get("policy") == "force" and row.get("status") == "compiled"
    }
    actual_keys = set()
    for row in rows:
        for field in CANDIDATE_SIGNATURE_FIELDS:
            if row.get(field, "") in ("", "none"):
                raise AssertionError(f"admission_evidence_summary.csv: missing {field}: {row}")
        key = admission_evidence_key(row)
        if key in actual_keys:
            raise AssertionError(f"admission_evidence_summary.csv: duplicate admission evidence row: {row}")
        actual_keys.add(key)
        if row_int(row, "force_compiled_regions") <= 0:
            raise AssertionError(f"admission_evidence_summary.csv: row has no force compiled regions: {row}")
        query_count = row_int(row, "query_count")
        classified_queries = (
            row_int(row, "force_winning_queries")
            + row_int(row, "force_losing_queries")
            + row_int(row, "force_equal_queries")
        )
        if query_count <= 0 or classified_queries != query_count:
            raise AssertionError(f"admission_evidence_summary.csv: query classification mismatch: {row}")
        if row["proof_status"] not in KNOWN_ADMISSION_EVIDENCE_STATUSES:
            raise AssertionError(f"admission_evidence_summary.csv: unknown proof status: {row}")
        if row["root_cause"] not in KNOWN_ADMISSION_EVIDENCE_ROOT_CAUSES:
            raise AssertionError(f"admission_evidence_summary.csv: unknown root cause: {row}")
        for field in (
            "min_force_speedup_vs_off",
            "median_force_speedup_vs_off",
            "max_force_speedup_vs_off",
        ):
            if row_float(row, field) <= 0:
                raise AssertionError(f"admission_evidence_summary.csv: non-positive speedup field {field}: {row}")
        if row_float(row, "min_force_speedup_vs_off") > row_float(row, "median_force_speedup_vs_off"):
            raise AssertionError(f"admission_evidence_summary.csv: min speedup exceeds median: {row}")
        if row_float(row, "median_force_speedup_vs_off") > row_float(row, "max_force_speedup_vs_off"):
            raise AssertionError(f"admission_evidence_summary.csv: median speedup exceeds max: {row}")
        if row["root_cause"] == "positive_shape_without_auto_admission":
            if row["auto_rule_present"] != "false":
                raise AssertionError(f"admission_evidence_summary.csv: positive shape without auto admission has auto rule: {row}")
            if row["proof_status"] != "positive_query_median":
                raise AssertionError(f"admission_evidence_summary.csv: positive shape without auto admission is not positive: {row}")
        if row["root_cause"] == "auto_rule_admitted_and_compiled":
            if row["auto_rule_present"] != "true":
                raise AssertionError(f"admission_evidence_summary.csv: admitted row has no auto rule: {row}")
            if row_int(row, "auto_compiled_regions") <= 0:
                raise AssertionError(f"admission_evidence_summary.csv: admitted row did not compile in auto: {row}")
            if row["proof_status"] != "positive_query_median":
                raise AssertionError(f"admission_evidence_summary.csv: auto admitted non-positive proof: {row}")
        if row["root_cause"] == "force_region_not_profitable" and row["proof_status"] != "negative_query_median":
            raise AssertionError(f"admission_evidence_summary.csv: force-not-profitable row has wrong status: {row}")

    if actual_keys != expected_keys:
        raise AssertionError(
            "admission_evidence_summary.csv: admission evidence keys do not match force compiled region keys; "
            f"missing={sorted(expected_keys - actual_keys)} extra={sorted(actual_keys - expected_keys)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify TPC-H JIT benchmark artifacts")
    parser.add_argument("benchmark_dir", type=Path)
    parser.add_argument("--expected-queries", type=int, default=22)
    parser.add_argument("--queries", nargs="+", default=None)
    parser.add_argument("--policies", nargs="+", default=["off", "auto", "force"])
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    trace_dir = args.benchmark_dir.resolve()
    expected_queries = (
        [f"{int(query_id):02d}" for query_id in args.queries]
        if args.queries is not None
        else expected_query_ids(args.expected_queries)
    )
    manifest = verify_trace_manifest(trace_dir, kind="tpch_jit_benchmark", required_artifacts=list(REQUIRED_ARTIFACTS))
    configuration = manifest.get("configuration", {})
    if configuration.get("policies") != args.policies:
        raise AssertionError(
            f"trace_manifest.json: expected policies {args.policies}, found {configuration.get('policies')}"
        )
    if configuration.get("queries") != expected_queries:
        raise AssertionError(
            f"trace_manifest.json: expected queries {expected_queries}, found {configuration.get('queries')}"
        )
    if int(configuration.get("repeats", 0)) != args.repeats:
        raise AssertionError(
            f"trace_manifest.json: expected repeats {args.repeats}, found {configuration.get('repeats')}"
        )

    runs = read_csv(trace_dir / "runs.csv")
    summary = read_csv(trace_dir / "summary.csv")
    query_summary = read_csv(trace_dir / "query_summary.csv")
    policy_summary = read_csv(trace_dir / "policy_summary.csv")
    correctness = read_csv(trace_dir / "correctness_summary.csv")
    decision_counters = read_csv(trace_dir / "decision_counter_summary.csv")
    admission_evidences = read_csv(trace_dir / "admission_evidence_summary.csv")
    operator_profile = read_csv(trace_dir / "operator_profile_summary.csv")

    verify_runs(trace_dir, runs, expected_queries, args.policies, args.repeats)
    verify_summary(summary, expected_queries, args.policies, args.repeats)
    if query_summary != summary:
        raise AssertionError("query_summary.csv must match summary.csv")
    verify_policy_summary(policy_summary, expected_queries, args.policies, args.repeats)
    verify_correctness(correctness, expected_queries, args.policies, args.repeats)
    verify_admission_evidences(admission_evidences, decision_counters, policy_summary)
    if not decision_counters:
        raise AssertionError("decision_counter_summary.csv: expected JIT decision counter rows")
    require_columns("decision_counter_summary.csv", decision_counters, CANDIDATE_TRAIT_REQUIRED_COLUMNS)
    for row in decision_counters:
        region_execution_form = row.get("region_execution_form", "")
        if row.get("target") == "region" and region_execution_form not in KNOWN_REGION_EXECUTION_FORMS:
            raise AssertionError(f"decision_counter_summary.csv: unknown region execution form: {row}")
        execution_body = row.get("execution_body", "")
        if row.get("target") == "region" and execution_body not in KNOWN_EXECUTION_BODIES:
            raise AssertionError(f"decision_counter_summary.csv: unknown execution body: {row}")
        if row.get("target") == "region" and row.get("status") == "compiled" and region_execution_form == "none":
            raise AssertionError(f"decision_counter_summary.csv: compiled region counter has no execution form: {row}")
        if row.get("target") == "region" and row.get("status") == "compiled" and execution_body == "none":
            raise AssertionError(f"decision_counter_summary.csv: compiled region counter has no execution body: {row}")
        if "source:source:SET:" in row.get("candidate_pipeline_shape", ""):
            raise AssertionError(f"decision_counter_summary.csv: setup pipeline leaked into candidate counters: {row}")
        pipeline_shape = row.get("pipeline_shape", "")
        if pipeline_shape in WRAPPER_ONLY_PIPELINE_SHAPES:
            raise AssertionError(f"decision_counter_summary.csv: wrapper-only pipeline leaked into counters: {row}")
        if any(marker in row.get("example_reason", "") for marker in WRAPPER_ONLY_REGION_SOURCE_MARKERS):
            raise AssertionError(f"decision_counter_summary.csv: wrapper-only source leaked into counters: {row}")
    if not operator_profile:
        raise AssertionError("operator_profile_summary.csv: expected operator profile rows")

    print(
        (
            "ok tpch_benchmark runs={runs} summary={summary} policy={policy} "
            "decision_counter={decision} profile={profile}"
        ).format(
            runs=len(runs),
            summary=len(summary),
            policy=len(policy_summary),
            decision=len(decision_counters),
            profile=len(operator_profile),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
