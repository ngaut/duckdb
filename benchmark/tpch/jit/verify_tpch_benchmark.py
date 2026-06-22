#!/usr/bin/env python3
#
# Verify artifacts produced by tpch_benchmark.py.

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import (
    normalize_query_ids,
    read_csv,
    require,
    require_columns,
    row_bool,
    row_float,
    row_int,
    verify_profile,
)
from tpch_common import (
    COUNTER_FIELDS,
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    PERFORMANCE_GAP_FIELDS,
    RUN_FIELDS,
    SUMMARY_FIELDS,
)

POLICY_ORDER = {policy: index for index, policy in enumerate(DEFAULT_POLICIES)}
DEFAULT_MIN_AUTO_SPEEDUP = 0.98
DEFAULT_AUTO_NO_DECISION_NOISE_S = 0.005


def expected_queries(rows: list[dict], requested: list[str] | None) -> list[str]:
    if requested is not None:
        if requested == ["all"]:
            return list(DEFAULT_QUERIES)
        return normalize_query_ids(requested)
    return sorted({row["query"] for row in rows})


def expected_policies(rows: list[dict], requested: list[str] | None) -> list[str]:
    if requested is not None:
        return list(requested)
    return sorted({row["policy"] for row in rows}, key=lambda policy: POLICY_ORDER.get(policy, 100))


def expected_repeats(rows: list[dict], requested: int | None) -> int:
    if requested is not None:
        return requested
    repeats = sorted({row_int(row, "repeat") for row in rows})
    require(repeats, "runs.csv: no repeat rows")
    expected = list(range(1, repeats[-1] + 1))
    require(repeats == expected, f"runs.csv: repeats must be contiguous from 1, found={repeats}")
    return repeats[-1]


def verify_summary(summary_rows: list[dict], queries: list[str], policies: list[str]) -> None:
    require_columns(summary_rows, SUMMARY_FIELDS)
    expected = {(query, policy) for query in queries for policy in policies}
    actual = {(row["query"], row["policy"]) for row in summary_rows}
    require(
        actual == expected,
        f"summary.csv: query/policy mismatch missing={sorted(expected - actual)} extra={sorted(actual - expected)}",
    )
    for row in summary_rows:
        require(row_float(row, "median_s") > 0, f"summary.csv: non-positive median: {row}")
        require(row_float(row, "speedup_vs_off_median") > 0, f"summary.csv: non-positive speedup: {row}")
        require(row_int(row, "correctness_diff") == 0, f"summary.csv: correctness mismatch: {row}")
        if row["policy"] == "off":
            require(row_int(row, "compiled_regions") == 0, f"summary.csv: off policy compiled regions: {row}")


def verify_runs(trace_dir: Path, run_rows: list[dict], queries: list[str], policies: list[str], repeats: int) -> None:
    require_columns(run_rows, RUN_FIELDS, "runs.csv")
    expected = {
        (query, policy, str(repeat)) for query in queries for policy in policies for repeat in range(1, repeats + 1)
    }
    actual = {(row["query"], row["policy"], row["repeat"]) for row in run_rows}
    require(
        actual == expected,
        f"runs.csv: query/policy/repeat mismatch missing={sorted(expected - actual)} extra={sorted(actual - expected)}",
    )
    for row in run_rows:
        require(row_int(row, "query_time_us") > 0, f"runs.csv: non-positive runtime: {row}")
        require(row_int(row, "correctness_diff") == 0, f"runs.csv: correctness mismatch: {row}")
        if row["policy"] == "off":
            require(row_int(row, "compiled_regions") == 0, f"runs.csv: off policy compiled regions: {row}")
        require(row["timing_mode"] in ("production", "profile"), f"runs.csv: bad timing mode: {row}")
        if row["profile_json"]:
            verify_profile(
                trace_dir,
                {"profile_json": row["profile_json"]},
                require_regions=False,
            )


def verify_counters(rows: list[dict], queries: list[str], policies: list[str], repeats: int) -> None:
    if not rows:
        return
    require_columns(rows, COUNTER_FIELDS, "counters.csv")
    query_set = set(queries)
    policy_set = set(policies)
    for row in rows:
        require(row["query"] in query_set, f"counters.csv: unexpected query: {row}")
        require(row["policy"] in policy_set, f"counters.csv: unexpected policy: {row}")
        repeat = row_int(row, "repeat")
        require(1 <= repeat <= repeats, f"counters.csv: unexpected repeat: {row}")
        require(row_int(row, "count") > 0, f"counters.csv: non-positive count: {row}")
        if row["status"] in ("skipped", "unsupported", "unavailable", "error"):
            require(row["blocker"], f"counters.csv: missing blocker: {row}")
        if row_bool(row, "runner_cost_profile"):
            require(row_int(row, "runner_cost_startup_cost") > 0, f"counters.csv: missing runner cost: {row}")


def verify_performance_gaps(
    rows: list[dict],
    queries: list[str],
    policies: list[str],
    require_no_auto_decisions: bool,
    min_auto_speedup: float,
    auto_no_decision_noise_s: float,
) -> None:
    require_columns(rows, PERFORMANCE_GAP_FIELDS, "performance_gaps.csv")
    expected_queries = set(queries)
    actual_queries = {row["query"] for row in rows}
    require(
        actual_queries == expected_queries,
        f"performance_gaps.csv: query mismatch missing={sorted(expected_queries - actual_queries)} "
        f"extra={sorted(actual_queries - expected_queries)}",
    )
    has_auto = "auto" in policies
    for row in rows:
        require(row_float(row, "off_median_s") > 0, f"performance_gaps.csv: missing off median: {row}")
        if has_auto:
            require(row_float(row, "auto_median_s") > 0, f"performance_gaps.csv: missing auto median: {row}")
            require(row_float(row, "auto_speedup_vs_off") > 0, f"performance_gaps.csv: missing auto speedup: {row}")
            auto_decisions = (
                row_int(row, "auto_compiled_regions")
                + row_int(row, "auto_unsupported_decisions")
                + row_int(row, "auto_skipped_decisions")
            )
            if require_no_auto_decisions:
                require(auto_decisions == 0, f"performance_gaps.csv: auto made JIT decisions: {row}")
            auto_slowdown_s = row_float(row, "auto_median_s") - row_float(row, "off_median_s")
            if auto_decisions == 0:
                require(
                    auto_slowdown_s <= auto_no_decision_noise_s,
                    f"performance_gaps.csv: zero-decision auto slowdown above {auto_no_decision_noise_s}s: {row}",
                )
            else:
                require(
                    row_float(row, "auto_speedup_vs_off") >= min_auto_speedup,
                    f"performance_gaps.csv: auto speedup below {min_auto_speedup}: {row}",
                )
            if auto_decisions > 0:
                require(row["auto_primary_blocker"], f"performance_gaps.csv: missing auto blocker: {row}")
                require(
                    row_int(row, "auto_primary_blocker_count") > 0,
                    f"performance_gaps.csv: missing blocker count: {row}",
                )
            require(
                row_int(row, "auto_unsupported_decisions") >= 0,
                f"performance_gaps.csv: bad auto unsupported count: {row}",
            )
            require(row_int(row, "auto_skipped_decisions") >= 0, f"performance_gaps.csv: bad auto skipped count: {row}")
            require(row_int(row, "auto_decision_time_us") >= 0, f"performance_gaps.csv: bad auto decision time: {row}")
            require(row_int(row, "auto_compile_time_us") >= 0, f"performance_gaps.csv: bad auto compile time: {row}")
            require(row_int(row, "auto_codegen_time_us") >= 0, f"performance_gaps.csv: bad auto codegen time: {row}")
            require(
                row_int(row, "auto_executable_build_time_us") >= 0,
                f"performance_gaps.csv: bad auto executable build time: {row}",
            )
            require(
                row_int(row, "auto_machine_codegen_time_us") >= 0,
                f"performance_gaps.csv: bad auto machine codegen time: {row}",
            )
            require(
                row_int(row, "auto_lazy_codegen_time_us") >= 0,
                f"performance_gaps.csv: bad auto lazy codegen time: {row}",
            )
            require(
                row_int(row, "auto_lazy_machine_codegen_time_us") >= 0,
                f"performance_gaps.csv: bad auto lazy machine codegen time: {row}",
            )
            require(
                row_int(row, "auto_lazy_code_size") >= 0,
                f"performance_gaps.csv: bad auto lazy code size: {row}",
            )
            require(row_int(row, "auto_runner_cost_startup_cost") >= 0, f"performance_gaps.csv: bad runner cost: {row}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify compact TPC-H JIT benchmark artifacts")
    parser.add_argument("trace_dir", type=Path)
    parser.add_argument("--queries", nargs="+", default=None)
    parser.add_argument("--policies", nargs="+", default=None, choices=DEFAULT_POLICIES)
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--require-no-auto-decisions", action="store_true")
    parser.add_argument("--min-auto-speedup", type=float, default=DEFAULT_MIN_AUTO_SPEEDUP)
    parser.add_argument("--auto-no-decision-noise-s", type=float, default=DEFAULT_AUTO_NO_DECISION_NOISE_S)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trace_dir = args.trace_dir.resolve()
    summary_rows = read_csv(trace_dir / "summary.csv")
    run_rows = read_csv(trace_dir / "runs.csv")
    counter_rows = read_csv(trace_dir / "counters.csv")
    performance_gap_rows = read_csv(trace_dir / "performance_gaps.csv")
    queries = expected_queries(summary_rows, args.queries)
    policies = expected_policies(summary_rows, args.policies)
    repeats = expected_repeats(run_rows, args.repeats)
    verify_summary(summary_rows, queries, policies)
    verify_runs(trace_dir, run_rows, queries, policies, repeats)
    verify_counters(counter_rows, queries, policies, repeats)
    verify_performance_gaps(
        performance_gap_rows,
        queries,
        policies,
        args.require_no_auto_decisions,
        args.min_auto_speedup,
        args.auto_no_decision_noise_s,
    )
    print(f"verified TPC-H JIT benchmark: {args.trace_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
