#!/usr/bin/env python3
#
# Verify artifacts produced by tpch_benchmark.py.

import argparse
import collections
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import (
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
    PERFORMANCE_GAP_FIELDS,
    RUN_FIELDS,
    RUNNER_COST_COMPONENT_FIELDS,
    SUMMARY_FIELDS,
    TPCHConfigurationError,
    normalize_tpch_query_ids,
)

POLICY_ORDER = {policy: index for index, policy in enumerate(DEFAULT_POLICIES)}
DEFAULT_MIN_AUTO_SPEEDUP = 0.98
DEFAULT_AUTO_NO_DECISION_NOISE_S = 0.005
MATERIALIZATION_ELISION_FORBIDDEN_RUNTIME_PATTERNS = (
    ("fallback", re.compile(r"fallback", re.IGNORECASE)),
    ("whole executor", re.compile(r"whole[-_]executor", re.IGNORECASE)),
    ("materialization", re.compile(r"materialization", re.IGNORECASE)),
    ("buffer append", re.compile(r"buffer_append", re.IGNORECASE)),
)
RUNTIME_PROOF_FIELDS = (
    "source_stage_count_breakdown",
    "source_stage_runtime_breakdown",
    "generated_stage_count_breakdown",
    "generated_stage_runtime_breakdown",
    "jit_runtime_path_counts",
    "jit_runtime_delegation_counts",
)


def expected_queries(rows: list[dict], requested: list[str] | None) -> list[str]:
    if requested is not None:
        return normalize_tpch_query_ids(requested)
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
            require(row_int(row, "runner_cost_startup_cost") >= 0, f"counters.csv: invalid runner cost: {row}")
            require(row_int(row, "runner_cost_rows") > 0, f"counters.csv: missing runner cost rows: {row}")
            require(row_int(row, "runner_cost_batches") > 0, f"counters.csv: missing runner cost batches: {row}")
            selection_reason = row.get("runner_cost_selection_reason", "")
            require(selection_reason, f"counters.csv: missing runner cost reason: {row}")
            if row_int(row, "runner_cost_selected_accelerated_runner_count") > 0:
                require(
                    selection_reason.startswith("admitted_"),
                    f"counters.csv: selected runner missing admission reason: {row}",
                )
            else:
                require(
                    selection_reason.startswith("rejected_"),
                    f"counters.csv: vectorized runner missing rejection reason: {row}",
                )
            if row_int(row, "runner_cost_saved_work_per_batch") != 0:
                has_cost_evidence = any(row_int(row, field) != 0 for field in RUNNER_COST_COMPONENT_FIELDS)
                require(has_cost_evidence, f"counters.csv: missing runner cost components: {row}")


def counter_kernel_key(row: dict) -> tuple[str, str, str, str, str]:
    return (row["query"], row["policy"], row["repeat"], row["backend_name"], row["kernel_id"])


def runtime_proof_names(value: str) -> list[str]:
    names = []
    for entry in value.split(";"):
        name = entry.split("=", 1)[0].strip()
        if not name:
            continue
        names.append(name)
        tail = name.split(":", 1)[-1]
        names.append(tail)
        names.extend(component for component in tail.split(".") if component)
    return names


def row_contains_materialization_elision_violation(row: dict) -> str:
    for field in RUNTIME_PROOF_FIELDS:
        proof_names = set(runtime_proof_names(row.get(field, "")))
        for proof_name in proof_names:
            for label, pattern in MATERIALIZATION_ELISION_FORBIDDEN_RUNTIME_PATTERNS:
                if pattern.search(proof_name):
                    return f"{field} contains {label} runtime work: {proof_name}"
    return ""


def row_runtime_proof_names(row: dict) -> set[str]:
    return set(runtime_proof_names(row.get("jit_runtime_proof_counts", "")))


def row_has_runtime_proof(row: dict, proof: str) -> bool:
    return proof in row_runtime_proof_names(row)


def counter_breakdown_has_positive_count(value: str) -> bool:
    for entry in value.split(";"):
        if "=" not in entry:
            continue
        _, count = entry.rsplit("=", 1)
        try:
            if int(count) > 0:
                return True
        except ValueError:
            continue
    return False


def row_has_generated_stage_runtime(row: dict) -> bool:
    return counter_breakdown_has_positive_count(row.get("generated_stage_count_breakdown", ""))


def row_has_no_runtime_work(row: dict) -> bool:
    return (
        row_int(row, "input_rows") == 0
        and row_int(row, "output_rows") == 0
        and not row_has_generated_stage_runtime(row)
        and not row.get("jit_runtime_delegation_counts", "")
    )


def selected_cbo_rows(rows: list[dict]) -> list[dict]:
    result = []
    for row in rows:
        if (
            row_bool(row, "runner_cost_profile")
            and row_int(row, "runner_cost_selected_accelerated_runner_count") > 0
            and row_int(row, "kernel_id") > 0
        ):
            result.append(row)
            require(row["status"] == "compiled", f"counters.csv: selected CBO row is not a compile row: {row}")
            require(row["execution_mode"] == "native", f"counters.csv: selected CBO row is not native: {row}")
            require(
                row["selected_runner"] == "compiled_vectorized",
                f"counters.csv: selected CBO row has unexpected runner: {row}",
            )
    return result


def require_runtime_rows(
    runtime_rows_by_kernel: dict[tuple[str, str, str, str, str], list[dict]],
    cbo_row: dict,
    require_runtime_proof: bool,
    label: str,
) -> list[dict]:
    key = counter_kernel_key(cbo_row)
    runtime_rows = runtime_rows_by_kernel.get(key, [])
    require(
        runtime_rows or not require_runtime_proof,
        f"counters.csv: selected {label} CBO row has no runtime counters: {cbo_row}",
    )
    return runtime_rows


def runtime_proof_requirements(row: dict) -> set[str]:
    return {proof for proof in row.get("runner_cost_required_runtime_proofs", "").split("|") if proof}


def runtime_proof_requirement_satisfied(proof: str, runtime_rows: list[dict]) -> bool:
    if proof == "generated_stage_work":
        return any(
            row_has_runtime_proof(row, proof) or row_has_generated_stage_runtime(row) for row in runtime_rows
        ) or all(row_has_runtime_proof(row, "no_work") or row_has_no_runtime_work(row) for row in runtime_rows)
    if proof == "generated_backend_work":
        if any(row_has_runtime_proof(row, proof) for row in runtime_rows):
            return True
        return all(
            row_has_runtime_proof(row, "no_work")
            and row_int(row, "lazy_code_size") == 0
            and not row.get("jit_runtime_delegation_counts", "")
            for row in runtime_rows
        )
    if proof == "full_pipeline_ownership":
        return any(row_has_runtime_proof(row, proof) for row in runtime_rows) or all(
            row_has_runtime_proof(row, "no_work") for row in runtime_rows
        )
    if proof == "materialization_elision":
        for row in runtime_rows:
            violation = row_contains_materialization_elision_violation(row)
            require(
                not violation,
                "counters.csv: materialization-elision runtime proof contains materialization/fallback work: "
                f"{violation}: {row}",
            )
            require(
                not row.get("jit_runtime_delegation_counts", ""),
                f"counters.csv: materialization-elision kernel delegated runtime work: {row}",
            )
        return any(row_has_runtime_proof(row, proof) and row_int(row, "invocation_count") > 0 for row in runtime_rows)
    require(False, f"counters.csv: unknown CBO runtime proof requirement: {proof}")
    return False


def adaptive_fallback_kernel_keys(rows: list[dict]) -> set[tuple[str, str, str, str, str]]:
    """Kernels whose measured runner verdict fell back to native execution.

    A recorded fallback verdict is the satisfied outcome of the measured runner
    decision: the compiled kernel ran its measurement leg and handed the pipeline
    to the vectorized continuation, so no executed compiled runtime rows exist and
    none are owed.
    """
    return {
        counter_kernel_key(row)
        for row in rows
        if row.get("runtime_result", "") == "adaptive_ab" and "verdict=fallback_native" in row.get("reason", "")
    }


def verify_cbo_runtime_counter_contract(rows: list[dict], require_runtime_proof: bool) -> None:
    runtime_rows_by_kernel = collections.defaultdict(list)
    for row in rows:
        if row["status"] == "executed" and row["execution_mode"] == "native" and row_int(row, "kernel_id") > 0:
            runtime_rows_by_kernel[counter_kernel_key(row)].append(row)
    fallback_kernels = adaptive_fallback_kernel_keys(rows)

    credited_work_fields = (
        "runner_cost_generated_stage_work",
        "runner_cost_generated_backend_stage_work",
        "runner_cost_native_operator_work",
        "runner_cost_materialization_elision_work",
        "runner_cost_full_pipeline_work",
    )
    for cbo_row in selected_cbo_rows(rows):
        requirements = runtime_proof_requirements(cbo_row)
        credited_work = sum(row_int(cbo_row, field) for field in credited_work_fields)
        require(
            requirements or credited_work == 0,
            f"counters.csv: selected CBO row credits work without typed runtime proof requirements: {cbo_row}",
        )
        if not requirements:
            continue
        if counter_kernel_key(cbo_row) in fallback_kernels:
            continue
        runtime_rows = require_runtime_rows(
            runtime_rows_by_kernel, cbo_row, require_runtime_proof, "typed-runtime-proof"
        )
        if not require_runtime_proof:
            continue
        for proof in sorted(requirements):
            require(
                runtime_proof_requirement_satisfied(proof, runtime_rows),
                f"counters.csv: selected CBO row did not satisfy required runtime proof {proof}: {cbo_row}",
            )


def median_value(values: list[float]) -> float:
    require(values, "cannot compute median for empty value list")
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2


def paired_policy_runtime_stats(
    run_rows: list[dict], query: str, baseline_policy: str, test_policy: str
) -> dict[str, float] | None:
    by_repeat = collections.defaultdict(dict)
    for row in run_rows:
        if row["query"] != query or row["policy"] not in (baseline_policy, test_policy):
            continue
        by_repeat[row_int(row, "repeat")][row["policy"]] = row_int(row, "query_time_us")

    deltas = []
    speedups = []
    for repeat in sorted(by_repeat):
        repeat_rows = by_repeat[repeat]
        if baseline_policy in repeat_rows and test_policy in repeat_rows:
            baseline_us = repeat_rows[baseline_policy]
            test_us = repeat_rows[test_policy]
            deltas.append((test_us - baseline_us) / 1_000_000)
            if baseline_us > 0 and test_us > 0:
                speedups.append(float(baseline_us) / float(test_us))
    if not deltas:
        return None
    require(
        len(speedups) == len(deltas),
        f"runs.csv: paired runtime stats require positive timings for query {query}",
    )
    return {
        "median_delta_s": median_value(deltas),
        "median_speedup": median_value(speedups),
    }


def verify_performance_gaps(
    rows: list[dict],
    run_rows: list[dict],
    queries: list[str],
    policies: list[str],
    require_no_auto_decisions: bool,
    min_auto_speedup: float,
    auto_no_decision_noise_s: float,
    performance_checks: bool,
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
            auto_selected_accelerated = row_int(row, "auto_runner_cost_selected_accelerated_runner_count")
            auto_compiled_regions = row_int(row, "auto_compiled_regions")
            auto_no_accelerated_runner = auto_compiled_regions == 0 and auto_selected_accelerated == 0
            if require_no_auto_decisions:
                require(auto_decisions == 0, f"performance_gaps.csv: auto made JIT decisions: {row}")
            auto_slowdown_s = row_float(row, "auto_median_s") - row_float(row, "off_median_s")
            paired_stats = paired_policy_runtime_stats(run_rows, row["query"], "off", "auto")
            paired_slowdown_s = paired_stats["median_delta_s"] if paired_stats is not None else auto_slowdown_s
            paired_speedup = (
                paired_stats["median_speedup"] if paired_stats is not None else row_float(row, "auto_speedup_vs_off")
            )
            if performance_checks and auto_no_accelerated_runner:
                require(
                    paired_slowdown_s <= auto_no_decision_noise_s,
                    f"performance_gaps.csv: no-accelerated-runner auto slowdown above "
                    f"{auto_no_decision_noise_s}s "
                    f"(paired_median_delta_s={paired_slowdown_s}, paired_median_speedup={paired_speedup}, "
                    f"aggregate_median_delta_s={auto_slowdown_s}): {row}",
                )
            elif performance_checks and paired_slowdown_s <= auto_no_decision_noise_s:
                pass
            elif performance_checks:
                require(
                    paired_speedup >= min_auto_speedup,
                    f"performance_gaps.csv: auto speedup below {min_auto_speedup} "
                    f"(paired_median_speedup={paired_speedup}, aggregate_speedup={row_float(row, 'auto_speedup_vs_off')}, "
                    f"paired_median_delta_s={paired_slowdown_s}): {row}",
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
    parser.add_argument("--performance-checks", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--min-auto-speedup", type=float, default=DEFAULT_MIN_AUTO_SPEEDUP)
    parser.add_argument("--auto-no-decision-noise-s", type=float, default=DEFAULT_AUTO_NO_DECISION_NOISE_S)
    parser.add_argument(
        "--require-cbo-runtime-proof",
        action="store_true",
        help="Require runtime-traced proof rows for selected CBO-credited generated-stage/backend/full-pipeline kernels.",
    )
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
    verify_cbo_runtime_counter_contract(counter_rows, args.require_cbo_runtime_proof)
    verify_performance_gaps(
        performance_gap_rows,
        run_rows,
        queries,
        policies,
        args.require_no_auto_decisions,
        args.min_auto_speedup,
        args.auto_no_decision_noise_s,
        args.performance_checks,
    )
    print(f"verified TPC-H JIT benchmark: {args.trace_dir.resolve()}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TPCHConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
