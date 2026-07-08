#!/usr/bin/env python3
#
# Compare two TPC-H JIT benchmark artifact directories and fail on regressions.

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import read_csv, require, row_float, row_int
from tpch_common import DEFAULT_POLICIES, normalize_tpch_query_ids


DEFAULT_MAX_AUTO_SLOWDOWN_RATIO = 1.02
DEFAULT_MAX_AUTO_SLOWDOWN_S = 0.002
DEFAULT_MIN_AUTO_SPEEDUP = 0.98
DEFAULT_PRESERVE_WIN_SPEEDUP = 1.02
DEFAULT_MAX_WIN_SPEEDUP_DROP = 0.03
DEFAULT_MAX_RUNTIME_COMPONENT_RATIO = 1.10
DEFAULT_MAX_RUNTIME_COMPONENT_US = 200
FAILURE_FIELDS = ("category", "query", "message")


def make_failure(category: str, query: str, message: str) -> dict[str, str]:
    return {"category": category, "query": query, "message": message}


def write_failure_report(path: Path, failures: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(FAILURE_FIELDS))
        writer.writeheader()
        writer.writerows(failures)


def keyed(rows: list[dict], keys: tuple[str, ...], name: str) -> dict[tuple[str, ...] | str, dict]:
    result = {}
    for row in rows:
        key_parts = tuple(row[field] for field in keys)
        key = key_parts[0] if len(key_parts) == 1 else key_parts
        require(key not in result, f"{name}: duplicate row for {key}")
        result[key] = row
    return result


def expected_queries(base_summary: dict, candidate_summary: dict, requested: list[str] | None) -> list[str]:
    if requested is not None:
        queries = normalize_tpch_query_ids(requested)
    else:
        queries = sorted({key[0] for key in base_summary} | {key[0] for key in candidate_summary})
    return queries


def allowed_auto_slowdown(base_s: float, ratio: float, absolute_s: float) -> float:
    return max(absolute_s, base_s * max(0.0, ratio - 1.0))


def off_normalized_candidate_auto_s(base_gap: dict, candidate_gap: dict) -> float:
    candidate_auto_s = row_float(candidate_gap, "auto_median_s")
    base_off_s = row_float(base_gap, "off_median_s")
    candidate_off_s = row_float(candidate_gap, "off_median_s")
    if base_off_s <= 0 or candidate_off_s <= 0:
        return candidate_auto_s
    return candidate_auto_s * (base_off_s / candidate_off_s)


def auto_runtime_preserved(base_gap: dict, candidate_gap: dict, max_slowdown_ratio: float,
                           max_slowdown_s: float) -> bool:
    base_auto_s = row_float(base_gap, "auto_median_s")
    candidate_auto_s = row_float(candidate_gap, "auto_median_s")
    if base_auto_s <= 0 or candidate_auto_s <= 0:
        return True
    allowed_s = allowed_auto_slowdown(base_auto_s, max_slowdown_ratio, max_slowdown_s)
    raw_slowdown_s = candidate_auto_s - base_auto_s
    normalized_candidate_auto_s = off_normalized_candidate_auto_s(base_gap, candidate_gap)
    normalized_slowdown_s = normalized_candidate_auto_s - base_auto_s
    return raw_slowdown_s <= allowed_s or normalized_slowdown_s <= allowed_s


def has_auto_decision(row: dict) -> bool:
	return (
		row_int(row, "auto_compiled_regions")
		+ row_int(row, "auto_unsupported_decisions")
		+ row_int(row, "auto_skipped_decisions")
	) > 0


def has_auto_accelerated_runner(row: dict) -> bool:
    return (
        row_int(row, "auto_compiled_regions") > 0
        or row_int(row, "auto_runner_cost_selected_accelerated_runner_count") > 0
    )


def is_jitted_win(row: dict, speedup_floor: float) -> bool:
	return row_int(row, "compiled_regions") > 0 and row_float(row, "speedup_vs_off_median") >= speedup_floor


def query_label(query: str) -> str:
    return f"Q{int(query):02d}"


def compare_required_rows(
    base_summary: dict,
    candidate_summary: dict,
    base_gaps: dict,
    candidate_gaps: dict,
    queries: list[str],
    policies: list[str],
) -> None:
    for query in queries:
        require(query in base_gaps, f"baseline performance_gaps.csv missing {query_label(query)}")
        require(query in candidate_gaps, f"candidate performance_gaps.csv missing {query_label(query)}")
        for policy in policies:
            key = (query, policy)
            require(key in base_summary, f"baseline summary.csv missing {query_label(query)}/{policy}")
            require(key in candidate_summary, f"candidate summary.csv missing {query_label(query)}/{policy}")


def compare_correctness(candidate_summary: dict, queries: list[str], policies: list[str]) -> list[dict[str, str]]:
    failures = []
    for query in queries:
        for policy in policies:
            row = candidate_summary[(query, policy)]
            if row_int(row, "correctness_diff") != 0:
                failures.append(
                    make_failure(
                        "correctness",
                        query,
                        f"{query_label(query)}/{policy}: correctness_diff={row['correctness_diff']}",
                    )
                )
            if policy == "off" and row_int(row, "compiled_regions") != 0:
                failures.append(
                    make_failure(
                        "off_policy",
                        query,
                        f"{query_label(query)}/off: compiled_regions={row['compiled_regions']}",
                    )
                )
    return failures


def compare_auto_speed(
    base_gaps: dict,
    candidate_gaps: dict,
    queries: list[str],
    max_slowdown_ratio: float,
    max_slowdown_s: float,
    min_auto_speedup: float,
) -> list[dict[str, str]]:
    failures = []
    for query in queries:
        base = base_gaps[query]
        candidate = candidate_gaps[query]
        base_auto_s = row_float(base, "auto_median_s")
        candidate_auto_s = row_float(candidate, "auto_median_s")
        if base_auto_s <= 0 or candidate_auto_s <= 0:
            continue
        slowdown_s = candidate_auto_s - base_auto_s
        if (has_auto_decision(base) or has_auto_decision(candidate)) and not auto_runtime_preserved(
            base, candidate, max_slowdown_ratio, max_slowdown_s
        ):
            normalized_candidate_auto_s = off_normalized_candidate_auto_s(base, candidate)
            normalized_slowdown_s = normalized_candidate_auto_s - base_auto_s
            failures.append(
                make_failure(
                    "auto_runtime",
                    query,
                    f"{query_label(query)}: auto median regressed {base_auto_s:.9f}s -> {candidate_auto_s:.9f}s "
                    f"(+{slowdown_s:.9f}s), off-normalized candidate={normalized_candidate_auto_s:.9f}s "
                    f"(+{normalized_slowdown_s:.9f}s)",
                )
            )
        base_speedup = row_float(base, "auto_speedup_vs_off")
        candidate_speedup = row_float(candidate, "auto_speedup_vs_off")
        if (
            has_auto_accelerated_runner(candidate)
            and base_speedup >= min_auto_speedup
            and candidate_speedup < min_auto_speedup
        ):
            failures.append(
                make_failure(
                    "auto_speedup",
                    query,
                    f"{query_label(query)}: candidate auto speedup dropped below {min_auto_speedup:.6f} "
                    f"({base_speedup:.6f} -> {candidate_speedup:.6f})",
                )
            )
    return failures


def compare_preserved_wins(
    base_summary: dict,
    candidate_summary: dict,
    base_gaps: dict,
    candidate_gaps: dict,
    queries: list[str],
    preserve_win_speedup: float,
    max_win_speedup_drop: float,
    max_slowdown_ratio: float,
    max_slowdown_s: float,
    fail_on_win_coverage_drop: bool,
) -> list[dict[str, str]]:
    failures = []
    base_wins = 0
    candidate_wins = 0
    preserved_baseline_wins = 0
    for query in queries:
        base = base_summary[(query, "auto")]
        candidate = candidate_summary[(query, "auto")]
        base_speedup = row_float(base, "speedup_vs_off_median")
        candidate_speedup = row_float(candidate, "speedup_vs_off_median")
        if is_jitted_win(base, preserve_win_speedup):
            base_wins += 1
            runtime_preserved = auto_runtime_preserved(
                base_gaps[query], candidate_gaps[query], max_slowdown_ratio, max_slowdown_s
            )
            win_preserved = candidate_speedup >= preserve_win_speedup or runtime_preserved
            if win_preserved:
                preserved_baseline_wins += 1
            if fail_on_win_coverage_drop and not runtime_preserved and row_int(candidate, "compiled_regions") == 0:
                failures.append(
                    make_failure(
                        "win_coverage", query, f"{query_label(query)}: lost compiled coverage for baseline JIT win"
                    )
                )
            if (
                fail_on_win_coverage_drop
                and not runtime_preserved
                and candidate_speedup + max_win_speedup_drop < base_speedup
            ):
                failures.append(
                    make_failure(
                        "win_coverage",
                        query,
                        f"{query_label(query)}: JIT win speedup dropped {base_speedup:.6f} -> {candidate_speedup:.6f}",
                    )
                )
        if is_jitted_win(candidate, preserve_win_speedup):
            candidate_wins += 1
    if fail_on_win_coverage_drop and preserved_baseline_wins < base_wins:
        failures.append(
            make_failure(
                "win_coverage",
                "",
                f"preserved baseline JIT wins dropped {base_wins} -> {preserved_baseline_wins}",
            )
        )
    return failures


def compare_runtime_components(
    base_summary: dict,
    candidate_summary: dict,
    queries: list[str],
    component_fields: tuple[str, ...],
    max_ratio: float,
    max_us: int,
) -> list[dict[str, str]]:
    failures = []
    for query in queries:
        base = base_summary[(query, "auto")]
        candidate = candidate_summary[(query, "auto")]
        if row_int(base, "runtime_regions") == 0 or row_int(candidate, "runtime_regions") == 0:
            continue
        for field in component_fields:
            base_us = row_int(base, field)
            candidate_us = row_int(candidate, field)
            if base_us <= 0 or candidate_us <= 0:
                continue
            growth_us = candidate_us - base_us
            if growth_us > max_us and candidate_us > int(base_us * max_ratio):
                failures.append(
                    make_failure(
                        "runtime_component",
                        query,
                        f"{query_label(query)}: {field} regressed {base_us}us -> {candidate_us}us "
                        f"(+{growth_us}us)",
                    )
                )
    return failures


def print_summary(base_summary: dict, candidate_summary: dict, queries: list[str]) -> None:
    base_jitted = []
    candidate_jitted = []
    base_jitted_wins = []
    candidate_jitted_wins = []
    for query in queries:
        base = base_summary[(query, "auto")]
        candidate = candidate_summary[(query, "auto")]
        if row_int(base, "compiled_regions") > 0:
            base_jitted.append(query)
        if row_int(candidate, "compiled_regions") > 0:
            candidate_jitted.append(query)
        if is_jitted_win(base, DEFAULT_PRESERVE_WIN_SPEEDUP):
            base_jitted_wins.append(query)
        if is_jitted_win(candidate, DEFAULT_PRESERVE_WIN_SPEEDUP):
            candidate_jitted_wins.append(query)
    print(
        "TPCH JIT comparison: "
        f"jitted {len(base_jitted)} -> {len(candidate_jitted)}, "
        f"material jitted wins {len(base_jitted_wins)} -> {len(candidate_jitted_wins)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two TPC-H JIT benchmark artifact directories")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--queries", nargs="+", default=None)
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--max-auto-slowdown-ratio", type=float, default=DEFAULT_MAX_AUTO_SLOWDOWN_RATIO)
    parser.add_argument("--max-auto-slowdown-s", type=float, default=DEFAULT_MAX_AUTO_SLOWDOWN_S)
    parser.add_argument("--min-auto-speedup", type=float, default=DEFAULT_MIN_AUTO_SPEEDUP)
    parser.add_argument("--preserve-win-speedup", type=float, default=DEFAULT_PRESERVE_WIN_SPEEDUP)
    parser.add_argument("--max-win-speedup-drop", type=float, default=DEFAULT_MAX_WIN_SPEEDUP_DROP)
    parser.add_argument(
        "--fail-on-win-coverage-drop",
        action="store_true",
        help="Also fail when material JIT win count, compiled coverage, or speedup ratio drops versus baseline",
    )
    parser.add_argument(
        "--failure-report",
        type=Path,
        default=None,
        help="Optional CSV path for structured comparison failures",
    )
    parser.add_argument("--max-runtime-component-ratio", type=float, default=DEFAULT_MAX_RUNTIME_COMPONENT_RATIO)
    parser.add_argument("--max-runtime-component-us", type=int, default=DEFAULT_MAX_RUNTIME_COMPONENT_US)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_summary = keyed(read_csv(args.baseline / "summary.csv"), ("query", "policy"), "baseline summary.csv")
    candidate_summary = keyed(read_csv(args.candidate / "summary.csv"), ("query", "policy"), "candidate summary.csv")
    base_gaps = keyed(read_csv(args.baseline / "performance_gaps.csv"), ("query",), "baseline performance_gaps.csv")
    candidate_gaps = keyed(
        read_csv(args.candidate / "performance_gaps.csv"), ("query",), "candidate performance_gaps.csv"
    )
    queries = expected_queries(base_summary, candidate_summary, args.queries)
    policies = list(args.policies)
    compare_required_rows(base_summary, candidate_summary, base_gaps, candidate_gaps, queries, policies)

    failures = []
    failures.extend(compare_correctness(candidate_summary, queries, policies))
    if "auto" in policies:
        failures.extend(
            compare_auto_speed(
                base_gaps,
                candidate_gaps,
                queries,
                args.max_auto_slowdown_ratio,
                args.max_auto_slowdown_s,
                args.min_auto_speedup,
            )
        )
        failures.extend(
            compare_preserved_wins(
                base_summary,
                candidate_summary,
                base_gaps,
                candidate_gaps,
                queries,
                args.preserve_win_speedup,
                args.max_win_speedup_drop,
                args.max_auto_slowdown_ratio,
                args.max_auto_slowdown_s,
                args.fail_on_win_coverage_drop,
            )
        )
        failures.extend(
            compare_runtime_components(
                base_summary,
                candidate_summary,
                queries,
                ("runtime_time_us", "generated_runtime_time_us"),
                args.max_runtime_component_ratio,
                args.max_runtime_component_us,
            )
        )

    print_summary(base_summary, candidate_summary, queries)
    if args.failure_report is not None:
        write_failure_report(args.failure_report, failures)
    if failures:
        print("TPC-H JIT regression comparison failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure['message']}", file=sys.stderr)
        return 1
    print("TPC-H JIT regression comparison passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
