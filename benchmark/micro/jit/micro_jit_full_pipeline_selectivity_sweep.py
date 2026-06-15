#!/usr/bin/env python3
#
# Sweep selectivity for the native full-pipeline decimal filter/projection/ungrouped-SUM shape.

import argparse
import csv
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import default_trace_output_directory, prepare_trace_output_directory, write_trace_manifest
from micro_jit_manifest import DIAGNOSTIC_POLICIES as POLICIES, MIN_ADMITTED_SPEEDUP


FAMILY = "native_full_pipeline_decimal_projection_ungrouped_sum"
SHAPE_KEY = "sljit:full-pipeline:filter-projection-ungrouped-aggregate-update"
CANDIDATE_SHAPE = "scan-filter-scan-project-projection-sink"
DEFAULT_ROW_COUNT = 20000000
DEFAULT_SELECTIVITY_PERCENT = (1, 2, 5, 10, 25, 50, 75)
PERIOD = 700
PERIOD_VALUES = [((idx % 100) + 1) * ((idx % 7) + 1) for idx in range(PERIOD)]
PERIOD_PREFIX = [0]
for value in PERIOD_VALUES:
    PERIOD_PREFIX.append(PERIOD_PREFIX[-1] + value)
PERIOD_SUM = PERIOD_PREFIX[-1]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def prefix_sum(row_count: int) -> int:
    periods, remainder = divmod(row_count, PERIOD)
    return periods * PERIOD_SUM + PERIOD_PREFIX[remainder]


def output_count(row_count: int, selectivity_percent: int) -> int:
    return max(1, row_count * selectivity_percent // 100)


def filter_bound(row_count: int, selectivity_percent: int) -> int:
    return row_count - output_count(row_count, selectivity_percent) - 1


def expected_result(row_count: int, selectivity_percent: int) -> str:
    lower = filter_bound(row_count, selectivity_percent) + 1
    total = prefix_sum(row_count) - prefix_sum(lower)
    return f"{total}.0000"


def benchmark_text(row_count: int, selectivity_percent: int, policy: str, path: Path) -> str:
    bound = filter_bound(row_count, selectivity_percent)
    event_check = (
        "(\n"
        "            SELECT count(*) = 0\n"
        "            FROM duckdb_jit_events()\n"
        "            WHERE target='region'\n"
        "              AND status='compiled'\n"
        "        )"
        if policy == "off"
        else "\n".join(
            [
                "(",
                "            SELECT (",
                "                (",
                "                    SELECT count(*) = 1",
                "                    FROM duckdb_jit_events()",
                "                    WHERE phase='compile'",
                "                      AND target='region'",
                "                      AND status='compiled'",
                "                      AND execution_mode='native'",
                "                      AND region_execution_form='fused'",
                "                      AND selected_source_execution='native-source'",
                "                      AND policy_decision='force'",
                f"                      AND candidate_shape='{CANDIDATE_SHAPE}'",
                "                      AND candidate_scope='full_pipeline'",
                "                      AND candidate_source_filter_count=1",
                "                      AND candidate_source_integer_comparison_filter_count=1",
                "                      AND candidate_projection_count=1",
                "                      AND candidate_sink_kind='ungrouped-aggregate-update'",
                f"                      AND admission_shape_key='{SHAPE_KEY}'",
                "                      AND admission_rule_present=false",
                "                      AND code_size > 0",
                "                      AND reason LIKE '%generated source-prefix table scan filters%'",
                "                      AND reason LIKE '%source-strategy=prepared-unfiltered-native-source%'",
                "                      AND reason LIKE '%owns-source-filters=true%'",
                "                      AND reason LIKE '%source:TABLE_SCAN:native:generated source-prefix table scan filters%'",
                "                      AND reason LIKE '%op0:PROJECTION:native:generated typed projection%'",
                "                      AND reason LIKE '%sink:UNGROUPED_AGGREGATE:native:generated native ungrouped aggregate state update%'",
                "                )",
                "                AND",
                "                (",
                "                    SELECT count(*) = 0",
                "                    FROM duckdb_jit_events()",
                "                    WHERE phase='compile'",
                "                      AND target='region'",
                "                      AND execution_mode <> 'native'",
                "                )",
                "            )",
                "        )",
            ]
        )
    )
    return f"""# name: {path.as_posix()}
# description: Full-pipeline decimal projection/ungrouped-SUM selectivity={selectivity_percent} policy={policy}
# group: [jit]

name JIT Full Pipeline Selectivity {selectivity_percent} {policy}
group micro
subgroup jit

require jit_sljit

load
CREATE TABLE jit_full_pipeline_selectivity AS
SELECT i::BIGINT AS i,
       ((i % 100) + 1)::DECIMAL(18,2) AS extendedprice,
       ((i % 7) + 1)::DECIMAL(18,2) AS discount
FROM range({row_count}) tbl(i);

init
SET threads=1;
SET enable_jit=true;
SET jit_backend='sljit';
SET jit_policy='{policy}';
SET jit_dump_ir=false;
SET jit_trace_runtime=false;
SET jit_event_log_size=10000;
SELECT * FROM duckdb_jit_clear_events();

run
SELECT sum(extendedprice * discount)
FROM jit_full_pipeline_selectivity
WHERE i > {bound};

cleanup
SELECT * FROM duckdb_jit_clear_events();

result_query I
SELECT (
    (SELECT * FROM __answer) = {expected_result(row_count, selectivity_percent)}
    AND {event_check}
)::INTEGER;
----
1
"""


def run_benchmark(benchmark_runner: Path, root_dir: Path, benchmark_file: Path) -> list:
    benchmark_name = benchmark_file.relative_to(root_dir).as_posix()
    result = subprocess.run(
        [str(benchmark_runner), "--disable-timeout", "--root-dir", str(root_dir), benchmark_name],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"benchmark_runner failed for {benchmark_file} with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    output = result.stdout if result.stdout.strip() else result.stderr
    rows = list(csv.DictReader(output.splitlines(), delimiter="\t"))
    timings = []
    incorrect_rows = []
    for row in rows:
        if row.get("timing", "") == "INCORRECT":
            incorrect_rows.append(row)
            continue
        if row.get("timing", "") == "":
            continue
        timings.append(
            {
                "benchmark_name": row["name"],
                "run": int(row["run"]),
                "timing_s": float(row["timing"]),
            }
        )
    if not timings:
        detail = "incorrect result rows were reported" if incorrect_rows else "no timing rows were reported"
        raise RuntimeError(
            f"benchmark_runner produced no usable timing rows for {benchmark_file}: {detail}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return timings


def write_csv(path: Path, rows: list) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_benchmark_file(out_dir: Path, row_count: int, selectivity_percent: int, policy: str) -> Path:
    benchmark_dir = out_dir / "benchmark" / "micro" / "jit"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    path = benchmark_dir / f"{FAMILY}_{row_count}_{selectivity_percent}_{policy}.benchmark"
    path.write_text(benchmark_text(row_count, selectivity_percent, policy, path), encoding="utf-8")
    return path


def collect_runs(args: argparse.Namespace, out_dir: Path) -> tuple[list, list]:
    run_rows = []
    summary_rows = []
    for row_count in args.row_counts:
        for selectivity_percent in args.selectivity_percent:
            by_policy = {}
            for policy in args.policies:
                benchmark_file = write_benchmark_file(out_dir, row_count, selectivity_percent, policy)
                timings = run_benchmark(args.benchmark_runner, out_dir, benchmark_file)
                benchmark_artifact = f"generated_{FAMILY}_{row_count}_{selectivity_percent}_{policy}.benchmark"
                benchmark_file.replace(out_dir / benchmark_artifact)
                timing_values = [entry["timing_s"] for entry in timings]
                for entry in timings:
                    run_rows.append(
                        {
                            "family": FAMILY,
                            "row_count": row_count,
                            "selectivity_percent": selectivity_percent,
                            "output_count": output_count(row_count, selectivity_percent),
                            "filter_bound": filter_bound(row_count, selectivity_percent),
                            "policy": policy,
                            "shape_key": SHAPE_KEY,
                            "candidate_shape": CANDIDATE_SHAPE,
                            "benchmark_file": benchmark_artifact,
                            "benchmark_name": entry["benchmark_name"],
                            "run": entry["run"],
                            "timing_s": f"{entry['timing_s']:.9f}",
                        }
                    )
                by_policy[policy] = {
                    "benchmark_file": benchmark_artifact,
                    "run_count": len(timing_values),
                    "min_s": min(timing_values),
                    "median_s": statistics.median(timing_values),
                    "mean_s": statistics.fmean(timing_values),
                    "max_s": max(timing_values),
                    "timings_s": ";".join(f"{value:.9f}" for value in timing_values),
                }
            off_median = by_policy["off"]["median_s"]
            for policy in args.policies:
                entry = by_policy[policy]
                speedup = off_median / entry["median_s"] if entry["median_s"] > 0 else 0
                summary_rows.append(
                    {
                        "family": FAMILY,
                        "row_count": row_count,
                        "selectivity_percent": selectivity_percent,
                        "output_count": output_count(row_count, selectivity_percent),
                        "filter_bound": filter_bound(row_count, selectivity_percent),
                        "policy": policy,
                        "shape_key": SHAPE_KEY,
                        "candidate_shape": CANDIDATE_SHAPE,
                        "benchmark_file": entry["benchmark_file"],
                        "run_count": entry["run_count"],
                        "min_s": f"{entry['min_s']:.9f}",
                        "median_s": f"{entry['median_s']:.9f}",
                        "mean_s": f"{entry['mean_s']:.9f}",
                        "max_s": f"{entry['max_s']:.9f}",
                        "speedup_vs_off": f"{speedup:.6f}",
                        "faster_than_off": "true" if policy == "off" or entry["median_s"] < off_median else "false",
                        "timings_s": entry["timings_s"],
                    }
                )
    return run_rows, summary_rows


def collect_selectivity_summary(summary_rows: list) -> list:
    force_rows = [row for row in summary_rows if row["policy"] == "force"]
    best_row = max(force_rows, key=lambda row: float(row["speedup_vs_off"]))
    first_at_margin = ""
    for row in sorted(force_rows, key=lambda entry: int(entry["selectivity_percent"])):
        if float(row["speedup_vs_off"]) >= MIN_ADMITTED_SPEEDUP:
            first_at_margin = row["selectivity_percent"]
            break
    return [
        {
            "family": FAMILY,
            "shape_key": SHAPE_KEY,
            "candidate_shape": CANDIDATE_SHAPE,
            "min_admitted_speedup": f"{MIN_ADMITTED_SPEEDUP:.6f}",
            "first_selectivity_percent_at_margin": first_at_margin,
            "best_selectivity_percent": best_row["selectivity_percent"],
            "best_output_count": best_row["output_count"],
            "best_speedup_vs_off": best_row["speedup_vs_off"],
            "best_median_s": best_row["median_s"],
        }
    ]


def cleanup_runner_directories(out_dir: Path) -> None:
    shutil.rmtree(out_dir / "benchmark", ignore_errors=True)
    shutil.rmtree(out_dir / "duckdb_benchmark_data", ignore_errors=True)


def write_manifest(args: argparse.Namespace, out_dir: Path, benchmark_files: list) -> None:
    write_trace_manifest(
        out_dir,
        kind="micro_jit_full_pipeline_selectivity_sweep",
        generator="benchmark/micro/jit/micro_jit_full_pipeline_selectivity_sweep.py",
        configuration={
            "benchmark_runner": str(args.benchmark_runner),
            "policies": list(args.policies),
            "row_counts": list(args.row_counts),
            "selectivity_percent": list(args.selectivity_percent),
            "family": FAMILY,
        },
        artifact_names=["runs.csv", "summary.csv", "selectivity_summary.csv"] + benchmark_files,
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Sweep full-pipeline JIT selectivity")
    parser.add_argument(
        "--benchmark-runner",
        type=Path,
        default=root / "build" / "release" / "benchmark" / "benchmark_runner",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    parser.add_argument("--row-counts", nargs="+", type=int, default=[DEFAULT_ROW_COUNT])
    parser.add_argument(
        "--selectivity-percent",
        nargs="+",
        type=int,
        default=list(DEFAULT_SELECTIVITY_PERCENT),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.benchmark_runner = args.benchmark_runner.resolve()
    if not args.benchmark_runner.exists():
        raise RuntimeError(f"benchmark_runner does not exist: {args.benchmark_runner}")
    args.row_counts = tuple(sorted(set(args.row_counts)))
    args.selectivity_percent = tuple(sorted(set(args.selectivity_percent)))
    for selectivity_percent in args.selectivity_percent:
        if selectivity_percent <= 0 or selectivity_percent >= 100:
            raise RuntimeError(f"selectivity percent must be between 1 and 99: {selectivity_percent}")
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("micro_full_pipeline_selectivity")
    out_dir = prepare_trace_output_directory(args.out_dir)
    run_rows, summary_rows = collect_runs(args, out_dir)
    selectivity_summary_rows = collect_selectivity_summary(summary_rows)
    cleanup_runner_directories(out_dir)
    write_csv(out_dir / "runs.csv", run_rows)
    write_csv(out_dir / "summary.csv", summary_rows)
    write_csv(out_dir / "selectivity_summary.csv", selectivity_summary_rows)
    benchmark_files = sorted(row["benchmark_file"] for row in summary_rows)
    write_manifest(args, out_dir, benchmark_files)
    print(f"full-pipeline selectivity output: {out_dir}")
    print(f"runs: {out_dir / 'runs.csv'}")
    print(f"summary: {out_dir / 'summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
