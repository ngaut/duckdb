#!/usr/bin/env python3
#
# Sweep cardinalities for unadmitted SLJIT source-prefix diagnostic shapes.

import argparse
import csv
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import default_trace_output_directory, prepare_trace_output_directory, write_trace_manifest
from micro_jit_manifest import (
    DEFAULT_SWEEP_ROW_COUNTS as DEFAULT_ROW_COUNTS,
    DIAGNOSTIC_POLICIES as POLICIES,
    MIN_ADMITTED_SPEEDUP,
    diagnostic_sweep_shapes,
)


SHAPES = diagnostic_sweep_shapes()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def benchmark_text(shape: dict, row_count: int, policy: str, path: Path) -> str:
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
                "                    WHERE target='region'",
                "                      AND status='unsupported'",
                "                      AND execution_mode='unsupported'",
                "                      AND region_execution_form='none'",
                "                      AND policy_decision='force'",
                f"                      AND candidate_shape='{shape['candidate_shape']}'",
                "                      AND candidate_scope='source_prefix'",
                f"                      AND admission_shape_key='{shape['shape_key']}'",
                "                      AND code_size=0",
                "                      AND reason LIKE '%execution:unsupported%'",
                "                      AND reason LIKE '%source-fusion-gap:requires-native-source%'",
                "                      AND reason LIKE '%DuckDB source boundary%'",
                "                )",
                "                AND",
                "                (",
                "                    SELECT count(*) = 0",
                "                    FROM duckdb_jit_events()",
                "                    WHERE target='region'",
                "                      AND status='compiled'",
                "                      AND execution_mode <> 'native'",
                "                      AND candidate_scope='source_prefix'",
                "                )",
                "                AND",
                "                (",
                "                    SELECT count(*) > 0",
                "                    FROM duckdb_jit_events()",
                "                    WHERE target='region'",
                "                      AND status='compiled'",
                "                      AND execution_mode='native'",
                "                      AND region_execution_form='fused'",
                "                      AND policy_decision='force'",
                "                      AND candidate_scope IN ('post_source_operator_interval', 'sink_pipeline')",
                "                      AND code_size > 0",
                "                )",
                "            )",
                "        )",
            ]
        )
    )
    return f"""# name: {path.as_posix()}
# description: Diagnostic sweep for {shape['family']} row_count={row_count} policy={policy}
# group: [jit]

name JIT Diagnostic Sweep {shape['family']} {row_count} {policy}
group micro
subgroup jit

require jit_sljit

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
{shape['query'](row_count)};

cleanup
SELECT * FROM duckdb_jit_clear_events();

result_query I
SELECT (
    (SELECT * FROM __answer) = {shape['expected'](row_count)}
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


def write_benchmark_file(out_dir: Path, shape: dict, row_count: int, policy: str) -> Path:
    benchmark_dir = out_dir / "benchmark" / "micro" / "jit"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    path = benchmark_dir / f"{shape['family']}_{row_count}_{policy}.benchmark"
    path.write_text(benchmark_text(shape, row_count, policy, path), encoding="utf-8")
    return path


def collect_runs(args: argparse.Namespace, out_dir: Path) -> tuple[list, list]:
    run_rows = []
    summary_rows = []
    for shape in SHAPES:
        for row_count in args.row_counts:
            by_policy = {}
            for policy in args.policies:
                benchmark_file = write_benchmark_file(out_dir, shape, row_count, policy)
                timings = run_benchmark(args.benchmark_runner, out_dir, benchmark_file)
                benchmark_artifact = f"generated_{shape['family']}_{row_count}_{policy}.benchmark"
                benchmark_file.replace(out_dir / benchmark_artifact)
                timing_values = [entry["timing_s"] for entry in timings]
                for entry in timings:
                    run_rows.append(
                        {
                            "family": shape["family"],
                            "row_count": row_count,
                            "policy": policy,
                            "shape_key": shape["shape_key"],
                            "candidate_shape": shape["candidate_shape"],
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
                        "family": shape["family"],
                        "row_count": row_count,
                        "policy": policy,
                        "shape_key": shape["shape_key"],
                        "candidate_shape": shape["candidate_shape"],
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


def cleanup_runner_directories(out_dir: Path) -> None:
    shutil.rmtree(out_dir / "benchmark", ignore_errors=True)
    shutil.rmtree(out_dir / "duckdb_benchmark_data", ignore_errors=True)


def collect_family_summary(summary_rows: list) -> list:
    result = []
    by_family = {}
    for row in summary_rows:
        if row["policy"] != "force":
            continue
        by_family.setdefault(row["family"], []).append(row)
    for family, rows in sorted(by_family.items()):
        rows.sort(key=lambda row: int(row["row_count"]))
        first_admitted = ""
        best_row = max(rows, key=lambda row: float(row["speedup_vs_off"]))
        for row in rows:
            if float(row["speedup_vs_off"]) >= MIN_ADMITTED_SPEEDUP:
                first_admitted = row["row_count"]
                break
        result.append(
            {
                "family": family,
                "shape_key": rows[0]["shape_key"],
                "candidate_shape": rows[0]["candidate_shape"],
                "min_admitted_speedup": f"{MIN_ADMITTED_SPEEDUP:.6f}",
                "first_row_count_at_margin": first_admitted,
                "best_row_count": best_row["row_count"],
                "best_speedup_vs_off": best_row["speedup_vs_off"],
                "best_median_s": best_row["median_s"],
                "threshold_row_count": "1000000",
                "threshold_speedup_vs_off": next(
                    row["speedup_vs_off"] for row in rows if int(row["row_count"]) == 1000000
                ),
            }
        )
    return result


def write_manifest(args: argparse.Namespace, out_dir: Path, benchmark_files: list) -> None:
    write_trace_manifest(
        out_dir,
        kind="micro_jit_diagnostic_sweep",
        generator="benchmark/micro/jit/micro_jit_diagnostic_sweep.py",
        configuration={
            "benchmark_runner": str(args.benchmark_runner),
            "policies": list(args.policies),
            "row_counts": list(args.row_counts),
            "families": [shape["family"] for shape in SHAPES],
        },
        artifact_names=["runs.csv", "summary.csv", "family_summary.csv"] + benchmark_files,
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Sweep JIT diagnostic cardinalities")
    parser.add_argument(
        "--benchmark-runner",
        type=Path,
        default=root / "build" / "release" / "benchmark" / "benchmark_runner",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    parser.add_argument("--row-counts", nargs="+", type=int, default=list(DEFAULT_ROW_COUNTS))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.benchmark_runner = args.benchmark_runner.resolve()
    if not args.benchmark_runner.exists():
        raise RuntimeError(f"benchmark_runner does not exist: {args.benchmark_runner}")
    args.row_counts = tuple(sorted(set(args.row_counts)))
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("micro_diagnostic_sweep")
    out_dir = prepare_trace_output_directory(args.out_dir)
    run_rows, summary_rows = collect_runs(args, out_dir)
    family_summary_rows = collect_family_summary(summary_rows)
    cleanup_runner_directories(out_dir)
    write_csv(out_dir / "runs.csv", run_rows)
    write_csv(out_dir / "summary.csv", summary_rows)
    write_csv(out_dir / "family_summary.csv", family_summary_rows)
    benchmark_files = sorted(row["benchmark_file"] for row in summary_rows)
    write_manifest(args, out_dir, benchmark_files)
    print(f"diagnostic sweep output: {out_dir}")
    print(f"runs: {out_dir / 'runs.csv'}")
    print(f"summary: {out_dir / 'summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
