#!/usr/bin/env python3
#
# Run benchmark-runner timings for SLJIT diagnostic shapes that are not auto-admitted.

import argparse
import csv
import statistics
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import default_trace_output_directory, prepare_trace_output_directory, write_trace_manifest
from micro_jit_manifest import DIAGNOSTIC_POLICIES as POLICIES, diagnostic_benchmark_shapes


DIAGNOSTIC_SHAPES = diagnostic_benchmark_shapes()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_benchmark(benchmark_runner: Path, benchmark_file: str) -> list:
    result = subprocess.run(
        [str(benchmark_runner), "--disable-timeout", benchmark_file],
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
    for row in rows:
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
        raise RuntimeError(
            f"benchmark_runner produced no timing rows for {benchmark_file}\n"
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


def collect_runs(args: argparse.Namespace) -> tuple[list, list]:
    run_rows = []
    summary_rows = []
    for shape in DIAGNOSTIC_SHAPES:
        by_policy = {}
        for policy in args.policies:
            benchmark_file = shape["benchmarks"][policy]
            timings = run_benchmark(args.benchmark_runner, benchmark_file)
            timing_values = [entry["timing_s"] for entry in timings]
            for entry in timings:
                run_rows.append(
                    {
                        "shape": shape["shape"],
                        "family": shape["family"],
                        "size": shape["size"],
                        "policy": policy,
                        "shape_key": shape["shape_key"],
                        "benchmark_file": benchmark_file,
                        "benchmark_name": entry["benchmark_name"],
                        "run": entry["run"],
                        "timing_s": f"{entry['timing_s']:.9f}",
                    }
                )
            by_policy[policy] = {
                "benchmark_file": benchmark_file,
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
                    "shape": shape["shape"],
                    "family": shape["family"],
                    "size": shape["size"],
                    "policy": policy,
                    "shape_key": shape["shape_key"],
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


def write_manifest(args: argparse.Namespace, out_dir: Path) -> None:
    write_trace_manifest(
        out_dir,
        kind="micro_jit_diagnostic_benchmark",
        generator="benchmark/micro/jit/micro_jit_diagnostic_benchmark.py",
        configuration={
            "benchmark_runner": str(args.benchmark_runner),
            "policies": list(args.policies),
            "shapes": [shape["shape"] for shape in DIAGNOSTIC_SHAPES],
        },
        artifact_names=["runs.csv", "summary.csv"],
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Run JIT microbenchmark diagnostic timings")
    parser.add_argument(
        "--benchmark-runner",
        type=Path,
        default=root / "build" / "release" / "benchmark" / "benchmark_runner",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.benchmark_runner = args.benchmark_runner.resolve()
    if not args.benchmark_runner.exists():
        raise RuntimeError(f"benchmark_runner does not exist: {args.benchmark_runner}")
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("micro_diagnostic_benchmark")
    out_dir = prepare_trace_output_directory(args.out_dir)
    run_rows, summary_rows = collect_runs(args)
    write_csv(out_dir / "runs.csv", run_rows)
    write_csv(out_dir / "summary.csv", summary_rows)
    write_manifest(args, out_dir)
    print(f"diagnostic benchmark output: {out_dir}")
    print(f"runs: {out_dir / 'runs.csv'}")
    print(f"summary: {out_dir / 'summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
