#!/usr/bin/env python3
#
# The JIT performance gate: runs each workload of a suite on the native engine and on the JIT, and checks the JIT's
# results, its counters and its cost against the thresholds checked in with the workload (see workloads.py).
#
# Every sample runs in its own shell on a read-only fixture database. It first runs the query on the native engine,
# which loads the fixture into the buffer pool and gives the result the sample must reproduce, then runs it under
# the sampled policy and measures that run. The cost of a policy is the work of its timed samples in
# performance-core cycles (see process_usage.py); one more JIT sample traces the runtime for the counter checks and
# is not timed.

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from counter_contract import counter_contract_failures, number
from duckdb_shell import DuckDBShell, Measurement, create_database, duckdb_shell_quote, repo_root
from process_usage import work_cycles
from workloads import ENCODINGS_WORKLOADS, GENERIC_WORKLOADS, TPCH_WORKLOADS

SUITES = {"generic": GENERIC_WORKLOADS, "encodings": ENCODINGS_WORKLOADS, "tpch": TPCH_WORKLOADS}
TRACED = "traced"
CORRECTNESS_SQL = (
    "SELECT (SELECT count(*) FROM (SELECT * FROM result EXCEPT ALL SELECT * FROM expected))"
    " + (SELECT count(*) FROM (SELECT * FROM expected EXCEPT ALL SELECT * FROM result)) AS correctness_diff;"
)
RUN_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "wall_time_us",
    "runnable_time_us",
    "instructions",
    "performance_core_instructions",
    "performance_core_cycles",
    "correctness_diff",
    "compiled_regions",
    "compile_errors",
    "runtime_events",
)
SUMMARY_FIELDS = (
    "workload",
    "off_cycles",
    "auto_cycles",
    "speedup",
    "floor",
    "ceiling",
    "off_wall_time_us",
    "auto_wall_time_us",
)


@dataclass(frozen=True)
class Sample:
    policy: str
    repeat: int | str
    measurement: Measurement
    correctness_diff: int
    counters: list[dict]

    def regions(self, status: str) -> int:
        return sum(number(row, "count") for row in self.counters if row["status"] == status)


def settings_sql(policy: str, threads: int, trace: bool = False) -> str:
    return (
        f"SET threads={threads}; SET enable_jit={str(policy != 'off').lower()}; SET jit_backend='sljit'; "
        f"SET jit_policy='{policy}'; SET jit_trace_runtime={str(trace).lower()}; "
        f"SET jit_trace_decisions={str(trace).lower()}; SET jit_event_log_size={10000 if trace else 0};"
    )


def run_sample(
    duckdb: Path, database: Path, workload: dict, threads: int, policy: str, repeat: int | str = TRACED
) -> Sample:
    with DuckDBShell(duckdb) as shell:
        shell.run(f".open --readonly {duckdb_shell_quote(database)}")
        shell.run(f"{settings_sql('off', threads)}\nCREATE TEMP TABLE expected AS\n{workload['sql']};")
        shell.run(settings_sql(policy, threads, repeat == TRACED))
        measurement = shell.measure(f"CREATE TEMP TABLE result AS\n{workload['sql']};")
        # an empty text counter is NULL
        counters = [
            {field: "" if value is None else value for field, value in row.items()}
            for row in shell.query("SELECT * FROM duckdb_jit_counters();")
        ]
        shell.run(settings_sql("off", threads))
        correctness_diff = int(shell.query(CORRECTNESS_SQL)[0]["correctness_diff"])
    return Sample(policy, repeat, measurement, correctness_diff, counters)


def run_workload(duckdb: Path, database: Path, workload: dict, threads: int, repeats: int) -> list[Sample]:
    """The timed samples of both policies, then the traced JIT sample. The policies alternate in which runs first, so
    neither is always the one that follows the other."""
    samples = []
    for repeat in range(1, repeats + 1):
        for policy in ("off", "auto") if repeat % 2 else ("auto", "off"):
            samples.append(run_sample(duckdb, database, workload, threads, policy, repeat))
    samples.append(run_sample(duckdb, database, workload, threads, "auto"))
    return samples


def run_row(workload: dict, sample: Sample) -> dict:
    usage = sample.measurement.usage
    return {
        "workload": workload["name"],
        "policy": sample.policy,
        "repeat": sample.repeat,
        "wall_time_us": sample.measurement.wall_time_ns // 1000,
        "runnable_time_us": usage.runnable_time_ns // 1000,
        "instructions": usage.instructions,
        "performance_core_instructions": usage.performance_core_instructions,
        "performance_core_cycles": usage.performance_core_cycles,
        "correctness_diff": sample.correctness_diff,
        "compiled_regions": sample.regions("compiled"),
        "compile_errors": sample.regions("error"),
        "runtime_events": sample.regions("executed"),
    }


def summary_row(workload: dict, threads: int, samples: list[Sample]) -> dict:
    timed = {
        policy: [sample.measurement for sample in samples if sample.policy == policy and sample.repeat != TRACED]
        for policy in ("off", "auto")
    }
    cycles = {policy: work_cycles([measurement.usage for measurement in timed[policy]]) for policy in timed}
    wall_time_us = {
        policy: int(statistics.median(measurement.wall_time_ns for measurement in timed[policy])) // 1000
        for policy in timed
    }
    return {
        "workload": workload["name"],
        "off_cycles": cycles["off"],
        "auto_cycles": cycles["auto"],
        "speedup": cycles["off"] / cycles["auto"],
        "floor": workload["floor"][threads],
        "ceiling": workload["ceiling"][threads],
        "off_wall_time_us": wall_time_us["off"],
        "auto_wall_time_us": wall_time_us["auto"],
    }


def workload_failures(workload: dict, threads: int, summary: dict, samples: list[Sample]) -> list[str]:
    compiled = workload["compiled"][threads]
    failures = []
    for sample in samples:
        label = f"{sample.policy} repeat {sample.repeat}"
        if sample.correctness_diff != 0:
            failures.append(f"{label}: {sample.correctness_diff} rows differ from the native engine's result")
        if sample.policy == "off":
            if sample.counters:
                failures.append(f"{label}: the native engine recorded JIT counters")
            continue
        if sample.regions("error") != 0:
            failures.append(f"{label}: JIT compile errors")
        if (sample.regions("compiled") > 0) != compiled:
            failures.append(f"{label}: compiled {sample.regions('compiled')} regions")
    traced = samples[-1]
    if compiled and traced.regions("executed") == 0:
        failures.append("the traced run executed no compiled region")
    for path in workload.get("required_runtime_paths", ()):
        if not any(path in row["jit_runtime_path_counts"] for row in traced.counters):
            failures.append(f"the traced run did not take runtime path {path!r}")
    failures.extend(counter_contract_failures(traced.counters))
    if summary["speedup"] < summary["floor"]:
        failures.append(f"speedup {summary['speedup']:.3f} is below the floor {summary['floor']:.3f}")
    if summary["auto_cycles"] > summary["ceiling"]:
        failures.append(f"JIT cost {summary['auto_cycles']} cycles exceeds the ceiling {summary['ceiling']}")
    return [f"{workload['name']}: {failure}" for failure in failures]


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=tuple(SUITES))
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--workloads", nargs="+", help="run only these workloads of the suite")
    parser.add_argument("--repeats", type=int, default=5, help="timed samples per policy")
    parser.add_argument("--out-dir", type=Path, help="where to write runs.csv, summary.csv and counters.json")
    parser.add_argument("--duckdb", type=Path, default=repo_root() / "build" / "reldebug" / "duckdb")
    args = parser.parse_args(argv)
    # a suite runs the workloads qualified for the thread count; a named workload must be qualified
    names = args.workloads or [workload["name"] for workload in SUITES[args.suite] if args.threads in workload["floor"]]
    workloads = {workload["name"]: workload for workload in SUITES[args.suite]}
    if not names:
        parser.error(f"no {args.suite} workloads are qualified on {args.threads} threads")
    unknown = [name for name in names if name not in workloads]
    if unknown:
        parser.error(f"unknown {args.suite} workloads: {', '.join(unknown)}")
    args.workloads = [workloads[name] for name in workloads if name in names]
    unqualified = [workload["name"] for workload in args.workloads if args.threads not in workload["floor"]]
    if unqualified:
        parser.error(f"no thresholds on {args.threads} threads for: {', '.join(unqualified)}")
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir = args.out_dir or Path(tempfile.mkdtemp(prefix=f"duckdb_jit_{args.suite}_gate_"))
    out_dir.mkdir(parents=True, exist_ok=True)
    database = out_dir / "fixture.duckdb"
    runs, summaries, counters, failures = [], [], [], []
    setup_sql = None
    try:
        for workload in args.workloads:
            if workload.get("setup_sql", "") != setup_sql:
                setup_sql = workload.get("setup_sql", "")
                create_database(args.duckdb, database, setup_sql)
            samples = run_workload(args.duckdb, database, workload, args.threads, args.repeats)
            summary = summary_row(workload, args.threads, samples)
            workload_failed = workload_failures(workload, args.threads, summary, samples)
            runs.extend(run_row(workload, sample) for sample in samples)
            summaries.append(summary)
            counters.extend(
                {"workload": workload["name"], "policy": sample.policy, "repeat": sample.repeat, **row}
                for sample in samples
                for row in sample.counters
            )
            failures.extend(workload_failed)
            print(
                f"{workload['name']}: speedup {summary['speedup']:.3f} (floor {summary['floor']:.3f}), "
                f"JIT {summary['auto_cycles']} cycles (ceiling {summary['ceiling']}), "
                f"wall {summary['off_wall_time_us']} -> {summary['auto_wall_time_us']} us"
                + "".join(f"\n  FAILED {failure}" for failure in workload_failed),
                flush=True,
            )
    finally:
        database.unlink(missing_ok=True)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, runs)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summaries)
        (out_dir / "counters.json").write_text(json.dumps(counters, indent=1) + "\n", encoding="utf-8")
        print(f"wrote {out_dir}")
    for failure in failures:
        print(f"FAILED {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
