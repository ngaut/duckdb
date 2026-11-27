#!/usr/bin/env python3
#
# Compares two builds on the performance gates' workloads. A gate run measures every workload five times per policy,
# but a change moves few of them, and instruction counts barely move between runs of one build. So the comparison
# first screens every workload once on each build, and keeps those whose counts differ: instructions on either engine
# past --instruction-tolerance, cycles past --cycle-tolerance, or the number of compiled regions. It then measures only
# those, the two builds alternating, for --rounds rounds, and reports each build's speedups.
#
# Both builds read fixtures the base build writes (kept in the gate's fixture cache), so they scan the same data; a
# change to how storage writes data compares with --fixtures-per-build.
#
# --apply moves the thresholds in workloads.py to the rounds: a ceiling falls to 5% above the new build's highest JIT
# instruction count; a floor rises to 90% of its lowest speedup. A floor falls to that only where the native engine
# got faster in every round (median at least 5% fewer cycles), which lowers the speedup without the JIT doing worse.

from __future__ import annotations

import argparse
import math
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

from duckdb_shell import repo_root
from performance_gate import (
    SUITES,
    default_fixture_cache,
    fixture_database,
    run_workload,
    summary_row,
    workload_failures,
)

PERFORMANCE_GATES = (("generic", 1), ("generic", 4), ("encodings", 1), ("encodings", 4), ("tpch", 1))
WORKLOADS_FILE = Path(__file__).resolve().parent / "workloads.py"


@dataclass
class Comparison:
    suite: str
    threads: int
    workload: dict
    reasons: list[str] = field(default_factory=list)
    # per round, the summary of each build
    base: list[dict] = field(default_factory=list)
    new: list[dict] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self.workload["name"]


def ratio(new: float, base: float) -> float:
    return new / base if base else math.inf


def screen_reasons(base: dict, new: dict, args: argparse.Namespace) -> list[str]:
    reasons = []
    for counter, tolerance in (
        ("auto_instructions", args.instruction_tolerance),
        ("off_instructions", args.instruction_tolerance),
        ("auto_cycles", args.cycle_tolerance),
        ("off_cycles", args.cycle_tolerance),
    ):
        change = ratio(new[counter], base[counter]) - 1
        if abs(change) > tolerance:
            reasons.append(f"{counter} {change:+.1%}")
    if new["compiled_regions"] != base["compiled_regions"]:
        reasons.append(f"compiled regions {base['compiled_regions']} -> {new['compiled_regions']}")
    return reasons


def measure(build: Path, fixture: Path, comparison: Comparison, repeats: int) -> dict:
    samples = run_workload(build, fixture, comparison.workload, comparison.threads, repeats)
    summary = summary_row(comparison.workload, comparison.threads, samples)
    summary["compiled_regions"] = samples[-1].regions("compiled")
    summary["failures"] = workload_failures(comparison.workload, comparison.threads, summary, samples)
    return summary


def thresholds(comparison: Comparison) -> tuple[float, int]:
    floor = comparison.workload["floor"][comparison.threads]
    ceiling = comparison.workload["ceiling"][comparison.threads]
    speedups = [summary["speedup"] for summary in comparison.new]
    derived = math.floor(0.9 * min(speedups) * 100) / 100
    native_faster = all(new["off_cycles"] < base["off_cycles"] for new, base in zip(comparison.new, comparison.base))
    native_change = statistics.median(s["off_cycles"] for s in comparison.new) / statistics.median(
        s["off_cycles"] for s in comparison.base
    )
    # only a speedup the faster native engine pushed below the floor re-derives it
    new_floor = derived if native_faster and native_change <= 0.95 and min(speedups) < floor else max(floor, derived)
    highest = max(summary["auto_instructions"] for summary in comparison.new)
    new_ceiling = min(ceiling, math.ceil(1.05 * highest / 100_000) * 100_000)
    return new_floor, new_ceiling


def report(comparison: Comparison) -> str:
    def median(build: list[dict], counter: str) -> float:
        return statistics.median(summary[counter] for summary in build)

    floor, ceiling = thresholds(comparison)
    old_floor = comparison.workload["floor"][comparison.threads]
    return (
        f"{comparison.suite:9} {comparison.name:34} t{comparison.threads} "
        f"speedup {median(comparison.base, 'speedup'):.2f} -> {median(comparison.new, 'speedup'):.2f} "
        f"(min {min(s['speedup'] for s in comparison.new):.2f}) "
        f"native {ratio(median(comparison.new, 'off_cycles'), median(comparison.base, 'off_cycles')) - 1:+.0%} "
        f"jit {ratio(median(comparison.new, 'auto_cycles'), median(comparison.base, 'auto_cycles')) - 1:+.0%} "
        f"jit instructions {ratio(median(comparison.new, 'auto_instructions'), median(comparison.base, 'auto_instructions')) - 1:+.1%} "
        f"floor {old_floor:.2f} -> {floor:.2f} ceiling -> {ceiling:_} [{'; '.join(comparison.reasons)}]"
    )


def apply_thresholds(comparisons: list[Comparison]) -> None:
    text = WORKLOADS_FILE.read_text(encoding="utf-8")
    for comparison in comparisons:
        floor, ceiling = thresholds(comparison)
        floors = dict(comparison.workload["floor"])
        ceilings = dict(comparison.workload["ceiling"])
        floors[comparison.threads] = floor
        ceilings[comparison.threads] = ceiling
        comparison.workload["floor"], comparison.workload["ceiling"] = floors, ceilings
        floor_text = "{" + ", ".join(f"{t}: {floors[t]:.2f}" for t in sorted(floors)) + "}"
        ceiling_text = "{" + ", ".join(f"{t}: {ceilings[t]:_}" for t in sorted(ceilings)) + "}"
        name = re.escape(comparison.name)
        if comparison.suite == "generic":
            pattern = re.compile(
                rf'("name": "{name}",\n(?:.*\n)*?\s*"floor": )\{{[^}}]*\}}(,\n\s*"ceiling": )\{{[^}}]*\}}'
            )
            text, count = pattern.subn(lambda m: m.group(1) + floor_text + m.group(2) + ceiling_text, text, count=1)
        elif comparison.suite == "encodings":
            pattern = re.compile(rf'(\n    "{name}": \()\{{[^}}]*\}}, \{{[^}}]*\}}(\),)')
            text, count = pattern.subn(lambda m: m.group(1) + floor_text + ", " + ceiling_text + m.group(2), text)
        else:
            pattern = re.compile(rf'(\n    "{re.escape(comparison.name[1:])}": \()[0-9.]+, [0-9_]+(\),)')
            text, count = pattern.subn(lambda m: m.group(1) + f"{floor:.2f}, {ceiling:_}" + m.group(2), text)
        if count != 1:
            raise SystemExit(f"error: could not find the thresholds of {comparison.suite} {comparison.name}")
    WORKLOADS_FILE.write_text(text, encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument("--base", type=Path, required=True, help="the build to compare against")
    parser.add_argument("--new", type=Path, default=repo_root() / "build" / "reldebug" / "duckdb")
    parser.add_argument(
        "--gates",
        nargs="+",
        default=[f"{suite}:{threads}" for suite, threads in PERFORMANCE_GATES],
        help="suite:threads pairs to compare",
    )
    parser.add_argument("--workloads", nargs="+", help="compare only these workloads, without screening")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5, help="counted samples per policy in a round")
    parser.add_argument("--instruction-tolerance", type=float, default=0.02)
    parser.add_argument("--cycle-tolerance", type=float, default=0.15)
    parser.add_argument("--fixtures-per-build", action="store_true")
    parser.add_argument("--fixture-cache", type=Path, default=default_fixture_cache())
    parser.add_argument("--apply", action="store_true", help="move the thresholds in workloads.py to the rounds")
    args = parser.parse_args(argv)
    args.gates = [(gate.split(":")[0], int(gate.split(":")[1])) for gate in args.gates]
    for suite, _ in args.gates:
        if suite not in SUITES:
            parser.error(f"unknown suite {suite}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    comparisons = []

    def fixture(build: Path, comparison: Comparison) -> Path:
        writer = build if args.fixtures_per_build else args.base
        return fixture_database(args.fixture_cache, writer, comparison.workload.get("setup_sql", ""))

    for suite, threads in args.gates:
        for workload in SUITES[suite]:
            if threads not in workload["floor"] or (args.workloads and workload["name"] not in args.workloads):
                continue
            comparison = Comparison(suite, threads, workload)
            if args.workloads:
                comparison.reasons.append("named")
            else:
                base = measure(args.base, fixture(args.base, comparison), comparison, 1)
                new = measure(args.new, fixture(args.new, comparison), comparison, 1)
                comparison.reasons = screen_reasons(base, new, args) + [f"FAILS {f}" for f in new["failures"]]
                print(
                    f"screen {suite} {workload['name']} t{threads}: {'; '.join(comparison.reasons) or 'same'}",
                    flush=True,
                )
            if comparison.reasons:
                comparisons.append(comparison)
    for round_idx in range(args.rounds):
        for comparison in comparisons:
            builds = [("base", args.base), ("new", args.new)]
            for label, build in builds if round_idx % 2 == 0 else reversed(builds):
                getattr(comparison, label).append(measure(build, fixture(build, comparison), comparison, args.repeats))
        print(f"round {round_idx + 1} of {args.rounds} done", flush=True)
    print(f"{len(comparisons)} workloads differ:")
    for comparison in comparisons:
        print(report(comparison) if comparison.new else f"{comparison.suite} {comparison.name} t{comparison.threads}")
        for failure in comparison.new[-1]["failures"] if comparison.new else []:
            print(f"  FAILED {failure}")
    if args.apply and args.rounds > 0:
        apply_thresholds(comparisons)
        print(f"updated {WORKLOADS_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
