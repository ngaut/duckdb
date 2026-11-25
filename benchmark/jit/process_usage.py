#!/usr/bin/env python3
#
# The work a process did, read from the kernel's per-process accounting: the instructions it retired, and the cycles
# its threads spent on all cores and on the performance cores. Instructions do not depend on the host at all. The
# cycles of a query are its instructions at the cycles per instruction of the cores it ran on, which a busy host moves
# much less than it moves wall time: it slows a core's clock and makes threads wait for a core, and neither adds cycles.

from __future__ import annotations

import ctypes
import ctypes.util
import functools
import math
import statistics
import sys
from dataclasses import dataclass

RUSAGE_INFO_V6 = 6
# struct rusage_info_v6 of <sys/resource.h>: a 16-byte UUID followed by 64-bit counters
RUSAGE_INFO_V6_COUNTERS = (
    "user_time",
    "system_time",
    "pkg_idle_wkups",
    "interrupt_wkups",
    "pageins",
    "wired_size",
    "resident_size",
    "phys_footprint",
    "proc_start_abstime",
    "proc_exit_abstime",
    "child_user_time",
    "child_system_time",
    "child_pkg_idle_wkups",
    "child_interrupt_wkups",
    "child_pageins",
    "child_elapsed_abstime",
    "diskio_bytesread",
    "diskio_byteswritten",
    "cpu_time_qos_default",
    "cpu_time_qos_maintenance",
    "cpu_time_qos_background",
    "cpu_time_qos_utility",
    "cpu_time_qos_legacy",
    "cpu_time_qos_user_initiated",
    "cpu_time_qos_user_interactive",
    "billed_system_time",
    "serviced_system_time",
    "logical_writes",
    "lifetime_max_phys_footprint",
    "instructions",
    "cycles",
    "billed_energy",
    "serviced_energy",
    "interval_max_phys_footprint",
    "runnable_time",
    "flags",
    "user_ptime",
    "system_ptime",
    "pinstructions",
    "pcycles",
    "energy_nj",
    "penergy_nj",
    "secure_time_in_system",
    "secure_ptime_in_system",
    "neural_footprint",
    "lifetime_max_neural_footprint",
    "interval_max_neural_footprint",
)


class RusageInfoV6(ctypes.Structure):
    _fields_ = (
        [("uuid", ctypes.c_uint8 * 16)]
        + [(name, ctypes.c_uint64) for name in RUSAGE_INFO_V6_COUNTERS]
        + [("reserved", ctypes.c_uint64 * 9)]
    )


@dataclass(frozen=True)
class ProcessUsage:
    """Cumulative counters of one process; subtract two readings for what it did between them."""

    instructions: int
    cycles: int
    performance_core_instructions: int
    performance_core_cycles: int

    def __sub__(self, other: ProcessUsage) -> ProcessUsage:
        return ProcessUsage(
            self.instructions - other.instructions,
            self.cycles - other.cycles,
            self.performance_core_instructions - other.performance_core_instructions,
            self.performance_core_cycles - other.performance_core_cycles,
        )


def performance_core_runs(runs: list[ProcessUsage]) -> list[ProcessUsage]:
    """The runs that retired instructions on the performance cores. A short query's threads often start on the
    efficiency cores and move later, so a fast run may do only part of its work there; that part's rate is still the
    performance cores' rate for the work"""
    return [run for run in runs if run.performance_core_instructions > 0]


def work_cycles(runs: list[ProcessUsage], performance_cores: bool) -> int:
    """The cycles one run of the repeated work takes: the median instruction count of the runs at the cycles per
    instruction pooled over the middle half of the rated runs, ranked by that rate. With performance_cores the rated
    runs are those that ran there, with their performance-core counters; otherwise every run on all cores. Compare two kinds
    of work on one basis: the efficiency cores retire fewer instructions per cycle. A run the host stalls (its memory
    paged out, its caches flushed) retires the same instructions several times slower; ranking leaves it out."""
    if performance_cores:
        rated = [
            (run.performance_core_cycles, run.performance_core_instructions) for run in performance_core_runs(runs)
        ]
    else:
        rated = [(run.cycles, run.instructions) for run in runs]
    rated.sort(key=lambda counters: counters[0] / max(counters[1], 1))
    middle = rated[len(rated) // 4 : math.ceil(len(rated) * 3 / 4)]
    rate = sum(cycles for cycles, _ in middle) / max(sum(instructions for _, instructions in middle), 1)
    return round(statistics.median(run.instructions for run in runs) * rate)


@functools.cache
def _proc_pid_rusage():
    if sys.platform != "darwin":
        raise RuntimeError("the performance gate reads per-process instruction counts, which requires macOS")
    return ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True).proc_pid_rusage


def read_process_usage(pid: int) -> ProcessUsage:
    """The counters of the process since it started."""
    info = RusageInfoV6()
    if _proc_pid_rusage()(pid, RUSAGE_INFO_V6, ctypes.byref(info)) != 0:
        raise OSError(ctypes.get_errno(), f"proc_pid_rusage failed for pid {pid}")
    return ProcessUsage(info.instructions, info.cycles, info.pinstructions, info.pcycles)
