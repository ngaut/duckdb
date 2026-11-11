#!/usr/bin/env python3
#
# The work a process did, read from the kernel's per-process accounting.
#
# Wall time measures the host as much as the query: competing processes lower the clock of the performance cores,
# push the query's threads to efficiency cores and make them wait for a core. The kernel counts the work itself: the
# instructions the process retired, and the cycles its threads spent on performance cores. The cost of a query is its
# instructions at the cycles per instruction of the performance cores, which is the cycles the query takes when every
# thread runs on a performance core, however busy the host is.

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


class MachTimebaseInfo(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


@dataclass(frozen=True)
class ProcessUsage:
    """Cumulative counters of one process; subtract two readings for what it did between them."""

    # Time the threads were on a core or waiting for one
    runnable_time_ns: int
    instructions: int
    performance_core_instructions: int
    performance_core_cycles: int

    def __sub__(self, other: ProcessUsage) -> ProcessUsage:
        return ProcessUsage(
            self.runnable_time_ns - other.runnable_time_ns,
            self.instructions - other.instructions,
            self.performance_core_instructions - other.performance_core_instructions,
            self.performance_core_cycles - other.performance_core_cycles,
        )


def work_cycles(runs: list[ProcessUsage]) -> int:
    """Performance-core cycles one run of the repeated work takes: the median instruction count of the runs at the
    cycles per instruction the performance cores needed for the work in the middle half of the runs, ranked by that
    rate. A run the host stalls (its memory paged out, its caches flushed) retires the same instructions at a rate
    several times slower; ranking leaves it out instead of letting it dominate the pooled rate."""

    def cycles_per_instruction(run: ProcessUsage) -> float:
        if run.performance_core_instructions == 0:
            return math.inf
        return run.performance_core_cycles / run.performance_core_instructions

    ranked = sorted(runs, key=cycles_per_instruction)
    middle = ranked[len(ranked) // 4 : math.ceil(len(ranked) * 3 / 4)]
    performance_core_instructions = sum(run.performance_core_instructions for run in middle)
    if performance_core_instructions == 0:
        raise RuntimeError("the host ran none of the measured work on a performance core")
    rate = sum(run.performance_core_cycles for run in middle) / performance_core_instructions
    return round(statistics.median(run.instructions for run in runs) * rate)


@functools.cache
def process_usage_reader():
    if sys.platform != "darwin":
        raise RuntimeError("the performance gate reads per-process hardware counters, which requires macOS")
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    timebase = MachTimebaseInfo()
    libc.mach_timebase_info(ctypes.byref(timebase))

    def read(pid: int) -> ProcessUsage:
        info = RusageInfoV6()
        if libc.proc_pid_rusage(pid, RUSAGE_INFO_V6, ctypes.byref(info)) != 0:
            raise OSError(ctypes.get_errno(), f"proc_pid_rusage failed for pid {pid}")
        return ProcessUsage(
            info.runnable_time * timebase.numer // timebase.denom,
            info.instructions,
            info.pinstructions,
            info.pcycles,
        )

    return read


def read_process_usage(pid: int) -> ProcessUsage:
    return process_usage_reader()(pid)
