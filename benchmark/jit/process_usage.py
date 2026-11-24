#!/usr/bin/env python3
#
# The instructions a process retired, read from the kernel's per-process accounting. Unlike time, the count does not
# depend on how busy the host is, which cores the threads ran on or how long they waited for one.

from __future__ import annotations

import ctypes
import ctypes.util
import functools
import sys

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


@functools.cache
def _proc_pid_rusage():
    if sys.platform != "darwin":
        raise RuntimeError("the performance gate reads per-process instruction counts, which requires macOS")
    return ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True).proc_pid_rusage


def read_instructions(pid: int) -> int:
    """The instructions the process retired since it started."""
    info = RusageInfoV6()
    if _proc_pid_rusage()(pid, RUSAGE_INFO_V6, ctypes.byref(info)) != 0:
        raise OSError(ctypes.get_errno(), f"proc_pid_rusage failed for pid {pid}")
    return info.instructions
