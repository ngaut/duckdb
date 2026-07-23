#!/usr/bin/env python3
#
# Shared host-admission checks for production performance measurements.

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

HOST_QUIESCENCE_SAMPLE_COUNT = 3
HOST_QUIESCENCE_SAMPLE_INTERVAL_S = 1.0
HOST_QUIESCENCE_MAX_CPU_FRACTION = 0.10
HOST_QUIESCENCE_MAX_PROCESS_CPU_PERCENT = 20.0
MACOS_SECURITY_MAX_CPU_PERCENT = 5.0
HOST_QUIESCENCE_MAX_ATTEMPTS = 12
HOST_QUIESCENCE_RETRY_INTERVAL_S = 5.0


class HostQuiescenceError(RuntimeError):
    pass


def is_macos_security_process(command: str) -> bool:
    process_name = Path(command).name.lower()
    return process_name == "syspolicyd" or process_name.startswith("xprotect")


def process_cpu_snapshot() -> tuple[float, float, list[tuple[float, int, str]]]:
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    try:
        result = subprocess.run(
            ["ps", "-A", "-o", "pid=,pcpu=,comm="],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=environment,
        )
    except FileNotFoundError as exc:
        raise HostQuiescenceError("host quiescence requires a POSIX-compatible ps command") from exc
    if result.returncode != 0:
        raise HostQuiescenceError(f"host quiescence ps failed: {result.stderr.strip()}")

    processes = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(maxsplit=2)
        if len(fields) < 2:
            continue
        try:
            pid = int(fields[0])
            cpu_percent = float(fields[1])
        except ValueError:
            continue
        command = fields[2] if len(fields) == 3 else "unknown"
        processes.append((cpu_percent, pid, command))
    if not processes:
        raise HostQuiescenceError("host quiescence ps returned no process CPU samples")
    logical_cpu_count = os.cpu_count() or 1
    cpu_fraction = sum(process[0] for process in processes) / (100.0 * logical_cpu_count)
    security_cpu_percent = 0.0
    if sys.platform == "darwin":
        security_cpu_percent = sum(
            cpu_percent for cpu_percent, _, command in processes if is_macos_security_process(command)
        )
    return cpu_fraction, security_cpu_percent, sorted(processes, reverse=True)


def require_host_quiescence() -> None:
    samples = []
    for sample_idx in range(HOST_QUIESCENCE_SAMPLE_COUNT):
        samples.append(process_cpu_snapshot())
        if sample_idx + 1 < HOST_QUIESCENCE_SAMPLE_COUNT:
            time.sleep(HOST_QUIESCENCE_SAMPLE_INTERVAL_S)

    cpu_fractions = [sample[0] for sample in samples]
    security_cpu_percents = [sample[1] for sample in samples]
    busiest_cpu_percents = [sample[2][0][0] for sample in samples]
    median_cpu_fraction = statistics.median(cpu_fractions)
    median_security_cpu_percent = statistics.median(security_cpu_percents)
    median_busiest_cpu_percent = statistics.median(busiest_cpu_percents)
    rendered_samples = ", ".join(f"{sample * 100.0:.1f}%" for sample in cpu_fractions)
    print(
        "host quiescence: "
        f"median process CPU {median_cpu_fraction * 100.0:.1f}% "
        f"across {os.cpu_count() or 1} logical CPUs (samples: {rendered_samples})",
        flush=True,
    )
    if sys.platform == "darwin" and median_security_cpu_percent > MACOS_SECURITY_MAX_CPU_PERCENT:
        raise HostQuiescenceError(
            "macOS security scanning is active: "
            f"median syspolicyd/XProtect CPU {median_security_cpu_percent:.1f}% exceeds "
            f"{MACOS_SECURITY_MAX_CPU_PERCENT:.1f}%"
        )
    if median_busiest_cpu_percent > HOST_QUIESCENCE_MAX_PROCESS_CPU_PERCENT:
        _, pid, command = max(samples, key=lambda sample: sample[2][0][0])[2][0]
        raise HostQuiescenceError(
            "a single competing process is too busy for production performance measurement: "
            f"median busiest-process CPU {median_busiest_cpu_percent:.1f}% exceeds "
            f"{HOST_QUIESCENCE_MAX_PROCESS_CPU_PERCENT:.1f}%; "
            f"busiest observed process pid {pid} {command}"
        )
    if median_cpu_fraction <= HOST_QUIESCENCE_MAX_CPU_FRACTION:
        return

    _, _, busiest_processes = max(samples, key=lambda sample: sample[0])
    top_processes = ", ".join(
        f"pid {pid} {command} {cpu_percent:.1f}%" for cpu_percent, pid, command in busiest_processes[:5]
    )
    raise HostQuiescenceError(
        "host is too busy for production performance measurement: "
        f"median process CPU {median_cpu_fraction * 100.0:.1f}% exceeds "
        f"{HOST_QUIESCENCE_MAX_CPU_FRACTION * 100.0:.1f}%; top processes: {top_processes}"
    )


def wait_for_host_quiescence() -> None:
    for attempt in range(1, HOST_QUIESCENCE_MAX_ATTEMPTS + 1):
        try:
            require_host_quiescence()
            return
        except HostQuiescenceError as exc:
            if attempt == HOST_QUIESCENCE_MAX_ATTEMPTS:
                raise
            print(
                f"host quiescence retry {attempt}/{HOST_QUIESCENCE_MAX_ATTEMPTS}: {exc}",
                flush=True,
            )
            time.sleep(HOST_QUIESCENCE_RETRY_INTERVAL_S)
