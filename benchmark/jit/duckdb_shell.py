#!/usr/bin/env python3
#
# Drives the DuckDB shell for the JIT gates.

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from process_usage import ProcessUsage, read_process_usage


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def duckdb_shell_quote(value) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def duckdb_shell_environment() -> dict[str, str]:
    """The environment the harness runs the shell in. An AI agent's environment variables switch the shell to agent
    mode, which changes how it prints results and errors; the harness reads it as a plain script runner."""
    return {**os.environ, "DUCKDB_AGENT_MODE": "0"}


class DuckDBShell:
    """An interactive shell on an in-memory database that prints results as JSON.

    The shell prints a marker after the statements of each call and then blocks on its input, so a call returns when
    the process is idle again: the counters read around a call are the cost of exactly those statements."""

    def __init__(self, duckdb: Path):
        self.process = subprocess.Popen(
            [str(duckdb), ":memory:"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=duckdb_shell_environment(),
        )
        self.calls = 0
        self.run(".bail on\n.mode json")

    def __enter__(self) -> DuckDBShell:
        return self

    def __exit__(self, exception_type, exception, traceback) -> None:
        self.process.kill()
        self.process.wait()
        self.process.stdout.close()

    def run(self, statements: str) -> str:
        """Runs the statements and returns what the shell printed for them."""
        self.calls += 1
        marker = f"__duckdb_shell_call_{self.calls}_done__"
        output = []
        try:
            self.process.stdin.write(f"{statements}\n.print {marker}\n")
            self.process.stdin.flush()
            for line in iter(self.process.stdout.readline, ""):
                if line.rstrip("\n") == marker:
                    return "".join(output)
                output.append(line)
        except BrokenPipeError:
            output.extend(self.process.stdout)
        raise RuntimeError(
            f"the DuckDB shell exited with code {self.process.wait()} while running:\n{statements}\n"
            f"OUTPUT:\n{''.join(output)}"
        )

    def query(self, statement: str) -> list[dict]:
        """Runs one statement and returns its rows."""
        return json.loads(self.run(statement))

    def measure(self, statements: str) -> ProcessUsage:
        """Runs the statements and returns what the process did for them."""
        before = read_process_usage(self.process.pid)
        self.run(statements)
        return read_process_usage(self.process.pid) - before


def create_database(duckdb: Path, database: Path, sql: str) -> None:
    """Replaces the database file with one the SQL fills, and leaves it closed."""
    database.unlink(missing_ok=True)
    with DuckDBShell(duckdb) as shell:
        shell.run(f".open {duckdb_shell_quote(database)}\n{sql}\n.open :memory:")
