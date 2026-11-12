#!/usr/bin/env python3
#
# The contract between the runner cost model and the kernels it selects, checked on the rows of
# duckdb_jit_counters() of one runtime-traced query.
#
# The cost model admits a compiled runner for the work it credits to it, and names the runtime proofs that work
# owes. A selected runner either reports why it could not compile, or its kernel records each of those proofs when
# it runs.

from __future__ import annotations

import collections
import re
from collections.abc import Iterator

FAILED_STATUSES = ("skipped", "unsupported", "unavailable", "error")
RUNNER_COST_COMPONENT_FIELDS = (
    "runner_cost_generated_expression_work",
    "runner_cost_generated_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_selected_hash_join_filter_materialization_penalty",
    "runner_cost_source_contract_scan_penalty",
    "runner_cost_full_pipeline_work",
    "runner_cost_stateful_protocol_penalty",
)
CREDITED_WORK_FIELDS = (
    "runner_cost_generated_stage_work",
    "runner_cost_generated_backend_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_full_pipeline_work",
)
# A kernel credited with materialization elision does none of this work at runtime
MATERIALIZATION_WORK = re.compile(r"fallback|whole[-_]executor|materialization|buffer_append", re.IGNORECASE)
# A single-key grouped update whose run sample finds no group runs hands the pipeline to DuckDB's operators by
# design; the kernel records the rows it handed off under this path, and every delegation it reports serves them.
NO_GROUP_RUNS_NATIVE_PIPELINE_PATH = "aggregate_update.no_group_runs_native_pipeline"
RUNTIME_WORK_FIELDS = (
    "source_stage_count_breakdown",
    "source_stage_runtime_breakdown",
    "generated_stage_count_breakdown",
    "generated_stage_runtime_breakdown",
    "jit_runtime_path_counts",
    "jit_runtime_delegation_counts",
)
STAGE_WORK_PROOFS = ("accelerated_stage_work", "backend_owned_work", "generated_stage_work")


def number(row: dict, field: str) -> int:
    return int(row.get(field) or 0)


def flag(row: dict, field: str) -> bool:
    return row.get(field) in (True, "true")


def named_counts(value: str) -> dict[str, int]:
    """The counts of a `name=count;name=count` counter field."""
    counts: dict[str, int] = collections.defaultdict(int)
    for entry in value.split(";"):
        name, separator, count = entry.rpartition("=")
        if separator:
            counts[name.strip()] += int(count)
    return counts


def describe(row: dict) -> str:
    return (
        f"{row['status']} {row['selected_runner']} kernel {row['kernel_id'] or 'none'} "
        f"({row['blocker'] or row['runner_cost_selection_reason']})"
    )


def has_proof(row: dict, proof: str) -> bool:
    return named_counts(row["jit_runtime_proof_counts"]).get(proof, 0) > 0


def materialization_work(row: dict) -> str:
    for field in RUNTIME_WORK_FIELDS:
        for entry in row[field].split(";"):
            name = entry.split("=", 1)[0]
            if MATERIALIZATION_WORK.search(name):
                return f"{field} {name.strip()}"
    return ""


def has_undeclared_delegation(row: dict) -> bool:
    handed_off = named_counts(row["jit_runtime_path_counts"]).get(NO_GROUP_RUNS_NATIVE_PIPELINE_PATH, 0)
    return any(count > handed_off for count in named_counts(row["jit_runtime_delegation_counts"]).values())


def proof_failure(proof: str, runtime_rows: list[dict]) -> str:
    """Why the runtime rows of a kernel do not prove the work, or nothing when they do. A kernel that received no
    work, or handed its pipeline to DuckDB's operators before its own saw a row, owes no proof of it."""
    if all(has_proof(row, "native_continuation") for row in runtime_rows):
        return ""
    proved = any(has_proof(row, proof) for row in runtime_rows)
    idle = all(has_proof(row, "no_work") for row in runtime_rows)
    if proof in STAGE_WORK_PROOFS:
        compiled_nothing_else = all(
            number(row, "lazy_code_size") == 0 and not row["jit_runtime_delegation_counts"] for row in runtime_rows
        )
        return "" if proved or (idle and compiled_nothing_else) else f"no runtime proof of {proof}"
    if proof == "full_pipeline_ownership":
        return "" if proved or idle else f"no runtime proof of {proof}"
    if proof == "materialization_elision":
        for row in runtime_rows:
            work = materialization_work(row)
            if work:
                return f"materialization-elision kernel did materialization work: {work}"
            if has_undeclared_delegation(row):
                return f"materialization-elision kernel delegated work: {row['jit_runtime_delegation_counts']}"
        return "" if proved else f"no runtime proof of {proof}"
    return f"unknown runtime proof requirement {proof}"


def decision_failures(row: dict) -> Iterator[str]:
    if number(row, "count") <= 0:
        yield "non-positive count"
    if row["status"] in FAILED_STATUSES and not row["blocker"]:
        yield "no blocker"
    if not flag(row, "runner_cost_profile"):
        return
    if number(row, "runner_cost_startup_cost") < 0:
        yield "negative startup cost"
    if number(row, "runner_cost_rows") <= 0 or number(row, "runner_cost_batches") <= 0:
        yield "costed no rows"
    selected = number(row, "runner_cost_selected_accelerated_runner_count") > 0
    if not row["runner_cost_selection_reason"].startswith("admitted_" if selected else "rejected_"):
        yield "selection reason contradicts the selection"
    if number(row, "runner_cost_saved_work_per_batch") != 0 and not any(
        number(row, field) != 0 for field in RUNNER_COST_COMPONENT_FIELDS
    ):
        yield "saved work has no cost component"


def selected_runner_failures(row: dict, runtime_rows: list[dict]) -> Iterator[str]:
    if row["execution_mode"] != "native" or row["selected_runner"] != "compiled_vectorized":
        yield "selected runner is not a native compiled kernel"
    proofs = sorted(proof for proof in row["runner_cost_required_runtime_proofs"].split("|") if proof)
    if not proofs and any(number(row, field) != 0 for field in CREDITED_WORK_FIELDS):
        yield "credits work that owes no runtime proof"
    if row["status"] != "compiled":
        if row["status"] not in FAILED_STATUSES or not row["blocker"]:
            yield "selected runner neither compiled nor reported a failure"
        return
    if not proofs:
        return
    if not runtime_rows:
        yield "compiled kernel has no runtime counters"
        return
    if any(runtime_row["status"] == "error" for runtime_row in runtime_rows):
        yield "kernel recorded a runtime error"
    for proof in proofs:
        failure = proof_failure(proof, runtime_rows)
        if failure:
            yield failure


def counter_contract_failures(rows: list[dict]) -> list[str]:
    runtime_rows_by_kernel = collections.defaultdict(list)
    for row in rows:
        if row["execution_mode"] == "native" and number(row, "kernel_id") > 0 and number(row, "invocation_count") > 0:
            runtime_rows_by_kernel[(row["backend_name"], row["kernel_id"])].append(row)
    failures = []
    for row in rows:
        row_failures = list(decision_failures(row))
        if flag(row, "runner_cost_profile") and number(row, "runner_cost_selected_accelerated_runner_count") > 0:
            runtime_rows = runtime_rows_by_kernel[(row["backend_name"], row["kernel_id"])]
            row_failures.extend(selected_runner_failures(row, runtime_rows))
        failures.extend(f"{describe(row)}: {failure}" for failure in row_failures)
    return failures
