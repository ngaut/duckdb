#!/usr/bin/env python3
#
# Structural verifier for DuckDB native vectorized compiled regions.
# This intentionally checks architecture invariants, not line-by-line history.

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

REQUIRED_FILES = (
    "JIT_ARCHITECTURE.md",
    "src/include/duckdb/planner/cost_model.hpp",
    "src/planner/cost_model.cpp",
    "src/include/duckdb/execution/execution_region_backend.hpp",
    "src/include/duckdb/execution/execution_region_common.hpp",
    "src/include/duckdb/execution/execution_region_ir.hpp",
    "src/include/duckdb/execution/execution_region_kernel.hpp",
    "src/include/duckdb/execution/execution_region_lowering.hpp",
    "src/include/duckdb/execution/execution_region_manager.hpp",
    "src/include/duckdb/execution/execution_region_plan.hpp",
    "src/include/duckdb/execution/execution_region_planner.hpp",
    "src/include/duckdb/execution/execution_region_runner.hpp",
    "src/include/duckdb/execution/execution_region_telemetry.hpp",
    "src/include/duckdb/parallel/execution_region_pipeline_adapter.hpp",
    "src/execution/execution_region_backend.cpp",
    "src/execution/execution_region_cost_input.cpp",
    "src/execution/execution_region_cost_input.hpp",
    "src/execution/execution_region_decision.cpp",
    "src/execution/execution_region_decision.hpp",
    "src/execution/execution_region_graph.cpp",
    "src/execution/execution_region_ir.cpp",
    "src/execution/execution_region_manager.cpp",
    "src/execution/execution_region_plan.cpp",
    "src/execution/execution_region_planner.cpp",
    "src/execution/execution_region_runner.cpp",
    "src/execution/execution_region_runtime.cpp",
    "src/execution/execution_region_telemetry.cpp",
    "src/execution/execution_region_telemetry_log.cpp",
    "src/parallel/execution_region_pipeline_adapter.cpp",
    "extension/jit_sljit/sljit_backend.cpp",
    "extension/jit_sljit/sljit_region_plan.cpp",
    "extension/jit_sljit/sljit_region_executable.cpp",
    "extension/jit_sljit/sljit_region_runtime.cpp",
    "benchmark/jit/benchmark_common.py",
    "benchmark/tpch/jit/tpch_common.py",
    "benchmark/tpch/jit/tpch_benchmark.py",
)

REQUIRED_TEXT = {
    "src/include/duckdb/planner/cost_model.hpp": (
        "class DuckDBCostModel",
        "struct PhysicalRunnerCostInput",
        "struct PhysicalRunnerCostProfile",
        "SelectPhysicalRunner",
        "InitialFilterOrder",
    ),
    "src/planner/cost_model.cpp": (
        "DuckDBCostModel::ExpressionCost",
        "DuckDBCostModel::InitialFilterOrder",
        "DuckDBCostModel::SelectPhysicalRunner",
        "PhysicalRunnerShouldSelectAccelerated",
        "has_costed_acceleration",
        "parameters.generated_stage_benefit > 0",
        "profile.saved_work_per_batch > 0",
    ),
    "src/execution/execution_region_planner.cpp": (
        "ExecutionRegionPlanner::Build",
        "ExecutionRegionPlannerDecisionRecorder",
        "SelectExecutionRegionPhysicalRunner",
        "physical_runner.runner_cost",
        "RecordEvent",
    ),
    "src/function/table/system/execution_region_table_function_utils.hpp": (
        "AddExecutionRegionStageTimingColumns",
        "AppendExecutionRegionStageTimingColumn",
        "AppendExecutionRegionRunnerCostProfileColumn",
        "AppendExecutionRegionRunnerCostWorkColumn",
    ),
    "src/function/table/system/duckdb_jit_events.cpp": (
        "JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET",
        "AppendExecutionRegionStageTimingColumn",
        "AppendExecutionRegionRunnerCostProfileColumn",
        "AppendExecutionRegionRunnerCostWorkColumn",
    ),
    "src/function/table/system/duckdb_jit_counters.cpp": (
        "JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET",
        "AppendExecutionRegionStageTimingColumn",
        "AppendExecutionRegionRunnerCostProfileColumn",
        "AppendExecutionRegionRunnerCostWorkColumn",
    ),
    "src/execution/execution_region_decision.cpp": (
        "BuildPhysicalRunnerCostParameters",
        "SelectExecutionRegionPhysicalRunner",
        "DuckDBCostModel::SelectPhysicalRunner",
        "selection.runner_cost.selected_accelerated_runner",
        "duckdb_cbo selects compiled-vectorized physical runner",
    ),
    "src/execution/execution_region_cost_input.cpp": (
        "ExecutionRegionRunnerCostInputBuilder",
        "BuildExecutionRegionCandidateCostInput",
        "TryBuildExecutionRegionPipelineCostInput",
        "candidate.traits.expression_cost",
        "candidate.stage_plan.stages",
        "AddGeneratedExpressionWork",
    ),
    "src/include/duckdb/execution/execution_region_common.hpp": (
        "enum class ExecutionRegionABI",
        "enum class ExecutionRunnerKind",
    ),
    "src/parallel/pipeline.cpp": (
        "execution_region_plan = ExecutionRegionPlanner::Build(client, *this);",
        "GetExecutionRegionRunnerKind(execution_region_plan)",
        "GetExecutionRegionOpenRequest(execution_region_plan)",
    ),
    "src/parallel/pipeline_executor.cpp": (
        "ExecutionRegionPipelineAdapter region_pipeline(*this);",
        "ExecuteExecutionRunner(pipeline.GetExecutionRunnerKind(), region_pipeline, max_chunks)",
    ),
    "src/include/duckdb/execution/execution_region_runner.hpp": (
        "class VectorizedRunner",
        "class CompiledVectorizedRunner",
        "PipelineExecuteResult ExecuteExecutionRunner",
    ),
    "src/execution/execution_region_runner.cpp": (
        "VectorizedRunner::Execute",
        "CompiledVectorizedRunner::Execute",
    ),
    "extension/jit_sljit/sljit_backend.cpp": (
        "AnalyzeSljitRegion",
        "CompileSljitRegion",
    ),
    "extension/jit_sljit/sljit_region_plan.cpp": (
        "BuildSljitRegionPlan",
        "SetFullyFused",
    ),
    "JIT_ARCHITECTURE.md": (
        "DuckDB Native Vectorized Compiled Regions",
        "DuckDB CBO physical-runner decision",
        "protocol-only regions stay vectorized",
    ),
}

REGEX_RULES = (
    (
        "SLJIT dependency in core DuckDB",
        (r"\bsljit\b", r"\bSljit\b", r"\bjit_sljit\b"),
        ("src/**/*.hpp", "src/**/*.cpp"),
        ("src/main/extension/extension_helper.cpp",),
    ),
    (
        "backend reaches across layer boundary",
        (r'#include "duckdb/execution/operator/', r'#include "duckdb/parallel/pipeline(_executor)?\.hpp"'),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
    (
        "private admission or duplicate CBO",
        (
            r"\bExpressionCostModel\b",
            r"expression_cost\.hpp",
            r"\bExecutionRegionRunnerCostProfile\b",
            r"\bPhysicalRunnerCostDecision\b",
            r"\bBuildExecutionRegionRunnerCostProfile\b",
            r"\bExecutionRegionCBO[A-Za-z0-9_]*\b",
            r"\bCompiledRegionCost(Input|Decision)\b",
            r"\bSelectCompiledRegionRunner\b",
            r"\bShouldCompile\(",
            r"\bselection_score\b",
            r"\bnative_ownership_score\b",
        ),
        (
            "src/**/*.hpp",
            "src/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "benchmark/**/*.py",
            "test/api/test_jit*.cpp",
            "test/api/test_jit*.hpp",
            "test/sql/jit/**/*.test",
        ),
        ("benchmark/jit/verify_jit_architecture.py",),
    ),
    (
        "whole executor fallback in compiled layers",
        (
            r"\bExecuteInterpreted\b",
            r"\bValue::Evaluate\b",
            r"\bExecutionCreateHashJoinProbeState\b",
            r"\bExecutionProbeHashJoin\b",
            r"\bExecutionSinkAggregateUpdate\b",
            r"\bfallback-native\b",
            r"\bvectorized_probe_primitive\b",
            r"\bvectorized_sink_update\b",
            r"\bwhole[-_ ]executor\b",
        ),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
    (
        "vectorized projection contract in SLJIT compiled region",
        (
            r"\buse_vectorized_projection\b",
            r"\bExecutionOperatorProject\b",
            r"vectorized projection operator contract",
            r"execution=vectorized-contract",
        ),
        ("extension/jit_sljit/**/*.cpp", "extension/jit_sljit/**/*.hpp"),
        (),
    ),
    (
        "text sentinel for successful execution-region blockers",
        (
            r"blocker=none",
            r"_blocker=none",
            r'blocker\s*(==|!=)\s*"none"',
            r'\.blocker\s*=\s*"none"',
            r'blocker\s*\+=\s*"none"',
            r'result\.blocker\s*=\s*result\.ready\s*\?\s*"none"',
            r'readiness\.blocker[^\n]*"none"',
        ),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
    (
        "duplicate pre-IR inventory or workload selector",
        (
            r"\bExecutionRegionPipelineInventory\b",
            r"\bTryInspectExecutionRegionPipeline\b",
            r"\bRenderExecutionRegionPipelineInventoryIR\b",
            r"\bworkload_relevant\b",
            r"\bworkload_relevance_reason\b",
            r"\bIsExecutionRegionPipelineInventoryWorkloadRelevant\b",
            r"\bHasWrapperOnlySource\b",
        ),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp"),
        (),
    ),
)


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def read_text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def files(patterns: tuple[str, ...]):
    seen = set()
    for pattern in patterns:
        for path in ROOT.glob(pattern):
            if path.is_file() and path not in seen:
                seen.add(path)
                yield path


def verify_files() -> None:
    missing = [path for path in REQUIRED_FILES if not (ROOT / path).exists()]
    if missing:
        raise AssertionError(f"missing required files: {missing}")


def verify_required_text() -> None:
    for path, snippets in REQUIRED_TEXT.items():
        data = read_text(path)
        missing = [snippet for snippet in snippets if snippet not in data]
        if missing:
            raise AssertionError(f"{path}: missing required text {missing}")


def verify_regex_rules() -> None:
    for name, patterns, globs, allowed in REGEX_RULES:
        allowed_paths = {ROOT / path for path in allowed}
        compiled = [(pattern, re.compile(pattern)) for pattern in patterns]
        for path in files(globs):
            if path in allowed_paths:
                continue
            data = path.read_text(encoding="utf-8")
            for pattern, regex in compiled:
                if regex.search(data):
                    raise AssertionError(f"{rel(path)}: {name}: {pattern}")


def verify_range_omits(path: str, start_marker: str, end_marker: str, forbidden: tuple[str, ...]) -> None:
    data = read_text(path)
    snippet = data[data.index(start_marker) : data.index(end_marker)]
    present = [text for text in forbidden if text in snippet]
    if present:
        raise AssertionError(f"{path}: {start_marker} stores rendered labels instead of typed state: {present}")


def verify_typed_observability_state() -> None:
    stage_breakdowns = (
        "string source_stage_runtime_breakdown;",
        "string source_stage_count_breakdown;",
        "string generated_stage_runtime_breakdown;",
        "string generated_stage_count_breakdown;",
    )
    event_labels = (
        "string phase;",
        "string status;",
        "string execution_mode;",
        *stage_breakdowns,
    )
    counter_labels = event_labels[1:]
    telemetry = "src/include/duckdb/execution/execution_region_telemetry.hpp"
    verify_range_omits(telemetry, "struct ExecutionRegionCounter", "struct ExecutionRegionStageTimings", counter_labels)
    verify_range_omits(telemetry, "struct ExecutionRegionEvent", "struct ExecutionRegionTraceSummary", event_labels)
    verify_range_omits(
        "src/include/duckdb/execution/execution_region_runtime.hpp",
        "struct ExecutionRegionRuntimeMetrics",
        "DUCKDB_API void AddExecutionRegionStageRuntime",
        stage_breakdowns,
    )


def verify_candidate_stage_timing_attribution() -> None:
    planner = "src/execution/execution_region_planner.cpp"
    data = read_text(planner)
    candidate_loop = data[
        data.index("for (idx_t candidate_index = 0; candidate_index < lowered_region.candidates.size();") : data.index(
            "Compile(context, *backend, backend_name, *plan, lowered_region, selected_regions);"
        )
    ]
    if "auto stage_timings = shared_stage_timings;" in candidate_loop:
        raise AssertionError(f"{planner}: candidate loop copies shared stage timings into every candidate")
    if "candidate_decision_time_us" in candidate_loop:
        raise AssertionError(f"{planner}: candidate loop still uses inline shared timing attribution")
    required = (
        "auto candidate_trace = decision_recorder.BeginCandidate();",
        "auto &stage_timings = candidate_trace.stage_timings;",
        "decision_recorder.ClaimCandidateDecisionTime(candidate_trace)",
    )
    missing = [snippet for snippet in required if snippet not in candidate_loop]
    if missing:
        raise AssertionError(f"{planner}: candidate shared stage timing attribution missing {missing}")
    recorder_required = (
        "class ExecutionRegionPlannerDecisionRecorder",
        "trace.stage_timings.pipeline_cbo_time_us = shared_stage_timings.pipeline_cbo_time_us;",
        "trace.stage_timings.graph_build_time_us = shared_stage_timings.graph_build_time_us;",
        "trace.stage_timings.ir_lowering_time_us = shared_stage_timings.ir_lowering_time_us;",
        "shared_decision_time_recorded = true;",
    )
    missing = [snippet for snippet in recorder_required if snippet not in data]
    if missing:
        raise AssertionError(f"{planner}: candidate shared stage timing attribution missing {missing}")
    stage_copy = "selected_region.stage_timings = stage_timings;"
    decision_copy = "selected_region.decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);"
    if candidate_loop.index(stage_copy) < candidate_loop.index(decision_copy):
        raise AssertionError(
            f"{planner}: compiled candidate copies stage timings before shared decision timing is attributed"
        )


def main() -> None:
    verify_files()
    verify_required_text()
    verify_regex_rules()
    verify_typed_observability_state()
    verify_candidate_stage_timing_attribution()
    print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
