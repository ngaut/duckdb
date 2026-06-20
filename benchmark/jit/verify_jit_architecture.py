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

RETIRED_FILES = (
    "src/include/duckdb/planner/expression_cost.hpp",
    "src/planner/expression_cost.cpp",
    "src/include/duckdb/execution/jit/common.hpp",
    "src/include/duckdb/execution/jit/compiled_contract.hpp",
    "src/include/duckdb/execution/jit/ir.hpp",
    "src/include/duckdb/execution/jit/lowering.hpp",
    "src/include/duckdb/execution/jit/manager.hpp",
    "src/include/duckdb/execution/jit/operator_descriptor.hpp",
    "src/include/duckdb/execution/jit/pipeline_descriptor.hpp",
    "src/include/duckdb/execution/jit/region_executor.hpp",
    "src/include/duckdb/execution/jit/runtime.hpp",
    "src/include/duckdb/execution/execution_region_registration.hpp",
    "src/include/duckdb/execution/execution_region_open_request.hpp",
    "src/include/duckdb/execution/execution_region_admission.hpp",
    "src/include/duckdb/execution/execution_region_cost_model.hpp",
    "src/include/duckdb/execution/execution_region_runner_selection.hpp",
    "src/execution/jit.cpp",
    "src/execution/jit_expression_ir.cpp",
    "src/execution/jit_operator_descriptor.cpp",
    "src/execution/jit_pipeline_descriptor.cpp",
    "src/execution/jit_region_executor.cpp",
    "src/execution/jit_region_ir.cpp",
    "src/execution/jit_runtime.cpp",
    "src/execution/execution_region_admission.cpp",
    "src/execution/execution_region_cost_model.cpp",
    "src/execution/execution_region_runner_selection.cpp",
    "src/include/duckdb/main/query_profiler_execution_regions.hpp",
    "src/main/query_profiler_execution_regions.cpp",
    "src/function/table/system/duckdb_jit_add_admission_rule.cpp",
    "src/function/table/system/duckdb_jit_admission_rules.cpp",
    "src/function/table/system/duckdb_jit_clear_admission_rules.cpp",
    "src/function/table/system/duckdb_jit_decision_counters.cpp",
    "src/function/table/system/duckdb_jit_kernel_counters.cpp",
    "benchmark/jit/trace_manifest.py",
    "benchmark/tpch/jit/tpch_schema.py",
    "benchmark/tpch/jit/tpch_calibrate.py",
    "benchmark/tpch/jit/verify_tpch_calibration.py",
    "JIT_ENCODED_GROUPBY_PROOF.md",
    "JIT_FILTER_STRATEGY.md",
    "JIT_REDESIGN.md",
    "JIT_SIMD_PLAN.md",
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
        "input.accelerated_stage_count > 0",
    ),
    "src/execution/execution_region_planner.cpp": (
        "BuildPhysicalRunnerCostInput",
        "SelectExecutionRegionPhysicalRunner",
        "ExecutionRegionHasAcceleratedRunnerWork",
        "lowering_plan.generated_stage_count",
        "DuckDBCostModel::SelectPhysicalRunner",
        "ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE",
        "runner_cost.accelerated_stage_count > 0",
    ),
    "src/include/duckdb/execution/execution_region_lowering.hpp": (
        "SetGeneratedStageCount",
        "generated_stage_count",
    ),
    "src/include/duckdb/execution/execution_region_common.hpp": (
        "enum class ExecutionRegionForm",
        "enum class ExecutionRegionABI",
        "enum class ExecutionRegionExecutionBody",
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
        "SljitNativeRegionGeneratedStageCount",
        "SetGeneratedStageCount",
    ),
    "JIT_ARCHITECTURE.md": (
        "DuckDB Native Vectorized Compiled Regions",
        "DuckDB CBO physical-runner decision",
        "protocol-only regions stay vectorized",
    ),
}

FORBIDDEN_TEXT = {
    "src/execution/execution_region_planner.cpp": (
        "ExecutionRegionHasCompiledRunnerWork",
        "ExecutionRegionExecutionBody::NATIVE_CONTRACT",
        "kernel=native-contract",
        ";cbo_",
        "selected_runner=",
    ),
    "src/planner/cost_model.cpp": (
        "ExpressionCostModel",
        "SelectCompiledRegionRunner",
        "DescribeCompiledRegion",
        ";cbo_",
    ),
    "src/include/duckdb/execution/execution_region_backend.hpp": (
        "CanAttemptAutoAdmission",
        "HasAutoAdmissionRules",
        "GetAutoAdmissionRule",
        "SupportsAutoGeneratedRegions",
    ),
    "src/include/duckdb/execution/execution_region_manager.hpp": (
        "AdmissionProfileRule",
        "AddAdmissionProfileRule",
        "GetAdmissionProfileRule",
        "ClearAdmissionProfileRules",
    ),
    "extension/jit_sljit/sljit_backend.cpp": (
        "SljitPipelineShapeCanAttemptAutoAdmission",
        "SljitPopulateMeasuredAutoAdmissionRule",
        "HasAutoAdmissionRules",
        "GetAutoAdmissionRule",
    ),
    "extension/jit_sljit/sljit_region_plan.cpp": (
        "ExecutionRegionExecutionBody::NATIVE_CONTRACT",
        "kernel=native-contract",
        "SljitNativeRegionHasProtocolExecutableBody",
        "SLJIT native region has no executable native body",
    ),
    "test/api/test_jit_helpers.hpp": (
        "native-contract",
        "RequireCompiledNativeContractRegion",
    ),
}

REGEX_RULES = (
    (
        "retired JIT overlay hook",
        (r'"duckdb/execution/jit/', r"\bGetJitOperatorDescriptor\b", r"\bBindJit",
         r"\bSupportsJit", r"\bGetJitSourceContractData\b", r"\bCompiledPipelinePlan\b"),
        ("src/**/*.hpp", "src/**/*.cpp", "extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
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
            r"\brunner_selection\b",
            r"duckdb_jit_(decision|kernel)_counters",
            r"\bcounter_blockers\.csv\b",
            r"\brunner_decisions\.csv\b",
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
        (r"\bExecuteInterpreted\b", r"\bValue::Evaluate\b", r"\bfallback-native\b",
         r"\bgenerated_helper\b", r"\bhelper-fusion\b", r"\bwhole[-_ ]executor\b"),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
    (
        "text sentinel for successful execution-region blockers",
        (
            r'blocker\s*(==|!=)\s*"none"',
            r'\.blocker\s*=\s*"none"',
            r'result\.blocker\s*=\s*result\.ready\s*\?\s*"none"',
            r'readiness\.blocker[^\n]*"none"',
        ),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        (),
    ),
    (
        "dead pipeline-inventory trace mode",
        (
            r"\bExecutionRegionPipelineInventoryMode\b",
            r"\binventory\.ir\b",
        ),
        ("src/execution/**/*.cpp", "src/include/duckdb/execution/**/*.hpp"),
        (),
    ),
    (
        "pre-CBO workload relevance gate",
        (
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

    retired = [path for path in RETIRED_FILES if (ROOT / path).exists()]
    if retired:
        raise AssertionError(f"retired files still present: {retired}")


def verify_required_text() -> None:
    for path, snippets in REQUIRED_TEXT.items():
        data = read_text(path)
        missing = [snippet for snippet in snippets if snippet not in data]
        if missing:
            raise AssertionError(f"{path}: missing required text {missing}")


def verify_forbidden_text() -> None:
    for path, snippets in FORBIDDEN_TEXT.items():
        data = read_text(path)
        present = [snippet for snippet in snippets if snippet in data]
        if present:
            raise AssertionError(f"{path}: forbidden text still present {present}")


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


def verify_counter_state_is_typed() -> None:
    data = read_text("src/include/duckdb/execution/execution_region_telemetry.hpp")
    start = data.index("struct ExecutionRegionCounter")
    end = data.index("struct ExecutionRegionStageTimings")
    counter = data[start:end]
    cached_labels = (
        "string target;",
        "string status;",
        "string execution_mode;",
        "string region_execution_form;",
        "string execution_body;",
        "string selected_runner;",
        "string requested_policy;",
        "string source_stage_runtime_breakdown;",
        "string source_stage_count_breakdown;",
        "string generated_stage_runtime_breakdown;",
        "string generated_stage_count_breakdown;",
    )
    present = [label for label in cached_labels if label in counter]
    if present:
        raise AssertionError(f"ExecutionRegionCounter stores rendered labels instead of typed state: {present}")


def verify_event_state_is_typed() -> None:
    data = read_text("src/include/duckdb/execution/execution_region_telemetry.hpp")
    start = data.index("struct ExecutionRegionEvent")
    end = data.index("struct ExecutionRegionTraceSummary")
    event = data[start:end]
    cached_labels = (
        "string phase;",
        "string target;",
        "string status;",
        "string execution_mode;",
        "string region_execution_form;",
        "string execution_body;",
        "string requested_policy;",
        "string source_stage_runtime_breakdown;",
        "string source_stage_count_breakdown;",
        "string generated_stage_runtime_breakdown;",
        "string generated_stage_count_breakdown;",
    )
    present = [label for label in cached_labels if label in event]
    if present:
        raise AssertionError(f"ExecutionRegionEvent stores rendered labels instead of typed state: {present}")


def verify_runtime_metrics_are_typed() -> None:
    data = read_text("src/include/duckdb/execution/execution_region_runtime.hpp")
    start = data.index("struct ExecutionRegionRuntimeMetrics")
    end = data.index("DUCKDB_API void AddExecutionRegionStageRuntime")
    metrics = data[start:end]
    cached_breakdowns = (
        "string source_stage_runtime_breakdown;",
        "string source_stage_count_breakdown;",
        "string generated_stage_runtime_breakdown;",
        "string generated_stage_count_breakdown;",
    )
    present = [breakdown for breakdown in cached_breakdowns if breakdown in metrics]
    if present:
        raise AssertionError(f"ExecutionRegionRuntimeMetrics stores rendered stage breakdowns: {present}")


def main() -> None:
    verify_files()
    verify_required_text()
    verify_forbidden_text()
    verify_regex_rules()
    verify_counter_state_is_typed()
    verify_event_state_is_typed()
    verify_runtime_metrics_are_typed()
    print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
