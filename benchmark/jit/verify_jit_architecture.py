#!/usr/bin/env python3
#
# Structural verifier for DuckDB native vectorized compiled regions.
#
# Keep this file small and architectural. It should verify layer boundaries and
# retired debt, not duplicate the implementation or encode query-specific rules.

import re
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_text(root: Path, path: str) -> str:
    full_path = root / path
    if not full_path.exists():
        raise AssertionError(f"missing required file: {path}")
    return full_path.read_text(encoding="utf-8")


def require_files(root: Path, paths: tuple[str, ...]) -> None:
    for path in paths:
        if not (root / path).exists():
            raise AssertionError(f"missing required file: {path}")


def forbid_paths(root: Path, paths: tuple[str, ...]) -> None:
    for path in paths:
        if (root / path).exists():
            raise AssertionError(f"retired file still present: {path}")


def require_text(root: Path, path: str, snippets: tuple[str, ...]) -> None:
    text = read_text(root, path)
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise AssertionError(f"{path}: missing required text {missing}")


def forbid_text(root: Path, path: str, snippets: tuple[str, ...]) -> None:
    text = read_text(root, path)
    present = [snippet for snippet in snippets if snippet in text]
    if present:
        raise AssertionError(f"{path}: forbidden text still present {present}")


def read_glob_text(root: Path, pattern: str) -> str:
    paths = sorted(path for path in root.glob(pattern) if path.is_file())
    if not paths:
        raise AssertionError(f"missing required files matching: {pattern}")
    return "\n".join(path.read_text(encoding="utf-8") for path in paths)


def require_glob_text(root: Path, pattern: str, snippets: tuple[str, ...]) -> None:
    text = read_glob_text(root, pattern)
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise AssertionError(f"{pattern}: missing required text {missing}")


def forbid_glob_text(root: Path, pattern: str, snippets: tuple[str, ...]) -> None:
    text = read_glob_text(root, pattern)
    present = [snippet for snippet in snippets if snippet in text]
    if present:
        raise AssertionError(f"{pattern}: forbidden text still present {present}")


def forbid_regex(root: Path, path: str, patterns: tuple[str, ...]) -> None:
    text = read_text(root, path)
    for pattern in patterns:
        if re.search(pattern, text):
            raise AssertionError(f"{path}: forbidden pattern still present: {pattern}")


def iter_source_files(root: Path, globs: tuple[str, ...]):
    seen = set()
    for glob in globs:
        for path in root.glob(glob):
            if path.is_file() and path not in seen:
                seen.add(path)
                yield path


def forbid_repo_regex(root: Path, patterns: tuple[str, ...], globs: tuple[str, ...],
                      allowed: tuple[str, ...] = ()) -> None:
    allowed_paths = {root / path for path in allowed}
    compiled = [(pattern, re.compile(pattern)) for pattern in patterns]
    for path in iter_source_files(root, globs):
        if path in allowed_paths:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern, regex in compiled:
            if regex.search(text):
                raise AssertionError(f"{path.relative_to(root)}: forbidden pattern still present: {pattern}")


def verify_required_surfaces(root: Path) -> None:
    require_files(
        root,
        (
            "src/include/duckdb/execution/execution_region_common.hpp",
            "src/include/duckdb/execution/execution_compiled_contract.hpp",
            "src/include/duckdb/execution/execution_expression_ir.hpp",
            "src/include/duckdb/execution/execution_region_ir.hpp",
            "src/include/duckdb/execution/execution_region_lowering.hpp",
            "src/include/duckdb/execution/execution_region_admission.hpp",
            "src/include/duckdb/execution/execution_region_backend.hpp",
            "src/include/duckdb/execution/execution_region_kernel.hpp",
            "src/include/duckdb/execution/execution_region_manager.hpp",
            "src/include/duckdb/execution/execution_region_open_request.hpp",
            "src/include/duckdb/execution/execution_region_plan.hpp",
            "src/include/duckdb/execution/execution_region_planner.hpp",
            "src/include/duckdb/execution/execution_region_runner.hpp",
            "src/include/duckdb/execution/execution_region_runtime.hpp",
            "src/include/duckdb/execution/execution_region_settings.hpp",
            "src/include/duckdb/execution/execution_region_telemetry.hpp",
            "src/include/duckdb/execution/execution_contract.hpp",
            "src/include/duckdb/execution/execution_operator_runtime.hpp",
            "src/include/duckdb/parallel/execution_region_pipeline_adapter.hpp",
            "src/include/duckdb/parallel/pipeline_execution.hpp",
            "src/execution/execution_region_admission.cpp",
            "src/execution/execution_region_backend.cpp",
            "src/execution/execution_region_graph.cpp",
            "src/execution/execution_region_ir.cpp",
            "src/execution/execution_region_manager.cpp",
            "src/execution/execution_region_plan.cpp",
            "src/execution/execution_region_planner.cpp",
            "src/execution/execution_region_runner.cpp",
            "src/execution/execution_region_runtime.cpp",
            "src/execution/execution_region_settings.cpp",
            "src/execution/execution_region_telemetry.cpp",
            "src/execution/execution_contract.cpp",
            "src/parallel/execution_region_pipeline_adapter.cpp",
            "src/parallel/pipeline.cpp",
            "src/parallel/pipeline_executor.cpp",
            "extension/jit_sljit/sljit_backend.cpp",
            "extension/jit_sljit/sljit_region.cpp",
            "extension/jit_sljit/sljit_region_plan.cpp",
            "extension/jit_sljit/sljit_region_codegen.cpp",
            "extension/jit_sljit/sljit_region_runtime.cpp",
            "benchmark/tpch/jit/verify_tpch_trace.py",
            "JIT_ARCHITECTURE.md",
        ),
    )


def verify_retired_overlay_is_gone(root: Path) -> None:
    forbid_paths(
        root,
        (
            "src/include/duckdb/execution/jit/common.hpp",
            "src/include/duckdb/execution/jit/compiled_contract.hpp",
            "src/include/duckdb/execution/jit/ir.hpp",
            "src/include/duckdb/execution/jit/lowering.hpp",
            "src/include/duckdb/execution/jit/manager.hpp",
            "src/include/duckdb/execution/jit/operator_descriptor.hpp",
            "src/include/duckdb/execution/jit/operator_runtime.hpp",
            "src/include/duckdb/execution/jit/pipeline_descriptor.hpp",
            "src/include/duckdb/execution/jit/region.hpp",
            "src/include/duckdb/execution/jit/region_executor.hpp",
            "src/include/duckdb/execution/jit/runtime.hpp",
            "src/execution/jit.cpp",
            "src/execution/jit_expression_ir.cpp",
            "src/execution/jit_join_runtime.cpp",
            "src/execution/jit_operator_descriptor.cpp",
            "src/execution/jit_pipeline_descriptor.cpp",
            "src/execution/jit_region_executor.cpp",
            "src/execution/jit_region_ir.cpp",
            "src/execution/jit_runtime.cpp",
            "src/function/table/system/duckdb_jit_table_function_utils.hpp",
        ),
    )
    forbid_repo_regex(
        root,
        (r'"duckdb/execution/jit/', r"\bGetJitOperatorDescriptor\b", r"\bBindJit", r"\bSupportsJit"),
        ("src/**/*.hpp", "src/**/*.cpp", "extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )


def verify_core_region_model(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_common.hpp",
        (
            "enum class ExecutionRegionExecutionMode : uint8_t { NONE, NATIVE, UNSUPPORTED };",
            "enum class ExecutionRegionForm : uint8_t { NONE, FUSED };",
            "enum class ExecutionRegionABI : uint8_t { NONE, FULL_PIPELINE };",
            "enum class ExecutionRegionResult : uint8_t { NOT_FINISHED, FINISHED, INTERRUPTED, DEFERRED };",
            "HASH_JOIN",
            "HASH_GROUP_BY",
            "PERFECT_HASH_GROUP_BY",
            "UNGROUPED_AGGREGATE",
            "ORDER_BY",
            "TOP_N",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_compiled_contract.hpp",
        (
            "enum class ExecutionCompiledContractKind",
            "SCAN_CURSOR",
            "HASH_JOIN_BUILD",
            "HASH_JOIN_PROBE_CURSOR",
            "AGGREGATE_LOOKUP",
            "AGGREGATE_UPDATE",
            "SINK_CURSOR",
            "STATE_SCAN_CURSOR",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_ir.hpp",
        (
            "struct ExecutionRegionCandidateTraits",
            "struct ExecutionRegionContract",
            "struct ExecutionRegionPipelineInventory",
            "workload_relevant",
            "workload_relevance_reason",
            "struct ExecutionRegionSignature",
            "struct ExecutionRegionCandidate",
            "vector<ExecutionRegionContractShapePart> contract_shape_parts",
            "primitive_update_ready",
            "primitive_update_kind",
            "primitive_update_input_type",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_plan.hpp",
        (
            "struct ExecutionRegionPlan",
            "ExecutionRegionOpenRequest source_open_request",
            "vector<unique_ptr<ExecutionRegionKernel>> kernels",
            "optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel()",
        ),
    )
    forbid_text(
        root,
        "src/include/duckdb/execution/execution_region_plan.hpp",
        (
            "ExecutionRegionIR",
            "ExecutionRegionLoweringPlan",
            "ExecutionRegionAdmissionDecision",
            "policy_decision",
        ),
    )
    require_text(
        root,
        "src/execution/execution_region_runner.cpp",
        (
            "if (!trace_runtime) {\n\t\t\treturn pipeline.FetchSourceContract(result);\n\t\t}",
            "auto trace_start = std::chrono::steady_clock::now();",
            "source_contract_runtime_time_us += source_elapsed_us;",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_graph.hpp",
        (
            "DescribeExecutionRegionGraphShape",
        ),
    )
    require_text(
        root,
        "src/execution/execution_region_planner.cpp",
        (
            "DescribeExecutionRegionLoweringRejection",
            "graph_shape=",
            "graph_blocker=",
        ),
    )
    require_text(
        root,
        "src/execution/execution_region_ir.cpp",
        (
            "BuildExecutionRegionFeatureSetShape",
            "std::sort(features.begin(), features.end())",
            "std::unique(features.begin(), features.end())",
            "BuildExecutionRegionInventoryFeatureShape",
            "BuildExecutionRegionFeatureShape",
        ),
    )
    forbid_text(
        root,
        "src/execution/execution_region_planner.cpp",
        (
            "core region lowering rejected the pipeline graph",
        ),
    )


def verify_generic_admission(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_admission.hpp",
        (
            "struct ExecutionRegionAdmissionRule",
            "BuildExecutionRegionAdmissionShapeKey",
            "BuildExecutionRegionAdmissionContextShapeKey",
            "ExecutionRegionLoweredRegionCanUseMeasuredAutoAdmission",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_backend.hpp",
        (
            "virtual bool HasAutoAdmissionRules(ExecutionRegionCompileTarget target) const;",
            "const ExecutionRegionPipelineInventory &inventory",
            "virtual bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionCandidate &candidate,",
            "const ExecutionRegionLoweringPlan &lowering_plan",
        ),
    )
    require_text(
        root,
        "src/execution/execution_region_planner.cpp",
        (
            "BuildExecutionRegionGraph(pipeline)",
            "AdmitExecutionRegionPipelineForLowering(execution_region_manager, backend_name, *backend, *inventory)",
            "TryLowerExecutionRegion(*pipeline_descriptor)",
            "backend->AnalyzeRegion(input)",
            "AdmitExecutionRegion(execution_region_manager, backend_name, *backend, policy, candidate,",
            "Compile(context, *plan, lowered_region, selected_regions)",
        ),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_backend.cpp",
        (
            "SljitAutoAdmissionWorkUnits",
            "SljitCandidateCanUseMeasuredAutoAdmission",
            "SljitPopulateMeasuredAutoAdmissionRule",
            "SljitLookupMeasuredAutoAdmissionRule",
            "SljitCandidateHasNativeOperatorProtocol",
        ),
    )
    forbidden = (
        "ExecutionRegionMeasuredAdmissionRule",
        "MeasuredAdmissionRules",
        "ExecutionRegionAdmissionProspectPattern",
        "ExecutionRegionPipelineProspect",
        "TryBuildExecutionRegionPipelineProspect",
        "AdmitExecutionRegionPipelineProspectForBackendAnalysis",
        "CanAdmitPipelineProspect",
        "MinAutoAdmissionCardinality",
        "force_median_speedup",
        "exact_contract",
        "SljitMeasuredAdmissionRules",
        "SLJIT_AUTO_ADMISSION_RULES",
        "FindSljitAutoAdmissionRule",
        "SupportsAutoAdmissionPrecheck",
        "ExecutionRegionAutoAdmissionPrecheck",
        "AdmitExecutionRegionCandidateForBackendAnalysis",
        "tpch-sf",
        "q13",
    )
    for path in (
        "src/include/duckdb/execution/execution_region_admission.hpp",
        "src/execution/execution_region_admission.cpp",
        "src/include/duckdb/execution/execution_region_backend.hpp",
        "src/execution/execution_region_backend.cpp",
        "src/execution/execution_region_planner.cpp",
        "extension/jit_sljit/sljit_backend.cpp",
        "test/api/test_jit.cpp",
    ):
        forbid_text(root, path, forbidden)


def verify_runner_first_pipeline(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_runner.hpp",
        (
            "enum class ExecutionRunnerKind : uint8_t { VECTORIZED, COMPILED_VECTORIZED };",
            "class ExecutionRunner",
            "class VectorizedRunner",
            "class CompiledVectorizedRunner",
            "ExecuteExecutionRunner(ExecutionRunnerKind kind, ExecutionRegionPipelineAdapter &pipeline,",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/parallel/execution_region_pipeline_adapter.hpp",
        (
            "class ExecutionRegionPipelineAdapter",
            "PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks)",
            "optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel() const",
            "SourceResultType FetchSourceContract",
            "BindOperator",
            "BindSink",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/parallel/pipeline.hpp",
        (
            "unique_ptr<ExecutionRegionPlan> execution_region_plan",
            "ExecutionRunnerKind execution_runner",
            "void BuildExecutionRegionPlan()",
            "bool PrepareExecutionRegionPlanForExecution()",
            "ExecutionRunnerKind GetExecutionRunnerKind() const",
        ),
    )
    require_text(
        root,
        "src/parallel/pipeline.cpp",
        (
            "execution_region_plan = ExecutionRegionPlanner::Build(client, *this)",
            "ExecutionRunnerKind::COMPILED_VECTORIZED",
            "ExecutionRunnerKind::VECTORIZED",
        ),
    )
    require_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        (
            "ExecutionRegionPipelineAdapter region_pipeline(*this);",
            "ExecuteExecutionRunner(pipeline.GetExecutionRunnerKind(), region_pipeline, max_chunks)",
            "pipeline.PrepareExecutionRegionPlanForExecution()",
        ),
    )
    forbid_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        (
            "BuildExecutionRegionGraph",
            "TryLowerExecutionRegion",
            "CompileRegion",
            "CompileSljitRegion",
        ),
    )


def verify_physical_operator_contracts(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/physical_operator.hpp",
        (
            "virtual ExecutionRegionOperatorKind GetExecutionRegionOperatorKind() const;",
            "virtual ExecutionContract GetExecutionContract() const;",
            "GetExecutionOperatorReadiness",
            "BindExecutionOperator",
            "BindExecutionSink",
            "GetGlobalSourceState(ClientContext &context, const ExecutionRegionOpenRequest &open_request) const",
            "SupportsExecutionSourceContract(const ExecutionRegionOpenRequest &open_request) const",
            "GetExecutionSourceContractData",
        ),
    )
    require_text(
        root,
        "src/execution/physical_operator.cpp",
        (
            "PhysicalOperator::GetExecutionRegionOperatorKind() const",
            "PhysicalOperator::GetGlobalSourceState(ClientContext &context,",
            "PhysicalOperator::SupportsExecutionSourceContract",
            "PhysicalOperator::GetExecutionSourceContractData",
        ),
    )
    forbid_text(
        root,
        "src/include/duckdb/execution/physical_operator.hpp",
        (
            "GetJitOperatorDescriptor",
            "BindJitNativeOperator",
            "BindJitNativeSink",
            "SupportsJitSourceContract",
            "CompiledPipelinePlan",
        ),
    )


def verify_sljit_is_backend_only(root: Path) -> None:
    sljit_paths = tuple(str(path.relative_to(root)) for path in iter_source_files(root, ("extension/jit_sljit/**/*.cpp", "extension/jit_sljit/**/*.hpp")))
    for path in sljit_paths:
        forbid_text(
            root,
            path,
            (
                "duckdb/execution/physical_operator.hpp",
                "duckdb/common/enums/join_type.hpp",
                "duckdb/common/enums/expression_type.hpp",
                "ExecuteInterpreted",
                "ExpressionExecutor",
                "executor_fallback",
                "fallback-native",
                "generated_helper",
                "value-loop",
                "aggregate-vectorized-sink",
                "vectorized-sink-contract",
                "execution_aggregate_runtime",
                "ExecutionAggregateSink",
                "aggregate_sink_info",
                "SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE",
                "SljitNativeRegionOpKind::HASH_AGGREGATE_DISTINCT_SINK",
                "SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE",
                "SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE",
            ),
        )
        forbid_regex(root, path, (r"\bPhysicalOperatorType::", r"\bExpressionType::", r"\bJoinType::"))
    require_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        (
            "BuildSljitRegionPlan",
            "const ExecutionRegionIR &region_ir",
            "const ExecutionRegionCandidate &candidate",
            'BuildExecutionRegionAdmissionShapeKey("sljit", candidate.signature)',
            "SljitNativeRegionHasOperatorContractLoop",
            "SljitNativeRegionGeneratesCode(region) || SljitNativeRegionHasOperatorContractLoop(region, contract)",
            'result.kernel_kind = "generated-machine-code"',
            'result.kernel_kind = "native-operator-protocol"',
            "source-strategy=duckdb-scan-filtered-source-contract",
            "source-strategy=compiled-unfiltered-source-contract",
            "SljitPrimitiveAggregatePayloadSupported",
            "primitive_payload_update=true",
        ),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_region_executable.cpp",
        (
            "BuildSljitNativeUngroupedSumInt64Reference",
            "BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant",
            "BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences",
        ),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        (
            "ExecutePrimitiveAggregatePayloadUpdate",
            "op.aggregate_update.plan.use_primitive_payloads",
            "aggregate primitive payload has no runtime input adapter",
        ),
    )
    require_text(
        root,
        "src/execution/execution_region_ir.cpp",
        (
            "AddExecutionRegionSourceFilterOwnershipCandidates",
            "GetExecutionRegionCandidateSourceFilterOwnership",
            "ExecutionRegionSourceFilterOwnershipKind::DUCKDB_SCAN",
            "ExecutionRegionSourceFilterOwnershipKind::GENERATED",
        ),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        (
            "requested_source_filter_ownership",
            "source_filter_ownership == ExecutionRegionSourceFilterOwnershipKind::DUCKDB_SCAN",
            "source_filter_ownership == ExecutionRegionSourceFilterOwnershipKind::GENERATED",
        ),
    )
    forbid_text(
        root,
        "src/execution/execution_region_planner.cpp",
        (
            "SourceFilterOwnership() == ExecutionRegionSourceFilterOwnershipKind::GENERATED",
            "source_filter_expression_count + 1",
        ),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_region.cpp",
        (
            "AnalyzeSljitRegion",
            "CompileSljitRegion",
            "ExecutionRegionCompilationInput",
            "ExecutionRegionCompileResult",
            'executable_region.ops.empty()',
            "const auto execution_body",
            "executable_region.CodeSize() == 0 ?",
            '"native-operator-protocol"',
            '"generated-machine-code"',
        ),
    )
    forbid_text(
        root,
        "extension/jit_sljit/sljit_region.cpp",
        ("without generated machine code",),
    )
    require_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        ("return !ops.empty();",),
    )
    forbid_text(
        root,
        "src/execution/execution_contract.cpp",
        (
            "hash-aggregate-vectorized-sink",
            "perfect-hash-aggregate-vectorized-sink",
            "ungrouped-aggregate-vectorized-sink",
            "aggregate_vectorized_sink",
            "native_distinct_sink_contract",
        ),
    )
    for path in (
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        "src/include/duckdb/execution/execution_region_runtime.hpp",
        "src/execution/execution_region_runtime.cpp",
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        "src/execution/operator/aggregate/physical_perfecthash_aggregate.cpp",
        "src/execution/operator/aggregate/physical_ungrouped_aggregate.cpp",
    ):
        forbid_text(root, path, ("execution_aggregate_runtime", "ExecutionAggregateSink", "aggregate_sink"))


def verify_observability_and_tooling(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_telemetry.hpp",
        (
            "struct ExecutionRegionEvent",
            "struct ExecutionRegionDecisionCounter",
            "execution_body",
            "admission_shape_key",
            "admission_rule_present",
            "admission_proof",
            "ir_lowering_time_us",
            "backend_analysis_time_us",
            "codegen_time_us",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/execution/execution_region_manager.hpp",
        (
            "class ExecutionRegionManager",
            "RegisterBackend",
            "AddAdmissionProfileRule",
            "GetAdmissionProfileRule",
            "ClearAdmissionProfileRules",
            "GetDecisionCounters",
            "ApplyEventRetentionLimit",
            "RecordRuntimeEvent",
            "vector<ExecutionRegionAdmissionProfileRule> admission_profile_rules",
            "ExecutionRegionEventLog event_log",
        ),
    )
    require_text(
        root,
        "src/function/table/system/duckdb_jit_events.cpp",
        (
            'names.emplace_back("execution_body")',
            "entry.execution_body",
        ),
    )
    require_text(
        root,
        "src/function/table/system/duckdb_jit_counters.cpp",
        (
            'names.emplace_back("execution_body")',
            "entry.execution_body",
        ),
    )
    require_text(
        root,
        "src/function/table/system/duckdb_jit_decision_counters.cpp",
        (
            'names.emplace_back("execution_body")',
            "entry.execution_body",
        ),
    )
    require_text(
        root,
        "src/function/table/system/duckdb_jit_kernel_counters.cpp",
        (
            'names.emplace_back("execution_body")',
            "entry.execution_body",
        ),
    )
    require_text(
        root,
        "src/include/duckdb/function/table/system_functions.hpp",
        (
            "DuckDBJitAdmissionRulesFun",
            "DuckDBJitAddAdmissionRuleFun",
            "DuckDBJitClearAdmissionRulesFun",
        ),
    )
    require_text(
        root,
        "benchmark/tpch/jit/verify_tpch_trace.py",
        (
            "verify_event_row",
            "verify_region_summary",
            "verify_kernel_runtime",
            "verify_ir_requirement",
        ),
    )
    require_text(
        root,
        "benchmark/tpch/jit/tpch_trace.py",
        (
            "canonical_feature_shape",
            "candidate_signature_feature_shape",
            "candidate_signature_context_feature_shape",
        ),
    )
    forbid_text(
        root,
        "benchmark/tpch/jit/verify_tpch_trace.py",
        (
            "SOURCE_BOUNDARY_HASH_JOIN_REQUIRED_FIELDS",
            "KNOWN_ADMISSION",
            "CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES",
            "force_median_speedup",
            "exact_contract",
        ),
    )
    require_text(
        root,
        "src/function/table/system/execution_region_table_function_utils.hpp",
        (
            "AddExecutionRegionCandidateTraceColumns",
            "AppendExecutionRegionCandidateTraceColumns",
            "AppendNullExecutionRegionCandidateTraceColumns",
        ),
    )
    forbid_text(
        root,
        "src/function/table/system/execution_region_table_function_utils.hpp",
        (
            "AddJitCandidateTraceColumns",
            "AppendJitCandidateTraceColumns",
            "AppendNullJitCandidateTraceColumns",
            "FormatJitTableFunctionStringList",
        ),
    )


def verify_docs_and_tests(root: Path) -> None:
    require_text(
        root,
        "JIT_ARCHITECTURE.md",
        (
            "DuckDB Native Vectorized Compiled Regions",
            "backend-neutral region IR",
            "VectorizedRunner",
            "CompiledVectorizedRunner",
            "SLJIT",
        ),
    )
    require_glob_text(
        root,
        "test/api/test_jit*.cpp",
        (
            "JIT auto policy uses database-local admission profile",
            "ExecutionRegionABI::FULL_PIPELINE",
            "JIT full pipeline uses ordered sink runtime protocol without whole operator fallback",
            "JIT fuses projection payloads into primitive ungrouped aggregate reducers",
        ),
    )
    require_text(
        root,
        "test/api/test_jit_helpers.hpp",
        (
            "struct JitTestDatabase",
            "RequireJitEvent",
            "RequireNoExpressionJitEvents",
            "measured-auto-admission",
        ),
    )
    require_text(
        root,
        "test/api/test_jit_contract_backends.hpp",
        (
            "class UnitTestExecutionRegionBackend",
            "class AutoRejectedCountingBackend",
            "class AutoMissingExecutionFormAdmissionBackend",
            "class FullPipelineAbiRejectRegionBackend",
        ),
    )
    forbid_text(
        root,
        "test/api/test_jit.cpp",
        (
            "class UnitTestExecutionRegionBackend",
            "class AutoRejectedCountingBackend",
            "class AutoMissingExecutionFormAdmissionBackend",
        ),
    )
    forbid_text(
        root,
        "test/api/test_jit_helpers.hpp",
        (
            "class UnitTestExecutionRegionBackend",
            "class ZeroCodeRegionBackend",
            "class AutoRejectedCountingBackend",
            "class AutoMissingExecutionFormAdmissionBackend",
        ),
    )
    forbid_glob_text(
        root,
        "test/api/test_jit*.cpp",
        (
            "RequireSljitUnmeasuredAutoSkipEvent",
            "measured fused-region admission proof",
            "force_median_speedup",
            "exact_contract",
        ),
    )


def main() -> None:
    root = repo_root()
    verify_required_surfaces(root)
    verify_retired_overlay_is_gone(root)
    verify_core_region_model(root)
    verify_generic_admission(root)
    verify_runner_first_pipeline(root)
    verify_physical_operator_contracts(root)
    verify_sljit_is_backend_only(root)
    verify_observability_and_tooling(root)
    verify_docs_and_tests(root)
    print("Compiled-region architecture verification passed")


if __name__ == "__main__":
    main()
