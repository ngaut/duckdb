#!/usr/bin/env python3
#
# Verify DuckDB JIT architecture source boundaries.

import argparse
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def removed_name(*parts: str) -> str:
    return "".join(parts)


def iter_source_files(root: Path, paths: list[Path]) -> list[Path]:
    result = []
    for path in paths:
        full_path = root / path
        if full_path.is_file():
            result.append(full_path)
            continue
        result.extend(
            sorted(
                entry
                for entry in full_path.rglob("*")
                if entry.is_file() and entry.suffix in (".cpp", ".hpp", ".h", ".test", ".py", ".md")
            )
        )
    return result


def source_lines(path: Path) -> list[tuple[int, str]]:
    return [(line_no, line) for line_no, line in enumerate(read_text(path).splitlines(), 1)]


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_required_text(root: Path, path: str, snippets: list[str]) -> None:
    full_path = root / path
    if not full_path.exists():
        fail(f"missing required file: {path}")
    text = read_text(full_path)
    for snippet in snippets:
        if snippet not in text:
            fail(f"{path}: missing required architecture text: {snippet}")


def assert_no_text(root: Path, paths: list[Path], forbidden: list[str], excluded: set[Path] | None = None) -> None:
    excluded = excluded or set()
    for path in iter_source_files(root, paths):
        rel_path = path.relative_to(root)
        if rel_path in excluded:
            continue
        text = read_text(path)
        for needle in forbidden:
            if needle in text:
                fail(f"{rel_path}: forbidden JIT text found: {needle}")


def assert_no_forbidden_includes(root: Path, paths: list[Path], forbidden: list[str]) -> None:
    for path in iter_source_files(root, paths):
        rel_path = path.relative_to(root)
        for line_no, line in source_lines(path):
            stripped = line.strip()
            if not stripped.startswith("#include"):
                continue
            for needle in forbidden:
                if needle in stripped:
                    fail(f"{rel_path}:{line_no}: forbidden include boundary crossing: {stripped}")


def assert_absent_paths(root: Path, paths: list[str]) -> None:
    for path in paths:
        if (root / path).exists():
            fail(f"removed architecture file still exists: {path}")


def assert_no_generated_artifacts(root: Path, paths: list[Path]) -> None:
    for path in paths:
        full_path = root / path
        if not full_path.exists():
            continue
        for entry in full_path.rglob("*"):
            if entry.is_dir() and entry.name == "__pycache__":
                fail(f"generated Python cache directory still exists: {entry.relative_to(root)}")
            if entry.is_file() and entry.suffix == ".pyc":
                fail(f"generated Python bytecode file still exists: {entry.relative_to(root)}")


def extract_cpp_function_body(path: Path, function_name: str) -> str:
    text = read_text(path)
    name_pos = text.find(function_name)
    if name_pos < 0:
        fail(f"{path}: missing function {function_name}")
    brace_pos = text.find("{", name_pos)
    if brace_pos < 0:
        fail(f"{path}: missing body for function {function_name}")
    depth = 0
    for pos in range(brace_pos, len(text)):
        char = text[pos]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[brace_pos : pos + 1]
    fail(f"{path}: unterminated body for function {function_name}")


def assert_no_raw_sljit_codegen_outside(root: Path, allowed: set[Path]) -> None:
    raw_codegen_tokens = (
        "sljit_emit_",
        "sljit_create_compiler",
        "sljit_generate_code",
        "sljit_free_compiler",
        "sljit_set_label",
        "struct sljit_compiler",
        "struct sljit_jump",
        "struct sljit_label",
    )
    for path in iter_source_files(root, [Path("extension/jit_sljit")]):
        rel_path = path.relative_to(root)
        if rel_path in allowed:
            continue
        text = read_text(path)
        for token in raw_codegen_tokens:
            if token in text:
                fail(f"{rel_path}: raw SLJIT codegen token outside codegen layer: {token}")


def verify_database_owned_manager(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/main/database.hpp",
        [
            "class JitManager;",
            "DUCKDB_API JitManager &GetJitManager();",
            "unique_ptr<JitManager> jit_manager;",
        ],
    )
    assert_required_text(
        root,
        "src/main/database.cpp",
        [
            "jit_manager = make_uniq<JitManager>(*this);",
            "JitManager &DatabaseInstance::GetJitManager()",
            "jit_manager.reset();",
        ],
    )


def verify_static_extension(root: Path) -> None:
    assert_required_text(root, "extension/extension_config.cmake", ["duckdb_extension_load(jit_sljit)"])
    assert_required_text(
        root,
        "extension/jit_sljit/CMakeLists.txt",
        ["add_third_party(sljit)", "build_static_extension(jit_sljit", "target_link_libraries(jit_sljit_extension duckdb_sljit)"],
    )


def verify_core_sources_registered(root: Path) -> None:
    assert_required_text(
        root,
        "src/execution/CMakeLists.txt",
        [
            "jit.cpp",
            "jit_expression_ir.cpp",
            "jit_join_runtime.cpp",
            "jit_operator_descriptor.cpp",
            "jit_region_executor.cpp",
            "jit_region_ir.cpp",
            "jit_runtime.cpp",
            "jit_types.cpp",
        ],
    )


def verify_backend_contract_tests(root: Path) -> None:
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "JIT region backend input must not expose DuckDB executor internals",
            "JIT region kernels must execute through the JIT runtime chunk ABI",
            "JIT sink kernels must execute through the JIT sink runtime ABI",
            "JIT source pipeline kernels must advertise the source-prefix executable ABI explicitly",
            "JIT sink pipeline kernels must advertise the sink executable ABI explicitly",
            "JIT lowers integral decompression intrinsic as native scalar projection",
            "integral_decompress",
            "__internal_decompress_integral",
            "JIT canonicalizes no-op optional table filters as constant predicates",
            "__internal_tablefilter_optional",
            "source_filter_fallback_count == 0",
            "JIT lowers constant string prefix predicates as native predicates",
            "string_prefix",
            "function=prefix",
            "JIT lowers constant string substring IN predicates as native predicates",
            "string_substring",
            "function=substring",
            "JIT manager rejects source pipeline kernels without source-prefix ABI",
            "JIT maximal region planner does not emit sink-only ABI candidates",
            "JIT manager rejects full pipeline kernels without full-pipeline ABI",
            "RequireAutoInventorySkipEvent",
            "JIT auto rejects source-helper full pipeline as proof-gap only",
            "JIT auto rejects native-source ungrouped aggregate without production proof",
            "JIT auto skips dynamic-filter join aggregate without measured proof",
            "JIT auto skips conjunctive-filter join aggregate without measured proof",
            "found_hash_aggregate_inventory_skip",
            "no SLJIT auto admission family can match pipeline inventory",
            "hash-aggregate-update",
            "JIT full pipeline ABI dispatch falls back honestly after decline",
            "SLJIT marks full pipeline result collector unsupported without native sink protocol",
            "JIT full pipeline ABI rejects decline after runtime side effects",
            "compiled full pipeline without full-pipeline executable ABI",
            "full pipeline kernel declined",
            "JIT full pipeline kernel declined after using runtime side-effect APIs",
            "JIT maximal region planner skips sink-only decline paths",
            "JIT full pipeline updates count aggregate through native sink update",
            "JIT full pipeline updates decimal sum aggregate through native sink update",
            "JIT full pipeline executes grouped hash aggregate native lookup protocol",
            "JIT full pipeline executes grouped decimal hash aggregate native lookup protocol",
            "JIT full pipeline updates perfect hash aggregate through native sink update",
            "JIT full pipeline executes native hash join build append protocol",
            "JIT hash join build append protocol supports multi-key reference builds",
            "JIT hash join probe lowers native non-equality match predicates",
            "JIT hash join probe keeps non-equality duplicate chains honest",
            "JIT hash join probe and build protocols stay honest across join shapes",
            "JIT full pipeline executes native hash join probe for regular native probe table",
            "JIT native hash join probe marks right semi duplicate chains",
            "jit_native_hash_probe_l",
            "jit_right_semi_build",
            "native_probe_output_mode=mark_build_only",
            "native-hash-join-probe-executable=ready",
            "hash_join_probe(hash_keys=1,conditions=key0<input_index=",
            "predicate1<input_index=",
            "comparison=notequal",
            "hash-join-native-runtime-non-equality-chain-protocol-missing",
            "mark_build_match=true",
            "matched_probe_only",
            "native_probe_output_mode=",
            "JIT region lowering exposes order and top-n native state scan source protocols",
            "JIT full pipeline exposes expanded hash join protocol blockers",
            "order-by-native-state-scan",
            "top-n-native-state-scan",
            "order_by_scan",
            "top_n_scan",
            "join_type=right",
            "join_type=full",
            "join_type=mark",
            "jit_hash_probe_fact_int",
            "jit_hash_probe_fact_text_key",
            "jit_hash_probe_fact_null_key",
            "jit_hash_probe_dim_half",
            "jit_hash_probe_dim_empty",
            "generated native hash join probe",
            "native-hash-join-probe-executable=ready",
            "generated native hash join build append protocol",
            "JIT full pipeline lowers native hash join build through backend",
            "native-aggregate-function-contract=ready",
            "native-grouped-state-contract=ready",
            "native-grouped-state-layout-contract=ready",
            "grouped_state_layout_ready=true",
            "hash-aggregate-native-grouped-state",
            "perfect-hash-aggregate-native-grouped-state",
            "native-grouped-state-blocker=none",
            "requires-native-grouped-state-abi=true",
            "native_state_scan_contract_status=ready",
            "native_state_scan_required_capability=",
            "hash-join-native-state-scan",
            "hash-aggregate-native-state-scan",
            "perfect-hash-aggregate-native-state-scan",
            "ungrouped-aggregate-native-state-scan",
            "native_state_scan_blocker=",
            "native_state_scan_blocker=none",
            "native_grouped_state_contract_status=ready",
            "native_grouped_state_required_capability=",
            "native_grouped_state_blocker=none",
            "regular_hash_table_layout_ready=true",
            "hash_join_layout_offsets=[",
            "hash_join_tuple_size=",
            "hash_join_entry_size=",
            "hash_join_pointer_offset=",
            "lhs_output_column_indices=",
            "hash_join_native_protocol_blocker=none",
            "native_hash_join_probe_contract_status=ready",
            "native_hash_join_probe_required_capability=",
            "hash-join-native-probe",
            "native_hash_join_probe_blocker=none",
            "native_hash_join_build_contract_status=ready",
            "native_hash_join_build_required_capability=",
            "hash-join-native-build",
            "native_hash_join_build_blocker=none",
            "native_hash_aggregate_lookup_required_capability=",
            "hash-aggregate-native-lookup",
            "native_hash_aggregate_lookup_contract_status=ready",
            "native_hash_aggregate_lookup_blocker=none",
            "hash-aggregate-distinct-grouped-state-protocol-boundary",
            "hash-aggregate-distinct-lookup-protocol-boundary",
            "perfect-hash-aggregate-native-lookup",
            "REQUIRE_FALSE(StringUtil::Contains(event.reason, \"typed-hash-aggregate-lookup-helper\"))",
            "REQUIRE_FALSE(StringUtil::Contains(event.ir, \"typed_hash_aggregate_lookup_helper\"))",
            "REQUIRE_FALSE(StringUtil::Contains(event.reason, \"typed_hash_join_build_helper\"))",
            "REQUIRE_FALSE(StringUtil::Contains(event.reason, \"typed_hash_join_probe_helper\"))",
            "generated native hash aggregate lookup and state update",
            "JIT region lowering exposes stateful source protocol candidates",
            "function=hash_join_probe",
            "function=hash_aggregate_scan",
            "function=ungrouped_aggregate_scan",
            "SLJIT marks full pipeline result collector unsupported without native sink protocol",
            "ungrouped_aggregate_update",
            "hash_aggregate_update",
            "perfect_hash_aggregate_update",
            "perfect-hash-aggregate-update",
            "found_compiled_hash_aggregate_sink",
            "kernel=generic-runtime-loop",
            "found_aggregate_sink_protocol",
            "backend cannot generate executable code for this whole region",
            "JIT auto region selection uses maximal transform candidates",
            "JIT auto precheck skips region candidates before backend analysis",
            "JitAdmissionInfo &info",
            "SET jit_event_log_size=0",
            "JIT region capability requires explicit compiled execution mode",
            "contract backend only admits maximal transform candidates",
            "candidate_context_pipeline_shape",
            "candidate_scope",
            "full_pipeline",
        ],
    )
    assert_required_text(
        root,
        "test/sql/jit/test_jit_framework.test",
        [
            "jit_resume_state_unnest",
            "candidate_scope='source_pipeline'",
            "candidate_has_table_scan_source=true",
            "candidate_arithmetic_projection_count=2",
            "candidate_integer_arithmetic_projection_count=2",
            "candidate_non_integer_arithmetic_projection_count=0",
            "candidate_reference_projection_count=1",
            "region_execution_form='none'",
            "status='unsupported'",
            "execution_mode='unsupported'",
            "backend cannot generate executable code for this whole region",
            "fusion-blocker:source-fusion-gap:requires-native-source",
            "hash aggregate sink has no aggregate payload bindings",
            "reason NOT LIKE '%hash aggregate native lookup and state update protocol missing%'",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "Integral compressed-materialization expressions are core typed intrinsics",
            "`__internal_decompress_integral_*`",
            "No-op optional table filters are canonicalized to `constant(true)`",
            "Constant-pattern `prefix(varchar, varchar)` is a core typed string predicate",
            "it allocates string payload storage",
        ],
    )


def verify_region_executor_resume_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "virtual bool CanExecuteSourcePipeline() const;",
            "virtual bool CanExecuteFullPipeline() const;",
            "virtual bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result);",
        ],
    )
    assert_no_text(
        root,
        [Path("src/include/duckdb/execution/jit/runtime.hpp")],
        [
            "SinkChunk()",
            "SinkThroughTypedHelper",
            "JitFullPipelineSinkFunction",
            "SinkHashJoinBuildThroughProtocol",
            "ProbeHashJoinThroughProtocol",
            "ExecuteOperatorThroughTypedHelper",
        ],
    )
    assert_no_text(
        root,
        [Path("src"), Path("extension/jit_sljit")],
        [
            "JitSinkHashJoinBuild",
            "JitProbeHashJoin",
            "JitSinkHashAggregatePayload",
            "JitHashJoinKeyBinding",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/region_executor.hpp",
        ["static bool TryExecuteSourcePrefix(PipelineExecutor &executor, DataChunk &source_chunk"],
    )
    assert_required_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        [
            "DataChunk &PipelineExecutor::GetSourceChunkForInitialIdx(idx_t initial_idx)",
            "source_chunk = &GetSourceChunkForInitialIdx(source_chunk_initial_idx)",
            "JitRegionExecutor::HasSourcePipelineKernel(*this)",
            "HasJitSourcePipelineKernelRequiringNativeSource(jit_kernels)",
            "source_fetch_time_us",
            "JitRegionExecutor::TryExecuteSourcePrefix(*this, fetch_chunk, result, res, source_fetch_time_us",
            "ExecutePushInternal(*source_chunk, chunk_budget, source_chunk_initial_idx)",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "source_helper_output_rows",
            "source_helper_invocation_count",
            "source_helper_runtime_time_us",
            "source_native_output_rows",
            "source_native_invocation_count",
            "source_native_runtime_time_us",
            "generated_body_runtime_time_us",
            "JitRuntimeMetrics",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/include/duckdb/execution/jit/runtime.hpp"),
            Path("src/execution/jit.cpp"),
            Path("src/execution/jit_runtime.cpp"),
            Path("src/execution/jit_region_executor.cpp"),
            Path("src/function/table/system/duckdb_jit_events.cpp"),
            Path("src/function/table/system/duckdb_jit_counters.cpp"),
            Path("src/function/table/system/duckdb_jit_kernel_counters.cpp"),
            Path("benchmark/tpch/jit/tpch_schema.py"),
            Path("benchmark/tpch/jit/tpch_trace.py"),
            Path("benchmark/tpch/jit/verify_tpch_trace.py"),
            Path("benchmark/jit/jit_sql_trace.py"),
            Path("benchmark/jit/verify_jit_sql_trace.py"),
        ],
        [
            "sink_helper_",
            "operator_helper_input_rows",
            "operator_helper_output_rows",
            "operator_helper_invocation",
            "operator_helper_runtime_time_us",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "TryExecuteFullPipeline",
            "PipelineJitFullPipelineRuntime runtime(executor, max_chunks, trace_runtime)",
            "idx_t MaxChunks() const override",
            "CanExecuteFullPipeline",
            "PushFinalize",
            "SinkRuntimeTime",
            "JitRuntimeMetrics Metrics(int64_t runtime_time_us) const",
            "generated_body_runtime_time_us",
            "remaining_sink_chunk = true",
            "cannot fetch source data after a blocked sink",
            "cannot fetch source data after a finished sink",
            "TryFlushCachingOperators",
            "core flushed final operators and finalized sink",
            "declined after using runtime side-effect APIs",
            "full pipeline kernel declined runtime input",
            "CanExecuteSourcePipeline",
            "RequiresNativeSource()",
            "DataChunk &prefix_result = executor.GetSourceChunkForInitialIdx(candidate_end)",
            "*chunks[operator_idx + 1]",
            "source pipeline kernel executed;next_operator_idx=",
            "generated prefix execution",
            "ExecuteFallback",
            "!executor.in_process_operators.empty()",
            "region kernel skipped because executor has in-process operators",
            "reference executor continues current resume state",
            "kernel.TrySink(executor.context, input, sink_input, sink_result)",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "Full-pipeline kernels are already a JIT-local multi-chunk execution unit",
            "`JitFullPipelineRuntime::MaxChunks()`",
            "not as a global `STANDARD_VECTOR_SIZE` change",
            "`source_native_invocation_count` or `source_helper_invocation_count`",
            "greater than one on a single full-pipeline runtime event",
        ],
    )
    assert_required_text(
        root,
        "test/sql/jit/test_jit_framework.test",
        [
            "jit_source_schema_change",
            "candidate_scope='full_pipeline'",
            "hash aggregate sink has no aggregate payload bindings",
            "full-pipeline-native-sink-update",
            "execution:unsupported",
        ],
    )
    assert_no_text(root, [Path("extension/jit_sljit/include/sljit_region_plan.hpp")], ["SINK_HELPER_BOUNDARY"])
    assert_no_text(
        root,
        [Path("src/execution/jit_operator_descriptor.cpp")],
        ["BuildJitDescriptorNativeSourceContract"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "PlanSljitFullPipelineSinkNode",
            "PlanSljitSinkNode(node)",
            "full pipeline sink requires native sink or operator update protocol",
            "hash-join-build-protocol-fallback",
            "full-pipeline-native-protocol-stage",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "TryExecuteFullPipeline",
            "ExecuteNativeFullPipeline",
            "SLJIT full pipeline region has no native sink operator",
            "SLJIT hash aggregate update reached runtime without native state update codegen",
            "SLJIT perfect hash aggregate update reached runtime without native state update codegen",
            "ExecuteNativeHashAggregateUpdate",
            "ExecuteNativeGroupedAggregateUpdate",
            "BindNativeHashAggregateStates",
            "FinishNativeHashAggregateUpdate",
        ],
    )
    assert_no_text(
        root,
        [
            Path("extension/jit_sljit/sljit_region_runtime.cpp"),
            Path("src/include/duckdb/execution/jit/runtime.hpp"),
            Path("src/include/duckdb/execution/jit/aggregate_runtime.hpp"),
        ],
        [
            "SinkThroughTypedHelper",
            "SinkHashJoinBuildThroughProtocol",
            "SljitFullPipelineHashAggregateUpdate",
            "SljitFullPipelinePerfectHashAggregateUpdate",
            "SljitFullPipelineUngroupedAggregateUpdate",
            "SLJIT hash join build reached runtime without native build codegen",
            "SLJIT hash join probe reached runtime without native probe codegen",
            "runtime.SinkChunk()",
            "runtime.Sink(",
        ],
    )
    assert_no_text(root, [Path("src/execution/jit_region_ir.cpp")], ["JitRegionTypesEqual"])
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "Region JIT must also respect DuckDB's resumable operator protocol.",
            "a prefix region must not",
            "run again on the same source chunk",
            "as `executor_fallback`",
            "`CanExecuteSourcePipeline()`",
            "`CanExecuteFullPipeline()`",
            "`JitRegionKernel::TryExecuteFullPipeline(JitFullPipelineRuntime &,",
                "no generic",
                "no typed sink callback",
                "finalization",
            "declines at runtime must do so before calling source or sink side-effect APIs",
            "Full-pipeline eligibility is also an executor-entry contract",
            "RequiredPartitionInfo().AnyRequired()",
            "maximal `source_pipeline` prefix",
            "must not manufacture post-source or sink-suffix candidates",
            "Sink-owned suffixes are not planner products",
            "reserved runtime contract",
            "core-owned source-prefix runtime ABI",
            "without that source-prefix executable ABI",
            "architecture error",
        ],
    )


def verify_native_operator_runtime_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/operator_runtime.hpp",
        [
            "struct JitNativeOperatorBinding",
            "struct JitNativeHashJoinProbeBinding",
            "JitNativeHashJoinTableLayout table_layout",
            "probe_key_input_indices",
            "rhs_output_column_count",
            "JitMaterializeNativeHashJoinProbe",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/join_runtime.hpp",
        [
            "uint64_t pointer_mask",
            "uint64_t salt_mask",
            "const ht_entry_t *entries",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "BindNativeOperator(idx_t operator_index, DataChunk &input, const JitRegionOperatorInfo &operator_info",
            "JitNativeOperatorBinding &binding",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/physical_operator.hpp",
        [
            "BindJitNativeOperator(ExecutionContext &context, DataChunk &input, GlobalOperatorState &gstate",
            "const JitRegionOperatorInfo &operator_info",
            "JitNativeOperatorBinding &binding",
        ],
    )
    assert_required_text(
        root,
        "src/execution/physical_operator.cpp",
        [
            "PhysicalOperator::BindJitNativeOperator",
            "jit-native-operator-binding-unsupported",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/operator/join/physical_hash_join.hpp",
        ["BindJitNativeOperator(ExecutionContext &context, DataChunk &input, GlobalOperatorState &gstate"],
    )
    assert_required_text(
        root,
        "src/execution/operator/join/physical_hash_join.cpp",
        [
            "PhysicalHashJoin::BindJitNativeOperator",
            "operator_info.kind != JitRegionOperatorKind::HASH_JOIN_PROBE",
            "protocol.native_probe_contract.status != JitRegionStateContractStatus::READY",
            "!protocol.native_probe_shape_ready",
            "JitGetNativeHashJoinTableLayout(*sink.hash_table, table_layout)",
            "!table_layout.single_match_probe",
            "protocol.native_probe_output_mode == JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD",
            "protocol.native_probe_output_mode == JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY",
            "hash-join-native-runtime-dictionary-chain-layout-missing",
            "hash-join-native-runtime-resumable-chain-protocol-missing",
            "binding.hash_join_probe.table_layout = std::move(table_layout)",
            "binding.hash_join_probe.probe_key_input_indices = std::move(probe_key_input_indices)",
            "binding.hash_join_probe.rhs_output_column_count = protocol.rhs_output_column_count",
            "binding.hash_join_probe.output_mode = protocol.native_probe_output_mode",
            "JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD",
            "JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY",
            "expected_column_count",
            "JitMaterializeNativeHashJoinProbe",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "BindNativeOperator(idx_t operator_index, DataChunk &input, const JitRegionOperatorInfo &operator_info",
            "jit-native-operator-runtime-index-out-of-range",
            "jit-native-operator-runtime-missing-local-state",
            "jit-native-operator-runtime-missing-global-state",
            "op.BindJitNativeOperator(executor.context, input, *op.op_state",
            "SetRuntimeDeclineReason",
            "ConsumeRuntimeDeclineReason",
        ],
    )
    assert_no_forbidden_includes(
        root,
        [Path("extension/jit_sljit")],
        [
            "duckdb/execution/join_hashtable",
            "duckdb/execution/operator/join/physical_hash_join",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "`operator_runtime.hpp` is the execution-time binding surface",
            "runtime asks the owning `PhysicalOperator` to bind",
            "`JitRegionOperatorInfo` to a `JitNativeOperatorBinding`",
            "Backends consume that binding",
            "they do not include `join_hashtable.hpp`",
            "`JoinHashTable::Probe`, `ScanStructure::Next`, or any whole-operator executor",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_native_types.hpp",
        [
            "enum class SljitNativeHashJoinKeyKind",
            "struct SljitNativeHashJoinProbeInput",
            "row_pointers",
            "match_sel",
            "aux_next_ptrs",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_codegen.cpp",
        [
            "BuildSljitHashJoinProbe",
            "EmitMarkHashJoinBuildChain",
            "mark_build_match",
            "found_match_offset",
            "pointer_offset",
            "mark_build_only",
            "EmitDuckDBMurmurHash64",
            "EmitLoadHashJoinSourceIndex",
            "EmitJumpIfHashJoinSourceNull",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "ExecuteNativeHashJoinProbe",
            "runtime.BindNativeOperator",
            "output mode mismatch",
            "found-match offset mismatch",
            "dictionary chain pointers",
            "JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY",
            "JitMaterializeNativeHashJoinProbe",
        ],
    )


def verify_native_sink_update_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/aggregate_runtime.hpp",
        [
            "struct JitNativeUngroupedAggregateState",
            "JitAggregateUpdateKind update_kind",
            "data_ptr_t state",
            "idx_t *count",
            "struct JitGroupedAggregateGroupBinding",
            "JitBindNativeUngroupedAggregateStates",
            "JitBindNativePerfectHashAggregateStates",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/include/duckdb/execution/jit/aggregate_runtime.hpp"),
            Path("src/execution/operator/aggregate/physical_perfecthash_aggregate.cpp"),
            Path("src/execution/operator/aggregate/physical_ungrouped_aggregate.cpp"),
        ],
        [
            "JitSinkPerfectHashAggregatePayload",
            "JitSinkUngroupedAggregatePayload",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "BindNativeUngroupedAggregateStates",
            "RecordNativeSinkResult",
        ],
    )
    assert_required_text(
        root,
        "src/execution/operator/aggregate/physical_ungrouped_aggregate.cpp",
        [
            "JitBindNativeUngroupedAggregateStates",
            "UngroupedAggregateLocalSinkState",
            "aggregate_data",
            "counts",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "BindNativeUngroupedAggregateStates",
            "JitBindNativeUngroupedAggregateStates(sink_input, requested_states, bound_states)",
            "RecordNativeSinkResult",
            "RecordNativeSinkResult(idx_t input_rows, SinkResultType sink_result)",
            "RecordSinkResult(input_rows, sink_result)",
            "JIT full pipeline native sink update returned BLOCKED",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_region_plan.hpp",
        [
            "struct SljitNativeUngroupedAggregateUpdatePlan",
            "native_ungrouped_aggregate_updates",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_region_executable.hpp",
        [
            "struct SljitExecutableUngroupedAggregateUpdate",
            "SljitNativeUngroupedAggregateFunction function",
            "native_ungrouped_aggregate_updates",
            "grouped_aggregate_groups",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_native_types.hpp",
        [
            "STRING_SUBSTRING_IN_LIST_CONSTANT",
            "struct SljitNativeUngroupedAggregateInput",
            "state_count",
            "source_validity",
            "source_sel",
            "state_value_offset",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_native_plan.hpp",
        ["TryReadNativeStringSubstringInListConstant"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_native_plan.cpp",
        [
            "TryReadNativeStringSubstringInListConstant",
            "JitExpressionIntrinsicKind::STRING_SUBSTRING",
            "STRING_SUBSTRING_IN_LIST_CONSTANT",
            "IsNativeAsciiString",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_native_codegen.cpp",
        [
            "EmitLoadPredicateStringDataPointer",
            "STRING_SUBSTRING_IN_LIST_CONSTANT",
            "predicate.substring_length",
            "predicate.string_constants",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "TryPlanSljitNativeUngroupedAggregateUpdate",
            "SLJIT native aggregate update supports count-star/count/sum only",
            "SLJIT native sum update supports optional BIGINT or HUGEINT state",
            "SLJIT native sum update requires one INT64 payload child",
            "payload_type=",
            "native_ungrouped_aggregate_updates.push_back",
            "native_op->native_ungrouped_aggregate_updates.size() == node.sink->aggregates.size()",
            "native_op->native_ungrouped_aggregate_updates.clear()",
            "native-aggregate-update-contract=ready",
            "native-aggregate-update-executable=ready",
            "native-aggregate-function-contract=ready",
            "native-grouped-state-contract=",
            "JitRegionStateContractStatusToString",
            "native-grouped-state-required-capability",
            "native-grouped-state-blocker",
            "PlanSljitPerfectHashAggregateSinkNode",
            "SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE",
            "JitGroupedAggregateGroupBinding",
            "perfect_hash_aggregate_update",
            "perfect-hash-aggregate-update",
            "requires-native-grouped-state-abi=true",
            "full-pipeline-native-sink-update",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/common.hpp",
        [
            "enum class JitRegionABI",
            "CHUNK_TRANSFORM",
            "SOURCE_PREFIX",
            "SINK_SUFFIX",
            "FULL_PIPELINE",
            "STATE_SCAN",
            "JitRegionABIIsChunkTransform",
            "JitRegionABIIsSourcePipeline",
            "JitRegionABIIsSinkPipeline",
            "JitRegionABIIsFullPipeline",
            "JitRegionABIOwnsSource",
            "JitRegionABIOwnsSink",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit")],
        [
            "SljitContractOwns",
            "source_pipeline_p",
            "sink_pipeline_p",
            "full_pipeline_p",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution"), Path("src/parallel")],
        [
            "contract.abi == JitRegionABI::SOURCE_PREFIX || contract.abi == JitRegionABI::STATE_SCAN",
            "contract.abi == JitRegionABI::SINK_SUFFIX",
            "contract.abi == JitRegionABI::FULL_PIPELINE",
            "contract.abi == JitRegionABI::CHUNK_TRANSFORM",
            "JitRegionContractRequires",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_codegen.cpp",
        [
            "BuildSljitUngroupedCountStarUpdate",
            "BuildSljitUngroupedCountUpdate",
            "BuildSljitUngroupedSumInt64Update",
            "EmitJumpIfUngroupedSourceNull",
            "EmitAddUngroupedStateCount",
            "EmitSetUngroupedStateIsSet",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "ExecuteNativeUngroupedAggregateUpdate",
            "BuildNativeUngroupedAggregateStateRequests",
            "runtime.BindNativeUngroupedAggregateStates",
            "JitBindNativeUngroupedAggregateStates",
            "runtime.RecordNativeSinkResult",
            "UnifiedVectorFormat",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit")],
        [
            "SljitFusedUngroupedAggregateInput",
            "SljitFusedUngroupedAggregateFunction",
            "SljitFusedUngroupedSumKernel",
            "BuildSljitFusedFilterProjectionUngroupedSum",
            "BuildSljitFusedProjectionUngroupedSum",
            "SljitFusedFilterProjectionInput",
            "SljitFusedFilterProjectionFunction",
            "SljitFusedFilterProjectionKernel",
            "BuildSljitFusedIntegerFilterProjection",
            "CreateSljitFusedFilterProjectionKernel",
            "CanFuseNativeFilterProjectionRegion",
            "SLJIT_SOURCE_PREFIX_FUSED_FILTER_PROJECTION_SHAPE",
            "sljit:source-prefix:fused-filter-projection",
            "SljitFusedPerfectHashAggregateInput",
            "SljitFusedPerfectHashAggregateFunction",
            "SljitFusedDirectPerfectHashAggregateKernel",
            "BuildSljitFusedDirectPerfectHashAggregate",
            "CreateSljitFusedDirectPerfectHashAggregateKernel",
            "CanFuseNativePerfectHashAggregateRegion",
            "SLJIT_FULL_PIPELINE_FUSED_PERFECT_HASH_AGGREGATE_UPDATE_SHAPE",
            "sljit:full-pipeline:fused-perfect-hash-aggregate-update",
            "CanFuseNativeFilterProjectionUngroupedSumRegion",
            "CanFuseNativeProjectionUngroupedSumRegion",
            "SLJIT_FULL_PIPELINE_FILTER_PROJECTION_UNGROUPED_SUM_SHAPE",
            "SLJIT_FULL_PIPELINE_PROJECTION_UNGROUPED_SUM_SHAPE",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region.cpp",
        [
            "execution_mode != JitExecutionMode::NATIVE",
            "execution:native-sljit-region-",
            "BuildSljitExecutableRegion",
            "SLJIT region compile requires native compiled execution mode",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "result.execution_reason = \"execution:native-sljit-region-\" + DescribeNativeRegionShape(region)",
        ],
    )
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "JIT full pipeline updates count aggregate through native sink update",
            "JIT full pipeline updates decimal sum aggregate through native sink update",
            "SELECT count(j) FROM jit_native_count",
            "SELECT sum(price * discount) FROM jit_native_decimal_sum",
            "sink:UNGROUPED_AGGREGATE:native",
            "generated native ungrouped aggregate state update",
            "native-aggregate-update-executable=ready",
            "aggregate0_native_update=sum",
            "JitRegionSourceExecutionKind::NATIVE_SOURCE",
            "generated source-prefix table scan filters",
            "source-strategy=prepared-unfiltered-native-source",
            "owns-source-filters=true",
            "source-execution:native-source",
            "execution:native-sljit-region-filter-projection-ungrouped-aggregate-update",
            "execution:native-sljit-region-ungrouped-aggregate-update",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "Native Sink-Update ABI",
            "JitNativeUngroupedAggregateState",
            "JitBindNativeUngroupedAggregateStates",
            "all-or-nothing",
            "A partial native aggregate update is an architecture bug",
            "native implementations cover `count(*)` and nullable `count(x)`",
            "optional BIGINT or HUGEINT state",
            "`AddToHugeint` aggregate helper",
            "Grouped hash and perfect-hash aggregate have separate contracts",
            "native_state_scan_*",
            "native_grouped_state_*",
            "native_hash_aggregate_lookup_*",
            "native-aggregate-function-contract=ready",
            "native-grouped-state-contract=ready",
            "native-grouped-state-required-capability=...-native-grouped-state",
            "native-grouped-state-blocker=none",
            "hash-aggregate-native-lookup",
            "native_hash_aggregate_lookup_contract_status=ready",
            "native_hash_aggregate_lookup_blocker=none",
            "hash-aggregate-distinct-grouped-state-protocol-boundary",
            "hash-aggregate-distinct-lookup-protocol-boundary",
            "perfect-hash-aggregate-native-lookup",
            "JitBindNativeHashAggregateStates",
            "FindOrCreateAggregateStatesFromBoundGroups",
            "JitBindNativePerfectHashAggregateStates",
            "not a typed sink helper payload callback",
            "perfect_hash_aggregate_update",
        ],
    )
    assert_no_forbidden_includes(
        root,
        [Path("extension/jit_sljit")],
        [
            "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate",
            "duckdb/execution/operator/aggregate/physical_hash_aggregate",
            "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit")],
        [
            "UngroupedAggregateLocalSinkState",
            "aggregate_data",
            "state.state.counts",
        ],
    )


def verify_region_selection_contract(root: Path) -> None:
    assert_no_text(
        root,
        [
            Path("src/execution/jit.cpp"),
            Path("src/execution/jit_runtime.cpp"),
            Path("src/include/duckdb/execution/jit"),
            Path("extension/jit_sljit"),
        ],
        [
            "TrySkipAutoRegionsBeforePipelineInventory",
            "MayHaveAutoAdmissionCandidate",
            "JitRegionPipelineInfo",
            "before region inventory",
            "pre-inventory",
            "TrySkipAutoRegionsBeforeFullLowering",
            "TryInventoryJitRegion",
            "JitRegionLoweringMode::INVENTORY",
            "JitRegionLoweringMode::FULL",
            "before full region IR lowering",
            "JitRegionCandidateSourceStrategyScore",
            "source_strategy_score",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit.cpp",
        [
            "JitAnalyzedRegionCandidate",
            "SelectJitAutoRegionCandidates",
            "SelectJitForceRegionCandidates",
            "SelectJitRegionCandidatesByScore",
            "JitRegionSelectionBetter",
            "explain_inventory",
            "JitRegionPipelineInventoryMode::ADMISSION",
            "JitRegionPipelineInventoryMode::DIAGNOSTIC",
            "BuildJitPipelineDescriptor(pipeline)",
            "TryInspectJitRegionPipeline(*pipeline_descriptor",
            "TryLowerJitRegion(*pipeline_descriptor)",
            "MayHaveAutoAdmissionRule",
            "ShouldRecordAutoAdmissionSkip",
            "admission.has_admission",
            "admission.rule_present",
            "ShouldRecordJitDecisionCounters(context)",
            "inventory.get()",
            "Settings::Get<JitDumpIrSetting>(context)",
            "policy == JitPolicyMode::AUTO",
            "ShouldRecordJitDecisionCounters(context) || Settings::Get<JitDumpIrSetting>(context)",
            "if (!region_ir)",
            "before backend analysis",
            "!Settings::Get<JitDumpIrSetting>(context)",
            "!ShouldRecordAutoAdmissionSkip(context, precheck_info)",
            "not selected by auto non-overlapping admission selection",
            "backend cannot generate executable code for ",
            "this whole region",
            "CompileRegion(input)",
            "compiled source pipeline without source-prefix executable ABI",
            "compiled sink pipeline without sink executable ABI",
            "HasExecutableBody",
            "CanExecuteSinkPipeline",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/pipeline_descriptor.hpp",
        [
            "enum class JitPipelineOperatorRole",
            "struct JitPipelineOperatorEntry",
            "struct JitPipelineDescriptor",
            "string source_boundary_reason",
            "JitOperatorSourceDescriptor source_payload",
            "JitRegionOperatorInfo operator_payload",
            "JitRegionSinkInfo sink_payload",
            "JitCompiledOperatorContract source_contract",
            "JitCompiledOperatorContract operator_contract",
            "JitCompiledOperatorContract sink_contract",
            "bool native_source",
            "bool native_operator",
            "bool native_sink",
            "BuildJitPipelineDescriptor(Pipeline &pipeline)",
        ],
    )
    assert_no_text(
        root,
        [Path("src/include/duckdb/execution/jit/pipeline_descriptor.hpp")],
        ["JitOperatorDescriptor descriptor"],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/lowering.hpp",
        [
            "JitRegionPipelineInventoryMode",
            "ADMISSION",
            "DIAGNOSTIC",
            "TryLowerJitRegion",
            "TryInspectJitRegionPipeline",
            "const JitPipelineDescriptor &descriptor",
        ],
    )
    assert_no_text(
        root,
        [Path("src/include/duckdb/execution/jit/lowering.hpp")],
        [
            "class Pipeline;",
            "TryInspectJitRegionPipeline(Pipeline",
            "TryLowerJitRegion(Pipeline",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionPipelineInventory",
            "bool explain",
            "MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_runtime.cpp",
        [
            "JitBackend::MayHaveAutoAdmissionRule(JitCompileTarget, const JitRegionPipelineInventory",
            "JitBackend::MayHaveAutoAdmissionRule(JitCompileTarget, const JitRegionCandidate",
        ],
    )
    assert_required_text(
        root,
        "src/execution/CMakeLists.txt",
        ["jit_pipeline_descriptor.cpp"],
    )
    assert_required_text(
        root,
        "src/execution/jit_pipeline_descriptor.cpp",
        [
            "BuildJitPipelineDescriptor(Pipeline &pipeline)",
            "BuildJitPipelineOperatorEntry",
            "GetJitOperatorDescriptor()",
            "SliceJitPipelineCompiledContract",
            "JitPipelineCompiledContractHasNativeSource",
            "JitPipelineCompiledContractHasNativeOperator",
            "JitPipelineCompiledContractHasNativeSink",
            "entry.source_boundary_reason = std::move(descriptor.source_boundary_reason)",
            "entry.source_payload = std::move(descriptor.source)",
            "entry.operator_payload = std::move(descriptor.operator_info)",
            "entry.sink_payload = std::move(descriptor.sink)",
            "pipeline.GetSource()",
            "pipeline.GetIntermediateOperators()",
            "pipeline.GetSink()",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_ir.cpp",
        [
            "BuildJitRegionInventoryFeatureShape",
            "DescribeJitRegionInventoryPipelineShape",
            "AccumulateJitRegionInventorySource",
            "AccumulateJitRegionInventoryOperator",
            "AccumulateJitRegionInventorySink",
            "IsJitRegionPipelineWrapperOnlySource",
            "IsJitRegionPipelineInventoryWorkloadRelevant",
            "inventory.source_operator_name == \"HASH_JOIN\"",
            "!inventory.source_produces_rows",
            "source_produces_rows = source_payload.hash_join_protocol.source_produces_rows",
            "CREATE_TABLE_AS",
            "RESULT_COLLECTOR",
            "EXPLAIN_ANALYZE",
            "TryInspectJitRegionPipeline",
            "mode == JitRegionPipelineInventoryMode::DIAGNOSTIC",
            "!IsJitRegionPipelineInventoryWorkloadRelevant(*result)",
            "TryInspectJitRegionPipeline(const JitPipelineDescriptor &descriptor",
            "TryBuildJitRegion(const JitPipelineDescriptor &descriptor)",
            "BuildJitRegionSourceInfo",
            "BuildJitRegionSinkInfo",
            "DescribeJitRegionNativeSourceContract",
            "BuildJitRegionGenericScanSourceInfo",
            "DescribeJitRegionSinkInfo",
            "BuildJitRegionCandidateTraits",
            "DescribeJitRegionCandidateTraits",
            "expression_traits_known",
            "resumable_operator_count",
            "resumable_operators=",
            "source_filter_count",
            "source_filter_expression_count",
            "source_filter_fallback_count",
            "source_comparison_filter_count",
            "source_integer_comparison_filter_count",
            "source_non_integer_comparison_filter_count",
            "source_conjunction_filter_count",
            "source_projected_column_count",
            "source_returned_column_count",
            "arithmetic_projection_count",
            "integer_arithmetic_projection_count",
            "non_integer_arithmetic_projection_count",
            "comparison_filter_count",
            "integer_comparison_filter_count",
            "non_integer_comparison_filter_count",
            "conjunction_filter_count",
            "JitExpressionContainsBinaryOpWithOperandType",
            "JitExpressionContainsKind",
            "inventory.has_scan_source",
            "traits.has_table_scan_source",
            "hash_join_keys",
            "groups",
            "TryLowerJitRegion(const JitPipelineDescriptor &descriptor)",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_region_ir.cpp")],
        [
            "BuildJitPipelineDescriptor(Pipeline &pipeline)",
            "GetJitOperatorDescriptor()",
            "JitOperatorDescriptor descriptor",
            "entry.descriptor",
            "SliceJitCompiledContract",
            "JitCompiledContractHasNativeProtocol",
            "JitCompiledStageIsSourceRole",
            "TryInspectJitRegionPipeline(Pipeline",
            "TryLowerJitRegion(Pipeline",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/region.hpp",
        [
            "struct JitRegionPipelineInventory",
            "bool has_source",
            "bool has_sink",
            "bool has_scan_source",
            "bool has_hash_join_operator",
            "bool has_hash_join_sink",
            "bool has_hash_aggregate_sink",
            "string feature_shape",
            "struct JitRegionNativeSourceContract",
            "JitRegionNativeSourceStatus status",
            "struct JitRegionNativeStateScanContract",
            "struct JitRegionNativeGroupedStateContract",
            "JitRegionStateContractStatus status",
            "string required_capability",
            "string protocol_version",
            "string blocker",
            "JitRegionNativeStateScanContract native_state_scan_contract",
            "JitRegionNativeGroupedStateContract native_grouped_state_contract",
            "struct JitRegionCandidateTraits",
            "JitRegionCandidateTraits upstream_traits",
            "JitRegionCandidateTraits context_traits",
            "JitRegionCandidateTraits continuation_traits",
            "bool has_table_scan_source",
            "bool expression_traits_known",
            "idx_t source_filter_count",
            "idx_t source_filter_expression_count",
            "idx_t source_filter_fallback_count",
            "idx_t source_comparison_filter_count",
            "idx_t source_integer_comparison_filter_count",
            "idx_t source_non_integer_comparison_filter_count",
            "idx_t source_conjunction_filter_count",
            "idx_t source_projected_column_count",
            "idx_t source_returned_column_count",
            "idx_t arithmetic_projection_count",
            "idx_t integer_arithmetic_projection_count",
            "idx_t non_integer_arithmetic_projection_count",
            "idx_t comparison_filter_count",
            "idx_t integer_comparison_filter_count",
            "idx_t non_integer_comparison_filter_count",
            "idx_t conjunction_filter_count",
            "JitRegionCandidateTraits traits",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_operator_descriptor.cpp",
        [
            "PhysicalTableScan::GetJitOperatorDescriptor",
            "PhysicalHashJoin::GetJitOperatorDescriptor",
            "PhysicalHashAggregate::GetJitOperatorDescriptor",
            "PhysicalPerfectHashAggregate::GetJitOperatorDescriptor",
            "PhysicalUngroupedAggregate::GetJitOperatorDescriptor",
            "BuildJitDescriptorHashJoinBoundaryReason",
            "BuildJitDescriptorHashAggregateBoundaryReason",
            "BuildJitDescriptorNativeStateScanContract",
            "BuildJitDescriptorNativeOperatorContract",
            "MarkJitDescriptorNativeOperatorContractReady",
            "MarkJitDescriptorNativeStateScanContractBlocked",
            "AppendJitDescriptorNativeOperatorReason",
            "hash-join-native-state-scan",
            "hash-join-source-does-not-produce-rows-for-join-type",
            "source_produces_rows=",
            "hash-aggregate-native-state-scan",
            "perfect-hash-aggregate-native-state-scan",
            "ungrouped-aggregate-native-state-scan",
            "hash-join-native-probe",
            "hash-join-native-build",
            "IsJitDescriptorNativeHashJoinOwnedJoinType",
            "BuildJitDescriptorHashJoinProbeNativeShapeBlocker",
            "BuildJitDescriptorHashJoinBuildAppendShapeBlocker",
            "JoinType::LEFT",
            "JoinType::SEMI",
            "JoinType::ANTI",
            "DuckDB hash join build sink protocol",
            "hash-aggregate-native-lookup",
            "perfect-hash-aggregate-native-lookup",
            "BuildJitDescriptorHashJoinBuildKeyInputs",
            "BuildJitDescriptorHashJoinProbeKeyInputs",
            "BuildJitDescriptorHashAggregateInputs",
            "JitRegionSinkKind::HASH_JOIN_BUILD",
            "JitRegionSinkKind::HASH_AGGREGATE_UPDATE",
            "JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_operator_descriptor.cpp")],
        [
            "BuildJitDescriptorTypedHelperContract",
            "MarkJitDescriptorTypedHelperContractReady",
            "AppendJitDescriptorTypedHelperReason",
            "BuildJitDescriptorHashJoinTypedHelperBlocker",
            "BuildJitDescriptorHashAggregateTypedLookupHelperBlocker",
            "IsJitDescriptorNativeHashJoinFixedWidthType",
            "BuildJitDescriptorNativeHashJoinTypeBlocker",
            "hash-join-native-key-type",
            "hash-join-native-payload-type",
            "hash-join-native-probe-type",
            "typed_hash_join_probe_helper",
            "typed_hash_join_build_helper",
            "typed_hash_aggregate_lookup_helper",
            "hash-join-typed",
            "hash-aggregate-typed-lookup-helper",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_region_ir.cpp")],
        [
            "PhysicalHashJoin",
            "PhysicalHashAggregate",
            "PhysicalPerfectHashAggregate",
            "PhysicalUngroupedAggregate",
            "PhysicalTableScan",
            "physical_hash_join",
            "physical_hash_aggregate",
            "physical_perfecthash_aggregate",
            "physical_ungrouped_aggregate",
            "physical_table_scan",
            "Cast<PhysicalHash",
            "Cast<PhysicalPerfectHash",
            "Cast<PhysicalUngrouped",
            "Cast<PhysicalTableScan",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/region.hpp",
        [
            "struct JitRegionSinkInfo",
            "JitRegionSinkKind kind",
            "vector<JitRegionProtocolField> fields",
            "struct JitRegionTableScanProtocol",
            "source_prefix_input_column_count",
            "source_prefix_input_types",
            "source_prefix_output_projection_map",
            "source_prefix_filter_column_map",
            "source_prefix_requires_unfiltered_input",
            "source_prefix_filter_prune_required",
            "source_prefix_filter_split_supported",
            "JitRegionTableScanProtocol table_scan_protocol",
            "struct JitRegionHashJoinProtocol",
            "struct JitRegionAggregateProtocol",
            "JitRegionHashJoinProtocol hash_join_protocol",
            "JitRegionAggregateProtocol aggregate_protocol",
            "vector<JitRegionHashJoinKeyInput> hash_join_keys",
            "lhs_output_column_indices",
            "vector<JitRegionGroupInput> groups",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_types.cpp",
        [
            "JitRegionSinkKindToString",
            "JitRegionAggregateOperatorKindToString",
            "hash-join-build",
            "hash-aggregate-update",
            "ungrouped-aggregate-update",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_types.cpp",
        [
            "void JitRegionLoweringPlan::SetCompiledExecutionMode",
            "return compiled_execution_mode;",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_types.cpp")],
        [
            "has_compiled_execution_mode",
            "if (HelperCallCount() > 0)",
            "if (NativeCount() > 0)",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "BuildSljitRegionCandidateShapeKey",
            "hash-join-build",
            "hash-aggregate-update",
            "ungrouped-aggregate-update",
            "candidate.signature.context",
            "candidate.signature.feature_shape",
            "SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED)",
            "executable_source",
            "native_region_possible = false",
            "PlanSljitHashJoinSinkNode",
            "PlanSljitHashAggregateSinkNode",
            "hash join build native protocol contract is not ready",
            "protocol.native_build_contract.status",
            "generated native hash join build append protocol",
            "requires=native_sink_runtime_binding",
            "SljitNativeRegionOpKind::HASH_JOIN_BUILD",
            "result.hash_join_build = input.hash_join_build",
            "protocol.build_append_shape_ready",
            "PlanSljitHashJoinProbeOperatorNode",
            "hash join probe native protocol contract is not ready",
            "protocol.native_probe_contract.status",
            "generated native hash join probe",
            "mark_build_match",
            "native_probe_output_mode",
            "mark_build_only",
            "output_mode=",
            "pointer_offset",
            "native-hash-join-probe-executable=ready",
            "protocol.native_probe_shape_ready",
            "requires=native_operator_runtime_binding",
            "SljitNativeRegionOpKind::HASH_JOIN_PROBE",
            "SljitNativeRegionHasCodegenGap",
            "generated native hash aggregate lookup and state update",
            "native-hash-aggregate-lookup-contract=",
            "JitRegionStateContractStatusToString(native_lookup_contract.status)",
            "native-grouped-aggregate-update-executable=ready",
            "SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE",
            "SetCompiledExecutionMode(",
            "source_helper",
            "source-fusion-gap:requires-native-source;source_execution=duckdb-getdata-helper",
            "SetCompiledExecutionMode(JitExecutionMode::NATIVE)",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_region_plan.cpp")],
        ["JitExecutionMode::GENERATED_HELPER", "generated_helper"],
    )
    assert_no_text(
        root,
        [Path("src/include/duckdb/execution/jit/common.hpp"), Path("src/execution/jit_types.cpp")],
        ["GENERATED_HELPER", "HELPER_BOUNDARY"],
    )
    assert_no_text(
        root,
        [
            Path("extension/jit_sljit/include/sljit_region_plan.hpp"),
            Path("extension/jit_sljit/include/sljit_region_executable.hpp"),
            Path("extension/jit_sljit/sljit_region_plan.cpp"),
            Path("extension/jit_sljit/sljit_region_executable.cpp"),
            Path("extension/jit_sljit/sljit_region_runtime.cpp"),
        ],
        [
            "native-protocol-codegen-missing",
            "hash join probe native codegen is not implemented",
            "hash join build native codegen is not implemented",
            "operator-fusion-gap:hash-join-probe-codegen-missing",
            "sink-fusion-gap:hash-join-build-codegen-missing",
            "SljitHashJoinKeyBinding",
            "CanExecuteNativeHashJoinProbe",
            "SljitHashJoinBuildProtocolAvailable",
            "SljitHashJoinProbeProtocolAvailable",
            "hash_join_build.keys",
            "native hash join probe operator protocol",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_operator_descriptor.cpp")],
        [
            "hash-join-native-build-append-condition-count",
            "hash-join-native-build-append-key-type",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/operator_runtime.hpp",
        [
            "struct JitNativeHashJoinBuildBinding",
            "struct JitNativeSinkBinding",
            "JitBindNativeHashJoinBuild",
            "JitAppendNativeHashJoinBuild",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "ExecuteNativeHashJoinBuild",
            "runtime.BindNativeSink",
            "JitAppendNativeHashJoinBuild",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/join_runtime.hpp",
        [
            "struct JitNativeHashJoinTableLayout",
            "single_match_probe",
            "can_have_null",
            "const ht_entry_t *entries",
            "JitGetNativeHashJoinTableLayout",
            "DescribeJitNativeHashJoinTableLayout",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_join_runtime.cpp",
        [
            "JitGetNativeHashJoinTableLayout",
            "hash_table.GetJitNativeHashJoinTableLayout(layout)",
            "native_hash_join_table_layout<",
        ],
    )
    assert_required_text(
        root,
        "src/execution/join_hashtable.cpp",
        [
            "JoinHashTable::GetJitNativeHashJoinTableLayout",
            "layout.dictionary_emission = use_dict_emission",
            "layout.can_have_null = layout_ptr && layout_ptr->CanHaveNull()",
            "layout.single_match_probe = !layout.chains_longer_than_one",
            "layout.ready = true",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/join_hashtable.cpp")],
        ["hash-join-native-dictionary-emission"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "bool HasExecutableBody() const override",
            "return !ops.empty();",
            "native-operator-runtime-binding-blocked",
            "native_input.rhs_keys_have_validity = layout.can_have_null",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_native_types.hpp",
        ["rhs_keys_have_validity"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/include/sljit_region_plan.hpp",
        ["bool null_equal = false"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_codegen.cpp",
        [
            "DuckDBNullHashImmediate",
            "EmitJumpIfHashJoinRhsKeyNull",
            "key.null_equal",
            "rhs_keys_have_validity",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/execution/join_hashtable.cpp"),
            Path("src/execution/jit_operator_descriptor.cpp"),
        ],
        [
            "hash-join-native-null-equal-condition",
            "hash-join-native-probe-null-equal-condition",
        ],
    )
    assert_required_text(
        root,
        "src/execution/operator/join/physical_hash_join.cpp",
        [
            "DataChunk lhs_output_data",
            "state.lhs_output_data.ReferenceColumns(input, lhs_output_columns.col_idxs)",
            "sink.perfect_join_executor->ProbePerfectHashTable(context, input, state.lhs_output_data, chunk",
            "ScanHashJoinSourceState",
            "PhysicalHashJoin::SupportsJitNativeSource",
            "PhysicalHashJoin::GetJitNativeSourceDataInternal",
            "return jit_prepared_pipeline.RequiresNativeSource() && PropagatesBuildSide(join_type);",
        ],
    )
    assert_required_text(
        root,
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        [
            "ScanHashAggregateState",
            "PhysicalHashAggregate::SupportsJitNativeSource",
            "PhysicalHashAggregate::GetJitNativeSourceDataInternal",
            "return jit_prepared_pipeline.RequiresNativeSource();",
        ],
    )
    assert_required_text(
        root,
        "src/execution/operator/scan/physical_column_data_scan.cpp",
        [
            "PhysicalColumnDataScan::SupportsJitNativeSource",
            "PhysicalColumnDataScan::GetJitNativeSourceDataInternal",
            "PhysicalOperatorType::CTE_SCAN",
            "PhysicalOperatorType::COLUMN_DATA_SCAN",
            "return GetDataInternal(context, chunk, input);",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/operator/join/physical_hash_join.cpp")],
        [
            "hash join finalized as perfect hash join",
            "hash join probe native primitive only supports inner join",
            "lhs_probe_data.ReferenceColumns(input, lhs_output_columns.col_idxs)",
            "lhs_probe_data.ReferenceColumns(lhs_probe_chunk, gstate.op.lhs_output_columns.col_idxs)",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_ir.cpp",
        [
            "BuildJitRegionSignature",
            "GetJitRegionNodeSignatureFeature",
            "table-scan-source",
            "candidate.signature = BuildJitRegionSignature",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_region_plan.cpp")],
        [
            "SljitRegionCandidateFeatureShape",
            "StringUtil::Split(pipeline_shape",
            "StringUtil::Contains(candidate.context_pipeline_shape",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_backend.cpp",
        [
            "SLJIT_AUTO_ADMISSION_RULES",
            "SLJIT_AUTO_ADMISSION_FAMILIES",
            "candidate.signature.shape",
            "IsSljitFilterProjectionInventory",
            "IsSljitProjectionChainInventory",
            "sljit:pipeline-inventory",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_backend.cpp")],
        [
            "candidate.shape ==",
            "StringUtil::StartsWith(candidate.shape",
        ],
    )
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "AutoInventoryGateCountingBackend",
            "JIT auto pipeline inventory skips typed IR lowering before candidate precheck",
            "JIT region lowering excludes wrapper-only pipelines",
            "candidate_precheck_count",
            "inventory_precheck_count",
            "Value::BOOLEAN(false)",
            "Value::BOOLEAN(true)",
            "candidate_precheck_count == 0",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "TryExecuteSinkPipeline",
            "JitRegionABIIsSinkPipeline(contract.abi)",
            "JitRegionABIIsSinkPipeline(kernel.TraceCandidateContract().abi)",
            "TraceCandidateNodeCount() != 1",
            "sink pipeline kernel executed;initial_idx=",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit.cpp",
        [
            "JitRegionCandidateOperatorCoverage",
            "JitRegionCandidateNativeFusionStrategyScore",
            "JitRegionABIOwnsSource(entry.candidate->contract.abi)",
            "JitRegionABIOwnsSink(entry.candidate->contract.abi)",
            "SelectedSourceExecution() != JitRegionSourceExecutionKind::NATIVE_SOURCE",
            "candidate.contract.source_ownership != JitRegionOwnershipKind::NATIVE_PROTOCOL",
            "JitRegionABIIsFullPipeline(candidate.contract.abi)",
            "JitRegionABIIsSourcePipeline(candidate.contract.abi)",
            "JitRegionABIIsSinkPipeline(candidate.contract.abi)",
            "result++",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "JitRegionPipelineInventory",
            "jit_policy=auto skips pipeline before typed IR lowering",
            "Inventory has two explicit modes",
            "`ADMISSION` mode",
            "`DIAGNOSTIC` mode",
            "must not render",
            "explicit `explain`",
            "inventory false positives",
            "Inventory false positives are cheap",
            "inventory false negatives",
            "architecture bugs",
            "full typed IR remains the semantic source",
            "of truth",
            "admission-inventory IR",
            "bounded",
            "event retention must not force full typed IR lowering",
            "cannot map to an admitted backend shape before backend analysis",
            "remaining supported candidates before codegen",
            "installs the maximum-score",
            "non-overlapping admitted set",
            "selects the non-overlapping admitted set",
            "Production `auto` is a positive-admission path",
            "Full missing-rule inventory remains",
            "silent negative return in production",
            "rule-backed rows",
            "region-level compiled execution mode",
            "native subnode counts",
            "counts are trace evidence only",
            "fallback capability",
            "candidate_context_pipeline_shape",
            "Backend auto admission is table-driven",
            "Neither predicate may parse rendered",
            "candidate_source_filter_count",
            "candidate_source_filter_expression_count",
            "candidate_source_filter_fallback_count",
            "candidate_source_comparison_filter_count",
            "candidate_source_integer_comparison_filter_count",
            "candidate_source_non_integer_comparison_filter_count",
            "candidate_source_conjunction_filter_count",
            "candidate_source_projected_column_count",
            "candidate_source_returned_column_count",
            "candidate_arithmetic_projection_count",
            "candidate_integer_arithmetic_projection_count",
            "candidate_non_integer_arithmetic_projection_count",
            "candidate_reference_projection_count",
            "candidate_integer_comparison_filter_count",
            "candidate_non_integer_comparison_filter_count",
            "candidate_conjunction_filter_count",
            "duckdb_jit_decision_counters()",
            "duckdb_jit_kernel_counters()",
            "duckdb_jit_clear_counters()",
            "pipeline_shape",
            "example_reason",
            "honest unknown estimate",
            "jit_event_log_size=0",
            "attribution must use the executable shape",
            "estimated work entering the executable",
            "compiled kernels: generated code exists",
            "row-processing kernels: the generated kernel processed",
            "compiled kernels were not reached",
            "pipeline-runtime summary",
            "source context as executable native code",
            "Protocol-only sink regions without generated",
            "`HasExecutableBody()`",
            "build/probe and generic hash aggregate",
            "helper-backed success",
            "candidate_scope",
            "planner output is intentionally narrow",
            "`source_pipeline` for one maximal",
            "full_pipeline",
            "core-owned source-prefix runtime ABI",
            "source-prefix executable ABI",
        ],
        )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "JitRegionCandidateTraits candidate_traits",
            "bool has_pipeline",
            "string pipeline_shape",
            "string example_reason",
            "const JitRegionCandidateTraits &TraceCandidateTraits() const",
            "JitRegionCandidateTraits trace_candidate_traits",
        ],
    )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_table_function_utils.hpp",
        [
            "AddJitCandidateTraceColumns",
            "AppendJitCandidateTraceColumns",
            "AppendNullJitCandidateTraceColumns",
            "candidate_arithmetic_projection_count",
            "candidate_integer_arithmetic_projection_count",
            "candidate_non_integer_arithmetic_projection_count",
            "JitRegionSourceKindToString",
        ],
    )
    for table_function in (
        "src/function/table/system/duckdb_jit_events.cpp",
        "src/function/table/system/duckdb_jit_decision_counters.cpp",
        "src/function/table/system/duckdb_jit_kernel_counters.cpp",
    ):
        assert_required_text(
            root,
            table_function,
            [
                "#include \"duckdb_jit_table_function_utils.hpp\"",
                "AddJitCandidateTraceColumns(return_types, names)",
                "AppendJitCandidateTraceColumns",
            ],
        )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_events.cpp",
        ["pipeline_shape", "AppendNullJitCandidateTraceColumns"],
    )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_decision_counters.cpp",
        ["pipeline_shape", "pipeline_estimated_cardinality", "example_reason"],
    )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_kernel_counters.cpp",
        ["AppendNullJitCandidateTraceColumns"],
    )
    assert_no_text(
        root,
        [
            Path("src/function/table/system/duckdb_jit_events.cpp"),
            Path("src/function/table/system/duckdb_jit_decision_counters.cpp"),
            Path("src/function/table/system/duckdb_jit_kernel_counters.cpp"),
        ],
        [
            "DuckDBJitEventsAppendCandidateTraits",
            "DuckDBJitDecisionCountersAppendCandidateTraits",
            "DuckDBJitKernelCountersAppendCandidateTraits",
            "FormatJitEventsStringList",
            "FormatJitDecisionCounterStringList",
            "FormatJitKernelCounterStringList",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/tpch_schema.py",
        [
            "CANDIDATE_TRAIT_FIELDS",
            "candidate_arithmetic_projection_count",
            "candidate_integer_arithmetic_projection_count",
            "candidate_non_integer_arithmetic_projection_count",
            "candidate_reference_projection_count",
            "pipeline_estimated_cardinality",
            "CONTEXT_SPECIFIC_POSITIVE_ADMISSION_SHAPES",
            "context_specific_positive_without_generic_admission_proof",
        ],
    )


def verify_region_execution_form_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/common.hpp",
        [
            "enum class JitRegionExecutionForm : uint8_t { NONE, FUSED };",
            "DUCKDB_API const char *JitRegionExecutionFormToString(JitRegionExecutionForm form);",
            "enum class JitRegionNativeSourceStatus : uint8_t { NONE, READY, BLOCKED };",
            "DUCKDB_API const char *JitRegionNativeSourceStatusToString",
            "enum class JitRegionStateContractStatus : uint8_t { NONE, READY, MISSING, BLOCKED };",
            "DUCKDB_API const char *JitRegionStateContractStatusToString",
            "GENERIC_SCAN",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "string region_execution_form",
            "void SetRegionExecutionForm(JitRegionExecutionForm execution_form);",
            "JitRegionExecutionForm ExpectedRegionExecutionForm() const;",
            "void SetTraceRegionExecutionForm(JitRegionExecutionForm execution_form);",
            "JitRegionExecutionForm RegionExecutionForm() const;",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_types.cpp",
        [
            "const char *JitRegionExecutionFormToString(JitRegionExecutionForm form)",
            "const char *JitRegionNativeSourceStatusToString",
            "const char *JitRegionStateContractStatusToString",
            "void JitRegionLoweringPlan::SetRegionExecutionForm",
            "JitRegionExecutionForm JitRegionLoweringPlan::ExpectedRegionExecutionForm() const",
            "\",execution-form=\" + JitRegionExecutionFormToString(region_execution_form)",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit.cpp",
        [
            "JitRegionExecutionForm region_execution_form",
            "event.region_execution_form = JitRegionExecutionFormToString(region_execution_form);",
            "event.region_execution_form = JitRegionExecutionFormToString(JitRegionExecutionForm::NONE);",
            "event.region_execution_form = JitRegionExecutionFormToString(kernel.RegionExecutionForm());",
            "compiled region without an explicit region execution form",
            "skips region kernel because region execution form is not fused",
            "region_execution_form=\" + string(JitRegionExecutionFormToString(region_execution_form))",
            "requires=fused",
            "decision.reason += \";\" + lowering_plan.EventReason();",
            "ValidateJitFusedRegionStageContract",
            "JitRegionStageExecutionIsFusionBlocker",
            "backend advertised fused region but core native-fusion contract is not ready",
            "backend advertised fused region across non-fused core stage",
            "backend advertised fused region but core operator-stage plan has no generated or native stage",
            "SetTraceRegionExecutionForm(prepared_region.lowering_plan.ExpectedRegionExecutionForm())",
            "PreparePipelineRegions",
            "CompilePreparedRegions",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_runtime.cpp",
        [
            "counter.region_execution_form != event.region_execution_form",
            "counter.region_execution_form == event.region_execution_form",
            "counter.region_execution_form = event.region_execution_form",
            "void JitRegionKernel::SetTraceRegionExecutionForm",
        ],
    )
    for table_function in (
        "src/function/table/system/duckdb_jit_events.cpp",
        "src/function/table/system/duckdb_jit_counters.cpp",
        "src/function/table/system/duckdb_jit_decision_counters.cpp",
        "src/function/table/system/duckdb_jit_kernel_counters.cpp",
    ):
        assert_required_text(
            root,
            table_function,
            [
                "names.emplace_back(\"region_execution_form\")",
                "output.data[col++].Append(Value(entry.region_execution_form))",
            ],
        )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_events.cpp",
        [
            "names.emplace_back(\"selected_source_execution\")",
            "JitRegionSourceExecutionKindToString(entry.selected_source_execution)",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
            [
                "ClassifySljitRegionExecutionForm",
                "stage_region.HasExecutableBody()",
                "SljitOperatorStageRegionPlan",
                "BuildSljitOperatorStageRegionPlan",
                "operator-stage-region<",
                "stage_plan_valid",
            "HasExecutableBody",
            "generic-runtime-loop",
            "kernel_blocker",
            "SetOperatorStageIR",
            "candidate.stage_plan.ir",
            "result.stages = core_stage_plan.stages",
            "AppendCoreOperatorStages",
            "core_stage_plan",
            "JitRegionStageKindToString(stage.kind)",
            "JitCompiledProtocolKindToString(stage.protocol)",
            "source-filter",
            "source_filter_count",
                "return JitRegionExecutionForm::FUSED;",
                "ClassifySljitRegionExecutionForm(*native_region, contract, candidate.stage_plan)",
            "PlanSljitNativeSourceNode",
            "source helper requires native-source contract IR",
            "return PlanSljitNativeSourceNode(node, contract);",
            "generated source-prefix table scan filters",
            "source-strategy=prepared-unfiltered-native-source",
            "SljitSourceFilterExecutionKind::GENERATED_REGION",
            "result.requires_native_source = true;",
            "SetSelectedSourceExecution",
            "AddFusionBlocker",
            "source-fusion-gap:requires-native-source",
            "full pipeline sink requires native sink or operator update protocol",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit")],
        ["enum class SljitOperatorStageKind", "OperatorStageKindToString", "AppendOperatorStages("],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_region_plan.cpp")],
        [
            "SljitNativeRegionOpIsTypedOperatorHelper",
            "SljitNativeRegionOpIsTypedSinkHelper",
            "SljitNativeRegionOpIsTypedOperatorHelper(op) || SljitNativeRegionOpIsTypedSinkHelper(op)",
            "operator-fusion-gap:multi-operator-helper-chain-protocol-missing",
            "SLJIT typed operator helper chains require a multi-operator helper protocol",
            "typed operator helper requires full-pipeline region ownership",
            "hash-join-build-primitive-fallback",
        ],
    )
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "compile_event.region_execution_form = \"fused\"",
            "full pipeline sink requires native sink or operator update protocol",
            "REQUIRE(event.region_execution_form == \"fused\")",
            "REQUIRE(event.region_execution_form == \"none\")",
            "JIT auto admission only compiles fused region forms",
            "JIT manager rejects backend-fused regions across non-fused core stages",
            "contract_test_auto_non_fused_jit_backend",
            "backend advertised fused region but core native-fusion contract is not ready",
            "REQUIRE(backend_ref.region_compile_count == 0)",
            "REQUIRE(StringUtil::Contains(event.reason, \"region execution form is not fused\"))",
            "REQUIRE(counter.region_execution_form == \"none\")",
        ],
    )
    assert_required_text(
        root,
        "test/sql/jit/test_jit_framework.test",
        [
            "region_execution_form='fused'",
            "region_execution_form='none'",
            "FROM duckdb_jit_decision_counters()",
        ],
    )
    assert_required_text(
        root,
        "benchmark/jit/verify_jit_sql_trace.py",
        [
            "KNOWN_REGION_EXECUTION_FORMS",
            "source_pipeline",
            "native-source pipeline has invalid execution form",
            "source-helper pipeline was compiled as executable JIT",
            "full-pipeline executable has invalid execution mode",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/verify_tpch_trace.py",
        [
            "KNOWN_REGION_EXECUTION_FORMS",
            "decision_counter_summary.csv: compiled region counter has no execution form",
                "source_pipeline",
                    "native-source pipeline has invalid execution form",
                    "source-helper pipeline was compiled as executable JIT",
                    "source-fusion-gap:requires-native-source",
                "full-pipeline executable has invalid execution mode",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "region execution form declared by the lowering plan",
            "`auto` is fused-only",
            "reject any region whose `region_execution_form` is not `fused`",
            "`force` is fused-only for compiled regions",
            "Core must reject any compiled region whose",
            "Node counts, helper counts,",
            "`execution_mode` and `region_execution_form` are intentionally separate.",
            "`duckdb-getdata-helper` is an explicit non-fused source boundary",
            "DuckDB owns source",
            "filter execution and pruning",
            "native-source ownership and must not be used as proof",
            "Every candidate has one effective source execution",
            "Core must not manufacture a helper-source",
            "Full-pipeline selective scan regions have one source-execution candidate.",
            "Helper-source candidates are diagnostic proof-gap evidence",
            "not measured admission families",
            "not native regions",
            "`generated source-prefix table scan filters`",
            "Architecture support is not production admission.",
            "A fused shape may enter",
            "`sljit:full-pipeline:filter-projection-ungrouped-aggregate-update`",
            "query-local proof gap is not an admission rule.",
            "must not inspect TPCH query text",
            "Core must skip `full_pipeline` candidates before backend",
            "Source-pushed table filters are normalized in core IR",
            "backend-neutral source-prefix filter ABI",
            "`PRUNE_ONLY` prepared source input",
            "Candidate IR must render deterministic `source<...>` text",
            "that effective execution",
            "`execution=duckdb-getdata-helper`",
            "Dynamic table filters do not block native-source ownership.",
            "`dynamic_filters=true`",
            "merged into the storage scan state",
            "Projection chains are also normalized at backend planning time.",
            "`compose-reference-projection(...)`",
            "remap its expression source",
            "adjacent native `FILTER` +",
            "projection `execute_sel`",
            "`runtime-fused:filter-projection=N`",
            "`execution:native-sljit-region-filter-projection-ungrouped-aggregate-update`",
            "shared-selection/all-valid loop",
            "`source_fusion_gap_summary`",
            "`admission_efficiency_summary`",
            "`auto_admitted_helper_dominated_region`",
            "`source_fusion_gap=requires_native_source`",
            "`fusion_blocker_summary`",
            "`fusion-blocker:*`",
            "full pipeline sink requires native sink or operator update protocol",
            "`native_source_status`",
            "`native_source_required_capability`",
            "`native_source_protocol`",
            "`native_source_blocker`",
            "native_source_contract<status=blocked",
            "required_capability=duckdb-table-scan-native-source",
                "Source-pushed scan work must also be visible in candidate traits and shape keys",
                "`scan-filter` and `scan-project`",
                "`JitRegionSignature`",
                "must not parse",
                "`fusion-blocker:source-fusion-gap:requires-native-source`",
            "missing native sink/operator protocol",
            "Selection-vector ownership is part of the runtime ABI.",
            "storage per filter stage",
            "`none` is never valid for a compiled region",
            "`duckdb_jit_counters()` shows cumulative counts by backend, target, status,",
            "`duckdb_jit_decision_counters()` shows cumulative candidate/admission counts",
            "execution form, so long-running runtime traces",
        ],
    )


def verify_stage_region_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/common.hpp",
        [
            "enum class JitRegionBoundaryKind",
            "SOURCE_NATIVE",
            "SINK_NATIVE",
            "OPERATOR_NATIVE",
            "enum class JitRegionOwnershipKind",
            "GENERATED_IR",
            "NATIVE_PROTOCOL",
            "TYPED_HELPER",
            "EXECUTOR_BOUNDARY",
            "MISSING_PROTOCOL",
            "JitRegionOwnershipKindToString",
            "enum class JitRegionStageKind",
            "SOURCE_FILTER",
            "HASH_JOIN_PROBE",
            "HASH_AGGREGATE_UPDATE",
            "enum class JitRegionStageExecutionKind",
            "JitRegionStageKindToString",
            "JitRegionStageExecutionKindToString",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/region.hpp",
        [
            "bool grouped_state_layout_ready",
            "vector<idx_t> grouped_state_offsets",
            "vector<idx_t> grouped_state_payload_sizes",
            "struct JitRegionContract",
            "JitRegionABI abi",
            "idx_t first_node",
            "idx_t node_count",
            "idx_t start_operator_index",
            "idx_t end_operator_index",
            "bool owns_source",
            "bool owns_transform",
            "bool owns_sink",
            "bool owns_state_scan",
            "JitRegionOwnershipKind source_ownership",
            "JitRegionOwnershipKind state_scan_ownership",
            "JitRegionOwnershipKind transform_ownership",
            "JitRegionOwnershipKind sink_ownership",
            "bool executor_boundary_free",
            "bool native_fusion_ready",
            "generated_operator_count",
            "typed_helper_boundary_count",
            "executor_boundary_count",
            "missing_protocol_count",
            "vector<string> required_capabilities",
            "vector<string> blockers",
            "JitRegionContract contract",
            "struct JitRegionStage",
            "JitRegionStageKind kind",
            "JitRegionStageExecutionKind execution",
            "JitRegionOwnershipKind ownership",
            "JitCompiledProtocolKind protocol",
            "JitCompiledDrainKind drain",
            "string required_capability",
            "struct JitRegionStagePlan",
            "vector<JitRegionStage> stages",
            "JitRegionStagePlan stage_plan",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/compiled_contract.hpp",
        [
            "enum class JitCompiledProtocolKind",
            "enum class JitCompiledDrainKind",
            "DUCKDB_API const char *JitCompiledProtocolKindToString",
            "DUCKDB_API const char *JitCompiledDrainKindToString",
            "struct JitCompiledStageContract",
            "JitCompiledProtocolKind protocol",
            "JitCompiledDrainKind drain",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_types.cpp",
        [
            "const char *JitRegionBoundaryKindToString",
            "return \"source-native\"",
            "return \"operator-native\"",
            "return \"sink-native\"",
            "const char *JitRegionABIToString",
            "bool JitRegionABIIsChunkTransform",
            "bool JitRegionABIIsSourcePipeline",
            "bool JitRegionABIIsSinkPipeline",
            "bool JitRegionABIIsFullPipeline",
            "bool JitRegionABIOwnsSource",
            "bool JitRegionABIOwnsSink",
            "source_prefix",
            "sink_suffix",
            "full_pipeline",
            "const char *JitRegionOwnershipKindToString",
            "const char *JitRegionStageKindToString",
            "const char *JitRegionStageExecutionKindToString",
            "const char *JitCompiledProtocolKindToString",
            "const char *JitCompiledDrainKindToString",
            "generated-ir",
            "native-protocol",
            "typed-helper",
            "executor-boundary",
            "missing-protocol",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_ir.cpp",
        [
            "RecordJitRegionContractOwnership",
            "RecordJitRegionMissingContract",
            "ClassifyJitRegionNativeSourceOwnership",
            "ClassifyJitRegionStateScanOwnership",
            "ClassifyJitRegionSourceOwnership",
            "RecordJitRegionGroupedStateContract",
            "grouped_state_layout_ready",
            "grouped_state_offsets",
            "grouped_state_payload_sizes",
            "JitCompiledOperatorContract",
            "ClassifyJitCompiledContractOwnership",
            "JitRegionBoundaryKind::SOURCE_NATIVE",
            "JitRegionBoundaryKind::SINK_NATIVE",
            "JitRegionBoundaryKind::OPERATOR_NATIVE",
            "ClassifyJitRegionSinkOwnership",
            "DetermineJitRegionContractABI",
            "DescribeJitRegionContract",
            "BuildJitRegionContract",
            "JitRegionStageExecutionFromOwnership",
            "JitRegionOwnershipFromStageExecution",
            "AddJitRegionCompiledStage",
            "AddJitRegionCompiledStages",
            "BuildJitRegionStagePlan",
            "compiled_stage.protocol",
            "compiled_stage.drain",
            "JitCompiledProtocolKindToString(stage.protocol)",
            "JitCompiledDrainKindToString(stage.drain)",
            "case JitRegionStageExecutionKind::MISSING_PROTOCOL:",
            "DescribeJitRegionStagePlan",
            "duckdb.operator-stage-region<",
            "BuildJitRegionUpstreamTraits",
            "BuildJitRegionContextTraits",
            "BuildJitRegionContinuationTraits",
            "JitRegionTraitsRequireOperatorResumeProtocol",
            "JitRegionCandidateRequiresMissingSplitProtocol",
            "JitRegionCandidateScope::SOURCE_PIPELINE",
            "JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL",
            "JitRegionCandidateScope::SINK_PIPELINE",
            "contract.native_fusion_ready",
            "contract.abi = DetermineJitRegionContractABI(contract)",
            "candidate.contract = BuildJitRegionContract(region_ir, candidate)",
            "candidate.stage_plan = BuildJitRegionStagePlan(region_ir, candidate)",
            "candidate.upstream_traits = BuildJitRegionUpstreamTraits(region_ir, candidate)",
            "candidate.context_traits = BuildJitRegionContextTraits(region_ir)",
            "candidate.continuation_traits = BuildJitRegionContinuationTraits(region_ir, candidate)",
            "if (JitRegionCandidateRequiresMissingSplitProtocol(candidate))",
            "candidate.stage_plan.ir",
            "upstream_\" + candidate.upstream_traits.ir",
            "context_\" + candidate.context_traits.ir",
            "continuation_\" + candidate.continuation_traits.ir",
            "result += \",\" + candidate.contract.ir",
            "contract<abi=",
            "owns_source=",
            "owns_transform=",
            "owns_sink=",
            "owns_state_scan=",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_region_ir.cpp")],
        ["JitRegionOperatorStageOwnership", "JitRegionSinkStageKind", "JitRegionOperatorStageKind"],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "auto &contract = candidate.contract",
            "JitRegionABIIsSourcePipeline(contract.abi)",
            "JitRegionABIIsFullPipeline(contract.abi)",
            "candidate-fusion-gap:executor-boundary",
            "candidate-fusion-gap:typed-helper-boundary",
            "candidate-fusion-gap:missing-protocol",
            "operator-fusion-gap:downstream-operator-helper-resume-protocol-missing",
            "sink-fusion-gap:hash-join-build-protocol-missing",
            "sink-fusion-gap:hash-join-build-native-lowering",
            "sink-fusion-gap:upstream-operator-resume-protocol-missing",
            "source-fusion-gap:downstream-operator-helper-resume-protocol-missing",
            "operator-fusion-gap:upstream-operator-resume-protocol-missing",
            "operator-fusion-gap:downstream-helper-continuation-protocol-missing",
            "SljitTraitsRequireUpstreamResumeProtocol",
            "SljitRejectsSourcePrefixResumeContext",
            "SljitRejectsPostSourceUpstreamResumeContext",
            "SljitRejectsPostSourceContinuationContext",
            "SljitRejectsSinkPipelineUpstreamResumeContext",
            "SljitTraitsRequireUpstreamResumeProtocol(candidate.upstream_traits)",
            "candidate.context_traits.operator_helper_count",
            "candidate.continuation_traits.operator_helper_count",
            "resumable_operator_count",
            "DescribeSljitGroupedStateLayoutContract",
            "native-grouped-state-layout-contract",
            "contract.ir",
        ],
    )
    assert_required_text(
        root,
        "src/function/table/system/duckdb_jit_table_function_utils.hpp",
        [
            "names.emplace_back(\"candidate_source_ownership\")",
            "names.emplace_back(\"candidate_contract_abi\")",
            "names.emplace_back(\"candidate_contract_first_node\")",
            "names.emplace_back(\"candidate_contract_node_count\")",
            "names.emplace_back(\"candidate_contract_start_operator_index\")",
            "names.emplace_back(\"candidate_contract_end_operator_index\")",
            "names.emplace_back(\"candidate_owns_source\")",
            "names.emplace_back(\"candidate_owns_transform\")",
            "names.emplace_back(\"candidate_owns_sink\")",
            "names.emplace_back(\"candidate_owns_state_scan\")",
            "names.emplace_back(\"candidate_state_scan_ownership\")",
            "names.emplace_back(\"candidate_transform_ownership\")",
            "names.emplace_back(\"candidate_sink_ownership\")",
            "names.emplace_back(\"candidate_executor_boundary_free\")",
            "names.emplace_back(\"candidate_native_fusion_ready\")",
            "names.emplace_back(\"candidate_generated_operator_count\")",
            "names.emplace_back(\"candidate_typed_helper_boundary_count\")",
            "names.emplace_back(\"candidate_executor_boundary_count\")",
            "names.emplace_back(\"candidate_missing_protocol_count\")",
            "names.emplace_back(\"candidate_required_capabilities\")",
            "names.emplace_back(\"candidate_fusion_blockers\")",
            "JitRegionABIToString(contract.abi)",
            "JitRegionOwnershipKindToString(contract.source_ownership)",
            "contract.first_node",
            "contract.node_count",
            "contract.start_operator_index",
            "contract.end_operator_index",
            "contract.owns_source",
            "contract.owns_transform",
            "contract.owns_sink",
            "contract.owns_state_scan",
            "JitRegionOwnershipKindToString(contract.state_scan_ownership)",
            "JitRegionOwnershipKindToString(contract.transform_ownership)",
            "JitRegionOwnershipKindToString(contract.sink_ownership)",
            "contract.native_fusion_ready",
        ],
    )
    assert_required_text(
        root,
        "benchmark/jit/jit_sql_trace.py",
        [
                "candidate_contract_first_node",
                "candidate_contract_abi",
                "candidate_contract_node_count",
            "candidate_contract_start_operator_index",
            "candidate_contract_end_operator_index",
            "candidate_owns_source",
            "candidate_owns_transform",
            "candidate_owns_sink",
            "candidate_owns_state_scan",
            "candidate_source_ownership",
            "candidate_state_scan_ownership",
            "candidate_transform_ownership",
            "candidate_sink_ownership",
            "candidate_executor_boundary_free",
            "candidate_native_fusion_ready",
            "candidate_generated_operator_count",
            "candidate_typed_helper_boundary_count",
            "candidate_executor_boundary_count",
            "candidate_missing_protocol_count",
            "candidate_required_capabilities",
            "candidate_fusion_blockers",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/tpch_schema.py",
        [
                "candidate_contract_first_node",
                "candidate_contract_abi",
                "candidate_contract_node_count",
            "candidate_contract_start_operator_index",
            "candidate_contract_end_operator_index",
            "candidate_owns_source",
            "candidate_owns_transform",
            "candidate_owns_sink",
            "candidate_owns_state_scan",
            "candidate_source_ownership",
            "candidate_state_scan_ownership",
            "candidate_transform_ownership",
            "candidate_sink_ownership",
            "candidate_executor_boundary_free",
            "candidate_native_fusion_ready",
            "candidate_generated_operator_count",
            "candidate_typed_helper_boundary_count",
            "candidate_executor_boundary_count",
            "candidate_missing_protocol_count",
            "candidate_required_capabilities",
            "candidate_fusion_blockers",
            "ADMISSION_EFFICIENCY_SUMMARY_FIELDS",
        ],
    )
    for trace_file in (
        "benchmark/tpch/jit/tpch_trace.py",
        "benchmark/tpch/jit/verify_tpch_trace.py",
        "benchmark/tpch/jit/verify_tpch_benchmark.py",
    ):
        assert_required_text(root, trace_file, ["from tpch_schema import", "CANDIDATE_TRAIT_FIELDS"])
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "event.candidate_traits.has_stateful_source",
            "event.candidate_contract.owns_source",
            "event.candidate_contract.abi",
            "event.candidate_contract.owns_transform",
            "event.candidate_contract.owns_sink",
            "event.candidate_contract.owns_state_scan",
            "event.candidate_contract.source_ownership",
            "event.candidate_contract.state_scan_ownership",
            "event.candidate_contract.native_fusion_ready",
            "perfect-hash-aggregate-native-state-scan",
            "ungrouped-aggregate-native-state-scan",
            "native-grouped-state-layout-contract=ready",
            "grouped_state_layout_ready=true",
            "contract<abi=",
            "source=native-protocol,state_scan=native-protocol",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "candidate-level fused-region ownership",
            "`JitRegionContract::abi` is the canonical runtime entry contract",
            "`candidate_contract_abi`",
            "The SQL-visible candidate contract columns are declared and appended through one",
            "is the schema owner",
            "Runtime dispatch, backend ABI validation, source preparation, and",
            "ownership booleans remain",
            "`first_node`, `node_count`, `start_operator_index`, `end_operator_index`",
            "`owns_source`, `owns_transform`",
            "`owns_sink`, `owns_state_scan`",
            "`JitRegionContract` records the owned interval",
            "`state_scan_ownership`",
            "`transform_ownership`",
            "`sink_ownership`",
            "Hash-join build/probe and generic",
            "hash aggregate lookup/update are native contracts",
            "not typed-helper success states",
            "unsupported variants must stay missing-protocol",
            "semantic boundary labels",
            "`source-native`, `operator-native`, and `sink-native`",
            "must not be counted as capability gaps",
            "legacy scope labels reporting-only",
            "`native_fusion_ready=true` only when",
            "from non-fused boundary diagnostics to",
            "Backends consume this core ownership contract",
            "`candidate-fusion-gap:missing-protocol`",
            "`candidate-fusion-gap:typed-helper-boundary`",
            "`candidate-fusion-gap:executor-boundary`",
            "`duckdb_jit_kernel_counters()` expose the",
        ],
    )


def verify_prepared_region_contract(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "struct JitPreparedSourceContract",
            "struct JitPreparedPipeline",
            "bool RequiresPreparedSourceInput() const",
            "const vector<LogicalType> &SourceInputTypes",
            "vector<JitPreparedRegionCandidate> selected_regions",
            "bool filter_split_supported",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_runtime.cpp",
        [
            "vector<unique_ptr<SelectionVector>> filter_selections",
            "filter_selections.push_back(make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE))",
            "ExecuteFilter(op, *current, output, *filter_selections[op_idx])",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "vector<unique_ptr<SelectionVector>> filter_selections",
            "filter_selections.reserve(source.filters.size())",
            "filtered->Slice(*current, *filter_sel, selected_count)",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/manager.hpp",
        [
            "PreparePipelineRegions(ClientContext &context, Pipeline &pipeline)",
            "CompilePreparedRegions(ClientContext &context, const JitPreparedPipeline &prepared)",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/include/duckdb/execution/jit/manager.hpp"),
            Path("src/execution/jit.cpp"),
            Path("src/parallel/pipeline_executor.cpp"),
        ],
        [removed_name("TryCompile", "Regions")],
    )
    assert_required_text(
        root,
        "src/include/duckdb/parallel/pipeline.hpp",
        [
            "void PrepareJitRegions();",
            "optional_ptr<const JitPreparedPipeline> GetJitPreparedPipeline() const",
            "unique_ptr<JitPreparedPipeline> jit_prepared_pipeline",
        ],
    )
    assert_required_text(
        root,
        "src/parallel/pipeline.cpp",
        [
            "void Pipeline::PrepareJitRegions()",
            "JitManager::IsJitIntrospectionPipeline(*this)",
            "PreparePipelineRegions(client, *this)",
            "source->GetGlobalSourceState(GetClientContext(), GetJitPreparedPipeline())",
        ],
    )
    assert_required_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        [
            "CompilePreparedRegions(context_p, *prepared_jit)",
            "SourceInputTypes(pipeline.source->GetTypes())",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/physical_operator.hpp",
        [
            "struct JitPreparedPipeline;",
            "GetGlobalSourceState(ClientContext &context, optional_ptr<const JitPreparedPipeline> jit_prepared_pipeline) const",
        ],
    )
    assert_required_text(
        root,
        "src/execution/operator/scan/physical_table_scan.cpp",
        [
            "BuildTableScanJitSourceConfig",
            "prepared->RequiresPreparedSourceInput()",
            "source_contract.filter_split_supported",
            "BuildIdentityTableScanProjection(op.column_ids.size())",
            "TableFilterExecutionMode::PRUNE_ONLY",
            "jit_source_config.use_prepared_source_input",
            "op.dynamic_filters && op.dynamic_filters->HasFilters()",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/execution/operator/scan/physical_table_scan.cpp"),
            Path("src/execution/jit_operator_descriptor.cpp"),
        ],
        [
            "if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {\n\t\treturn false;",
            "if (scan.dynamic_filters && scan.dynamic_filters->HasFilters()) {\n\t\treturn false;",
        ],
    )
    assert_required_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        [
            "prepared_source_input",
            "TryExecutePreparedSourceReference",
            "prepared JIT source input requires source-prefix reference materialization",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "GetJitPreparedSourceReferenceNode",
            "BuildJitSourcePrefixReference(executor.context.client, *source_node, source_chunk, *result)",
        ],
    )
    assert_required_text(
        root,
        "src/main/client_context.cpp",
        [
            "static thread_local unordered_map<const ClientContext *, idx_t> jit_scoped_suppression_depth",
            "GetJitScopedSuppressionDepth(*this)",
        ],
    )


def verify_core_lowering_boundary(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/lowering.hpp",
        [
            "class Expression;",
            "TryLowerJitExpression",
            "DescribeJitExpressionLoweringFailure",
            "TryLowerJitRegion",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/include/duckdb/execution/jit/ir.hpp"),
            Path("src/include/duckdb/execution/jit/region.hpp"),
        ],
        [
            "class Expression;",
            "class Pipeline;",
            "TryLowerJitExpression",
            "DescribeJitExpressionLoweringFailure",
            "TryLowerJitRegion",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit.cpp",
        ["#include \"duckdb/execution/jit/lowering.hpp\""],
    )
    assert_required_text(
        root,
        "src/execution/jit_expression_ir.cpp",
        [
            "#include \"duckdb/execution/jit/lowering.hpp\"",
            "JitIsOptionalTableFilterFunction",
            "BuildJitBooleanConstant(true)",
            "JitIsStringPrefixFunction",
            "JitExpressionIntrinsicKind::STRING_PREFIX",
            "JitIsStringSubstringFunction",
            "JitExpressionIntrinsicKind::STRING_SUBSTRING",
            "TryBuildJitStringSubstringIR",
            "BuildJitExpressionFailureReason",
            "function_or_operator_unsupported",
            "expression_class_unsupported",
            "core expression lowering unsupported;",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_ir.cpp",
        [
            "DescribeJitExpressionLoweringFailure(*filter.expression)",
            "DescribeJitExpressionLoweringFailure(*projection.select_list[expr_idx])",
            "expression_index=",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_region_ir.cpp",
        [
            "#include \"duckdb/execution/jit/lowering.hpp\"",
            "static bool IsJitRegionScanSource(PhysicalOperatorType type)",
            "BuildJitRegionSourceInfo",
            "JitOperatorSourceDescriptor",
            "DescribeJitRegionSourceInfo",
            "DescribeJitRegionNativeSourceContract",
            "BuildJitRegionNativeSourceContract",
            "BuildJitRegionEffectiveSourceFields",
            "JitRegionSourceBoundaryMarker",
            "BuildJitRegionStatefulSourceInfo",
            "stateful-source-protocol-boundary",
            "DescribeJitRegionNativeStateScanContract",
            "DescribeJitRegionNativeGroupedStateContract",
            "DescribeJitRegionNativeOperatorContract",
            "AppendJitRegionContractIR",
            "JitRegionNativeOperatorContract",
            "DescribeJitRegionTableScanProtocol",
            "source<kind=",
            "native_source_contract<status=",
            "native_state_scan_contract_status=",
            "native_state_scan_required_capability=",
            "native_state_scan_blocker=",
            "native_grouped_state_contract_status=",
            "native_grouped_state_required_capability=",
            "native_grouped_state_blocker=",
            "\"native_hash_join_probe\"",
            "\"native_hash_join_build\"",
            "\"native_hash_aggregate_lookup\"",
            "\"_contract_status=\"",
            "fields=",
            "table_scan_protocol<",
            "source_prefix_input_columns=",
            "source_prefix_input_types=",
            "source_prefix_output_projection_map=",
            "source_prefix_filter_column_map=",
            "source_prefix_requires_unfiltered_input=",
            "source_prefix_filter_prune_required=",
            "source_prefix_filter_split_supported=",
            "hash_join_keys=",
            "aggregates=",
            "groups=",
            "native_source_contract",
            "generic-scan-native-source",
            "generic-scan-getdata-helper-boundary",
            "projection_pushdown=",
            "filter_count=",
            "dynamic_filters=",
            "DuckDB stateful source operator fallback boundary",
            "JitRegionSourceHasGeneratedPrefixWork",
            "JitRegionNodeCanStayInMaximalSourcePrefixSpan",
            "FindJitRegionMaximalSourcePrefixEnd",
            "GetJitRegionPrefixEndOperatorIndex",
            "AddJitRegionMaximalPrefixCandidate",
            "AddJitRegionMaximalPrefixCandidate(region_ir, candidate_id, operator_count)",
            "AddJitRegionCandidateAndIncrement(region_ir, candidate_id, 0, region_ir.nodes.size(), 0, operator_count)",
            "JitRegionCandidateScope::FULL_PIPELINE",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_region_ir.cpp")],
        [
            "JitRegionTypedHelperContract",
            "DescribeJitRegionTypedHelperContract",
            "typed_hash_join_probe_helper",
            "typed_hash_join_build_helper",
            "typed_hash_aggregate_lookup_helper",
            "hash-join-typed",
            "hash-aggregate-typed-lookup-helper",
        ],
    )
    assert_no_text(
        root,
        [Path("src/execution/jit_region_ir.cpp")],
        [
            "IsJitRegionSortStateSource",
            "BuildJitRegionSortStateScanCapability",
            "AddJitRegionStatefulSourceStateScanContract",
            "DuckDB sort source state-scan protocol missing",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_operator_descriptor.cpp",
        [
            "PhysicalOrder::GetJitOperatorDescriptor",
            "PhysicalTopN::GetJitOperatorDescriptor",
            "order-by-native-state-scan",
            "top-n-native-state-scan",
            "DuckDB order by native state scan protocol",
            "DuckDB top-n native state scan protocol",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit_operator_descriptor.cpp",
        [
            "JitRegionSourceKind::DUCKDB_TABLE_SCAN",
            "JitRegionSourceExecutionKind::DUCKDB_GETDATA_HELPER",
            "BuildJitRegionNativeSourceContract",
            "BuildJitDescriptorNativeGroupedStateContract",
            "AddJitDescriptorHashAggregateGroupedStateLayout",
            "AddJitDescriptorPerfectHashAggregateGroupedStateLayout",
            "JitRegionStateContractStatus::MISSING",
            "hash-aggregate-native-grouped-state",
            "perfect-hash-aggregate-native-grouped-state",
            "grouped-state-protocol-boundary",
            "BuildJitDescriptorTableScanProtocol",
            "BuildJitDescriptorTableScanSourceInputTypes",
            "BuildJitDescriptorTableScanOutputProjectionMap",
            "BuildJitDescriptorTableScanFilterColumnMap",
            "scan.dynamic_filters && scan.dynamic_filters->HasFilters()",
            "table_scan_protocol =",
            "DuckDB table scan source helper boundary",
            "PhysicalColumnDataScan::GetJitOperatorDescriptor",
            "DuckDB column data native source protocol",
            "\";function=\"",
            "BuildJitDescriptorHashJoinBoundaryReason",
            "BuildJitDescriptorHashJoinProtocol",
            "_contract_status=",
            "DuckDB hash join native state scan protocol",
            "DuckDB hash join state scan source does not produce rows",
            "hash-join-native-state-scan",
            "source_produces_rows=",
            "BuildJitDescriptorHashJoinProbeKeyInputs",
            "function_name = \"hash_join_probe\"",
            "hash_join_protocol =",
            "join_type=",
            "condition_count=",
            "filter_pushdown_probe_count=",
            "BuildJitDescriptorHashAggregateBoundaryReason",
            "BuildJitDescriptorHashAggregateProtocol",
            "JitDescriptorHashAggregateHasDistinctState",
            "MarkJitDescriptorHashAggregateDistinctStateBoundary",
            "hash-aggregate-distinct-grouped-state-protocol-boundary",
            "hash-aggregate-distinct-lookup-protocol-boundary",
            "DuckDB hash aggregate native state scan protocol",
            "function_name = \"hash_aggregate_scan\"",
            "BuildJitDescriptorPerfectHashAggregateInputs",
            "BuildJitDescriptorGroupInputs(const PhysicalPerfectHashAggregate",
            "BuildJitDescriptorPerfectHashAggregateBoundaryReason",
            "BuildJitDescriptorPerfectHashAggregateProtocol",
            "function_name = \"perfect_hash_aggregate_scan\"",
            "BuildJitDescriptorUngroupedAggregateBoundaryReason",
            "BuildJitDescriptorUngroupedAggregateProtocol",
            "function_name = \"ungrouped_aggregate_scan\"",
            "DuckDB aggregate source state protocol missing",
            "DuckDB hash aggregate sink update protocol",
            "DuckDB perfect hash aggregate sink update protocol",
            "DuckDB ungrouped aggregate payload update protocol",
            "aggregate_protocol =",
            "aggregate_operator_kind=",
            "aggregate_functions=",
            "radix_table_count=",
            "perfect_required_bits_total=",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "DuckDB source GetData helper boundary;",
            "+ node.fallback_reason",
            "PlanSljitSourceHelperNode",
            "table_scan_protocol.present",
            "protocol.source_prefix_input_column_count",
            "protocol.source_prefix_requires_unfiltered_input",
            "protocol.source_prefix_filter_prune_required",
            "protocol.source_prefix_filter_split_supported",
            "SljitSourceFallbackReason",
            "SljitSourceIR",
            "AppendSljitSourceIR",
            "DescribeJitRegionSourceInfo(*node.source, execution)",
            "candidate.source_execution",
            "protocol.present",
            "native_build_contract.status",
            "native_probe_contract.status",
            "native_lookup_contract.status",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_region_plan.cpp")],
        [
            "SljitRegionSinkField",
            "sink.fields",
            "protocol.join_type != JoinType::INNER",
            "protocol.non_equality_condition_count != 0",
            "protocol.null_equal_condition_count != 0",
            "protocol.residual_predicate || protocol.residual_info",
            "typed_lookup_helper_contract",
            "typed-hash-aggregate-lookup-helper-contract",
            "hash-aggregate-typed-lookup-helper",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "ir.hpp` exposes backend-neutral expression IR only",
            "must not expose DuckDB planner `Expression` lowering",
            "region.hpp` exposes backend-neutral region IR",
            "must not expose DuckDB `Pipeline` lowering",
            "lowering.hpp` exposes the core-only lowering boundary",
            "into backend-neutral JIT IR. Backends",
            "must not include it",
            "pipeline_descriptor.hpp` exposes the core pipeline",
            "BuildJitPipelineDescriptor",
            "single pipeline-level discovery point",
            "jit_pipeline_descriptor.cpp` owns DuckDB-facing physical",
            "single place region preparation",
            "slice compiled",
            "derive native-role flags",
            "unpack role payloads",
            "operator_descriptor.hpp` exposes the physical-operator",
            "GetJitOperatorDescriptor()",
            "Concrete operator knowledge must terminate at the descriptor adapter",
            "implementations must consume only backend-neutral JIT IR",
            "Source boundaries are semantic IR records",
            "JitRegionSourceInfo",
            "typed table-scan protocol records",
            "typed native stateful source protocol records",
            "typed stateful state-scan protocol records",
            "hash-join probe key bindings",
            "aggregate source payload/group bindings",
            "deterministic `source<...>` text",
            "parsing `reason`",
            "scraping `source<...>` / `sink<...>` text",
            "JitRegionSinkInfo",
            "deterministic `sink<...>` text",
            "hash-join build",
            "aggregate update",
            "the planner does not emit interior intervals",
            "Sink suffix candidates are not executable planner products",
            "Other sinks remain visible through full-pipeline analysis",
            "generic hash aggregate lookup/update use",
            "not typed-helper success states",
            "Distinct grouped aggregates are a separate native protocol",
            "`hash-aggregate-distinct-grouped-state-protocol-boundary`",
            "`hash-aggregate-distinct-lookup-protocol-boundary`",
            "without relabeling DuckDB sink",
            "Stateful source rows for `HASH_JOIN` must expose the join protocol",
            "Hash-join build append is not limited to one primitive key",
            "Typed non-equality match predicates are part of the native probe",
            "Correlated MARK joins use the same",
            "MARK probe output is a native probe output mode",
            "runtime filter-pushdown probe state",
            "profiler time must be allocated",
            "It must never copy the full",
            "operator time into each protocol row",
            "allocated source-boundary",
            "Stateful source rows for aggregate operators must expose",
            "aggregate state",
            "protocol inventory",
            "stateful_native_state_scan",
            "stateful_native_source",
            "execution=native-source",
            "native_state_scan_*",
            "order-by-native-state-scan",
            "top-n-native-state-scan",
            "perfect-hash required-bit metadata",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "SljitCanExecuteSourceNode",
            "native_source_contract.status == JitRegionNativeSourceStatus::READY",
            "native_state_scan_contract.status == JitRegionStateContractStatus::READY",
            "PlanSljitNativeStatefulSourceNode",
            "DuckDB native stateful source runtime",
            "native stateful source requires a ready native-source contract",
            "AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE)",
        ],
    )


def verify_backend_registration_boundary(root: Path) -> None:
    assert_required_text(
        root,
        "src/include/duckdb/execution/jit/registration.hpp",
        [
            "class DatabaseInstance;",
            "RegisterJitBackend",
            "unique_ptr<JitBackend>",
        ],
    )
    assert_required_text(
        root,
        "src/execution/jit.cpp",
        [
            "#include \"duckdb/execution/jit/registration.hpp\"",
            "void RegisterJitBackend(DatabaseInstance &db, unique_ptr<JitBackend> backend)",
            "JitManager::Get(db).RegisterBackend(std::move(backend));",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_backend.cpp",
        [
            "#include \"duckdb/execution/jit/registration.hpp\"",
            "MayHaveAutoAdmissionRule",
            "SLJIT_AUTO_ADMISSION_RULES",
            "SLJIT_AUTO_ADMISSION_FAMILIES",
            "BuildSljitRegionCandidateShapeKey(candidate)",
            "candidate.signature.shape",
            "candidate.traits.has_table_scan_source",
            "candidate.traits.expression_traits_known",
            "candidate.traits.integer_arithmetic_projection_count",
            "candidate.traits.non_integer_arithmetic_projection_count",
            "candidate.traits.integer_comparison_filter_count",
            "candidate.traits.non_integer_comparison_filter_count",
            "IsSljitNativeSourceFilterProjectionCandidate",
            "BuildSljitRegionCandidateContextShapeKey",
            "SetSljitAdmissionInfo",
            "no admitted production performance proof",
            "full-pipeline region",
            "requires measured operator-aware admission proof",
            "sink pipeline region",
            "estimated cardinality below admitted SLJIT",
            "min_cardinality",
            "candidate cannot map to an admitted SLJIT",
            "RegisterJitBackend(loader.GetDatabaseInstance(), make_uniq<SljitJitBackend>());",
        ],
    )
    assert_required_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "source-fusion-gap:requires-native-source",
            "source-strategy=duckdb-source-helper",
        ],
    )
    assert_no_text(
        root,
        [Path("extension/jit_sljit/sljit_backend.cpp")],
        [
            "benchmark/tpch/jit/tpch_benchmark:duckdb_filtered_source_projection_ungrouped_sum",
            "IsSljitFilteredSourceProjectionUngroupedSumCandidate",
            "IsSljitFusedUngroupedAggregateUpdateCandidate",
            "IsSljitFusedUngroupedAggregateUpdateInventory",
            "SLJIT_FULL_PIPELINE_FILTER_PROJECTION_UNGROUPED_SUM_SHAPE",
            "SLJIT_FULL_PIPELINE_PROJECTION_UNGROUPED_SUM_SHAPE",
        ],
    )
    assert_no_text(
        root,
        [
            Path("extension/jit_sljit"),
            Path("src/execution/jit_region_ir.cpp"),
            Path("test/api/test_jit.cpp"),
            Path("JIT_ARCHITECTURE.md"),
        ],
        [
            removed_name("filtered-source-", "handoff"),
            removed_name("filtered_source_", "handoff"),
            removed_name("FILTERED_SOURCE_", "HANDOFF"),
            removed_name("source-filters-owned", "-by-duckdb-source"),
            removed_name("CanAddDuckDBFilteredSource", "HandoffCandidate"),
            "AddJitRegionCandidateWithSourceAlternatives",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "A single positive",
            "query-local proof gap is not an admission rule.",
            "must not inspect TPCH query text",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "registration.hpp` exposes the narrow backend",
            "Backend extensions should",
            "include this instead of `manager.hpp`",
            "force/debug capability",
            "sljit:full-pipeline:filter-projection-ungrouped-aggregate-update",
        ],
    )


def verify_trace_contracts(root: Path) -> None:
    forbidden_single_query_trace_files = (
        "benchmark/tpch/jit/q06_auto.benchmark",
        "benchmark/tpch/jit/q06_force.benchmark",
        "benchmark/tpch/jit/q06_off.benchmark",
        "benchmark/tpch/jit/tpch_q06_jit.benchmark.in",
    )
    for path in forbidden_single_query_trace_files:
        if (root / path).exists():
            fail(f"single-query TPC-H JIT benchmark still exists: {path}")

    for path in (
        "benchmark/jit/trace_manifest.py",
        "benchmark/jit/jit_sql_trace.py",
        "benchmark/jit/verify_jit_sql_trace.py",
        "benchmark/micro/jit/micro_jit_benchmark.py",
        "benchmark/micro/jit/verify_micro_jit_benchmark.py",
        "benchmark/micro/jit/micro_jit_diagnostic_benchmark.py",
        "benchmark/micro/jit/verify_micro_jit_diagnostic_benchmark.py",
        "benchmark/micro/jit/micro_jit_diagnostic_sweep.py",
        "benchmark/micro/jit/verify_micro_jit_diagnostic_sweep.py",
        "benchmark/micro/jit/verify_micro_jit_inventory.py",
        "benchmark/micro/jit/micro_jit_trace.py",
        "benchmark/micro/jit/verify_micro_jit_trace.py",
        "benchmark/tpch/jit/tpch_trace.py",
        "benchmark/tpch/jit/verify_tpch_trace.py",
        "benchmark/tpch/jit/tpch_benchmark.py",
        "benchmark/tpch/jit/verify_tpch_benchmark.py",
    ):
        if not (root / path).exists():
            fail(f"missing trace contract file: {path}")
        assert_required_text(
            root,
            "benchmark/tpch/jit/tpch_trace.py",
            [
            "operator_profile_summary.csv",
            "capability_gap_summary.csv",
            "capability_priority_summary.csv",
            "query_capability_priority_summary.csv",
            "expression_fallback_summary.csv",
            "source_boundary_summary.csv",
            "source_boundary_priority_summary.csv",
            "source_fusion_gap_summary.csv",
            "fusion_blocker_summary.csv",
            "stage_pipeline_summary.csv",
            "pipeline_runtime_summary.csv",
            "admission_efficiency_summary.csv",
            "decision_counters_csv",
            "decision_counter_summary.csv",
            "duckdb_jit_decision_counters()",
            "duckdb_jit_clear_counters()",
            "pipeline_shape",
            "example_reason",
                "prepare_trace_output_directory",
                "default_trace_output_directory(\"tpch_trace\")",
                "write_trace_manifest",
            "TRACE_MANIFEST_NAME",
            "collect_stage_pipeline_summary",
            "collect_pipeline_runtime_summary",
            "collect_admission_efficiency_summary",
            "auto_admitted_helper_dominated_region",
            "source_filter_loop_not_generated",
            "is_trace_wrapper_pipeline_shape",
            "return [entry for entry in entries if not is_trace_wrapper_region(entry)]",
            "force_top_profile_operator",
            "force_scan_join_groupby_profile_percent",
            "force_row_processing_kernels",
            "compiled_kernels_not_reached",
            "partial_compiled_kernel_reach",
            "collect_capability_gap_summary",
            "classify_capability_gap",
            "if node[\"boundary\"] in (\"source-native\", \"operator-native\", \"sink-native\"):",
            "attribution_pipeline_shape",
            "CAPABILITY_RUNTIME_FIELDS",
            "collect_capability_runtime_summary",
            "collect_capability_priority_summary",
            "collect_query_capability_priority_summary",
            "collect_expression_fallback_summary",
            "iter_expression_fallback_details",
            "expression_fallback_example_text",
            "EXPRESSION_FALLBACK_MARKER",
            "collect_source_boundary_summary",
            "collect_source_boundary_priority_summary",
            "collect_source_fusion_gap_summary",
            "collect_fusion_blocker_summary",
            "FUSION_BLOCKER_RE",
            "FUSION_BLOCKER_SUMMARY_FIELDS",
            "fusion_blocker_rows",
            "is_source_fusion_gap_event",
            "NATIVE_SOURCE_CONTRACT_RE",
            "extract_native_source_contract",
            "native_source_status",
            "native_source_required_capability",
            "native_source_blocker",
            "source-fusion-gap:requires-native-source",
            "collect_region_runtime_by_candidate",
            "runtime_input_rows",
            "runtime_time_us",
            "allocate_source_boundary_profile_time",
            "format_profile_operator_allocations",
            "profile_key_occurrences",
            "event.get(\"phase\") not in (\"decision\", \"compile\")",
            "source_helper_output_rows",
            "source_helper_runtime_time_us",
            "source_native_runtime_time_us",
            "generated_body_runtime_time_us",
            "extract_source_boundary_details",
            "clean_source_boundary_field_value",
            "iter_source_boundary_event_entries",
            "SOURCE_BOUNDARY_MARKERS",
            "DuckDB hash join native state scan protocol",
            "DuckDB hash join state scan source does not produce rows",
            "DuckDB hash aggregate native state scan protocol",
            "DuckDB aggregate source state protocol missing",
            "join_type",
            "condition_count",
            "filter_pushdown_probe_count",
            "lhs_output_column_indices",
            "aggregate_operator_kind",
            "aggregate_functions",
            "native_state_scan_contract_status",
            "native_state_scan_required_capability",
            "native_state_scan_blocker",
            "native_hash_join_probe_contract_status",
            "native_hash_join_build_contract_status",
            "native_hash_aggregate_lookup_contract_status",
            "perfect_required_bits_total",
            "candidate_scope",
            "candidate_scopes",
            "admission_shape_key",
            "admission_rule_present",
            "admission_proof",
            "append_micro_diagnostic_snapshot",
            "Micro Diagnostic Rejection Snapshot",
            "micro_diagnostic_dir",
            "force_compiled_pipeline_scopes",
            "auto_top_skip_scope",
            "force_top_unsupported_scope",
        ],
    )
    assert_no_text(
        root,
        [Path("benchmark/tpch/jit/tpch_trace.py"), Path("benchmark/tpch/jit/tpch_schema.py"),
         Path("benchmark/tpch/jit/verify_tpch_trace.py")],
        [
            "typed_hash_join_probe_helper",
            "typed_hash_join_build_helper",
            "typed_hash_aggregate_lookup_helper",
            "hash-join-typed",
            "hash-aggregate-typed-lookup-helper",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/verify_tpch_trace.py",
        [
            "stage_pipeline_summary.csv",
            "capability_gap_summary.csv",
            "capability_priority_summary.csv",
            "query_capability_priority_summary.csv",
            "expression_fallback_summary.csv",
            "source_boundary_summary.csv",
            "source_boundary_priority_summary.csv",
            "source_fusion_gap_summary.csv",
            "fusion_blocker_summary.csv",
            "pipeline_runtime_summary.csv",
            "decision_counters_csv",
            "decision_counter_summary.csv",
            "verify_trace_manifest",
            "expected_query_ids",
            "expected queries",
            "verify_capability_gaps",
            "verify_capability_priorities",
            "verify_query_capability_priorities",
            "if node[\"boundary\"] in (\"source-native\", \"operator-native\", \"sink-native\"):",
            "hash_aggregate_missing_grouped_state_blocker",
            "hash_aggregate_missing_lookup_blocker",
            "hash-aggregate-distinct-grouped-state-protocol-boundary",
            "hash-aggregate-distinct-lookup-protocol-boundary",
            "CAPABILITY_PRIORITY_REQUIRED_COLUMNS",
            "QUERY_CAPABILITY_PRIORITY_REQUIRED_COLUMNS",
            "collect_capability_runtime_from_kernel_rows",
            "runtime field does not match kernel summary",
            "verify_expression_fallback_summary",
            "verify_source_boundary_summary",
            "verify_source_boundary_priorities",
            "verify_source_fusion_gaps",
            "verify_fusion_blockers",
            "SOURCE_FUSION_GAP_REQUIRED_COLUMNS",
            "FUSION_BLOCKER_REQUIRED_COLUMNS",
            "operator-fusion-gap:multi-operator-helper-chain-protocol-missing",
                "operator-fusion-gap:downstream-operator-helper-resume-protocol-missing",
                "operator-fusion-gap:upstream-operator-resume-protocol-missing",
                "operator-fusion-gap:downstream-helper-continuation-protocol-missing",
                "operator-fusion-gap:hash-join-probe-native-lowering-missing",
                "sink-fusion-gap:hash-join-build-protocol-missing",
                "sink-fusion-gap:hash-join-build-native-lowering",
                "sink-fusion-gap:upstream-operator-resume-protocol-missing",
            "source-fusion-gap:downstream-operator-helper-resume-protocol-missing",
            "requires_native_source",
            "compiled gap lacks kernel runtime",
            "requires-native-sink-or-operator-update",
            "native_source_required_capability",
            "duckdb-table-scan-native-source",
            "duckdb-getdata-helper-boundary",
            "verify_state_scan_contract",
            "hash-join-native-state-scan",
            "hash-aggregate-native-state-scan",
            "perfect-hash-aggregate-native-state-scan",
            "ungrouped-aggregate-native-state-scan",
            "native_hash_join_probe_contract_status",
            "lhs_output_column_indices",
            "native_hash_join_build_contract_status",
            "native_hash_aggregate_lookup_contract_status",
            "compiled source-helper rows lack runtime source-helper totals",
            "source_helper_output_rows",
            "source_helper_runtime_time_us",
            "source_native_runtime_time_us",
            "generated_body_runtime_time_us",
            "verify_pipeline_runtime",
            "verify_source_boundary_features",
            "verify_expression_fallback_reasons",
            "DuckDB table scan source helper boundary",
            "DuckDB hash join native state scan protocol",
            "DuckDB hash join state scan source does not produce rows",
            "SOURCE_BOUNDARY_HASH_JOIN_REQUIRED_FIELDS",
            "SOURCE_BOUNDARY_HASH_JOIN_NUMERIC_FIELDS",
            "SOURCE_BOUNDARY_AGGREGATE_REQUIRED_FIELDS",
            "SOURCE_BOUNDARY_AGGREGATE_NUMERIC_FIELDS",
            "verify_bool_field",
            "allocated source profile time exceeds measured policy",
            "allocated source profile time exceeds measured operator",
            "DuckDB hash aggregate native state scan protocol",
            "DuckDB aggregate source state protocol missing",
            "aggregate_operator_kind=hash",
            "aggregate_operator_kind=perfect_hash",
            "aggregate_operator_kind=ungrouped",
            "projection_pushdown=",
            "dynamic_filters=",
            "join_type=",
            "condition_count=",
            "non-producing hash join source was not excluded",
            "row[\"source_produces_rows\"] != \"true\"",
            "filter_pushdown_probe_count=",
            "core expression lowering unsupported;",
            "expression_index=",
            "occurrence totals do not match operator gaps",
            "verify_stage_pipelines",
            "trace wrapper leaked into stage summary",
            "source-helper pipeline was compiled as executable JIT",
            "unsupported executable candidate scope",
            "sink blocker row is not full pipeline",
            "trace wrapper leaked into force unsupported",
            "trace wrapper leaked into example pipeline",
            "unsupported label without relevant unsupported",
            "compiled/reached/unreached mismatch",
            "compiled/row/zero-input mismatch",
            "missing unreached-kernel label",
            "candidate_scope",
            "candidate_scopes",
            "force_compiled_pipeline_scopes",
            "verify_scope_summary_field",
            "full_pipeline",
            "ADMISSION_SUMMARY_FIELDS",
            "REGION_DECISION_REQUIRED_COLUMNS",
            "FLOW_STEP_REQUIRED_COLUMNS",
            "verify_admission_metadata",
            "jit_policy=auto skips region kernel without admitted performance proof",
            "jit_policy=auto skips region before backend analysis",
            "auto precheck skip performed backend analysis",
            "admission_rule=missing",
            "WRAPPER_ONLY_PIPELINE_SHAPES",
            "WRAPPER_ONLY_REGION_SOURCE_MARKERS",
            "wrapper-only pipeline leaked into counters",
            "wrapper-only source leaked into counters",
        ],
    )
    assert_no_text(
        root,
        [Path("benchmark/tpch/jit/verify_tpch_trace.py")],
        [
            "missing inner hash-join protocol rows",
            "missing left delimiter hash-join protocol rows",
            "required_hash_join_blocked_state_scan_features",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/tpch_benchmark.py",
        [
            "tpch_jit_benchmark",
            "default_trace_output_directory(\"tpch_benchmark\")",
            "event_log_size",
            "repeats",
            "policy_summary.csv",
            "correctness_summary.csv",
            "operator_profile_summary.csv",
            "decision_counter_summary.csv",
            "admission_proof_gap_summary.csv",
            "collect_admission_proof_gap_summary",
            "counter_table",
            "duckdb_jit_decision_counters()",
            "duckdb_jit_clear_counters()",
            "pipeline_shape",
            "pipeline_estimated_cardinality",
            "example_reason",
            "write_trace_manifest",
            "This harness keeps diagnostic tracing separate from production timing",
        ],
    )
    assert_required_text(
        root,
        "benchmark/tpch/jit/verify_tpch_benchmark.py",
        [
            "tpch_jit_benchmark",
            "verify_trace_manifest",
            "runs.csv",
            "policy_summary.csv",
            "correctness_summary.csv",
            "decision_counter_summary.csv",
            "admission_proof_gap_summary.csv",
            "verify_admission_proof_gaps",
            "operator_profile_summary.csv",
            "pipeline_shape",
            "pipeline_estimated_cardinality",
            "example_reason",
            "candidate_pipeline_shape",
            "setup pipeline leaked into candidate counters",
            "wrapper-only pipeline leaked into counters",
            "WRAPPER_ONLY_REGION_SOURCE_MARKERS",
            "wrapper-only source leaked into counters",
            "non-zero correctness diff",
            "non-positive query timing",
            "speed classification mismatch",
            "auto compiled regions without positive production speedup",
            "auto compiled regions but lost to off policy",
            "force policy did not compile any diagnostic regions",
            "--queries",
        ],
    )
    assert_required_text(
        root,
        "src/include/duckdb/common/enums/statement_type.hpp",
        [
            "bool suppress_jit",
            "Whether or not JIT should be suppressed for this statement and its result wrapper pipelines",
        ],
    )
    assert_required_text(
        root,
        "src/planner/binder/tableref/bind_table_function.cpp",
        [
            "IsJitSystemTableFunction",
            "GetStatementProperties().suppress_jit = true",
        ],
    )
    assert_no_text(
        root,
        [Path("src/planner/binder/tableref/bind_table_function.cpp")],
        ["context.SuppressJitForCurrentQuery()"],
    )
    assert_required_text(
        root,
        "src/include/duckdb/execution/executor.hpp",
        [
            "void SetSuppressJit(bool suppress_jit_p)",
            "bool IsJitSuppressed() const",
            "Statement-local JIT suppression",
        ],
    )
    assert_required_text(
        root,
        "src/parallel/pipeline.cpp",
        [
            "executor.IsJitSuppressed()",
            "JitManager::IsJitIntrospectionPipeline(*this)",
            "jit_prepared_pipeline.reset()",
        ],
    )
    assert_no_text(
        root,
        [Path("src/function/table/system")],
        ["context.SuppressJitForCurrentQuery()"],
    )
    assert_required_text(
        root,
        "src/main/client_context.cpp",
        [
            "statement_data.properties.suppress_jit",
            "executor.SetSuppressJit",
        ],
    )
    assert_required_text(
        root,
        "src/planner/binder/statement/bind_set.cpp",
        [
            "properties.suppress_jit = true",
        ],
    )
    assert_no_text(
        root,
        [
            Path("src/planner/binder/statement/bind_set.cpp"),
        ],
        ["context.SuppressJitForCurrentQuery()"],
    )
    assert_required_text(
        root,
        "test/api/test_jit.cpp",
        [
            "COPY (SELECT * FROM duckdb_jit_decision_counters())",
            "jit_decision_counters_copy.csv",
            "JIT introspection does not suppress later statements in one SQL batch",
            "SELECT * FROM duckdb_jit_clear_events();",
            "execution:native-sljit-region-filter-projection-ungrouped-aggregate-update",
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "Binding a `duckdb_jit_*` table function must not set query-global JIT suppression",
            "binding sets the statement-local suppression property",
            "prepared execution transfers that property onto the `Executor`",
            "pipeline preparation and executor-time compilation observe different JIT state",
            "`JitSuppressionGuard` only while reading or mutating JIT metadata",
            "a later analytical statement in the same SQL batch must still be able to compile fused regions",
        ],
    )
    assert_required_text(
        root,
        "benchmark/jit/trace_manifest.py",
        [
            "default_trace_output_directory",
            "tempfile.mkdtemp",
            "prepare_trace_output_directory",
            "trace output directory is not empty",
            "unexpected files in trace directory",
            "manifest files missing from trace directory",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_trace.py",
        [
            "admitted_trace_shapes",
            "SHAPES = admitted_trace_shapes()",
            "default_trace_output_directory(\"micro_trace\")",
            "write_trace_manifest",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_benchmark.py",
        [
            "admitted_threshold_shapes",
            "SHAPES = admitted_threshold_shapes()",
            "benchmark_runner",
            "speedup_vs_off",
            "default_trace_output_directory(\"micro_benchmark\")",
            "write_trace_manifest",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_benchmark.py",
        [
            "micro_jit_benchmark",
            "expected at least five benchmark runs",
            "speedup mismatch",
            "faster_than_off flag mismatch",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_diagnostic_benchmark.py",
        [
            "micro_jit_diagnostic_benchmark",
            "diagnostic_benchmark_shapes",
            "DIAGNOSTIC_SHAPES = diagnostic_benchmark_shapes()",
            "default_trace_output_directory(\"micro_diagnostic_benchmark\")",
            "write_trace_manifest",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_diagnostic_benchmark.py",
        [
            "micro_jit_diagnostic_benchmark",
            "MIN_ADMITTED_SPEEDUP",
            "diagnostic threshold",
            "promote it with proof",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_diagnostic_sweep.py",
        [
            "micro_jit_diagnostic_sweep",
            "family_summary.csv",
            "diagnostic_sweep_shapes",
            "MIN_ADMITTED_SPEEDUP",
            "candidate_shape",
            "admission_shape_key",
            "duckdb_jit_events()",
            "default_trace_output_directory(\"micro_diagnostic_sweep\")",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_diagnostic_sweep.py",
        [
            "micro_jit_diagnostic_sweep",
            "family_summary.csv",
            "first_row_count_at_margin",
            "generated force benchmark missing",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_full_pipeline_selectivity_sweep.py",
        [
            "micro_jit_full_pipeline_selectivity_sweep",
            "sljit:full-pipeline:filter-projection-ungrouped-aggregate-update",
            "scan-filter-scan-project-projection-sink",
            "selected_source_execution='native-source'",
            "candidate_sink_kind='ungrouped-aggregate-update'",
            "selectivity_summary.csv",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_full_pipeline_selectivity_sweep.py",
        [
            "micro_jit_full_pipeline_selectivity_sweep",
            "selectivity_summary.csv",
            "first_selectivity_percent_at_margin",
            "generated force benchmark missing",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_trace.py",
        [
            "micro_jit_trace",
            "admission proof mismatch",
            "expected native compiled regions",
            "force did not compile a native fused region",
            "expected one non-fused source-boundary",
            "candidate_scope",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/verify_micro_jit_inventory.py",
        [
            "micro_inventory",
            "diagnostic family",
            "backend admission proofs mismatch",
            "missing admitted",
            "missing diagnostic",
        ],
    )
    assert_required_text(
        root,
        "benchmark/micro/jit/micro_jit_manifest.py",
        [
            "ADMITTED_FAMILIES",
            "DIAGNOSTIC_FAMILIES",
            "native_filter_projection",
            "native_projection_chain",
            "sljit:source-prefix:filter-projection",
            "sljit:source-prefix:projection-chain",
            "native_filter",
            "native_filter_projection_generic",
            "sljit:source-prefix:filter",
            "sljit:source-prefix:filter-projection-projection",
        ],
    )
    assert_required_text(
        root,
        "benchmark/jit/jit_sql_trace.py",
        [
            "sql_equivalence_matrix",
            "region_resume_state_fallback",
            "jit_matrix_complex",
            "jit_matrix_temporal_result",
            "jit_matrix_interval_result",
            "test_surface_coverage.csv",
            "flow_step_summary.csv",
            "collect_test_surface_coverage",
            "collect_flow_step_summary",
            "FLOW_STEP_SUMMARY_FIELDS",
            "prepare_trace_output_directory",
            "default_trace_output_directory(\"sql_trace\")",
            "write_trace_manifest",
        ],
    )
    assert_required_text(
        root,
        "benchmark/jit/verify_jit_sql_trace.py",
        [
            "verify_trace_manifest",
            "verify_resume_state_events",
            "verify_test_surface_coverage",
            "verify_flow_step_summary",
            "test_surface_coverage.csv",
            "flow_step_summary.csv",
            "duplicate test surface rows",
            "exact test surface mismatch",
            "kernel/row/zero-input mismatch",
            "non-region JIT target appeared",
            "region event missing pipeline shape",
            "candidate_scope",
            "event IDs are not monotonic and unique",
            "MATRIX_REQUIRED_TEXT",
        ],
    )
    assert_absent_paths(
        root,
        [
            "benchmark/jit/trace-out",
            "benchmark/micro/jit/benchmark-out",
            "benchmark/micro/jit/diagnostic-benchmark-out",
            "benchmark/micro/jit/diagnostic-sweep-out",
            "benchmark/micro/jit/trace-out",
            "benchmark/tpch/jit/benchmark-out",
            "benchmark/tpch/jit/trace-out",
            "benchmark/tpch/jit/TPC_H_JIT_TRACE_REPORT.md",
            removed_name("src/execution/jit_", "expression_executor.cpp"),
            removed_name("src/include/duckdb/execution/jit/", "expression_executor.hpp"),
            removed_name("extension/jit_sljit/sljit_", "expression.cpp"),
            removed_name("extension/jit_sljit/sljit_native_", "expression_compile.cpp"),
            removed_name("extension/jit_sljit/sljit_native_", "expression.cpp"),
            removed_name("extension/jit_sljit/include/sljit_", "expression.hpp"),
            removed_name("extension/jit_sljit/include/sljit_native_", "expression.hpp"),
            removed_name("extension/jit_sljit/include/sljit_native_", "expression_runtime.hpp"),
        ],
    )
    assert_no_generated_artifacts(
        root,
        [
            Path("benchmark/jit"),
            Path("benchmark/micro/jit"),
            Path("benchmark/tpch/jit"),
        ],
    )
    assert_no_text(
        root,
        [
            Path("benchmark/jit/jit_sql_trace.py"),
            Path("benchmark/micro/jit/micro_jit_benchmark.py"),
            Path("benchmark/micro/jit/micro_jit_diagnostic_benchmark.py"),
            Path("benchmark/micro/jit/micro_jit_diagnostic_sweep.py"),
            Path("benchmark/micro/jit/micro_jit_trace.py"),
            Path("benchmark/tpch/jit/tpch_benchmark.py"),
            Path("benchmark/tpch/jit/tpch_trace.py"),
        ],
        [
            'root / "benchmark" / "jit" / "trace-out"',
            'root / "benchmark" / "micro" / "jit" / "benchmark-out"',
            'root / "benchmark" / "micro" / "jit" / "diagnostic-benchmark-out"',
            'root / "benchmark" / "micro" / "jit" / "diagnostic-sweep-out"',
            'root / "benchmark" / "micro" / "jit" / "trace-out"',
            'root / "benchmark" / "tpch" / "jit" / "benchmark-out"',
            'root / "benchmark" / "tpch" / "jit" / "trace-out"',
        ],
    )
    assert_required_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "flow_step_summary.csv",
            "expression_fallback_summary",
            "source_boundary_summary",
            "source_boundary_priority_summary",
            "benchmark/tpch/jit/tpch_benchmark.py",
            "production timing harness",
            "jit_event_log_size=0",
            "policy_summary.csv",
            "correctness_summary.csv",
            "decision_counter_summary.csv",
            "admission_proof_gap_summary.csv",
            "A performance claim should use the benchmark harness for repeated timing",
            "`verify_tpch_benchmark.py` is also an admission-policy gate",
            "compiled auto query must",
            "force speedups or slowdowns are",
            "candidate shape, and candidate scope, then reconciles event counts",
            "timing, runtime rows, kernel reachability, declined invocations, and fallback",
            "existing-test equivalent of the TPC-H stage/runtime summaries",
            "auto-admission precheck",
            "JitRegionPipelineInventory",
            "jit_policy=auto skips pipeline before typed IR lowering",
            "skipped pipeline inventory row or candidate row",
            "must lower full typed region IR before",
            "backend analysis so diagnostics",
            "backend-analysis time",
            "Pure wrapper pipelines such as",
            "must not create pipeline-inventory decision counters",
            "must not enter typed region lowering",
        ],
    )

def verify_text_boundaries(root: Path) -> None:
    forbidden_text = [
        "Execute" + "Interpreted",
        "Value::" + "Evaluate",
        "Vector::" + "SetValue",
        "native-" + "filter-projection",
        "value-" + "loop",
        "source-pushed filters require native source-prefix fusion",
        "benchmark/tpch/jit/tpch_benchmark:source_prefix_native_filter_projection",
        "build/jit_tpch",
        "currently admitted SLJIT",
        "admitted SLJIT table-scan filter/projection source-prefix family",
        "core region lowering produced " + "no JIT IR",
        removed_name("jit_", "expression_executor"),
        removed_name("JitExpression", "CompilationInput"),
        removed_name("JitExpression", "CompileResult"),
        removed_name("JitExpression", "Kernel"),
        removed_name("JitExpression", "Capability"),
        removed_name("Supports", "Expressions"),
        removed_name("TryCompile", "Expression"),
        removed_name("sljit_", "expression"),
        removed_name("sljit_native_", "expression"),
        removed_name("supports_", "expressions"),
        removed_name("JitCompileTarget::", "EXPRESSION"),
        removed_name("compiled_", "expressions"),
        removed_name("expression_", "native_and_unsupported"),
        removed_name("force_small_native_", "isl", "ands"),
        removed_name("native", "-isl", "and"),
        removed_name("native", " isl", "and"),
        removed_name("helper", "-isl", "and"),
        removed_name("middle", "-isl", "and"),
        removed_name("generated", " isl", "ands"),
    ]
    assert_no_text(
        root,
        [
            Path("src/execution/jit.cpp"),
            Path("src/execution/jit_expression_ir.cpp"),
            Path("src/execution/jit_operator_descriptor.cpp"),
            Path("src/execution/jit_region_executor.cpp"),
            Path("src/execution/jit_region_ir.cpp"),
            Path("src/execution/jit_runtime.cpp"),
            Path("src/execution/jit_types.cpp"),
            Path("src/include/duckdb/execution/jit"),
            Path("extension/jit_sljit"),
            Path("test/sql/jit"),
            Path("test/api/test_jit.cpp"),
            Path("benchmark/jit"),
            Path("benchmark/micro/jit"),
            Path("benchmark/tpch/jit"),
            Path("JIT_ARCHITECTURE.md"),
        ],
        forbidden_text,
        excluded={Path("benchmark/jit/verify_jit_architecture.py")},
    )


def verify_include_boundaries(root: Path) -> None:
    assert_absent_paths(root, ["extension/jit_sljit/include/sljit_native.hpp"])

    core_paths = [
        Path("src/execution/jit.cpp"),
        Path("src/execution/jit_expression_ir.cpp"),
        Path("src/execution/jit_operator_descriptor.cpp"),
        Path("src/execution/jit_region_executor.cpp"),
        Path("src/execution/jit_region_ir.cpp"),
        Path("src/execution/jit_runtime.cpp"),
        Path("src/execution/jit_types.cpp"),
        Path("src/include/duckdb/execution/jit"),
    ]
    assert_no_forbidden_includes(
        root,
        core_paths,
        ["sljitLir.h", "third_party/sljit", "extension/jit_sljit", "jit_sljit_extension", "duckdb_sljit"],
    )

    assert_no_forbidden_includes(
        root,
        [Path("src/include/duckdb/execution/jit")],
        [
            "duckdb/planner/",
            "duckdb/execution/expression_executor",
            "duckdb/execution/physical_operator",
            "duckdb/execution/operator/",
            "duckdb/parallel/",
        ],
    )

    assert_no_forbidden_includes(
        root,
        [Path("extension/jit_sljit")],
        [
            "duckdb/execution/expression_executor",
            "duckdb/execution/expression_executor_state",
            "duckdb/execution/jit/manager.hpp",
            "duckdb/execution/physical_operator",
            "duckdb/execution/physical_operator_states",
            "duckdb/execution/operator/",
            "duckdb/execution/jit/lowering.hpp",
            "duckdb/parallel/pipeline",
            "duckdb/parallel/pipeline_executor",
            "duckdb/parallel/executor",
            "duckdb/planner/",
        ],
    )

    assert_no_raw_sljit_codegen_outside(
        root,
        {
            Path("extension/jit_sljit/sljit_native_codegen.cpp"),
            Path("extension/jit_sljit/sljit_region_codegen.cpp"),
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify DuckDB JIT architecture source boundaries")
    parser.add_argument("--root", type=Path, default=repo_root())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    verify_database_owned_manager(root)
    verify_static_extension(root)
    verify_core_sources_registered(root)
    verify_backend_contract_tests(root)
    verify_region_executor_resume_contract(root)
    verify_native_operator_runtime_contract(root)
    verify_native_sink_update_contract(root)
    verify_region_selection_contract(root)
    verify_region_execution_form_contract(root)
    verify_stage_region_contract(root)
    verify_prepared_region_contract(root)
    verify_core_lowering_boundary(root)
    verify_backend_registration_boundary(root)
    verify_trace_contracts(root)
    verify_text_boundaries(root)
    verify_include_boundaries(root)
    print("ok jit architecture source and sink boundaries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
