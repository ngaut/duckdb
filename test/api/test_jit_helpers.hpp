#pragma once

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"
#include "duckdb/main/settings.hpp"

using namespace duckdb;

namespace {

static constexpr const char *JIT_HASH_JOIN_PROBE_READY_CONTRACT = "native_hash_join_probe_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_BUILD_READY_CONTRACT = "native_hash_join_build_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_READY_BLOCKER = "native_hash_join_probe_blocker=none";
static constexpr const char *JIT_HASH_JOIN_BUILD_READY_BLOCKER = "native_hash_join_build_blocker=none";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_READY = "native-hash-join-probe-executable=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON = "generated native hash join probe";
static constexpr const char *JIT_HASH_JOIN_BUILD_GENERATED_PROTOCOL_REASON =
    "hash_join_build_native<execution=primitive-protocol-build";
static constexpr const char *JIT_NESTED_LOOP_JOIN_PROBE_EXECUTABLE_REASON = "generated native nested loop join probe";
static constexpr const char *JIT_NESTED_LOOP_JOIN_BUILD_EXECUTABLE_REASON =
    "native nested loop join build sink contract";

static bool IsCompiledExecutionMode(const string &execution_mode) {
	return execution_mode == "native";
}

static string EventPhase(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventPhaseToString(event.phase_kind);
}

static string EventTarget(const ExecutionRegionEvent &event) {
	return ExecutionRegionCompileTargetToString(event.target_kind);
}

static string EventStatus(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventStatusToString(event.status_kind);
}

static string EventExecutionMode(const ExecutionRegionEvent &event) {
	return ExecutionRegionExecutionModeToString(event.execution_mode_kind);
}

static string EventRegionExecutionForm(const ExecutionRegionEvent &event) {
	return ExecutionRegionFormToString(event.region_execution_form_kind);
}

static string EventExecutionBody(const ExecutionRegionEvent &event) {
	return ExecutionRegionExecutionBodyToString(event.execution_body_kind);
}

static string EventRequestedPolicy(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventPolicyToString(event.requested_policy_kind);
}

static bool CandidateHasStructure(const ExecutionRegionEvent &event, idx_t filters, idx_t projections,
                                  ExecutionRegionSinkKind sink_kind) {
	return event.has_candidate && event.candidate_traits.filter_count == filters &&
	       event.candidate_traits.projection_count == projections && event.candidate_traits.sink_kind == sink_kind;
}

static void RequireCandidateStructure(const ExecutionRegionEvent &event, idx_t filters, idx_t projections,
                                      ExecutionRegionSinkKind sink_kind) {
	REQUIRE(CandidateHasStructure(event, filters, projections, sink_kind));
}

static string EventSourceStageRuntimeBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageRuntimeBreakdown(event.source_stage_runtime);
}

static string EventSourceStageCountBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageCountBreakdown(event.source_stage_runtime);
}

static string EventGeneratedStageRuntimeBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageRuntimeBreakdown(event.generated_stage_runtime);
}

static string EventGeneratedStageCountBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageCountBreakdown(event.generated_stage_runtime);
}

struct JitTestDatabase {
	JitTestDatabase() : con(db), context(*con.context), manager(ExecutionRegionManager::Get(context)) {
	}

	DuckDB db;
	Connection con;
	ClientContext &context;
	ExecutionRegionManager &manager;
};

static void RequireStatefulSourceMissingContractABI(const ExecutionRegionEvent &event,
                                                    bool &found_source_only_contract) {
	const auto &contract = event.candidate_contract;
	ExecutionRegionABI expected_abi;
	if (contract.OwnsSink()) {
		expected_abi = ExecutionRegionABI::FULL_PIPELINE;
	} else {
		expected_abi = ExecutionRegionABI::NONE;
		found_source_only_contract = true;
	}
	REQUIRE(contract.abi == expected_abi);
	REQUIRE(StringUtil::Contains(contract.ir, "contract<abi=" + string(ExecutionRegionABIToString(expected_abi))));
}

static void RequireStatefulSourceNativeContractABI(const ExecutionRegionEvent &event,
                                                   bool &found_source_only_contract) {
	const auto &contract = event.candidate_contract;
	ExecutionRegionABI expected_abi;
	if (contract.OwnsSink()) {
		expected_abi = ExecutionRegionABI::FULL_PIPELINE;
	} else {
		expected_abi = ExecutionRegionABI::NONE;
		found_source_only_contract = true;
	}
	REQUIRE(contract.abi == expected_abi);
	REQUIRE(contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
	REQUIRE(contract.state_scan_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
	REQUIRE(StringUtil::Contains(contract.ir, "contract<abi=" + string(ExecutionRegionABIToString(expected_abi))));
}

static idx_t TotalExecutionRegionCounterCount(const vector<ExecutionRegionCounter> &counters) {
	idx_t result = 0;
	for (auto &counter : counters) {
		result += counter.count;
	}
	return result;
}

static bool ContainsTypedIrNode(const string &ir, const string &node_kind, const string &logical_type,
                                const string &physical_type) {
	return StringUtil::Contains(ir, node_kind + "<logical=" + logical_type + ",physical=" + physical_type);
}

static void RequireRegionBoundaryCoreIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "core=(duckdb.region typed-vector-ir"));
	REQUIRE(StringUtil::Contains(ir, "candidate0<first_node="));
	REQUIRE(StringUtil::Contains(ir, "boundary=scan"));
	REQUIRE(StringUtil::Contains(ir, "boundary=sink"));
}

static void RequireFilterCoreRegionIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "input_format=unified-vector,output_format=selection-vector"));
	REQUIRE(StringUtil::Contains(ir, "vector_source=region-input"));
	REQUIRE(StringUtil::Contains(ir, "selection_source=input-selection"));
}

static void RequireProjectionCoreRegionIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "input_format=unified-vector,output_format=flat-vector"));
	REQUIRE(StringUtil::Contains(ir, "vector_source=operator-output"));
	REQUIRE(StringUtil::Contains(ir, "selection_source=filter-selection"));
}

static void RequireUnsupportedFilterProjectionSinkEvent(const ExecutionRegionEvent &event) {
	REQUIRE(EventExecutionMode(event) == "unsupported");
	REQUIRE(EventRegionExecutionForm(event) == "none");
	REQUIRE(event.has_candidate);
	REQUIRE(!event.candidate_pipeline_shape.empty());
	REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "source:source:"));
	REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "op0:filter:FILTER:none"));
	REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "op1:projection:PROJECTION:none"));
	REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, ":sink:"));
	REQUIRE(StringUtil::Contains(event.pipeline_shape, "source:source:"));
	REQUIRE(StringUtil::Contains(event.pipeline_shape, "sink:sink:"));
	REQUIRE(event.candidate_node_count > 0);
	REQUIRE(event.candidate_start_operator_index == 0);
	REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
	REQUIRE(event.candidate_estimated_cardinality > 0);
	REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native="));
	REQUIRE(StringUtil::Contains(event.reason, "source-contract-blocker:requires-source-contract"));
	REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:boundary"));
	REQUIRE(StringUtil::Contains(event.reason, "op0:FILTER:native:generated typed predicate filter"));
	REQUIRE(StringUtil::Contains(event.reason, "op1:PROJECTION:native:generated typed projection"));
	REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
	REQUIRE(StringUtil::Contains(event.reason, "append sink contract"));
	REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
	REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=result-collector-execution-sink"));
	REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
	REQUIRE(event.code_size == 0);
	REQUIRE(StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir"));
	RequireFilterCoreRegionIr(event.ir);
	RequireProjectionCoreRegionIr(event.ir);
}

static void SetJitTestOptions(ClientContext &context, const string &backend_name) {
	Settings::Set<EnableJitSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	Settings::Set<JitBackendSetting>(context, SetScope::SESSION, Value(backend_name));
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("force"));
}

static void LoadSljit(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
}

static void ConfigureSljitSettings(Connection &con, const string &policy = "force", bool verify = false,
                                   bool dump_ir = false, bool trace_runtime = false, idx_t event_log_size = 0) {
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='" + policy + "'"));
	if (verify) {
		REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	}
	if (dump_ir) {
		REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	}
	if (trace_runtime) {
		REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	}
	if (event_log_size > 0) {
		REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=" + to_string(event_log_size)));
	}
}

static void ConfigureSljit(Connection &con, const string &policy = "force", bool verify = false, bool dump_ir = false,
                           bool trace_runtime = false, idx_t event_log_size = 0) {
	LoadSljit(con);
	ConfigureSljitSettings(con, policy, verify, dump_ir, trace_runtime, event_log_size);
}

static void ClearJitTrace(ExecutionRegionManager &manager, bool counters = false) {
	manager.ClearEvents();
	if (counters) {
		manager.ClearCounters();
	}
}

static bool IsSljitRegionEvent(const ExecutionRegionEvent &event) {
	return event.backend_name == "sljit" && EventTarget(event) == "region";
}

static bool IsCompiledSljitRegionEvent(const ExecutionRegionEvent &event) {
	return IsSljitRegionEvent(event) && EventStatus(event) == "compiled";
}

static bool HasGeneratedMachineCode(const ExecutionRegionEvent &event) {
	return EventExecutionBody(event) == "generated-machine-code";
}

static bool HasCompiledRegionBody(const ExecutionRegionEvent &event) {
	return EventExecutionBody(event) == "generated-machine-code";
}

static void RequireNativeFusedRegion(const ExecutionRegionEvent &event) {
	REQUIRE(EventExecutionMode(event) == "native");
	REQUIRE(EventRegionExecutionForm(event) == "fused");
	REQUIRE(HasCompiledRegionBody(event));
	REQUIRE(event.code_size > 0);
}

static void RequireFusedGeneratedRegion(const ExecutionRegionEvent &event) {
	REQUIRE(EventRegionExecutionForm(event) == "fused");
	REQUIRE(HasGeneratedMachineCode(event));
	REQUIRE(event.code_size > 0);
}

static void RequireCompiledFusedRegion(const ExecutionRegionEvent &event) {
	REQUIRE(IsCompiledExecutionMode(EventExecutionMode(event)));
	REQUIRE(EventRegionExecutionForm(event) == "fused");
	REQUIRE(EventExecutionMode(event) == "native");
	REQUIRE(HasCompiledRegionBody(event));
	REQUIRE(event.code_size > 0);
}

static void RequireCompiledGeneratedRegion(const ExecutionRegionEvent &event) {
	REQUIRE(IsCompiledExecutionMode(EventExecutionMode(event)));
	REQUIRE(EventRegionExecutionForm(event) == "fused");
	REQUIRE(EventExecutionMode(event) == "native");
	REQUIRE(EventExecutionBody(event) == "generated-machine-code");
	REQUIRE(event.code_size > 0);
	REQUIRE(StringUtil::Contains(event.reason, "execution-body=generated-machine-code"));
	REQUIRE(StringUtil::Contains(event.reason, "kernel=generated-machine-code"));
}

static void RequireDuckDBScanFilteredSourceContract(const ExecutionRegionEvent &event) {
	REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	REQUIRE(event.selected_uses_scan_filters);
	REQUIRE(event.candidate_uses_scan_filters);
	REQUIRE(StringUtil::Contains(event.reason, "vectorized table scan filters"));
	REQUIRE(StringUtil::Contains(event.reason, "source-strategy=duckdb-scan-filtered-source-contract"));
	REQUIRE(StringUtil::Contains(event.reason, "uses-scan-filters=true"));
}

static void RequireUnsupportedContractOnlyRegion(const ExecutionRegionEvent &event) {
	REQUIRE(EventStatus(event) == "unsupported");
	REQUIRE(EventExecutionMode(event) == "unsupported");
	REQUIRE(EventRegionExecutionForm(event) == "none");
	REQUIRE(EventExecutionBody(event) == "none");
	REQUIRE(event.code_size == 0);
	REQUIRE(StringUtil::Contains(event.reason, "operator-contract-blocker:"));
	const bool has_native_body_gap =
	    StringUtil::Contains(event.reason, "SLJIT native region emits no generated machine code") ||
	    StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary");
	REQUIRE(has_native_body_gap);
	REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
}

template <class MATCH>
static bool HasJitEvent(ExecutionRegionManager &manager, MATCH match) {
	bool found = false;
	for (auto &event : manager.GetEvents()) {
		if (match(event)) {
			found = true;
		}
	}
	return found;
}

struct NoExtraJitEventCheck {
	void operator()(const ExecutionRegionEvent &) const {
	}
};

template <class MATCH, class CHECK>
static void RequireJitEvent(ExecutionRegionManager &manager, MATCH match, CHECK check) {
	bool found = false;
	for (auto &event : manager.GetEvents()) {
		if (!match(event)) {
			continue;
		}
		found = true;
		check(event);
	}
	REQUIRE(found);
}

template <class MATCH>
static void RequireJitEvent(ExecutionRegionManager &manager, MATCH match) {
	RequireJitEvent(manager, match, NoExtraJitEventCheck());
}

static void RequireNoExpressionJitEvents(ExecutionRegionManager &manager) {
	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventTarget(event) != "expression");
	}
}

template <class CHECK>
static void RequireNativeSljitIr(ExecutionRegionManager &manager, const string &ir_fragment, CHECK check) {
	bool found = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || EventExecutionMode(event) != "native" ||
		    !StringUtil::Contains(event.ir, ir_fragment)) {
			continue;
		}
		found = true;
		RequireNativeFusedRegion(event);
		check(event);
	}
	REQUIRE(found);
}

static void RequireNativeSljitIr(ExecutionRegionManager &manager, const string &ir_fragment) {
	RequireNativeSljitIr(manager, ir_fragment, NoExtraJitEventCheck());
}

static void RequireNoUnsupportedReason(ExecutionRegionManager &manager, const string &reason_fragment) {
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE((StringUtil::Contains(event.reason, reason_fragment) &&
		               StringUtil::Contains(event.reason, "function_or_operator_unsupported")));
	}
}

} // namespace
