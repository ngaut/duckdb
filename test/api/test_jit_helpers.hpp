#pragma once

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"

using namespace duckdb;

namespace {

static constexpr const char *JIT_HASH_JOIN_PROBE_READY_CONTRACT = "native_hash_join_probe_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_BUILD_READY_CONTRACT = "native_hash_join_build_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_READY = "native-hash-join-probe-executable=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON = "generated native hash join probe";
static constexpr const char *JIT_HASH_JOIN_BUILD_PROTOCOL_REASON =
    "hash_join_build_native<execution=primitive-protocol-build";

static string EventPhase(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventPhaseToString(event.phase_kind);
}

static string EventStatus(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventStatusToString(event.status_kind);
}

static string EventExecutionMode(const ExecutionRegionEvent &event) {
	return ExecutionRegionExecutionModeToString(event.execution_mode_kind);
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

static string EventGeneratedStageRuntimeBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageRuntimeBreakdown(event.generated_stage_runtime);
}

static string EventGeneratedStageCountBreakdown(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionStageCountBreakdown(event.generated_stage_runtime);
}

static idx_t TotalGeneratedStageExecutions(const ExecutionRegionEvent &event) {
	idx_t result = 0;
	for (auto &stage : event.generated_stage_runtime) {
		result += stage.count;
	}
	return result;
}

static bool ExecutionRegionStageNameHasOperatorKind(const string &name, const string &operator_kind) {
	auto separator = name.find(':');
	if (separator == string::npos) {
		return false;
	}
	const auto kind_start = separator + 1;
	auto kind_end = name.find('.', kind_start);
	if (kind_end == string::npos) {
		kind_end = name.size();
	}
	return name.substr(kind_start, kind_end - kind_start) == operator_kind;
}

static idx_t GeneratedOperatorStageEntryCount(const ExecutionRegionEvent &event, const string &operator_kind) {
	idx_t result = 0;
	for (auto &stage : event.generated_stage_runtime) {
		if (ExecutionRegionStageNameHasOperatorKind(stage.stage.name, operator_kind)) {
			result++;
		}
	}
	return result;
}

static idx_t GeneratedOperatorStageExecutionCount(const ExecutionRegionEvent &event, const string &operator_kind) {
	idx_t result = 0;
	for (auto &stage : event.generated_stage_runtime) {
		if (ExecutionRegionStageNameHasOperatorKind(stage.stage.name, operator_kind)) {
			result += stage.count;
		}
	}
	return result;
}

static void RequireNativeGeneratedRuntimeWork(const ExecutionRegionEvent &event) {
	REQUIRE(EventPhase(event) == "runtime");
	REQUIRE(EventStatus(event) == "executed");
	REQUIRE(EventExecutionMode(event) == "native");
	REQUIRE(event.generated_body_runtime_time_us >= 0);
	REQUIRE(!event.generated_stage_runtime.empty());
	REQUIRE(TotalGeneratedStageExecutions(event) > 0);
}

static string EventJitRuntimePathCounts(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_path_counts);
}

static string EventJitRuntimeProofCounts(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_proof_counts);
}

static string EventJitRuntimeDelegationCounts(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_delegation_counts);
}

static bool StageNameHasPrefix(const vector<ExecutionRegionRecordedStageRuntime> &stages, const string &prefix) {
	for (auto &stage : stages) {
		if (StringUtil::StartsWith(stage.stage.name, prefix)) {
			return true;
		}
	}
	return false;
}

static bool StageNameContains(const vector<ExecutionRegionRecordedStageRuntime> &stages, const string &needle) {
	for (auto &stage : stages) {
		if (StringUtil::Contains(stage.stage.name, needle)) {
			return true;
		}
	}
	return false;
}

static bool CounterNameHasPrefix(const vector<ExecutionRegionRecordedCounter> &counters, const string &prefix) {
	for (auto &counter : counters) {
		if (StringUtil::StartsWith(counter.counter.name, prefix)) {
			return true;
		}
	}
	return false;
}

static bool CounterNameHasComponent(const string &name, const string &component) {
	if (name == component) {
		return true;
	}
	idx_t component_start = 0;
	for (idx_t idx = 0; idx <= name.size(); idx++) {
		if (idx != name.size() && name[idx] != '.' && name[idx] != ':') {
			continue;
		}
		if (idx > component_start && name.substr(component_start, idx - component_start) == component) {
			return true;
		}
		component_start = idx + 1;
	}
	return false;
}

static bool CounterNameHasComponent(const vector<ExecutionRegionRecordedCounter> &counters,
                                    const string &component) {
	for (auto &counter : counters) {
		if (CounterNameHasComponent(counter.counter.name, component)) {
			return true;
		}
	}
	return false;
}

static bool HasJitRuntimeProof(const ExecutionRegionEvent &event, const string &proof) {
	return CounterNameHasComponent(event.jit_runtime.runtime_proof_counts, proof);
}

static bool HasJitRuntimeProof(const ExecutionRegionEvent &event, ExecutionRegionJitRuntimeProof proof) {
	return HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProofName(proof));
}

static bool HasGeneratedHashJoinProbeStage(const ExecutionRegionEvent &event) {
	return GeneratedOperatorStageEntryCount(event, "hash_join_probe") > 0;
}

static bool HasJitRuntimePathPrefix(const ExecutionRegionEvent &event, const string &prefix) {
	return CounterNameHasPrefix(event.jit_runtime.runtime_path_counts, prefix);
}

static bool HasJitAggregateUpdatePath(const ExecutionRegionEvent &event) {
	return HasJitRuntimePathPrefix(event, "aggregate_update.");
}

static bool HasJitFilterRuntimePath(const ExecutionRegionEvent &event) {
	return HasJitRuntimePathPrefix(event, "filter.");
}

static bool HasJitDelimJoinSinkRuntimePath(const ExecutionRegionEvent &event) {
	return HasJitRuntimePathPrefix(event, "delim_join_sink.");
}

static bool HasGeneratedAggregateUpdateStage(const ExecutionRegionEvent &event) {
	return GeneratedOperatorStageEntryCount(event, "aggregate_update") > 0;
}

static bool HasGeneratedDelimJoinSinkStage(const ExecutionRegionEvent &event) {
	return StageNameContains(event.generated_stage_runtime, "delim_join_sink.");
}

static bool HasHashJoinProbeRuntimePath(const ExecutionRegionEvent &event) {
	return HasGeneratedHashJoinProbeStage(event) && HasJitRuntimePathPrefix(event, "hash_join_probe.");
}

static void RequireHashProbeAggregateUpdateRuntimeOwnership(const ExecutionRegionEvent &event) {
	REQUIRE(HasHashJoinProbeRuntimePath(event));
	REQUIRE(HasJitAggregateUpdatePath(event));
	REQUIRE(event.jit_runtime.runtime_delegation_counts.empty());
}

template <class COUNTER>
static idx_t TotalExecutionRegionCounterCount(const vector<COUNTER> &counters) {
	idx_t result = 0;
	for (auto &counter : counters) {
		result += counter.count;
	}
	return result;
}

static bool EventKernelIdMatches(const ExecutionRegionEvent &event, const vector<idx_t> &kernel_ids) {
	for (auto kernel_id : kernel_ids) {
		if (event.kernel_id == kernel_id) {
			return true;
		}
	}
	return false;
}

static vector<idx_t> RequireSljitMaterializationElisionCboKernelIds(ExecutionRegionManager &manager,
                                                                    ExecutionRegionSinkKind sink_kind) {
	vector<idx_t> kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || EventStatus(event) != "compiled" || !event.has_candidate ||
		    event.candidate_traits.sink_kind != sink_kind || event.runner_cost.materialization_elision_count == 0) {
			continue;
		}
		kernel_ids.push_back(event.kernel_id);
		REQUIRE(event.runner_cost.selected_accelerated_runner);
	}
	REQUIRE(!kernel_ids.empty());
	return kernel_ids;
}

static vector<idx_t> RequireSljitMaterializationElisionCboKernelIds(ExecutionRegionManager &manager) {
	vector<idx_t> kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || EventStatus(event) != "compiled" ||
		    event.runner_cost.materialization_elision_count == 0) {
			continue;
		}
		kernel_ids.push_back(event.kernel_id);
		REQUIRE(event.runner_cost.selected_accelerated_runner);
	}
	REQUIRE(!kernel_ids.empty());
	return kernel_ids;
}

static void RequireMaterializationElisionRuntimeProof(const ExecutionRegionEvent &event) {
	INFO(EventJitRuntimeDelegationCounts(event));
	INFO(EventJitRuntimePathCounts(event));
	INFO(EventJitRuntimeProofCounts(event));
	RequireNativeGeneratedRuntimeWork(event);
	REQUIRE(event.jit_runtime.runtime_delegation_counts.empty());
	REQUIRE(!event.jit_runtime.runtime_path_counts.empty());
	REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK));
	REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION));
}

static idx_t RequireMaterializationElisionRuntimeProof(ExecutionRegionManager &manager,
                                                       const vector<idx_t> &kernel_ids) {
	idx_t runtime_count = 0;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" ||
		    EventExecutionMode(event) != "native" || !EventKernelIdMatches(event, kernel_ids)) {
			continue;
		}
		runtime_count++;
		RequireMaterializationElisionRuntimeProof(event);
	}
	REQUIRE(runtime_count > 0);
	return runtime_count;
}

static idx_t RequireAllSljitMaterializationElisionRuntimeProof(ExecutionRegionManager &manager) {
	auto kernel_ids = RequireSljitMaterializationElisionCboKernelIds(manager);
	return RequireMaterializationElisionRuntimeProof(manager, kernel_ids);
}

static bool IsGeneratedAggregateUpdateRuntime(const ExecutionRegionEvent &event) {
	return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
	       EventExecutionMode(event) == "native" && event.backend_name == "sljit" && HasJitAggregateUpdatePath(event);
}

static void RequireGeneratedAggregateUpdateRuntimeOwnership(const ExecutionRegionEvent &event) {
	RequireNativeGeneratedRuntimeWork(event);
	REQUIRE(HasJitAggregateUpdatePath(event));
	REQUIRE(HasGeneratedAggregateUpdateStage(event));
	REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
}

struct JitTestDatabase {
	JitTestDatabase() : con(db), context(*con.context), manager(ExecutionRegionManager::Get(context)) {
	}

	DuckDB db;
	Connection con;
	ClientContext &context;
	ExecutionRegionManager &manager;
};

static bool ContainsTypedIrNode(const string &ir, const string &node_kind, const string &logical_type,
                                const string &physical_type) {
	return StringUtil::Contains(ir, node_kind + "<logical=" + logical_type + ",physical=" + physical_type);
}

static void LoadSljit(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
}

static void ConfigureSljitSettings(Connection &con, const string &policy = "auto", bool verify = false,
                                   bool dump_ir = false, bool trace_runtime = false, idx_t event_log_size = 10000) {
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

static void ConfigureSljit(Connection &con, const string &policy = "auto", bool verify = false, bool dump_ir = false,
                           bool trace_runtime = false, idx_t event_log_size = 10000) {
	LoadSljit(con);
	ConfigureSljitSettings(con, policy, verify, dump_ir, trace_runtime, event_log_size);
}

static void ConfigureJitCoverageCbo(Connection &con) {
	// Coverage tests must reach generated code paths even when production CBO would reject stateful protocol glue.
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_source_contract_scan_filter_penalty=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
}

static void ConfigureJitDecisionTrace(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
}

static void ConfigureSljitForCoverageSettings(Connection &con, bool verify = false, bool dump_ir = false,
                                              bool trace_runtime = false, idx_t event_log_size = 10000) {
	ConfigureSljitSettings(con, "auto", verify, dump_ir, trace_runtime, event_log_size);
	ConfigureJitCoverageCbo(con);
}

static void ConfigureSljitForCoverage(Connection &con, bool verify = false, bool dump_ir = false,
                                      bool trace_runtime = false, idx_t event_log_size = 10000) {
	LoadSljit(con);
	ConfigureSljitForCoverageSettings(con, verify, dump_ir, trace_runtime, event_log_size);
}

static PhysicalRunnerCostParameters ZeroStartupRunnerCostParameters() {
	PhysicalRunnerCostParameters parameters;
	parameters.startup_base_cost = 0;
	parameters.startup_margin_basis_points = 0;
	return parameters;
}

static void ClearJitTrace(ExecutionRegionManager &manager, bool counters = false) {
	manager.ClearEvents();
	if (counters) {
		manager.ClearCounters();
	}
}

static bool IsSljitRegionEvent(const ExecutionRegionEvent &event) {
	return event.backend_name == "sljit";
}

static bool IsCompiledSljitRegionEvent(const ExecutionRegionEvent &event) {
	return IsSljitRegionEvent(event) && EventStatus(event) == "compiled";
}

static bool IsVectorizedCboSkipEvent(const ExecutionRegionEvent &event) {
	return event.blocker == EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED && event.runner_cost.present;
}

static void RequireGeneratedMachineCodeRegion(const ExecutionRegionEvent &event) {
	REQUIRE(EventExecutionMode(event) == "native");
	REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
}

static void RequireVectorizedCboSkip(const ExecutionRegionEvent &event) {
	REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	REQUIRE(event.runner_cost.present);
	REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
	REQUIRE(event.blocker == EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED);
}

static void RequirePipelineCboOnlyTiming(const ExecutionRegionEvent &event) {
	REQUIRE(event.stage_timings.pipeline_cbo_time_us >= 0);
	REQUIRE(event.stage_timings.graph_build_time_us == 0);
	REQUIRE(event.stage_timings.candidate_cbo_time_us == 0);
	REQUIRE(event.stage_timings.ir_lowering_time_us == 0);
	REQUIRE(event.stage_timings.backend_analysis_time_us == 0);
}

static void RequireDuckDBScanFilteredSourceContract(const ExecutionRegionEvent &event) {
	REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	REQUIRE(event.selected_uses_scan_filters);
	REQUIRE(StringUtil::Contains(event.reason, "vectorized table scan filters"));
	REQUIRE(StringUtil::Contains(event.reason, "source-strategy=duckdb-scan-filtered-source-contract"));
	REQUIRE(StringUtil::Contains(event.reason, "uses-scan-filters=true"));
}

static void RequireGeneratedSourceFilterContract(const ExecutionRegionEvent &event) {
	REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	REQUIRE_FALSE(event.selected_uses_scan_filters);
	REQUIRE(StringUtil::Contains(event.reason, "generated table scan filters"));
	REQUIRE(StringUtil::Contains(event.reason, "source-strategy=generated-source-filter"));
	REQUIRE(StringUtil::Contains(event.reason, "source_contract_input_layout=true"));
	REQUIRE_FALSE(StringUtil::Contains(event.reason, "uses-scan-filters=true"));
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

template <class CHECK>
static void RequireNativeSljitIr(ExecutionRegionManager &manager, const string &ir_fragment, CHECK check) {
	bool found = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || EventExecutionMode(event) != "native" ||
		    !StringUtil::Contains(event.ir, ir_fragment)) {
			continue;
		}
		found = true;
		RequireGeneratedMachineCodeRegion(event);
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
