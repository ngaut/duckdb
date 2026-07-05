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

static string EventJitRuntimePathCounts(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_path_counts);
}

static string EventJitMaterializationBoundaryCounts(const ExecutionRegionEvent &event) {
	return RenderExecutionRegionCounterBreakdown(event.jit_runtime.materialization_boundary_counts);
}

struct JitTestDatabase {
	JitTestDatabase() : con(db), context(*con.context), manager(ExecutionRegionManager::Get(context)) {
	}

	DuckDB db;
	Connection con;
	ClientContext &context;
	ExecutionRegionManager &manager;
};

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
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=4096"));
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

static bool IsStatefulSortProjectionGlueCandidate(const ExecutionRegionEvent &event) {
	return event.has_candidate && event.candidate_traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
	       event.candidate_traits.source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	       event.candidate_traits.sink_kind == ExecutionRegionSinkKind::SORT &&
	       event.candidate_traits.projection_count > 0 && event.candidate_traits.filter_count == 0 &&
	       event.candidate_traits.operator_count == 0;
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

static void RequireNativeContractProjectionGlueSkipped(const ExecutionRegionEvent &event) {
	REQUIRE(event.runner_cost.present);
	REQUIRE(event.runner_cost.full_pipeline);
	REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE);
	REQUIRE(event.runner_cost.native_protocol_class ==
	        PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL);
	REQUIRE(event.runner_cost.generated_expression_work == 0);
	REQUIRE(event.runner_cost.generated_stage_work == 0);
	REQUIRE(event.runner_cost.full_pipeline_work == 0);
	REQUIRE(event.runner_cost.stateful_protocol_penalty == 0);
	REQUIRE(event.runner_cost.saved_work_per_batch == 0);
	RequireVectorizedCboSkip(event);
}

static void RequireDuckDBScanFilteredSourceContract(const ExecutionRegionEvent &event) {
	REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	REQUIRE(event.selected_uses_scan_filters);
	REQUIRE(StringUtil::Contains(event.reason, "vectorized table scan filters"));
	REQUIRE(StringUtil::Contains(event.reason, "source-strategy=duckdb-scan-filtered-source-contract"));
	REQUIRE(StringUtil::Contains(event.reason, "uses-scan-filters=true"));
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
