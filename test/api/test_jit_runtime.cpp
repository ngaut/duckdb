#include "test_jit_helpers.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"
#include "duckdb/execution/execution_region_plan.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include "sljit_native_runtime.hpp"

#include <atomic>
#include <cstdint>
#include <thread>

using namespace duckdb;

static bool ContainsSourceContractStageBreakdown(const string &value) {
	return StringUtil::Contains(value, "source_contract.table_scan.") ||
	       StringUtil::Contains(value, "source_contract.column_data_scan.") ||
	       StringUtil::Contains(value, "source_contract.hash_aggregate_state_scan.") ||
	       StringUtil::Contains(value, "source_contract.perfect_hash_aggregate_state_scan.") ||
	       StringUtil::Contains(value, "source_contract.ungrouped_aggregate_state_scan.") ||
	       StringUtil::Contains(value, "source_contract.hash_join_state_scan.") ||
	       StringUtil::Contains(value, "source_contract.order_state_scan.") ||
	       StringUtil::Contains(value, "source_contract.topn_state_scan.");
}

static bool HasSourceContractStageBreakdown(const ExecutionRegionManager &manager) {
	for (auto &counter : manager.GetCounters()) {
		auto source_breakdown = RenderExecutionRegionStageRuntimeBreakdown(counter.source_stage_runtime);
		if (ContainsSourceContractStageBreakdown(source_breakdown)) {
			return true;
		}
	}
	return false;
}

static string ReadLocalTextFile(const string &path) {
	auto fs = FileSystem::CreateLocal();
	auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = fs->GetFileSize(*handle);
	REQUIRE(size >= 0);
	string result;
	result.resize(NumericCast<idx_t>(size));
	if (size > 0) {
		fs->Read(*handle, &result[0], size);
	}
	return result;
}

class UnitFullPipelineKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	bool HasExecutableBody() const override {
		return true;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

private:
	string backend_name = "unit";
};

TEST_CASE("Execution region plan selects executable ABI without diagnostic candidate payload", "[api][jit]") {
	ExecutionRegionPlan plan;
	auto kernel = make_uniq<UnitFullPipelineKernel>();
	kernel->SetExecutionABI(ExecutionRegionABI::FULL_PIPELINE);
	REQUIRE_FALSE(kernel->HasTracePipeline());
	plan.kernels.push_back(std::move(kernel));

	REQUIRE(plan.HasExecutableFullPipeline());
	auto selected = plan.GetExecutableFullPipelineKernel();
	REQUIRE(selected);
	REQUIRE(selected->ExecutionABI() == ExecutionRegionABI::FULL_PIPELINE);
	REQUIRE_FALSE(selected->HasTracePipeline());
}

TEST_CASE("Execution region stage runtimes merge by typed stage id", "[api][jit]") {
	ExecutionRegionEventLog log;

	for (idx_t event_idx = 0; event_idx < 4; event_idx++) {
		ExecutionRegionEvent event;
		event.phase_kind = ExecutionRegionEventPhase::RUNTIME;
		event.backend_name = "sljit";
		event.target_kind = ExecutionRegionCompileTarget::REGION;
		event.status_kind = ExecutionRegionEventStatus::EXECUTED;
		event.execution_mode_kind = ExecutionRegionExecutionMode::NATIVE;
		event.region_execution_form_kind = ExecutionRegionForm::FUSED;
		event.execution_body_kind = ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE;
		event.requested_policy_kind = ExecutionRegionEventPolicy::RUNTIME;
		AddExecutionRegionStageRuntime(event.source_stage_runtime, "source_contract.table_scan.storage_scan_projected",
		                               7);
		AddExecutionRegionStageRuntime(event.generated_stage_runtime, "op0:hash_join_build.append", 11);
		log.Record(0, std::move(event));
	}

	auto counters = log.GetCounters();
	REQUIRE(counters.size() == 1);
	auto &counter = counters[0];
	REQUIRE(counter.count == 4);
	REQUIRE(counter.source_stage_runtime.size() == 1);
	REQUIRE(counter.source_stage_runtime[0].runtime_time_us == 28);
	REQUIRE(counter.source_stage_runtime[0].count == 4);
	REQUIRE(RenderExecutionRegionStageCountBreakdown(counter.source_stage_runtime) ==
	        "source_contract.table_scan.storage_scan_projected=4");
	REQUIRE(counter.generated_stage_runtime.size() == 1);
	REQUIRE(counter.generated_stage_runtime[0].runtime_time_us == 44);
	REQUIRE(counter.generated_stage_runtime[0].count == 4);
	REQUIRE(RenderExecutionRegionStageCountBreakdown(counter.generated_stage_runtime) ==
	        "op0:hash_join_build.append=4");
}

TEST_CASE("Execution region event retention uses circular buffers", "[api][jit]") {
	ExecutionRegionEventLog log;

	auto make_compile_event = [](idx_t kernel_id) {
		ExecutionRegionEvent event;
		event.phase_kind = ExecutionRegionEventPhase::COMPILE;
		event.backend_name = "sljit";
		event.target_kind = ExecutionRegionCompileTarget::REGION;
		event.status_kind = ExecutionRegionEventStatus::COMPILED;
		event.execution_mode_kind = ExecutionRegionExecutionMode::NATIVE;
		event.region_execution_form_kind = ExecutionRegionForm::FUSED;
		event.execution_body_kind = ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE;
		event.requested_policy_kind = ExecutionRegionEventPolicy::FORCE;
		event.kernel_id = kernel_id;
		event.reason = "compiled";
		return event;
	};
	auto make_runtime_event = [](idx_t kernel_id, bool has_compile_identity) {
		ExecutionRegionEvent event;
		event.phase_kind = ExecutionRegionEventPhase::RUNTIME;
		event.backend_name = "sljit";
		event.target_kind = ExecutionRegionCompileTarget::REGION;
		event.status_kind = ExecutionRegionEventStatus::EXECUTED;
		event.execution_mode_kind = ExecutionRegionExecutionMode::NATIVE;
		event.region_execution_form_kind = ExecutionRegionForm::FUSED;
		event.execution_body_kind = ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE;
		event.requested_policy_kind = ExecutionRegionEventPolicy::RUNTIME;
		event.kernel_id = kernel_id;
		event.reason = "runtime";
		event.invocation_count = 1;
		event.input_rows = 5;
		event.runtime_result = "finished";
		if (has_compile_identity) {
			event.kernel_compile_reason = "compiled";
		}
		return event;
	};

	auto first_id = log.Record(2, make_compile_event(10));
	auto second_id = log.Record(2, make_compile_event(20));
	auto third_id = log.Record(2, make_compile_event(30));
	REQUIRE(first_id + 1 == second_id);
	REQUIRE(second_id + 1 == third_id);

	auto events = log.GetEvents();
	REQUIRE(events.size() == 2);
	REQUIRE(events[0].event_id == second_id);
	REQUIRE(events[1].event_id == third_id);

	auto counters = log.GetCounters();
	REQUIRE(counters.size() == 1);
	REQUIRE(counters[0].count == 3);
	REQUIRE(counters[0].status_kind == ExecutionRegionEventStatus::COMPILED);

	auto runtime_id = log.Record(2, make_runtime_event(10, false));
	events = log.GetEvents();
	REQUIRE(events.size() == 2);
	REQUIRE(events[0].event_id == third_id);
	REQUIRE(events[1].event_id == runtime_id);
	REQUIRE(events[1].kernel_id == 10);
	REQUIRE(events[1].invocation_count == 1);
	REQUIRE(events[1].input_rows == 5);

	log.Record(2, make_runtime_event(20, false));
	counters = log.GetCounters();
	REQUIRE(counters.size() == 2);
	bool found_runtime_counter = false;
	for (auto &counter : counters) {
		if (counter.status_kind == ExecutionRegionEventStatus::EXECUTED) {
			found_runtime_counter = true;
			REQUIRE(counter.count == 2);
			REQUIRE(counter.invocation_count == 2);
			REQUIRE(counter.input_rows == 10);
		}
	}
	REQUIRE(found_runtime_counter);

	log.ApplyRetentionLimit(1);
	events = log.GetEvents();
	REQUIRE(events.size() == 1);
}

TEST_CASE("SLJIT predicate source preparation uses referenced slots only", "[api][jit]") {
	vector<LogicalType> types;
	for (idx_t column_idx = 0; column_idx < 8; column_idx++) {
		types.push_back(LogicalType::BIGINT);
	}
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), types);
	input.SetChildCardinality(5);
	for (idx_t column_idx = 0; column_idx < input.ColumnCount(); column_idx++) {
		auto data = FlatVector::GetDataMutable<int64_t>(input.data[column_idx]);
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			data[row_idx] = NumericCast<int64_t>(column_idx * 100 + row_idx);
		}
	}

	SljitNativePredicateSourceAdapter adapter;
	adapter.Prepare(&input, {6, 2});

	REQUIRE(adapter.formats.size() == 2);
	REQUIRE(adapter.source_data.size() == 2);
	REQUIRE(adapter.source_sel.size() == 2);
	REQUIRE(adapter.source_validity.size() == 2);
	REQUIRE(adapter.source_data[0] != nullptr);
	REQUIRE(adapter.source_data[1] != nullptr);
	auto first_source = reinterpret_cast<const int64_t *>(adapter.source_data[0]);
	auto second_source = reinterpret_cast<const int64_t *>(adapter.source_data[1]);
	REQUIRE(first_source[0] == 600);
	REQUIRE(second_source[0] == 200);
}

TEST_CASE("Execution region events are bounded and counters are cumulative", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=3"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_event_bound_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	auto counter_count_before = TotalExecutionRegionCounterCount(manager.GetCounters());
	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));

	auto events = manager.GetEvents();
	REQUIRE(events.size() == 3);
	REQUIRE(events[0].event_id + 1 == events[1].event_id);
	REQUIRE(events[1].event_id + 1 == events[2].event_id);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) > counter_count_before);

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=2"));
	events = manager.GetEvents();
	REQUIRE(!events.empty());
	REQUIRE(events.size() <= 2);
	if (events.size() == 2) {
		REQUIRE(events[0].event_id + 1 == events[1].event_id);
	}

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=3"));

	auto last_event_id = events.back().event_id;
	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	events = manager.GetEvents();
	REQUIRE(!events.empty());
	REQUIRE(events.front().event_id > last_event_id);

	auto counter_count_before_sql_clear = TotalExecutionRegionCounterCount(manager.GetCounters());
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_events()"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count_before_sql_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_counters()"));
	REQUIRE(manager.GetCounters().empty());
	auto counter_count_after_explicit_clear = TotalExecutionRegionCounterCount(manager.GetCounters());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) > counter_count_after_explicit_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	auto counters = manager.GetCounters();
	REQUIRE(TotalExecutionRegionCounterCount(counters) > counter_count_after_explicit_clear);
	bool found_auto_skip = false;
	for (auto &counter : counters) {
		if (counter.backend_name != "sljit" || counter.target_kind != ExecutionRegionCompileTarget::REGION ||
		    counter.status_kind != ExecutionRegionEventStatus::SKIPPED ||
		    counter.execution_mode_kind != ExecutionRegionExecutionMode::UNSUPPORTED ||
		    counter.requested_policy_kind != ExecutionRegionEventPolicy::AUTO) {
			continue;
		}
		found_auto_skip = true;
		REQUIRE(counter.backend_analysis_time_us >= 0);
		REQUIRE(counter.codegen_time_us == 0);
	}
	REQUIRE(found_auto_skip);
}

TEST_CASE("Production auto records planner cost counters without diagnostics", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_telemetry_cold_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_telemetry_cold_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	auto production_counters = manager.GetCounters();
	auto counter_count_after_production_auto = TotalExecutionRegionCounterCount(production_counters);
	REQUIRE(counter_count_after_production_auto > 0);
	bool found_auto_runner_cost_decision = false;
	for (auto &counter : production_counters) {
		if (counter.backend_name != "sljit" || counter.target_kind != ExecutionRegionCompileTarget::REGION ||
		    counter.status_kind != ExecutionRegionEventStatus::SKIPPED ||
		    counter.execution_mode_kind != ExecutionRegionExecutionMode::UNSUPPORTED ||
		    counter.requested_policy_kind != ExecutionRegionEventPolicy::AUTO ||
		    counter.blocker != "duckdb_selected_vectorized") {
			continue;
		}
		found_auto_runner_cost_decision = true;
		REQUIRE(counter.has_runner_cost);
		REQUIRE(counter.runner_cost_startup_cost > 0);
		REQUIRE(counter.runner_cost_accelerated_runner_benefit >= 0);
		REQUIRE(counter.runner_cost_required_benefit > 0);
		REQUIRE(counter.runner_cost_selected_accelerated_runner_count == 0);
		REQUIRE(counter.decision_time_us >= 0);
		REQUIRE(counter.backend_analysis_time_us >= 0);
		REQUIRE(counter.codegen_time_us == 0);
	}
	REQUIRE(found_auto_runner_cost_decision);

	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_telemetry_cold_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) > counter_count_after_production_auto);
}

TEST_CASE("Execution region compact event retention omits diagnostic candidate payload", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=false"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_compact_event_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_compact_event_input WHERE i > 500"));

	bool found_compiled_region = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		found_compiled_region = true;
		REQUIRE(EventExecutionMode(event) == "native");
		REQUIRE(EventRegionExecutionForm(event) == "fused");
		REQUIRE_FALSE(event.has_candidate);
		REQUIRE(event.candidate_shape.empty());
		REQUIRE(event.candidate_signature.context.empty());
		REQUIRE(event.candidate_signature.shape.empty());
		REQUIRE(event.candidate_signature.contract_shape.empty());
		REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::NONE);
	}
	REQUIRE(found_compiled_region);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) > 0);
}

TEST_CASE("Generic profiling does not capture execution region diagnostics without JIT trace", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto fs = FileSystem::CreateLocal();
	auto profile_path = StringUtil::Format("/tmp/duckdb_jit_profile_gate_%llu.json",
	                                       static_cast<unsigned long long>(reinterpret_cast<uintptr_t>(&db)));

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_profile_gate_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	fs->TryRemoveFile(profile_path);
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling=json"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_mode=detailed"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output='" + profile_path + "'"));
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM jit_profile_gate_input WHERE i > 5000"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_profiling"));
	auto profile = ReadLocalTextFile(profile_path);
	REQUIRE_FALSE(StringUtil::Contains(profile, "\"execution_regions\""));

	fs->TryRemoveFile(profile_path);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling=json"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_mode=detailed"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output='" + profile_path + "'"));
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM jit_profile_gate_input WHERE i > 5000"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_profiling"));
	profile = ReadLocalTextFile(profile_path);
	REQUIRE(StringUtil::Contains(profile, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(profile, "\"events\""));
	fs->TryRemoveFile(profile_path);
}

TEST_CASE("SLJIT marks table-function full pipeline unsupported without source contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", true, true, true);
	ClearJitTrace(manager);
	auto result = con.Query("SELECT i + 1 AS j FROM range(10000) tbl(i) WHERE i > 5000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4999);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 5002);
	REQUIRE(result->GetValue(0, 4998).GetValue<int64_t>() == 10000);

	bool found_unsupported_full_pipeline = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			continue;
		}
		REQUIRE_FALSE(EventStatus(event) == "compiled");
		REQUIRE_FALSE(EventPhase(event) == "runtime");
		if (EventStatus(event) == "unsupported") {
			found_unsupported_full_pipeline = true;
			REQUIRE(EventExecutionMode(event) == "unsupported");
			REQUIRE(EventRegionExecutionForm(event) == "none");
			REQUIRE(event.code_size == 0);
			RequireCandidateStructure(event, 1, 1, ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK);
			REQUIRE(StringUtil::Contains(event.reason, "source-contract-blocker:requires-source-contract"));
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:boundary"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
			REQUIRE(StringUtil::Contains(event.reason, "append sink contract"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=result-collector-execution-sink"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
			REQUIRE(StringUtil::Contains(event.ir, "source_contract<status=blocked,"
			                                       "required_capability=table-function-source-contract"));
			REQUIRE(StringUtil::Contains(event.ir, "blocker=table-function-source-boundary"));
			REQUIRE(StringUtil::Contains(event.ir, "contract<abi=full_pipeline"));
		}
	}
	REQUIRE(found_unsupported_full_pipeline);
}

TEST_CASE("JIT full pipeline uses explicit append sink contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_result_collector AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitSettings(con, "force", true, true, true);

	ClearJitTrace(manager);
	auto result = con.Query("SELECT i + 1 AS j FROM jit_native_result_collector WHERE i > 500");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 499);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 502);
	REQUIRE(result->GetValue(0, 498).GetValue<int64_t>() == 1000);

	bool found_compiled_result_collector = false;
	bool found_runtime_result_collector = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native operator "
		                                                 "sink contract"));
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" && EventRegionExecutionForm(event) == "fused" &&
		    StringUtil::Contains(event.reason, "append sink contract") &&
		    StringUtil::Contains(event.reason, "sink_kind=result-collector-sink")) {
			REQUIRE(event.has_candidate);
			REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
			found_compiled_result_collector = true;
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:native"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=result-collector-execution-sink"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
			REQUIRE(StringUtil::Contains(event.ir, "append_sink("));
			REQUIRE(StringUtil::Contains(event.ir, "sink<kind=result-collector-sink"));
			REQUIRE(event.candidate_contract.OwnsSink());
			REQUIRE(event.candidate_contract.sink_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" && EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.output_rows == 499) {
			found_runtime_result_collector = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_result_collector);
	REQUIRE(found_runtime_result_collector);
}

TEST_CASE("JIT full pipeline uses append sink contract for CTE materialization", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_cte_materialization AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitSettings(con, "force", true, true, true);

	ClearJitTrace(manager);
	auto result = con.Query("WITH cte AS MATERIALIZED ("
	                        "SELECT i + 1 AS j FROM jit_native_cte_materialization WHERE i > 500"
	                        ") SELECT sum(j) FROM cte");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 374749);

	bool found_compiled_materialization = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			continue;
		}
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" && EventRegionExecutionForm(event) == "fused" &&
		    StringUtil::Contains(event.reason, "append sink contract") &&
		    StringUtil::Contains(event.reason, "sink_kind=materialization")) {
			found_compiled_materialization = true;
			REQUIRE(StringUtil::Contains(event.reason, "sink:CTE:native"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=materialization-append-sink"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
			REQUIRE(StringUtil::Contains(event.ir, "append_sink("));
			REQUIRE(StringUtil::Contains(event.ir, "sink<kind=materialization"));
			REQUIRE(event.candidate_contract.OwnsSink());
			REQUIRE(event.candidate_contract.sink_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
		}
	}
	REQUIRE(found_compiled_materialization);
}

TEST_CASE("JIT full pipeline uses ordered sink native contract when order keys generate code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_sort_sink AS "
	                          "SELECT (1000 - i)::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitSettings(con, "force", true, true, true);

	auto require_ordered_sink_contract = [&](const string &sql, const string &operator_name,
	                                         const string &expected_order_kind, idx_t expected_rows,
	                                         int64_t expected_first) {
		ClearJitTrace(manager);
		auto result = con.Query(sql);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == expected_rows);
		REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == expected_first);

		bool found_ordered_sink_contract = false;
		bool found_ordered_sink_runtime_breakdown = false;
		for (auto &event : manager.GetEvents()) {
			if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "order_sink.key_projection=") &&
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "order_sink.sink_update=")) {
				found_ordered_sink_runtime_breakdown = true;
			}
			if (!IsSljitRegionEvent(event) || !event.has_candidate ||
			    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
				continue;
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "whole operator sink"));
			if (StringUtil::Contains(event.reason, "ordered sink contract") &&
			    StringUtil::Contains(event.reason, "operator_kind=" + expected_order_kind) &&
			    StringUtil::Contains(event.reason, "sink:" + operator_name + ":native")) {
				found_ordered_sink_contract = true;
				RequireCompiledGeneratedRegion(event);
				REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:native"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=order-native-sink-update"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_blocker=none"));
				REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=order-native-sink-update"));
				REQUIRE_FALSE(
				    StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary;stage=order-sink"));
				const bool has_ordered_sink_trace = StringUtil::Contains(event.ir, "ordered_sink") ||
				                                    StringUtil::Contains(event.reason, "ordered_sink");
				REQUIRE(has_ordered_sink_trace);
				REQUIRE(StringUtil::Contains(event.ir, "sink<kind=sort"));
				REQUIRE(StringUtil::Contains(event.ir, "order_contract<operator_kind=" + expected_order_kind));
				REQUIRE(StringUtil::Contains(event.ir, "order_keys=[order_key0"));
				REQUIRE(StringUtil::Contains(event.ir, "expression_ready=true"));
				REQUIRE(StringUtil::Contains(event.ir, "expression_ir=(duckdb.expr typed-vector-ir"));
				REQUIRE(event.candidate_contract.OwnsSink());
				REQUIRE(event.candidate_contract.sink_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
			}
		}
		REQUIRE(found_ordered_sink_contract);
		REQUIRE(found_ordered_sink_runtime_breakdown);
	};

	require_ordered_sink_contract("SELECT i FROM jit_native_sort_sink ORDER BY i * 2", "ORDER_BY", "order-by", 1000, 1);
	require_ordered_sink_contract("SELECT i FROM jit_native_sort_sink ORDER BY i * 2 DESC LIMIT 5", "TOP_N", "top-n", 5,
	                              1000);
}

TEST_CASE("Execution region runtime events preserve kernel linkage", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", false, false, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_event_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_runtime_event_input WHERE i > 0)"));

	optional_idx fused_kernel_id;
	bool found_runtime_event = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit" || EventTarget(event) != "region" ||
		    !IsCompiledExecutionMode(EventExecutionMode(event)) || EventRegionExecutionForm(event) != "fused" ||
		    event.invocation_count == 0 || EventStatus(event) != "executed") {
			continue;
		}
		fused_kernel_id = event.kernel_id;
		found_runtime_event = true;
		REQUIRE_FALSE(event.has_candidate);
		REQUIRE(event.has_pipeline);
		REQUIRE(!event.candidate_shape.empty());
		REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::NONE);
		REQUIRE(!event.pipeline_shape.empty());
		REQUIRE(event.candidate_estimated_cardinality > 0);
		REQUIRE(event.input_rows + event.output_rows > 0);
		REQUIRE(event.generated_body_runtime_time_us >= 0);
		const bool expected_runtime_result =
		    event.runtime_result == "need_more_input" || event.runtime_result == "finished";
		REQUIRE(expected_runtime_result);
		REQUIRE(!event.kernel_compile_reason.empty());
		REQUIRE(event.kernel_code_size > 0);
	}
	REQUIRE(found_runtime_event);
	REQUIRE(fused_kernel_id.IsValid());

	bool found_compile_event = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "compile" && event.kernel_id == fused_kernel_id.GetIndex()) {
			found_compile_event = true;
		}
	}
	REQUIRE(found_compile_event);

	auto visible = con.Query("SELECT count(*) FROM duckdb_jit_events() WHERE phase='runtime' AND kernel_id = " +
	                         std::to_string(fused_kernel_id.GetIndex()));
	REQUIRE_NO_FAIL(*visible);
	REQUIRE(CHECK_COLUMN(visible, 0, {1}));

	auto aggregate_count_before_clear = TotalExecutionRegionCounterCount(manager.GetCounters());
	ClearJitTrace(manager);
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == aggregate_count_before_clear);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_runtime_event_input WHERE i > 0)"));
	REQUIRE(!manager.GetEvents().empty());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	auto hidden = con.Query("SELECT count(*) FROM duckdb_jit_events()");
	REQUIRE_NO_FAIL(*hidden);
	REQUIRE(CHECK_COLUMN(hidden, 0, {0}));
}

TEST_CASE("JIT dump IR and execution mode expose backend honesty", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", false, true);

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_ir = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventTarget(event) != "expression");
		if (EventStatus(event) == "compiled") {
			REQUIRE(EventExecutionMode(event) == "native");
			RequireCompiledGeneratedRegion(event);
		}
		if (event.backend_name == "sljit" && EventTarget(event) == "region" && !event.ir.empty() &&
		    StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir") && StringUtil::Contains(event.ir, ".add")) {
			found_ir = true;
			REQUIRE(ContainsTypedIrNode(event.ir, "binary", "BIGINT", "INT64"));
		}
	}
	REQUIRE(found_ir);

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT b, a FROM (VALUES (1::BIGINT, 10::BIGINT), "
	                          "(2::BIGINT, 20::BIGINT)) t(a, b)"));

	bool found_reference_projection_pruned = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "op0:PROJECTION:native:reference projection remap"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "SLJIT native region emits no generated machine code"));
		if (EventStatus(event) == "unsupported" && !event.has_candidate &&
		    StringUtil::Contains(event.reason, "core region lowering produced no candidates")) {
			REQUIRE(EventStatus(event) == "unsupported");
			REQUIRE(EventExecutionMode(event) == "unsupported");
			REQUIRE(EventExecutionBody(event) == "none");
			REQUIRE(event.code_size == 0);
			found_reference_projection_pruned = true;
		}
	}
	REQUIRE(found_reference_projection_pruned);
}

TEST_CASE("Execution region runtime trace records kernel execution facts", "[api][jit]") {
	bool found_region_runtime = false;
	bool found_runtime_counter = false;
	{
		DuckDB db;
		Connection con(db);
		auto &context = *con.context;
		auto &manager = ExecutionRegionManager::Get(context);

		LoadSljit(con);
		REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
		REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_trace_input AS "
		                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

		ClearJitTrace(manager);
		REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM "
		                          "(SELECT i + 1 AS j FROM jit_runtime_trace_input WHERE i > 0)"));
		for (auto &event : manager.GetEvents()) {
			REQUIRE(EventTarget(event) != "expression");
			if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" || EventTarget(event) != "region" ||
			    event.output_rows == 0) {
				continue;
			}
			found_region_runtime = true;
			REQUIRE(event.kernel_id > 0);
			REQUIRE(event.invocation_count == 1);
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows > 0);
			REQUIRE(event.runtime_time_us >= 0);
			REQUIRE_FALSE(event.has_candidate);
			REQUIRE(event.has_pipeline);
			REQUIRE(!event.pipeline_shape.empty());
			REQUIRE(event.generated_body_runtime_time_us >= 0);
			REQUIRE(event.generated_body_runtime_time_us + event.source_contract_runtime_time_us <=
			        event.runtime_time_us);
			const bool expected_runtime_result = event.runtime_result == "need_more_input" ||
			                                     event.runtime_result == "have_more_output" ||
			                                     event.runtime_result == "finished";
			REQUIRE(expected_runtime_result);
			REQUIRE(StringUtil::Contains(event.reason, "kernel executed"));
		}
		for (auto &counter : manager.GetCounters()) {
			if (counter.status_kind == ExecutionRegionEventStatus::EXECUTED &&
			    counter.requested_policy_kind == ExecutionRegionEventPolicy::RUNTIME && counter.invocation_count > 0 &&
			    counter.input_rows > 0 && counter.execution_mode_kind == ExecutionRegionExecutionMode::NATIVE) {
				found_runtime_counter = true;
			}
		}
	}
	REQUIRE(found_region_runtime);
	REQUIRE(found_runtime_counter);
}

TEST_CASE("EXPLAIN ANALYZE exposes compact execution region profile", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", false, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_analyze_input AS "
	                          "SELECT i::BIGINT AS i, (i + 1)::BIGINT AS g FROM range(10000) tbl(i)"));

	auto trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->GetValue(0, 0).ToString() == "false");

	ClearJitTrace(manager, true);
	auto result = con.Query("EXPLAIN ANALYZE "
	                        "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "JIT_EXECUTION_REGIONS"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "policy=force"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "selected_backend=sljit"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "compiled=1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runtime_regions=1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "generated_runtime_us="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runtime_dominant="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "source_runtime_pct="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "generated_runtime_pct="));
	REQUIRE(HasSourceContractStageBreakdown(manager));

	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_result\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_source_execution\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_uses_scan_filters\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"candidate_uses_scan_filters\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_enabled\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_policy\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_requested_backend\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_selected_backend\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"kernel_id\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"query_runtime_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"source_contract_output_rows\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"source_contract_invocation_count\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_runtime_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_unattributed_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"source_runtime_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_runtime_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_unattributed_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_dominant_component\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"sink_next_batch_runtime_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"sink_next_batch_invocation_count\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"estimated_cardinality\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"ir_lowering_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"backend_analysis_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runner_cost_profile\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runner_cost_accelerated_runner_benefit\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runner_cost_startup_cost\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runner_cost_selected_accelerated_runner\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"blocker\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "aggregate_update"));

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	ClearJitTrace(manager, true);
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(manager.GetEvents().empty());
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_runtime_time_us\""));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 0"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"disabled_decisions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"status\": \"disabled\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_policy\": \"off\""));
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "\"comparison_"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"decision_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"estimated_cardinality\": 10000"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_runner\": \"vectorized\""));

	trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->GetValue(0, 0).ToString() == "false");
}

TEST_CASE("EXPLAIN ANALYZE exposes grouped hash aggregate generated lookup blocker", "[api][jit]") {
	DuckDB db;
	Connection con(db);

	ConfigureSljit(con, "force", false, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_grouped_lookup AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(50000) tbl(i)"));

	auto result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                        "SELECT k, sum(v) FROM jit_explain_grouped_lookup GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 0"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_regions\": 0"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"status\": \"unsupported\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_mode\": \"unsupported\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash aggregate update requires generated hash lookup ownership"));
	REQUIRE(StringUtil::Contains(analyzed_plan,
	                             "native_hash_aggregate_lookup_blocker=hash-aggregate-generated-lookup-backend-"
	                             "lowering-missing"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "native_hash_aggregate_lookup_layout"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash_aggregate_lookup_mode=blocked"));
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "hash_aggregate_lookup=vectorized-address-contract"));
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "aggregate_update.resolve_grouped_state_addresses"));
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "aggregate_update.primitive_payload_update"));
}

TEST_CASE("EXPLAIN ANALYZE reports compact aggregate auto vectorized-selection facts", "[api][jit]") {
	DuckDB db;
	Connection con(db);

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=false"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_auto_aggregate_blocker AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b, (i + 2)::BIGINT AS c, "
	                          "(i + 3)::BIGINT AS d FROM range(100000) tbl(i)"));

	auto result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                        "SELECT sum(CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) + "
	                        "(((a - b + c) * 17) - ((a + d) * 19)) + "
	                        "(((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT)) "
	                        "FROM jit_explain_auto_aggregate_blocker");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_runner\": \"vectorized\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"estimated_cardinality\": 100000"));
}

TEST_CASE("Execution region runtime trace separates compiled coverage from executed kernels", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", false, false, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_coverage_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_runtime_coverage_input WHERE i > 0)"));

	idx_t compiled_region_kernel_id = 0;
	bool found_region_runtime = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventTarget(event) != "expression");
		if (EventPhase(event) == "compile" && event.backend_name == "sljit" && EventTarget(event) == "region" &&
		    EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" && EventRegionExecutionForm(event) == "fused" &&
		    compiled_region_kernel_id == 0) {
			compiled_region_kernel_id = event.kernel_id;
		}
		if (EventPhase(event) == "runtime" && EventTarget(event) == "region" && event.kernel_id == compiled_region_kernel_id &&
		    EventStatus(event) == "executed") {
			found_region_runtime = true;
			REQUIRE_FALSE(event.has_candidate);
			REQUIRE(event.has_pipeline);
			REQUIRE(!event.pipeline_shape.empty());
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows >= 0);
		}
	}
	REQUIRE(compiled_region_kernel_id > 0);
	REQUIRE(found_region_runtime);
}

TEST_CASE("Execution region introspection does not record telemetry events", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_introspection_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM jit_introspection_input WHERE i > 0"));
	auto event_count = manager.GetEvents().size();
	auto counter_count = TotalExecutionRegionCounterCount(manager.GetCounters());
	REQUIRE(event_count > 0);
	REQUIRE(counter_count > 0);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(selected_uses_scan_filters) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_uses_scan_filters) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(generated_stage_count_breakdown) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_shape) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_signature_ir) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(selected_runner) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_profile) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_accelerated_runner_benefit) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_startup_cost) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_selected_accelerated_runner) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(blocker) FROM duckdb_jit_events()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_profile) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_accelerated_runner_benefit) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(runner_cost_startup_cost) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(
	    con.Query("SELECT count(runner_cost_selected_accelerated_runner_count) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(blocker) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(generated_stage_count_breakdown) FROM duckdb_jit_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_backends()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);

	auto copy_path = TestCreatePath("jit_events_copy.csv");
	REQUIRE_NO_FAIL(
	    con.Query("COPY (SELECT * FROM duckdb_jit_events()) TO " + SQLString(copy_path) + " (HEADER, DELIMITER ',')"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
}

TEST_CASE("Execution region introspection does not suppress later statements in one SQL batch", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_batch_source AS "
	                          "SELECT i::BIGINT AS i, i::BIGINT AS j FROM range(10000) tbl(i)"));
	ConfigureSljitSettings(con, "force", false, false, true);

	ClearJitTrace(manager);
	auto result = con.Query("SELECT * FROM duckdb_jit_clear_events();"
	                        "SELECT * FROM duckdb_jit_clear_counters();"
	                        "SELECT v FROM (SELECT i + 1 AS v, j FROM jit_batch_source WHERE j > 2500) "
	                        "WHERE v < 2510");
	REQUIRE_NO_FAIL(*result);

	bool found_compiled_fused_region = false;
	bool found_runtime_fused_region = false;
	for (auto &event : manager.GetEvents()) {
		if (EventTarget(event) != "region") {
			continue;
		}
		if (EventStatus(event) == "compiled" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    StringUtil::Contains(event.reason, "vectorized table scan filters") &&
		    StringUtil::Contains(event.reason, "append sink contract")) {
			REQUIRE(event.has_candidate);
			found_compiled_fused_region = true;
			RequireCompiledFusedRegion(event);
			RequireDuckDBScanFilteredSourceContract(event);
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_runtime_fused_region = true;
			REQUIRE(EventRegionExecutionForm(event) == "fused");
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.source_contract_output_rows == event.input_rows);
		}
	}
	REQUIRE(found_compiled_fused_region);
	REQUIRE(found_runtime_fused_region);
}

TEST_CASE("JIT event IDs are unique under concurrent compilation", "[api][jit]") {
	DuckDB db;
	Connection setup(db);
	auto &manager = ExecutionRegionManager::Get(*setup.context);
	REQUIRE_NO_FAIL(setup.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(setup.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE jit_concurrent_event_input AS "
	                            "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));
	ClearJitTrace(manager);

	vector<std::thread> workers;
	std::atomic<bool> failed(false);
	for (idx_t worker_idx = 0; worker_idx < 8; worker_idx++) {
		workers.emplace_back([&db, &failed, worker_idx]() {
			Connection con(db);
			if (con.Query("SET jit_backend='sljit'")->HasError()) {
				failed = true;
				return;
			}
			if (con.Query("SET jit_policy='force'")->HasError()) {
				failed = true;
				return;
			}
			for (idx_t query_idx = 0; query_idx < 8; query_idx++) {
				if (con.Query("SELECT i + " + std::to_string(worker_idx + query_idx) +
				              " FROM jit_concurrent_event_input WHERE i > 0")
				        ->HasError()) {
					failed = true;
					return;
				}
			}
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	REQUIRE(!failed.load());

	auto events = manager.GetEvents();
	REQUIRE(!events.empty());
	vector<idx_t> event_ids;
	event_ids.reserve(events.size());
	for (auto &event : events) {
		event_ids.push_back(event.event_id);
	}
	std::sort(event_ids.begin(), event_ids.end());
	REQUIRE(std::adjacent_find(event_ids.begin(), event_ids.end()) == event_ids.end());
}
