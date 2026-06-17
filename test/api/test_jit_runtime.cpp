#include "test_jit_helpers.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

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
	auto decision_counter_count_before = TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters());
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
	auto decision_counter_count_before_sql_clear =
	    TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters());
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_events()"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count_before_sql_clear);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) ==
	        decision_counter_count_before_sql_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_counters()"));
	REQUIRE(manager.GetCounters().empty());
	REQUIRE(manager.GetDecisionCounters().empty());
	auto counter_count_after_explicit_clear = TotalExecutionRegionCounterCount(manager.GetCounters());
	auto decision_counter_count_after_explicit_clear =
	    TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) > counter_count_after_explicit_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	auto decision_counters = manager.GetDecisionCounters();
	REQUIRE(TotalExecutionRegionDecisionCounterCount(decision_counters) > decision_counter_count_after_explicit_clear);
	bool found_measured_admission_skip = false;
	for (auto &counter : decision_counters) {
		if (counter.backend_name != "sljit" || counter.target != "region" || counter.status != "skipped" ||
		    counter.execution_mode != "unsupported") {
			continue;
		}
		found_measured_admission_skip = true;
		REQUIRE_FALSE(counter.has_pipeline);
		REQUIRE(counter.pipeline_estimated_cardinality == 0);
		REQUIRE(counter.candidate_shape.empty());
		REQUIRE(StringUtil::Contains(counter.example_reason, "before graph lowering"));
		REQUIRE_FALSE(counter.admission_rule_present);
		REQUIRE(counter.admission_min_cardinality == 0);
		REQUIRE_FALSE(counter.has_admission_score);
		REQUIRE(counter.admission_proof.empty());
		REQUIRE(StringUtil::Contains(counter.example_reason, "backend has no measured auto admission policy"));
	}
	REQUIRE(found_measured_admission_skip);
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
		REQUIRE_FALSE(event.status == "compiled");
		REQUIRE_FALSE(event.phase == "runtime");
		if (event.status == "unsupported") {
			found_unsupported_full_pipeline = true;
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.code_size == 0);
			REQUIRE(event.candidate_shape == "filter-projection-sink");
			REQUIRE(StringUtil::Contains(event.reason, "source-contract-blocker:requires-source-contract"));
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:boundary"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
			REQUIRE(StringUtil::Contains(event.reason, "append sink protocol"));
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

TEST_CASE("JIT full pipeline uses explicit append sink protocol", "[api][jit]") {
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
		if (!IsSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native operator "
		                                                 "sink protocol"));
		if (event.status == "compiled" && event.execution_mode == "native" && event.region_execution_form == "fused" &&
		    StringUtil::Contains(event.reason, "append sink protocol") &&
		    StringUtil::Contains(event.reason, "sink_kind=result-collector-sink")) {
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
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.output_rows == 499) {
			found_runtime_result_collector = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_result_collector);
	REQUIRE(found_runtime_result_collector);
}

TEST_CASE("JIT full pipeline uses append sink protocol for CTE materialization", "[api][jit]") {
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
		if (event.status == "compiled" && event.execution_mode == "native" && event.region_execution_form == "fused" &&
		    StringUtil::Contains(event.reason, "append sink protocol") &&
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

TEST_CASE("JIT full pipeline uses ordered sink runtime protocol without whole operator fallback", "[api][jit]") {
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

		bool found_compiled_ordered_sink = false;
		for (auto &event : manager.GetEvents()) {
			if (!IsSljitRegionEvent(event) || !event.has_candidate ||
			    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
				continue;
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "whole operator sink"));
			if (event.status == "compiled" && event.region_execution_form == "fused" &&
			    StringUtil::Contains(event.reason, "ordered sink protocol") &&
			    StringUtil::Contains(event.reason, "operator_kind=" + expected_order_kind) &&
			    StringUtil::Contains(event.reason, "sink:" + operator_name + ":native")) {
				found_compiled_ordered_sink = true;
				REQUIRE(IsCompiledExecutionMode(event.execution_mode));
				REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:native"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=order-native-sink-update"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_blocker=none"));
				REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=order-native-sink-update"));
				REQUIRE(StringUtil::Contains(event.ir, "ordered_sink(keys="));
				REQUIRE(StringUtil::Contains(event.ir, "native:bigint-multiply-constant"));
				REQUIRE(StringUtil::Contains(event.ir, "sink<kind=sort"));
				REQUIRE(StringUtil::Contains(event.ir, "order_contract<operator_kind=" + expected_order_kind));
				REQUIRE(StringUtil::Contains(event.ir, "order_keys=[order_key0"));
				REQUIRE(StringUtil::Contains(event.ir, "expression_ready=true"));
				REQUIRE(StringUtil::Contains(event.ir, "expression_ir=(duckdb.expr typed-vector-ir"));
				REQUIRE(event.candidate_contract.OwnsSink());
				REQUIRE(event.candidate_contract.sink_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
			}
		}
		REQUIRE(found_compiled_ordered_sink);
	};

	require_ordered_sink_contract("SELECT i FROM jit_native_sort_sink ORDER BY i * 2", "ORDER_BY", "order-by", 1000, 1);
	require_ordered_sink_contract("SELECT i FROM jit_native_sort_sink ORDER BY i * 2 DESC LIMIT 5", "TOP_N", "top-n", 5,
	                              1000);
}

TEST_CASE("Execution region kernel counters preserve runtime linkage after event eviction", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force", false, false, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_kernel_counter_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_kernel_counter_input WHERE i > 0)"));

	optional_idx fused_kernel_id;
	for (auto &counter : manager.GetKernelCounters()) {
		if (counter.backend_name == "sljit" && counter.target == "region" &&
		    IsCompiledExecutionMode(counter.execution_mode) && counter.region_execution_form == "fused" &&
		    counter.invocation_count > 0 && counter.last_runtime_status == "executed") {
			fused_kernel_id = counter.kernel_id;
			REQUIRE(counter.has_candidate);
			REQUIRE(!counter.candidate_shape.empty());
			REQUIRE(counter.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
			REQUIRE(!counter.candidate_pipeline_shape.empty());
			REQUIRE(counter.candidate_estimated_cardinality > 0);
			REQUIRE(counter.input_rows + counter.output_rows > 0);
			REQUIRE(counter.generated_body_runtime_time_us >= 0);
			REQUIRE(counter.last_runtime_status == "executed");
			const bool expected_runtime_result =
			    counter.last_runtime_result == "need_more_input" || counter.last_runtime_result == "finished";
			REQUIRE(expected_runtime_result);
		}
	}
	REQUIRE(fused_kernel_id.IsValid());

	bool retained_compile_event = false;
	for (auto &event : manager.GetEvents()) {
		if (event.phase == "compile" && event.kernel_id == fused_kernel_id.GetIndex()) {
			retained_compile_event = true;
		}
	}
	REQUIRE(!retained_compile_event);

	auto visible = con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters() WHERE kernel_id = " +
	                         std::to_string(fused_kernel_id.GetIndex()));
	REQUIRE_NO_FAIL(*visible);
	REQUIRE(CHECK_COLUMN(visible, 0, {1}));

	auto aggregate_count_before_clear = TotalExecutionRegionCounterCount(manager.GetCounters());
	ClearJitTrace(manager);
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == aggregate_count_before_clear);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_kernel_counter_input WHERE i > 0)"));
	REQUIRE(!manager.GetKernelCounters().empty());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	auto hidden = con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters()");
	REQUIRE_NO_FAIL(*hidden);
	REQUIRE(CHECK_COLUMN(hidden, 0, {0}));
}

TEST_CASE("Execution region kernel counters can be recreated from runtime trace identity", "[api][jit]") {
	ExecutionRegionEventLog log;

	ExecutionRegionEvent compile_event;
	compile_event.phase = "compile";
	compile_event.backend_name = "unit";
	compile_event.target = "region";
	compile_event.status = "compiled";
	compile_event.execution_mode = "native";
	compile_event.region_execution_form = "fused";
	compile_event.policy_decision = "force";
	compile_event.reason = "compile-shape";
	compile_event.decision_time_us = 3;
	compile_event.compile_time_us = 11;
	compile_event.code_size = 17;
	auto kernel_id = log.Record(1, true, std::move(compile_event));
	REQUIRE(kernel_id > 0);
	REQUIRE(log.GetKernelCounters().size() == 1);
	auto aggregate_counters = log.GetCounters();
	REQUIRE(aggregate_counters.size() == 1);
	REQUIRE(aggregate_counters[0].decision_time_us == 3);
	REQUIRE(aggregate_counters[0].compile_time_us == 11);

	log.ClearEvents();
	REQUIRE(log.GetEvents().empty());
	REQUIRE(log.GetKernelCounters().empty());

	ExecutionRegionEvent runtime_event;
	runtime_event.phase = "runtime";
	runtime_event.backend_name = "unit";
	runtime_event.target = "region";
	runtime_event.status = "executed";
	runtime_event.execution_mode = "native";
	runtime_event.region_execution_form = "fused";
	runtime_event.policy_decision = "runtime";
	runtime_event.reason = "region kernel executed";
	runtime_event.kernel_id = kernel_id;
	runtime_event.kernel_compile_reason = "compile-shape";
	runtime_event.kernel_compile_time_us = 11;
	runtime_event.kernel_code_size = 17;
	runtime_event.has_candidate = true;
	runtime_event.candidate_id = 2;
	runtime_event.candidate_shape = "filter-projection";
	runtime_event.candidate_contract.abi = ExecutionRegionABI::FULL_PIPELINE;
	runtime_event.candidate_pipeline_shape =
	    "pipeline;source:source:TABLE_SCAN:source-contract;op0:filter:FILTER:none;op1:projection:PROJECTION:none";
	runtime_event.candidate_node_count = 4;
	runtime_event.candidate_start_operator_index = 0;
	runtime_event.candidate_end_operator_index = 2;
	runtime_event.candidate_estimated_cardinality = 64;
	runtime_event.input_rows = 8;
	runtime_event.output_rows = 4;
	runtime_event.invocation_count = 1;
	runtime_event.runtime_time_us = 2;
	runtime_event.runtime_result = "need_more_input";
	runtime_event.source_contract_output_rows = 3;
	runtime_event.source_contract_invocation_count = 1;
	runtime_event.source_contract_runtime_time_us = 1;
	runtime_event.generated_body_runtime_time_us = 0;
	log.Record(1, true, std::move(runtime_event));

	auto counters = log.GetKernelCounters();
	REQUIRE(counters.size() == 1);
	REQUIRE(counters[0].kernel_id == kernel_id);
	REQUIRE(counters[0].region_execution_form == "fused");
	REQUIRE(counters[0].compile_reason == "compile-shape");
	REQUIRE(counters[0].compile_time_us == 11);
	REQUIRE(counters[0].code_size == 17);
	REQUIRE(counters[0].has_candidate);
	REQUIRE(counters[0].candidate_id == 2);
	REQUIRE(counters[0].candidate_shape == "filter-projection");
	REQUIRE(counters[0].candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
	REQUIRE(counters[0].candidate_pipeline_shape ==
	        "pipeline;source:source:TABLE_SCAN:source-contract;op0:filter:FILTER:none;op1:projection:PROJECTION:none");
	REQUIRE(counters[0].candidate_node_count == 4);
	REQUIRE(counters[0].candidate_start_operator_index == 0);
	REQUIRE(counters[0].candidate_end_operator_index == 2);
	REQUIRE(counters[0].candidate_estimated_cardinality == 64);
	REQUIRE(counters[0].input_rows == 8);
	REQUIRE(counters[0].output_rows == 4);
	REQUIRE(counters[0].invocation_count == 1);
	REQUIRE(counters[0].runtime_time_us == 2);
	REQUIRE(counters[0].source_contract_output_rows == 3);
	REQUIRE(counters[0].source_contract_invocation_count == 1);
	REQUIRE(counters[0].source_contract_runtime_time_us == 1);
	REQUIRE(counters[0].generated_body_runtime_time_us == 0);
	REQUIRE(counters[0].last_runtime_result == "need_more_input");
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
		REQUIRE(event.target != "expression");
		if (event.status == "compiled") {
			REQUIRE(event.execution_mode == "native");
			if (event.code_size == 0) {
				RequireCompiledFusedOperatorProtocolRegion(event);
			} else {
				REQUIRE(StringUtil::Contains(event.reason, "execution-body=generated-machine-code"));
			}
		}
		if (event.backend_name == "sljit" && event.target == "region" && !event.ir.empty() &&
		    StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir") && StringUtil::Contains(event.ir, ".add")) {
			found_ir = true;
			REQUIRE(ContainsTypedIrNode(event.ir, "binary", "BIGINT", "INT64"));
		}
	}
	REQUIRE(found_ir);

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT b, a FROM (VALUES (1::BIGINT, 10::BIGINT), "
	                          "(2::BIGINT, 20::BIGINT)) t(a, b)"));

	bool found_reference_projection_protocol = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (StringUtil::Contains(event.reason, "op0:PROJECTION:native:reference projection remap")) {
			RequireCompiledFusedOperatorProtocolRegion(event);
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "pass-through"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
			REQUIRE(StringUtil::Contains(event.reason, "append sink protocol"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "native operator "
			                                                 "sink protocol"));
			found_reference_projection_protocol = true;
		}
	}
	REQUIRE(found_reference_projection_protocol);
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
			REQUIRE(event.target != "expression");
			if (event.phase != "runtime" || event.status != "executed" || event.target != "region" ||
			    event.output_rows == 0) {
				continue;
			}
			found_region_runtime = true;
			REQUIRE(event.kernel_id > 0);
			REQUIRE(event.invocation_count == 1);
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows > 0);
			REQUIRE(event.runtime_time_us >= 0);
			REQUIRE(event.has_candidate);
			REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
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
			if (counter.status == "executed" && counter.policy_decision == "runtime" && counter.invocation_count > 0 &&
			    counter.input_rows > 0 && counter.execution_mode == "native") {
				found_runtime_counter = true;
			}
		}
	}
	REQUIRE(found_region_runtime);
	REQUIRE(found_runtime_counter);
}

TEST_CASE("EXPLAIN ANALYZE renders execution region runtime profile", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "force");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_analyze_input AS "
	                          "SELECT i::BIGINT AS i, (i % 10)::INTEGER AS g FROM range(10000) tbl(i)"));

	auto trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->GetValue(0, 0).ToString() == "false");

	ClearJitTrace(manager, true);
	auto result = con.Query("EXPLAIN ANALYZE "
	                        "SELECT g, sum(i) FROM jit_explain_analyze_input GROUP BY g ORDER BY g");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "JIT_COMPILED_REGIONS"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "JIT_REGION #"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "features:"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "stage: op1:aggregate_update"));

	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT g, sum(i) FROM jit_explain_analyze_input GROUP BY g ORDER BY g");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"feature_shape\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "aggregate_update"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT g, sum(i) FROM jit_explain_analyze_input GROUP BY g ORDER BY g");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));

	trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->GetValue(0, 0).ToString() == "false");
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
		REQUIRE(event.target != "expression");
		if (event.phase == "compile" && event.backend_name == "sljit" && event.target == "region" &&
		    event.status == "compiled" && event.execution_mode == "native" && event.region_execution_form == "fused" &&
		    compiled_region_kernel_id == 0) {
			compiled_region_kernel_id = event.kernel_id;
		}
		if (event.phase == "runtime" && event.target == "region" && event.kernel_id == compiled_region_kernel_id &&
		    event.status == "executed") {
			found_region_runtime = true;
			REQUIRE(event.has_candidate);
			REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
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
	auto decision_counter_count = TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters());
	auto kernel_counter_count = manager.GetKernelCounters().size();
	REQUIRE(event_count > 0);
	REQUIRE(counter_count > 0);
	REQUIRE(decision_counter_count > 0);
	REQUIRE(kernel_counter_count > 0);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_shape) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_signature_ir) FROM duckdb_jit_events()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_decision_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_decision_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_decision_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_shape) FROM duckdb_jit_decision_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(execution_body) FROM duckdb_jit_kernel_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_kernel_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_shape) FROM duckdb_jit_kernel_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_backends()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	auto copy_path = TestCreatePath("jit_decision_counters_copy.csv");
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT * FROM duckdb_jit_decision_counters()) TO " + SQLString(copy_path) +
	                          " (HEADER, DELIMITER ',')"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalExecutionRegionCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalExecutionRegionDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);
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
		if (event.target != "region" || !event.has_candidate) {
			continue;
		}
		if (event.status == "compiled" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    StringUtil::Contains(event.reason, "vectorized table scan filters") &&
		    StringUtil::Contains(event.reason, "append sink protocol")) {
			found_compiled_fused_region = true;
			RequireCompiledFusedRegion(event);
			RequireDuckDBScanFilteredSourceContract(event);
		}
		if (event.phase == "runtime" && event.status == "executed" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_runtime_fused_region = true;
			REQUIRE(event.region_execution_form == "fused");
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
