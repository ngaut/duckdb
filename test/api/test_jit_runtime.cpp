#include "test_jit_helpers.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
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

	REQUIRE(adapter.SourceCount() == 2);
	auto source_data = adapter.DataArray();
	REQUIRE(source_data[0] != nullptr);
	REQUIRE(source_data[1] != nullptr);
	auto first_source = reinterpret_cast<const int64_t *>(source_data[0]);
	auto second_source = reinterpret_cast<const int64_t *>(source_data[1]);
	REQUIRE(first_source[0] == 600);
	REQUIRE(second_source[0] == 200);
}

TEST_CASE("Execution region events are bounded and counters are cumulative", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "auto");
	ConfigureJitDecisionTrace(con);
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
		if (counter.backend_name != "sljit" || counter.status_kind != ExecutionRegionEventStatus::SKIPPED ||
		    counter.execution_mode_kind != ExecutionRegionExecutionMode::UNSUPPORTED) {
			continue;
		}
		found_auto_skip = true;
		REQUIRE(counter.stage_timings.pipeline_cbo_time_us >= 0);
		REQUIRE(counter.stage_timings.graph_build_time_us >= 0);
		REQUIRE(counter.stage_timings.candidate_cbo_time_us >= 0);
		REQUIRE(counter.stage_timings.backend_analysis_time_us >= 0);
		REQUIRE(counter.stage_timings.codegen_time_us == 0);
		REQUIRE(counter.stage_timings.executable_build_time_us == 0);
		REQUIRE(counter.stage_timings.machine_codegen_time_us == 0);
		REQUIRE(counter.stage_timings.kernel_build_time_us == 0);
		REQUIRE(counter.jit_runtime.lazy_codegen.codegen_time_us == 0);
		REQUIRE(counter.jit_runtime.lazy_codegen.machine_codegen_time_us == 0);
		REQUIRE(counter.jit_runtime.lazy_codegen.code_size == 0);
	}
	REQUIRE(found_auto_skip);
}

TEST_CASE("JIT auto vectorized skips avoid telemetry when unobserved", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "auto", false, false, false, 0);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=false"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_unobserved_skip_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_unobserved_skip_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetCounters().empty());

	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_unobserved_skip_input WHERE i > 500"));
	REQUIRE(!manager.GetCounters().empty());
}

TEST_CASE("JIT full pipeline uses explicit append sink contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_result_collector AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true, true);

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
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" &&
		    ExecutionRegionEventProfileCodeSize(event) > 0 &&
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
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.output_rows == 499) {
			found_runtime_result_collector = true;
			REQUIRE(event.runtime_result == "finished");
			REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP));
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
	ConfigureSljitForCoverageSettings(con, true, true, true);

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
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" &&
		    ExecutionRegionEventProfileCodeSize(event) > 0 &&
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
	ConfigureSljitForCoverageSettings(con, true, true, true);

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
			if (StringUtil::Contains(event.reason, "ordered sink contract") &&
			    StringUtil::Contains(event.reason, "operator_kind=" + expected_order_kind) &&
			    StringUtil::Contains(event.reason, "sink:" + operator_name + ":native")) {
				found_ordered_sink_contract = true;
				RequireGeneratedMachineCodeRegion(event);
				REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:native"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
				REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=order-native-sink-update"));
				REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=order-native-sink-update"));
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

TEST_CASE("JIT dump IR and execution mode expose backend honesty", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljitForCoverage(con, false, true);

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_ir = false;
	for (auto &event : manager.GetEvents()) {
		if (EventStatus(event) == "compiled") {
			REQUIRE(EventExecutionMode(event) == "native");
			RequireGeneratedMachineCodeRegion(event);
		}
		if (event.backend_name == "sljit" && !event.ir.empty() &&
		    StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir") && StringUtil::Contains(event.ir, ".add")) {
			found_ir = true;
			REQUIRE(ContainsTypedIrNode(event.ir, "binary", "BIGINT", "INT64"));
		}
	}
	REQUIRE(found_ir);
}

TEST_CASE("Execution region runtime trace records kernel execution facts", "[api][jit]") {
	bool found_region_runtime = false;
	bool found_runtime_counter = false;
	{
		DuckDB db;
		Connection con(db);
		auto &context = *con.context;
		auto &manager = ExecutionRegionManager::Get(context);

		ConfigureSljitForCoverage(con, false, false, true);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_trace_input AS "
		                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

		ClearJitTrace(manager);
		REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM "
		                          "(SELECT i + 1 AS j FROM jit_runtime_trace_input WHERE i > 0)"));
		for (auto &event : manager.GetEvents()) {
			if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" || event.output_rows == 0) {
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
			REQUIRE(event.jit_runtime.lazy_codegen.codegen_time_us >= 0);
			REQUIRE(event.jit_runtime.lazy_codegen.machine_codegen_time_us >= 0);
			REQUIRE(event.generated_body_runtime_time_us + event.source_contract_runtime_time_us <=
			        event.runtime_time_us);
			const bool expected_runtime_result = event.runtime_result == "need_more_input" ||
			                                     event.runtime_result == "have_more_output" ||
			                                     event.runtime_result == "finished";
			REQUIRE(expected_runtime_result);
			REQUIRE(StringUtil::Contains(event.reason, "kernel executed"));
		}
		for (auto &counter : manager.GetCounters()) {
			if (counter.status_kind == ExecutionRegionEventStatus::EXECUTED && counter.invocation_count > 0 &&
			    counter.input_rows > 0 && counter.execution_mode_kind == ExecutionRegionExecutionMode::NATIVE) {
				found_runtime_counter = true;
			}
		}
	}
	REQUIRE(found_region_runtime);
	REQUIRE(found_runtime_counter);
}

TEST_CASE("JIT auto keeps count distinct on regular aggregate keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_distinct_regular_key_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "       (i % 37)::INTEGER AS g, "
	                          "       CASE WHEN i % 13 = 0 THEN NULL ELSE (i % 97)::BIGINT END AS x "
	                          "FROM range(20000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE jit_distinct_regular_key_expected AS "
	                          "SELECT g, count(DISTINCT x + 0) AS c "
	                          "FROM jit_distinct_regular_key_input "
	                          "WHERE i >= 0 "
	                          "GROUP BY g"));

	auto run_and_require_regular_distinct_plan = [&](bool trace_decisions) {
		ClearJitTrace(manager, true);
		REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
		REQUIRE_NO_FAIL(con.Query(string("SET jit_trace_decisions=") + (trace_decisions ? "true" : "false")));
		REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE jit_distinct_regular_key_actual AS "
		                          "SELECT g, count(DISTINCT x1) AS c "
		                          "FROM ("
		                          "    SELECT g, x + 0 AS x1 "
		                          "    FROM jit_distinct_regular_key_input "
		                          "    WHERE i >= 0"
		                          ") projected "
		                          "GROUP BY g"));

		auto diff = con.Query("SELECT count(*) FROM ("
		                      "    (SELECT * FROM jit_distinct_regular_key_expected "
		                      "     EXCEPT ALL SELECT * FROM jit_distinct_regular_key_actual) "
		                      "    UNION ALL "
		                      "    (SELECT * FROM jit_distinct_regular_key_actual "
		                      "     EXCEPT ALL SELECT * FROM jit_distinct_regular_key_expected)"
		                      ") diff");
		REQUIRE_NO_FAIL(*diff);
		REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

		for (auto &event : manager.GetEvents()) {
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "distinct_count_pointer"));
		}
	};

	run_and_require_regular_distinct_plan(false);
	run_and_require_regular_distinct_plan(true);
}

TEST_CASE("JIT source contracts preserve joined table scan filter contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_contract_nation AS "
	                          "SELECT i::BIGINT AS nationkey, "
	                          "       CASE i WHEN 0 THEN 'alpha' WHEN 1 THEN 'beta' ELSE 'other' END AS name "
	                          "FROM range(8) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_contract_supplier AS "
	                          "SELECT i::BIGINT AS suppkey, "
	                          "       (i % 2)::BIGINT AS nationkey "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_contract_customer AS "
	                          "SELECT i::BIGINT AS custkey, "
	                          "       ((i + 1) % 2)::BIGINT AS nationkey "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_contract_orders AS "
	                          "SELECT i::BIGINT AS orderkey, "
	                          "       (i % 4096)::BIGINT AS custkey "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_contract_lineitem AS "
	                          "SELECT i::BIGINT AS linekey, "
	                          "       (i % 65536)::BIGINT AS orderkey, "
	                          "       (i % 4096)::BIGINT AS suppkey, "
	                          "       DATE '2020-01-01' + (i % 730)::INTEGER AS shipdate, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST((i % 7)::DOUBLE / 100 AS DECIMAL(15,2)) AS discount "
	                          "FROM range(131072) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT source_name, target_name, ship_year, sum(volume) AS revenue "
	                        "FROM ("
	                        "    SELECT n1.name AS source_name, n2.name AS target_name, "
	                        "           extract(year FROM l.shipdate) AS ship_year, "
	                        "           l.extendedprice * (1 - l.discount) AS volume "
	                        "    FROM jit_source_contract_supplier s, jit_source_contract_lineitem l, "
	                        "         jit_source_contract_orders o, jit_source_contract_customer c, "
	                        "         jit_source_contract_nation n1, jit_source_contract_nation n2 "
	                        "    WHERE s.suppkey = l.suppkey "
	                        "      AND o.orderkey = l.orderkey "
	                        "      AND c.custkey = o.custkey "
	                        "      AND s.nationkey = n1.nationkey "
	                        "      AND c.nationkey = n2.nationkey "
	                        "      AND ((n1.name = 'alpha' AND n2.name = 'beta') "
	                        "        OR (n1.name = 'beta' AND n2.name = 'alpha')) "
	                        "      AND l.shipdate BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'"
	                        ") AS shipping "
	                        "GROUP BY source_name, target_name, ship_year "
	                        "ORDER BY source_name, target_name, ship_year");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.operator_count >= 2 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "dynamic_filters=true") &&
		           StringUtil::Contains(event.reason, "source-execution:source-contract");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.candidate_uses_scan_filters);
		    REQUIRE(event.selected_uses_scan_filters);
		    RequireGeneratedMachineCodeRegion(event);
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.selected_uses_scan_filters && event.source_contract_output_rows > 0 &&
		           event.input_rows == event.source_contract_output_rows &&
		           HasJitRuntimePathPrefix(event, "hash_join_probe.");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.source_contract_output_rows > 0);
		    REQUIRE(event.input_rows == event.source_contract_output_rows);
		    REQUIRE(HasJitRuntimePathPrefix(event, "hash_join_probe."));
	    });
}

TEST_CASE("JIT composes dynamic scan filters with generated static filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_filter_keys AS "
	                          "SELECT i::INTEGER AS join_key FROM range(2500, 7500) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_filter_fact AS "
	                          "SELECT (i % 10000)::INTEGER AS join_key, i::BIGINT AS payload, "
	                          "       (i % 50000)::INTEGER AS group_key, "
	                          "       CASE WHEN i % 10 = 0 THEN 'special-' || i || '-requests' "
	                          "            ELSE 'regular-' || i END AS comment "
	                          "FROM range(131072) tbl(i)"));

	const string query = "SELECT sum(group_total) FROM ("
	                     "SELECT fact.group_key, sum(fact.payload) AS group_total "
	                     "FROM jit_mixed_filter_fact fact "
	                     "JOIN jit_mixed_filter_keys keys USING (join_key) "
	                     "WHERE fact.comment NOT LIKE '%special%requests%' "
	                     "GROUP BY group_key) grouped";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto expected = con.Query(query);
	REQUIRE_NO_FAIL(*expected);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto actual = con.Query(query);
	REQUIRE_NO_FAIL(*actual);
	REQUIRE(actual->GetValue(0, 0) == expected->GetValue(0, 0));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.source_filter_count > 0 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "dynamic_filters=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.candidate_uses_scan_filters);
		    RequireMixedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.selected_uses_scan_filters && event.source_contract_output_rows > 0 &&
		           HasJitRuntimePathPrefix(event, "filter.") && HasJitRuntimePathPrefix(event, "hash_join_probe.");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(HasJitRuntimePathPrefix(event, "filter."));
		    REQUIRE(HasJitRuntimePathPrefix(event, "hash_join_probe."));
	    });
}

TEST_CASE("EXPLAIN ANALYZE exposes compact execution region profile", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljitForCoverage(con, false, true);
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
	REQUIRE(StringUtil::Contains(analyzed_plan, "policy=auto"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "selected_backend=sljit"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "compiled=1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runtime_regions=1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "generated_runtime_us="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "lazy_codegen_us="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runtime_dominant="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "source_runtime_pct="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "generated_runtime_pct="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "CBO_PIPELINE"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "shape=ungrouped-aggregate-update"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "stages=gen:"));
	REQUIRE(StringUtil::Contains(analyzed_plan, ",agg:"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "benefit="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "required="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "selected=true"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "why="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "RUNTIME_PIPELINE"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "dominant="));
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
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"pipeline_cbo_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"graph_build_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"candidate_cbo_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"ir_lowering_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"backend_analysis_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"executable_build_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"machine_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"kernel_build_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_machine_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_code_size\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"hash_join_probe_layout\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_path_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_proof_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_delegation_counts\""));
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

TEST_CASE("EXPLAIN ANALYZE exposes grouped hash aggregate native state-address lookup", "[api][jit]") {
	DuckDB db;
	Connection con(db);

	ConfigureSljitForCoverage(con, false, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_grouped_lookup AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(50000) tbl(i)"));

	auto result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                        "SELECT k, sum(v) FROM jit_explain_grouped_lookup GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"status\": \"compiled\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_mode\": \"native\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "grouped_state_lookup=native-state-address"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "native_grouped_state_contract_status=ready"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "payload_update=generated-primitive"));
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

	result = con.Query("EXPLAIN ANALYZE "
	                   "SELECT sum(CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) + "
	                   "(((a - b + c) * 17) - ((a + d) * 19)) + "
	                   "(((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT)) "
	                   "FROM jit_explain_auto_aggregate_blocker");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "CBO_PIPELINE"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runner=vectorized"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "selected=false"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "benefit="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "required="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "why="));
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
			if (con.Query("SET jit_policy='auto'")->HasError()) {
				failed = true;
				return;
			}
			if (con.Query("SET jit_trace_decisions=true")->HasError()) {
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
