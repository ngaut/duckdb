#include "test_jit_helpers.hpp"
#include "duckdb/main/query_result_stream.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_chunk_coalescer.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include "sljit_native_runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <thread>

using namespace duckdb;

namespace {
class RecordingChunkStep final : public ExecutionRegionChunkStep {
public:
	SinkResultType ExecuteChunk(DataChunk &input, ExecutionSourceChunkProof proof) override {
		counts.push_back(input.size());
		proofs.push_back(proof.table_filters_hold);
		for (idx_t row = 0; row < input.size(); row++) {
			vector<Value> values;
			for (idx_t col = 0; col < input.ColumnCount(); col++) {
				values.push_back(input.GetValue(col, row));
			}
			rows.push_back(std::move(values));
		}
		return stop ? SinkResultType::FINISHED : SinkResultType::NEED_MORE_INPUT;
	}
	void FlushChunks() override {
		flush_count++;
	}
	void FinishChunks() override {
		finish_count++;
	}
	vector<idx_t> counts;
	vector<bool> proofs;
	vector<vector<Value>> rows;
	idx_t flush_count = 0;
	idx_t finish_count = 0;
	bool stop = false;
};
} // namespace

TEST_CASE("Core chunk coalescer retains selected long strings and nulls after source destruction", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer),
	                                  ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS);
	const string long_text(256, 'x');
	for (idx_t batch = 0; batch < 2; batch++) {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR, LogicalType::BIGINT});
		input.SetChildCardinality(3);
		input.data[0].SetValue(0, Value(long_text + std::to_string(batch)));
		input.data[0].SetValue(1, Value(LogicalType::VARCHAR));
		input.data[0].SetValue(2, Value("unused"));
		input.data[1].SetValue(0, Value::BIGINT(10 + batch));
		input.data[1].SetValue(1, Value(LogicalType::BIGINT));
		input.data[1].SetValue(2, Value::BIGINT(-1));
		SelectionVector selected(2);
		selected.set_index(0, 1);
		selected.set_index(1, 0);
		input.Slice(selected, 2);
		REQUIRE(ExecutionChunkCoalescer::CanAppend(input));
		REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
		REQUIRE(record.rows.empty());
	}
	step.FinishChunks();
	REQUIRE(record.counts == vector<idx_t> {4});
	REQUIRE(record.finish_count == 1);
	for (idx_t batch = 0; batch < 2; batch++) {
		REQUIRE(record.rows[batch * 2][0].IsNull());
		REQUIRE(record.rows[batch * 2][1].IsNull());
		REQUIRE(record.rows[batch * 2 + 1][0].GetValue<string>() == long_text + std::to_string(batch));
		REQUIRE(record.rows[batch * 2 + 1][1].GetValue<int64_t>() == 10 + batch);
	}
}

TEST_CASE("Core chunk coalescer batches only chunks under its consumer's passthrough rows", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer), STANDARD_VECTOR_SIZE / 4);
	auto execute = [&](idx_t count) {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
		for (idx_t row = 0; row < count; row++) {
			FlatVector::GetDataMutable<int64_t>(input.data[0])[row] = NumericCast<int64_t>(row);
		}
		input.SetChildCardinality(count);
		REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
	};
	// a chunk from a quarter vector on runs as it is; smaller chunks wait for a batch
	execute(STANDARD_VECTOR_SIZE / 4);
	execute(STANDARD_VECTOR_SIZE / 4 - 1);
	execute(100);
	REQUIRE(record.counts == vector<idx_t> {STANDARD_VECTOR_SIZE / 4});
	// once rows wait, every chunk joins them while the batch has room
	execute(STANDARD_VECTOR_SIZE / 2);
	REQUIRE(record.counts.size() == 1);
	step.FinishChunks();
	REQUIRE(record.counts ==
	        vector<idx_t> {STANDARD_VECTOR_SIZE / 4, STANDARD_VECTOR_SIZE / 4 - 1 + 100 + STANDARD_VECTOR_SIZE / 2});
}

TEST_CASE("Core chunk coalescer owns strings borrowed from an advancing source", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer),
	                                  ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS);
	string borrowed(256, 'a');
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR});
	input.SetChildCardinality(2);
	FlatVector::GetDataMutable<string_t>(input.data[0])[0] = string_t(borrowed.c_str(), borrowed.size());
	REQUIRE_FALSE(input.data[0].Buffer().GetAuxiliaryData());
	input.data[0].SetValue(1, Value(string(256, 'c')));
	REQUIRE(input.data[0].Buffer().GetAuxiliaryData());
	REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
	std::fill(borrowed.begin(), borrowed.end(), 'b');
	step.FinishChunks();
	REQUIRE(record.rows.size() == 2);
	REQUIRE(record.rows[0][0].GetValue<string>() == string(256, 'a'));
	REQUIRE(record.rows[1][0].GetValue<string>() == string(256, 'c'));
}

TEST_CASE("Core chunk coalescer flushes partitions and stops consuming after a finished sink", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer),
	                                  ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS);
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	input.SetChildCardinality(1);
	input.data[0].SetValue(0, Value::INTEGER(7));
	REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
	step.FlushChunks();
	REQUIRE(record.counts == vector<idx_t> {1});
	REQUIRE(record.flush_count == 1);
	input.data[0].SetValue(0, Value::INTEGER(9));
	REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
	record.stop = true;
	step.FlushChunks();
	REQUIRE(record.counts == vector<idx_t> {1, 1});
	REQUIRE(record.rows[0][0].GetValue<int32_t>() == 7);
	REQUIRE(record.rows[1][0].GetValue<int32_t>() == 9);
	REQUIRE(record.flush_count == 1);
	REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::FINISHED);
	step.FinishChunks();
	REQUIRE(record.rows.size() == 2);
	REQUIRE(record.finish_count == 1);
}

TEST_CASE("Core chunk coalescer proves for a buffered batch only what every chunk in it proved", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer),
	                                  ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS);
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	auto run = [&](idx_t count, bool filters_hold) {
		input.Reset();
		input.data[0].Reference(Value::BIGINT(1), count_t(count));
		input.CheckCardinality(count);
		ExecutionSourceChunkProof proof;
		proof.table_filters_hold = filters_hold;
		REQUIRE(step.ExecuteChunk(input, proof) == SinkResultType::NEED_MORE_INPUT);
	};
	run(3, true);
	run(4, true);
	step.FlushChunks();
	run(3, true);
	run(4, false);
	step.FlushChunks();
	run(3, false);
	run(4, true);
	step.FlushChunks();
	run(STANDARD_VECTOR_SIZE, true);
	step.FinishChunks();
	REQUIRE(record.counts == vector<idx_t> {7, 7, 7, STANDARD_VECTOR_SIZE});
	REQUIRE(record.proofs == vector<bool> {true, false, false, true});
}

TEST_CASE("Core chunk coalescer preserves full batches and flushes before capacity overflow", "[api][jit]") {
	auto consumer = make_uniq<RecordingChunkStep>();
	auto &record = *consumer;
	ExecutionCoalescingChunkStep step(Allocator::DefaultAllocator(), std::move(consumer),
	                                  ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS);
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	for (auto count : vector<idx_t> {STANDARD_VECTOR_SIZE, 3, STANDARD_VECTOR_SIZE - 1}) {
		input.Reset();
		input.data[0].Reference(Value::BIGINT(42), count_t(count));
		input.CheckCardinality(count);
		REQUIRE(step.ExecuteChunk(input, ExecutionSourceChunkProof()) == SinkResultType::NEED_MORE_INPUT);
	}
	step.FinishChunks();
	REQUIRE(record.counts == vector<idx_t> {STANDARD_VECTOR_SIZE, 3, STANDARD_VECTOR_SIZE - 1});
	REQUIRE(record.rows.size() == STANDARD_VECTOR_SIZE * 2 + 2);
	for (auto &row : record.rows) {
		REQUIRE(row[0].GetValue<int64_t>() == 42);
	}
}

TEST_CASE("JIT cached casts report the current query identity", "[api][jit]") {
	JitTestDatabase test;
	ConfigureSljitForCoverage(test.con, true, true, 10000);
	REQUIRE_NO_FAIL(test.con.Query("CREATE TABLE cast_inputs AS SELECT "
	                               "CASE WHEN i = 77777 THEN 40000 ELSE i % 30000 END::BIGINT AS k "
	                               "FROM range(100000) t(i)"));
	for (idx_t threads : {1, 4}) {
		REQUIRE_NO_FAIL(test.con.Query("SET threads=" + to_string(threads)));
		for (idx_t variant = 0; variant < 3; variant++) {
			INFO("threads=" << threads << "; variant=" << variant);
			auto alias = "column_" + to_string(variant);
			auto query = string(variant * 10, ' ') + "SELECT " + alias + "::SMALLINT FROM (SELECT k AS " + alias +
			             " FROM cast_inputs) t";
			REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='off'"));
			auto expected = test.con.Query(query);
			REQUIRE_FAIL(expected);
			REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='auto'"));
			ClearJitTrace(test.manager, true);
			auto actual = test.con.Query(query);
			REQUIRE_FAIL(actual);
			REQUIRE(actual->GetError() == expected->GetError());
			RequireJitEvent(test.manager, [&](const ExecutionRegionEvent &event) {
				return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
				       (variant == 0 || StringUtil::Contains(event.reason, "compiled artifact cache hit"));
			});
			RequireJitEvent(test.manager, [](const ExecutionRegionEvent &event) {
				return event.backend_name == "sljit" && EventPhase(event) == "runtime" && EventStatus(event) == "error";
			});
		}
	}
	vector<unique_ptr<Connection>> connections;
	vector<string> queries;
	vector<string> expected_errors;
	vector<string> actual_errors(3);
	for (idx_t i = 0; i < 3; i++) {
		auto con = make_uniq<Connection>(test.db);
		ConfigureSljitForCoverage(*con, true, true, 10000);
		auto alias = "concurrent_column_" + to_string(i);
		queries.push_back(string(i * 7, ' ') + "SELECT " + alias +
		                  "::SMALLINT FROM "
		                  "(SELECT k AS " +
		                  alias + " FROM cast_inputs) t");
		REQUIRE_NO_FAIL(con->Query("SET jit_policy='off'"));
		auto expected = con->Query(queries.back());
		REQUIRE_FAIL(expected);
		expected_errors.push_back(expected->GetError());
		REQUIRE_NO_FAIL(con->Query("SET jit_policy='auto'"));
		connections.push_back(std::move(con));
	}
	vector<std::thread> workers;
	for (idx_t i = 0; i < connections.size(); i++) {
		workers.emplace_back([&, i]() {
			auto result = connections[i]->Query(queries[i]);
			actual_errors[i] = result->HasError() ? result->GetError() : "unexpected success";
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	REQUIRE(actual_errors == expected_errors);
}

TEST_CASE("JIT cached aggregates use current cardinality and match native results", "[api][jit]") {
	auto require_equal = [](QueryResult &actual, QueryResult &expected) {
		REQUIRE(actual.GetTypes() == expected.GetTypes());
		REQUIRE(actual.RowCount() == expected.RowCount());
		auto actual_rows = actual.Collection().GetRows();
		auto expected_rows = expected.Collection().GetRows();
		for (idx_t row = 0; row < actual.RowCount(); row++) {
			for (idx_t col = 0; col < actual.ColumnCount(); col++) {
				REQUIRE(actual_rows.GetValue(col, row) == expected_rows.GetValue(col, row));
			}
		}
	};
	JitTestDatabase test;
	ConfigureSljitForCoverage(test.con, true, true, 10000);
	REQUIRE_NO_FAIL(test.con.Query("SET perfect_ht_threshold=0"));
	vector<string> queries;
	vector<unique_ptr<QueryResult>> expected;
	for (idx_t variant = 0; variant < 2; variant++) {
		INFO("variant=" << variant);
		auto table = "aggregate_inputs_" + to_string(variant);
		REQUIRE_NO_FAIL(test.con.Query("CREATE TABLE " + table +
		                               " AS SELECT (((i // 8) % 5000) * 17)::BIGINT AS k, (i % 7)::BIGINT AS v "
		                               "FROM range(" +
		                               to_string(100000 * (variant + 1)) + ") t(i)"));
		REQUIRE_NO_FAIL(test.con.Query("VACUUM " + table));
		queries.push_back("SELECT k, sum(v) FROM " + table + " GROUP BY k ORDER BY k");
		REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='off'"));
		expected.push_back(test.con.Query(queries.back()));
		REQUIRE_NO_FAIL(*expected.back());
		REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='auto'"));
		ClearJitTrace(test.manager, true);
		auto actual = test.con.Query(queries.back());
		REQUIRE_NO_FAIL(*actual);
		require_equal(*actual, *expected.back());
		RequireJitEvent(test.manager, [&](const ExecutionRegionEvent &event) {
			return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
			       event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
			       (variant == 0 || StringUtil::Contains(event.reason, "compiled artifact cache hit"));
		});
		RequireJitEvent(test.manager, [](const ExecutionRegionEvent &event) {
			return event.backend_name == "sljit" && EventPhase(event) == "runtime" &&
			       EventStatus(event) == "executed" && HasGeneratedAggregateUpdateStage(event);
		});
	}
	vector<unique_ptr<Connection>> connections;
	vector<unique_ptr<QueryResult>> results(queries.size());
	for (idx_t i = 0; i < queries.size(); i++) {
		auto con = make_uniq<Connection>(test.db);
		ConfigureSljitForCoverage(*con, true, true, 10000);
		REQUIRE_NO_FAIL(con->Query("SET perfect_ht_threshold=0"));
		connections.push_back(std::move(con));
	}
	vector<std::thread> workers;
	for (idx_t i = 0; i < queries.size(); i++) {
		workers.emplace_back([&, i]() { results[i] = connections[i]->Query(queries[i]); });
	}
	for (auto &worker : workers) {
		worker.join();
	}
	for (idx_t i = 0; i < results.size(); i++) {
		REQUIRE_NO_FAIL(*results[i]);
		require_equal(*results[i], *expected[i]);
	}
}

TEST_CASE("JIT operator contracts distinguish nested loops from hash joins", "[api][jit]") {
	JitTestDatabase test;
	ConfigureSljitForCoverage(test.con, true, true, 10000);
	REQUIRE_NO_FAIL(test.con.Query("SET nested_loop_join_threshold=1000000"));
	REQUIRE_NO_FAIL(test.con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(
	    test.con.Query("CREATE TABLE contract_inputs AS SELECT i % 37 AS k, i AS v FROM range(20000) t(i)"));
	REQUIRE_NO_FAIL(test.con.Query("CREATE TABLE contract_bounds AS SELECT i * 10 AS k FROM range(5) t(i)"));
	const vector<string> queries {
	    "SELECT count(*), sum(a.v) FROM contract_inputs a LEFT JOIN contract_bounds b ON a.k < b.k",
	    "SELECT count(*), sum(a.v) FROM contract_inputs a JOIN contract_bounds b ON a.k = b.k",
	    "SELECT count(*), sum(a.v) FROM contract_inputs a JOIN "
	    "(SELECT x.k FROM contract_bounds x JOIN contract_bounds y ON x.k < y.k) b ON a.k = b.k"};
	for (idx_t index = 0; index < queries.size(); index++) {
		INFO(queries[index]);
		auto plan = test.con.Query("EXPLAIN (FORMAT JSON) " + queries[index]);
		REQUIRE_NO_FAIL(*plan);
		auto plan_text = plan->Collection().GetValue(1, 0).ToString();
		if (index != 1) {
			REQUIRE(StringUtil::Contains(plan_text, "NESTED_LOOP_JOIN"));
		}
		if (index != 0) {
			REQUIRE(StringUtil::Contains(plan_text, "HASH_JOIN"));
		}
		REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='off'"));
		auto expected = test.con.Query(queries[index]);
		REQUIRE_NO_FAIL(*expected);
		REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='auto'"));
		auto actual = test.con.Query(queries[index]);
		REQUIRE_NO_FAIL(*actual);
		REQUIRE(actual->Equals(*expected));
	}
}

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
	for (idx_t query_idx = 0; query_idx < 4; query_idx++) {
		REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	}

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

TEST_CASE("Execution region runner cost schema accumulates every numeric field", "[api][jit]") {
	ExecutionRegionEvent event;
	event.backend_name = "schema-test";
	event.runner_cost.present = true;
	int64_t next_value = 1;
	ForEachPhysicalRunnerCostShapeField(event.runner_cost, event.runner_cost,
	                                    [&](const char *, int64_t &value, int64_t &) { value = next_value++; });
	ForEachPhysicalRunnerCostWorkField(event.runner_cost, event.runner_cost,
	                                   [&](const char *, int64_t &value, int64_t &) { value = next_value++; });

	ExecutionRegionEventLog log;
	log.Record(0, event);
	auto counters = log.GetCounters();
	REQUIRE(counters.size() == 1);
	ForEachPhysicalRunnerCostShapeField(event.runner_cost, counters[0].runner_cost,
	                                    [&](const char *name, const int64_t &source, const int64_t &total) {
		                                    INFO(name);
		                                    REQUIRE(total == source);
	                                    });
	ForEachPhysicalRunnerCostWorkField(event.runner_cost, counters[0].runner_cost,
	                                   [&](const char *name, const int64_t &source, const int64_t &total) {
		                                   INFO(name);
		                                   REQUIRE(total == source);
	                                   });
}

TEST_CASE("JIT auto vectorized skips avoid telemetry when unobserved", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con, "auto", false, false, 0);
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

TEST_CASE("Recursive rescheduling reuses the pipeline execution-region plan", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con);
	ConfigureJitDecisionTrace(con);
	ClearJitTrace(manager, true);
	auto result = con.Query("WITH RECURSIVE counter(value) AS ("
	                        "SELECT 1::BIGINT "
	                        "UNION ALL "
	                        "SELECT value + 1 FROM counter WHERE value < 1000"
	                        ") SELECT sum(value) FROM counter");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "500500");

	// Decision telemetry is emitted while the pipeline plan is built. It must
	// stay proportional to the fixed pipeline graph, not to 1,000 recursive
	// iterations.
	REQUIRE(manager.GetEvents().size() <= 10);
}

TEST_CASE("JIT full pipeline uses explicit append sink contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_result_collector AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true);

	ClearJitTrace(manager);
	auto result = con.Query("SELECT i + 1 AS j FROM jit_native_result_collector WHERE i > 500");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 499);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 502);
	REQUIRE(result->Collection().GetValue(0, 498).GetValue<int64_t>() == 1000);

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
			found_compiled_result_collector = true;
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:native"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:native"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_required_capability=result-collector-execution-sink"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
			REQUIRE(StringUtil::Contains(event.ir, "append_sink("));
			REQUIRE(StringUtil::Contains(event.ir, "sink<kind=result-collector-sink"));
			REQUIRE(event.candidate_traits.HasSink());
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

TEST_CASE("JIT blocked append sink transfers retry ownership to the core executor", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_blocked_append_sink AS "
	                          "SELECT i::BIGINT AS i FROM range(160000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true, 10000, 4);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	ClearJitTrace(manager);
	auto result = make_uniq<QueryResultStream<>>(con.Submit("SELECT i * 3 + 7 AS v FROM jit_blocked_append_sink"));
	REQUIRE(!result->HasError());

	vector<bool> seen(160000, false);
	idx_t row_count = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			const auto value = chunk->GetValue(0, row_idx).GetValue<int64_t>();
			REQUIRE(value >= 7);
			REQUIRE((value - 7) % 3 == 0);
			const auto input_row = NumericCast<idx_t>((value - 7) / 3);
			REQUIRE(input_row < seen.size());
			CAPTURE(row_count, input_row, value);
			REQUIRE_FALSE(seen[input_row]);
			seen[input_row] = true;
			row_count++;
		}
	}
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	REQUIRE(row_count == seen.size());
	for (auto present : seen) {
		REQUIRE(present);
	}

	// The compiled step ran while the sink blocked: the executor yielded to the result reader mid-pipeline
	bool found_interrupted_native_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" && HasInterruptedChunkStepEntry(event) &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_interrupted_native_sink = true;
			REQUIRE(event.output_rows > 0);
		}
	}
	REQUIRE(found_interrupted_native_sink);
}

TEST_CASE("JIT blocked append sink keeps the rest of a fanning-out hash join probe", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	LoadSljit(con);
	// Every probe row matches 100 build rows: one probe chunk drains into many output chunks, so the streaming
	// result sink blocks while the probes still have output pending for the same input chunk
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_blocked_fanout_probe AS "
	                          "SELECT i::BIGINT AS i, (i % 4)::BIGINT AS k4, (i % 5)::BIGINT AS k5 "
	                          "FROM range(20000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_blocked_fanout_build AS "
	                          "SELECT i::BIGINT AS j, (i % 4)::BIGINT AS k FROM range(400) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true, 10000, 1);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	struct StreamFingerprint {
		idx_t rows = 0;
		hugeint_t sum = 0;
		hugeint_t weighted = 0;
	};
	auto stream = [&](const string &sql) {
		StreamFingerprint fingerprint;
		auto result = make_uniq<QueryResultStream<>>(con.Submit(sql));
		REQUIRE(!result->HasError());
		while (true) {
			auto chunk = result->Fetch();
			if (!chunk) {
				break;
			}
			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				auto i = chunk->GetValue(0, row_idx).GetValue<int64_t>();
				auto j_value = chunk->GetValue(1, row_idx);
				auto j = j_value.IsNull() ? int64_t(-1) : j_value.GetValue<int64_t>();
				fingerprint.rows++;
				fingerprint.sum += i;
				fingerprint.weighted += hugeint_t(i) * hugeint_t(1000003) + hugeint_t(j);
			}
		}
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return fingerprint;
	};

	const vector<string> queries {
	    // inner: 2,000,000 rows
	    "SELECT p.i, b.j FROM jit_blocked_fanout_probe p JOIN jit_blocked_fanout_build b ON p.k4 = b.k",
	    // left: the unmatched rows of a probe chunk drain after its matches
	    "SELECT p.i, b.j FROM jit_blocked_fanout_probe p LEFT JOIN jit_blocked_fanout_build b ON p.k5 = b.k",
	    // chained: the second probe drains inside each output chunk of the first
	    "SELECT p.i, b.j + c.j FROM jit_blocked_fanout_probe p JOIN jit_blocked_fanout_build b ON p.k4 = b.k "
	    "JOIN jit_blocked_fanout_build c ON b.k = c.k WHERE p.i < 300"};
	for (auto &sql : queries) {
		CAPTURE(sql);
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		auto expected = stream(sql);
		REQUIRE(expected.rows > 100000);
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));

		ClearJitTrace(manager);
		auto actual = stream(sql);
		REQUIRE(actual.rows == expected.rows);
		REQUIRE(actual.sum == expected.sum);
		REQUIRE(actual.weighted == expected.weighted);

		bool found_interrupted_kernel = false;
		for (auto &event : manager.GetEvents()) {
			if (IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
			    EventExecutionMode(event) == "native" && HasInterruptedChunkStepEntry(event)) {
				found_interrupted_kernel = true;
			}
		}
		REQUIRE(found_interrupted_kernel);
	}
}

TEST_CASE("JIT chunk step sinks the rows it holds at the end into a blocked streaming sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_held_rows_profit AS "
	                          "SELECT ('n' || (i % 25))::VARCHAR AS nation, (1992 + i % 7)::BIGINT AS o_year, "
	                          "(i % 1000)::DECIMAL(15,2) AS amount FROM range(1000000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true, 10000, 1);

	// The sorted groups leave the ORDER BY source as one small chunk, which the step batches and holds until the
	// source ends. Its final flush is the stream's first chunk, and the first chunk parks until the reader decides how
	// to consume the result: the sink blocks inside the final flush
	const string sql = "SELECT nation, o_year, sum(amount) AS sum_profit FROM jit_held_rows_profit "
	                   "GROUP BY nation, o_year ORDER BY nation, o_year DESC";
	auto stream = [&]() {
		vector<string> rows;
		auto result = make_uniq<QueryResultStream<>>(con.Submit(sql));
		REQUIRE(!result->HasError());
		while (auto chunk = result->Fetch()) {
			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				rows.push_back(chunk->GetValue(0, row_idx).ToString() + "|" + chunk->GetValue(1, row_idx).ToString() +
				               "|" + chunk->GetValue(2, row_idx).ToString());
			}
		}
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return rows;
	};
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto expected = stream();
	REQUIRE(expected.size() == 175);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto actual = stream();
	REQUIRE(actual == expected);
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) &&
		       StringUtil::Contains(event.pipeline_shape, "source:source:ORDER_BY:source-contract") &&
		       StringUtil::Contains(event.pipeline_shape, "sink:sink:RESULT_COLLECTOR");
	});
}

TEST_CASE("JIT full pipeline uses append sink contract for CTE materialization", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_cte_materialization AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true);

	ClearJitTrace(manager);
	// Two hash-join consumers make a small CTE cheaper to materialize than to fan out, so it runs MATERIALIZED.
	auto result = con.Query("WITH cte AS MATERIALIZED ("
	                        "SELECT i + 1 AS j FROM jit_native_cte_materialization WHERE i > 500"
	                        ") SELECT sum(a.j) FROM cte a JOIN cte b USING (j)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 374749);

	bool found_compiled_materialization = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate) {
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
			REQUIRE(event.candidate_traits.HasSink());
		}
	}
	REQUIRE(found_compiled_materialization);
}

TEST_CASE("JIT generated projections hand their rows to DuckDB's sort", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_sort_sink AS "
	                          "SELECT (1000 - i)::BIGINT AS i FROM range(1000) tbl(i)"));
	ConfigureSljitForCoverageSettings(con, true, true);

	auto require_ordered_sink_contract = [&](const string &sql, const string &operator_name,
	                                         const string &expected_order_kind, idx_t expected_rows,
	                                         int64_t expected_first) {
		ClearJitTrace(manager);
		auto result = con.Query(sql);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == expected_rows);
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == expected_first);

		bool found_ordered_sink_contract = false;
		bool found_ordered_sink_runtime_breakdown = false;
		for (auto &event : manager.GetEvents()) {
			// the region computes the projections; DuckDB's sort computes its keys and sorts
			if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection=") &&
			    StringUtil::Contains(EventJitRuntimePathCounts(event), "order_sink.native_tail_delegation=")) {
				found_ordered_sink_runtime_breakdown = true;
			}
			if (!IsSljitRegionEvent(event) || !event.has_candidate) {
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
				REQUIRE(event.candidate_traits.HasSink());
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

	ConfigureSljitForCoverage(con, true);

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

		ConfigureSljitForCoverage(con, false, true);
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
			REQUIRE(event.runtime_result == "finished");
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
		REQUIRE(diff->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(actual->Collection().GetValue(0, 0) == expected->Collection().GetValue(0, 0));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.source_filter_count > 0 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "dynamic_filters=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireMixedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
	// Without group runs to win on, the hash aggregate leaves the pipeline after the region's composed source filters
	// to DuckDB's operators
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.selected_uses_scan_filters && event.source_contract_output_rows > 0 &&
		           HasJitRuntimePathPrefix(event, "source.native_continuation");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::NATIVE_CONTINUATION));
	    });
}

TEST_CASE("EXPLAIN ANALYZE exposes compact execution region profile", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljitForCoverage(con, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_analyze_input AS "
	                          "SELECT i::BIGINT AS i, (i + 1)::BIGINT AS g FROM range(10000) tbl(i)"));

	auto event_schema = con.Query("SELECT * FROM duckdb_jit_events() LIMIT 0");
	REQUIRE_NO_FAIL(*event_schema);
	ExecutionRegionEvent empty_event;
	auto event_schema_has_column = [&](const string &name) {
		auto &names = event_schema->GetNames();
		return std::any_of(names.begin(), names.end(),
		                   [&](const Identifier &column) { return column.GetIdentifierName() == name; });
	};
	REQUIRE_FALSE(event_schema_has_column("candidate_uses_scan_filters"));
	ForEachExecutionRegionEventRuntimeCountField(
	    empty_event, [&](const char *name, idx_t) { REQUIRE(event_schema_has_column(name)); });
	ForEachExecutionRegionEventRuntimeTimeField(
	    empty_event, [&](const char *name, const char *, int64_t) { REQUIRE(event_schema_has_column(name)); });
	ForEachPhysicalRunnerCostShapeField(empty_event.runner_cost, empty_event.runner_cost,
	                                    [&](const char *name, int64_t &, int64_t &) {
		                                    REQUIRE(event_schema_has_column(string("runner_cost_") + name));
	                                    });
	ForEachPhysicalRunnerCostWorkField(empty_event.runner_cost, empty_event.runner_cost,
	                                   [&](const char *name, int64_t &, int64_t &) {
		                                   REQUIRE(event_schema_has_column(string("runner_cost_") + name));
	                                   });

	auto trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->Collection().GetValue(0, 0).ToString() == "false");

	ClearJitTrace(manager, true);
	auto result = con.Query("EXPLAIN ANALYZE "
	                        "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto condensed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	// Default rendering is condensed: one line per decision and per kernel, and
	// the operator tree is annotated with the compiled-region attribution.
	REQUIRE(StringUtil::Contains(condensed_plan, "JIT_EXECUTION_REGIONS"));
	REQUIRE(StringUtil::Contains(condensed_plan, "CBO_PIPELINE"));
	REQUIRE(StringUtil::Contains(condensed_plan, "net_benefit="));
	REQUIRE(StringUtil::Contains(condensed_plan, "reason="));
	REQUIRE(StringUtil::Contains(condensed_plan, "RUNTIME_KERNELS"));
	REQUIRE(StringUtil::Contains(condensed_plan, "generated_us="));
	REQUIRE(StringUtil::Contains(condensed_plan, "compiled full-pipeline"));
	REQUIRE(!StringUtil::Contains(condensed_plan, "stages=gen:"));
	REQUIRE(!StringUtil::Contains(condensed_plan, "RUNTIME_PIPELINE"));

	// Full per-event detail is opt-in through jit_trace_decisions.
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	ClearJitTrace(manager, true);
	result = con.Query("EXPLAIN ANALYZE "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
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
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));

	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_result\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_source_execution\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_uses_scan_filters\""));
	REQUIRE(!StringUtil::Contains(analyzed_plan, "\"candidate_uses_scan_filters\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_enabled\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_policy\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_requested_backend\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_selected_backend\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"kernel_id\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"query_runtime_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_unattributed_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"source_runtime_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_runtime_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_unattributed_pct\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runtime_dominant_component\""));
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
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"runner_cost_selected_accelerated_runner\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"blocker\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "aggregate_update"));
	ForEachExecutionRegionEventRuntimeCountField(empty_event, [&](const char *name, idx_t) {
		REQUIRE(StringUtil::Contains(analyzed_plan, string("\"") + name + "\""));
	});
	ForEachExecutionRegionEventRuntimeTimeField(empty_event, [&](const char *, const char *profile_name, int64_t) {
		REQUIRE(StringUtil::Contains(analyzed_plan, string("\"") + profile_name + "\""));
	});
	ForEachPhysicalRunnerCostShapeField(
	    empty_event.runner_cost, empty_event.runner_cost, [&](const char *name, int64_t &, int64_t &) {
		    REQUIRE(StringUtil::Contains(analyzed_plan, string("\"runner_cost_") + name + "\""));
	    });
	ForEachPhysicalRunnerCostWorkField(
	    empty_event.runner_cost, empty_event.runner_cost, [&](const char *name, int64_t &, int64_t &) {
		    REQUIRE(StringUtil::Contains(analyzed_plan, string("\"runner_cost_") + name + "\""));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	ClearJitTrace(manager, true);
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(manager.GetEvents().empty());
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\": 1"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_runtime_time_us\""));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                   "SELECT sum(i * g) FROM jit_explain_analyze_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
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
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"execution_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"compiled_regions\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"decision_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"estimated_cardinality\": 10000"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"selected_runner\": \"vectorized\""));

	trace_setting = con.Query("SELECT current_setting('jit_trace_runtime')");
	REQUIRE_NO_FAIL(*trace_setting);
	REQUIRE(trace_setting->Collection().GetValue(0, 0).ToString() == "false");
}

TEST_CASE("EXPLAIN ANALYZE exposes grouped hash aggregate native state-address lookup", "[api][jit]") {
	DuckDB db;
	Connection con(db);

	ConfigureSljitForCoverage(con, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_explain_grouped_lookup AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(50000) tbl(i)"));

	auto result = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                        "SELECT k, sum(v) FROM jit_explain_grouped_lookup GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	auto analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
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
	auto analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
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
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "CBO_PIPELINE"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "runner=vectorized"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "net_benefit="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "reason="));

	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	result = con.Query("EXPLAIN ANALYZE "
	                   "SELECT sum(CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) + "
	                   "(((a - b + c) * 17) - ((a + d) * 19)) + "
	                   "(((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT)) "
	                   "FROM jit_explain_auto_aggregate_blocker");
	REQUIRE_NO_FAIL(*result);
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "selected=false"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "benefit="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "required="));
	REQUIRE(StringUtil::Contains(analyzed_plan, "why="));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=false"));

	// Tree annotations must not leak across queries: a later vectorized-only
	// query on the same connection must not inherit a stale compiled-kernel
	// annotation through operator address reuse.
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	result = con.Query("EXPLAIN ANALYZE "
	                   "SELECT sum(a + b) FROM jit_explain_auto_aggregate_blocker");
	REQUIRE_NO_FAIL(*result);
	analyzed_plan = result->Collection().GetValue(1, 0).GetValue<string>();
	REQUIRE(!StringUtil::Contains(analyzed_plan, "compiled full-pipeline"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
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
