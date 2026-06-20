#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT hash join build protocol compiles only inside generated fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_l AS SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE jit_hash_build_r AS SELECT i::BIGINT AS j, (i + 1)::BIGINT AS x FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT a.i, b.j FROM jit_hash_build_l a "
	                        "JOIN (SELECT j, x FROM jit_hash_build_r WHERE j > 10) b ON a.i=b.x "
	                        "WHERE a.i < 20 ORDER BY a.i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14, 15, 16, 17, 18, 19}));

	bool found_build_compile = false;
	bool found_build_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event)) {
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-build-generated-body-missing"));
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_GENERATED_PROTOCOL_REASON)) {
				found_build_compile = true;
				REQUIRE(EventExecutionMode(event) == "native");
				REQUIRE(EventExecutionBody(event) == "generated-machine-code");
				REQUIRE(event.runner_cost.present);
				REQUIRE(event.runner_cost.accelerated_stage_count > 0);
				REQUIRE(event.code_size > 0);
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_prepare"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_hash"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_append"));
			}
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_prepare")) {
			found_build_runtime = true;
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.bind_sink_contract"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.reference_keys"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.filter_pushdown"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.reference_payload"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_hash"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_append"));
			REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                                   "hash_join_build.vectorized_sink_update"));
		}
	}
	REQUIRE(found_build_compile);
	REQUIRE(found_build_runtime);
}

TEST_CASE("JIT auto skips hash join regions through planner cost selection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_join_l AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_join_r AS "
	                          "SELECT i::BIGINT AS k, (i + 1)::BIGINT AS w FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(l.v + r.w) FROM jit_auto_hash_join_l l "
	                        "JOIN jit_auto_hash_join_r r ON l.k=r.k WHERE l.k BETWEEN 10 AND 9990");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE_FALSE(result->GetValue(0, 0).IsNull());

	bool found_hash_join_decision = false;
	bool found_hash_join_vectorized_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventRequestedPolicy(event) != "auto") {
			continue;
		}
		const auto hash_join_shape =
		    StringUtil::Contains(event.ir, "hash-join") || StringUtil::Contains(event.reason, "hash-join");
			if (!hash_join_shape) {
				continue;
			}
			found_hash_join_decision = true;
			if (IsCompiledSljitRegionEvent(event)) {
				FAIL("auto policy must not compile hash join regions until DuckDB selects compiled execution");
			}
		if (EventPhase(event) == "decision" && EventStatus(event) == "skipped") {
			found_hash_join_vectorized_skip = true;
			REQUIRE(EventExecutionMode(event) == "unsupported");
			REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
			REQUIRE(event.runner_cost.present);
		} else if (EventPhase(event) == "decision") {
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
		}
	}
	REQUIRE(found_hash_join_decision);
	REQUIRE(found_hash_join_vectorized_skip);
}

TEST_CASE("JIT plain hash join build protocol-only region stays vectorized", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_plain_hash_build_l AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_plain_hash_build_r AS "
	                          "SELECT i::BIGINT AS j, (i + 1)::BIGINT AS x FROM range(1000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT count(*) FROM jit_plain_hash_build_l l "
	                        "JOIN jit_plain_hash_build_r r ON l.i=r.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 999);

	bool found_hash_build_unsupported = false;
	bool found_hash_build_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_GENERATED_PROTOCOL_REASON)) {
			REQUIRE_FALSE(IsCompiledSljitRegionEvent(event));
			found_hash_build_unsupported = true;
			REQUIRE(EventStatus(event) == "unsupported");
			REQUIRE(EventExecutionMode(event) == "unsupported");
			REQUIRE(event.code_size == 0);
			REQUIRE(StringUtil::Contains(event.reason, "SLJIT native region emits no generated machine code"));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_append")) {
			found_hash_build_runtime = true;
		}
	}
	REQUIRE(found_hash_build_unsupported);
	REQUIRE_FALSE(found_hash_build_runtime);
}

TEST_CASE("JIT hash join probe generates native probe with append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_l AS "
	                          "SELECT ((i % 32) * 1000003)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(32) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT l.k + 1 AS kk, l.v, r.w FROM jit_hash_probe_l l "
	                        "JOIN jit_hash_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 65536);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 0);
	REQUIRE(result->GetValue(2, 0).GetValue<int64_t>() == 0);

	bool found_probe = false;
	bool found_runtime = false;
	bool found_generated_probe_stage = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
			found_probe = true;
			RequireNativeFusedRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE_FALSE(
			    StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary;stage=hash-join-probe"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe("));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" && EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows > 0 &&
		    event.output_rows > 0) {
			found_runtime = true;
			REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                                   "hash_join_probe.vectorized_probe_primitive="));
			if (StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.generated_probe_function=")) {
				found_generated_probe_stage = true;
			}
		}
	}
	REQUIRE(found_probe);
	REQUIRE(found_runtime);
	REQUIRE(found_generated_probe_stage);

	auto explain = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                         "SELECT l.k + 1 AS kk, l.v, r.w FROM jit_hash_probe_l l "
	                         "JOIN jit_hash_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*explain);
	REQUIRE(explain->RowCount() == 1);
	auto analyzed_plan = explain->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash_join_probe.generated_probe_function"));
	REQUIRE_FALSE(StringUtil::Contains(analyzed_plan, "hash_join_probe.vectorized_probe_primitive"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_stage_runtime_breakdown\""));
}

TEST_CASE("JIT nested loop join probe contract-only region is pruned by core candidate builder", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_probe_l AS "
	                          "SELECT CAST(i AS DECIMAL(38,2)) AS x FROM range(4) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_probe_r AS "
	                          "SELECT CAST(1.500000000000 AS DECIMAL(38,12)) AS y"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT l.x, r.y FROM jit_nested_probe_l l "
	                        "JOIN jit_nested_probe_r r ON CAST(l.x AS DECIMAL(38,12)) > r.y ORDER BY l.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2);

	bool found_nested_loop_probe_candidate = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate || event.candidate_traits.operator_count == 0) {
			continue;
		}
		if (StringUtil::Contains(event.reason, "nested_loop_join_probe") ||
		    StringUtil::Contains(event.ir, "nested_loop_join_probe")) {
			found_nested_loop_probe_candidate = true;
			REQUIRE_FALSE(
			    StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary;stage=nested-loop-join-probe"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "native-operator-executable-body-missing"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "SLJIT native region emits no generated machine code"));
		}
	}
	REQUIRE_FALSE(found_nested_loop_probe_candidate);
}

TEST_CASE("JIT delimiter join sink contract-only region is pruned by core candidate builder", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_outer(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_delim_outer SELECT i, i % 5 FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_inner(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_delim_inner SELECT i, i % 5 FROM range(1000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT k FROM jit_delim_outer o "
	                        "WHERE EXISTS (SELECT 1 FROM jit_delim_inner i WHERE i.g=o.g AND i.k>o.k) "
	                        "ORDER BY k LIMIT 3");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 3);

	bool found_delim_sink_candidate = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate || event.candidate_traits.sink_kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			continue;
		}
		found_delim_sink_candidate = true;
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary;stage=delim-join-sink"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native-operator-executable-body-missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "SLJIT native region emits no generated machine code"));
	}
	REQUIRE_FALSE(found_delim_sink_candidate);
}
