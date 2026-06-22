#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT hash join build protocol compiles only inside generated fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_PROTOCOL_REASON)) {
				found_build_compile = true;
				REQUIRE(EventExecutionMode(event) == "native");
				REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
				REQUIRE(event.runner_cost.present);
				REQUIRE(event.runner_cost.generated_stage_count > 0);
				REQUIRE(event.runner_cost.native_join_stage_count > 0);
				REQUIRE(event.code_size > 0);
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
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
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.filter_pushdown"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.reference_payload"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_hash"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_append"));
		}
	}
	REQUIRE(found_build_compile);
	REQUIRE(found_build_runtime);
}

TEST_CASE("JIT CBO admits generated hash-build regions after generated work pays native join protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	ConfigureJitCoverageCbo(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_cbo_l AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_cbo_r AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_cbo_l l "
	                        "JOIN (SELECT k + 1 AS x FROM jit_hash_build_cbo_r WHERE k > 10) r ON l.k = r.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "9988");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_join_stage_count > 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
	    });
}

TEST_CASE("JIT CBO skips bodyless native hash-build candidates before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, false, false, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_bodyless_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 32)::BIGINT AS g, "
	                          "(i * 13 % 997)::BIGINT AS payload FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_bodyless_dim AS "
	                          "SELECT g::BIGINT AS g, (g * 101)::BIGINT AS payload FROM range(32) tbl(g)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(f.i + d.payload) FROM jit_hash_bodyless_fact f "
	                        "JOIN jit_hash_bodyless_dim d ON f.g = d.g WHERE f.i % 3 = 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "4935358");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "decision" && EventStatus(event) == "skipped" &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(event.blocker == "duckdb_selected_vectorized");
		    REQUIRE(event.backend_analysis_time_us == 0);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.native_join_stage_count > 0);
		    REQUIRE_FALSE(event.runner_cost.full_pipeline);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(StringUtil::Contains(event.reason, "backend_analysis=skipped"));
	    });
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
	bool found_hash_join_probe_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
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
			REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON)) {
				found_hash_join_probe_skip = true;
				REQUIRE(event.runner_cost.native_join_stage_count > 0);
			}
		} else if (EventPhase(event) == "decision") {
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
		}
	}
	REQUIRE(found_hash_join_decision);
	REQUIRE(found_hash_join_vectorized_skip);
	REQUIRE(found_hash_join_probe_skip);
}

TEST_CASE("JIT hash join probe generates native probe with append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE_FALSE(
			    StringUtil::Contains(event.reason, "whole-vectorized-operator-boundary;stage=hash-join-probe"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe("));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows > 0 &&
		    event.output_rows > 0) {
			found_runtime = true;
			REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
			REQUIRE(event.jit_runtime.lazy_codegen.codegen_time_us >= 0);
			REQUIRE(event.jit_runtime.lazy_codegen.machine_codegen_time_us >= 0);
			REQUIRE(event.jit_runtime.lazy_codegen.code_size > 0);
			if (StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.generated_regular_probe_function=")) {
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
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash_join_probe.generated_regular_probe_function"));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"hash_join_probe_layout\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_stage_runtime_breakdown\""));
}
