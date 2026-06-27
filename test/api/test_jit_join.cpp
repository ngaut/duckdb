#include "test_jit_helpers.hpp"

using namespace duckdb;

static void RequireRowPointerDescriptorLookupStages(const string &stage_counts) {
	const bool direct_descriptor_probe =
	    StringUtil::Contains(stage_counts, "find_or_create_row_pointer_descriptor.hash=") &&
	    StringUtil::Contains(stage_counts, "find_or_create_row_pointer_descriptor.probe=");
	const bool one_pass_descriptor_materialization =
	    StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.fill_hash=");
	REQUIRE((direct_descriptor_probe || one_pass_descriptor_materialization));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.fill="));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.hash="));
}

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
	ConfigureJitDecisionTrace(con);
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
	ConfigureJitDecisionTrace(con);
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
		    RequireVectorizedCboSkip(event);
		    REQUIRE(event.stage_timings.backend_analysis_time_us == 0);
		    REQUIRE(event.runner_cost.native_join_stage_count > 0);
		    REQUIRE_FALSE(event.runner_cost.full_pipeline);
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
	bool found_probe_stage = false;
	bool found_probe_path = false;
	bool found_materialization_boundary = false;
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
			REQUIRE(StringUtil::Contains(event.reason, "backend_join_key_type=int64:1"));
			REQUIRE(StringUtil::Contains(event.reason, "backend_join=hash_probe:1"));
			REQUIRE(StringUtil::Contains(event.reason, "equality_key:1"));
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
			if (StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.")) {
				found_probe_path = true;
			}
			if (StringUtil::Contains(EventJitMaterializationBoundaryCounts(event), "hash_join_probe.final_output=")) {
				found_materialization_boundary = true;
			}
			if (StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.generated_regular_probe_function=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.generated_regular_probe_flat_all_valid_function=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_notequal_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain=") ||
			    StringUtil::Contains(
			        EventGeneratedStageRuntimeBreakdown(event),
			        "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_notequal_chain=") ||
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
			                         "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_chain=")) {
				found_probe_stage = true;
			}
		}
	}
	REQUIRE(found_probe);
	REQUIRE(found_runtime);
	REQUIRE(found_probe_stage);
	REQUIRE(found_probe_path);
	REQUIRE(found_materialization_boundary);

	auto explain = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                         "SELECT l.k + 1 AS kk, l.v, r.w FROM jit_hash_probe_l l "
	                         "JOIN jit_hash_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*explain);
	REQUIRE(explain->RowCount() == 1);
	auto analyzed_plan = explain->GetValue(1, 0).GetValue<string>();
	const bool analyzed_plan_has_probe_stage =
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.generated_regular_probe_function") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.generated_regular_probe_flat_all_valid_function") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_chain") ||
	    StringUtil::Contains(analyzed_plan,
	                         "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_chain") ||
	    StringUtil::Contains(analyzed_plan,
	                         "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_notequal_chain") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain") ||
	    StringUtil::Contains(analyzed_plan,
	                         "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain") ||
	    StringUtil::Contains(analyzed_plan,
	                         "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_notequal_chain") ||
	    StringUtil::Contains(analyzed_plan, "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_chain");
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(analyzed_plan_has_probe_stage);
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"hash_join_probe_layout\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_path_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_materialization_boundary_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash_join_probe."));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_stage_runtime_breakdown\""));
}

TEST_CASE("JIT hash join probe uses selected all-valid single-key fast path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_selected_probe_l AS "
	                          "SELECT ((i % 10000) * 1000003)::BIGINT AS k, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_selected_probe_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(10000) tbl(i)"));

	const string query = "SELECT l.k, sum(l.v + r.w), sum(l.v - r.w), sum(l.v * 2), sum(r.w * 3), "
	                     "sum(l.v + 42), sum(r.w + 17) FROM jit_hash_selected_probe_l l "
	                     "JOIN jit_hash_selected_probe_r r ON l.k = r.k "
	                     "WHERE l.d > DATE '1994-01-01' GROUP BY l.k ORDER BY l.k LIMIT 3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT hash join filter ungrouped aggregate avoids final join materialization", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_filtered_agg_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 37)::DOUBLE AS quantity, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_filtered_agg_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, "
	                          "       ((i % 23) + 5)::DOUBLE AS threshold "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT sum(l.price)::HUGEINT AS revenue "
	                     "FROM jit_direct_filtered_agg_l l "
	                     "JOIN jit_direct_filtered_agg_r r ON l.k = r.k "
	                     "WHERE l.quantity < r.threshold OR l.k = -1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 1);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_hash_join_filtered_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_hash_join_filtered_payload_update="));
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.residual_predicate="));
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_hash_join_filtered_payload_input="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.residual_source_chunk="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_hash_join_filtered_state_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
	    });
}

TEST_CASE("JIT perfect hash join materializes selection-only output before post-join projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_post_join_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_post_join_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_post_join_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_post_join_dim"));

	const string query = "SELECT k_i, sum(price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM ("
	                     "    SELECT CAST(k * 1000003 AS INTEGER) AS k_i, price "
	                     "    FROM ("
	                     "        SELECT f.k, f.price "
	                     "        FROM jit_perfect_post_join_fact f "
	                     "        JOIN jit_perfect_post_join_dim d ON f.k=d.k"
	                     "    ) joined"
	                     ") projected "
	                     "GROUP BY k_i";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_post_join_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_post_join_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_post_join_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_post_join_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_post_join_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_post_join_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "hash_join_probe.materialize_output_fallback=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.generated_perfect_probe_function="));
		    REQUIRE(StringUtil::Contains(stage_counts,
		                                 "hash_join_probe.materialize_output_fallback.dictionary_build_payload="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
	    });
}

TEST_CASE("JIT join grouped aggregate updates through probe-side grouped keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_post_join_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 31)::INTEGER AS g, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_post_join_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k FROM range(8192) tbl(i)"));

	const string query = "SELECT l.g, sum((l.p - l.d) * 2.00::DECIMAL(15,2))::HUGEINT AS revenue "
	                     "FROM jit_direct_post_join_l l "
	                     "JOIN jit_direct_post_join_r r ON l.k = r.k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY l.g ORDER BY l.g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_row_pointer_grouped_lookup_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    RequireRowPointerDescriptorLookupStages(stage_counts);
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
	    });
}

TEST_CASE("JIT join grouped aggregate splits probe-side group keys from primitive payload", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_split_payload_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 97)::BIGINT AS g, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_split_payload_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k FROM range(8192) tbl(i)"));

	const string query = "SELECT l.g::SMALLINT AS g, sum(l.p)::HUGEINT AS revenue "
	                     "FROM jit_split_payload_l l "
	                     "JOIN jit_split_payload_r r ON l.k = r.k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY g ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_projected_group_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projected_group_payload_update="));
		    REQUIRE(StringUtil::Contains(stage_counts, "projection.post_join_direct_remap_batch_projection_hash="));
		    REQUIRE(StringUtil::Contains(stage_counts,
		                                 "direct_projected_group_payload_update.direct_new_split_payload.append="));
		    REQUIRE_FALSE(StringUtil::Contains(
		        stage_counts,
		        "direct_projected_group_payload_update.direct_new_split_payload.append.find_or_create_fast.hash="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "direct_projected_group_payload_update_unsupported."));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
	    });
}

TEST_CASE("JIT join grouped aggregate updates through RHS row-pointer keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_group_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_group_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       (i * 100003)::BIGINT AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS shippriority "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT r.orderkey::INTEGER AS orderkey, r.orderdate, r.shippriority::TINYINT AS shippriority, "
	                     "       sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	                     "FROM jit_row_pointer_group_l l "
	                     "JOIN jit_row_pointer_group_r r ON l.k = r.join_k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY orderkey, r.orderdate, shippriority "
	                     "ORDER BY orderkey, r.orderdate, shippriority "
	                     "LIMIT 64";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_row_pointer_grouped_lookup_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
	    });
}

TEST_CASE("JIT join grouped aggregate batches variable-width RHS row-pointer keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_varwidth_row_pointer_group_l AS "
	                          "SELECT ((i % 8192) * 100019)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST((i % 9)::DOUBLE / 100 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_varwidth_row_pointer_group_r AS "
	                          "SELECT (i * 100019)::BIGINT AS join_k, "
	                          "       'customer-' || (i % 97)::VARCHAR AS customer_name "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT r.customer_name, sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	                     "FROM jit_varwidth_row_pointer_group_l l "
	                     "JOIN jit_varwidth_row_pointer_group_r r ON l.k = r.join_k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY r.customer_name "
	                     "ORDER BY r.customer_name";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_row_pointer_grouped_lookup_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    RequireRowPointerDescriptorLookupStages(stage_counts);
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT two-join grouped aggregate direct-projects mixed VARCHAR before perfect fallback", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       (i % 8192)::BIGINT AS orderkey, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'nation_' || CAST(i % 7 AS VARCHAR) AS nation_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_orders AS "
	                          "SELECT i::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT x.nation, year(o.orderdate) AS o_year, sum(x.amount)::HUGEINT AS sum_profit "
	    "FROM ("
	    "  SELECT b.nation_name AS nation, "
	    "         CAST(l.orderkey AS INTEGER) AS orderkey_i, "
	    "         l.extendedprice * (1.00::DECIMAL(15,2) - l.discount) - "
	    "         b.supplycost * l.quantity AS amount "
	    "  FROM jit_two_join_l l "
	    "  JOIN jit_two_join_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	    ") x "
	    "JOIN jit_two_join_orders o ON x.orderkey_i = o.orderkey "
	    "GROUP BY x.nation, o_year "
	    "ORDER BY x.nation, o_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "projection.direct_between_join_projection=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_between_join_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_selection_reference="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op0:hash_join_probe.materialize_output"));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op2:hash_join_probe.materialize_output_fallback"));
		    });
	}

TEST_CASE("JIT Q5-like two-join aggregate groups compressed input-vector VARCHAR through row pointers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	REQUIRE_NO_FAIL(con.Query("LOAD tpch"));
	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=0.01)"));
	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));

	const string query = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue "
	                     "FROM customer, orders, lineitem, supplier, nation, region "
	                     "WHERE c_custkey = o_custkey "
	                     "  AND l_orderkey = o_orderkey "
	                     "  AND l_suppkey = s_suppkey "
	                     "  AND c_nationkey = s_nationkey "
	                     "  AND s_nationkey = n_nationkey "
	                     "  AND n_regionkey = r_regionkey "
	                     "  AND r_name = 'ASIA' "
	                     "  AND o_orderdate >= CAST('1994-01-01' AS date) "
	                     "  AND o_orderdate < CAST('1995-01-01' AS date) "
	                     "GROUP BY n_name "
	                     "ORDER BY revenue DESC";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_row_pointer_grouped_lookup_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "projection.direct_second_join_projection="));
		    RequireRowPointerDescriptorLookupStages(stage_counts);
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
	    });
}

TEST_CASE("JIT Q9-like two-join aggregate removes second-join projection source copies", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_like_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       ((i % 8192) * 100003)::BIGINT AS orderkey, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_like_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'nation_' || CAST(i % 7 AS VARCHAR) AS nation_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_like_orders AS "
	                          "SELECT (i * 100003)::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT x.nation, year(o.orderdate) AS o_year, "
	    "       sum(x.extendedprice * (1.00::DECIMAL(15,2) - x.discount) - "
	    "           x.supplycost * x.quantity)::HUGEINT AS sum_profit "
	    "FROM ("
	    "  SELECT b.nation_name AS nation, "
	    "         CAST(l.orderkey AS INTEGER) AS orderkey_i, "
	    "         l.extendedprice AS extendedprice, "
	    "         l.discount AS discount, "
	    "         b.supplycost AS supplycost, "
	    "         l.quantity AS quantity "
	    "  FROM jit_q9_like_l l "
	    "  JOIN jit_q9_like_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	    ") x "
	    "JOIN jit_q9_like_orders o ON x.orderkey_i = o.orderkey "
	    "GROUP BY x.nation, o_year "
	    "ORDER BY x.nation, o_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "projection.direct_second_join_projection=");
	    },
		    [](const ExecutionRegionEvent &event) {
			    const auto runtime_paths = EventJitRuntimePathCounts(event);
			    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
			    const auto stages = EventGeneratedStageCountBreakdown(event);
			    const bool has_compressed_passthrough =
			        StringUtil::Contains(runtime_paths, "projection.direct_between_join_compressed_passthrough_projection=");
			    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
			    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_between_join_projection="));
			    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_second_join_projection="));
			    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_rhs_row_pointer_generated_projection="));
			    REQUIRE(StringUtil::Contains(runtime_paths,
			                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
				    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
				    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
				    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
				    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
				    RequireRowPointerDescriptorLookupStages(stages);
				    REQUIRE(StringUtil::Contains(stages, "op5:aggregate_update.direct_row_pointer_grouped_lookup_update="));
				    if (has_compressed_passthrough) {
					    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_batch_passthrough_projection="));
				    REQUIRE(StringUtil::Contains(stages, "direct_batch_expression.compressed_passthrough="));
				    REQUIRE_FALSE(StringUtil::Contains(stages, "op4:projection.direct_batch_expression.string_compress="));
			    }
			    REQUIRE_FALSE(StringUtil::Contains(
			        stages,
			        "direct_projected_group_payload_update.direct_new_selected_state.append_update.find_or_create_fast.hash="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_existing_update="));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "op4:projection.post_join_direct_remap_batch_projection="));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "op4:projection.post_join_direct_remap_batch_projection_hash="));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "op4:projection.direct_batch_expression.integral_compress="));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "materialize_projection_sources"));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "post_join_batch_append"));
			    REQUIRE_FALSE(StringUtil::Contains(runtime_paths,
			                                       "direct_projected_group_payload_update_unsupported."));
	    });
}

TEST_CASE("JIT Q9-like two-projection between-join chain keeps first join live", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_chain_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       ((i % 8192) * 100003)::BIGINT AS orderkey, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_chain_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'nation_' || CAST(i % 7 AS VARCHAR) AS nation_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q9_chain_orders AS "
	                          "SELECT (i * 100003)::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT x.nation_alias, year(o.orderdate) AS o_year, sum(x.amount)::HUGEINT AS sum_profit "
	    "FROM ("
	    "  SELECT y.nation AS nation_alias, "
	    "         CAST(y.orderkey_raw AS INTEGER) AS orderkey_i, "
	    "         y.extendedprice * (1.00::DECIMAL(15,2) - y.discount) - "
	    "         y.supplycost * y.quantity AS amount "
	    "  FROM ("
	    "    SELECT b.nation_name AS nation, "
	    "           l.orderkey AS orderkey_raw, "
	    "           l.extendedprice AS extendedprice, "
	    "           l.discount AS discount, "
	    "           b.supplycost AS supplycost, "
	    "           l.quantity AS quantity "
	    "    FROM jit_q9_chain_l l "
	    "    JOIN jit_q9_chain_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	    "  ) y "
	    ") x "
	    "JOIN jit_q9_chain_orders o ON x.orderkey_i = o.orderkey "
	    "GROUP BY x.nation_alias, o_year "
	    "ORDER BY x.nation_alias, o_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "projection.direct_between_join_projection=");
	    },
		    [](const ExecutionRegionEvent &event) {
			    const auto runtime_paths = EventJitRuntimePathCounts(event);
			    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
			    const auto stages = EventGeneratedStageCountBreakdown(event);
			    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_between_join_projection="));
			    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_second_join_projection="));
				    REQUIRE(StringUtil::Contains(runtime_paths,
				                                 "aggregate_update.direct_row_pointer_grouped_lookup_update="));
				    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
				    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
				    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
				    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
				    RequireRowPointerDescriptorLookupStages(stages);
				    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
				    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
				    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
			    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "op0:hash_join_probe.materialize_projection_sources"));
			    REQUIRE_FALSE(StringUtil::Contains(stages, "op0:hash_join_probe.materialize_output_fallback"));
		    });
}

TEST_CASE("JIT hash join pair probe preserves bloom-filtered sparse misses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_bloom_l AS "
	                          "SELECT i::BIGINT AS k0, (i * 17)::BIGINT AS k1, i::BIGINT AS v "
	                          "FROM range(300000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_bloom_r AS "
	                          "SELECT (i * 73)::BIGINT AS k0, (i * 73 * 17)::BIGINT AS k1, (i * 3)::BIGINT AS w "
	                          "FROM range(4096) tbl(i)"));

	const string query = "SELECT count(*), sum(l.v + r.w) FROM jit_hash_pair_bloom_l l "
	                     "JOIN jit_hash_pair_bloom_r r ON l.k0 = r.k0 AND l.k1 = r.k1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "4096");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_no_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_no_chain="));
	    });
}

TEST_CASE("JIT hash join pair probe uses selected all-valid no-chain fast path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_selected_l AS "
	                          "SELECT ((i % 10000) * 1000003)::BIGINT AS k0, "
	                          "((i % 10000) * 17)::BIGINT AS k1, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_selected_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k0, (i * 17)::BIGINT AS k1, "
	                          "(i * 3)::BIGINT AS w FROM range(10000) tbl(i)"));

	const string query = "SELECT l.k0, sum(l.v + r.w), sum(l.v - r.w), sum(l.v * 2), sum(r.w * 3), "
	                     "sum(l.v + 42), sum(r.w + 17) FROM jit_hash_pair_selected_l l "
	                     "JOIN jit_hash_pair_selected_r r ON l.k0 = r.k0 AND l.k1 = r.k1 "
	                     "WHERE l.d > DATE '1994-01-01' GROUP BY l.k0 ORDER BY l.k0 LIMIT 3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT hash join pair probe uses all-valid chain fast path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_chain_l AS "
	                          "SELECT (i % 64)::BIGINT AS k0, ((i % 64) * 17)::BIGINT AS k1, i::BIGINT AS v "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_chain_r AS "
	                          "SELECT (i % 64)::BIGINT AS k0, ((i % 64) * 17)::BIGINT AS k1, i::BIGINT AS w "
	                          "FROM range(256) tbl(i)"));

	const string query = "SELECT count(*), sum(l.v + r.w) FROM jit_hash_pair_chain_l l "
	                     "JOIN jit_hash_pair_chain_r r ON l.k0 = r.k0 AND l.k1 = r.k1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "16384");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_flat_all_valid_int64_pair_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT hash join pair probe uses selected all-valid chain fast path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_selected_chain_l AS "
	                          "SELECT (i % 64)::BIGINT AS k0, ((i % 64) * 17)::BIGINT AS k1, i::BIGINT AS v "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_selected_chain_r AS "
	                          "SELECT (i % 64)::BIGINT AS k0, ((i % 64) * 17)::BIGINT AS k1, i::BIGINT AS w "
	                          "FROM range(256) tbl(i)"));

	const string query = "SELECT count(*), sum(l.v + r.w) FROM jit_hash_pair_selected_chain_l l "
	                     "JOIN jit_hash_pair_selected_chain_r r ON l.k0 = r.k0 AND l.k1 = r.k1 "
	                     "WHERE l.k0 + l.k1 > 100";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT hash join probe uses all-valid single-key chain fast path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_probe_l AS "
	                          "SELECT (i % 64)::BIGINT AS k, i::BIGINT AS v FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_probe_r AS "
	                          "SELECT (i % 64)::BIGINT AS k, i::BIGINT AS w FROM range(256) tbl(i)"));

	const string query = "SELECT count(*), sum(r.w) FROM jit_hash_chain_probe_l l "
	                     "JOIN jit_hash_chain_probe_r r ON l.k = r.k WHERE l.k < 32";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT hash join chain probe preserves bloom-filtered sparse misses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_bloom_l AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_bloom_r AS "
	                          "SELECT ((i % 64) * 128 + 7)::BIGINT AS k, i::BIGINT AS w, "
	                          "(i % 2 = 0) AS keep FROM range(512) tbl(i)"));

	const string query = "SELECT count(*), sum(r.w) FROM jit_hash_chain_bloom_l l "
	                     "JOIN (SELECT k, w FROM jit_hash_chain_bloom_r WHERE keep) r ON l.k = r.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "256");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}
