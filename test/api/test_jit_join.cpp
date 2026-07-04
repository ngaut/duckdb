#include "test_jit_helpers.hpp"

using namespace duckdb;

static void RequireDirectRowPointerDescriptorLookupStages(const string &stage_counts) {
	REQUIRE(StringUtil::Contains(stage_counts, "find_or_create_descriptor.hash="));
	REQUIRE(StringUtil::Contains(stage_counts, "find_or_create_descriptor.probe="));
	REQUIRE(StringUtil::Contains(stage_counts, "direct_row_pointer_grouped_targets.lookup="));
	REQUIRE((StringUtil::Contains(stage_counts, "find_or_create_descriptor.emit_targets=") ||
	         StringUtil::Contains(stage_counts, "find_or_create_descriptor.emit_existing_targets=")));
	REQUIRE((StringUtil::Contains(stage_counts, "direct_row_pointer_split_payload.target_existing_update=") ||
	         StringUtil::Contains(stage_counts, "direct_row_pointer_split_payload.target_new_update=") ||
	         StringUtil::Contains(stage_counts, "direct_row_pointer_grouped_target_payload_update=")));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.fill="));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.hash="));
}

static void RequireMaterializedRowPointerDescriptorLookupStages(const string &stage_counts) {
	REQUIRE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.fill="));
	REQUIRE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.lookup_addresses="));
	REQUIRE(StringUtil::Contains(stage_counts, "find_or_create_descriptor_keys.emit_targets="));
	REQUIRE(StringUtil::Contains(stage_counts, "direct_row_pointer_grouped_targets.lookup="));
	REQUIRE((StringUtil::Contains(stage_counts, "direct_row_pointer_split_payload.target_input_order_update=") ||
	         StringUtil::Contains(stage_counts, "direct_row_pointer_grouped_target_payload_update=")));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor.hash="));
	REQUIRE_FALSE(StringUtil::Contains(stage_counts, "find_or_create_descriptor.probe="));
}

TEST_CASE("JIT hash join build protocol compiles only inside generated fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	string lhs_key = "i";
	string rhs_key = "j";
	for (idx_t term_idx = 1; term_idx <= 60; term_idx++) {
		auto term = std::to_string(term_idx);
		lhs_key = "(" + lhs_key + " + (i * " + term + "))";
		rhs_key = "(" + rhs_key + " + (j * " + term + "))";
	}
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_hash_build_l AS SELECT " + lhs_key + "::BIGINT AS i FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_r AS SELECT i::BIGINT AS j FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_l a "
	                        "JOIN (SELECT " +
	                        rhs_key + " AS x FROM jit_hash_build_r WHERE j > 10) b ON a.i=b.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 9989);

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
				REQUIRE(event.runner_cost.native_join_stage_count == 0);
				REQUIRE(event.runner_cost.native_hash_join_build_sink_count == 1);
				REQUIRE(event.runner_cost.saved_work_per_batch > 0);
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

TEST_CASE("JIT CBO charges generated hash-build sink protocol before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, false, false, 10000);
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
		    return EventPhase(event) == "decision" && EventStatus(event) == "skipped" && event.runner_cost.present &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE &&
		           event.runner_cost.native_hash_join_build_sink_count == 1 &&
		           event.runner_cost.generated_stage_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_join_stage_count == 0);
		    REQUIRE(event.runner_cost.native_hash_join_build_sink_count == 1);
		    REQUIRE(event.runner_cost.stateful_protocol_penalty == 720);
		    REQUIRE(event.runner_cost.saved_work_per_batch < 0);
		    REQUIRE(event.runner_cost.selection_reason == "rejected_saved_work_non_positive");
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
	    });
}

TEST_CASE("JIT primitive sequence composes projection filter projection before hash build", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,expression_rewriter'"));
	string lhs_key = "i";
	string rhs_key = "p";
	for (idx_t term_idx = 1; term_idx <= 60; term_idx++) {
		auto term = std::to_string(term_idx);
		lhs_key = "(" + lhs_key + " + (i * " + term + "))";
		rhs_key = "(" + rhs_key + " + (p * " + term + "))";
	}
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preproject_filter_l AS SELECT " + lhs_key +
	                          "::BIGINT AS k FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preproject_filter_r AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS p FROM range(4096) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_preproject_filter_l l "
	                        "JOIN (SELECT x FROM (SELECT k, " +
	                        rhs_key +
	                        " AS x FROM jit_preproject_filter_r) t WHERE k > 10) b "
	                        "ON l.k = b.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 4085);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "compile" && EventStatus(event) == "compiled" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(event.reason, "candidate_shape=projection-filter-projection-sink");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(event.reason,
		                                 "execution:native-sljit-region-projection-filter-projection-hash-join-build"));
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(stage_counts, "op0:projection.post_join_direct_batch_projection=") &&
		           StringUtil::Contains(stage_counts, "op1:filter.selection=") &&
		           StringUtil::Contains(stage_counts, "op2:projection.batch_append=") &&
		           StringUtil::Contains(stage_counts, "op3:hash_join_build.reference_keys=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "op3:hash_join_build.filter_pushdown="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
	    });
}

TEST_CASE("JIT CBO skips low-value hash-build sink pipelines before backend analysis", "[api][jit]") {
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
		    return EventPhase(event) == "decision" && EventStatus(event) == "skipped" && event.runner_cost.present &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE &&
		           event.runner_cost.native_hash_join_build_sink_count == 1 &&
		           event.runner_cost.generated_stage_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireVectorizedCboSkip(event);
		    REQUIRE_FALSE(event.has_candidate);
		    REQUIRE(event.stage_timings.backend_analysis_time_us == 0);
		    REQUIRE(event.runner_cost.native_join_stage_count == 0);
		    REQUIRE(event.runner_cost.native_hash_join_build_sink_count == 1);
		    REQUIRE(event.runner_cost.stateful_protocol_penalty == 720);
		    REQUIRE(event.runner_cost.saved_work_per_batch < 0);
		    REQUIRE(event.runner_cost.selection_reason == "rejected_saved_work_non_positive");
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
	    });
}

TEST_CASE("JIT CBO does not cost nested-loop join protocol as native join work", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET nested_loop_join_threshold=1000000"));
	REQUIRE_NO_FAIL(con.Query("SET merge_join_threshold=1000000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_cost_l AS "
	                          "SELECT i::BIGINT AS i FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_cost_r AS "
	                          "SELECT i::BIGINT AS j FROM range(64) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_nested_cost_l"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_nested_cost_r"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_nested_cost_l l JOIN jit_nested_cost_r r ON l.i < r.j");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2016");

	bool found_nested_loop_physical_cbo = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" || !event.runner_cost.present ||
		    event.runner_cost.input_scope != PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE) {
			continue;
		}
		found_nested_loop_physical_cbo = true;
		REQUIRE(event.runner_cost.native_join_stage_count == 0);
	}
	REQUIRE(found_nested_loop_physical_cbo);
}

TEST_CASE("JIT auto costs hash join regions through planner cost selection", "[api][jit]") {
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
	bool found_costed_hash_join_probe = false;
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
		if (EventPhase(event) == "decision" && (EventStatus(event) == "skipped" || EventStatus(event) == "compiled")) {
			REQUIRE(event.runner_cost.present);
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON)) {
				found_costed_hash_join_probe = true;
				REQUIRE(event.runner_cost.native_join_stage_count > 0);
				REQUIRE(event.runner_cost.generated_stage_count >= event.runner_cost.native_join_stage_count);
				REQUIRE(event.runner_cost.generated_backend_stage_count >= event.runner_cost.native_join_stage_count);
				REQUIRE(event.runner_cost.admission_class != "none");
				REQUIRE_FALSE(event.runner_cost.selection_reason == "rejected_native_operator_work_uncosted");
			}
		}
		if (EventPhase(event) == "decision" && EventStatus(event) != "compiled") {
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
		}
	}
	REQUIRE(found_hash_join_decision);
	REQUIRE(found_costed_hash_join_probe);
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

TEST_CASE("JIT hash join probe batches selected reference source explicitly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=65536"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=65536"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=65536"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
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
		           StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                "hash_join_probe.row_pointer_selection_reference=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.source_batch_boundary="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.fast_regular_probe_"));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "hash_join_probe.source_materialize_boundary="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.source_batch_append="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.source_contract_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.filtered_input_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_breakdown, "hash_join_probe.generated_regular_probe"));
	    });
}

TEST_CASE("JIT first hash join native tail uses source batch boundary recipe", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_tail_l AS "
	                          "SELECT ((i % 4096) * 1000003)::BIGINT AS k, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_tail_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 5)::BIGINT AS w "
	                          "FROM range(4096) tbl(i)"));

	const string query = "SELECT l.k, l.v + r.w AS projected_value, r.w "
	                     "FROM jit_hash_native_tail_l l "
	                     "JOIN jit_hash_native_tail_r r ON l.k = r.k "
	                     "WHERE l.d > DATE '1996-01-01'";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_hash_native_tail_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_hash_native_tail_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.source_batch_boundary=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.source_batch_boundary="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.fast_regular_probe_"));
		    REQUIRE(StringUtil::Contains(stages, "op0:hash_join_probe.source_batch_boundary_append="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op0:hash_join_probe.source_batch_append="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.source_contract_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.filtered_input_batch="));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_hash_native_tail_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_hash_native_tail_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_hash_native_tail_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_hash_native_tail_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT hash join filter ungrouped aggregate avoids final join materialization", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
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
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_hash_join_filtered_payload_update="));
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.residual_predicate="));
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_hash_join_filtered_payload_input="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
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
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
	    });
}

TEST_CASE("JIT perfect hash join grouped aggregate direct-projects input-vector keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_direct_agg_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_direct_agg_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_direct_agg_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_direct_agg_dim"));

	const string query = "SELECT f.k, sum(f.price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM jit_perfect_direct_agg_fact f "
	                     "JOIN jit_perfect_direct_agg_dim d ON f.k=d.k "
	                     "GROUP BY f.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_direct_agg_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_direct_agg_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_direct_agg_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_direct_agg_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_direct_agg_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_direct_agg_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_projection_input_vector_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.generated_perfect_probe_function="));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT perfect hash join grouped aggregate composes cast-chain input-vector keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_cast_direct_agg_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_cast_direct_agg_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_cast_direct_agg_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_cast_direct_agg_dim"));

	const string query = "SELECT CAST(f.k AS INTEGER) AS k_i, sum(f.price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM jit_perfect_cast_direct_agg_fact f "
	                     "JOIN jit_perfect_cast_direct_agg_dim d ON f.k=d.k "
	                     "GROUP BY CAST(f.k AS INTEGER)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_cast_direct_agg_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_cast_direct_agg_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_cast_direct_agg_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_cast_direct_agg_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_cast_direct_agg_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_cast_direct_agg_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_projection_input_vector_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.generated_perfect_probe_function="));
		    REQUIRE((
		        StringUtil::Contains(
		            runtime_paths,
		            "aggregate_update.direct_projection_aggregate_group.input_vector_cast_key0_unchecked=") ||
		        StringUtil::Contains(
		            runtime_paths, "aggregate_update.direct_projection_aggregate_group.input_vector_cast_unchecked=")));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT hash join grouped aggregate proves probe-key casts from matched build keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_matched_key_proof_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_matched_key_proof_r AS "
	                          "SELECT (i * 100003)::INTEGER AS join_k, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS shippriority "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT CAST(l.k AS INTEGER) AS orderkey, r.orderdate, r.shippriority::TINYINT AS shippriority, "
	    "       sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	    "FROM jit_matched_key_proof_l l "
	    "JOIN jit_matched_key_proof_r r ON l.k = r.join_k "
	    "WHERE l.shipdate > DATE '1994-01-01' "
	    "GROUP BY 1, 2, 3 "
	    "ORDER BY orderkey, r.orderdate, shippriority "
	    "LIMIT 64";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_matched_key_proof_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_matched_key_proof_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_matched_key_proof_output "
	                      "   EXCEPT SELECT * FROM jit_matched_key_proof_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_matched_key_proof_reference "
	                      "   EXCEPT SELECT * FROM jit_matched_key_proof_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector_cast_key0_"
		                                 "unchecked="));
		    REQUIRE_FALSE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_aggregate_group.input_vector_cast_key0_checked="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.row_pointer_field="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_aggregate_group.row_pointer_field_cast_unchecked="));
		    REQUIRE_FALSE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_aggregate_group.row_pointer_field_cast="));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
	    });
}

TEST_CASE("JIT row-pointer aggregate accepts identity-cast build-side group keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='expression_rewriter,join_order,build_side_probe_side,"
	                          "partial_aggregate_pushdown,compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_identity_group_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       (i % 13)::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_identity_group_r AS "
	                          "SELECT (i * 100003)::INTEGER AS k, "
	                          "       (i % 257)::INTEGER AS g, "
	                          "       'bucket_' || CAST(i % 17 AS VARCHAR) AS label "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT CAST(r.g AS INTEGER) AS g, r.label, sum(l.v) AS total "
	                     "FROM jit_identity_group_l l "
	                     "JOIN jit_identity_group_r r ON l.k = r.k "
	                     "GROUP BY 1, 2 "
	                     "ORDER BY g, r.label "
	                     "LIMIT 64";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_identity_group_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_identity_group_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_identity_group_output "
	                      "   EXCEPT SELECT * FROM jit_identity_group_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_identity_group_reference "
	                      "   EXCEPT SELECT * FROM jit_identity_group_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.row_pointer_field="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths,
		                                       "projection.direct_projection_row_pointer_aggregate_unsupported."
		                                       "group0_source="));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects probe-side grouped keys", "[api][jit]") {
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
		                                "aggregate_update.direct_projection_input_vector_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_reference_payload_view="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects split probe-side keys", "[api][jit]") {
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
		                                "aggregate_update.direct_projection_input_vector_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector="));
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_projected_group_payload_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_new_grouped_primitive_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects RHS keys", "[api][jit]") {
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

	const string query =
	    "SELECT r.orderkey::INTEGER AS orderkey, r.orderdate, r.shippriority::TINYINT AS shippriority, "
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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_reference_payload_view="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate separates leading-key hash collisions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_leading_key_hash_l AS "
	                          "SELECT ((i % 512) * 100003)::BIGINT AS k, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS amount "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_leading_key_hash_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       42::BIGINT AS g0, "
	                          "       (i % 4)::BIGINT AS g1, "
	                          "       ((i // 4) % 4)::BIGINT AS g2, "
	                          "       ((i // 16) % 4)::BIGINT AS g3 "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT r.g0, r.g1, r.g2, r.g3, sum(l.amount)::HUGEINT AS total_amount "
	                     "FROM jit_leading_key_hash_l l "
	                     "JOIN jit_leading_key_hash_r r ON l.k = r.join_k "
	                     "GROUP BY r.g0, r.g1, r.g2, r.g3 "
	                     "ORDER BY r.g0, r.g1, r.g2, r.g3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    RequireDirectRowPointerDescriptorLookupStages(stages);
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate preaggregates consecutive descriptor runs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_preagg_l AS "
	                          "SELECT (((i // 4) % 2048) * 100003)::BIGINT AS k, "
	                          "       CAST(1 + (i % 29) AS DECIMAL(15,2)) AS amount "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_preagg_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       i::BIGINT AS custkey, "
	                          "       ('cust-' || CAST(i AS VARCHAR))::VARCHAR AS cname "
	                          "FROM range(2048) tbl(i)"));

	const string query = "SELECT r.custkey, r.cname, sum(l.amount)::HUGEINT AS total_amount "
	                     "FROM jit_row_pointer_preagg_l l "
	                     "JOIN jit_row_pointer_preagg_r r ON l.k = r.join_k "
	                     "GROUP BY r.custkey, r.cname "
	                     "ORDER BY r.custkey";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_row_pointer_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.preaggregated_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
		    REQUIRE(StringUtil::Contains(stages, "aggregate_update.local_preaggregate_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(stages, "direct_row_pointer_grouped_targets.lookup="));
		    REQUIRE(StringUtil::Contains(
		        stages, "aggregate_update.direct_row_pointer_preaggregated_primitive_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate preaggregates mixed input-vector descriptor runs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_row_pointer_preagg_l AS "
	                          "SELECT (((i // 4) % 2048) * 100003)::BIGINT AS k, "
	                          "       (i % 17)::BIGINT AS amount "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_row_pointer_preagg_r AS "
	                          "SELECT (i * 100003)::INTEGER AS join_k, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS priority "
	                          "FROM range(2048) tbl(i)"));

	const string query = "SELECT CAST(l.k AS INTEGER) AS orderkey, r.orderdate, r.priority::TINYINT AS priority, "
	                     "       sum(l.amount) AS total_amount "
	                     "FROM jit_mixed_row_pointer_preagg_l l "
	                     "JOIN jit_mixed_row_pointer_preagg_r r ON l.k = r.join_k "
	                     "GROUP BY orderkey, r.orderdate, priority "
	                     "ORDER BY orderkey, r.orderdate, priority";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector_cast_key0_"
		                                 "unchecked="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.row_pointer_field="));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_row_pointer_preaggregated_grouped_primitive_update="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths,
		                                       "aggregate_update.direct_input_vector_grouped_update_unsupported."));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.preaggregated_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
		    REQUIRE(StringUtil::Contains(stages, "aggregate_update.local_preaggregate_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(stages, "direct_row_pointer_grouped_targets.lookup="));
		    REQUIRE(StringUtil::Contains(
		        stages, "aggregate_update.direct_row_pointer_preaggregated_primitive_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects variable-width RHS keys", "[api][jit]") {
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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_reference_payload_view="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.projection_source="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

static void RequireComposedTwoJoinProjectionChainEvent(ExecutionRegionManager &manager) {
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.direct_projection_input_vector_grouped_update=") &&
		           (StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference=") ||
		            StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference="));
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    const bool has_regular_selection =
		        StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference=");
		    const bool has_perfect_selection =
		        StringUtil::Contains(boundaries, "hash_join_probe.perfect_selection_reference=");
		    REQUIRE((has_regular_selection || has_perfect_selection));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector="));
		    REQUIRE(StringUtil::Contains(stages, "projection.batch_append="));
		    REQUIRE(StringUtil::Contains(stages, "projection.post_join_direct_reference_payload_view="));
		    REQUIRE(StringUtil::Contains(stages, "projection.post_join_direct_computed_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.projected_group_payload_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "projection.direct_between_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "projection.direct_second_join_projection="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "projection.direct_rhs_row_pointer_generated_projection="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.direct_row_pointer_reference="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.direct_selection_reference="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
	    });
}

TEST_CASE("JIT two-join grouped aggregate composes mixed VARCHAR projection chain", "[api][jit]") {
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

	const string query = "SELECT x.nation, year(o.orderdate) AS o_year, sum(x.amount)::HUGEINT AS sum_profit "
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

	RequireComposedTwoJoinProjectionChainEvent(manager);
}

TEST_CASE("JIT Q5-like two-join aggregate direct-projects compressed VARCHAR groups", "[api][jit]") {
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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_row_pointer_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "projection.direct_reference_payload_view="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.preaggregated_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.pending_probe_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_remap_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.copied_post_join_batch="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT Q7-like aggregate preaggregates compressed interleaved descriptor groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	REQUIRE_NO_FAIL(con.Query("LOAD tpch"));
	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=0.01)"));
	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));

	const string query = "SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue "
	                     "FROM ("
	                     "  SELECT n1.n_name AS supp_nation, "
	                     "         n2.n_name AS cust_nation, "
	                     "         extract(year FROM l_shipdate) AS l_year, "
	                     "         l_extendedprice * (1 - l_discount) AS volume "
	                     "  FROM supplier, lineitem, orders, customer, nation n1, nation n2 "
	                     "  WHERE s_suppkey = l_suppkey "
	                     "    AND o_orderkey = l_orderkey "
	                     "    AND c_custkey = o_custkey "
	                     "    AND s_nationkey = n1.n_nationkey "
	                     "    AND c_nationkey = n2.n_nationkey "
	                     "    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') "
	                     "      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) "
	                     "    AND l_shipdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1996-12-31' AS date)"
	                     ") AS shipping "
	                     "GROUP BY supp_nation, cust_nation, l_year "
	                     "ORDER BY supp_nation, cust_nation, l_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 4);

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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    REQUIRE_FALSE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_aggregate_group.input_vector_cast="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.row_pointer_field_cast="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector="));
		    REQUIRE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_projection_row_pointer_grouped_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_row_pointer_preaggregated_grouped_primitive_update="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.direct_row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(stages, "aggregate_update.local_preaggregate_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.preaggregated_row_pointer_primitive_groups="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_post_join_batch_projection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.state_address_selection_new_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate probes compressed string keys as single fields", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_string_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_string_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       CASE WHEN i % 2 = 0 THEN 'MAIL' ELSE 'SHIP' END AS shipmode "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT r.shipmode, sum(l.p - l.d)::HUGEINT AS revenue "
	                     "FROM jit_row_pointer_string_l l "
	                     "JOIN jit_row_pointer_string_r r ON l.k = r.join_k "
	                     "GROUP BY r.shipmode ORDER BY r.shipmode";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

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
		                                "aggregate_update.direct_projection_row_pointer_grouped_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.row_pointer_field_cast="));
		    REQUIRE(StringUtil::Contains(stages, "find_or_create_row_pointer_single_field.probe="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "find_or_create_descriptor.hash="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "find_or_create_descriptor.probe="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.row_pointer_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.row_pointer_grouped_lookup_update="));
	    });
}

TEST_CASE("JIT two-join grouped aggregate materializes wide projection tail", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       ((i % 8192) * 100003)::BIGINT AS orderkey, "
	                          "       (i % 2)::BIGINT AS g0, "
	                          "       (i % 3)::BIGINT AS g1, "
	                          "       (i % 5)::BIGINT AS g2, "
	                          "       (i % 7)::BIGINT AS g3, "
	                          "       (i % 11)::BIGINT AS g4, "
	                          "       (i % 13)::BIGINT AS g5, "
	                          "       (i % 17)::BIGINT AS g6, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'nation_' || CAST(i % 7 AS VARCHAR) AS nation_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_orders AS "
	                          "SELECT (i * 100003)::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT x.nation, year(o.orderdate) AS o_year, "
	                     "       x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6, "
	                     "       sum(x.extendedprice * (1.00::DECIMAL(15,2) - x.discount) - "
	                     "           x.supplycost * x.quantity)::HUGEINT AS total_payload "
	                     "FROM ("
	                     "  SELECT b.nation_name AS nation, "
	                     "         CAST(l.orderkey AS INTEGER) AS orderkey_i, "
	                     "         l.g0, l.g1, l.g2, l.g3, l.g4, l.g5, l.g6, "
	                     "         l.extendedprice AS extendedprice, "
	                     "         l.discount AS discount, "
	                     "         b.supplycost AS supplycost, "
	                     "         l.quantity AS quantity "
	                     "  FROM jit_wide_group_l l "
	                     "  JOIN jit_wide_group_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	                     ") x "
	                     "JOIN jit_wide_group_orders o ON x.orderkey_i = o.orderkey "
	                     "GROUP BY x.nation, o_year, x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6 "
	                     "ORDER BY x.nation, o_year, x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6";
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

	RequireComposedTwoJoinProjectionChainEvent(manager);
}

TEST_CASE("JIT Q9-like two-join aggregate composes second-join projection chain", "[api][jit]") {
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

	const string query = "SELECT x.nation, year(o.orderdate) AS o_year, "
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

	RequireComposedTwoJoinProjectionChainEvent(manager);
}

TEST_CASE("JIT single mark filter aggregate uses boundary primitive", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_mark_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS item_key, "
	                          "       (i % 19)::INTEGER AS group_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_mark_blocked AS "
	                          "SELECT ((i * 5) % 4096)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_single_mark_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_single_mark_blocked b"
	                     ") "
	                     "GROUP BY group_id "
	                     "ORDER BY group_id";
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
		                                "filter.direct_mark_probe_nonmatch_selection=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "hash_join_probe.generated_regular_probe_mark_nonmatch_flat_all_valid_"
		                                 "function="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "filter.direct_mark_probe_nonmatch_selection="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.primitive_grouped_preaggregated_count_star_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_nonmatch_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_lhs_selected_view="));
		    REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.preaggregated_count_star_groups="));
		    REQUIRE(StringUtil::Contains(stages,
		                                 "op0:hash_join_probe.generated_regular_probe_mark_nonmatch_flat_all_valid_"
		                                 "function="));
		    REQUIRE(StringUtil::Contains(stages, "op2:projection.batch_append="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "filter.direct_mark_nonmatch_selection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_flags="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_vector="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "projection.direct_mark_projection="));
	    });
}

TEST_CASE("JIT mark match filter emits selected boundary without marker flags", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_match_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i % 7)::INTEGER AS group_id "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_match_allowed AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_mark_match_fact f "
	                     "WHERE EXISTS ("
	                     "  SELECT 1 FROM jit_mark_match_allowed a "
	                     "  WHERE a.item_key = f.item_key"
	                     ") "
	                     "GROUP BY group_id "
	                     "ORDER BY group_id";
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
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "filter.direct_mark_probe_match_selection=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.generated_regular_probe_mark_match"));
		    REQUIRE(StringUtil::Contains(runtime_paths, "filter.direct_mark_probe_match_selection="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_match_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_lhs_selected_view="));
		    REQUIRE(StringUtil::Contains(stages, "op0:hash_join_probe.generated_regular_probe_mark_match"));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths, "filter.direct_mark_selection="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_flags="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_vector="));
	    });
}

TEST_CASE("JIT mark filter projection native tail uses boundary primitive", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_native_tail_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i % 23)::INTEGER AS group_id "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_native_tail_blocked AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, item_key + 100 AS projected_key "
	                     "FROM jit_mark_native_tail_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_mark_native_tail_blocked b"
	                     ")";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_native_tail_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_native_tail_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                "hash_join_probe.mark_filter_lhs_selected_view=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "hash_join_probe.generated_regular_probe_mark_nonmatch_flat_all_valid_"
		                                 "function="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "filter.direct_mark_probe_nonmatch_selection="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_nonmatch_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_lhs_selected_view="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_flags="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_vector="));
		    REQUIRE(StringUtil::Contains(stages,
		                                 "op0:hash_join_probe.generated_regular_probe_mark_nonmatch_flat_all_valid_"
		                                 "function="));
		    REQUIRE(StringUtil::Contains(stages, "op2:projection.batch_append="));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_mark_native_tail_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_native_tail_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_mark_native_tail_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_native_tail_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT mark filter projection delimiter sink slices reference projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_supplier AS "
	                          "SELECT i::BIGINT AS suppkey, ('S' || i::VARCHAR) AS name, "
	                          "       (i % 8)::BIGINT AS nationkey "
	                          "FROM range(256) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_nation AS "
	                          "SELECT i::BIGINT AS nationkey, "
	                          "       CASE WHEN i=1 THEN 'SAUDI ARABIA' ELSE ('N' || i::VARCHAR) END AS name "
	                          "FROM range(8) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_orders AS "
	                          "SELECT i::BIGINT AS orderkey, "
	                          "       CASE WHEN i % 3 = 0 THEN 'F' ELSE 'O' END AS status "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_lineitem AS "
	                          "SELECT (i % 8192)::BIGINT AS orderkey, "
	                          "       ((i / 8192)::BIGINT % 256) AS suppkey, "
	                          "       DATE '1994-01-01' + ((i * 7) % 1000)::INTEGER AS receiptdate, "
	                          "       DATE '1994-01-01' + ((i * 5) % 1000)::INTEGER AS commitdate "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT s.name, count(*) AS numwait "
	                     "FROM jit_delim_supplier s, jit_delim_lineitem l1, jit_delim_orders o, jit_delim_nation n "
	                     "WHERE s.suppkey = l1.suppkey "
	                     "  AND o.orderkey = l1.orderkey "
	                     "  AND o.status = 'F' "
	                     "  AND l1.receiptdate > l1.commitdate "
	                     "  AND s.nationkey = n.nationkey "
	                     "  AND n.name = 'SAUDI ARABIA' "
	                     "  AND EXISTS ("
	                     "      SELECT * FROM jit_delim_lineitem l2 "
	                     "      WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey"
	                     "  ) "
	                     "  AND NOT EXISTS ("
	                     "      SELECT * FROM jit_delim_lineitem l3 "
	                     "      WHERE l3.orderkey = l1.orderkey AND l3.suppkey <> l1.suppkey "
	                     "        AND l3.receiptdate > l3.commitdate"
	                     "  ) "
	                     "GROUP BY s.name "
	                     "ORDER BY numwait DESC, s.name "
	                     "LIMIT 100";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_delim_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_delim_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "delim_join_sink.selected_hash_join_delim_sink=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.source_batch_boundary="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "delim_join_sink.selected_hash_join_delim_sink="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.selected_delim_sink_input="));
		    REQUIRE(StringUtil::Contains(stages, "op0:hash_join_probe.source_batch_boundary_append="));
		    REQUIRE(StringUtil::Contains(stages, "op2:hash_join_probe.materialize_selected_delim_sink_input="));
		    REQUIRE(StringUtil::Contains(stages, "op3:delim_join_sink.sink_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op2:hash_join_probe.materialize_output="));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "delim_join_sink.direct_reference_projection_delim_sink=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "hash_join_probe.generated_regular_probe_mark_match"));
		    REQUIRE(StringUtil::Contains(runtime_paths, "filter.direct_mark_probe_match_selection="));
		    REQUIRE(StringUtil::Contains(runtime_paths, "delim_join_sink.direct_reference_projection_delim_sink="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_match_selection_reference="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_lhs_selected_view="));
		    REQUIRE(StringUtil::Contains(stages, "op2:projection.reference_slice="));
		    REQUIRE(StringUtil::Contains(stages, "op3:delim_join_sink.sink_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op2:projection.batch_append="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_filter_vector="));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_delim_reference "
	                      "     EXCEPT ALL SELECT * FROM jit_delim_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_delim_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_delim_reference)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT mark filter reference aggregate slices selected projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_reference_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i * 1000000000000)::BIGINT AS group_id "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_reference_blocked AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(128) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_mark_reference_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_mark_reference_blocked b"
	                     ") "
	                     "GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_reference_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_reference_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.primitive_projected_count_star_row_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(stages, "op3:aggregate_update.primitive_projected_count_star_row_groups="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "op2:projection.batch_append="));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_mark_reference_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_reference_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_mark_reference_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_reference_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT two-join mark distinct aggregate uses row-pointer distinct backend", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_filter_fact AS "
	                          "SELECT ((i % 2048) * 1000003)::BIGINT AS partkey, "
	                          "       (i % 4096)::BIGINT AS suppkey "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_filter_part AS "
	                          "SELECT (i * 1000003)::BIGINT AS partkey, "
	                          "       'Brand#' || CAST(i % 17 AS VARCHAR) AS brand, "
	                          "       'TYPE_' || CAST(i % 31 AS VARCHAR) AS type, "
	                          "       (1 + (i % 11))::INTEGER AS size "
	                          "FROM range(2048) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_filter_bad_supplier AS "
	                          "SELECT (i * 13)::BIGINT AS suppkey "
	                          "FROM range(256) tbl(i)"));

	const string query = "SELECT p.brand, p.type, p.size, count(DISTINCT f.suppkey) AS supplier_count "
	                     "FROM jit_mark_filter_fact f "
	                     "JOIN jit_mark_filter_part p ON f.partkey = p.partkey "
	                     "WHERE f.suppkey NOT IN ("
	                     "  SELECT b.suppkey FROM jit_mark_filter_bad_supplier b"
	                     ") "
	                     "GROUP BY p.brand, p.type, p.size "
	                     "ORDER BY p.brand, p.type, p.size";
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

	bool found_distinct_backend = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                   "aggregate_update.row_pointer_distinct_count_pointer_direct_update="));
		REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                   "aggregate_update.distinct_count_pointer_direct_update="));
		if (EventStatus(event) != "executed") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                   "aggregate_update.distinct_count_pointer_selected_payload_update="));
		REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                   "aggregate_update.distinct_count_pointer_group_key_update_unsupported."));
		if (!StringUtil::Contains(EventJitRuntimePathCounts(event),
		                          "aggregate_update.distinct_count_pointer_group_key_update=") &&
		    !StringUtil::Contains(EventJitRuntimePathCounts(event),
		                          "aggregate_update.distinct_count_pointer_row_pointer_group_key_update=")) {
			continue;
		}
		found_distinct_backend = true;
	}
	REQUIRE(found_distinct_backend);
}

TEST_CASE("JIT distinct aggregate uses global pair set for high-payload probe groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_dense_distinct_fact AS "
	                          "SELECT ((i % 2048) * 1000003)::BIGINT AS partkey, "
	                          "       (i % 4)::INTEGER AS group_id, "
	                          "       i::BIGINT AS suppkey "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_dense_distinct_part AS "
	                          "SELECT (i * 1000003)::BIGINT AS partkey "
	                          "FROM range(2048) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_dense_distinct_blocked AS "
	                          "SELECT (i * 13)::BIGINT AS suppkey "
	                          "FROM range(256) tbl(i)"));

	const string query = "SELECT f.group_id, count(DISTINCT f.suppkey) AS supplier_count "
	                     "FROM jit_dense_distinct_fact f "
	                     "JOIN jit_dense_distinct_part p ON f.partkey = p.partkey "
	                     "WHERE f.suppkey NOT IN ("
	                     "  SELECT b.suppkey FROM jit_dense_distinct_blocked b"
	                     ") "
	                     "GROUP BY f.group_id "
	                     "ORDER BY f.group_id";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dense_distinct_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dense_distinct_actual AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_dense_distinct_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_dense_distinct_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_dense_distinct_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_dense_distinct_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.distinct_count_pointer_global_payload_set=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.distinct_count_pointer_global_payload_set="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.distinct_count_pointer_row_pointer_group_key_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_distinct_count_pointer_row_pointer_update="));
		    REQUIRE(StringUtil::Contains(stages, "aggregate_update.distinct_count_pointer_global_payload_set="));
		    REQUIRE_FALSE(StringUtil::Contains(runtime_paths,
		                                       "aggregate_update.distinct_count_pointer_selected_payload_update="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.distinct_count_pointer_payload_set_update="));
	    });
}

TEST_CASE("JIT Q16 selected hash-join distinct aggregate uses mixed row-pointer backend", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	REQUIRE_NO_FAIL(con.Query("LOAD tpch"));
	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=0.01)"));

	const string query = "SELECT "
	                     "    p_brand, "
	                     "    p_type, "
	                     "    p_size, "
	                     "    count(DISTINCT ps_suppkey) AS supplier_cnt "
	                     "FROM partsupp, part "
	                     "WHERE p_partkey = ps_partkey "
	                     "  AND p_brand <> 'Brand#45' "
	                     "  AND p_type NOT LIKE 'MEDIUM POLISHED%' "
	                     "  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) "
	                     "  AND ps_suppkey NOT IN ("
	                     "      SELECT s_suppkey "
	                     "      FROM supplier "
	                     "      WHERE s_comment LIKE '%Customer%Complaints%'"
	                     "  ) "
	                     "GROUP BY p_brand, p_type, p_size "
	                     "ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_q16_selected_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_q16_selected_actual AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_q16_selected_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_q16_selected_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_q16_selected_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_q16_selected_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.distinct_count_pointer_row_pointer_group_key_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto boundaries = EventJitMaterializationBoundaryCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.distinct_count_pointer_row_pointer_group_key_update="));
		    REQUIRE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.direct_projection_distinct_count_pointer_row_pointer_update="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_input.projection_output="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_input.hash_join_lhs_input="));
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.direct_projection_aggregate_group.input_vector="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.distinct_count_pointer_payload_set_update="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.selected_mark_probe_input="));
		    REQUIRE(StringUtil::Contains(boundaries, "hash_join_probe.mark_nonmatch_selection_reference="));
		    REQUIRE_FALSE(StringUtil::Contains(boundaries, "hash_join_probe.mark_flags="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_reference_projection="));
		    REQUIRE(StringUtil::Contains(boundaries, "projection.direct_post_join_computed_projection="));
		    REQUIRE(StringUtil::Contains(boundaries,
		                                 "aggregate_update.distinct_count_pointer_row_pointer_group_key_update="));
		    REQUIRE(StringUtil::Contains(stages, "projection.post_join_direct_reference_projection="));
		    REQUIRE(StringUtil::Contains(stages, "projection.post_join_direct_computed_projection="));
		    REQUIRE(StringUtil::Contains(stages, "aggregate_update.distinct_count_pointer_row_pointer_group_key_update."
		                                         "direct_row_pointer_grouped_targets."));
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "projection.reference_view_handoff=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(stages, "projection.reference_view_handoff="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "projection.batch_append="));
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.runner_cost.present &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "distinct_count_pointer");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.native_distinct_count_pointer_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.generated_distinct_count_pointer_aggregate_update_count > 0);
	    });
}

TEST_CASE("JIT Q9-like two-projection between-join chain composes primitive boundaries", "[api][jit]") {
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

	const string query = "SELECT x.nation_alias, year(o.orderdate) AS o_year, sum(x.amount)::HUGEINT AS sum_profit "
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

	RequireComposedTwoJoinProjectionChainEvent(manager);
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

TEST_CASE("JIT hash join probe consumes generated-filter selected reference input through flat fast path",
          "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_full_vector_selected_l AS "
	                          "SELECT ((i % 10000) * 1000003)::BIGINT AS k, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_full_vector_selected_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT l.k, sum(l.v + r.w), sum(l.v - r.w), sum(l.v * 2), sum(r.w * 3), "
	                     "sum(l.v + 42), sum(r.w + 17) FROM jit_hash_full_vector_selected_l l "
	                     "JOIN jit_hash_full_vector_selected_r r ON l.k = r.k "
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
		                                "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown, "op0:filter.selection="));
		    REQUIRE(StringUtil::Contains(runtime_breakdown, "op1:projection.batch_append="));
		    REQUIRE(StringUtil::Contains(runtime_breakdown,
		                                 "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain="));
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
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    const auto has_single_key_chain =
		        StringUtil::Contains(runtime_breakdown,
		                             "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain=") ||
		        StringUtil::Contains(runtime_breakdown,
		                             "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_chain=");
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && has_single_key_chain;
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    const auto has_single_key_chain =
		        StringUtil::Contains(runtime_breakdown,
		                             "hash_join_probe.fast_regular_probe_flat_all_valid_single_key_chain=") ||
		        StringUtil::Contains(runtime_breakdown,
		                             "hash_join_probe.fast_regular_probe_selected_all_valid_single_key_chain=");
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(has_single_key_chain);
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
