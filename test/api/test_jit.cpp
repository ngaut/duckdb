#include "test_jit_helpers.hpp"

using namespace duckdb;

namespace {

class UnitTestExecutionRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "unit_test_jit_backend";
	}

	string Description() const override {
		return "unit test execution region backend";
	}
};

} // namespace

TEST_CASE("Execution region manager registers and selects database-local backends", "[api][jit]") {
	JitTestDatabase test;

	test.manager.RegisterBackend(make_uniq<UnitTestExecutionRegionBackend>());

	bool found_backend = false;
	for (auto &backend : test.manager.GetBackends(&test.context)) {
		if (backend.name != "unit_test_jit_backend") {
			continue;
		}
		found_backend = true;
		REQUIRE(backend.description == "unit test execution region backend");
		REQUIRE(backend.available);
		REQUIRE(!backend.supports_regions);
	}
	REQUIRE(found_backend);

	REQUIRE_NO_FAIL(test.con.Query("SET jit_backend='unit_test_jit_backend'"));

	bool selected_backend = false;
	for (auto &backend : test.manager.GetBackends(&test.context)) {
		if (backend.name == "unit_test_jit_backend") {
			selected_backend = backend.selected;
		}
	}
	REQUIRE(selected_backend);
}

TEST_CASE("JIT CBO keeps generated and native protocol stage costs partitioned", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.native_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.generated_stage_benefit = 10;
	parameters.native_operator_stage_benefit = 100;
	parameters.startup_base_cost = 0;
	parameters.startup_margin_basis_points = 0;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 1);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO requires native protocol or full-pipeline proof for native operator regions", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.generated_stage_count = 1;
	input.native_join_stage_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.generated_stage_benefit = 100;
	parameters.startup_base_cost = 0;
	parameters.startup_margin_basis_points = 0;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.full_pipeline_benefit = 100;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.full_pipeline);
	REQUIRE(profile.native_join_stage_count == 1);
	REQUIRE(profile.saved_work_per_batch == 200);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits explicit full-pipeline proof without duplicate stage credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.full_pipeline_benefit = 100;
	parameters.startup_base_cost = 0;
	parameters.startup_margin_basis_points = 0;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_join_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 0);
	REQUIRE(profile.native_sort_stage_count == 0);
	REQUIRE(profile.full_pipeline);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT auto policy selects high-work materialization through planner cost selection", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_cost_input AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b, "
	                          "(i + 2)::BIGINT AS c, (i + 3)::BIGINT AS d "
	                          "FROM range(1000000) tbl(i)"));

	const string query = "CREATE TEMP TABLE jit_auto_cost_output AS "
	                     "SELECT CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) "
	                     "+ (((a - b + c) * 17) - ((a + d) * 19)) "
	                     "+ (((b - c + d) * 23) - ((a - d) * 29)) "
	                     "+ (((a * 31) - (b * 37) + (c * 41) - (d * 43)) "
	                     "+ (((a + b + c + d) * 47) - ((a - c) * 53)))) AS BIGINT) AS v "
	                     "FROM jit_auto_cost_input";

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS jit_auto_cost_output"));
	REQUIRE_NO_FAIL(con.Query(query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit > event.runner_cost.required_benefit);
		    REQUIRE(event.runner_cost.startup_cost > 0);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.expression_cost > 0);
		    REQUIRE(event.runner_cost.batches > 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "before region lowering"));
	    });
}

TEST_CASE("JIT auto planner cost skips cheap projections", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_cost_projection_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_cost_projection_output AS "
	                          "SELECT i + 1 AS j FROM jit_auto_cost_projection_input"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventStatus(event) == "skipped" && event.has_candidate &&
		           event.candidate_estimated_cardinality == 16;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.candidate_estimated_cardinality == 16);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	    });

	for (auto &event : manager.GetEvents()) {
		const bool compiled_auto_region = EventStatus(event) == "compiled";
		REQUIRE_FALSE(compiled_auto_region);
	}
}

TEST_CASE("JIT auto planner skips region graph when CBO already selects vectorized", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_fast_cost_input AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i + 1) FROM jit_auto_fast_cost_input WHERE i > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2014");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" &&
		           event.blocker == "duckdb_selected_vectorized" && event.runner_cost.present;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.pipeline_cbo_time_us >= 0);
		    REQUIRE(event.graph_build_time_us == 0);
		    REQUIRE(event.candidate_cbo_time_us == 0);
		    REQUIRE(event.ir_lowering_time_us == 0);
		    REQUIRE(event.backend_analysis_time_us == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "before region graph"));
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
	    });

	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventStatus(event) != "compiled");
	}
}

TEST_CASE("JIT auto planner skips region graph when pipeline has no costed acceleration", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_no_cost_left AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_no_cost_right AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_auto_no_cost_left l JOIN jit_auto_no_cost_right r USING (i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "64");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" &&
		           event.blocker == "duckdb_selected_vectorized" &&
		           StringUtil::Contains(event.reason, "region_graph=skipped");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.graph_build_time_us >= 0);
		    REQUIRE(event.candidate_cbo_time_us == 0);
		    REQUIRE(event.ir_lowering_time_us == 0);
		    REQUIRE(event.backend_analysis_time_us == 0);
	    });
}

TEST_CASE("JIT diagnostic tracing analyzes fused contract boundary regions", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCompilation(con, false, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM range(1000) tbl(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "499500");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "unsupported" && event.has_candidate &&
		           StringUtil::Contains(event.reason, "region-lowering");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(StringUtil::Contains(event.reason, "source-contract-blocker"));
	    });

	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || event.blocker != "fused_region_contract_has_boundaries") {
			continue;
		}
		REQUIRE(StringUtil::Contains(event.reason, "region-lowering"));
	}
}

TEST_CASE("JIT auto planner cost skips default uncalibrated aggregate codegen", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_default_aggregate_input AS "
	                          "SELECT (i % 5)::BIGINT AS i FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM jit_auto_default_aggregate_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2000000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.saved_work_per_batch >= event.runner_cost.expression_cost);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects vectorized physical runner"));
	    });

	for (auto &event : manager.GetEvents()) {
		const bool compiled_auto_region = EventStatus(event) == "compiled";
		REQUIRE_FALSE(compiled_auto_region);
	}
}

TEST_CASE("JIT auto planner cost uses configurable CBO settings", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=1000000000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_configured_cbo_input AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b, "
	                          "(i + 2)::BIGINT AS c, (i + 3)::BIGINT AS d "
	                          "FROM range(500000) tbl(i)"));

	const string query = "CREATE TEMP TABLE jit_auto_configured_cbo_output AS "
	                     "SELECT CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) "
	                     "+ (((a - b + c) * 17) - ((a + d) * 19)) "
	                     "+ (((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT) AS v "
	                     "FROM jit_auto_configured_cbo_input";

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query(query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.startup_cost >= 1000000000);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit < event.runner_cost.required_benefit);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects vectorized physical runner"));
	    });
}

TEST_CASE("JIT auto compiles branchy native expressions", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_branchy_expression_input AS "
	                          "SELECT i::BIGINT AS a, (i % 97)::BIGINT AS b, (i % 193)::BIGINT AS c, "
	                          "(i % 389)::BIGINT AS d, (i % 769)::BIGINT AS e FROM range(1000000) tbl(i)"));

	const string expression = "CASE WHEN (((a + b * 3 - c * 5 + d * 7 - e * 11) > 0) "
	                          "AND ((a - b + c + 1) < (d + e + 3))) "
	                          "THEN (a * 13 + b * 17 - c * 19 + d * 23 - e * 29) "
	                          "ELSE (a - b + c - d + e) END";
	auto reference = con.Query("SELECT sum(" + expression + ") FROM jit_auto_branchy_expression_input");
	REQUIRE_NO_FAIL(*reference);

	ConfigureSljitForCompilationSettings(con, false, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_branchy_expression_output AS "
	                          "SELECT " +
	                          expression + " AS v FROM jit_auto_branchy_expression_input"));
	auto result = con.Query("SELECT sum(v) FROM jit_auto_branchy_expression_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsCompiledSljitRegionEvent(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });
}

TEST_CASE("JIT auto policy selects high-work ungrouped aggregate updates through planner cost selection",
          "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_aggregate_cbo_input AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b, (i + 2)::BIGINT AS c, "
	                          "(i + 3)::BIGINT AS d FROM range(500000) tbl(i)"));

	const string expression = "CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) "
	                          "+ (((a - b + c) * 17) - ((a + d) * 19)) "
	                          "+ (((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT)";
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(" + expression + ") FROM jit_auto_aggregate_cbo_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "19750163000000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.candidate_estimated_cardinality == 500000);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit > event.runner_cost.required_benefit);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count > 0);
		    REQUIRE(event.runner_cost.expression_cost > 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		    RequireCandidateStructure(event, 0, 1, ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE);
		    RequireGeneratedMachineCodeRegion(event);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "aggregate_update");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "aggregate_update"));
	    });
}

TEST_CASE("JIT auto planner cost skips tiny filtered sums", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_filtered_sum_small AS "
	                          "SELECT i::BIGINT AS a, (i % 17)::BIGINT AS b FROM range(1024) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_auto_filtered_sum_small WHERE a >= 10 AND a < 1024");
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) { return EventStatus(event) == "skipped" && event.has_candidate; },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	    });
}
