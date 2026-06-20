#include "test_jit_contract_backends.hpp"

#include <type_traits>

using namespace duckdb;

static_assert(std::is_constructible<ExecutionRegionCompilationInput, ClientContext &, const ExecutionRegionIR &,
                                    const ExecutionRegionCandidate &>::value,
              "execution region backends must compile from core-owned region IR candidates");
static_assert(
    !std::is_constructible<ExecutionRegionCompilationInput, ClientContext &, const ExecutionRegionIR &>::value,
    "execution region backend input must include an explicit selected candidate");
static_assert(!std::is_constructible<ExecutionRegionCompilationInput, ClientContext &, Pipeline &>::value,
              "execution region backend input must not expose DuckDB executor internals");
static_assert(std::is_same<decltype(&ExecutionRegionKernel::CanExecuteFullPipeline),
                           bool (ExecutionRegionKernel::*)() const>::value,
              "execution region full-pipeline kernels must advertise the full-pipeline executable ABI explicitly");
static_assert(std::is_same<decltype(&ExecutionRegionKernel::TryExecuteFullPipeline),
                           bool (ExecutionRegionKernel::*)(ExecutionRegionRuntime &, ExecutionRegionResult &)>::value,
              "execution region full-pipeline kernels must execute through the JIT full-pipeline runtime ABI, not "
              "DuckDB executor internals");

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

	auto missing_backend = test.con.Query("SET jit_backend='missing_jit_backend'");
	REQUIRE(missing_backend->HasError());
	REQUIRE(StringUtil::Contains(missing_backend->GetError(),
	                             "Execution region backend \"missing_jit_backend\" is not registered"));
}

TEST_CASE("Execution region manager records compile events from the selected backend", "[api][jit]") {
	JitTestDatabase test;

	ConfigureSljit(test.con, "force", true, true);

	ClearJitTrace(test.manager);
	REQUIRE_NO_FAIL(test.con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	RequireNoExpressionJitEvents(test.manager);
	RequireJitEvent(
	    test.manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "decision" && EventStatus(event) == "unsupported" &&
		           CandidateHasStructure(event, 1, 1, ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK);
	    },
	    RequireUnsupportedFilterProjectionSinkEvent);
}

TEST_CASE("JIT auto policy selects high-work materialization through planner cost selection", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
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
		           EventRequestedPolicy(event) == "auto" &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit > event.runner_cost.required_benefit);
		    REQUIRE(event.runner_cost.startup_cost > 0);
		    REQUIRE(event.runner_cost.accelerated_stage_count > 0);
		    REQUIRE(event.runner_cost.expression_cost > 0);
		    REQUIRE(event.runner_cost.batches > 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    RequireCompiledGeneratedRegion(event);
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
		    return EventTarget(event) == "region" && EventStatus(event) == "skipped" && EventRequestedPolicy(event) == "auto" &&
		           event.has_candidate && event.candidate_estimated_cardinality == 16;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.candidate_estimated_cardinality == 16);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	    });

	for (auto &event : manager.GetEvents()) {
		const bool compiled_auto_region =
		    EventTarget(event) == "region" && EventStatus(event) == "compiled" && EventRequestedPolicy(event) == "auto";
		REQUIRE_FALSE(compiled_auto_region);
	}
}

TEST_CASE("JIT force compiles branchy native expressions", "[api][jit]") {
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

	ConfigureSljitSettings(con, "force", false, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_branchy_expression_output AS "
	                          "SELECT " +
	                          expression + " AS v FROM jit_auto_branchy_expression_input"));
	auto result = con.Query("SELECT sum(v) FROM jit_auto_branchy_expression_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && EventRequestedPolicy(event) == "force";
	    },
	    [](const ExecutionRegionEvent &event) { RequireCompiledGeneratedRegion(event); });
}

TEST_CASE("JIT force compiles high-intensity ungrouped aggregate updates", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "force");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_force_aggregate_cbo_input AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b, (i + 2)::BIGINT AS c, "
	                          "(i + 3)::BIGINT AS d FROM range(1000000) tbl(i)"));

	const string expression = "CAST(((((a * 3) + (b * 5) - (c * 7) + (d * 11)) * 13) "
	                          "+ (((a - b + c) * 17) - ((a + d) * 19)) "
	                          "+ (((b - c + d) * 23) - ((a - d) * 29))) AS BIGINT)";
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(" + expression + ") FROM jit_force_aggregate_cbo_input");
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && EventRequestedPolicy(event) == "force" &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.candidate_estimated_cardinality == 1000000);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(EventExecutionMode(event) == "native");
		    REQUIRE(EventExecutionBody(event) == "generated-machine-code");
		    RequireCandidateStructure(event, 0, 1, ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE);
		    RequireCompiledGeneratedRegion(event);
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
	    [](const ExecutionRegionEvent &event) {
		    return EventTarget(event) == "region" && EventStatus(event) == "skipped" && EventRequestedPolicy(event) == "auto" &&
		           event.has_candidate;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	    });
}

TEST_CASE("JIT auto policy selects vectorized through one DuckDB execution path", "[api][jit]") {
	JitTestDatabase test;
	auto &context = test.context;
	auto &manager = test.manager;

	auto backend = make_uniq<AutoRejectedCountingBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_reject_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	Settings::Set<JitTraceDecisionsSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	REQUIRE_NO_FAIL(test.con.Query("CREATE TEMP TABLE jit_auto_reject_input AS "
	                               "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(test.con.Query("SELECT i + 1 FROM jit_auto_reject_input WHERE i > 0"));

	REQUIRE(backend_ref.region_analyze_count > 0);
	REQUIRE(backend_ref.region_compile_count == 0);

	RequireNoExpressionJitEvents(manager);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return event.backend_name == "contract_test_auto_reject_jit_backend" && EventTarget(event) == "region" &&
		           EventStatus(event) == "skipped" && EventRequestedPolicy(event) == "auto";
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(EventStatus(event) == "skipped");
		    REQUIRE(EventExecutionMode(event) == "unsupported");
		    REQUIRE(EventRequestedPolicy(event) == "auto");
		    REQUIRE(event.decision_time_us >= 0);
		    REQUIRE(event.compile_time_us == 0);
		    REQUIRE(event.code_size == 0);
		    REQUIRE(event.backend_analysis_time_us >= 0);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "before region lowering"));
	    });
}

TEST_CASE("JIT compiled execution only accepts fused region forms", "[api][jit]") {
	JitTestDatabase test;
	auto &context = test.context;
	auto &manager = test.manager;

	auto backend = make_uniq<AutoMissingExecutionFormBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_missing_execution_form_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("force"));
	REQUIRE_NO_FAIL(test.con.Query("CREATE TEMP TABLE jit_auto_non_fused_input AS "
	                               "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(test.con.Query("SELECT i + 1 AS j FROM jit_auto_non_fused_input WHERE i > 0"));

	REQUIRE(backend_ref.region_analyze_count > 0);
	REQUIRE(backend_ref.region_compile_count == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return event.backend_name == "contract_test_auto_missing_execution_form_jit_backend" &&
		           EventTarget(event) == "region" && EventStatus(event) == "skipped" && EventRegionExecutionForm(event) == "none" &&
		           StringUtil::Contains(event.reason, "contract:auto-missing-execution-form");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "unsupported");
		    REQUIRE(EventRequestedPolicy(event) == "force");
		    REQUIRE(event.compile_time_us == 0);
		    REQUIRE(event.code_size == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "region execution form is not fused"));
		    REQUIRE(StringUtil::Contains(event.reason, "region_execution_form=none"));
		    REQUIRE(StringUtil::Contains(event.reason, "requires=fused"));
	    });
}
