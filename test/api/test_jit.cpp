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
		    return IsSljitRegionEvent(event) && event.phase == "decision" && event.status == "unsupported" &&
		           event.candidate_shape == "filter-projection-sink";
	    },
	    RequireUnsupportedFilterProjectionSinkEvent);
}

TEST_CASE("JIT auto policy uses database-local admission profile", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_policy_small AS "
	                          "SELECT i::BIGINT AS i FROM range(200000) tbl(i)"));

	auto require_auto_skipped_before_graph = [&]() {
		RequireJitEvent(
		    manager,
		    [](const ExecutionRegionEvent &event) {
			    return IsSljitRegionEvent(event) && event.policy_decision == "auto" &&
			           StringUtil::Contains(event.reason, "before graph lowering");
		    },
		    [](const ExecutionRegionEvent &event) {
			    REQUIRE(event.status == "skipped");
			    REQUIRE(event.execution_mode == "unsupported");
			    REQUIRE_FALSE(event.has_admission);
			    REQUIRE_FALSE(event.has_pipeline);
			    REQUIRE_FALSE(event.has_candidate);
			    REQUIRE(event.ir.empty());
			    REQUIRE(StringUtil::Contains(event.reason, "backend has no measured auto admission policy"));
		    });
	};
	auto require_auto_skipped_before_region_lowering = [&]() {
		RequireJitEvent(
		    manager,
		    [](const ExecutionRegionEvent &event) {
			    return IsSljitRegionEvent(event) && event.policy_decision == "auto" &&
			           StringUtil::Contains(event.reason, "before region lowering");
		    },
		    [](const ExecutionRegionEvent &event) {
			    REQUIRE(event.status == "skipped");
			    REQUIRE(event.execution_mode == "unsupported");
			    REQUIRE(event.has_admission);
			    REQUIRE_FALSE(event.admission_rule_present);
			    REQUIRE(event.admission_min_cardinality == 0);
			    REQUIRE_FALSE(event.has_candidate);
			    REQUIRE(event.has_pipeline);
			    REQUIRE(event.backend_analysis_time_us == 0);
			    REQUIRE(StringUtil::Contains(event.reason, "without measured auto admission"));
			    REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
		    });
	};

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 1000)"));
	require_auto_skipped_before_graph();

	ConfigureSljitSettings(con, "force", false, true);
	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 1000)"));

		string admission_key;
		idx_t min_cardinality = 0;
		RequireJitEvent(
		    manager,
		    [](const ExecutionRegionEvent &event) {
			    return IsCompiledSljitRegionEvent(event) && event.has_admission && !event.admission_shape_key.empty() &&
			           event.candidate_estimated_cardinality > 0;
		    },
		    [&](const ExecutionRegionEvent &event) {
			    admission_key = event.admission_shape_key;
			    auto canonical_admission_key = BuildExecutionRegionAdmissionShapeKey("sljit", event.candidate_signature);
			    canonical_admission_key =
			        BuildExecutionRegionAdmissionContextShapeKey(event.candidate_signature, canonical_admission_key);
			    REQUIRE(admission_key == canonical_admission_key);
			    min_cardinality = event.candidate_estimated_cardinality;
		    });
		REQUIRE(!admission_key.empty());
		REQUIRE(min_cardinality > 0);

		REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_add_admission_rule('sljit', 'region', " +
		                          SQLString(admission_key) + ", " + to_string(min_cardinality) + "::UBIGINT" +
		                          ", 'measured-auto-admission:test-profile')"));
		auto rules = con.Query("SELECT count(*) FROM duckdb_jit_admission_rules()");
		REQUIRE_NO_FAIL(*rules);
		REQUIRE(rules->GetValue(0, 0).GetValue<int64_t>() == 1);

	ConfigureSljitSettings(con, "auto", false, true);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 1000)"));

	RequireJitEvent(
	    manager,
	    [&](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.policy_decision == "auto" &&
		           event.admission_shape_key == admission_key;
	    },
	    [&](const ExecutionRegionEvent &event) {
		    REQUIRE(event.admission_rule_present);
		    REQUIRE(event.admission_min_cardinality == min_cardinality);
		    REQUIRE(event.admission_proof == "measured-auto-admission:test-profile");
		    REQUIRE(event.has_admission_score);
		    REQUIRE(event.admission_score >= 0);
		    REQUIRE(StringUtil::Contains(event.reason, "execution_region_policy=auto admits shape="));
		    if (event.code_size == 0) {
			    RequireCompiledFusedOperatorProtocolRegion(event);
		    } else {
			    RequireNativeFusedRegion(event);
			    REQUIRE(StringUtil::Contains(event.reason, "execution-body=generated-machine-code"));
		    }
	    });

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 1000"));
	require_auto_skipped_before_region_lowering();

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_admission_rules()"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=false"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 1000)"));
	require_auto_skipped_before_graph();
}

TEST_CASE("JIT auto admission uses canonical feature identity", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "force", false, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_canonical_probe AS "
	                          "SELECT (i % 4096)::BIGINT AS k, i::BIGINT AS v FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_canonical_build AS "
	                          "SELECT i::BIGINT AS k FROM range(4096) tbl(i)"));

	const string query = "SELECT p.k, p.v, b.k + 1 AS next_k "
	                     "FROM jit_auto_canonical_probe p "
	                     "JOIN jit_auto_canonical_build b ON p.k=b.k "
	                     "WHERE p.v > 1000 AND p.v < 1100";

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query(query));

		string candidate_admission_key;
		string lowered_admission_key;
	idx_t min_cardinality = 0;
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           StringUtil::Contains(event.candidate_signature.feature_shape, "hash-join-operator") &&
		           StringUtil::Contains(event.candidate_signature.feature_shape, "table-scan-source");
	    },
	    [&](const ExecutionRegionEvent &event) {
		    lowered_admission_key = event.admission_shape_key;
		    candidate_admission_key = BuildExecutionRegionAdmissionShapeKey("sljit", event.candidate_signature);
		    candidate_admission_key =
		        BuildExecutionRegionAdmissionContextShapeKey(event.candidate_signature, candidate_admission_key);
		    min_cardinality = event.candidate_estimated_cardinality;
	    });
		REQUIRE(!lowered_admission_key.empty());
		REQUIRE(!candidate_admission_key.empty());
		REQUIRE(lowered_admission_key == candidate_admission_key);
		REQUIRE(min_cardinality > 0);

		REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_admission_rules()"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_add_admission_rule('sljit', 'region', " +
		                          SQLString(candidate_admission_key) + ", " + to_string(min_cardinality) + "::UBIGINT" +
		                          ", 'measured-auto-admission:canonical-feature-identity')"));

		ConfigureSljitSettings(con, "auto", false, true);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query(query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.policy_decision == "auto" &&
		           event.admission_proof == "measured-auto-admission:canonical-feature-identity";
	    },
	    [&](const ExecutionRegionEvent &event) {
		    REQUIRE(event.admission_rule_present);
		    REQUIRE(event.admission_min_cardinality == min_cardinality);
		    REQUIRE(event.has_admission_score);
		    REQUIRE(event.admission_score >= 0);
		    RequireNativeFusedRegion(event);
	    });
}

TEST_CASE("JIT auto policy skips backends without measured auto admission policy", "[api][jit]") {
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

	REQUIRE(backend_ref.region_compile_count == 0);

	RequireNoExpressionJitEvents(manager);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return event.backend_name == "contract_test_auto_reject_jit_backend" && event.target == "region" &&
		           StringUtil::Contains(event.reason, "before graph lowering");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(event.has_candidate);
		    REQUIRE(event.status == "skipped");
		    REQUIRE(event.execution_mode == "unsupported");
		    REQUIRE(event.policy_decision == "auto");
		    REQUIRE(event.decision_time_us >= 0);
		    REQUIRE(event.compile_time_us == 0);
		    REQUIRE(event.code_size == 0);
		    REQUIRE(event.backend_analysis_time_us == 0);
		    REQUIRE_FALSE(event.has_admission);
		    REQUIRE_FALSE(event.admission_rule_present);
		    REQUIRE(StringUtil::Contains(event.reason, "backend has no measured auto admission policy"));
	    });
}

TEST_CASE("JIT auto admission only compiles fused region forms", "[api][jit]") {
	JitTestDatabase test;
	auto &context = test.context;
	auto &manager = test.manager;

	auto backend = make_uniq<AutoMissingExecutionFormAdmissionBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_missing_execution_form_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
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
		           event.target == "region" && event.has_admission &&
		           event.admission_shape_key == "contract:auto-missing-execution-form" && event.status == "skipped" &&
		           event.region_execution_form == "none";
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.execution_mode == "unsupported");
		    REQUIRE(event.policy_decision == "auto");
		    REQUIRE(event.has_admission);
		    REQUIRE(event.admission_shape_key == "contract:auto-missing-execution-form");
		    REQUIRE(event.admission_rule_present);
		    REQUIRE(event.admission_min_cardinality == 0);
		    REQUIRE(event.admission_proof == "contract:auto-missing-execution-form-proof");
		    REQUIRE(event.has_admission_score);
		    REQUIRE(event.compile_time_us == 0);
		    REQUIRE(event.code_size == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "region execution form is not fused"));
		    REQUIRE(StringUtil::Contains(event.reason, "region_execution_form=none"));
		    REQUIRE(StringUtil::Contains(event.reason, "requires=fused"));
	    });
}
