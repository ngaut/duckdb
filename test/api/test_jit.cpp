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

class UnitTestGpuExecutionRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "unit_test_gpu_backend";
	}

	string Description() const override {
		return "unit test GPU execution region backend";
	}

	ExecutionRunnerKind RunnerKind() const override {
		return ExecutionRunnerKind::COMPILED_GPU;
	}
};

static string MakeRepeatedIntegerExpression(const string &column_name, idx_t terms) {
	string result = column_name;
	for (idx_t term = 0; term < terms; term++) {
		result += " + ((" + column_name + " * " + to_string(term + 3) + ") - " + to_string(term) + ")";
	}
	return "CAST((" + result + ") AS BIGINT)";
}

static string MakeRepeatedFloatExpression(const string &column_name, idx_t operations) {
	string result = column_name;
	for (idx_t op_idx = 0; op_idx < operations; op_idx++) {
		string op;
		switch (op_idx % 4) {
		case 0:
			op = "+ " + to_string(op_idx + 1) + ".0::FLOAT";
			break;
		case 1:
			op = "* 1.0001::FLOAT";
			break;
		case 2:
			op = "- " + to_string((op_idx / 2) + 1) + ".0::FLOAT";
			break;
		default:
			op = "/ 1.0003::FLOAT";
			break;
		}
		result = "(" + result + " " + op + ")";
	}
	return result;
}

static void CreateJitStatefulSortInput(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_stateful_sort_input AS "
	                          "SELECT (i % 1024)::INTEGER AS k, "
	                          "'customer-' || ((i % 1024)::VARCHAR) AS name, "
	                          "i::BIGINT AS v FROM range(100000) tbl(i)"));
}

static unique_ptr<QueryResult> RunJitStatefulSortQuery(Connection &con) {
	return con.Query("CREATE TEMP TABLE jit_stateful_sort_output AS "
	                 "SELECT k, name, s FROM ("
	                 "    SELECT k, name, sum(v) AS s "
	                 "    FROM jit_stateful_sort_input "
	                 "    GROUP BY k, name"
	                 ") grouped ORDER BY s DESC LIMIT 20");
}

static void RunPersistentJitDirectAppendSQL(const string &db_path, const string &sql, bool enable_direct_append_jit) {
	DuckDB db(db_path);
	Connection con(db);
	LoadSljit(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	if (!enable_direct_append_jit) {
		REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		REQUIRE_NO_FAIL(con.Query(sql));
		return;
	}
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query(sql));
}

#if defined(__APPLE__)
static bool IsMetalRegionEvent(const ExecutionRegionEvent &event) {
	return event.backend_name == "jit_metal";
}

static bool IsCompiledMetalRegionEvent(const ExecutionRegionEvent &event) {
	return IsMetalRegionEvent(event) && EventStatus(event) == "compiled";
}

static bool MetalBackendAvailable(ClientContext &context, ExecutionRegionManager &manager) {
	bool found_metal_backend = false;
	bool available = false;
	for (auto &backend : manager.GetBackends(&context)) {
		if (backend.name != "jit_metal") {
			continue;
		}
		found_metal_backend = true;
		REQUIRE(backend.runner_kind == ExecutionRunnerKind::COMPILED_GPU);
		available = backend.available && backend.supports_regions;
	}
	REQUIRE(found_metal_backend);
	return available;
}

static void ConfigureMetalGpuSettings(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=32000"));
	REQUIRE_NO_FAIL(con.Query("SET gpu_cbo_generated_stage_benefit=1000000"));
	REQUIRE_NO_FAIL(con.Query("SET gpu_cbo_full_pipeline_benefit=1000000"));
	REQUIRE_NO_FAIL(con.Query("SET gpu_cbo_startup_base_cost=0"));
}

static void RequireExecutedMetalProjection(ExecutionRegionManager &manager, idx_t output_rows) {
	RequireJitEvent(
	    manager,
	    [output_rows](const ExecutionRegionEvent &event) {
		    return IsMetalRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == output_rows;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "gpu");
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "metal.projection"));
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "metal.projection=1"));
	    });
}
#endif

} // namespace

TEST_CASE("Execution region manager registers and selects database-local backends", "[api][jit]") {
	JitTestDatabase test;

	test.manager.RegisterBackend(make_uniq<UnitTestExecutionRegionBackend>());
	test.manager.RegisterBackend(make_uniq<UnitTestGpuExecutionRegionBackend>());

	bool found_backend = false;
	bool found_gpu_backend = false;
	for (auto &backend : test.manager.GetBackends(&test.context)) {
		if (backend.name == "unit_test_jit_backend") {
			found_backend = true;
			REQUIRE(backend.description == "unit test execution region backend");
			REQUIRE(backend.runner_kind == ExecutionRunnerKind::COMPILED_VECTORIZED);
			REQUIRE(backend.available);
			REQUIRE(!backend.supports_regions);
		}
		if (backend.name == "unit_test_gpu_backend") {
			found_gpu_backend = true;
			REQUIRE(backend.description == "unit test GPU execution region backend");
			REQUIRE(backend.runner_kind == ExecutionRunnerKind::COMPILED_GPU);
			REQUIRE(backend.available);
			REQUIRE(!backend.supports_regions);
		}
	}
	REQUIRE(found_backend);
	REQUIRE(found_gpu_backend);

	REQUIRE_NO_FAIL(test.con.Query("SET jit_backend='unit_test_gpu_backend'"));
	string backend_name;
	auto gpu_backend = test.manager.SelectBackend(test.context, backend_name, ExecutionRunnerKind::COMPILED_GPU);
	REQUIRE(gpu_backend);
	REQUIRE(backend_name == "unit_test_gpu_backend");

	REQUIRE_NO_FAIL(test.con.Query("SET jit_backend='unit_test_jit_backend'"));

	bool selected_backend = false;
	for (auto &backend : test.manager.GetBackends(&test.context)) {
		if (backend.name == "unit_test_jit_backend") {
			selected_backend = backend.selected;
		}
	}
	REQUIRE(selected_backend);
}

TEST_CASE("JIT CBO can select a GPU physical runner when GPU benefit wins", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 4096;
	input.expression_cost = 100;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 1;
	parameters.gpu_runner_available = true;
	parameters.gpu_generated_stage_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_accelerated_runner);
	REQUIRE(profile.selected_gpu_runner);
	REQUIRE_FALSE(profile.selected_compiled_vectorized_runner);
	REQUIRE(profile.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
	REQUIRE(profile.gpu_runner_benefit > profile.compiled_vectorized_runner_benefit);
	REQUIRE(profile.accelerated_runner_benefit == profile.gpu_runner_benefit);
	REQUIRE(profile.gpu_transfer_cost == 0);
}

TEST_CASE("JIT CBO keeps CPU JIT when GPU transfer cost dominates", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.expression_cost = 100;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 100;
	parameters.gpu_runner_available = true;
	parameters.gpu_generated_stage_benefit = 200;
	parameters.gpu_transfer_cost_per_batch = 1000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_accelerated_runner);
	REQUIRE(profile.selected_compiled_vectorized_runner);
	REQUIRE_FALSE(profile.selected_gpu_runner);
	REQUIRE(profile.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
	REQUIRE(profile.gpu_transfer_cost == 1000);
	REQUIRE(profile.gpu_net_benefit < profile.compiled_vectorized_net_benefit);
}

#if defined(__APPLE__)
TEST_CASE("JIT auto policy runs supported projections on Apple Metal GPU", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_auto_input AS "
	                          "SELECT i::BIGINT AS i FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_auto_output AS "
	                          "SELECT i + 7 AS j FROM jit_metal_auto_input"));

	auto result = con.Query("SELECT count(*), sum(j) FROM jit_metal_auto_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 5000650000LL);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledMetalRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventPhase(event) == "compile");
		    REQUIRE(EventExecutionMode(event) == "gpu");
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.selected_gpu_runner);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.candidate_estimated_cardinality == 100000);
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-gpu physical runner"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsMetalRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed";
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "gpu");
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "metal.projection"));
		    const auto unbatched_projection_count =
		        (event.output_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "metal.projection=1"));
		    REQUIRE(unbatched_projection_count > 1);
	    });
}

TEST_CASE("JIT Metal projection supports FLOAT arithmetic", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_float_input AS "
	                          "SELECT i::FLOAT AS x FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_float_output AS "
	                          "SELECT ((x + 7.0::FLOAT) * 2.0::FLOAT) / 4.0::FLOAT AS y "
	                          "FROM jit_metal_float_input"));

	auto result = con.Query("SELECT count(*), sum(y)::DOUBLE FROM jit_metal_float_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->GetValue(1, 0).GetValue<double>() == 2500325000.0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledMetalRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "gpu");
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
	    });
	RequireExecutedMetalProjection(manager, 100000);
}

TEST_CASE("JIT Metal projection linearizes deep FLOAT expression trees", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_deep_float_input AS "
	                          "SELECT i::FLOAT AS x FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto expression = MakeRepeatedFloatExpression("x", 160);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_deep_float_output AS "
	                          "SELECT " +
	                          expression + " AS y FROM jit_metal_deep_float_input"));

	auto result = con.Query("SELECT count(*), count(y) FROM jit_metal_deep_float_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 10000);

	RequireExecutedMetalProjection(manager, 10000);
}

TEST_CASE("JIT Metal projection supports INT64-backed DECIMAL add/subtract", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_decimal_input AS "
	                          "SELECT (i % 100000)::DECIMAL(12,2) AS d FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_decimal_output AS "
	                          "SELECT d + 7.25::DECIMAL(12,2) AS y FROM jit_metal_decimal_input"));

	auto result = con.Query("SELECT count(*), sum(y) FROM jit_metal_decimal_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->GetValue(1, 0).ToString() == "5000675000.00");

	RequireExecutedMetalProjection(manager, 100000);
}

TEST_CASE("JIT Metal projection reports DECIMAL overflow", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_decimal_overflow_input AS "
	                          "SELECT 999999999999999999::DECIMAL(18,0) AS d"));

	ClearJitTrace(manager, true);
	auto overflow = con.Query("CREATE TEMP TABLE jit_metal_decimal_overflow_output AS "
	                          "SELECT d + 1::DECIMAL(18,0) AS y FROM jit_metal_decimal_overflow_input");
	REQUIRE_FAIL(overflow);
	REQUIRE(StringUtil::Contains(overflow->GetError(), "Overflow in Metal DECIMAL projection"));

	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) { return IsCompiledMetalRegionEvent(event); });
}
#endif

TEST_CASE("JIT CBO rejects native operator stages without generated stage credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.native_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 10;
	parameters.native_operator_stage_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_aggregate_stage_count = 0;
	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_join_stage_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits generated native fusion from quantified benefit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2100122;
	input.expression_cost = 1398;
	input.generated_stage_count = 4;
	input.native_join_stage_count = 2;
	input.native_aggregate_stage_count = 1;
	input.native_grouped_aggregate_stage_count = 1;
	input.blocked_hash_aggregate_lookup_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1;
	parameters.startup_base_cost = 32000;
	parameters.startup_margin_basis_points = 5000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.generated_expression_work == 1398);
	REQUIRE(profile.generated_stage_work == 4);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 1504);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.startup_cost == 32000);
	REQUIRE(profile.required_benefit == 48000);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 1600;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated_native_fusion|generated_stage_benefit");
	REQUIRE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 3;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 2;
	input.native_sort_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO only scores native protocol work after generated admission", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 300000;
	input.native_join_stage_count = 1;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.native_operator_stage_benefit = 1000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 0;
	input.native_aggregate_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 1;
	input.native_aggregate_stage_count = 0;
	input.expression_cost = 1000;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	parameters.generated_stage_benefit = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 1000);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == 1281);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 800000;
	input.expression_cost = 0;
	input.generated_stage_count = 0;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	parameters.generated_stage_benefit = 0;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 30000;
	parameters.vectorized_parallelism = 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 2000000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO scores native-stage fusion uniformly after generated admission", "[api][jit]") {
	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 32000;
	parameters.startup_margin_basis_points = 0;

	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 6226410;
	input.expression_cost = 211;
	input.generated_stage_count = 5;
	input.native_join_stage_count = 4;
	input.native_aggregate_stage_count = 1;
	input.native_grouped_aggregate_stage_count = 1;
	input.blocked_hash_aggregate_lookup_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.source_filter_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows < 1000000);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 5120);
	REQUIRE(profile.stateful_protocol_penalty == 2944);
	REQUIRE(profile.saved_work_per_batch == 2392);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 1346398;
	input.expression_cost = 154;
	input.generated_stage_count = 3;
	input.native_join_stage_count = 5;
	input.native_aggregate_stage_count = 0;
	input.native_grouped_aggregate_stage_count = 0;
	input.blocked_hash_aggregate_lookup_count = 0;
	input.uses_scan_filters = false;
	input.source_filter_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows > 1000000);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 5120);
	REQUIRE(profile.stateful_protocol_penalty == 3600);
	REQUIRE(profile.saved_work_per_batch == 1677);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 6001215;
	input.expression_cost = 45;
	input.generated_stage_count = 1;
	input.native_join_stage_count = 1;
	input.native_aggregate_stage_count = 1;
	input.native_grouped_aggregate_stage_count = 1;
	input.blocked_hash_aggregate_lookup_count = 1;
	input.uses_scan_filters = true;
	input.source_filter_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows < 1000000);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 2048);
	REQUIRE(profile.stateful_protocol_penalty == 784);
	REQUIRE(profile.saved_work_per_batch == 1310);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges grouped aggregate fusion against parallel vectorized baseline", "[api][jit]") {
	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1024;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 30000;
	parameters.startup_margin_basis_points = 0;

	PhysicalRunnerCostInput q13_shape;
	q13_shape.estimated_cardinality = 4619408;
	q13_shape.expression_cost = 174;
	q13_shape.generated_stage_count = 6;
	q13_shape.native_join_stage_count = 1;
	q13_shape.native_aggregate_stage_count = 3;
	q13_shape.native_grouped_aggregate_stage_count = 3;
	q13_shape.blocked_hash_aggregate_lookup_count = 3;
	q13_shape.full_pipeline = true;
	q13_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	q13_shape.has_accelerated_work = true;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(q13_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 3532);
	REQUIRE(profile.selected_accelerated_runner);

	parameters.vectorized_parallelism = 12;
	profile = DuckDBCostModel::SelectPhysicalRunner(q13_shape, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput q13_small_grouped_shape;
	q13_small_grouped_shape.estimated_cardinality = 302394;
	q13_small_grouped_shape.expression_cost = 79;
	q13_small_grouped_shape.generated_stage_count = 3;
	q13_small_grouped_shape.native_join_stage_count = 1;
	q13_small_grouped_shape.native_aggregate_stage_count = 1;
	q13_small_grouped_shape.native_grouped_aggregate_stage_count = 1;
	q13_small_grouped_shape.blocked_hash_aggregate_lookup_count = 1;
	q13_small_grouped_shape.full_pipeline = true;
	q13_small_grouped_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	q13_small_grouped_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(q13_small_grouped_shape, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput q10_shape;
	q10_shape.estimated_cardinality = 20386135;
	q10_shape.expression_cost = 1312;
	q10_shape.generated_stage_count = 7;
	q10_shape.native_join_stage_count = 3;
	q10_shape.native_aggregate_stage_count = 1;
	q10_shape.native_grouped_aggregate_stage_count = 1;
	q10_shape.blocked_hash_aggregate_lookup_count = 1;
	q10_shape.full_pipeline = true;
	q10_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	q10_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(q10_shape, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput q13_generated_shape;
	q13_generated_shape.estimated_cardinality = 3023933;
	q13_generated_shape.expression_cost = 39;
	q13_generated_shape.generated_stage_count = 3;
	q13_generated_shape.full_pipeline = true;
	q13_generated_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	q13_generated_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(q13_generated_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 78);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput q10_generated_shape;
	q10_generated_shape.estimated_cardinality = 3000000;
	q10_generated_shape.expression_cost = 78;
	q10_generated_shape.generated_stage_count = 6;
	q10_generated_shape.full_pipeline = true;
	q10_generated_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	q10_generated_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(q10_generated_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 156);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO scales startup against parallel vectorized baseline", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 2000;
	input.expression_cost = 199;
	input.generated_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1;
	parameters.startup_base_cost = 32000;
	parameters.startup_margin_basis_points = 5000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated");
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 200);
	REQUIRE(profile.startup_cost == 32000);
	REQUIRE(profile.required_benefit == 48000);
	REQUIRE(profile.selected_accelerated_runner);

	parameters.vectorized_parallelism = 12;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.startup_cost == 384000);
	REQUIRE(profile.required_benefit == 576000);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO does not use physical pipeline scope as full-pipeline proof", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
	REQUIRE(profile.admission_class == "full_pipeline");
	REQUIRE(profile.full_pipeline_work == 100);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.selected_accelerated_runner);

	input.input_scope = PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges hash join build sink protocol separately from native probe work", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 400000;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_hash_join_build_sink_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 104;
	input.generated_stage_count = 4;
	input.native_join_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	parameters.full_pipeline_benefit = 0;
	parameters.generated_stage_benefit = 1024;
	parameters.native_operator_stage_benefit = 1024;

	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.generated_expression_work == 104);
	REQUIRE(profile.generated_stage_work == 104);
	REQUIRE(profile.native_operator_work == 1024);
	REQUIRE(profile.stateful_protocol_penalty == 1440);
	REQUIRE(profile.saved_work_per_batch == -208);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO does not fund generated backend stages through native hash join build sink", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO discounts DuckDB-owned scan-filter rows before admitting native work", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 1500304;
	input.expression_cost = 169;
	input.generated_stage_count = 2;
	input.materialization_elision_count = 1;
	input.native_join_stage_count = 1;
	input.native_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.source_filter_count = 2;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1;
	parameters.startup_base_cost = 32000;
	parameters.startup_margin_basis_points = 5000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows < input.estimated_cardinality);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.uses_scan_filters = false;
	input.native_join_stage_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits explicit full-pipeline proof without duplicate stage credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_join_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 0);
	REQUIRE(profile.native_sort_stage_count == 0);
	REQUIRE(profile.full_pipeline);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO does not count native-contract projection glue as accelerated work", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 1956412;
	input.expression_cost = 389;
	input.generated_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.full_pipeline_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.full_pipeline);
	REQUIRE(profile.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE);
	REQUIRE(profile.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.accelerated_runner_benefit == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::HIGH_COST_PROJECTION;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 93831;
	input.expression_cost = 2130;
	input.generated_stage_count = 2;
	PhysicalRunnerCostParameters q7_parameters;
	q7_parameters.compiled_vectorized_runner_available = true;
	q7_parameters.generated_stage_benefit = 1;
	q7_parameters.startup_base_cost = 32000;
	q7_parameters.startup_margin_basis_points = 5000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, q7_parameters);
	REQUIRE(profile.batches < 128);
	REQUIRE(profile.saved_work_per_batch == 2132);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated|generated_stage_benefit");
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO requires backend ownership before funding stateful native source-sink glue", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 1380556;
	input.expression_cost = 64;
	input.generated_stage_count = 1;
	input.native_join_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.generated_backend_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_operator_work == 1024);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selection_reason ==
	        "admitted_admission_class:generated_native_fusion|generated_stage_benefit|native_operator_stage_benefit");
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO separates generated distinct count-pointer backend from native distinct penalty", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.generated_distinct_count_pointer_aggregate_update_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_aggregate_stage_count == 0);
	REQUIRE(profile.native_distinct_count_pointer_aggregate_stage_count == 0);
	REQUIRE(profile.generated_distinct_count_pointer_aggregate_update_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 4096);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated|generated_stage_benefit");
	REQUIRE(profile.selected_accelerated_runner);

	input.generated_distinct_count_pointer_aggregate_update_count = 0;
	input.native_distinct_count_pointer_aggregate_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_distinct_count_pointer_aggregate_stage_count == 1);
	REQUIRE(profile.generated_distinct_count_pointer_aggregate_update_count == 0);
	REQUIRE(profile.stateful_protocol_penalty == 8192);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_distinct_count_pointer_aggregate_stage_count = 0;
	input.generated_distinct_count_pointer_aggregate_update_count = 1;
	input.native_join_stage_count = 2;
	input.materialization_elision_count = 1;
	parameters.native_operator_stage_benefit = 4096;
	parameters.materialization_elision_benefit = 4096;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 1440);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits configured grouped aggregate work without synthetic full-pipeline penalty", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.expression_cost = 100;
	input.generated_stage_count = 3;
	input.native_aggregate_stage_count = 1;
	input.native_grouped_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 4096;
	parameters.full_pipeline_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 100);
	REQUIRE(profile.generated_stage_work == 100);
	REQUIRE(profile.native_operator_work == 4096);
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 4296);
	REQUIRE(profile.selected_accelerated_runner);

	input.expression_cost = 10000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 24096);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT auto policy selects high-work materialization through planner cost selection", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	ConfigureJitDecisionTrace(con);
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
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_cost_projection_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_cost_projection_output AS "
	                          "SELECT i + 1 AS j FROM jit_auto_cost_projection_input"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           event.runner_cost.rows == 16;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(event.has_candidate);
		    RequireVectorizedCboSkip(event);
		    RequirePipelineCboOnlyTiming(event);
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
	    });

	for (auto &event : manager.GetEvents()) {
		const bool compiled_auto_region = EventStatus(event) == "compiled";
		REQUIRE_FALSE(compiled_auto_region);
	}
}

TEST_CASE("SLJIT native projection handles many FLOAT expressions sharing sources", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_many_float_projection_input AS "
	                          "SELECT i::FLOAT AS x, (i * 0.5)::FLOAT AS y FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_many_float_projection_output AS "
	                        "SELECT (x + 1.0::FLOAT) AS a, "
	                        "(x * 1.0001::FLOAT) AS b, "
	                        "(y - 3.0::FLOAT) AS c, "
	                        "(x + y) AS d, "
	                        "(x - y) AS e, "
	                        "(y * 1.25::FLOAT) AS f, "
	                        "(x / 1.5::FLOAT) AS g, "
	                        "(y + 9.0::FLOAT) AS h "
	                        "FROM jit_many_float_projection_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), count(a), count(h), sum(a)::DOUBLE, sum(h)::DOUBLE "
	                       "FROM jit_many_float_projection_output");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->GetValue(3, 0).GetValue<double>() == Approx(5000050000.0));
	REQUIRE(check->GetValue(4, 0).GetValue<double>() == Approx(2500875000.0));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(event.candidate_traits.arithmetic_projection_count == 8);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 100000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "op0:projection");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "op0:projection"));
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                 "op0:projection.direct_materialize_generated"));
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "op1:append_sink"));
	    });
}

TEST_CASE("SLJIT direct FLOAT materialization crosses row group boundaries", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

	auto db_path = TestCreatePath("jit_direct_append_rollover.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS rg (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE rg.jit_direct_append_rollover_input AS "
	                          "SELECT i::FLOAT AS x, (i * 0.5)::FLOAT AS y FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TABLE rg.jit_direct_append_rollover_output AS "
	                        "SELECT (x + 1.0::FLOAT) AS a, "
	                        "(x * 1.0001::FLOAT) AS b, "
	                        "(y - 3.0::FLOAT) AS c, "
	                        "(x + y) AS d, "
	                        "(x - y) AS e, "
	                        "(y * 1.25::FLOAT) AS f, "
	                        "(x / 1.5::FLOAT) AS g, "
	                        "(y + 9.0::FLOAT) AS h "
	                        "FROM rg.jit_direct_append_rollover_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), count(a), count(h), sum(a)::DOUBLE, sum(h)::DOUBLE "
	                       "FROM rg.jit_direct_append_rollover_output");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(3, 0).GetValue<double>() == Approx(50005000.0));
	REQUIRE(check->GetValue(4, 0).GetValue<double>() == Approx(25087500.0));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 10000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t direct_materialize_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_generated") {
				    direct_materialize_count += stage.count;
			    }
		    }
		    REQUIRE(direct_materialize_count >= 5);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "op1:append_sink"));
	    });
}

TEST_CASE("SLJIT native integer projection elides proven overflow checks", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_integer_no_overflow_input AS "
	                          "SELECT (i % 1000)::INTEGER AS a, "
	                          "(i % 500)::INTEGER AS c, "
	                          "i::BIGINT AS b "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_integer_no_overflow_output AS "
	                        "SELECT (a + 1) AS a2, "
	                        "(a + c) AS ac, "
	                        "(b + 2) AS b2 "
	                        "FROM jit_integer_no_overflow_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), sum(a2)::BIGINT, sum(ac)::BIGINT, sum(b2)::BIGINT "
	                       "FROM jit_integer_no_overflow_output");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 7490000);
	REQUIRE(check->GetValue(3, 0).GetValue<int64_t>() == 50015000);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "overflow_check=false");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native=3"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_native="));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_input_format="));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_output_format="));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_selection_source="));
		    REQUIRE(StringUtil::Contains(event.ir, "native:integer-add-constant:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:integer-add-references:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:bigint-add-constant:no-overflow"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "overflow_check=true"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 10000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                 "op0:projection.direct_materialize_fixed_fused_generated"));
	    });
}

TEST_CASE("SLJIT native integer flat arithmetic handles SIMD-width tails", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_integer_tail_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(i % 1000)::INTEGER AS a, "
	                          "i::BIGINT AS b "
	                          "FROM range(2053) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_integer_tail_output AS "
	                        "SELECT i, "
	                        "(a + 7) AS ai, "
	                        "(7 - a) AS asub, "
	                        "(a * 3) AS amul, "
	                        "(b + 11) AS bi, "
	                        "(11 - b) AS bsub, "
	                        "(a + a) AS aa, "
	                        "(b + b) AS bb, "
	                        "(a - a) AS az, "
	                        "(b - b) AS bz, "
	                        "(a * a) AS am "
	                        "FROM jit_integer_tail_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*) "
	                       "FROM jit_integer_tail_output "
	                       "WHERE ai <> ((i % 1000)::INTEGER + 7) "
	                       "OR asub <> (7 - (i % 1000)::INTEGER) "
	                       "OR amul <> ((i % 1000)::INTEGER * 3) "
	                       "OR bi <> (i + 11) "
	                       "OR bsub <> (11 - i) "
	                       "OR aa <> ((i % 1000)::INTEGER + (i % 1000)::INTEGER) "
	                       "OR bb <> (i + i) "
	                       "OR az <> 0 "
	                       "OR bz <> 0 "
	                       "OR am <> ((i % 1000)::INTEGER * (i % 1000)::INTEGER)");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "overflow_check=false");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "native:integer-add-constant:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:integer-subtract-constant:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:integer-multiply-constant:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:bigint-add-constant:no-overflow"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:bigint-subtract-constant:no-overflow"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 2053 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                 "op0:projection.direct_materialize_fixed_fused_generated"));
	    });
}

TEST_CASE("SLJIT native floating flat arithmetic handles SIMD-width tails", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_floating_tail_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "       (i::FLOAT + 0.5::FLOAT) AS f, "
	                          "       ((i % 13)::FLOAT + 1.0::FLOAT) AS g, "
	                          "       (i::DOUBLE + 0.25::DOUBLE) AS d, "
	                          "       ((i % 17)::DOUBLE + 1.5::DOUBLE) AS e "
	                          "FROM range(2053) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_floating_tail_f_const AS "
	                          "SELECT i, f + 1.25::FLOAT AS v FROM jit_floating_tail_input"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_floating_tail_f_ref AS "
	                          "SELECT i, f / g AS v FROM jit_floating_tail_input"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_floating_tail_d_const AS "
	                          "SELECT i, 2.5::DOUBLE - d AS v FROM jit_floating_tail_input"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_floating_tail_d_ref AS "
	                          "SELECT i, d / e AS v FROM jit_floating_tail_input"));

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto check = con.Query("SELECT "
	                       "(SELECT count(*) FROM jit_floating_tail_f_const out "
	                       " JOIN jit_floating_tail_input src USING (i) "
	                       " WHERE abs(out.v::DOUBLE - ((src.f + 1.25::FLOAT)::DOUBLE)) > 0.001), "
	                       "(SELECT count(*) FROM jit_floating_tail_f_ref out "
	                       " JOIN jit_floating_tail_input src USING (i) "
	                       " WHERE abs(out.v::DOUBLE - ((src.f / src.g)::DOUBLE)) > 0.001), "
	                       "(SELECT count(*) FROM jit_floating_tail_d_const out "
	                       " JOIN jit_floating_tail_input src USING (i) "
	                       " WHERE abs(out.v - (2.5::DOUBLE - src.d)) > 1e-12), "
	                       "(SELECT count(*) FROM jit_floating_tail_d_ref out "
	                       " JOIN jit_floating_tail_input src USING (i) "
	                       " WHERE abs(out.v - (src.d / src.e)) > 1e-12)");
	REQUIRE_NO_FAIL(*check);
	for (idx_t col = 0; col < 4; col++) {
		REQUIRE(check->GetValue(col, 0).GetValue<int64_t>() == 0);
	}
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));

	RequireNativeSljitIr(manager, "double-add-constant");
	RequireNativeSljitIr(manager, "double-divide-references");
	RequireNativeSljitIr(manager, "double-subtract-constant");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 2053 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "op0:projection");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "op0:projection"));
	    });
}

TEST_CASE("SLJIT fixed direct append fuses mixed INTEGER and BIGINT groups", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mixed_integer_groups_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(i % 1000)::INTEGER AS a, "
	                          "((i * 3) % 1000)::INTEGER AS c, "
	                          "i::BIGINT AS b, "
	                          "(i * 2)::BIGINT AS d, "
	                          "DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_mixed_integer_groups_output AS "
	                        "SELECT i, "
	                        "(a + 7) AS a7, "
	                        "(c - 5) AS c5, "
	                        "(a + c) AS ac, "
	                        "(b + 11) AS b11, "
	                        "(d - 13) AS d13, "
	                        "(b + d) AS bd, "
	                        "dt "
	                        "FROM jit_mixed_integer_groups_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*) "
	                       "FROM jit_mixed_integer_groups_output "
	                       "WHERE a7 <> ((i % 1000)::INTEGER + 7) "
	                       "OR c5 <> (((i * 3) % 1000)::INTEGER - 5) "
	                       "OR ac <> ((i % 1000)::INTEGER + ((i * 3) % 1000)::INTEGER) "
	                       "OR b11 <> (i + 11) "
	                       "OR d13 <> ((i * 2) - 13) "
	                       "OR bd <> (i + (i * 2)) "
	                       "OR dt <> DATE '1992-01-01' + ((i % 365)::INTEGER)");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 100000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t fused_direct_count = 0;
		    idx_t fallback_projection_count = 0;
		    idx_t fallback_append_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    fused_direct_count += stage.count;
			    } else if (stage.stage.name == "op0:projection") {
				    fallback_projection_count += stage.count;
			    } else if (stage.stage.name == "op1:append_sink") {
				    fallback_append_count += stage.count;
			    }
		    }
		    REQUIRE(fused_direct_count >= 49);
		    REQUIRE(fallback_projection_count == 0);
		    REQUIRE(fallback_append_count == 0);
	    });
}

TEST_CASE("SLJIT fixed direct append fuses DECIMAL64 groups with checks", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_decimal_fused_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(i % 100000)::DECIMAL(15,2) AS d1, "
	                          "((i * 3) % 100000)::DECIMAL(15,2) AS d2 "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_decimal_fused_output AS "
	                        "SELECT i, "
	                        "(d1 + 1.25::DECIMAL(15,2)) AS d1p, "
	                        "(d2 - 2.50::DECIMAL(15,2)) AS d2m, "
	                        "(d1 + d2) AS dsum, "
	                        "(d2 - d1) AS ddiff "
	                        "FROM jit_decimal_fused_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*) "
	                       "FROM jit_decimal_fused_output "
	                       "WHERE d1p <> ((i % 100000)::DECIMAL(15,2) + 1.25::DECIMAL(15,2)) "
	                       "OR d2m <> (((i * 3) % 100000)::DECIMAL(15,2) - 2.50::DECIMAL(15,2)) "
	                       "OR dsum <> ((i % 100000)::DECIMAL(15,2) + "
	                       "((i * 3) % 100000)::DECIMAL(15,2)) "
	                       "OR ddiff <> (((i * 3) % 100000)::DECIMAL(15,2) - "
	                       "(i % 100000)::DECIMAL(15,2))");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "native:decimal64-add-constant") &&
		           StringUtil::Contains(event.ir, "native:decimal64-subtract-constant") &&
		           StringUtil::Contains(event.ir, "native:decimal64-add-references") &&
		           StringUtil::Contains(event.ir, "native:decimal64-subtract-references");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(event.candidate_traits.arithmetic_projection_count == 4);
		    REQUIRE(event.candidate_traits.reference_projection_count == 1);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 100000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t fused_direct_count = 0;
		    idx_t fallback_projection_count = 0;
		    idx_t fallback_append_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    fused_direct_count += stage.count;
			    } else if (stage.stage.name == "op0:projection") {
				    fallback_projection_count += stage.count;
			    } else if (stage.stage.name == "op1:append_sink") {
				    fallback_append_count += stage.count;
			    }
		    }
		    REQUIRE(fused_direct_count >= 49);
		    REQUIRE(fallback_projection_count == 0);
		    REQUIRE(fallback_append_count == 0);
	    });
}

TEST_CASE("SLJIT fused DECIMAL64 direct append preserves projection overflow message", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_decimal_fused_overflow_input AS "
	                          "SELECT (-999999999999999999)::DECIMAL(18,0) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto overflow = con.Query("CREATE TEMP TABLE jit_decimal_fused_overflow_output AS "
	                          "SELECT (d + 1::DECIMAL(18,0)) AS ok, "
	                          "(d - 1::DECIMAL(18,0)) AS lo "
	                          "FROM jit_decimal_fused_overflow_input");
	REQUIRE_FAIL(overflow);
	REQUIRE(StringUtil::Contains(overflow->GetError(), "Overflow in subtract of DECIMAL"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "native:decimal64-add-constant") &&
		           StringUtil::Contains(event.ir, "native:decimal64-subtract-constant");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "error" &&
		           StringUtil::Contains(event.reason, "Overflow in subtract of DECIMAL");
	    },
	    NoExtraJitEventCheck());
}

TEST_CASE("SLJIT fixed direct append fuses DATE arithmetic groups with checks", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_date_fused_input AS "
	                          "SELECT i::BIGINT AS i, "
	                          "((i % 11)::INTEGER - 5) AS off, "
	                          "CASE WHEN i = 0 THEN DATE 'infinity' "
	                          "WHEN i = 1 THEN DATE '-infinity' "
	                          "ELSE DATE '1992-01-01' + ((i % 365)::INTEGER) END AS dt "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_date_fused_output AS "
	                        "SELECT i, dt, "
	                        "(dt + 7) AS dtp, "
	                        "(dt - 5) AS dtm, "
	                        "(off + dt) AS dtoff_l, "
	                        "(dt + off) AS dtoff_r "
	                        "FROM jit_date_fused_input");
	REQUIRE_NO_FAIL(*result);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto check = con.Query("SELECT count(*) "
	                       "FROM jit_date_fused_output o "
	                       "JOIN jit_date_fused_input i USING (i) "
	                       "WHERE o.dtp <> i.dt + 7 "
	                       "OR o.dtm <> i.dt - 5 "
	                       "OR o.dtoff_l <> i.off + i.dt "
	                       "OR o.dtoff_r <> i.dt + i.off "
	                       "OR o.dt <> i.dt");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "native:date-add-constant") &&
		           StringUtil::Contains(event.ir, "native:date-subtract-constant") &&
		           StringUtil::Contains(event.ir, "native:date-add-references");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 100000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t fused_direct_count = 0;
		    idx_t fallback_projection_count = 0;
		    idx_t fallback_append_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    fused_direct_count += stage.count;
			    } else if (stage.stage.name == "op0:projection") {
				    fallback_projection_count += stage.count;
			    } else if (stage.stage.name == "op1:append_sink") {
				    fallback_append_count += stage.count;
			    }
		    }
		    REQUIRE(fused_direct_count >= 49);
		    REQUIRE(fallback_projection_count == 0);
		    REQUIRE(fallback_append_count == 0);
	    });
}

TEST_CASE("SLJIT fused DATE direct append preserves date range errors", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_date_fused_overflow_input AS "
	                          "SELECT DATE '5881580-07-10' AS dt FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto overflow = con.Query("CREATE TEMP TABLE jit_date_fused_overflow_output AS "
	                          "SELECT (dt + 1) AS bad FROM jit_date_fused_overflow_input");
	REQUIRE_FAIL(overflow);
	REQUIRE(StringUtil::Contains(overflow->GetError(), "Date out of range"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "native:date-add-constant");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "error" &&
		           StringUtil::Contains(event.reason, "Date out of range");
	    },
	    NoExtraJitEventCheck());
}

TEST_CASE("SLJIT native integer projection preserves required overflow checks", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_integer_checked_overflow_input AS "
	                          "SELECT 2147483647::INTEGER AS a FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto overflow = con.Query("SELECT (a + 1) AS a2 FROM jit_integer_checked_overflow_input");
	REQUIRE_FAIL(overflow);
	REQUIRE(StringUtil::Contains(overflow->GetError(), "Overflow in addition"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK) &&
		           StringUtil::Contains(event.ir, "overflow_check=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native=3"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, ":no-overflow"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "overflow_check=false"));
	    });
}

TEST_CASE("SLJIT direct fixed-width materialization handles INTEGER BIGINT DECIMAL and DATE", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

	auto db_path = TestCreatePath("jit_direct_fixed_append.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS rg (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE rg.jit_direct_fixed_input AS "
	                          "SELECT (i % 1000)::INTEGER AS a, "
	                          "i::BIGINT AS b, "
	                          "(i % 100000)::DECIMAL(15,2) AS d, "
	                          "DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt, "
	                          "((i % 2) = 0) AS flag "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TABLE rg.jit_direct_fixed_output AS "
	                        "SELECT (a + 1) AS a2, "
	                        "(b - 3) AS b2, "
	                        "(d + 1.25::DECIMAL(15,2)) AS d2, "
	                        "dt, "
	                        "flag "
	                        "FROM rg.jit_direct_fixed_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), sum(a2)::BIGINT, sum(b2)::BIGINT, sum(d2)::VARCHAR, min(dt), max(dt), "
	                       "sum(CASE WHEN flag THEN 1 ELSE 0 END)::BIGINT "
	                       "FROM rg.jit_direct_fixed_output");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 49965000);
	REQUIRE(check->GetValue(3, 0).ToString() == "50007500.00");
	REQUIRE(check->GetValue(4, 0).ToString() == "1992-01-01");
	REQUIRE(check->GetValue(5, 0).ToString() == "1992-12-30");
	REQUIRE(check->GetValue(6, 0).GetValue<int64_t>() == 5000);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 10000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t direct_materialize_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    direct_materialize_count += stage.count;
			    }
		    }
		    REQUIRE(direct_materialize_count >= 5);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "op1:append_sink"));
	    });
}

TEST_CASE("SLJIT direct append source-appends VARCHAR with fixed projections", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

	auto db_path = TestCreatePath("jit_direct_varchar_append.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS rg (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE rg.jit_direct_varchar_input AS "
	                          "SELECT i::INTEGER AS id, "
	                          "(i % 1000)::INTEGER AS a, "
	                          "i::BIGINT AS b, "
	                          "(i % 100000)::DECIMAL(15,2) AS d, "
	                          "DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt, "
	                          "CASE WHEN (i % 997) = 0 THEN NULL "
	                          "     WHEN (i % 251) = 0 THEN 'long-' || repeat((i::VARCHAR || '-'), 128) "
	                          "     ELSE 'customer-' || ((i % 1024)::VARCHAR) END AS name "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TABLE rg.jit_direct_varchar_output AS "
	                        "SELECT id, "
	                        "(a + 1) AS a2, "
	                        "(b - 3) AS b2, "
	                        "(d + 1.25::DECIMAL(15,2)) AS d2, "
	                        "dt, "
	                        "name "
	                        "FROM rg.jit_direct_varchar_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), "
	                       "sum(CASE WHEN o.name IS NOT DISTINCT FROM i.name THEN 1 ELSE 0 END)::BIGINT, "
	                       "sum(CASE WHEN o.name IS NULL THEN 1 ELSE 0 END)::BIGINT, "
	                       "max(length(o.name)), "
	                       "sum(o.a2)::BIGINT, "
	                       "sum(o.b2)::BIGINT, "
	                       "sum(o.d2)::VARCHAR "
	                       "FROM rg.jit_direct_varchar_output o "
	                       "JOIN rg.jit_direct_varchar_input i USING (id)");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 11);
	REQUIRE(check->GetValue(3, 0).GetValue<int64_t>() > 512);
	REQUIRE(check->GetValue(4, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->GetValue(5, 0).GetValue<int64_t>() == 49965000);
	REQUIRE(check->GetValue(6, 0).ToString() == "50007500.00");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 10000 &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "op0:projection.direct_materialize_fixed_fused_generated");
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t direct_materialize_count = 0;
		    idx_t fallback_projection_count = 0;
		    idx_t fallback_append_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_generated" ||
			        stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    direct_materialize_count += stage.count;
			    } else if (stage.stage.name == "op0:projection") {
				    fallback_projection_count += stage.count;
			    } else if (stage.stage.name == "op1:append_sink") {
				    fallback_append_count += stage.count;
			    }
		    }
		    REQUIRE(direct_materialize_count >= 5);
		    REQUIRE(fallback_projection_count == 0);
		    REQUIRE(fallback_append_count == 0);
	    });
}

TEST_CASE("SLJIT direct fixed-width materialization rolls over column segments without fallback", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_direct_segment_rollover_input AS "
	                          "SELECT i::INTEGER AS a, (i * 3)::INTEGER AS b "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_direct_segment_rollover_output AS "
	                        "SELECT (a + 1) AS a1, "
	                        "(a + 2) AS a2, "
	                        "(b - 1) AS b1, "
	                        "(b - 2) AS b2 "
	                        "FROM jit_direct_segment_rollover_input");
	REQUIRE_NO_FAIL(*result);

	auto check = con.Query("SELECT count(*), sum(a1)::BIGINT, sum(b2)::BIGINT "
	                       "FROM jit_direct_segment_rollover_output");
	REQUIRE_NO_FAIL(*check);
	REQUIRE(check->GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->GetValue(1, 0).GetValue<int64_t>() == 5000050000);
	REQUIRE(check->GetValue(2, 0).GetValue<int64_t>() == 14999650000);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.output_rows == 100000 &&
		           (StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "op0:projection.direct_materialize_fixed_generated") ||
		            StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "op0:projection.direct_materialize_fixed_fused_generated"));
	    },
	    [](const ExecutionRegionEvent &event) {
		    idx_t direct_materialize_count = 0;
		    idx_t fallback_projection_count = 0;
		    idx_t fallback_append_count = 0;
		    for (auto &stage : event.generated_stage_runtime) {
			    if (stage.stage.name == "op0:projection.direct_materialize_fixed_generated" ||
			        stage.stage.name == "op0:projection.direct_materialize_fixed_fused_generated") {
				    direct_materialize_count += stage.count;
			    } else if (stage.stage.name == "op0:projection") {
				    fallback_projection_count += stage.count;
			    } else if (stage.stage.name == "op1:append_sink") {
				    fallback_append_count += stage.count;
			    }
		    }
		    REQUIRE(direct_materialize_count >= 49);
		    REQUIRE(fallback_projection_count == 0);
		    REQUIRE(fallback_append_count == 0);
	    });
}

TEST_CASE("SLJIT direct append resized transient segments survive reopen and drop", "[api][jit]") {
	auto db_path = TestCreatePath("jit_direct_append_reopen_drop.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");

	const string float_projection = "SELECT "
	                                "(x + 1.0::FLOAT) AS a, "
	                                "(x * 1.0001::FLOAT) AS b, "
	                                "(y - 3.0::FLOAT) AS c, "
	                                "(x + y) AS d, "
	                                "(x - y) AS e, "
	                                "(y * 1.25::FLOAT) AS f, "
	                                "(x / 1.5::FLOAT) AS g, "
	                                "(y + 9.0::FLOAT) AS h "
	                                "FROM float_in";
	const string double_projection = "SELECT "
	                                 "(x + 1.0::DOUBLE) AS a, "
	                                 "(x * 1.0001::DOUBLE) AS b, "
	                                 "(y - 3.0::DOUBLE) AS c, "
	                                 "(x + y) AS d, "
	                                 "(x - y) AS e, "
	                                 "(y * 1.25::DOUBLE) AS f, "
	                                 "(x / 1.5::DOUBLE) AS g, "
	                                 "(y + 9.0::DOUBLE) AS h "
	                                 "FROM double_in";

	RunPersistentJitDirectAppendSQL(db_path,
	                                "CREATE OR REPLACE TABLE float_in AS "
	                                "SELECT i::FLOAT AS x, (i * 0.5)::FLOAT AS y FROM range(1024) tbl(i);"
	                                "CREATE OR REPLACE TABLE double_in AS "
	                                "SELECT i::DOUBLE AS x, (i * 0.5)::DOUBLE AS y FROM range(1024) tbl(i);",
	                                false);
	RunPersistentJitDirectAppendSQL(db_path, "CREATE OR REPLACE TABLE base_float AS " + float_projection, false);
	RunPersistentJitDirectAppendSQL(db_path, "CREATE OR REPLACE TABLE base_double AS " + double_projection, false);
	RunPersistentJitDirectAppendSQL(db_path,
	                                "CREATE OR REPLACE TABLE result_float_off AS " + float_projection +
	                                    ";"
	                                    "DROP TABLE result_float_off;",
	                                false);
	RunPersistentJitDirectAppendSQL(db_path,
	                                "CREATE OR REPLACE TABLE result_float_auto AS " + float_projection +
	                                    ";"
	                                    "DROP TABLE result_float_auto;",
	                                true);
	RunPersistentJitDirectAppendSQL(db_path,
	                                "CREATE OR REPLACE TABLE result_double_off AS " + double_projection +
	                                    ";"
	                                    "DROP TABLE result_double_off;",
	                                false);
	RunPersistentJitDirectAppendSQL(db_path,
	                                "CREATE OR REPLACE TABLE result_double_auto AS " + double_projection +
	                                    ";"
	                                    "DROP TABLE result_double_auto;",
	                                true);

	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT auto planner skips native-contract projection glue before region graph", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, false, 10000);
	CreateJitStatefulSortInput(con);

	ClearJitTrace(manager, true);
	auto result = RunJitStatefulSortQuery(con);
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
		           event.runner_cost.native_protocol_class ==
		               PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
		           StringUtil::Contains(event.reason, "region_graph=skipped");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(event.has_candidate);
		    RequireVectorizedCboSkip(event);
		    REQUIRE(event.runner_cost.full_pipeline);
		    REQUIRE(event.runner_cost.generated_stage_count == 1);
		    REQUIRE(event.runner_cost.native_sort_stage_count == 0);
		    REQUIRE(event.runner_cost.materialization_elision_count == 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch == 0);
		    RequirePipelineCboOnlyTiming(event);
	    });
}

TEST_CASE("JIT auto planner compiles division projection aggregate and sort as compute candidates", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	CreateJitStatefulSortInput(con);

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_stateful_sort_division_output AS "
	                        "SELECT k, name, CAST(s AS DOUBLE) / 7.0 AS scaled "
	                        "FROM ("
	                        "    SELECT k, name, sum(v) AS s "
	                        "    FROM jit_stateful_sort_input "
	                        "    GROUP BY k, name"
	                        ") grouped ORDER BY scaled DESC LIMIT 20");
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
		    REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_operator_stage_benefit"));
		    RequireGeneratedMachineCodeRegion(event);
		    RequireCandidateStructure(event, 0, 2, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::SORT;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    RequireGeneratedMachineCodeRegion(event);
		    RequireCandidateStructure(event, 0, 2, ExecutionRegionSinkKind::SORT);
	    });
}

TEST_CASE("JIT generated work class does not label source-filter compute as projection-only", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_source_filter_class_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	const auto expression = MakeRepeatedIntegerExpression("i", 512);
	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_source_filter_class_output AS "
	                        "SELECT " +
	                        expression +
	                        " AS v "
	                        "FROM jit_source_filter_class_input "
	                        "WHERE i BETWEEN 10 AND 5000");
	REQUIRE_NO_FAIL(*result);

	auto count = con.Query("SELECT count(*) FROM jit_source_filter_class_output");
	REQUIRE_NO_FAIL(*count);
	REQUIRE(count->GetValue(0, 0).ToString() == "4991");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.candidate_traits.source_filter_expression_count > 0 &&
		           event.candidate_traits.high_cost_projection_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE);
	    });
}

TEST_CASE("JIT auto planner rejects tiny aggregate pipeline before region graph", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_fast_cost_input AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i + 1) FROM jit_auto_fast_cost_input WHERE i > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2014");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           !event.has_candidate &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE &&
		           event.runner_cost.materialization_elision_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireVectorizedCboSkip(event);
		    REQUIRE(event.runner_cost.admission_class == "materialization_elision");
		    REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.stage_timings.graph_build_time_us == 0);
		    REQUIRE(event.stage_timings.candidate_cbo_time_us == 0);
		    REQUIRE(event.stage_timings.ir_lowering_time_us == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "before region graph"));
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
		    REQUIRE(StringUtil::Contains(event.reason, "rejected_insufficient_benefit"));
	    });

	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventStatus(event) != "compiled");
	}
}

TEST_CASE("JIT auto planner does not enter region graph when pipeline has no costed acceleration", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_no_cost_left AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_no_cost_right AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_auto_no_cost_left l JOIN jit_auto_no_cost_right r USING (i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "64");

	REQUIRE(manager.GetEvents().empty());
}

TEST_CASE("JIT diagnostic tracing analyzes fused contract boundary regions", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

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
		    REQUIRE(StringUtil::Contains(event.reason, "backend_native=projection:1|ungrouped-aggregate:1"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_boundary=table-scan:1"));
	    });

	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) ||
		    event.blocker != EXECUTION_REGION_BLOCKER_FUSED_REGION_CONTRACT_HAS_BOUNDARIES) {
			continue;
		}
		REQUIRE(StringUtil::Contains(event.reason, "region-lowering"));
	}
}

TEST_CASE("JIT auto planner costs generated aggregate updates as generated backend work", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_default_aggregate_input AS "
	                          "SELECT (i % 5)::BIGINT AS i FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM jit_auto_default_aggregate_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2000000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_operator_stage_benefit"));
		    RequireGeneratedMachineCodeRegion(event);
	    });
}

TEST_CASE("JIT auto planner cost uses configurable CBO settings", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false, false);
	ConfigureJitDecisionTrace(con);
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
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           event.runner_cost.rows == 500000 && event.runner_cost.generated_stage_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(event.has_candidate);
		    RequireVectorizedCboSkip(event);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.startup_cost >= 1000000000);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit < event.runner_cost.required_benefit);
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
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

	ConfigureSljitForCoverageSettings(con, false, true);
	ConfigureJitDecisionTrace(con);
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
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
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
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.expression_cost > 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_operator_stage_benefit"));
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
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_filtered_sum_small AS "
	                          "SELECT i::BIGINT AS a, (i % 17)::BIGINT AS b FROM range(1024) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_auto_filtered_sum_small WHERE a >= 10 AND a < 1024");
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           !event.has_candidate &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE &&
		           event.runner_cost.materialization_elision_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireVectorizedCboSkip(event);
		    REQUIRE(event.runner_cost.admission_class == "materialization_elision");
		    REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.stage_timings.graph_build_time_us == 0);
		    REQUIRE(event.stage_timings.candidate_cbo_time_us == 0);
		    REQUIRE(event.stage_timings.ir_lowering_time_us == 0);
		    REQUIRE(StringUtil::Contains(event.reason, "before region graph"));
		    REQUIRE(StringUtil::Contains(event.reason, "region_graph=skipped"));
		    REQUIRE(StringUtil::Contains(event.reason, "rejected_insufficient_benefit"));
	    });
}
