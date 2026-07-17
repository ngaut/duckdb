#include "test_jit_helpers.hpp"

#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

namespace duckdb {
PhysicalRunnerCostInput
BuildExecutionRegionPipelineCandidateUpperBoundCostInput(const PhysicalRunnerCostInput &pipeline_input);
}

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

struct UnitTestExecutionRegionBackendMetadataCalls {
	idx_t name = 0;
	idx_t description = 0;
	idx_t runner_kind = 0;
	idx_t supports_regions = 0;
};

class UnitTestMetadataExecutionRegionBackend : public ExecutionRegionBackend {
public:
	explicit UnitTestMetadataExecutionRegionBackend(UnitTestExecutionRegionBackendMetadataCalls &calls_p)
	    : calls(calls_p) {
	}

	string Name() const override {
		calls.get().name++;
		return "unit_test_metadata_backend";
	}

	string Description() const override {
		calls.get().description++;
		return "unit test metadata backend";
	}

	ExecutionRunnerKind RunnerKind() const override {
		calls.get().runner_kind++;
		return ExecutionRunnerKind::COMPILED_VECTORIZED;
	}

	bool SupportsRegions() const override {
		calls.get().supports_regions++;
		return true;
	}

private:
	reference<UnitTestExecutionRegionBackendMetadataCalls> calls;
};

struct UnitTestBlockingAvailabilityState {
	std::mutex lock;
	std::condition_variable condition;
	bool entered = false;
	bool release = false;
};

class UnitTestBlockingAvailabilityBackend : public ExecutionRegionBackend {
public:
	explicit UnitTestBlockingAvailabilityBackend(UnitTestBlockingAvailabilityState &state_p) : state(state_p) {
	}

	string Name() const override {
		return "unit_test_blocking_availability_backend";
	}

	string Description() const override {
		return "unit test blocking availability backend";
	}

	bool IsAvailable() const override {
		auto &availability = state.get();
		std::unique_lock<std::mutex> guard(availability.lock);
		availability.entered = true;
		availability.condition.notify_all();
		availability.condition.wait(guard, [&availability] { return availability.release; });
		return true;
	}

	bool SupportsRegions() const override {
		return true;
	}

private:
	reference<UnitTestBlockingAvailabilityState> state;
};

class UnitTestConcurrentRegistrationBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "unit_test_concurrent_registration_backend";
	}

	string Description() const override {
		return "unit test concurrent registration backend";
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

	const auto initial_backend_count = test.manager.GetBackends(&test.context).size();
	REQUIRE_THROWS(test.manager.RegisterBackend(make_uniq<UnitTestExecutionRegionBackend>(),
	                                            EXECUTION_REGION_BACKEND_ABI_VERSION + 1));
	REQUIRE(test.manager.GetBackends(&test.context).size() == initial_backend_count);
	test.manager.RegisterBackend(make_uniq<UnitTestExecutionRegionBackend>(), EXECUTION_REGION_BACKEND_ABI_VERSION);
	test.manager.RegisterBackend(make_uniq<UnitTestGpuExecutionRegionBackend>(), EXECUTION_REGION_BACKEND_ABI_VERSION);

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

TEST_CASE("Execution region manager freezes backend metadata at registration", "[api][jit]") {
	UnitTestExecutionRegionBackendMetadataCalls calls;
	JitTestDatabase test;
	test.manager.RegisterBackend(make_uniq<UnitTestMetadataExecutionRegionBackend>(calls),
	                             EXECUTION_REGION_BACKEND_ABI_VERSION);

	REQUIRE(calls.name == 1);
	REQUIRE(calls.description == 1);
	REQUIRE(calls.runner_kind == 1);
	REQUIRE(calls.supports_regions == 1);

	auto backends = test.manager.GetBackends(&test.context);
	REQUIRE(test.manager.HasAvailableBackendForRunner(test.context, ExecutionRunnerKind::COMPILED_VECTORIZED));
	string backend_name;
	auto backend = test.manager.SelectBackend(test.context, backend_name);
	REQUIRE(backend);

	REQUIRE(calls.name == 1);
	REQUIRE(calls.description == 1);
	REQUIRE(calls.runner_kind == 1);
	REQUIRE(calls.supports_regions == 1);
}

TEST_CASE("Execution region backend availability does not hold the registry lock", "[api][jit]") {
	UnitTestBlockingAvailabilityState availability;
	JitTestDatabase test;
	test.manager.RegisterBackend(make_uniq<UnitTestBlockingAvailabilityBackend>(availability),
	                             EXECUTION_REGION_BACKEND_ABI_VERSION);

	std::exception_ptr reader_error;
	std::thread reader([&] {
		try {
			test.manager.GetBackends();
		} catch (...) {
			reader_error = std::current_exception();
		}
	});

	bool availability_entered;
	{
		std::unique_lock<std::mutex> guard(availability.lock);
		availability_entered = availability.condition.wait_for(guard, std::chrono::seconds(2),
		                                                       [&availability] { return availability.entered; });
	}

	std::mutex registration_lock;
	std::condition_variable registration_condition;
	bool registration_complete = false;
	unique_ptr<std::thread> registrar;
	if (availability_entered) {
		registrar = make_uniq<std::thread>([&] {
			test.manager.RegisterBackend(make_uniq<UnitTestConcurrentRegistrationBackend>(),
			                             EXECUTION_REGION_BACKEND_ABI_VERSION);
			{
				std::lock_guard<std::mutex> guard(registration_lock);
				registration_complete = true;
			}
			registration_condition.notify_all();
		});
	}

	bool registration_completed_without_waiting = false;
	if (registrar) {
		std::unique_lock<std::mutex> guard(registration_lock);
		registration_completed_without_waiting = registration_condition.wait_for(
		    guard, std::chrono::seconds(2), [&registration_complete] { return registration_complete; });
	}
	{
		std::lock_guard<std::mutex> guard(availability.lock);
		availability.release = true;
	}
	availability.condition.notify_all();
	reader.join();
	if (registrar) {
		registrar->join();
	}

	REQUIRE(availability_entered);
	REQUIRE(registration_completed_without_waiting);
	REQUIRE(reader_error == nullptr);
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
	REQUIRE(profile.stateful_protocol_penalty == 1440);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.startup_cost == 32000);
	REQUIRE(profile.required_benefit == 48000);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 1600;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 164);
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
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.source_filter_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 5120);
	REQUIRE(profile.stateful_protocol_penalty == 2880);
	REQUIRE(profile.saved_work_per_batch == 2456);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 1346398;
	input.expression_cost = 154;
	input.generated_stage_count = 3;
	input.native_join_stage_count = 5;
	input.native_aggregate_stage_count = 0;
	input.native_grouped_aggregate_stage_count = 0;
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
	input.uses_scan_filters = true;
	input.source_filter_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 2048);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == 1374);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges grouped aggregate fusion against parallel vectorized baseline", "[api][jit]") {
	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 1024;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 30000;
	parameters.startup_margin_basis_points = 0;

	PhysicalRunnerCostInput high_grouped_native_stage_shape;
	high_grouped_native_stage_shape.estimated_cardinality = 4619408;
	high_grouped_native_stage_shape.expression_cost = 174;
	high_grouped_native_stage_shape.generated_stage_count = 6;
	high_grouped_native_stage_shape.native_join_stage_count = 1;
	high_grouped_native_stage_shape.native_aggregate_stage_count = 3;
	high_grouped_native_stage_shape.native_grouped_aggregate_stage_count = 3;
	high_grouped_native_stage_shape.full_pipeline = true;
	high_grouped_native_stage_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	high_grouped_native_stage_shape.has_accelerated_work = true;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(high_grouped_native_stage_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 3724);
	REQUIRE(profile.selected_accelerated_runner);

	parameters.vectorized_parallelism = 12;
	profile = DuckDBCostModel::SelectPhysicalRunner(high_grouped_native_stage_shape, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput low_grouped_native_stage_shape;
	low_grouped_native_stage_shape.estimated_cardinality = 302394;
	low_grouped_native_stage_shape.expression_cost = 79;
	low_grouped_native_stage_shape.generated_stage_count = 3;
	low_grouped_native_stage_shape.native_join_stage_count = 1;
	low_grouped_native_stage_shape.native_aggregate_stage_count = 1;
	low_grouped_native_stage_shape.native_grouped_aggregate_stage_count = 1;
	low_grouped_native_stage_shape.full_pipeline = true;
	low_grouped_native_stage_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	low_grouped_native_stage_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(low_grouped_native_stage_shape, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput high_join_grouped_stage_shape;
	high_join_grouped_stage_shape.estimated_cardinality = 20386135;
	high_join_grouped_stage_shape.expression_cost = 16000;
	high_join_grouped_stage_shape.generated_stage_count = 7;
	high_join_grouped_stage_shape.native_join_stage_count = 3;
	high_join_grouped_stage_shape.native_aggregate_stage_count = 1;
	high_join_grouped_stage_shape.native_grouped_aggregate_stage_count = 1;
	high_join_grouped_stage_shape.full_pipeline = true;
	high_join_grouped_stage_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	high_join_grouped_stage_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(high_join_grouped_stage_shape, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	parameters.vectorized_parallelism = 1;
	PhysicalRunnerCostInput large_low_work_generated_grouped_shape;
	large_low_work_generated_grouped_shape.estimated_cardinality = 20000000;
	large_low_work_generated_grouped_shape.source_contract_input_cardinality = 20000000;
	large_low_work_generated_grouped_shape.expression_cost = 43;
	large_low_work_generated_grouped_shape.generated_stage_count = 3;
	large_low_work_generated_grouped_shape.generated_backend_stage_count = 2;
	large_low_work_generated_grouped_shape.generated_grouped_aggregate_stage_count = 2;
	large_low_work_generated_grouped_shape.native_grouped_state_address_lookup_count = 1;
	large_low_work_generated_grouped_shape.materialization_elision_count = 1;
	large_low_work_generated_grouped_shape.full_pipeline = true;
	large_low_work_generated_grouped_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	large_low_work_generated_grouped_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(large_low_work_generated_grouped_shape, parameters);
	REQUIRE(profile.admission_class == "generated");
	REQUIRE(profile.generated_expression_work > 0);
	REQUIRE(profile.generated_stage_work > 0);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated|generated_stage_benefit");
	REQUIRE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput join_grouped_replacement_shape;
	join_grouped_replacement_shape.estimated_cardinality = 20000000;
	join_grouped_replacement_shape.source_contract_input_cardinality = 20000000;
	join_grouped_replacement_shape.expression_cost = 43;
	join_grouped_replacement_shape.generated_stage_count = 3;
	join_grouped_replacement_shape.generated_backend_stage_count = 2;
	join_grouped_replacement_shape.generated_grouped_aggregate_stage_count = 1;
	join_grouped_replacement_shape.native_join_stage_count = 1;
	join_grouped_replacement_shape.native_grouped_state_address_lookup_count = 1;
	join_grouped_replacement_shape.full_pipeline = true;
	join_grouped_replacement_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	join_grouped_replacement_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(join_grouped_replacement_shape, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.generated_backend_stage_work > 0);
	REQUIRE(profile.native_operator_work > 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	join_grouped_replacement_shape.native_join_stage_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(join_grouped_replacement_shape, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
	parameters.vectorized_parallelism = 12;

	PhysicalRunnerCostInput low_generated_compute_shape;
	low_generated_compute_shape.estimated_cardinality = 3023933;
	low_generated_compute_shape.expression_cost = 39;
	low_generated_compute_shape.generated_stage_count = 3;
	low_generated_compute_shape.full_pipeline = true;
	low_generated_compute_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	low_generated_compute_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(low_generated_compute_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 78);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput medium_generated_compute_shape;
	medium_generated_compute_shape.estimated_cardinality = 3000000;
	medium_generated_compute_shape.expression_cost = 78;
	medium_generated_compute_shape.generated_stage_count = 6;
	medium_generated_compute_shape.full_pipeline = true;
	medium_generated_compute_shape.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	medium_generated_compute_shape.has_accelerated_work = true;

	profile = DuckDBCostModel::SelectPhysicalRunner(medium_generated_compute_shape, parameters);
	REQUIRE(profile.saved_work_per_batch == 156);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT backend owns scan-filtered aggregate terminals through DuckDB scan filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_scan_filtered_aggregate_terminal AS "
	                          "SELECT i, CAST((i % 100) AS DOUBLE) + 0.25 AS x, "
	                          "CAST(((i * 3) % 200) AS DOUBLE) + 0.5 AS y "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT sum(x), sum((x * 1.5) + (y / 4.0)) "
	                     "FROM jit_scan_filtered_aggregate_terminal WHERE i > 1000 AND i % 97 = 0";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	vector<idx_t> scan_filtered_kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !event.candidate_uses_scan_filters) {
			continue;
		}
		scan_filtered_kernel_ids.push_back(event.kernel_id);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(event.runner_cost.selected_accelerated_runner);
		REQUIRE(event.runner_cost.materialization_elision_count > 0);
	}
	REQUIRE(!scan_filtered_kernel_ids.empty());
	RequireMaterializationElisionRuntimeProof(manager, scan_filtered_kernel_ids);
	bool found_compiled_storage_filter = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    HasJitRuntimePathPrefix(event, "source.storage_scan.compiled_filter")) {
			found_compiled_storage_filter = true;
			REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK));
		}
	}
	REQUIRE(found_compiled_storage_filter);
}

TEST_CASE("JIT production CBO uses modulo domains for filtered reductions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, false, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_modulo_filter_cost_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto dense = con.Query("SELECT sum(i * 31 + i % 97) FROM jit_modulo_filter_cost_input WHERE i % 7 = 3");
	REQUIRE_NO_FAIL(*dense);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
		           event.candidate_uses_scan_filters;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.rows >= 100000);
		    REQUIRE(event.runner_cost.rows <= 150000);
		    REQUIRE(event.runner_cost.materialization_elision_count == 1);
		    REQUIRE(event.runner_cost.source_contract_scan_penalty == 0);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
	    });

	ClearJitTrace(manager, true);
	auto sparse = con.Query("SELECT sum(i * 31 + i % 97) FROM jit_modulo_filter_cost_input WHERE i % 1000000 = 3");
	REQUIRE_NO_FAIL(*sparse);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && IsVectorizedCboSkipEvent(event) &&
		           event.runner_cost.present && event.runner_cost.generated_stage_count > 0 &&
		           event.runner_cost.rows <= 2;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(event.has_candidate);
		    REQUIRE(event.runner_cost.materialization_elision_count == 1);
		    REQUIRE(event.runner_cost.source_contract_scan_penalty == 0);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit < event.runner_cost.required_benefit);
		    REQUIRE_FALSE(event.runner_cost.selected_accelerated_runner);
	    });
}

TEST_CASE("JIT selected scan-filtered grouped aggregate uses direct grouped primitive recipe", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_scan_filtered_grouped_aggregate AS "
	                          "SELECT i::BIGINT AS id, "
	                          "       CAST(i % 1024 AS INTEGER) AS group_id, "
	                          "       CAST((i % 100) AS DOUBLE) + 0.25 AS x, "
	                          "       CAST(((i * 3) % 200) AS DOUBLE) + 0.5 AS y, "
	                          "       CASE WHEN i % 7 = 0 THEN 'OTHER' ELSE 'EUROPE' END AS region "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT group_id, "
	                     "       sum((x * 1.5) + (y / 4.0)) AS payload_sum "
	                     "FROM jit_scan_filtered_grouped_aggregate "
	                     "WHERE region LIKE 'EUROPE%' "
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

	vector<idx_t> scan_filtered_kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !event.selected_uses_scan_filters) {
			continue;
		}
		scan_filtered_kernel_ids.push_back(event.kernel_id);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(event.runner_cost.selected_accelerated_runner);
		REQUIRE(event.runner_cost.materialization_elision_count > 0);
	}
	REQUIRE(!scan_filtered_kernel_ids.empty());
	RequireMaterializationElisionRuntimeProof(manager, scan_filtered_kernel_ids);
}

TEST_CASE("JIT keeps generated-safe grouped aggregate filters in generated source", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_generated_grouped_source_filter AS "
	                          "SELECT i::BIGINT AS id, "
	                          "       CAST(i % 1024 AS INTEGER) AS group_id, "
	                          "       CAST((i % 100) AS DOUBLE) + 0.25 AS x, "
	                          "       CAST(((i * 3) % 200) AS DOUBLE) + 0.5 AS y "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT group_id, "
	                     "       sum((x * 1.5) + (y / 4.0)) AS payload_sum "
	                     "FROM jit_generated_grouped_source_filter "
	                     "WHERE id % 7 <> 0 "
	                     "GROUP BY group_id "
	                     "ORDER BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

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

	bool found_generated_grouped_filter = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		found_generated_grouped_filter = true;
		REQUIRE_FALSE(event.selected_uses_scan_filters);
		REQUIRE(StringUtil::Contains(event.reason, "source-strategy=generated-source-filter"));
		RequireGeneratedSourceFilterContract(event);
	}
	REQUIRE(found_generated_grouped_filter);
}

TEST_CASE("JIT materialization-elision runtime proves projected aggregate ownership", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_aggregate_input AS "
	                          "SELECT (i % 4096)::BIGINT AS g, "
	                          "       CASE WHEN i % 11 = 0 THEN NULL ELSE i::BIGINT END AS v "
	                          "FROM range(262144) tbl(i)"));

	const string query = "SELECT k32, count(v) AS n "
	                     "FROM ("
	                     "    SELECT CAST(g AS INTEGER) AS k32, v "
	                     "    FROM jit_projected_grouped_aggregate_input"
	                     ") projected "
	                     "GROUP BY k32 "
	                     "ORDER BY k32";
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

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate preserves dense key semantics", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_sum_input AS "
	                          "SELECT (i % 1024)::BIGINT AS g, (i % 17)::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));

	const string query = "SELECT k32, sum(v) AS s "
	                     "FROM ("
	                     "    SELECT CAST(g AS INTEGER) AS k32, v "
	                     "    FROM jit_projected_grouped_sum_input"
	                     ") projected "
	                     "GROUP BY k32 "
	                     "ORDER BY k32";
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

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate supports substring group keys without delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_substring_input AS "
	                          "SELECT CASE "
	                          "           WHEN i % 17 = 0 THEN NULL "
	                          "           WHEN i % 5 = 0 THEN 'ab' || ((i % 3)::VARCHAR) "
	                          "           WHEN i % 7 = 0 THEN 'a' "
	                          "           ELSE 'cd' || ((i % 11)::VARCHAR) "
	                          "       END AS s, "
	                          "       (i % 19)::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));

	const string query = "SELECT prefix, sum(v) AS s "
	                     "FROM ("
	                     "    SELECT substring(s, 1, 2) AS prefix, v "
	                     "    FROM jit_projected_grouped_substring_input"
	                     ") projected "
	                     "GROUP BY prefix "
	                     "ORDER BY prefix NULLS FIRST";
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

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate handles sparse projected groups without delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_sparse_sum_input AS "
	                          "SELECT ((i / 3)::BIGINT * 50000::BIGINT) AS g, (i % 17)::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));

	const string query = "SELECT k32, sum(v) AS s "
	                     "FROM ("
	                     "    SELECT CASE WHEN g >= 0 THEN CAST(g AS INTEGER) ELSE 0 END AS k32, v "
	                     "    FROM jit_projected_grouped_sparse_sum_input"
	                     ") projected "
	                     "GROUP BY k32 "
	                     "ORDER BY k32";
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

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
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

	input.source_contract_input_cardinality = 150000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_rows == 150000);
	REQUIRE(profile.rows > profile.source_contract_input_rows);
	REQUIRE(profile.stateful_protocol_penalty == 1696);
	REQUIRE(profile.saved_work_per_batch == -464);
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
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO does not credit unknown-output backend stages through hash build fusion", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 800000;
	input.source_contract_input_cardinality = 800000;
	input.source_contract_output_cardinality_unknown = true;
	input.generated_stage_count = 3;
	input.generated_backend_stage_count = 2;
	input.native_join_stage_count = 2;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges generated expression prefixes before native hash join build sinks", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 40000;
	input.expression_cost = 842;
	input.generated_stage_count = 3;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 8912);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO keeps DuckDB-owned scan-filter rows in the scan contract", "[api][jit]") {
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
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.accelerated_runner_benefit == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.uses_scan_filters = false;
	input.native_join_stage_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges source-contract scan-filter input against downstream fusion", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 1200243;
	input.source_contract_input_cardinality = 6001215;
	input.expression_cost = 106;
	input.generated_stage_count = 4;
	input.generated_backend_stage_count = 2;
	input.native_join_stage_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.source_filter_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_rows == 6001215);
	REQUIRE(profile.source_contract_input_batches > profile.batches);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.native_grouped_state_address_lookup_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_grouped_state_address_lookup_count = 0;
	input.source_contract_output_cardinality_unknown = false;
	input.native_join_stage_count = 0;
	input.native_hash_join_build_sink_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_hash_join_build_sink_count = 0;
	input.native_join_stage_count = 1;
	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput scan_filtered_aggregate_tail;
	scan_filtered_aggregate_tail.estimated_cardinality = 1200243;
	scan_filtered_aggregate_tail.source_contract_input_cardinality = 6001215;
	scan_filtered_aggregate_tail.expression_cost = 26;
	scan_filtered_aggregate_tail.generated_stage_count = 5;
	scan_filtered_aggregate_tail.generated_backend_stage_count = 1;
	scan_filtered_aggregate_tail.materialization_elision_count = 1;
	scan_filtered_aggregate_tail.full_pipeline = true;
	scan_filtered_aggregate_tail.uses_scan_filters = true;
	scan_filtered_aggregate_tail.source_filter_count = 1;
	scan_filtered_aggregate_tail.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	scan_filtered_aggregate_tail.has_accelerated_work = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(scan_filtered_aggregate_tail, parameters);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	scan_filtered_aggregate_tail.selected_hash_join_filter_materialization_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(scan_filtered_aggregate_tail, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
	scan_filtered_aggregate_tail.selected_hash_join_filter_materialization_count = 0;

	auto scan_filtered_grouped_aggregate = scan_filtered_aggregate_tail;
	scan_filtered_grouped_aggregate.generated_backend_stage_count = 2;
	scan_filtered_grouped_aggregate.generated_grouped_aggregate_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(scan_filtered_grouped_aggregate, parameters);
	REQUIRE(profile.generated_grouped_aggregate_stage_count == 2);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO caps unknown filtered source output batch credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 6001215;
	input.source_contract_output_cardinality_unknown = true;
	input.expression_cost = 106;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 0;
	input.native_join_stage_count = 1;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == 0);
	REQUIRE(profile.costed_batches == 1);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 60000;
	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.source_contract_scan_filter_penalty = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	parameters.source_contract_scan_filter_penalty = 4096;

	input.generated_backend_stage_count = 1;
	input.estimated_cardinality = 60000;
	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);

	input.estimated_cardinality = 6001215;
	input.source_contract_input_cardinality = 0;
	input.source_contract_output_cardinality_unknown = false;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.source_contract_output_cardinality_unknown = true;
	input.materialization_elision_count = 1;
	parameters.materialization_elision_benefit = 4096;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.materialization_elision_count == 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO trusts padded finalized dynamic-filter estimates", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 44000;
	input.source_contract_input_cardinality = 6001215;
	input.source_contract_output_cardinality_unknown = true;
	input.finalized_dynamic_filter_cardinality_estimate = true;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.native_join_stage_count = 1;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 3072;
	parameters.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	input.finalized_dynamic_filter_cardinality_estimate = false;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 2;
	input.generated_backend_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
	input.native_join_stage_count = 1;
	input.generated_backend_stage_count = 1;

	input.estimated_cardinality = 12820;
	input.source_contract_output_cardinality_unknown = true;
	input.finalized_dynamic_filter_cardinality_estimate = true;
	input.generated_stage_count = 3;
	input.generated_backend_stage_count = 2;
	input.generated_grouped_aggregate_stage_count = 1;
	input.native_grouped_state_address_lookup_count = 1;
	input.native_join_stage_count = 1;
	input.native_hash_join_build_sink_count = 0;
	input.expression_cost = 45;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 2;
	input.generated_grouped_aggregate_stage_count = 0;
	input.native_grouped_state_address_lookup_count = 0;
	input.native_join_stage_count = 2;
	input.native_aggregate_stage_count = 1;
	input.generated_stage_count = 3;
	input.generated_backend_stage_count = 2;
	input.expression_cost = 61;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 2 * int64_t(parameters.native_operator_stage_benefit));
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality++;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work > 0);
	REQUIRE(profile.native_operator_work == 3 * int64_t(parameters.native_operator_stage_benefit));
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 4;
	input.native_aggregate_stage_count = 0;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 2;
	input.expression_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality++;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work > 0);
	REQUIRE(profile.native_operator_work == 2 * int64_t(parameters.native_operator_stage_benefit));
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 90000;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.startup_base_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == int64_t(parameters.native_operator_stage_benefit));
	REQUIRE(profile.selected_accelerated_runner);
	parameters.startup_base_cost = 10000;

	input.native_delim_join_sink = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == int64_t(parameters.native_operator_stage_benefit));
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.startup_base_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == int64_t(parameters.native_operator_stage_benefit));
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO caps high-expansion unknown-output hash-build estimates", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 35924;
	input.source_contract_input_cardinality = 385;
	input.source_contract_output_cardinality_unknown = true;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.native_join_stage_count = 1;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 3072;
	parameters.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches == 18);
	REQUIRE(profile.costed_batches == 1);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 2464);
	REQUIRE(profile.saved_work_per_batch == 608);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 296279;
	input.source_contract_input_cardinality = 150000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.saved_work_per_batch == 608);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits stateful acceleration through net benefit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 100000;
	input.source_contract_input_cardinality = 100000;
	input.generated_stage_count = 4;
	input.generated_backend_stage_count = 3;
	input.materialization_elision_count = 1;
	input.native_join_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;
	parameters.materialization_elision_benefit = 4096;
	parameters.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches < 64);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 1767425;
	input.source_contract_input_cardinality = 1767425;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO prices unknown-output generated join fusion from physical estimates", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 800000;
	input.source_contract_input_cardinality = 800000;
	input.expression_cost = 61;
	input.generated_stage_count = 3;
	input.generated_backend_stage_count = 2;
	input.native_join_stage_count = 3;
	input.native_aggregate_stage_count = 1;
	input.native_grouped_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.uses_scan_filters = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown == false);
	REQUIRE(profile.selected_accelerated_runner);

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.generated_backend_stage_work == 8192);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	input.expression_cost = 20000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 6001215;
	input.source_contract_input_cardinality = 6001215;
	input.expression_cost = 61;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1024);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	PhysicalRunnerCostInput state_scan_input;
	parameters.materialization_elision_benefit = 4096;
	state_scan_input.estimated_cardinality = 1767425;
	state_scan_input.expression_cost = 50;
	state_scan_input.generated_stage_count = 2;
	state_scan_input.generated_backend_stage_count = 1;
	state_scan_input.materialization_elision_count = 1;
	state_scan_input.full_pipeline = true;
	state_scan_input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	state_scan_input.has_accelerated_work = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(state_scan_input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selected_accelerated_runner);

	state_scan_input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(state_scan_input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	state_scan_input.estimated_cardinality = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(state_scan_input, parameters);
	REQUIRE(profile.batches == 1);
	REQUIRE(profile.costed_batches == 1);
}

TEST_CASE("JIT CBO amortizes parallel generated join grouped lookup fusion", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 1500000;
	input.source_contract_input_cardinality = 1500000;
	input.source_contract_output_cardinality_unknown = true;
	input.expression_cost = 427;
	input.generated_stage_count = 7;
	input.generated_backend_stage_count = 2;
	input.generated_grouped_aggregate_stage_count = 1;
	input.native_grouped_state_address_lookup_count = 1;
	input.native_join_stage_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 3072;
	parameters.startup_base_cost = 10000;
	parameters.vectorized_parallelism = 4;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.stateful_protocol_penalty == 1200);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selected_accelerated_runner);

	input.generated_backend_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 1200);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO prices generated join fusion through candidate upper bounds", "[api][jit]") {
	PhysicalRunnerCostInput pipeline_input;
	pipeline_input.input_scope = PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE;
	pipeline_input.estimated_cardinality = 800000;
	pipeline_input.source_contract_input_cardinality = 800000;
	pipeline_input.source_contract_output_cardinality_unknown = true;
	pipeline_input.native_join_stage_count = 3;
	pipeline_input.full_pipeline = true;
	pipeline_input.uses_scan_filters = true;
	pipeline_input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	pipeline_input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.native_operator_stage_benefit = 1024;
	parameters.startup_base_cost = 10000;

	auto upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	REQUIRE(upper_bound.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
	REQUIRE(upper_bound.generated_backend_stage_count == 3);
	REQUIRE(upper_bound.native_join_stage_count == 3);
	auto profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selected_accelerated_runner);

	pipeline_input.estimated_cardinality = 6001215;
	pipeline_input.source_contract_input_cardinality = 6001215;
	upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.batches > 1024);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selected_accelerated_runner);

	pipeline_input.estimated_cardinality = 800000;
	pipeline_input.source_contract_input_cardinality = 800000;
	pipeline_input.native_hash_join_build_sink_count = 1;
	upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.native_hash_join_build_sink_count == 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.generated_backend_stage_count == 3);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.materialization_elision_count == 0);
	REQUIRE(profile.stateful_protocol_penalty == 4 * 720);
	REQUIRE(profile.selection_reason.rfind("admitted_", 0) == 0);
	REQUIRE(profile.selected_accelerated_runner);

	pipeline_input.native_join_stage_count = 0;
	pipeline_input.expression_cost = 512;
	upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.native_hash_join_build_sink_count == 0);
	REQUIRE(profile.generated_backend_stage_count == 1);
	REQUIRE(profile.materialization_elision_count == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
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
	PhysicalRunnerCostParameters high_startup_margin_parameters;
	high_startup_margin_parameters.compiled_vectorized_runner_available = true;
	high_startup_margin_parameters.generated_stage_benefit = 1;
	high_startup_margin_parameters.startup_base_cost = 32000;
	high_startup_margin_parameters.startup_margin_basis_points = 5000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, high_startup_margin_parameters);
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
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK));
	REQUIRE_FALSE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                                     ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP));
	REQUIRE(profile.selected_accelerated_runner);

	input.expression_cost = 10000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 24096);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO charges selected hash-join generated filter materialization", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 10;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.selected_hash_join_filter_materialization_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_hash_join_filter_materialization_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.selected_hash_join_filter_materialization_penalty == 4096);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.selected_hash_join_filter_materialization_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_hash_join_filter_materialization_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 4096);
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK));
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO admits a proven generated grouped lookup replacement", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 6001215;
	input.source_contract_input_cardinality = 6001215;
	input.expression_cost = 37;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 1;
	input.generated_grouped_aggregate_stage_count = 1;
	input.native_grouped_state_address_lookup_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.materialization_elision_benefit = 4096;
	parameters.vectorized_parallelism = 12;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_count == 1);
	REQUIRE(profile.generated_grouped_aggregate_stage_count == 1);
	REQUIRE(profile.native_grouped_state_address_lookup_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.generated_stage_work > profile.generated_backend_stage_work);
	REQUIRE(profile.stateful_protocol_penalty > profile.generated_backend_stage_work);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.materialization_elision_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.materialization_elision_work > 0);
	REQUIRE(profile.native_grouped_state_address_lookup_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.stateful_protocol_penalty > profile.generated_backend_stage_work);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	parameters.vectorized_parallelism = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.materialization_elision_count = 0;
	input.native_grouped_state_address_lookup_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO rejects a large low-work generated grouped update", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 30000000;
	input.source_contract_input_cardinality = 30000000;
	input.expression_cost = 16;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 2;
	input.generated_grouped_aggregate_stage_count = 2;
	input.grouped_aggregate_estimated_cardinality = 9;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.vectorized_parallelism = 1;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_grouped_aggregate_stage_count == 2);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 39;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 40;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work > 0);
	REQUIRE(profile.generated_stage_work > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.native_grouped_state_address_lookup_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.expression_cost = 64;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work > 0);
	REQUIRE(profile.generated_stage_work > 0);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO prices estimated grouped reduction in parallel", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 6000000;
	input.source_contract_input_cardinality = 6000000;
	input.grouped_aggregate_estimated_cardinality = 1900000;
	input.expression_cost = 37;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 1;
	input.generated_grouped_aggregate_stage_count = 1;
	input.native_grouped_state_address_lookup_count = 1;
	input.materialization_elision_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;
	parameters.materialization_elision_benefit = 4096;
	parameters.vectorized_parallelism = 4;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.grouped_aggregate_estimated_cardinality == 1900000);
	REQUIRE(profile.stateful_protocol_penalty == 480);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.grouped_aggregate_estimated_cardinality = 3000001;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > profile.generated_backend_stage_work);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO requires enough batches to amortize stateful backend startup", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 31680;
	input.source_contract_input_cardinality = 31680;
	input.expression_cost = 84;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 1;
	input.materialization_elision_count = 1;
	input.full_pipeline = true;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized_runner_available = true;
	parameters.generated_stage_benefit = 4096;
	parameters.materialization_elision_benefit = 4096;
	parameters.startup_base_cost = 10000;
	parameters.startup_margin_basis_points = 0;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.estimated_cardinality = 316800;
	input.source_contract_input_cardinality = 316800;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.materialization_elision_work == 4096);
	REQUIRE(profile.selected_accelerated_runner);
}

TEST_CASE("JIT CBO amortizes native grouped state-address lookup through deep serial native work", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 6001215;
	input.source_contract_input_cardinality = 6001215;
	input.expression_cost = 53;
	input.generated_stage_count = 4;
	input.generated_backend_stage_count = 1;
	input.generated_grouped_aggregate_stage_count = 1;
	input.native_grouped_state_address_lookup_count = 1;
	input.full_pipeline = true;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_grouped_state_address_lookup_count == 1);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = input.source_contract_input_cardinality / 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.generated_backend_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 2 * 4096);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);
	input.generated_backend_stage_count = 1;

	input.materialization_elision_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);
	input.materialization_elision_count = 0;

	input.native_join_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 2 * 720);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selected_accelerated_runner);

	input.estimated_cardinality = input.source_contract_input_cardinality;
	input.native_hash_join_build_sink_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);

	input.native_hash_join_build_sink_count = 0;
	parameters.vectorized_parallelism = 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.selected_accelerated_runner);
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
		           event.output_rows == 100000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 10000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 10000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 2053 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 100000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 100000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 100000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 10000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 10000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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
		           event.output_rows == 100000 && !event.generated_stage_runtime.empty();
	    },
	    [](const ExecutionRegionEvent &event) { RequireNativeGeneratedRuntimeWork(event); });
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

TEST_CASE("JIT auto planner skips native-contract projection glue in pipeline CBO", "[api][jit]") {
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
		               PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
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

TEST_CASE("JIT auto planner amortizes grouped lookup for generated division aggregate", "[api][jit]") {
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
		    return IsSljitRegionEvent(event) && EventStatus(event) == "compiled" && event.has_candidate &&
		           event.runner_cost.selected_accelerated_runner &&
		           event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE &&
		           event.runner_cost.native_grouped_state_address_lookup_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.has_candidate);
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
		    REQUIRE(event.runner_cost.generated_work_class == PhysicalRunnerGeneratedWorkClass::COMPUTE);
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.generated_grouped_aggregate_stage_count > 0);
		    REQUIRE(event.runner_cost.native_grouped_state_address_lookup_count > 0);
		    REQUIRE(event.runner_cost.materialization_elision_count > 0);
		    REQUIRE(event.runner_cost.stateful_protocol_penalty == 0);
		    REQUIRE(event.runner_cost.saved_work_per_batch > 0);
		    REQUIRE(event.runner_cost.selection_reason.rfind("admitted_", 0) == 0);
		    REQUIRE(event.stage_timings.graph_build_time_us > 0);
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

TEST_CASE("JIT auto planner rejects tiny aggregate pipeline in pipeline CBO", "[api][jit]") {
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
		    REQUIRE(event.runner_cost.selection_reason == "rejected_insufficient_benefit");
	    });

	for (auto &event : manager.GetEvents()) {
		REQUIRE(EventStatus(event) != "compiled");
	}
}

TEST_CASE("JIT CBO uses finalized perfect-hash dynamic filter cardinality", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dynamic_filter_fact AS "
	                          "SELECT (i % 50000)::INTEGER AS k1, (i % 10000)::INTEGER AS k2, "
	                          "       i::BIGINT AS payload, "
	                          "       CASE WHEN i % 7 = 0 THEN 'keep' ELSE 'drop' END AS tag "
	                          "FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dynamic_filter_dim1 AS "
	                          "SELECT i::INTEGER AS k1 FROM range(500) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dynamic_filter_dim2 AS "
	                          "SELECT i::INTEGER AS k2 FROM range(2000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(f.payload) "
	                        "FROM jit_dynamic_filter_fact f "
	                        "JOIN jit_dynamic_filter_dim1 d1 USING (k1) "
	                        "JOIN jit_dynamic_filter_dim2 d2 USING (k2)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 150499000);
	bool saw_exact_perfect_filter_proof = false;
	for (auto &event : manager.GetEvents()) {
		if (StringUtil::Contains(EventJitRuntimePathCounts(event),
		                         "hash_join_probe.perfect_probe.exact_source_filter=")) {
			saw_exact_perfect_filter_proof = true;
			REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
			break;
		}
	}
	REQUIRE(saw_exact_perfect_filter_proof);
	string observed_costs;
	for (auto &event : manager.GetEvents()) {
		if (!event.runner_cost.present) {
			continue;
		}
		observed_costs += "status=" + EventStatus(event) +
		                  ",candidate=" + string(event.has_candidate ? "true" : "false") +
		                  ",rows=" + std::to_string(event.runner_cost.rows) +
		                  ",source_rows=" + std::to_string(event.runner_cost.source_contract_input_rows) +
		                  ",joins=" + std::to_string(event.candidate_traits.hash_join_operator_count) + ";";
	}
	INFO(observed_costs);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.candidate_traits.source_contract_output_cardinality_unknown &&
		           event.candidate_traits.hash_join_operator_count >= 1 &&
		           event.runner_cost.source_contract_input_rows == 200000;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.rows > 0);
		    REQUIRE(event.runner_cost.rows < event.runner_cost.source_contract_input_rows / 10);
		    REQUIRE(event.runner_cost.source_contract_output_cardinality_unknown);
	    });

	ClearJitTrace(manager, true);
	result = con.Query("SELECT sum(f.payload) FROM jit_dynamic_filter_fact f "
	                   "JOIN jit_dynamic_filter_dim1 d1 USING (k1) "
	                   "WHERE contains(f.tag, 'keep')");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 21521500);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "hash_join_probe.perfect_probe.exact_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
	    });
}

TEST_CASE("JIT exact dynamic filters compose with bitpacked storage scans", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='bitpacking'"));
	auto db_path = TestCreatePath("jit_bitpacked_exact_dynamic_filter.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS packed (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.fact AS "
	                          "SELECT (i % 50000)::INTEGER AS k, i::BIGINT AS payload, "
	                          "       CASE WHEN i % 7 = 0 THEN 'keep' ELSE 'drop' END AS tag "
	                          "FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.dim AS SELECT i::INTEGER AS k FROM range(100, 200) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT packed"));
	auto storage = con.Query("SELECT count(*) FROM pragma_storage_info('packed.fact') "
	                         "WHERE column_name = 'k' AND compression = 'BitPacking'");
	REQUIRE_NO_FAIL(*storage);
	REQUIRE(storage->GetValue(0, 0).GetValue<int64_t>() > 0);

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(f.payload) FROM packed.fact f JOIN packed.dim d USING(k) "
	                        "WHERE f.k BETWEEN 120 AND 180");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 18336600);

	int64_t mixed_expected = 0;
	for (int64_t i = 0; i < 200000; i++) {
		auto key = i % 50000;
		if (key >= 120 && key <= 180 && i % 7 == 0) {
			mixed_expected += i;
		}
	}
	result = con.Query("SELECT sum(f.payload) FROM packed.fact f JOIN packed.dim d USING(k) "
	                   "WHERE f.k BETWEEN 120 AND 180 AND contains(f.tag, 'keep')");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == mixed_expected);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "hash_join_probe.perfect_probe.exact_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
	    });

	REQUIRE_NO_FAIL(con.Query("DETACH packed"));
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT Bloom dynamic filters preserve bitpacked scan semantics", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='bitpacking'"));
	auto db_path = TestCreatePath("jit_bitpacked_bloom_dynamic_filter.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS packed (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.fact_big AS "
	                          "SELECT CASE WHEN i % 257 = 0 THEN NULL ELSE i END::BIGINT AS k "
	                          "FROM range(700000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.fact_int AS "
	                          "SELECT CASE WHEN i % 257 = 0 THEN NULL ELSE i END::INTEGER AS k "
	                          "FROM range(700000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.dim AS "
	                          "SELECT (i * 3)::BIGINT AS k FROM range(524289) tbl(i) "
	                          "UNION ALL SELECT NULL::BIGINT"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT packed"));
	auto storage = con.Query("SELECT count(*) FROM pragma_storage_info('packed.fact_big') "
	                         "WHERE column_name = 'k' AND compression = 'BitPacking'");
	REQUIRE_NO_FAIL(*storage);
	REQUIRE(storage->GetValue(0, 0).GetValue<int64_t>() > 0);
	storage = con.Query("SELECT count(*) FROM pragma_storage_info('packed.fact_int') "
	                    "WHERE column_name = 'k' AND compression = 'BitPacking'");
	REQUIRE_NO_FAIL(*storage);
	REQUIRE(storage->GetValue(0, 0).GetValue<int64_t>() > 0);

	const string exact_query = "SELECT count(*) FROM packed.fact_big f JOIN packed.dim d ON f.k = d.k";
	const string null_equal_query =
	    "SELECT count(*) FROM packed.fact_big f JOIN packed.dim d ON f.k IS NOT DISTINCT FROM d.k";
	const string cast_query = "SELECT count(*) FROM packed.fact_int f JOIN packed.dim d ON f.k = d.k";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto exact_reference = con.Query(exact_query);
	REQUIRE_NO_FAIL(*exact_reference);
	REQUIRE(exact_reference->GetValue(0, 0).GetValue<int64_t>() == 232426);
	auto null_equal_reference = con.Query(null_equal_query);
	REQUIRE_NO_FAIL(*null_equal_reference);
	REQUIRE(null_equal_reference->GetValue(0, 0).GetValue<int64_t>() == 235150);
	auto cast_reference = con.Query(cast_query);
	REQUIRE_NO_FAIL(*cast_reference);
	REQUIRE(cast_reference->GetValue(0, 0).GetValue<int64_t>() == 232426);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto exact_result = con.Query(exact_query);
	REQUIRE_NO_FAIL(*exact_result);
	REQUIRE(exact_result->GetValue(0, 0) == exact_reference->GetValue(0, 0));
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       StringUtil::Contains(EventJitRuntimePathCounts(event),
		                            "hash_join_probe.perfect_probe.exact_source_filter=");
	});
	ClearJitTrace(manager, true);
	auto null_equal_result = con.Query(null_equal_query);
	REQUIRE_NO_FAIL(*null_equal_result);
	REQUIRE(null_equal_result->GetValue(0, 0) == null_equal_reference->GetValue(0, 0));
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.regular_probe.generated=");
	});
	auto cast_result = con.Query(cast_query);
	REQUIRE_NO_FAIL(*cast_result);
	REQUIRE(cast_result->GetValue(0, 0) == cast_reference->GetValue(0, 0));

	REQUIRE_NO_FAIL(con.Query("DETACH packed"));
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT CBO carries finalized dynamic filter cardinality through regular semi joins", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_regular_dynamic_filter_fact AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS payload FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_regular_dynamic_filter_dim AS "
	                          "SELECT (i * 1000000)::BIGINT AS k FROM range(500) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_regular_dynamic_filter_fact f "
	                        "SEMI JOIN jit_regular_dynamic_filter_dim d USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.candidate_traits.finalized_dynamic_filter_cardinality_estimate &&
		           event.candidate_traits.hash_join_operator_count == 1 &&
		           event.runner_cost.source_contract_input_rows == 200000;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.source_contract_output_cardinality_unknown);
		    REQUIRE(event.runner_cost.rows > 0);
		    REQUIRE(event.runner_cost.rows <= 1000);
	    });
}

TEST_CASE("JIT CBO preserves empty finalized dynamic filter cardinality", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_empty_dynamic_filter_fact AS "
	                          "SELECT i::BIGINT AS k FROM range(200000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query(
	    "SELECT count(*) FROM jit_empty_dynamic_filter_fact f "
	    "SEMI JOIN (SELECT CASE WHEN random()>=0 THEN 0::BIGINT ELSE 100000::BIGINT END AS k FROM range(1)) l "
	    "USING (k) "
	    "SEMI JOIN (SELECT CASE WHEN random()>=0 THEN 100000::BIGINT ELSE 0::BIGINT END AS k FROM range(1)) h "
	    "USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.candidate_traits.finalized_dynamic_filter_cardinality_estimate &&
		           event.runner_cost.source_contract_input_rows == 200000;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.rows == 1);
		    REQUIRE(event.runner_cost.costed_batches == 1);
	    });
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

TEST_CASE("JIT diagnostic tracing records generic table-function source ownership", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, false, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i * 2) FROM range(1000) tbl(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "999000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.source_kind == ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN &&
		           event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "table-function source contract"));
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "source-contract-blocker"));
	    });

	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventStatus(event) != "compiled") {
			continue;
		}
		REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
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
		    REQUIRE(event.runner_cost.selection_reason == "rejected_insufficient_benefit");
	    });
}
