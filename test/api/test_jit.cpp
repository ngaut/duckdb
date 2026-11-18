#include "test_jit_helpers.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_platform.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_region_plan_internal.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_register_layout.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/execution/execution_region_kernel.hpp"

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
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

struct UnitTestStageMutationBackendState {
	bool selected_mismatched_stage = false;
	bool compile_called = false;
};

class UnitTestStageMutationBackend : public ExecutionRegionBackend {
public:
	explicit UnitTestStageMutationBackend(UnitTestStageMutationBackendState &state_p) : state(state_p) {
	}

	string Name() const override {
		return "unit_test_stage_mutation_backend";
	}

	string Description() const override {
		return "unit test backend that mutates core stage execution";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		ExecutionRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
		plan.AddCompactNode(ExecutionRegionOperatorKind::GENERIC, ExecutionRegionLoweringKind::NATIVE,
		                    "unit test stage mutation");

		ExecutionRegionSelectedSourceRecipe source_recipe;
		source_recipe.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
		plan.SetSourceRecipe(std::move(source_recipe));

		auto &backend_state = state.get();
		for (idx_t stage_idx = 0; stage_idx < input.candidate.stage_plan.stages.size(); stage_idx++) {
			auto &stage = input.candidate.stage_plan.stages[stage_idx];
			auto execution = stage.execution;
			if (!backend_state.selected_mismatched_stage && stage.kind == ExecutionRegionStageKind::APPEND_SINK &&
			    execution == ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
				execution = ExecutionRegionStageExecutionKind::GENERATED_IR;
				backend_state.selected_mismatched_stage = true;
			}
			plan.SelectStage(stage_idx, execution);
		}
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		state.get().compile_called = true;
		return ExecutionRegionCompileResult::Unsupported("unit test backend must be rejected before compilation");
	}

private:
	reference<UnitTestStageMutationBackendState> state;
};

class UnitTestExecutionRegionArtifact : public ExecutionRegionArtifact {
public:
	explicit UnitTestExecutionRegionArtifact(idx_t value_p) : value(value_p) {
	}

	idx_t value;
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

struct alignas(16) SljitSimdBinaryTestInput {
	uint8_t left[16];
	uint8_t right[16];
	uint8_t output[16];
};

template <class T, std::size_t N>
static std::array<T, N> RunSljitSimdBinary(sljit_s32 elem_type, sljit_s32 operation, const std::array<T, N> &left,
                                           const std::array<T, N> &right, bool alias_right = false) {
	static_assert(sizeof(T) * N == 16, "SLJIT SIMD backend tests use one 128-bit register");
	SljitSimdBinaryTestInput input {};
	memcpy(input.left, left.data(), sizeof(input.left));
	memcpy(input.right, right.data(), sizeof(input.right));

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		throw InternalException("failed to create SLJIT compiler for SIMD backend test");
	}
	auto require_success = [&](sljit_s32 result) {
		if (result != SLJIT_SUCCESS) {
			sljit_free_compiler(compiler);
			throw InternalException("SLJIT SIMD backend test emission failed with error %d", result);
		}
	};
	require_success(sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 1 | SLJIT_ENTER_VECTOR(3), 1, 0));
	const auto simd_type = SLJIT_SIMD_REG_128 | elem_type;
	require_success(sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S0),
	                                    offsetof(SljitSimdBinaryTestInput, left)));
	require_success(sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR1, SLJIT_MEM1(SLJIT_S0),
	                                    offsetof(SljitSimdBinaryTestInput, right)));
	const auto destination = alias_right ? SLJIT_VR1 : SLJIT_VR2;
	require_success(sljit_emit_simd_op2(compiler, simd_type | operation, destination, SLJIT_VR0, SLJIT_VR1, 0));
	require_success(sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, destination, SLJIT_MEM1(SLJIT_S0),
	                                    offsetof(SljitSimdBinaryTestInput, output)));
	require_success(sljit_emit_return_void(compiler));

	using TestFunction = void(SLJIT_FUNC *)(SljitSimdBinaryTestInput *);
	TestFunction function = nullptr;
	string error;
	auto handle = FinishSljitCode(compiler, function, error);
	if (!handle) {
		throw InternalException("SLJIT SIMD backend test code generation failed: %s", error);
	}
	function(&input);
	std::array<T, N> result;
	memcpy(result.data(), input.output, sizeof(input.output));
	return result;
}

template <class T, std::size_t N>
static void RequireSljitSimdBinaryIfSupported(sljit_s32 elem_type, sljit_s32 operation, const std::array<T, N> &left,
                                              const std::array<T, N> &right, const std::array<T, N> &expected,
                                              bool alias_right = false) {
	if (!SljitSimd128Op2Supported(elem_type, operation)) {
		return;
	}
	REQUIRE(RunSljitSimdBinary(elem_type, operation, left, right, alias_right) == expected);
}

} // namespace

TEST_CASE("SLJIT backend requires an addressable native-vector register ABI", "[api][jit][sljit]") {
	const auto &capabilities = GetSljitTargetCapabilities();
	REQUIRE(SljitPlatformAvailable() == capabilities.BackendAvailable());
	REQUIRE(capabilities.registers.addressable_saved_register_count <= capabilities.registers.saved_register_count);
	for (sljit_s32 saved_index = 0; saved_index < capabilities.registers.addressable_saved_register_count;
	     saved_index++) {
		REQUIRE(SljitSavedRegisterAt(saved_index) != 0);
	}
	REQUIRE(SljitSavedRegisterAt(-1) == 0);
	REQUIRE(SljitSavedRegisterAt(capabilities.registers.addressable_saved_register_count) == 0);

	if (capabilities.BackendAvailable()) {
		const auto &native_vector = GetSljitNativeVectorRegisterLayout();
		const auto has_optional_invariant = capabilities.registers.SupportsLayout(5, 7);
		REQUIRE(native_vector.Available());
		REQUIRE(native_vector.saved_register_count == (has_optional_invariant ? 7 : 6));
		REQUIRE(native_vector.HasOptionalInvariant() == has_optional_invariant);

		const auto &perfect_hash = GetSljitPerfectHashRegisterLayout();
		CHECK(perfect_hash.has_dedicated_state == capabilities.registers.SupportsLayout(5, 8));
		CHECK(perfect_hash.has_group_data == capabilities.registers.SupportsLayout(5, 10));

		const auto &ungrouped = GetSljitUngroupedAggregateRegisterLayout();
		CHECK(ungrouped.has_source_data_hoists == capabilities.registers.SupportsLayout(5, 10));
		CHECK(ungrouped.has_register_accumulators ==
		      capabilities.registers.CanEnter(10, ungrouped.source_data_saved_register_count));
		if (ungrouped.has_register_accumulators) {
			CHECK(ungrouped.accumulator_scratch_register_count == 10);
		}

		const auto &primitive_run = GetSljitPrimitiveRunRegisterLayout();
		CHECK(primitive_run.supported == capabilities.SupportsPrimitiveRunRegisterABI());
		CHECK(primitive_run.has_affine_accumulators ==
		      (primitive_run.supported && capabilities.registers.SupportsLayout(7, 8)));
		CHECK(primitive_run.has_output_pointer_hoists ==
		      (primitive_run.supported && capabilities.registers.SupportsLayout(7, 10)));

		const auto &perfect_hash_probe = GetSljitPerfectHashProbeRegisterLayout();
		CHECK(perfect_hash_probe.has_invariant_hoists == capabilities.registers.SupportsLayout(5, 10));
	}
}

TEST_CASE("SLJIT target capability policy covers representative register ABIs", "[api][jit][sljit]") {
	auto target = [](SljitTargetArchitecture architecture, sljit_s32 register_count, sljit_s32 saved_register_count,
	                 sljit_s32 addressable_saved_register_count, sljit_s32 machine_word_bytes) {
		SljitTargetCapabilities result;
		result.architecture = architecture;
		result.registers.register_count = register_count;
		result.registers.saved_register_count = saved_register_count;
		result.registers.addressable_saved_register_count = addressable_saved_register_count;
		result.machine_word_bytes = machine_word_bytes;
		result.platform_available = true;
		return result;
	};
	struct TargetCase {
		const char *name;
		SljitTargetCapabilities capabilities;
		sljit_s32 max_native_saved_registers;
		bool native_layout_fits;
		bool backend_available;
		bool primitive_run_available;
	};
	auto unavailable = target(SljitTargetArchitecture::X86_64, 13, 6, 6, 8);
	unavailable.platform_available = false;
	const std::array<TargetCase, 7> cases {{
	    {"x86-32 virtual saved registers", target(SljitTargetArchitecture::X86_32, 12, 7, 3, 4), 3, false, false,
	     false},
	    {"x86-64 SysV", target(SljitTargetArchitecture::X86_64, 13, 6, 6, 8), 6, true, true, true},
	    {"x86-64 Windows", target(SljitTargetArchitecture::X86_64, 13, 8, 8, 8), 8, true, true, true},
	    {"ARM64", target(SljitTargetArchitecture::ARM_64, 26, 10, 10, 8), 10, true, true, true},
	    {"ARM32", target(SljitTargetArchitecture::ARM_32, 12, 8, 8, 4), 7, true, false, false},
	    {"S390X constrained enter frame", target(SljitTargetArchitecture::S390X, 12, 8, 8, 8), 7, true, true, false},
	    {"unavailable x86-64 backend", unavailable, 6, true, false, false},
	}};
	for (auto &test : cases) {
		CAPTURE(test.name);
		CHECK(test.capabilities.BackendAvailable() == test.backend_available);
		CHECK(test.capabilities.SupportsPrimitiveRunRegisterABI() == test.primitive_run_available);
		CHECK(test.capabilities.registers.SupportsLayout(5, 6) == test.native_layout_fits);
		CHECK(test.capabilities.registers.MaxAddressableSavedRegisters(5) == test.max_native_saved_registers);
		CHECK(test.capabilities.registers.MaxAddressableSavedRegisters(-1) == 0);
		CHECK(test.capabilities.registers.MaxAddressableSavedRegisters(test.capabilities.registers.register_count +
		                                                               1) == 0);
	}
}

TEST_CASE("SLJIT SIMD binary backend operations preserve lane semantics", "[api][jit][sljit]") {
	if (!GetSljitTargetCapabilities().simd_available) {
		return;
	}
	using I8x16 = std::array<int8_t, 16>;
	using I16x8 = std::array<int16_t, 8>;
	using I32x4 = std::array<int32_t, 4>;
	using I64x2 = std::array<int64_t, 2>;
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_8, SLJIT_SIMD_OP2_ADD,
	                                  I8x16 {7, -9, 41, -100, 1, 2, 3, 4, -1, -2, -3, -4, 10, 20, 30, 40},
	                                  I8x16 {3, 5, -11, 101, 4, 3, 2, 1, 1, 2, 3, 4, -10, -20, -30, -40},
	                                  I8x16 {10, -4, 30, 1, 5, 5, 5, 5, 0, 0, 0, 0, 0, 0, 0, 0});
	RequireSljitSimdBinaryIfSupported(
	    SLJIT_SIMD_ELEM_16, SLJIT_SIMD_OP2_SUB, I16x8 {7, -9, 41, -100, 1000, -2000, 3000, -4000},
	    I16x8 {3, 5, -11, 101, -500, 500, 1000, -1000}, I16x8 {4, -14, 52, -201, 1500, -2500, 2000, -3000}, true);
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_16, SLJIT_SIMD_OP2_MUL, I16x8 {7, -9, 41, -100, 10, -20, 30, -40},
	                                  I16x8 {3, 5, -11, 101, -5, 5, 10, -10},
	                                  I16x8 {21, -45, -451, -10100, -50, -100, 300, 400});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_8, SLJIT_SIMD_OP2_CMPGT,
	                                  I8x16 {7, -9, 41, -100, 1, 2, 3, 4, -1, -2, -3, -4, 10, 20, 30, 40},
	                                  I8x16 {3, 5, 41, -101, 0, 3, 2, 5, -2, -1, -4, -3, 10, 19, 31, 39},
	                                  I8x16 {-1, 0, 0, -1, -1, 0, -1, 0, -1, 0, -1, 0, 0, -1, 0, -1}, true);
	RequireSljitSimdBinaryIfSupported(
	    SLJIT_SIMD_ELEM_16, SLJIT_SIMD_OP2_CMPEQ, I16x8 {7, -9, 41, -100, 1000, -2000, 3000, -4000},
	    I16x8 {3, -9, 41, -101, 1000, 2000, -3000, -4000}, I16x8 {0, -1, -1, 0, -1, 0, 0, -1});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_32, SLJIT_SIMD_OP2_ADD, I32x4 {7, -9, 41, -100},
	                                  I32x4 {3, 5, -11, 101}, I32x4 {10, -4, 30, 1});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_32, SLJIT_SIMD_OP2_SUB, I32x4 {7, -9, 41, -100},
	                                  I32x4 {3, 5, -11, 101}, I32x4 {4, -14, 52, -201}, true);
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_32, SLJIT_SIMD_OP2_MUL, I32x4 {7, -9, 41, -100},
	                                  I32x4 {3, 5, -11, 101}, I32x4 {21, -45, -451, -10100});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_32, SLJIT_SIMD_OP2_CMPGT, I32x4 {7, -9, 41, -100},
	                                  I32x4 {3, 5, 41, -101}, I32x4 {-1, 0, 0, -1}, true);
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_32, SLJIT_SIMD_OP2_CMPEQ, I32x4 {7, -9, 41, -100},
	                                  I32x4 {3, -9, 41, -101}, I32x4 {0, -1, -1, 0});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_64, SLJIT_SIMD_OP2_ADD, I64x2 {7, -9}, I64x2 {3, 5},
	                                  I64x2 {10, -4});
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_64, SLJIT_SIMD_OP2_SUB, I64x2 {7, -9}, I64x2 {3, 5},
	                                  I64x2 {4, -14}, true);
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_64, SLJIT_SIMD_OP2_CMPGT, I64x2 {7, -9}, I64x2 {3, -10},
	                                  I64x2 {-1, -1}, true);
	RequireSljitSimdBinaryIfSupported(SLJIT_SIMD_ELEM_64, SLJIT_SIMD_OP2_CMPEQ, I64x2 {7, -9}, I64x2 {3, -9},
	                                  I64x2 {0, -1});
}

TEST_CASE("Execution region artifact cache publishes one build to concurrent executions", "[api][jit]") {
	ExecutionRegionArtifactCache cache;
	std::mutex gate_lock;
	std::condition_variable gate_condition;
	bool build_entered = false;
	std::atomic<idx_t> build_count {0};
	std::atomic<bool> first_builder {false};
	std::atomic<bool> second_hit {false};
	shared_ptr<const ExecutionRegionCachedArtifact> second;
	shared_ptr<const UnitTestExecutionRegionArtifact> published_artifact;

	std::thread first_thread([&] {
		auto reservation = cache.LookupOrReserve("shared-artifact");
		first_builder.store(reservation.IsBuilder());
		build_count.fetch_add(1);
		{
			lock_guard<std::mutex> guard(gate_lock);
			build_entered = true;
			gate_condition.notify_all();
		}
		while (cache.WaitingCount("shared-artifact") != 1) {
			std::this_thread::yield();
		}
		auto artifact = make_shared_ptr<UnitTestExecutionRegionArtifact>(4096);
		published_artifact = artifact;
		auto cached = make_shared_ptr<ExecutionRegionCachedArtifact>();
		cached->artifact = std::move(artifact);
		cached->execution_mode = ExecutionRegionExecutionMode::NATIVE;
		cached->compile_reason = "unit-test-compile";
		cached->ir = "unit-test-ir";
		cache.Publish(reservation, std::move(cached));
	});
	{
		std::unique_lock<std::mutex> guard(gate_lock);
		gate_condition.wait(guard, [&] { return build_entered; });
	}
	std::thread second_thread([&] {
		auto reservation = cache.LookupOrReserve("shared-artifact");
		second_hit.store(reservation.IsHit());
		second = reservation.Cached();
	});
	first_thread.join();
	second_thread.join();

	REQUIRE(build_count.load() == 1);
	REQUIRE(first_builder.load());
	REQUIRE(second_hit.load());
	REQUIRE(second);
	REQUIRE(second->artifact == published_artifact);
	REQUIRE(second->execution_mode == ExecutionRegionExecutionMode::NATIVE);
	REQUIRE(second->compile_reason == "unit-test-compile");
	REQUIRE(second->ir == "unit-test-ir");
	REQUIRE(cache.ReadyCount() == 1);
}

TEST_CASE("Execution region artifact cache retries after an aborted build", "[api][jit]") {
	ExecutionRegionArtifactCache cache;
	auto failed = cache.LookupOrReserve("retry-after-failure");
	REQUIRE(failed.IsBuilder());
	cache.Abort(failed);

	auto retry = cache.LookupOrReserve("retry-after-failure");
	REQUIRE(retry.IsBuilder());
	auto cached = make_shared_ptr<ExecutionRegionCachedArtifact>();
	cached->artifact = make_shared_ptr<UnitTestExecutionRegionArtifact>(2048);
	cached->execution_mode = ExecutionRegionExecutionMode::NATIVE;
	cache.Publish(retry, std::move(cached));

	auto recovered = cache.LookupOrReserve("retry-after-failure");
	REQUIRE(recovered.IsHit());
	auto artifact = dynamic_cast<const UnitTestExecutionRegionArtifact *>(recovered.Cached()->artifact.get());
	REQUIRE(artifact);
	REQUIRE(artifact->value == 2048);
}

TEST_CASE("Execution region artifact cache eviction preserves active artifact ownership", "[api][jit]") {
	ExecutionRegionArtifactCache cache(2);
	auto publish = [&](const string &key, idx_t value) {
		auto reservation = cache.LookupOrReserve(key);
		REQUIRE(reservation.IsBuilder());
		auto cached = make_shared_ptr<ExecutionRegionCachedArtifact>();
		cached->artifact = make_shared_ptr<UnitTestExecutionRegionArtifact>(value);
		cached->execution_mode = ExecutionRegionExecutionMode::NATIVE;
		cache.Publish(reservation, std::move(cached));
	};
	publish("one", 1);
	publish("two", 2);
	auto active = cache.LookupOrReserve("one").Cached()->artifact;
	publish("three", 3);
	REQUIRE(cache.ReadyCount() == 2);
	auto active_artifact = dynamic_cast<const UnitTestExecutionRegionArtifact *>(active.get());
	REQUIRE(active_artifact);
	REQUIRE(active_artifact->value == 1);
	auto evicted = cache.LookupOrReserve("two");
	REQUIRE(evicted.IsBuilder());
	cache.Abort(evicted);
}

TEST_CASE("SLJIT artifact cache keys include compile-affecting bindings", "[api][jit]") {
	REQUIRE(ComposeSljitRegionArtifactCacheKey("same-semantic-plan", "source-min=0;source-max=10") !=
	        ComposeSljitRegionArtifactCacheKey("same-semantic-plan", "source-min=0;source-max=20"));
	REQUIRE(ComposeSljitRegionArtifactCacheKey("same-semantic-plan", "source-min=0;source-max=10") ==
	        ComposeSljitRegionArtifactCacheKey("same-semantic-plan", "source-min=0;source-max=10"));
	// the length prefix keeps a delimiter inside one part from shifting the boundary between the parts
	REQUIRE(ComposeSljitRegionArtifactCacheKey("a;binding=1:b", "") != ComposeSljitRegionArtifactCacheKey("a", "b"));

	SljitNativeRegionPlan one_delimited_value;
	one_delimited_value.source_min_values.emplace_back("x|VARCHAR:y");
	SljitNativeRegionPlan two_plain_values;
	two_plain_values.source_min_values.emplace_back("x");
	two_plain_values.source_min_values.emplace_back("y");
	REQUIRE(BuildSljitRegionArtifactBindingKey(one_delimited_value) !=
	        BuildSljitRegionArtifactBindingKey(two_plain_values));

	SljitNativeRegionPlan null_value;
	null_value.source_min_values.emplace_back(LogicalType::VARCHAR);
	SljitNativeRegionPlan literal_null;
	literal_null.source_min_values.emplace_back("NULL");
	REQUIRE(BuildSljitRegionArtifactBindingKey(null_value) != BuildSljitRegionArtifactBindingKey(literal_null));

	SljitNativeRegionExpressionPlan first_double;
	first_double.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT;
	first_double.double_constant = 1.0000001;
	auto second_double = first_double.Copy();
	second_double.double_constant = 1.0000002;
	REQUIRE(BuildSljitExpressionArtifactKey(first_double) != BuildSljitExpressionArtifactKey(second_double));

	first_double.double_constant = 0.0;
	second_double.double_constant = -0.0;
	REQUIRE(BuildSljitExpressionArtifactKey(first_double) != BuildSljitExpressionArtifactKey(second_double));

	SljitNativeRegionExpressionPlan first_predicate;
	first_predicate.kind = SljitNativeRegionExpressionKind::PREDICATE;
	first_predicate.predicate = make_uniq<SljitNativePredicate>();
	first_predicate.predicate->kind = SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT;
	first_predicate.predicate->double_constant = 1.0000001;
	auto second_predicate = first_predicate.Copy();
	second_predicate.predicate->double_constant = 1.0000002;
	REQUIRE(BuildSljitExpressionArtifactKey(first_predicate) != BuildSljitExpressionArtifactKey(second_predicate));
}

TEST_CASE("SLJIT operator descriptors own exactly one kind payload", "[api][jit]") {
	const vector<SljitNativeRegionOpKind> kinds {
	    SljitNativeRegionOpKind::FILTER,          SljitNativeRegionOpKind::PROJECTION,
	    SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::HASH_JOIN_BUILD,
	    SljitNativeRegionOpKind::ORDER_SINK,      SljitNativeRegionOpKind::APPEND_SINK,
	    SljitNativeRegionOpKind::AGGREGATE_UPDATE};
	for (auto kind : kinds) {
		SljitNativeRegionOpPlan native(kind);
		SljitExecutableRegionOp executable(kind);
		REQUIRE(native.Kind() == kind);
		REQUIRE(executable.Kind() == kind);
		if (kind != SljitNativeRegionOpKind::PROJECTION) {
			REQUIRE_THROWS_AS(native.Projections(), std::bad_variant_access);
			REQUIRE_THROWS_AS(executable.Projections(), std::bad_variant_access);
		}
		native.SetKind(SljitNativeRegionOpKind::PROJECTION);
		executable.SetKind(SljitNativeRegionOpKind::PROJECTION);
		native.Projections().emplace_back();
		executable.Projections().emplace_back();
		auto moved_native = std::move(native);
		auto moved_executable = std::move(executable);
		REQUIRE(moved_native.Projections().size() == 1);
		REQUIRE(moved_executable.Projections().size() == 1);
		moved_native.SetKind(SljitNativeRegionOpKind::HASH_JOIN_BUILD);
		moved_executable.SetKind(SljitNativeRegionOpKind::HASH_JOIN_BUILD);
		REQUIRE(moved_executable.CodeSize() == 0);
		REQUIRE_FALSE(moved_executable.HasExecutableBody());
	}
	INFO("native descriptor bytes=" << sizeof(SljitNativeRegionOpPlan));
	INFO("executable descriptor bytes=" << sizeof(SljitExecutableRegionOp));
	REQUIRE(sizeof(SljitNativeRegionOpPlan) < sizeof(SljitNativeHashJoinProbePlan) +
	                                              sizeof(SljitNativeAggregateUpdatePlan) +
	                                              sizeof(SljitNativeOrderSinkPlan));
	REQUIRE(sizeof(SljitExecutableRegionOp) < sizeof(SljitExecutableHashJoinProbe) +
	                                              sizeof(SljitExecutableAggregateUpdate) +
	                                              sizeof(SljitExecutableOrderSink));
}

TEST_CASE("SLJIT aggregate plans own only aggregate sink metadata", "[api][jit]") {
	INFO("aggregate plan bytes=" << sizeof(SljitNativeAggregateUpdatePlan));
	INFO("aggregate sink bytes=" << sizeof(ExecutionRegionAggregateSinkInfo));
	REQUIRE(sizeof(decltype(SljitNativeAggregateUpdatePlan::sink_info)) == sizeof(ExecutionRegionAggregateSinkInfo));
	SljitNativeAggregateUpdatePlan plan;
	REQUIRE(plan.sink_info.Kind() == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE);
	plan.sink_info.contract.kind = ExecutionRegionAggregateOperatorKind::HASH;
	REQUIRE(plan.sink_info.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	plan.sink_info.groups.emplace_back();
	plan.sink_info.groups[0].input_index = 7;
	auto copy = plan.sink_info;
	plan.sink_info.groups[0].input_index = 9;
	plan.sink_info.contract.kind = ExecutionRegionAggregateOperatorKind::PERFECT_HASH;
	REQUIRE(plan.sink_info.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE);
	REQUIRE(copy.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	REQUIRE(copy.groups[0].input_index == 7);
}

TEST_CASE("SLJIT hash-build and ordered plans own only their family metadata", "[api][jit]") {
	REQUIRE(sizeof(decltype(SljitNativeHashJoinBuildPlan::sink_info)) == sizeof(ExecutionRegionHashJoinInfo));
	REQUIRE(sizeof(decltype(SljitNativeOrderSinkPlan::sink_info)) == sizeof(ExecutionRegionOrderSinkInfo));
	REQUIRE(sizeof(SljitNativeHashJoinBuildPlan) < sizeof(ExecutionRegionSinkInfo));
	REQUIRE(sizeof(SljitNativeOrderSinkPlan) < sizeof(ExecutionRegionSinkInfo));
	SljitNativeRegionPlan plan;
	plan.ops.emplace_back(SljitNativeRegionOpKind::HASH_JOIN_BUILD);
	plan.ops[0].HashJoinBuild().sink_info.keys.emplace_back();
	auto key = BuildSljitRegionArtifactSemanticKey(plan);
	plan.ops[0].HashJoinBuild().sink_info.keys[0].input_index = 7;
	REQUIRE(BuildSljitRegionArtifactSemanticKey(plan) != key);
	plan.ops[0].SetKind(SljitNativeRegionOpKind::ORDER_SINK);
	key = BuildSljitRegionArtifactSemanticKey(plan);
	plan.ops[0].OrderSink().sink_info.contract.limit = 7;
	REQUIRE(BuildSljitRegionArtifactSemanticKey(plan) != key);
}

TEST_CASE("SLJIT hash-build lowering rejects mismatched sink families", "[api][jit]") {
	ExecutionRegionNode node;
	for (bool diagnostics : {false, true}) {
		REQUIRE(PlanSljitHashJoinBuildSinkNode(node, diagnostics).kind == ExecutionRegionLoweringKind::BOUNDARY);
		for (auto kind : {ExecutionRegionSinkKind::NONE, ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD,
		                  ExecutionRegionSinkKind::SORT, ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK,
		                  ExecutionRegionSinkKind::MATERIALIZATION, ExecutionRegionSinkKind::DELIM_JOIN_SINK,
		                  ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE}) {
			node.sink = make_uniq<ExecutionRegionSinkInfo>(kind);
			REQUIRE(PlanSljitHashJoinBuildSinkNode(node, diagnostics).kind == ExecutionRegionLoweringKind::BOUNDARY);
		}
		node.sink.reset();
	}
}

TEST_CASE("SLJIT aggregate lowering rejects mismatched sink families", "[api][jit]") {
	ExecutionRegionNode node;
	for (bool diagnostics : {false, true}) {
		REQUIRE(PlanSljitAggregateUpdateSinkNode(node, diagnostics).kind == ExecutionRegionLoweringKind::BOUNDARY);
		for (auto kind : {ExecutionRegionSinkKind::NONE, ExecutionRegionSinkKind::HASH_JOIN_BUILD,
		                  ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD, ExecutionRegionSinkKind::SORT,
		                  ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK, ExecutionRegionSinkKind::MATERIALIZATION,
		                  ExecutionRegionSinkKind::DELIM_JOIN_SINK}) {
			node.sink = make_uniq<ExecutionRegionSinkInfo>(kind);
			auto lowered = PlanSljitAggregateUpdateSinkNode(node, diagnostics);
			REQUIRE(lowered.kind == ExecutionRegionLoweringKind::BOUNDARY);
			REQUIRE(lowered.reason == "aggregate update sink kind mismatch");
		}
		for (auto kind :
		     {ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE, ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE,
		      ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE}) {
			node.sink = make_uniq<ExecutionRegionSinkInfo>(kind);
			auto &aggregate = node.sink->Aggregate();
			REQUIRE(aggregate.Kind() == kind);
			auto &contract = aggregate.contract;
			contract.present = true;
			contract.native_state_update_contract.status = ExecutionRegionStateContractStatus::READY;
			contract.native_grouped_state_contract.status = ExecutionRegionStateContractStatus::READY;
			REQUIRE(PlanSljitAggregateUpdateSinkNode(node, diagnostics).kind == ExecutionRegionLoweringKind::NATIVE);
			auto expected_kind = contract.kind;
			for (auto operator_kind :
			     {ExecutionRegionAggregateOperatorKind::NONE, ExecutionRegionAggregateOperatorKind::HASH,
			      ExecutionRegionAggregateOperatorKind::PERFECT_HASH,
			      ExecutionRegionAggregateOperatorKind::UNGROUPED}) {
				if (operator_kind == expected_kind) {
					continue;
				}
				contract.kind = operator_kind;
				auto lowered = PlanSljitAggregateUpdateSinkNode(node, diagnostics);
				REQUIRE(lowered.kind == ExecutionRegionLoweringKind::BOUNDARY);
				REQUIRE(lowered.reason == "aggregate update sink operator kind mismatch");
			}
		}
		node.sink.reset();
	}
}

TEST_CASE("Execution sink contracts exclude unrelated families", "[api][jit]") {
	INFO("sink contract bytes=" << sizeof(ExecutionRegionSinkInfo));
	REQUIRE(sizeof(ExecutionRegionSinkInfo) < sizeof(ExecutionRegionHashJoinContract) +
	                                              sizeof(ExecutionRegionAggregateContract) +
	                                              sizeof(ExecutionRegionNestedLoopJoinContract));
	ExecutionRegionSinkInfo sink;
	REQUIRE(sink.Kind() == ExecutionRegionSinkKind::NONE);
	REQUIRE(DescribeExecutionRegionSinkContract(sink) == "sink<kind=none>");
	REQUIRE_THROWS_AS(sink.HashJoin(), std::bad_variant_access);
	REQUIRE_THROWS_AS(sink.Aggregate(), std::bad_variant_access);
	REQUIRE_THROWS_AS(sink.Native(), std::bad_variant_access);
	sink.SetKind(ExecutionRegionSinkKind::HASH_JOIN_BUILD);
	REQUIRE(sink.Kind() == ExecutionRegionSinkKind::HASH_JOIN_BUILD);
	sink.HashJoin().contract.present = true;
	sink.HashJoin().keys.emplace_back();
	sink.HashJoin().keys[0].input_index = 7;
	auto hash_copy = sink;
	sink.HashJoin().keys[0].input_index = 9;
	REQUIRE(hash_copy.HashJoin().keys[0].input_index == 7);
	REQUIRE_THROWS_AS(sink.Aggregate(), std::bad_variant_access);
	for (auto kind :
	     {ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE, ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE,
	      ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE}) {
		sink.SetKind(kind);
		REQUIRE(sink.Kind() == kind);
		REQUIRE(sink.Aggregate().aggregates.empty());
		sink.Aggregate().contract.present = true;
		sink.Aggregate().aggregates.emplace_back();
		sink.Aggregate().aggregates[0].aggregate_index = 4;
		auto aggregate_copy = sink;
		sink.Aggregate().aggregates[0].aggregate_index = 8;
		REQUIRE(aggregate_copy.Aggregate().aggregates[0].aggregate_index == 4);
		REQUIRE_THROWS_AS(sink.HashJoin(), std::bad_variant_access);
		REQUIRE_THROWS_AS(sink.Native(), std::bad_variant_access);
		REQUIRE_FALSE(StringUtil::Contains(DescribeExecutionRegionSinkContract(sink), "hash_join_contract<"));
	}
	sink.SetKind(ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD);
	REQUIRE(sink.Kind() == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD);
	sink.NestedLoopJoin().present = true;
	REQUIRE(StringUtil::Contains(DescribeExecutionRegionSinkContract(sink), "nested_loop_join_contract<"));
	for (auto kind : {ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK, ExecutionRegionSinkKind::MATERIALIZATION,
	                  ExecutionRegionSinkKind::SORT, ExecutionRegionSinkKind::DELIM_JOIN_SINK}) {
		sink.SetKind(kind);
		REQUIRE(sink.Kind() == kind);
		REQUIRE(sink.Native().status == ExecutionRegionStateContractStatus::NONE);
		sink.Native().status = ExecutionRegionStateContractStatus::READY;
		REQUIRE_FALSE(DescribeExecutionRegionSinkContract(sink).empty());
		REQUIRE_THROWS_AS(sink.Aggregate(), std::bad_variant_access);
		REQUIRE_THROWS_AS(sink.HashJoin(), std::bad_variant_access);
	}
	sink.DelimJoin().distinct.groups.emplace_back();
	sink.DelimJoin().distinct.groups[0].input_index = 3;
	sink.DelimJoin().build = hash_copy.HashJoin();
	auto delim_copy = sink;
	sink.DelimJoin().distinct.groups[0].input_index = 5;
	REQUIRE(delim_copy.DelimJoin().distinct.groups[0].input_index == 3);
	ExecutionRegionSinkInfo build(ExecutionRegionSinkKind::HASH_JOIN_BUILD);
	build.HashJoin() = sink.DelimJoin().build;
	REQUIRE(build.HashJoin().keys[0].input_index == 7);
	REQUIRE_THROWS_AS(build.DelimJoin(), std::bad_variant_access);
	sink.SetKind(ExecutionRegionSinkKind::SORT);
	sink.Order().contract.present = true;
	auto moved = std::move(sink);
	REQUIRE(moved.Kind() == ExecutionRegionSinkKind::SORT);
	REQUIRE(moved.Order().contract.present);
	moved.SetKind(ExecutionRegionSinkKind::NONE);
	REQUIRE_THROWS_AS(moved.Order(), std::bad_variant_access);
	for (auto kind :
	     {ExecutionRegionSinkKind::NONE, ExecutionRegionSinkKind::HASH_JOIN_BUILD, ExecutionRegionSinkKind::SORT,
	      ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD, ExecutionRegionSinkKind::DELIM_JOIN_SINK,
	      ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE, ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE,
	      ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE}) {
		SljitNativeRegionPlan unsupported;
		unsupported.ops.emplace_back(SljitNativeRegionOpKind::APPEND_SINK);
		unsupported.ops[0].AppendSink().sink_info.SetKind(kind);
		REQUIRE_THROWS_AS(BuildSljitRegionArtifactSemanticKey(unsupported), InternalException);
		REQUIRE_THROWS_AS(BindSljitRegionInstanceSlots(unsupported), InternalException);
	}
}

TEST_CASE("Execution operator contracts store only one family payload", "[api][jit]") {
	INFO("operator contract bytes=" << sizeof(ExecutionRegionOperatorInfo));
	INFO("hash join contract bytes=" << sizeof(ExecutionRegionHashJoinContract));
	INFO("nested loop contract bytes=" << sizeof(ExecutionRegionNestedLoopJoinContract));
	REQUIRE(sizeof(ExecutionRegionOperatorInfo) <
	        sizeof(ExecutionRegionHashJoinContract) + sizeof(ExecutionRegionNestedLoopJoinContract));
	ExecutionRegionOperatorInfo info;
	REQUIRE(info.Kind() == ExecutionRegionOperatorContractKind::NONE);
	REQUIRE_THROWS_AS(info.HashJoin(), std::bad_variant_access);
	REQUIRE_THROWS_AS(info.NestedLoopJoin(), std::bad_variant_access);
	REQUIRE(DescribeExecutionRegionOperatorContract(info) == "operator<kind=none>");
	info.SetKind(ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE);
	REQUIRE(info.Kind() == ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE);
	info.HashJoin().contract.present = true;
	info.HashJoin().contract.condition_count = 2;
	info.HashJoin().keys.emplace_back();
	info.HashJoin().keys[0].input_index = 7;
	auto hash_copy = info;
	info.HashJoin().keys[0].input_index = 9;
	REQUIRE(hash_copy.HashJoin().keys[0].input_index == 7);
	REQUIRE_THROWS_AS(info.NestedLoopJoin(), std::bad_variant_access);
	auto hash_description = DescribeExecutionRegionOperatorContract(info);
	REQUIRE(StringUtil::Contains(hash_description, "hash_join_contract<"));
	REQUIRE_FALSE(StringUtil::Contains(hash_description, "nested_loop_join_contract<"));
	info.SetKind(ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE);
	REQUIRE(info.Kind() == ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE);
	REQUIRE_THROWS_AS(info.HashJoin(), std::bad_variant_access);
	REQUIRE(info.NestedLoopJoin().conditions.empty());
	info.NestedLoopJoin().present = true;
	info.NestedLoopJoin().conditions.emplace_back();
	info.NestedLoopJoin().conditions[0].condition_index = 3;
	auto nested_copy = info;
	info.NestedLoopJoin().conditions[0].condition_index = 5;
	REQUIRE(nested_copy.NestedLoopJoin().conditions[0].condition_index == 3);
	auto nested_description = DescribeExecutionRegionOperatorContract(info);
	REQUIRE(StringUtil::Contains(nested_description, "nested_loop_join_contract<"));
	REQUIRE_FALSE(StringUtil::Contains(nested_description, ",hash_join_contract<"));
	auto moved = std::move(info);
	REQUIRE(moved.Kind() == ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE);
	REQUIRE(moved.NestedLoopJoin().conditions[0].condition_index == 5);
	moved.SetKind(ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE);
	REQUIRE(moved.HashJoin().keys.empty());
	REQUIRE_FALSE(moved.HashJoin().contract.present);
	moved.SetKind(ExecutionRegionOperatorContractKind::NONE);
	REQUIRE(moved.Kind() == ExecutionRegionOperatorContractKind::NONE);
	REQUIRE_THROWS_AS(moved.HashJoin(), std::bad_variant_access);
}

TEST_CASE("SLJIT artifact identity is structural and excludes nested diagnostics", "[api][jit]") {
	SljitNativeRegionPlan region;
	SljitNativeRegionOpPlan aggregate;
	aggregate.SetKind(SljitNativeRegionOpKind::AGGREGATE_UPDATE);
	aggregate.AggregateUpdate().sink_info.contract.kind = ExecutionRegionAggregateOperatorKind::HASH;
	region.ops.push_back(std::move(aggregate));
	auto initial = BuildSljitRegionArtifactSemanticKey(region);
	auto &sink = region.ops[0].AggregateUpdate().sink_info;
	region.ops[0].AggregateUpdate().ir = "rendered IR";
	sink.contract.native_grouped_state_contract.blocker = "nested diagnostic";
	sink.contract.native_state_update_contract.blocker = "sink diagnostic";
	region.aggregate_state_source.state_scan_contract.blocker = "source diagnostic";
	region.aggregate_state_source.state_scan_contract.primitive_aggregate_batch_blocker = "lane diagnostic";
	REQUIRE(initial == BuildSljitRegionArtifactSemanticKey(region));
	sink.contract.grouped_state_offsets.push_back(16);
	REQUIRE(initial != BuildSljitRegionArtifactSemanticKey(region));
	sink.contract.grouped_state_offsets.clear();
	REQUIRE(initial == BuildSljitRegionArtifactSemanticKey(region));
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::PERFECT_HASH;
	REQUIRE(initial != BuildSljitRegionArtifactSemanticKey(region));
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::UNGROUPED;
	REQUIRE(initial != BuildSljitRegionArtifactSemanticKey(region));
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::NONE;
	REQUIRE_THROWS_AS(BuildSljitRegionArtifactSemanticKey(region), InternalException);
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::HASH;
	region.ops[0].estimated_input_count = 512;
	REQUIRE(initial == BuildSljitRegionArtifactSemanticKey(region));

	SljitNativeRegionPlan join_region;
	SljitNativeRegionOpPlan join;
	join.SetKind(SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	join_region.ops.push_back(std::move(join));
	auto join_key = BuildSljitRegionArtifactSemanticKey(join_region);
	join_region.ops[0].HashJoinProbe().operator_info.HashJoin().contract.output_reaches_dictionary_narrowing_sink =
	    true;
	REQUIRE(join_key != BuildSljitRegionArtifactSemanticKey(join_region));
	join_region.ops[0].HashJoinProbe().operator_info.SetKind(
	    ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE);
	REQUIRE_THROWS_AS(BuildSljitRegionArtifactSemanticKey(join_region), InternalException);
}

TEST_CASE("SLJIT runtime aggregate estimates do not partition the artifact cache", "[api][jit]") {
	SljitNativeRegionPlan region;
	region.ops.emplace_back(SljitNativeRegionOpKind::AGGREGATE_UPDATE);
	region.ops[0].estimated_input_count = 10000;
	region.source_distinct_counts = {32};
	region.source_distinct_reserve_counts = {64};
	region.source_min_values = {Value::BIGINT(0)};
	region.source_max_values = {Value::BIGINT(31)};
	auto key = [&]() {
		return ComposeSljitRegionArtifactCacheKey(BuildSljitRegionArtifactSemanticKey(region),
		                                          BuildSljitRegionArtifactBindingKey(region));
	};
	auto initial = key();
	region.ops[0].estimated_input_count = 20000;
	CHECK(bool(initial == key()));
	region.source_distinct_reserve_counts = {128};
	CHECK(bool(initial == key()));
	region.source_max_values = {Value::BIGINT(63)};
	CHECK(bool(initial != key()));
}

TEST_CASE("SLJIT aggregate instance parameters preserve reservation policy and column mapping", "[api][jit]") {
	SljitNativeRegionPlan region;
	region.source_distinct_counts = {3, 5};
	region.source_distinct_reserve_counts = {3, 5};
	region.ops.emplace_back(SljitNativeRegionOpKind::PROJECTION);
	for (idx_t index : {1, 0}) {
		SljitNativeRegionExpressionPlan expression;
		expression.kind = SljitNativeRegionExpressionKind::REFERENCE;
		expression.return_type = LogicalType::BIGINT;
		expression.source_index = index;
		region.ops[0].Projections().push_back(std::move(expression));
	}
	region.ops.emplace_back(SljitNativeRegionOpKind::AGGREGATE_UPDATE);
	auto &aggregate = region.ops.back();
	aggregate.estimated_input_count = 100;
	auto &sink = aggregate.AggregateUpdate().sink_info;
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::HASH;
	ExecutionRegionGroupInput group;
	group.supported_reference = true;
	group.input_index = 0;
	group.type = LogicalType::BIGINT;
	sink.groups.push_back(group);
	auto first = BuildSljitAggregateInstanceParameters(region);
	REQUIRE(first.size() == region.ops.size());
	REQUIRE(first[0].estimated_input_count == 0);
	REQUIRE(first[0].group_reserve_count == 0);
	REQUIRE(first[1].estimated_input_count == 100);
	REQUIRE(first[1].group_reserve_count == 5);
	aggregate.estimated_input_count = 5;
	auto second = BuildSljitAggregateInstanceParameters(region);
	REQUIRE(second[1].estimated_input_count == 5);
	REQUIRE(second[1].group_reserve_count == 0);
	REQUIRE(first[1].estimated_input_count == 100);
	REQUIRE(first[1].group_reserve_count == 5);
	aggregate.estimated_input_count = 0;
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 5);
	group.input_index = 1;
	sink.groups.push_back(group);
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 15);
	region.source_distinct_reserve_counts = {NumericLimits<idx_t>::Maximum(), 2};
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 0);
	region.source_distinct_reserve_counts = {3, 0};
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 0);
	region.source_distinct_reserve_counts.clear();
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 0);
	region.source_distinct_reserve_counts = {3, 5};
	sink.groups[0].supported_reference = false;
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 0);
	sink.groups[0].supported_reference = true;
	sink.contract.kind = ExecutionRegionAggregateOperatorKind::PERFECT_HASH;
	REQUIRE(BuildSljitAggregateInstanceParameters(region)[1].group_reserve_count == 0);
}

TEST_CASE("SLJIT expression identity preserves binary values and cast binding structure", "[api][jit]") {
	SljitNativeRegionExpressionPlan expression;
	expression.kind = SljitNativeRegionExpressionKind::CONSTANT;
	expression.return_type = LogicalType::DOUBLE;
	expression.constant_value = Value::DOUBLE(0.0);
	auto key = BuildSljitExpressionArtifactKey(expression);
	expression.constant_value = Value::DOUBLE(-0.0);
	REQUIRE(key != BuildSljitExpressionArtifactKey(expression));
	expression.ir = "diagnostics do not change identity";
	auto diagnostic_key = BuildSljitExpressionArtifactKey(expression);
	expression.ir.clear();
	REQUIRE(diagnostic_key == BuildSljitExpressionArtifactKey(expression));
	expression.cast_chain.source_type = LogicalType::BIGINT;
	expression.cast_chain.steps.push_back({LogicalType::INTEGER, QueryLocation(), "first"});
	vector<SljitCastErrorIdentity> errors;
	BindSljitCastErrorSlots(expression.cast_chain, errors);
	auto cast_key = BuildSljitExpressionArtifactKey(expression);
	errors[0].source_alias = "second";
	REQUIRE(cast_key == BuildSljitExpressionArtifactKey(expression));
	expression.cast_chain.steps[0].error_binding = 1;
	REQUIRE(cast_key != BuildSljitExpressionArtifactKey(expression));
}

TEST_CASE("SLJIT instance slots remove cast identities from shared plans", "[api][jit]") {
	auto make_region = [](const string &alias, idx_t position) {
		SljitNativeRegionPlan region;
		SljitNativeRegionOpPlan projection(SljitNativeRegionOpKind::PROJECTION);
		projection.Projections().emplace_back();
		auto &expression = projection.Projections().back();
		expression.cast_chain.source_type = LogicalType::BIGINT;
		expression.cast_chain.steps.push_back({LogicalType::INTEGER, QueryLocation(position, 5), alias});
		expression.expression_tree = make_uniq<ExecutionExpressionIR>();
		expression.expression_tree->kind = ExecutionExpressionIRKind::CAST;
		expression.expression_tree->query_location = QueryLocation(position, 5);
		expression.expression_tree->cast_source_alias = alias;
		region.ops.push_back(std::move(projection));
		BindSljitRegionInstanceSlots(region);
		return region;
	};
	auto first = make_region("first_column", 7);
	auto second = make_region("second_column", 101);
	REQUIRE(BuildSljitRegionArtifactSemanticKey(first) == BuildSljitRegionArtifactSemanticKey(second));
	REQUIRE(first.cast_errors.size() == 2);
	REQUIRE(second.cast_errors[0].source_alias == "second_column");
	REQUIRE(second.cast_errors[0].query_location.offset == 101);
	auto &expression = first.ops[0].Projections()[0];
	REQUIRE(expression.cast_chain.steps[0].source_alias.empty());
	REQUIRE_FALSE(expression.cast_chain.steps[0].query_location.IsValid());
	REQUIRE(expression.expression_tree->cast_source_alias.empty());
	REQUIRE_FALSE(expression.expression_tree->query_location.IsValid());
	auto copy = expression.Copy();
	REQUIRE(copy.expression_tree->cast_error_binding == expression.expression_tree->cast_error_binding);
	auto key = BuildSljitRegionArtifactSemanticKey(first);
	BindSljitRegionInstanceSlots(first);
	REQUIRE(first.cast_errors.size() == 2);
	REQUIRE(BuildSljitRegionArtifactSemanticKey(first) == key);
}

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

TEST_CASE("Execution region core rejects backend stage execution mutation", "[api][jit]") {
	UnitTestStageMutationBackendState state;
	JitTestDatabase test;
	test.manager.RegisterBackend(make_uniq<UnitTestStageMutationBackend>(state), EXECUTION_REGION_BACKEND_ABI_VERSION);

	REQUIRE_NO_FAIL(test.con.Query("SET enable_jit=false"));
	REQUIRE_NO_FAIL(test.con.Query("CREATE TEMP TABLE jit_stage_mutation_input AS "
	                               "SELECT i::BIGINT AS i FROM range(100000) tbl(i)"));
	REQUIRE_NO_FAIL(test.con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(test.con.Query("SET jit_backend='unit_test_stage_mutation_backend'"));
	REQUIRE_NO_FAIL(test.con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(test.con.Query("SET jit_event_log_size=10000"));
	ConfigureJitCoverageCbo(test.con);
	ConfigureJitDecisionTrace(test.con);

	ClearJitTrace(test.manager);
	REQUIRE_NO_FAIL(test.con.Query("CREATE TEMP TABLE jit_stage_mutation_output AS "
	                               "SELECT i + 1 AS j FROM jit_stage_mutation_input"));
	REQUIRE(state.selected_mismatched_stage);
	REQUIRE_FALSE(state.compile_called);

	bool found_execution_mismatch = false;
	for (auto &event : test.manager.GetEvents()) {
		if (event.backend_name == "unit_test_stage_mutation_backend" &&
		    event.blocker == "fused_region_stage_selection_execution_mismatch") {
			found_execution_mismatch = true;
		}
	}
	REQUIRE(found_execution_mismatch);

	REQUIRE_NO_FAIL(test.con.Query("SET enable_jit=false"));
	auto result = test.con.Query("SELECT sum(j) FROM jit_stage_mutation_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "5000050000");
}

TEST_CASE("JIT CBO can select a GPU physical runner when GPU benefit wins", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 4096;
	input.expression_cost = 100;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	parameters.gpu.available = true;
	parameters.gpu.generated_stage_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.SelectedAcceleratedRunner());
	REQUIRE(profile.SelectedGpuRunner());
	REQUIRE_FALSE(profile.SelectedCompiledVectorizedRunner());
	REQUIRE(profile.selected_runner == ExecutionRunnerKind::COMPILED_GPU);
	REQUIRE(profile.gpu.runner_benefit > profile.compiled_vectorized.runner_benefit);
	REQUIRE(profile.accelerated_runner_benefit == profile.gpu.runner_benefit);
	REQUIRE(profile.gpu.transfer_cost == 0);
}

TEST_CASE("JIT CBO keeps CPU JIT when GPU transfer cost dominates", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.expression_cost = 100;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.generated_stage_benefit = 100;
	parameters.gpu.available = true;
	parameters.gpu.generated_stage_benefit = 200;
	parameters.gpu.transfer_cost_per_batch = 1000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.SelectedAcceleratedRunner());
	REQUIRE(profile.SelectedCompiledVectorizedRunner());
	REQUIRE_FALSE(profile.SelectedGpuRunner());
	REQUIRE(profile.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
	REQUIRE(profile.gpu.transfer_cost == 1000);
	REQUIRE(profile.gpu.net_benefit < profile.compiled_vectorized.net_benefit);
}

TEST_CASE("JIT prepared executions reuse immutable compiled artifacts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("PREPARE jit_artifact_cache AS "
	                          "SELECT sum(i * 7) FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto first = con.Query("EXECUTE jit_artifact_cache");
	REQUIRE_NO_FAIL(*first);
	REQUIRE(first->Collection().GetValue(0, 0).ToString() == "3499996500000");
	bool found_initial_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || EventPhase(event) != "compile") {
			continue;
		}
		found_initial_compile = true;
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "compiled artifact cache hit"));
		REQUIRE(event.ir.empty());
		REQUIRE(event.code_size > 0);
	}
	REQUIRE(found_initial_compile);

	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	ClearJitTrace(manager, true);
	auto second = con.Query("EXECUTE jit_artifact_cache");
	REQUIRE_NO_FAIL(*second);
	REQUIRE(second->Collection().GetValue(0, 0).ToString() == first->Collection().GetValue(0, 0).ToString());
	bool found_cached_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || EventPhase(event) != "compile") {
			continue;
		}
		found_cached_compile = true;
		REQUIRE(StringUtil::Contains(event.reason, "compiled artifact cache hit"));
		REQUIRE_FALSE(event.ir.empty());
		REQUIRE(event.compile_time_us == 0);
		REQUIRE(event.stage_timings.codegen_time_us == 0);
		REQUIRE(event.stage_timings.executable_build_time_us == 0);
		REQUIRE(event.stage_timings.machine_codegen_time_us == 0);
		REQUIRE(event.code_size > 0);
	}
	REQUIRE(found_cached_compile);
}

TEST_CASE("JIT artifact cache separates refreshed source bindings after DML", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_prepared_cast_source(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_prepared_cast_source SELECT i FROM range(100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE jit_prepared_cast AS "
	                          "SELECT sum(i::INTEGER) FROM jit_prepared_cast_source"));

	auto first = con.Query("EXECUTE jit_prepared_cast");
	REQUIRE_NO_FAIL(*first);
	REQUIRE(first->Collection().GetValue(0, 0).GetValue<int64_t>() == 4999950000LL);

	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_prepared_cast_source VALUES (2147483648)"));
	ClearJitTrace(manager, true);
	auto after_insert = con.Query("EXECUTE jit_prepared_cast");
	REQUIRE_FAIL(after_insert);
	REQUIRE(StringUtil::Contains(after_insert->GetError(), "out of range"));
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
		       !StringUtil::Contains(event.reason, "compiled artifact cache hit");
	});
}

TEST_CASE("JIT prepared artifact reuse preserves parameter-dependent semantics", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("PREPARE jit_parameter_artifact AS "
	                          "SELECT sum(i * $1) FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto first = con.Query("EXECUTE jit_parameter_artifact(7)");
	REQUIRE_NO_FAIL(*first);
	REQUIRE(first->Collection().GetValue(0, 0).ToString() == "34999650000");

	ClearJitTrace(manager, true);
	auto second = con.Query("EXECUTE jit_parameter_artifact(7)");
	REQUIRE_NO_FAIL(*second);
	REQUIRE(second->Collection().GetValue(0, 0).ToString() == first->Collection().GetValue(0, 0).ToString());
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
		       StringUtil::Contains(event.reason, "compiled artifact cache hit");
	});

	ClearJitTrace(manager, true);
	auto third = con.Query("EXECUTE jit_parameter_artifact(9)");
	REQUIRE_NO_FAIL(*third);
	REQUIRE(third->Collection().GetValue(0, 0).ToString() == "44999550000");
	// the parameter is a generated constant, so a new value compiles its own artifact
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
		       !StringUtil::Contains(event.reason, "compiled artifact cache hit");
	});
}

TEST_CASE("JIT prepared artifact reuse refreshes exact runtime-filter identity", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_prepared_filter_fact AS "
	                          "SELECT i::BIGINT AS k FROM range(100000) tbl(i)"));
	// Sparse even build keys leave room for the later insert of 999: join statistics bake the build keys' [min, max]
	// into a fact-scan filter, so only a key inside that range keeps the rebound plan, and its artifact, unchanged.
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_prepared_filter_dim AS "
	                          "SELECT (i * 2)::BIGINT AS k FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE jit_prepared_filter AS "
	                          "SELECT count(*) FROM jit_prepared_filter_fact f "
	                          "JOIN jit_prepared_filter_dim d USING (k)"));

	auto first = con.Query("EXECUTE jit_prepared_filter");
	REQUIRE_NO_FAIL(*first);
	REQUIRE(first->Collection().GetValue(0, 0).GetValue<int64_t>() == 1000);
	auto exact_filter_executed = [](const ExecutionRegionEvent &event) {
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			return false;
		}
		auto paths = EventJitRuntimePathCounts(event);
		return StringUtil::Contains(paths, "hash_join_probe.regular_probe.exact_source_filter=") ||
		       StringUtil::Contains(paths, "hash_join_probe.perfect_probe.exact_source_filter=");
	};

	ClearJitTrace(manager, true);
	auto second = con.Query("EXECUTE jit_prepared_filter");
	REQUIRE_NO_FAIL(*second);
	REQUIRE(second->Collection().GetValue(0, 0).GetValue<int64_t>() == 1000);
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
		       StringUtil::Contains(event.reason, "compiled artifact cache hit");
	});
	RequireJitEvent(manager, exact_filter_executed);

	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_prepared_filter_dim VALUES (999)"));
	ClearJitTrace(manager, true);
	auto after_insert = con.Query("EXECUTE jit_prepared_filter");
	REQUIRE_NO_FAIL(*after_insert);
	REQUIRE(after_insert->Collection().GetValue(0, 0).GetValue<int64_t>() == 1001);
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsCompiledSljitRegionEvent(event) && EventPhase(event) == "compile" &&
		       StringUtil::Contains(event.reason, "compiled artifact cache hit");
	});
	RequireJitEvent(manager, exact_filter_executed);
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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->Collection().GetValue(1, 0).GetValue<int64_t>() == 5000650000LL);

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
		    REQUIRE(event.runner_cost.SelectedGpuRunner());
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
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

TEST_CASE("JIT Metal never claims filtered source rows it does not execute", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_filtered_input AS "
	                          "SELECT i::BIGINT AS i FROM range(100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_filtered_baseline AS "
	                          "SELECT i + 7 AS j FROM jit_metal_filtered_input WHERE i < 10"));

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='jit_metal'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_filtered_output AS "
	                          "SELECT i + 7 AS j FROM jit_metal_filtered_input WHERE i < 10"));

	bool rejected_filtered_recipe = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE(IsCompiledMetalRegionEvent(event));
		if (IsMetalRegionEvent(event) && event.blocker == "fused_region_stage_selection_incomplete") {
			rejected_filtered_recipe = true;
		}
	}
	REQUIRE(rejected_filtered_recipe);

	auto result = con.Query("SELECT count(*), sum(j) FROM jit_metal_filtered_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 10);
	REQUIRE(result->Collection().GetValue(1, 0).GetValue<int64_t>() == 115);
	auto difference = con.Query("SELECT * FROM (FROM jit_metal_filtered_baseline EXCEPT ALL "
	                            "FROM jit_metal_filtered_output) "
	                            "UNION ALL SELECT * FROM (FROM jit_metal_filtered_output EXCEPT ALL "
	                            "FROM jit_metal_filtered_baseline)");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->RowCount() == 0);
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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->Collection().GetValue(1, 0).GetValue<double>() == 2500325000.0);

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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(result->Collection().GetValue(1, 0).GetValue<int64_t>() == 10000);

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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(result->Collection().GetValue(1, 0).ToString() == "5000675000.00");

	RequireExecutedMetalProjection(manager, 100000);
}
TEST_CASE("JIT Metal step dispatches once per row group and keeps insertion order", "[api][jit][metal]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	REQUIRE_NO_FAIL(con.Query("LOAD jit_metal"));
	if (!MetalBackendAvailable(test.context, manager)) {
		WARN("Skipping Metal JIT GPU test because no available Metal device was reported");
		return;
	}

	ConfigureMetalGpuSettings(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	// Eight row groups over two executors, so each executor's row groups are not adjacent: a step batches each row
	// group into one dispatch, which sinks before the executor's next row group moves the order-preserving
	// materialization to its batch
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_row_groups_input AS "
	                          "SELECT i::BIGINT AS i FROM range(983040) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_metal_row_groups_output AS "
	                          "SELECT i + 7 AS j FROM jit_metal_row_groups_input"));
	auto result = con.Query("SELECT count(*), sum(j), count(*) FILTER (WHERE previous >= j) FROM "
	                        "(SELECT j, lag(j) OVER (ORDER BY rowid) AS previous FROM jit_metal_row_groups_output)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 983040);
	REQUIRE(result->Collection().GetValue(1, 0).GetValue<hugeint_t>() == hugeint_t(483190210560LL));
	REQUIRE(result->Collection().GetValue(2, 0).GetValue<int64_t>() == 0);

	idx_t output_rows = 0;
	idx_t dispatches = 0;
	for (auto &event : manager.GetEvents()) {
		if (!IsMetalRegionEvent(event) || EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			continue;
		}
		output_rows += event.output_rows;
		for (auto &stage : event.generated_stage_runtime) {
			if (stage.stage.name == "metal.projection") {
				dispatches += stage.count;
			}
		}
	}
	REQUIRE(output_rows == 983040);
	REQUIRE(dispatches == 8);
}

#endif

TEST_CASE("JIT CBO rejects native operator stages without generated stage credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.native_aggregate_stage_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.generated_stage_benefit = 10;
	parameters.compiled_vectorized.native_operator_stage_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	// a native aggregate runs in DuckDB's sink: it is no operator work the backend can own
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_aggregate_stage_count = 0;
	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_join_stage_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	parameters.compiled_vectorized.startup_base_cost = 32000;
	parameters.compiled_vectorized.startup_margin_basis_points = 5000;

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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 1600;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 164);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated_native_fusion|generated_stage_benefit");
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 3;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 2;
	input.native_sort_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	parameters.compiled_vectorized.native_operator_stage_benefit = 1000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 0;
	input.native_aggregate_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 1;
	input.native_aggregate_stage_count = 0;
	input.expression_cost = 1000;
	input.generated_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 1000);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == 1281);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 800000;
	input.expression_cost = 0;
	input.generated_stage_count = 0;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	parameters.compiled_vectorized.generated_stage_benefit = 0;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.startup_base_cost = 30000;
	parameters.vectorized_parallelism = 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 2000000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.selection_reason == "rejected_native_operator_work_uncosted");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO scores native-stage fusion uniformly after generated admission", "[api][jit]") {
	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.startup_base_cost = 32000;
	parameters.compiled_vectorized.startup_margin_basis_points = 0;

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
	REQUIRE(profile.native_operator_work == 4096);
	REQUIRE(profile.stateful_protocol_penalty == 2880);
	REQUIRE(profile.saved_work_per_batch == 1432);
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.native_operator_work == 1024);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == 350);
	REQUIRE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO charges grouped aggregate fusion against parallel vectorized baseline", "[api][jit]") {
	PhysicalRunnerCostParameters parameters;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 1024;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.startup_base_cost = 30000;
	parameters.compiled_vectorized.startup_margin_basis_points = 0;

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
	REQUIRE(profile.saved_work_per_batch == 652);
	REQUIRE(profile.SelectedAcceleratedRunner());

	parameters.vectorized_parallelism = 12;
	profile = DuckDBCostModel::SelectPhysicalRunner(high_grouped_native_stage_shape, parameters);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

	join_grouped_replacement_shape.native_join_stage_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(join_grouped_replacement_shape, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT backend owns generated source-filter aggregate terminals", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == reference->Collection().GetValue(0, 0).ToString());
	REQUIRE(result->Collection().GetValue(1, 0).ToString() == reference->Collection().GetValue(1, 0).ToString());
	vector<idx_t> generated_filter_kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		generated_filter_kernel_ids.push_back(event.kernel_id);
		RequireGeneratedSourceFilterContract(event);
		REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
		REQUIRE(event.runner_cost.materialization_elision_count > 0);
	}
	REQUIRE(!generated_filter_kernel_ids.empty());
	RequireMaterializationElisionRuntimeProof(manager, generated_filter_kernel_ids);
}

TEST_CASE("JIT production CBO uses modulo domains for filtered reductions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, 10000);
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
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    REQUIRE(event.runner_cost.rows >= 100000);
		    REQUIRE(event.runner_cost.rows <= 150000);
		    REQUIRE(event.runner_cost.materialization_elision_count == 1);
		    REQUIRE(event.runner_cost.source_contract_scan_penalty == 0);
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
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
		    REQUIRE_FALSE(event.runner_cost.SelectedAcceleratedRunner());
	    });
}

TEST_CASE("JIT selected scan-filtered grouped aggregate uses direct grouped primitive recipe", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	// Group runs are what lets the generated update own the lookup; without them the pipeline runs natively
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_scan_filtered_grouped_aggregate AS "
	                          "SELECT i::BIGINT AS id, "
	                          "       CAST((i // 64) % 1024 AS INTEGER) AS group_id, "
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
	RequireSameResultValues(*result, *reference);

	vector<idx_t> scan_filtered_kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !event.selected_uses_scan_filters) {
			continue;
		}
		scan_filtered_kernel_ids.push_back(event.kernel_id);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
		REQUIRE(event.runner_cost.materialization_elision_count > 0);
	}
	REQUIRE(!scan_filtered_kernel_ids.empty());
	RequireMaterializationElisionRuntimeProof(manager, scan_filtered_kernel_ids);
}

TEST_CASE("JIT keeps generated-safe grouped aggregate filters in generated source", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	RequireSameResultValues(*result, *reference);

	bool found_generated_grouped_filter = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		found_generated_grouped_filter = true;
		REQUIRE(event.selected_uses_scan_filters);
		REQUIRE(StringUtil::Contains(event.reason, "source-strategy=generated-source-filter"));
		RequireGeneratedSourceFilterContract(event);
	}
	REQUIRE(found_generated_grouped_filter);
}

TEST_CASE("JIT materialization-elision runtime proves projected aggregate ownership", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_aggregate_input AS "
	                          "SELECT ((i // 64) % 4096)::BIGINT AS g, "
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
	RequireSameResultValues(*result, *reference);

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate preserves dense key semantics", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_grouped_sum_input AS "
	                          "SELECT ((i // 64) % 1024)::BIGINT AS g, (i % 17)::BIGINT AS v "
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
	RequireSameResultValues(*result, *reference);

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate supports substring group keys without delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	RequireSameResultValues(*result, *reference);

	auto materialization_elision_kernel_ids =
	    RequireSljitMaterializationElisionCboKernelIds(manager, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	RequireMaterializationElisionRuntimeProof(manager, materialization_elision_kernel_ids);
}

TEST_CASE("JIT projected source grouped aggregate handles sparse projected groups without delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	RequireSameResultValues(*result, *reference);

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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	parameters.compiled_vectorized.startup_base_cost = 32000;
	parameters.compiled_vectorized.startup_margin_basis_points = 5000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated");
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 200);
	REQUIRE(profile.startup_cost == 32000);
	REQUIRE(profile.required_benefit == 48000);
	REQUIRE(profile.SelectedAcceleratedRunner());

	parameters.vectorized_parallelism = 12;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.startup_cost == 384000);
	REQUIRE(profile.required_benefit == 576000);
	REQUIRE(profile.accelerated_runner_benefit < profile.required_benefit);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO does not use physical pipeline scope as full-pipeline proof", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
	REQUIRE(profile.admission_class == "full_pipeline");
	REQUIRE(profile.full_pipeline_work == 100);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.input_scope = PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.input_scope == PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO charges hash join build sink protocol separately from native probe work", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 400000;
	input.native_hash_join_build_sink_count = 1;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_hash_join_build_sink_count == 1);
	REQUIRE(profile.admission_class == "none");
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch == -720);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 104;
	input.generated_stage_count = 4;
	input.native_join_stage_count = 1;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	parameters.compiled_vectorized.full_pipeline_benefit = 0;
	parameters.compiled_vectorized.generated_stage_benefit = 1024;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;

	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.generated_expression_work == 104);
	REQUIRE(profile.generated_stage_work == 104);
	REQUIRE(profile.native_operator_work == 1024);
	REQUIRE(profile.stateful_protocol_penalty == 1440);
	REQUIRE(profile.saved_work_per_batch == -208);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.source_contract_input_cardinality = 150000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_rows == 150000);
	REQUIRE(profile.rows > profile.source_contract_input_rows);
	REQUIRE(profile.stateful_protocol_penalty == 1696);
	REQUIRE(profile.saved_work_per_batch == -464);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 8912);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 1;
	parameters.compiled_vectorized.startup_base_cost = 32000;
	parameters.compiled_vectorized.startup_margin_basis_points = 5000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.accelerated_runner_benefit == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.uses_scan_filters = false;
	input.native_join_stage_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.rows == int64_t(input.estimated_cardinality));
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_rows == 6001215);
	REQUIRE(profile.source_contract_input_batches > profile.batches);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.native_grouped_state_address_lookup_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_grouped_state_address_lookup_count = 0;
	input.source_contract_output_cardinality_unknown = false;
	input.native_join_stage_count = 0;
	input.native_hash_join_build_sink_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_hash_join_build_sink_count = 0;
	input.native_join_stage_count = 1;
	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

	scan_filtered_aggregate_tail.selected_hash_join_filter_materialization_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(scan_filtered_aggregate_tail, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
	scan_filtered_aggregate_tail.selected_hash_join_filter_materialization_count = 0;

	auto scan_filtered_grouped_aggregate = scan_filtered_aggregate_tail;
	scan_filtered_grouped_aggregate.generated_backend_stage_count = 2;
	scan_filtered_grouped_aggregate.generated_grouped_aggregate_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(scan_filtered_grouped_aggregate, parameters);
	REQUIRE(profile.generated_grouped_aggregate_stage_count == 2);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == 0);
	REQUIRE(profile.costed_batches == 1);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch <= 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 60000;
	input.source_contract_input_cardinality = input.estimated_cardinality;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.source_contract_input_batches == profile.batches);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.source_contract_output_cardinality_unknown = true;
	input.materialization_elision_count = 1;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.materialization_elision_count == 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 3072;
	parameters.compiled_vectorized.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.finalized_dynamic_filter_cardinality_estimate = false;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 2;
	input.generated_backend_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_scan_penalty > 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.native_operator_work == 2 * int64_t(parameters.compiled_vectorized.native_operator_stage_benefit));
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality++;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work > 0);
	REQUIRE(profile.native_operator_work == 2 * int64_t(parameters.compiled_vectorized.native_operator_stage_benefit));
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 4;
	input.native_aggregate_stage_count = 0;
	input.generated_stage_count = 2;
	input.generated_backend_stage_count = 2;
	input.expression_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality++;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work > 0);
	REQUIRE(profile.native_operator_work == 2 * int64_t(parameters.compiled_vectorized.native_operator_stage_benefit));
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 90000;
	input.generated_stage_count = 1;
	input.generated_backend_stage_count = 1;
	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	parameters.compiled_vectorized.startup_base_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == int64_t(parameters.compiled_vectorized.native_operator_stage_benefit));
	REQUIRE(profile.SelectedAcceleratedRunner());
	parameters.compiled_vectorized.startup_base_cost = 10000;

	input.estimated_cardinality = STANDARD_VECTOR_SIZE * 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	parameters.compiled_vectorized.startup_base_cost = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.native_operator_work == int64_t(parameters.compiled_vectorized.native_operator_stage_benefit));
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 3072;
	parameters.compiled_vectorized.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches == 18);
	REQUIRE(profile.costed_batches == 1);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 2464);
	REQUIRE(profile.saved_work_per_batch == 608);
	REQUIRE(profile.selection_reason == "rejected_insufficient_benefit");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 296279;
	input.source_contract_input_cardinality = 150000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.saved_work_per_batch == 608);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
	parameters.compiled_vectorized.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches < 64);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 1767425;
	input.source_contract_input_cardinality = 1767425;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.startup_base_cost = 10000;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown == false);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.generated_backend_stage_work == 8192);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 20000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 6001215;
	input.source_contract_input_cardinality = 6001215;
	input.expression_cost = 61;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.batches > 1024);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	PhysicalRunnerCostInput state_scan_input;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
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
	REQUIRE(profile.SelectedAcceleratedRunner());

	state_scan_input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(state_scan_input, parameters);
	REQUIRE(profile.batches > 1);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 3072;
	parameters.compiled_vectorized.startup_base_cost = 10000;
	parameters.vectorized_parallelism = 4;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.source_contract_output_cardinality_unknown);
	REQUIRE(profile.stateful_protocol_penalty == 1200);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.accelerated_runner_benefit > profile.required_benefit);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.generated_backend_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 1200);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;
	parameters.compiled_vectorized.startup_base_cost = 10000;

	auto upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	REQUIRE(upper_bound.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
	REQUIRE(upper_bound.generated_backend_stage_count == 3);
	REQUIRE(upper_bound.native_join_stage_count == 3);
	auto profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.SelectedAcceleratedRunner());

	pipeline_input.estimated_cardinality = 6001215;
	pipeline_input.source_contract_input_cardinality = 6001215;
	upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.batches > 1024);
	REQUIRE(profile.costed_batches == profile.batches);
	REQUIRE(profile.SelectedAcceleratedRunner());

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
	REQUIRE(profile.SelectedAcceleratedRunner());

	pipeline_input.native_join_stage_count = 0;
	pipeline_input.expression_cost = 512;
	upper_bound = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_input);
	profile = DuckDBCostModel::SelectPhysicalRunner(upper_bound, parameters);
	REQUIRE(profile.native_hash_join_build_sink_count == 0);
	REQUIRE(profile.generated_backend_stage_count == 1);
	REQUIRE(profile.materialization_elision_count == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO admits explicit full-pipeline proof without duplicate stage credit", "[api][jit]") {
	PhysicalRunnerCostInput input;
	input.estimated_cardinality = 2048;
	input.full_pipeline = true;
	input.has_accelerated_work = true;

	auto parameters = ZeroStartupRunnerCostParameters();
	parameters.compiled_vectorized.full_pipeline_benefit = 100;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_stage_count == 0);
	REQUIRE(profile.native_join_stage_count == 0);
	REQUIRE(profile.native_aggregate_stage_count == 0);
	REQUIRE(profile.native_sort_stage_count == 0);
	REQUIRE(profile.full_pipeline);
	REQUIRE(profile.saved_work_per_batch == 100);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.full_pipeline_benefit = 4096;

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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::COMPUTE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_protocol_class = PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::HIGH_COST_PROJECTION;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 93831;
	input.expression_cost = 2130;
	input.generated_stage_count = 2;
	PhysicalRunnerCostParameters high_startup_margin_parameters;
	high_startup_margin_parameters.compiled_vectorized.available = true;
	high_startup_margin_parameters.compiled_vectorized.available = true;
	high_startup_margin_parameters.compiled_vectorized.generated_stage_benefit = 1;
	high_startup_margin_parameters.compiled_vectorized.startup_base_cost = 32000;
	high_startup_margin_parameters.compiled_vectorized.startup_margin_basis_points = 5000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, high_startup_margin_parameters);
	REQUIRE(profile.batches < 128);
	REQUIRE(profile.saved_work_per_batch == 2132);
	REQUIRE(profile.selection_reason == "admitted_admission_class:generated|generated_stage_benefit");
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 1024;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.admission_class == "generated_native_fusion");
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.generated_backend_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_operator_work == 1024);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.selection_reason ==
	        "admitted_admission_class:generated_native_fusion|generated_stage_benefit|native_operator_stage_benefit");
	REQUIRE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT CBO admits generated work beside a native grouped aggregate without crediting the aggregate",
          "[api][jit]") {
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.native_operator_stage_benefit = 4096;
	parameters.compiled_vectorized.full_pipeline_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 100);
	REQUIRE(profile.generated_stage_work == 100);
	REQUIRE(profile.native_operator_work == 0);
	REQUIRE(profile.full_pipeline_work == 0);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 200);
	REQUIRE(profile.admission_class == "generated");
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK));
	// the aggregate runs in DuckDB's sink, so no backend-owned work is promised
	REQUIRE_FALSE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                                     ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK));
	REQUIRE_FALSE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                                     ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP));
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 10000;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.saved_work_per_batch == 20000);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_hash_join_filter_materialization_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.selected_hash_join_filter_materialization_penalty == 4096);
	REQUIRE(profile.saved_work_per_batch == 0);
	REQUIRE(profile.selection_reason == "rejected_saved_work_non_positive");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.selected_hash_join_filter_materialization_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.selected_hash_join_filter_materialization_penalty == 0);
	REQUIRE(profile.saved_work_per_batch == 4096);
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK));
	REQUIRE(ExecutionRegionJitRuntimeProofRequired(profile.required_runtime_proofs,
	                                               ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK));
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
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
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.materialization_elision_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.materialization_elision_work > 0);
	REQUIRE(profile.native_grouped_state_address_lookup_count == 1);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.stateful_protocol_penalty > profile.generated_backend_stage_work);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	parameters.vectorized_parallelism = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.materialization_elision_count = 0;
	input.native_grouped_state_address_lookup_count = 0;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.vectorized_parallelism = 1;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_grouped_aggregate_stage_count == 2);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE(profile.selection_reason == "rejected_no_costed_acceleration");
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 39;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 40;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work > 0);
	REQUIRE(profile.generated_stage_work > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.native_grouped_state_address_lookup_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work == 0);
	REQUIRE(profile.generated_stage_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.expression_cost = 64;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_expression_work > 0);
	REQUIRE(profile.generated_stage_work > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
	parameters.vectorized_parallelism = 4;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.grouped_aggregate_estimated_cardinality == 1900000);
	REQUIRE(profile.stateful_protocol_penalty == 480);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.grouped_aggregate_estimated_cardinality = 3000001;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > profile.generated_backend_stage_work);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.available = true;
	parameters.compiled_vectorized.generated_stage_benefit = 4096;
	parameters.compiled_vectorized.materialization_elision_benefit = 4096;
	parameters.compiled_vectorized.startup_base_cost = 10000;
	parameters.compiled_vectorized.startup_margin_basis_points = 0;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 0);
	REQUIRE(profile.materialization_elision_work == 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = 316800;
	input.source_contract_input_cardinality = 316800;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 4096);
	REQUIRE(profile.materialization_elision_work == 4096);
	REQUIRE(profile.SelectedAcceleratedRunner());
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
	parameters.compiled_vectorized.generated_stage_benefit = 4096;

	auto profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.native_grouped_state_address_lookup_count == 1);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.source_contract_output_cardinality_unknown = true;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = input.source_contract_input_cardinality / 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_join_stage_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.generated_backend_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.generated_backend_stage_work == 2 * 4096);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());
	input.generated_backend_stage_count = 1;

	input.materialization_elision_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 720);
	REQUIRE(profile.source_contract_scan_penalty == 0);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());
	input.materialization_elision_count = 0;

	input.native_join_stage_count = 2;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty == 2 * 720);
	REQUIRE(profile.saved_work_per_batch > 0);
	REQUIRE(profile.SelectedAcceleratedRunner());

	input.estimated_cardinality = input.source_contract_input_cardinality;
	input.native_hash_join_build_sink_count = 1;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());

	input.native_hash_join_build_sink_count = 0;
	parameters.vectorized_parallelism = 4;
	profile = DuckDBCostModel::SelectPhysicalRunner(input, parameters);
	REQUIRE(profile.stateful_protocol_penalty > 0);
	REQUIRE(profile.saved_work_per_batch < 0);
	REQUIRE_FALSE(profile.SelectedAcceleratedRunner());
}

TEST_CASE("JIT auto policy selects high-work materialization through planner cost selection", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false);
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
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
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

	ConfigureSljitForCoverage(con, false, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->Collection().GetValue(3, 0).GetValue<double>() == Approx(5000050000.0));
	REQUIRE(check->Collection().GetValue(4, 0).GetValue<double>() == Approx(2500875000.0));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION);
	    },
	    [](const ExecutionRegionEvent &event) {
		    INFO(event.ir);
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

	ConfigureSljitForCoverage(con, false, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(3, 0).GetValue<double>() == Approx(50005000.0));
	REQUIRE(check->Collection().GetValue(4, 0).GetValue<double>() == Approx(25087500.0));

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 7490000);
	REQUIRE(check->Collection().GetValue(3, 0).GetValue<int64_t>() == 50015000);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "overflow_check=false");
	    },
	    [](const ExecutionRegionEvent &event) {
		    INFO(event.ir);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
		REQUIRE(check->Collection().GetValue(col, 0).GetValue<int64_t>() == 0);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_decimal_fused_overflow_input AS "
	                          "SELECT (-999999999999999999)::DECIMAL(18,0) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	// DECIMAL(18) +/- widens to DECIMAL(19) and cannot overflow; multiplication stays DECIMAL64 with checks.
	auto overflow = con.Query("CREATE TEMP TABLE jit_decimal_fused_overflow_output AS "
	                          "SELECT (d * CAST(-1 AS DECIMAL(1,0))) AS ok, "
	                          "(d * CAST(2 AS DECIMAL(1,0))) AS hi "
	                          "FROM jit_decimal_fused_overflow_input");
	REQUIRE_FAIL(overflow);
	REQUIRE(StringUtil::Contains(overflow->GetError(), "Overflow in multiplication of DECIMAL"));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           CandidateHasStructure(event, 0, 1, ExecutionRegionSinkKind::MATERIALIZATION) &&
		           StringUtil::Contains(event.ir, "native:decimal64-multiply-constant");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "error" &&
		           StringUtil::Contains(event.reason, "Overflow in multiplication of DECIMAL");
	    },
	    NoExtraJitEventCheck());
}

TEST_CASE("SLJIT fixed direct append fuses DATE arithmetic groups with checks", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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

	ConfigureSljitForCoverage(con, false, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 49965000);
	REQUIRE(check->Collection().GetValue(3, 0).ToString() == "50007500.00");
	REQUIRE(check->Collection().GetValue(4, 0).ToString() == "1992-01-01");
	REQUIRE(check->Collection().GetValue(5, 0).ToString() == "1992-12-30");
	REQUIRE(check->Collection().GetValue(6, 0).GetValue<int64_t>() == 5000);

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

	ConfigureSljitForCoverage(con, false, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 10000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 11);
	REQUIRE(check->Collection().GetValue(3, 0).GetValue<int64_t>() > 512);
	REQUIRE(check->Collection().GetValue(4, 0).GetValue<int64_t>() == 5005000);
	REQUIRE(check->Collection().GetValue(5, 0).GetValue<int64_t>() == 49965000);
	REQUIRE(check->Collection().GetValue(6, 0).ToString() == "50007500.00");

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

	ConfigureSljitForCoverage(con, false, true, 10000);
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
	REQUIRE(check->Collection().GetValue(0, 0).GetValue<int64_t>() == 100000);
	REQUIRE(check->Collection().GetValue(1, 0).GetValue<int64_t>() == 5000050000);
	REQUIRE(check->Collection().GetValue(2, 0).GetValue<int64_t>() == 14999650000);

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

	ConfigureSljitForCoverage(con, false, false, 10000);
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

	ConfigureSljitForCoverage(con, false, false, 10000);
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
		           event.runner_cost.SelectedAcceleratedRunner() &&
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

	ConfigureSljitForCoverage(con, false, false, 10000);
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
	REQUIRE(count->Collection().GetValue(0, 0).ToString() == "4991");

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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "2014");

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 150499000);
	bool saw_exact_perfect_filter_proof = false;
	for (auto &event : manager.GetEvents()) {
		if (StringUtil::Contains(EventJitRuntimePathCounts(event),
		                         "hash_join_probe.perfect_probe.exact_source_filter=")) {
			saw_exact_perfect_filter_proof = true;
			REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "backend_owned_work="));
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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 21521500);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "hash_join_probe.perfect_probe.exact_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "backend_owned_work="));
	    });
}

TEST_CASE("JIT exact dynamic filters compose with bitpacked storage scans", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(storage->Collection().GetValue(0, 0).GetValue<int64_t>() > 0);

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(f.payload) FROM packed.fact f JOIN packed.dim d USING(k) "
	                        "WHERE f.k BETWEEN 120 AND 180");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 18336600);

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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == mixed_expected);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "hash_join_probe.perfect_probe.exact_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "backend_owned_work="));
	    });

	REQUIRE_NO_FAIL(con.Query("DETACH packed"));
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT Bloom dynamic filters preserve bitpacked scan semantics", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='bitpacking'"));
	auto db_path = TestCreatePath("jit_bitpacked_bloom_dynamic_filter.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS packed (ROW_GROUP_SIZE 2048)"));
	// The trailing 1572864 (the largest dim key) makes the probe keys span the dim keys. Otherwise join statistics
	// clamp the build scan to the probe range, shrinking the build key range enough to make this a perfect hash join.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE packed.fact_big AS "
	                          "SELECT CASE WHEN i % 257 = 0 THEN NULL ELSE i END::BIGINT AS k "
	                          "FROM range(700000) tbl(i) UNION ALL SELECT 1572864"));
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
	REQUIRE(storage->Collection().GetValue(0, 0).GetValue<int64_t>() > 0);
	storage = con.Query("SELECT count(*) FROM pragma_storage_info('packed.fact_int') "
	                    "WHERE column_name = 'k' AND compression = 'BitPacking'");
	REQUIRE_NO_FAIL(*storage);
	REQUIRE(storage->Collection().GetValue(0, 0).GetValue<int64_t>() > 0);

	const string exact_query = "SELECT count(*) FROM packed.fact_big f JOIN packed.dim d ON f.k = d.k";
	const string null_equal_query =
	    "SELECT count(*) FROM packed.fact_big f JOIN packed.dim d ON f.k IS NOT DISTINCT FROM d.k";
	const string cast_query = "SELECT count(*) FROM packed.fact_int f JOIN packed.dim d ON f.k = d.k";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto exact_reference = con.Query(exact_query);
	REQUIRE_NO_FAIL(*exact_reference);
	REQUIRE(exact_reference->Collection().GetValue(0, 0).GetValue<int64_t>() == 232427);
	auto null_equal_reference = con.Query(null_equal_query);
	REQUIRE_NO_FAIL(*null_equal_reference);
	REQUIRE(null_equal_reference->Collection().GetValue(0, 0).GetValue<int64_t>() == 235151);
	auto cast_reference = con.Query(cast_query);
	REQUIRE_NO_FAIL(*cast_reference);
	REQUIRE(cast_reference->Collection().GetValue(0, 0).GetValue<int64_t>() == 232426);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto exact_result = con.Query(exact_query);
	REQUIRE_NO_FAIL(*exact_result);
	REQUIRE(exact_result->Collection().GetValue(0, 0) == exact_reference->Collection().GetValue(0, 0));
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       StringUtil::Contains(EventJitRuntimePathCounts(event),
		                            "hash_join_probe.regular_probe.exact_source_filter=");
	});
	ClearJitTrace(manager, true);
	auto null_equal_result = con.Query(null_equal_query);
	REQUIRE_NO_FAIL(*null_equal_result);
	REQUIRE(null_equal_result->Collection().GetValue(0, 0) == null_equal_reference->Collection().GetValue(0, 0));
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.regular_probe.generated=");
	});
	auto cast_result = con.Query(cast_query);
	REQUIRE_NO_FAIL(*cast_result);
	REQUIRE(cast_result->Collection().GetValue(0, 0) == cast_reference->Collection().GetValue(0, 0));

	REQUIRE_NO_FAIL(con.Query("DETACH packed"));
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT CBO carries finalized dynamic filter cardinality through regular semi joins", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_regular_dynamic_filter_fact AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS payload FROM range(200000) tbl(i)"));
	// Keep the build key span below 2^26 so DuckDB emits an exact prefix-range filter instead of Bloom + min/max.
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_regular_dynamic_filter_dim AS "
	                          "SELECT (i * 100000 + 199999)::BIGINT AS k FROM range(500) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_regular_dynamic_filter_fact f "
	                        "SEMI JOIN jit_regular_dynamic_filter_dim d USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 1);

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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);

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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "64");

	REQUIRE(manager.GetEvents().empty());
}

TEST_CASE("JIT diagnostic tracing records generic table-function source ownership", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con, true);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	ConfigureJitDecisionTrace(con);

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i * 2) FROM range(1000) tbl(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "999000");

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

TEST_CASE("JIT auto planner leaves bare ungrouped aggregates native and costs fused ones as generated work",
          "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljitForCoverage(con);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_default_aggregate_input AS "
	                          "SELECT (i % 5)::BIGINT AS i FROM range(1000000) tbl(i)"));

	// a bare ungrouped update does exactly the native update's work: nothing is left to fuse
	ClearJitTrace(manager, true);
	auto bare = con.Query("SELECT sum(i), count(*), min(i) FROM jit_auto_default_aggregate_input");
	REQUIRE_NO_FAIL(*bare);
	REQUIRE(bare->Collection().GetValue(0, 0).ToString() == "2000000");
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE(IsCompiledSljitRegionEvent(event));
	}

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i * 3) FROM jit_auto_default_aggregate_input");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "6000000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate && event.runner_cost.present &&
		           event.runner_cost.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
		    REQUIRE(event.runner_cost.generated_stage_count > 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    RequireGeneratedMachineCodeRegion(event);
	    });
}

TEST_CASE("JIT auto planner cost uses configurable CBO settings", "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", false);
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
		    REQUIRE_FALSE(event.runner_cost.SelectedAcceleratedRunner());
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

	ConfigureSljitForCoverageSettings(con, true);
	ConfigureJitDecisionTrace(con);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_branchy_expression_output AS "
	                          "SELECT " +
	                          expression + " AS v FROM jit_auto_branchy_expression_input"));
	auto result = con.Query("SELECT sum(v) FROM jit_auto_branchy_expression_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == reference->Collection().GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsCompiledSljitRegionEvent(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });
}

TEST_CASE("JIT auto policy selects high-work ungrouped aggregate updates through planner cost selection",
          "[api][jit]") {
	JitTestDatabase test;
	auto &con = test.con;
	auto &manager = test.manager;

	ConfigureSljit(con, "auto", true, true, 10000);
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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "19750163000000");

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
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
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
