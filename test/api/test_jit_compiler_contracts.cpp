#include "test_jit_contract_backends.hpp"

using namespace duckdb;

TEST_CASE("JIT region lowering excludes wrapper-only pipelines", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	auto has_wrapper_only_pipeline_shape = [](const string &value) {
		auto wrapper_only_pipeline_shape = [](const string &source, const string &sink = string()) {
			auto result = "pipeline;source:source:" + source + ":source-missing-contract";
			if (!sink.empty()) {
				result += ";sink:sink:" + sink + ":sink";
			}
			return result;
		};
		return value == wrapper_only_pipeline_shape("CREATE_TABLE_AS", "RESULT_COLLECTOR") ||
		       value == wrapper_only_pipeline_shape("RESULT_COLLECTOR") ||
		       value == wrapper_only_pipeline_shape("EXPLAIN_ANALYZE");
	};
	auto has_wrapper_only_region_source = [](const string &value) {
		auto wrapper_only_region_source = [](const string &source) {
			return "source:" + source + ":boundary";
		};
		return StringUtil::Contains(value, wrapper_only_region_source("CREATE_TABLE_AS")) ||
		       StringUtil::Contains(value, wrapper_only_region_source("RESULT_COLLECTOR")) ||
		       StringUtil::Contains(value, wrapper_only_region_source("EXPLAIN_ANALYZE"));
	};

	ConfigureSljit(con, "auto");
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE jit_inventory_wrapper_only AS SELECT 42 AS i"));
	REQUIRE(manager.GetEvents().empty());
	for (auto &counter : manager.GetDecisionCounters()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(counter.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(counter.example_reason));
	}

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE jit_inventory_wrapper_only_force AS SELECT 43 AS i"));
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.candidate_pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.candidate_context_pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(event.reason));
		REQUIRE_FALSE(has_wrapper_only_region_source(event.ir));
	}
	for (auto &counter : manager.GetDecisionCounters()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(counter.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(counter.example_reason));
	}
}

TEST_CASE("JIT region capability requires explicit compiled execution mode", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	auto backend = make_uniq<ImplicitModeRegionBackend>();
	auto &backend_ref = *backend;
	ExecutionRegionManager::Get(context).RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_implicit_mode_region_jit_backend");

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SELECT i FROM range(3) tbl(i) WHERE i > 0"));
	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_unsupported_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_implicit_mode_region_jit_backend" || event.target != "region") {
			continue;
		}
		if (event.status == "unsupported" &&
		    StringUtil::Contains(event.reason, "backend cannot generate executable code for this whole region")) {
			found_unsupported_region = true;
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.policy_decision == "force");
			REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native=1"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
		}
	}
	REQUIRE(found_unsupported_region);
}

TEST_CASE("Execution region compiler rejects compiled kernels without executable code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<ZeroCodeRegionBackend>());
	SetJitTestOptions(context, "contract_test_zero_code_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_zero_code_input AS "
	                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT i + 1 FROM jit_zero_code_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled region without executable code"));
}

TEST_CASE("Execution region compiler rejects non-compiled results with kernels", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<NonCompiledKernelResultBackend>());
	SetJitTestOptions(context, "contract_test_non_compiled_kernel_result_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_non_compiled_kernel_input AS "
	                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT i + 1 FROM jit_non_compiled_kernel_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "returned kernel for non-compiled region status unsupported"));
}

TEST_CASE("Execution region compiler rejects backend-fused regions across core boundary stages", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<FullPipelineAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_full_pipeline_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_core_gate = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_full_pipeline_abi_region_jit_backend" || event.target != "region" ||
		    event.status != "unsupported") {
			continue;
		}
		if (!StringUtil::Contains(event.reason,
		                          "backend advertised fused region but core contract still has boundaries")) {
			continue;
		}
		found_core_gate = true;
		REQUIRE(event.execution_mode == "unsupported");
		REQUIRE(event.region_execution_form == "fused");
		REQUIRE((event.candidate_contract.source_boundary_count > 0 ||
		         event.candidate_contract.missing_contract_count > 0));
		REQUIRE((StringUtil::Contains(event.reason, "source_boundaries=") ||
		         StringUtil::Contains(event.reason, "missing_contracts=")));
		REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
	}
	REQUIRE(found_core_gate);
}

TEST_CASE("JIT maximal region planner does not emit sink-only ABI candidates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ConfigureSljit(con);

	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM range(3) tbl(i)"));
	for (auto &event : manager.GetEvents()) {
		if (!event.has_candidate) {
			continue;
		}
		REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
		if (event.candidate_contract.OwnsSink()) {
			REQUIRE(event.candidate_contract.OwnsSource());
		}
	}
}

TEST_CASE("Execution region compiler rejects full pipeline kernels without full-pipeline ABI", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<FullPipelineAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_full_pipeline_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_full_abi_input AS SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT sum(i) FROM jit_full_abi_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled full pipeline without full-pipeline executable ABI"));
}

TEST_CASE("JIT full pipeline ABI rejects runtime false return", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = ExecutionRegionManager::Get(context);

	ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<FalseReturningFullPipelineRegionBackend>());
	SetJitTestOptions(context, "contract_test_false_returning_full_pipeline_region_jit_backend");
	Settings::Set<JitTraceRuntimeSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_false_returning_full_input AS SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT sum(i) FROM jit_false_returning_full_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled full pipeline kernel returned false at runtime"));

	bool found_compiled_full_pipeline = false;
	bool found_error_full_pipeline = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_false_returning_full_pipeline_region_jit_backend" ||
		    event.target != "region" || !event.has_candidate ||
		    event.candidate_contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			continue;
		}
		if (event.status == "compiled") {
			found_compiled_full_pipeline = true;
			REQUIRE(event.execution_mode == "native");
		}
		if (event.phase == "runtime" && event.status == "error") {
			found_error_full_pipeline = true;
			REQUIRE(event.execution_mode == "native");
			REQUIRE(StringUtil::Contains(event.reason, "compiled full pipeline kernel returned false at runtime"));
		}
		REQUIRE(event.status != string("de") + "clined");
	}
	REQUIRE(found_compiled_full_pipeline);
	REQUIRE(found_error_full_pipeline);
}

TEST_CASE("Compiled full pipeline runtime reports JIT-only exceptions", "[api][jit]") {
	{
		DuckDB db;
		Connection con(db);
		auto &context = *con.context;
		auto &manager = ExecutionRegionManager::Get(context);

		ExecutionRegionManager::Get(context).RegisterBackend(make_uniq<ThrowingVerifiedRegionBackend>());
		SetJitTestOptions(context, "contract_test_throwing_region_jit_backend");
		Settings::Set<JitVerifySetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

		REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_throwing_verify_input AS "
		                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
		auto result = con.Query("SELECT i + 1 FROM jit_throwing_verify_input WHERE i > 0");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "contract test region runtime failure"));
	}
}

TEST_CASE("JIT code handles clean up through their base interface", "[api][jit]") {
	bool destroyed = false;
	{
		unique_ptr<ExecutionRegionCodeHandle> handle = make_uniq<CountingCodeHandle>(destroyed);
		REQUIRE(handle->CodeSize() == 17);
	}
	REQUIRE(destroyed);
}
