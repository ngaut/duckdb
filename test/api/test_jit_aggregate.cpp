#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT ungrouped aggregate sinks use native state-update contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_boundary AS "
	                          "SELECT i::BIGINT AS i, CASE WHEN i % 5 = 0 THEN NULL ELSE i::BIGINT END AS v "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT sum(v) FROM jit_aggregate_boundary WHERE i > 100");
	REQUIRE_NO_FAIL(*result);

	bool found_aggregate_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate || !event.candidate_contract.OwnsSink() ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		found_aggregate_update = true;
		RequireCompiledFusedOperatorProtocolRegion(event);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink protocol"));
		REQUIRE(StringUtil::Contains(event.reason, "ungrouped-aggregate-native-state-update"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_aggregate_update);
}

TEST_CASE("JIT fuses projection payloads into primitive ungrouped aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_payload AS "
	                          "SELECT (i % 1000)::BIGINT AS a, (i % 10)::BIGINT AS b "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_aggregate_payload");
	REQUIRE_NO_FAIL(*result);

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_compile = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "operator-stage:aggregate-update"));
		REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
	}
	REQUIRE(found_compile);

	bool found_runtime = false;
	for (auto &counter : manager.GetKernelCounters()) {
		if (counter.backend_name != "sljit" || counter.target != "region" ||
		    counter.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(counter.compile_reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(counter.execution_body == "generated-machine-code");
		REQUIRE(counter.code_size > 0);
		REQUIRE(counter.invocation_count > 0);
		REQUIRE(StringUtil::Contains(counter.generated_stage_runtime_breakdown, "aggregate_update"));
		REQUIRE_FALSE(StringUtil::Contains(counter.generated_stage_runtime_breakdown, "projection"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT grouped aggregate contracts compile native state updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_aggregate_boundary AS "
	                          "SELECT (i % 8)::BIGINT AS k, i::BIGINT AS v FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT k, count(*), sum(v) FROM jit_grouped_aggregate_boundary GROUP BY k ORDER BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 8);

	bool found_grouped_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate || !event.candidate_contract.OwnsSink() ||
		    (event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		     event.candidate_traits.sink_kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE)) {
			continue;
		}
		found_grouped_update = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_contract<"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_layout_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_grouped_update);
}
