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
		RequireCompiledGeneratedRegion(event);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
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
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit" || EventTarget(event) != "region" ||
		    !StringUtil::Contains(event.kernel_compile_reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(EventExecutionBody(event) == "generated-machine-code");
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(event.invocation_count > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "aggregate_update"));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT fuses multiple primitive ungrouped aggregate payload lanes into one reducer", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b), count(*) FROM jit_multi_aggregate_payload");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "333333330000");
	REQUIRE(result->GetValue(1, 0).ToString() == "10000");

	bool found_fused_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit" || EventTarget(event) != "region" ||
		    !StringUtil::Contains(event.kernel_compile_reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(
		    StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_fused_runtime);
}

TEST_CASE("JIT generic grouped primitive aggregate payload lanes require generated lookup", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_multi_aggregate_payload AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(200000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT k, sum(v), count(*) FROM jit_grouped_multi_aggregate_payload GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 200000);

	bool found_hash_lookup_boundary = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			FAIL("generic hash aggregate primitive payload update must not compile without generated lookup ownership");
		}
	}
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		found_hash_lookup_boundary = true;
		REQUIRE(EventStatus(event) == "unsupported");
		REQUIRE(EventExecutionMode(event) == "unsupported");
		REQUIRE(event.code_size == 0);
		REQUIRE(StringUtil::Contains(event.reason, "hash aggregate update requires generated hash lookup ownership"));
		REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
		                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
	}
	REQUIRE(found_hash_lookup_boundary);
}

TEST_CASE("JIT grouped aggregate uses canonical hash table lookup under high cardinality", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_high_cardinality_grouped AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(150000) tbl(i)"));

	auto reference = con.Query("SELECT count(*), sum(c)::HUGEINT, sum(s)::HUGEINT "
	                           "FROM (SELECT k, count(*) AS c, sum(v) AS s "
	                           "      FROM jit_high_cardinality_grouped GROUP BY k)");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "150000");

	ConfigureSljitSettings(con, "force", false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(c)::HUGEINT, sum(s)::HUGEINT "
	                        "FROM (SELECT k, count(*) AS c, sum(v) AS s "
	                        "      FROM jit_high_cardinality_grouped GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).ToString() == reference->GetValue(2, 0).ToString());

	bool found_high_cardinality_boundary = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			FAIL("hash aggregate lookup without generated lookup ownership must not create a compiled kernel");
		}
		if (!IsSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		found_high_cardinality_boundary = true;
		REQUIRE(EventStatus(event) == "unsupported");
		REQUIRE(EventExecutionMode(event) == "unsupported");
		REQUIRE(event.code_size == 0);
		REQUIRE(StringUtil::Contains(event.reason, "hash aggregate update requires generated hash lookup ownership"));
		REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
		                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_layout"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
	}
	REQUIRE(found_high_cardinality_boundary);
}

TEST_CASE("JIT fuses scaled decimal expression payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_aggregate_payload AS "
	                          "SELECT i, CAST(i % 1000 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum((((((((((d * CAST(2 AS DECIMAL(15,2))) + CAST(5 AS DECIMAL(15,2))) "
	                        "* CAST(3 AS DECIMAL(15,2))) - CAST(7 AS DECIMAL(15,2))) "
	                        "+ CAST(11 AS DECIMAL(15,2))) * CAST(2 AS DECIMAL(15,2))) "
	                        "- CAST(13 AS DECIMAL(15,2))) + CAST(17 AS DECIMAL(15,2))) "
	                        "* CAST(2 AS DECIMAL(15,2))) - CAST(19 AS DECIMAL(15,2))) "
	                        "FROM jit_decimal_aggregate_payload WHERE i >= 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "120530000.0000000000");

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_compile = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT fuses nullable BIGINT case payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_bigint_case_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i * 3 + 17)::BIGINT AS b, "
	                          "(i * 5 - 11)::BIGINT AS c, "
	                          "CASE WHEN i % 10 = 0 THEN NULL ELSE (i * 7 + 19)::BIGINT END AS d "
	                          "FROM range(10000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(CASE "
	                           "WHEN ((a + (b * 2)) < (c + coalesce(d, a)) "
	                           "      AND coalesce(d, a) BETWEEN (a + b - c - 1000000) "
	                           "      AND (a + b + c + 1000000)) "
	                           "THEN (((a * 3) + (b * 5) - (c * 7) + (coalesce(d, 0) * 11)) * 13) "
	                           "   + (((a - b + c) * 17) - ((a + coalesce(d, 0)) * 19)) "
	                           "   + (((b - c + coalesce(d, 0)) * 23) - ((a - coalesce(d, 0)) * 29)) "
	                           "WHEN ((a + c) > (b + coalesce(d, c)) OR "
	                           "      (a + b + c) < (coalesce(d, 0) * 2)) "
	                           "THEN (((a * 31) - (b * 37) + (c * 41) - (coalesce(d, 0) * 43)) "
	                           "   + (((a + b + c + coalesce(d, 0)) * 47) - ((a - c) * 53))) "
	                           "ELSE (((a + 7) * (b - 3)) - ((c + 5) * (coalesce(d, a) - 11))) "
	                           "END) FROM jit_bigint_case_aggregate_payload");
	REQUIRE_NO_FAIL(*reference);

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	auto result = con.Query("SELECT sum(CASE "
	                        "WHEN ((a + (b * 2)) < (c + coalesce(d, a)) "
	                        "      AND coalesce(d, a) BETWEEN (a + b - c - 1000000) "
	                        "      AND (a + b + c + 1000000)) "
	                        "THEN (((a * 3) + (b * 5) - (c * 7) + (coalesce(d, 0) * 11)) * 13) "
	                        "   + (((a - b + c) * 17) - ((a + coalesce(d, 0)) * 19)) "
	                        "   + (((b - c + coalesce(d, 0)) * 23) - ((a - coalesce(d, 0)) * 29)) "
	                        "WHEN ((a + c) > (b + coalesce(d, c)) OR "
	                        "      (a + b + c) < (coalesce(d, 0) * 2)) "
	                        "THEN (((a * 31) - (b * 37) + (c * 41) - (coalesce(d, 0) * 43)) "
	                        "   + (((a + b + c + coalesce(d, 0)) * 47) - ((a - c) * 53))) "
	                        "ELSE (((a + 7) * (b - 3)) - ((c + 5) * (coalesce(d, a) - 11))) "
	                        "END) FROM jit_bigint_case_aggregate_payload");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_compile = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
		REQUIRE(StringUtil::Contains(event.ir, "coalesce<"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT generic BIGINT sum uses hugeint local accumulation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_bigint_sum_hugeint_local AS "
	                          "SELECT i, 9223372036854775807::BIGINT AS v FROM range(4096) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "37778931862957161705472");

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	auto result = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s "
	                        "FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_compile = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT preserves stats-proven non-overflowing decimal arithmetic in expression reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_safe_aggregate_payload AS "
	                          "SELECT i, CAST(i % 1000 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum((((((((((d * CAST(2 AS DECIMAL(15,2))) + CAST(5 AS DECIMAL(15,2))) "
	                        "* CAST(3 AS DECIMAL(15,2))) - CAST(7 AS DECIMAL(15,2))) "
	                        "+ CAST(11 AS DECIMAL(15,2))) * CAST(2 AS DECIMAL(15,2))) "
	                        "- CAST(13 AS DECIMAL(15,2))) + CAST(17 AS DECIMAL(15,2))) "
	                        "* CAST(2 AS DECIMAL(15,2))) - CAST(19 AS DECIMAL(15,2))) "
	                        "FROM jit_decimal_safe_aggregate_payload WHERE i >= 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "120530000.0000000000");

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_compile = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=false"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT perfect hash aggregate generates primitive decimal sum and count star updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_count_star AS "
	                          "SELECT (i % 4)::UTINYINT AS g1, (i % 3)::UTINYINT AS g2, "
	                          "CAST(i % 100 AS DECIMAL(15,2)) AS v FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference =
	    con.Query("SELECT g1, g2, sum(v), count(*) FROM jit_perfect_hash_count_star GROUP BY g1, g2 ORDER BY g1, g2");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 12);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	ClearJitTrace(manager, true);
	auto result =
	    con.Query("SELECT g1, g2, sum(v), count(*) FROM jit_perfect_hash_count_star GROUP BY g1, g2 ORDER BY g1, g2");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	bool found_compiled_primitive_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate || !event.candidate_contract.OwnsSink() ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
			continue;
		}
		if (EventPhase(event) != "compile") {
			continue;
		}
		found_compiled_primitive_sink = true;
		RequireCompiledGeneratedRegion(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_contract<"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_layout_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_update_kind=count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "fused:primitive-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_compiled_primitive_sink);

	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit" || EventTarget(event) != "region" ||
		    !StringUtil::Contains(event.kernel_compile_reason, "fused:primitive-aggregate-update=1")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.resolve_grouped_state_addresses="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "resolve_grouped_state_addresses"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT hash aggregate cast-only keys require generated lookup ownership", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_aggregate_cast_projection AS "
	                          "SELECT i::BIGINT AS k, CAST(i % 100 AS DECIMAL(15,2)) AS v "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT cast_key, sum(v) "
	                        "FROM (SELECT CAST(k AS INTEGER) AS cast_key, v "
	                        "      FROM jit_hash_aggregate_cast_projection) t "
	                        "GROUP BY cast_key");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 100000);

	bool found_hash_lookup_boundary = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate || !event.candidate_contract.OwnsSink()) {
			continue;
		}
		if (event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		if (!StringUtil::Contains(event.reason, "vectorized projection operator contract")) {
			continue;
		}
		found_hash_lookup_boundary = true;
		REQUIRE(EventStatus(event) == "unsupported");
		REQUIRE(EventExecutionMode(event) == "unsupported");
		REQUIRE(event.code_size == 0);
		REQUIRE(StringUtil::Contains(event.reason, "hash aggregate update requires generated hash lookup ownership"));
		REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
		                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_blocker="
		                                       "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_layout"));
		REQUIRE(StringUtil::Contains(event.ir, "append_contract_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "backend_lowering_ready=false"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "generated typed projection"));
	}
	REQUIRE(found_hash_lookup_boundary);

	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			FAIL("hash aggregate lookup without generated lookup ownership must not create a compiled kernel");
		}
	}
}

TEST_CASE("JIT auto skips grouped hash aggregate updates without native lookup ownership", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_aggregate_large AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(1000000) tbl(i)"));

	auto reference = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                           "FROM (SELECT k, sum(v) AS s FROM jit_auto_hash_aggregate_large GROUP BY k)");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "1000000");

	ConfigureSljitSettings(con, "auto", false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                        "FROM (SELECT k, sum(v) AS s FROM jit_auto_hash_aggregate_large GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	bool found_auto_hash_aggregate_decision = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventRequestedPolicy(event) != "auto") {
			continue;
		}
		if (!(StringUtil::Contains(event.ir, "hash-aggregate-update") ||
		      StringUtil::Contains(event.ir, "hash-group-by") ||
		      StringUtil::Contains(event.reason, "hash-aggregate-update") ||
		      StringUtil::Contains(event.reason, "hash-group-by") ||
		      event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE)) {
			continue;
		}
		found_auto_hash_aggregate_decision = true;
		if (EventStatus(event) == "compiled") {
			REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		} else {
			REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "pure_hash_aggregate_update=true"));
	}
	REQUIRE(found_auto_hash_aggregate_decision);
}

TEST_CASE("JIT auto skips grouped hash aggregate address-only updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_aggregate_small AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                        "FROM (SELECT k, sum(v) AS s FROM jit_auto_hash_aggregate_small GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "10000");

	bool found_hash_aggregate_decision = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" ||
		    !(StringUtil::Contains(event.ir, "hash-aggregate-update") ||
		      StringUtil::Contains(event.ir, "sink=hash-group-by") ||
		      StringUtil::Contains(event.reason, "hash-aggregate-update") ||
		      StringUtil::Contains(event.reason, "sink=hash-group-by") ||
		      event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE)) {
			continue;
		}
		found_hash_aggregate_decision = true;
		REQUIRE(EventRequestedPolicy(event) == "auto");
		REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	}
	REQUIRE(found_hash_aggregate_decision);
}

TEST_CASE("JIT auto planner cost does not treat filtered hash aggregates as pure aggregate work", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_aggregate_filtered AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                        "FROM (SELECT k, sum(v) AS s "
	                        "      FROM jit_auto_hash_aggregate_filtered WHERE k >= 0 GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "1000000");

	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate || EventRequestedPolicy(event) != "auto" ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
	}
}
