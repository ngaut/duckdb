#include "test_jit_helpers.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"

using namespace duckdb;

TEST_CASE("JIT ungrouped aggregate sinks use native state-update contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
		RequireGeneratedMachineCodeRegion(event);
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

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
	}
	REQUIRE(found_compile);

	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update=")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(event.invocation_count > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "aggregate_update"));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                   "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT fuses generated filters into primitive ungrouped aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_aggregate_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b "
	                          "FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(a * b) FROM jit_filtered_aggregate_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_filtered_aggregate_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_filtered_aggregate = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_aggregate = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_filtered_aggregate);
}

TEST_CASE("JIT filtered aggregate remaps expression-tree sources after scan filter projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_aggregate_source_remap AS "
	                          "SELECT CASE i % 10 "
	                          "       WHEN 0 THEN 0::BIGINT "
	                          "       WHEN 1 THEN 1::BIGINT "
	                          "       WHEN 2 THEN 2::BIGINT "
	                          "       WHEN 3 THEN 3::BIGINT "
	                          "       WHEN 4 THEN 4::BIGINT "
	                          "       WHEN 5 THEN 5::BIGINT "
	                          "       WHEN 6 THEN 6::BIGINT "
	                          "       WHEN 7 THEN 7::BIGINT "
	                          "       WHEN 8 THEN 8::BIGINT "
	                          "       ELSE 9::BIGINT END AS discount, "
	                          "       CAST(1 + (i % 50) AS BIGINT) AS quantity, "
	                          "       CAST(100 + (i % 1000) AS BIGINT) AS extendedprice "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT sum(extendedprice * discount) "
	                     "FROM jit_filtered_aggregate_source_remap "
	                     "WHERE discount BETWEEN 5 AND 7 "
	                     "  AND quantity < 24";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_scan_filtered_aggregate = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || event.candidate_traits.source_filter_count <= 1 ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		found_scan_filtered_aggregate = true;
		RequireDuckDBScanFilteredSourceContract(event);
	}
	REQUIRE(found_scan_filtered_aggregate);

}

TEST_CASE("JIT fuses generated filters into primitive count-star aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_count_star_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b "
	                          "FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT count(*) FROM jit_filtered_count_star_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_filtered_count_star_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_filtered_count = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_count = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_filtered_count);
}

TEST_CASE("JIT fuses generated filters into multiple primitive aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_multi_aggregate_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b, "
	                          "CASE WHEN i % 3571 = 0 THEN NULL ELSE (i * 10)::BIGINT END AS d "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT count(*), sum(a * b), sum(d) "
	                     "FROM jit_filtered_multi_aggregate_payload WHERE (a + b) > 1000";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).ToString() == reference->GetValue(2, 0).ToString());

	bool found_filtered_multi = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_multi = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused="));
	}
	REQUIRE(found_filtered_multi);
}

TEST_CASE("JIT auto CBO selects high-work filtered primitive aggregate fusion", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_filtered_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i + 3)::BIGINT AS b, (i % 97)::BIGINT AS c "
	                          "FROM range(500000) tbl(i)"));

	const string payload = "((((a * b) + (a * 7)) - (b * 13)) + ((a + c) * (b - c)))";
	const string predicate = "((a + b) > 1000) AND (((a * 3) + (b * 5)) > 10000)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference =
	    con.Query("SELECT sum(" + payload + ") FROM jit_auto_filtered_aggregate_payload WHERE " + predicate);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(" + payload + ") FROM jit_auto_filtered_aggregate_payload WHERE " + predicate);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
		    REQUIRE(event.runner_cost.materialization_elision_count == 1);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit > event.runner_cost.required_benefit);
		    REQUIRE(StringUtil::Contains(event.reason, "duckdb_cbo selects compiled-vectorized physical runner"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		    RequireGeneratedMachineCodeRegion(event);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.filtered_primitive_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "filter.selection="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                       "aggregate_update.primitive_payload_update="));
	    });
}

TEST_CASE("JIT fuses multiple primitive ungrouped aggregate payload lanes into one reducer", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_fused_runtime);
}

TEST_CASE("JIT fuses decimal CASE aggregate payload lanes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_payload_probe AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       100.00::DECIMAL(15,2) + (i % 13)::DECIMAL(15,2) AS base_amount, "
	                          "       0.10::DECIMAL(15,2) AS rebate_rate "
	                          "FROM range(0, 20000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_payload_dim AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       CASE WHEN i % 7 IN (0, 3) THEN 'PRIORITY BRUSHED STEEL' "
	                          "            ELSE 'STANDARD ANODIZED COPPER' END AS category_name "
	                          "FROM range(0, 20000) tbl(i)"));

	const string query = "SELECT sum(CASE WHEN category_name LIKE 'PRIORITY%' "
	                     "                THEN base_amount * (1.00 - rebate_rate) "
	                     "                ELSE 0.0000 END) AS matched_sum, "
	                     "       sum(base_amount * (1.00 - rebate_rate)) AS total_sum "
	                     "FROM jit_decimal_case_payload_probe, jit_decimal_case_payload_dim "
	                     "WHERE jit_decimal_case_payload_probe.item_key = jit_decimal_case_payload_dim.item_key";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "hash_join_probe") &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "case<logical=DECIMAL"));
		    REQUIRE(StringUtil::Contains(event.ir, "string_prefix"));
	    });

	bool found_fused_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		const bool fused_ungrouped_update =
		    StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused=") ||
		    StringUtil::Contains(stage_counts, "aggregate_update.direct_join_output_ungrouped_payload_update=");
		if (!fused_ungrouped_update) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_fused_runtime);
}

TEST_CASE("JIT perfect hash aggregate composes date-year group projection into fused payload update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_date_year_group AS "
	                          "SELECT DATE '1995-01-01' + (i % 700)::INTEGER AS d, "
	                          "       (i % 100)::DECIMAL(15,2) AS ep, "
	                          "       0.05::DECIMAL(15,2) AS disc, "
	                          "       CASE WHEN i % 2 = 0 THEN 'BRAZIL' ELSE 'PERU' END AS n "
	                          "FROM range(0, 10000) tbl(i)"));

	const string query = "SELECT year(d) AS y, "
	                     "       sum(CASE WHEN n='BRAZIL' THEN ep * (1 - disc) ELSE 0 END) AS a, "
	                     "       sum(ep * (1 - disc)) AS b "
	                     "FROM jit_perfect_hash_date_year_group GROUP BY 1 ORDER BY 1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < reference->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < reference->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "group_expressions=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		    REQUIRE(StringUtil::Contains(event.ir, "integral_compress"));
		    REQUIRE(StringUtil::Contains(event.ir, "date_year"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "op0=projection("));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
	    });
}

TEST_CASE("JIT generic grouped primitive aggregate payload lanes use native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_multi_aggregate_payload AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(200000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT k, sum(v), count(*) FROM jit_grouped_multi_aggregate_payload GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 200000);

	bool found_hash_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			found_hash_state_address_update = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		}
	}
	REQUIRE(found_hash_state_address_update);

	bool found_direct_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto runtime_paths = EventJitRuntimePathCounts(event);
		if (!StringUtil::Contains(runtime_paths, "aggregate_update.direct_append_new_grouped_primitive_update=")) {
			continue;
		}
		found_direct_runtime = true;
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		auto boundaries = EventJitMaterializationBoundaryCounts(event);
		REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_append_new_grouped_primitive_update"
		                                           ".find_new.state_address_update="));
		REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_append_new_grouped_primitive_update"
		                                           ".direct_append_new.primitive_update="));
		REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
		REQUIRE_FALSE(StringUtil::Contains(boundaries, "aggregate_update.address_vector_payload_update="));
		REQUIRE_FALSE(
		    StringUtil::Contains(runtime_paths, "aggregate_update.fused_payload_update_with_grouped_state_addresses="));
	}
	REQUIRE(found_direct_runtime);
}

TEST_CASE("Grouped aggregate dense lookup supports nullable all-valid group layouts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	const idx_t group_count = STANDARD_VECTOR_SIZE;
	DataChunk groups;
	groups.Initialize(allocator, {LogicalType::INTEGER});
	auto group_data = FlatVector::GetDataMutable<int32_t>(groups.data[0]);
	for (idx_t row_idx = 0; row_idx < group_count; row_idx++) {
		group_data[row_idx] = UnsafeNumericCast<int32_t>(row_idx);
	}
	groups.SetChildCardinality(group_count);

	ExecutionDenseGroupDomain dense_domain;
	dense_domain.ready = true;
	dense_domain.physical_type = PhysicalType::INT32;
	dense_domain.min_key = 0;
	dense_domain.max_key = group_count - 1;
	dense_domain.distinct_count = group_count;

	struct DenseLookupUpdateState {
		DataChunk *groups = nullptr;
		vector<uintptr_t> address_by_key;
		bool expect_existing = false;
		idx_t update_count = 0;
	};
	auto record_addresses = [](const uintptr_t *addresses, const sel_t *address_sel, const sel_t *execute_sel,
	                           idx_t count, void *state_p) {
		auto &state = *reinterpret_cast<DenseLookupUpdateState *>(state_p);
		REQUIRE(addresses);
		REQUIRE(state.groups);
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			const auto address_idx = address_sel ? address_sel[row_idx] : idx;
			const auto address = addresses[address_idx];
			REQUIRE(address != 0);
			const auto key = state.groups->data[0].GetValue(row_idx).GetValue<int32_t>();
			REQUIRE(key >= 0);
			REQUIRE(UnsafeNumericCast<idx_t>(key) < state.address_by_key.size());
			auto &stored_address = state.address_by_key[UnsafeNumericCast<idx_t>(key)];
			if (state.expect_existing) {
				REQUIRE(stored_address == address);
			} else {
				REQUIRE(stored_address == 0);
				stored_address = address;
			}
			state.update_count++;
		}
	};

	DenseLookupUpdateState state;
	state.groups = &groups;
	state.address_by_key.resize(group_count, 0);
	REQUIRE(ht.TryFindOrCreateGroupsSelectedStateUpdateFast(groups, record_addresses, &state, nullptr, nullptr,
	                                                        &dense_domain));
	REQUIRE(state.update_count == group_count);
	REQUIRE(ht.Count() == group_count);

	const idx_t selected_count = 512;
	vector<sel_t> selected_rows;
	selected_rows.reserve(selected_count);
	for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
		selected_rows.push_back(UnsafeNumericCast<sel_t>(selected_count - row_idx - 1));
	}
	SelectionVector selected_row_selection(selected_rows.data(), selected_count);
	DataChunk selected_groups;
	selected_groups.Initialize(allocator, {LogicalType::INTEGER});
	selected_groups.data[0].Slice(groups.data[0], selected_row_selection, selected_count);
	selected_groups.SetChildCardinality(selected_count);

	state.groups = &selected_groups;
	state.expect_existing = true;
	state.update_count = 0;
	REQUIRE(ht.TryFindOrCreateGroupsSelectedStateUpdateFast(selected_groups, record_addresses, &state, nullptr, nullptr,
	                                                        &dense_domain));
	REQUIRE(state.update_count == selected_count);
	REQUIRE(ht.Count() == group_count);
}

static ExecutionRowPointerGroupKeySource BuildBigintToIntegerInputVectorGroupSource() {
	ExecutionRowPointerGroupKeySource source;
	source.ready = true;
	source.source_kind = ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR;
	source.source_type = LogicalType::BIGINT;
	source.target_type = LogicalType::INTEGER;
	source.source_physical_type = PhysicalType::INT64;
	source.target_physical_type = PhysicalType::INT32;
	source.input_vector_index = 0;
	source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32;
	source.unchecked_integral_cast = true;
	source.all_valid = true;
	return source;
}

TEST_CASE("Grouped aggregate input-vector group targets use descriptor lookup directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	const idx_t group_count = STANDARD_VECTOR_SIZE;
	DataChunk payload_input;
	payload_input.Initialize(allocator, {LogicalType::BIGINT});
	auto input_data = FlatVector::GetDataMutable<int64_t>(payload_input.data[0]);
	for (idx_t row_idx = 0; row_idx < group_count; row_idx++) {
		input_data[row_idx] = UnsafeNumericCast<int64_t>(row_idx);
	}
	FlatVector::SetSize(payload_input.data[0], group_count);
	payload_input.SetChildCardinality(group_count);

	vector<ExecutionRowPointerGroupKeySource> group_sources;
	group_sources.push_back(BuildBigintToIntegerInputVectorGroupSource());

	auto verify_targets = [&](DataChunk &input, ExecutionGroupedAggregateStateTargetBatch &targets,
	                          vector<uintptr_t> &address_by_key, bool expect_existing) {
		auto &span = targets.InputOrder();
		REQUIRE(span.addresses);
		REQUIRE(span.count == input.size());
		for (idx_t target_idx = 0; target_idx < span.count; target_idx++) {
			const auto row_idx = span.row_sel ? span.row_sel[target_idx] : target_idx;
			const auto address_idx = span.address_sel ? span.address_sel[target_idx] : target_idx;
			const auto address = span.addresses[address_idx];
			REQUIRE(address != 0);
			const auto key = input.data[0].GetValue(row_idx).GetValue<int64_t>();
			REQUIRE(key >= 0);
			REQUIRE(UnsafeNumericCast<idx_t>(key) < address_by_key.size());
			auto &stored_address = address_by_key[UnsafeNumericCast<idx_t>(key)];
			if (expect_existing) {
				REQUIRE(stored_address == address);
			} else {
				REQUIRE(stored_address == 0);
				stored_address = address;
			}
		}
	};

	ExecutionGroupedAggregateStateTargetBatch targets;
	vector<uintptr_t> address_by_key(group_count, 0);
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(payload_input, payload_input.size(), group_sources,
	                                                           targets));
	verify_targets(payload_input, targets, address_by_key, false);
	REQUIRE(ht.Count() == group_count);

	const idx_t selected_count = 512;
	vector<sel_t> selected_rows;
	selected_rows.reserve(selected_count);
	for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
		selected_rows.push_back(UnsafeNumericCast<sel_t>(selected_count - row_idx - 1));
	}
	SelectionVector selected_row_selection(selected_rows.data(), selected_count);
	DataChunk selected_payload_input;
	selected_payload_input.Initialize(allocator, {LogicalType::BIGINT});
	selected_payload_input.data[0].Slice(payload_input.data[0], selected_row_selection, selected_count);
	selected_payload_input.SetChildCardinality(selected_count);

	targets.Reset();
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(selected_payload_input, selected_payload_input.size(),
	                                                           group_sources, targets));
	verify_targets(selected_payload_input, targets, address_by_key, true);
	REQUIRE(ht.Count() == group_count);
}

TEST_CASE("Grouped aggregate input-vector targets rebuild dense cache from existing groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	DataChunk existing_groups;
	existing_groups.Initialize(allocator, {LogicalType::INTEGER});
	auto existing_group_data = FlatVector::GetDataMutable<int32_t>(existing_groups.data[0]);
	existing_group_data[0] = 0;
	FlatVector::SetSize(existing_groups.data[0], 1);
	existing_groups.SetChildCardinality(1);

	Vector existing_addresses(LogicalType::POINTER);
	ht.FindOrCreateGroups(existing_groups, existing_addresses);
	existing_addresses.Flatten();
	const auto existing_group_address =
	    reinterpret_cast<uintptr_t>(FlatVector::GetData<data_ptr_t>(existing_addresses)[0]);
	REQUIRE(existing_group_address != 0);
	REQUIRE(ht.Count() == 1);

	DataChunk payload_input;
	payload_input.Initialize(allocator, {LogicalType::BIGINT});
	auto input_data = FlatVector::GetDataMutable<int64_t>(payload_input.data[0]);
	input_data[0] = 0;
	input_data[1] = 1;
	input_data[2] = 2;
	FlatVector::SetSize(payload_input.data[0], 3);
	payload_input.SetChildCardinality(3);

	ExecutionDenseGroupDomain dense_domain;
	dense_domain.ready = true;
	dense_domain.physical_type = PhysicalType::INT32;
	dense_domain.min_key = 0;
	dense_domain.max_key = 2;
	dense_domain.distinct_count = 3;

	vector<ExecutionRowPointerGroupKeySource> group_sources;
	group_sources.push_back(BuildBigintToIntegerInputVectorGroupSource());

	ExecutionGroupedAggregateStateTargetBatch targets;
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(payload_input, payload_input.size(), group_sources,
	                                                           targets, nullptr, &dense_domain));
	auto &span = targets.InputOrder();
	REQUIRE(span.addresses);
	REQUIRE(span.count == 3);
	REQUIRE(span.addresses[0] == existing_group_address);
	REQUIRE(span.addresses[1] != 0);
	REQUIRE(span.addresses[2] != 0);
	REQUIRE(span.addresses[1] != existing_group_address);
	REQUIRE(span.addresses[2] != existing_group_address);
	REQUIRE(ht.Count() == 3);
}

TEST_CASE("JIT regular hash aggregate updates mixed preaggregated groups directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_existing_grouped_update AS "
	                          "SELECT CASE WHEN i % 4 = 0 THEN 'A' "
	                          "            WHEN i % 4 = 1 THEN 'B' "
	                          "            WHEN i % 4 = 2 THEN 'C' ELSE 'D' END AS k, "
	                          "       (i % 100)::BIGINT AS v "
	                          "FROM range(200000) tbl(i)"));

	const string query = "SELECT k, sum(v), count(*) FROM jit_existing_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 4);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return StringUtil::Contains(stage_counts, "aggregate_update.local_preaggregate_primitive_groups=") &&
		           StringUtil::Contains(stage_counts,
		                                "aggregate_update.direct_preaggregated_grouped_primitive_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "aggregate_update.direct_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.preaggregated_primitive_groups="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.direct_state_update="));
	    });
}

TEST_CASE("JIT preaggregated grouped primitive updates existing compact groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preaggregated_existing_grouped_update AS "
	                          "SELECT ((i // 2) % 2)::INTEGER AS k, "
	                          "       (i % 2 = 0)::INTEGER AS high_payload, "
	                          "       (i % 2 <> 0)::INTEGER AS low_payload "
	                          "FROM range(200000) tbl(i)"));

	const string query = "SELECT k, "
	                     "       sum(high_payload), "
	                     "       sum(low_payload) "
	                     "FROM jit_preaggregated_existing_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return StringUtil::Contains(stage_counts, "aggregate_update.local_preaggregate_primitive_groups=") &&
		           StringUtil::Contains(stage_counts,
		                                "aggregate_update.direct_preaggregated_grouped_primitive_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "aggregate_update.direct_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_preaggregated_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.preaggregated_primitive_groups="));
	    });
}

TEST_CASE("JIT preaggregated grouped aggregate avoids source-row reserve", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preaggregated_group_reserve AS "
	                          "SELECT ((i // 2) % 100000)::INTEGER AS k, "
	                          "       (i % 2 = 0)::INTEGER AS high_payload, "
	                          "       (i % 2 <> 0)::INTEGER AS low_payload "
	                          "FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_preaggregated_group_reserve"));

	const string query = "SELECT k, "
	                     "       sum(high_payload), "
	                     "       sum(low_payload) "
	                     "FROM jit_preaggregated_group_reserve GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 100000);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    return StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.preaggregated_grouped_primitive_reserve_target=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths,
		                                 "aggregate_update.preaggregated_grouped_primitive_reserve_target="));
		    REQUIRE_FALSE(StringUtil::Contains(
		        runtime_paths, "aggregate_update.preaggregated_grouped_primitive_reserve_target=200000"));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.preaggregated_grouped_primitive_reserve=1"));
		    REQUIRE_FALSE(
		        StringUtil::Contains(stage_counts, "preaggregated_grouped_primitive_reserve.reserve_groups.resize="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "direct_append_preaggregated_grouped_primitive_update.find_new.resize"));
	    });
}

TEST_CASE("JIT count-star distribution aggregate uses dense preaggregated groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_distribution_ids AS "
	                          "SELECT i::BIGINT AS id FROM range(4200) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_distribution_orders AS "
	                          "SELECT id, seq "
	                          "FROM jit_count_star_distribution_ids, range(42) reps(seq) "
	                          "WHERE seq < id % 42"));

	const string query = "SELECT c_count, count(*) AS customer_count "
	                     "FROM ("
	                     "    SELECT ids.id, count(orders.seq) AS c_count "
	                     "    FROM jit_count_star_distribution_ids ids "
	                     "    LEFT JOIN jit_count_star_distribution_orders orders ON ids.id = orders.id "
	                     "    GROUP BY ids.id"
	                     ") grouped_counts "
	                     "GROUP BY c_count ORDER BY c_count";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 42);
	for (idx_t row_idx = 0; row_idx < reference->RowCount(); row_idx++) {
		REQUIRE(reference->GetValue(0, row_idx).ToString() == to_string(row_idx));
		REQUIRE(reference->GetValue(1, row_idx).ToString() == "100");
	}

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths, "aggregate_update.direct_input_vector_group_count_one_lookup=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "aggregate_update.direct_input_vector_group_count_one_lookup"
		                                 ".find_or_create_input_vector_dense.probe="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_input_vector_group_count_one_lookup="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_projection_input_vector_grouped_update="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.input_vector_group_count_one_update="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "projection.batch_append="));
	    });
}

TEST_CASE("JIT count-star grouped aggregate uses row-delta backend for high-cardinality batches", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_row_delta AS "
	                          "SELECT i::INTEGER AS k FROM range(2048) tbl(i)"));

	const string query = "SELECT k + 0 AS g, count(*) AS c "
	                     "FROM jit_count_star_row_delta GROUP BY g ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2048);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		REQUIRE(result->GetValue(0, row_idx).ToString() == reference->GetValue(0, row_idx).ToString());
		REQUIRE(result->GetValue(1, row_idx).ToString() == "1");
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    return StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.primitive_grouped_count_star_row_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.primitive_grouped_count_star_row_update=2048"));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.direct_state_update=2048"));
	    });
}

TEST_CASE("JIT regular hash aggregate appends new groups directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_new_grouped_update AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v "
	                          "FROM range(64) tbl(i)"));

	const string query = "SELECT k, sum(v), count(*) FROM jit_new_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 64);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return StringUtil::Contains(stage_counts, "direct_append_new_grouped_primitive_update=") &&
		           StringUtil::Contains(stage_counts, "find_new.append_new_groups=") &&
		           StringUtil::Contains(stage_counts, "find_new.state_address_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(
		        StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "direct_append_new.primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_append_new_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.direct_state_update="));
	    });
}

TEST_CASE("JIT regular hash aggregate fuses typed expression payloads with native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_typed_payload AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT count(*), sum(s)::HUGEINT, sum(s2)::HUGEINT, sum(s3)::HUGEINT, sum(s4)::HUGEINT "
	                     "FROM (SELECT k, "
	                     "             sum(p * (1.00::DECIMAL(15,2) - d)) AS s, "
	                     "             sum((p + 3.00::DECIMAL(15,2)) * (d + 1.00::DECIMAL(15,2))) AS s2, "
	                     "             sum((p - d) * (p + d)) AS s3, "
	                     "             sum(((p * 2.00::DECIMAL(15,2)) + (d * 7.00::DECIMAL(15,2))) - "
	                     "                 11.00::DECIMAL(15,2)) AS s4 "
	                     "      FROM jit_regular_hash_typed_payload GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "10000");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
		    REQUIRE(StringUtil::Contains(event.ir, "columns=3"));
		    REQUIRE(StringUtil::Contains(event.ir, "aggregates=4"));
		    REQUIRE(StringUtil::Contains(event.ir, "DECIMAL(18,4)"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_grouped_dense_group_domain="));
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update"));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.local_preaggregate_primitive_groups="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.direct_append_new_grouped_primitive_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.direct_new_grouped_primitive_update="));
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update"
		                                               ".find_or_create_dense.probe="));
		    auto boundary_counts = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(boundary_counts, "aggregate_update.direct_state_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update"
		                                                     ".find_or_create_fast.probe="));
		    REQUIRE_FALSE(StringUtil::Contains(boundary_counts, "aggregate_update.address_vector_direct_new="));
		    REQUIRE_FALSE(StringUtil::Contains(boundary_counts, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT regular hash aggregate fuses typed expression payloads for existing groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_existing_typed_payload AS "
	                          "SELECT (i % 100)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT count(*), sum(s)::HUGEINT, sum(s2)::HUGEINT, sum(s3)::HUGEINT, sum(s4)::HUGEINT "
	                     "FROM (SELECT k, "
	                     "             sum(p * (1.00::DECIMAL(15,2) - d)) AS s, "
	                     "             sum((p + 3.00::DECIMAL(15,2)) * (d + 1.00::DECIMAL(15,2))) AS s2, "
	                     "             sum((p - d) * (p + d)) AS s3, "
	                     "             sum(((p * 2.00::DECIMAL(15,2)) + (d * 7.00::DECIMAL(15,2))) - "
	                     "                 11.00::DECIMAL(15,2)) AS s4 "
	                     "      FROM jit_regular_hash_existing_typed_payload GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "100");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree") &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.direct_new_grouped_primitive_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update"));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.local_preaggregate_primitive_groups="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.direct_append_new_grouped_primitive_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts,
		                                       "aggregate_update.direct_new_grouped_primitive_update="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.resolve_grouped_state_addresses="));
		    auto boundary_counts = EventJitMaterializationBoundaryCounts(event);
		    REQUIRE(StringUtil::Contains(boundary_counts, "aggregate_update.direct_state_update="));
		    REQUIRE_FALSE(StringUtil::Contains(boundary_counts, "aggregate_update.address_vector_payload_update="));
	    });
}

TEST_CASE("JIT native tail does not consume rewritten grouped aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_regular_hash_typed_payload AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       DATE '1996-01-01' + CAST(i % 180 AS INTEGER) AS shipdate, "
	                          "       CAST(10000 + (i % 1000) AS DECIMAL(15,2)) AS price, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS discount "
	                          "FROM range(20000) tbl(i)"));

	const string query = "SELECT count(*), sum(revenue)::HUGEINT "
	                     "FROM (SELECT supplier_no, sum(price * (1.00::DECIMAL(15,2) - discount)) AS revenue "
	                     "      FROM (SELECT CAST(k AS INTEGER) AS supplier_no, price, discount "
	                     "            FROM jit_filtered_regular_hash_typed_payload "
	                     "            WHERE shipdate >= DATE '1996-02-01' AND shipdate < DATE '1996-05-01') q "
	                     "      GROUP BY supplier_no) grouped_revenue";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 1);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payload_projection_partially_composed=true") &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
		    REQUIRE(StringUtil::Contains(event.ir, "columns=3"));
	    });

	bool found_generated_update = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.native_sink_update="));
		if (StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update=")) {
			found_generated_update = true;
		}
	}
	REQUIRE(found_generated_update);
}

TEST_CASE("JIT regular hash aggregate fuses INT32 CASE payloads into hugeint grouped reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_int32_case_payload AS "
	                          "SELECT CASE WHEN i % 4 = 0 THEN 'MAIL' "
	                          "            WHEN i % 4 = 1 THEN 'SHIP' "
	                          "            WHEN i % 4 = 2 THEN 'RAIL' "
	                          "            ELSE 'AIR' END AS shipmode, "
	                          "       CASE WHEN i % 5 = 0 THEN '1-URGENT' "
	                          "            WHEN i % 5 = 1 THEN '2-HIGH' "
	                          "            WHEN i % 5 = 2 THEN '3-MEDIUM' "
	                          "            ELSE '4-NOT SPECIFIED' END AS priority "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT shipmode, "
	                     "       sum(CASE WHEN priority = '1-URGENT' OR priority = '2-HIGH' THEN 1 ELSE 0 END) "
	                     "           AS high_line_count, "
	                     "       sum(CASE WHEN priority = '1-URGENT' THEN 1 ELSE 0 END) AS urgent_line_count "
	                     "FROM jit_regular_hash_int32_case_payload GROUP BY shipmode ORDER BY shipmode";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 4);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree") &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		    REQUIRE(StringUtil::Contains(event.ir, "case<"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.direct_new_grouped_primitive_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_new_grouped_primitive_payload_update="));
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.direct_new_grouped_primitive_payload_update="));
	    });
}

TEST_CASE("JIT grouped aggregate uses native state addresses under high cardinality", "[api][jit]") {
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

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(c)::HUGEINT, sum(s)::HUGEINT "
	                        "FROM (SELECT k, count(*) AS c, sum(v) AS s "
	                        "      FROM jit_high_cardinality_grouped GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).ToString() == reference->GetValue(2, 0).ToString());

	bool found_high_cardinality_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			found_high_cardinality_state_address_update = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		}
	}
	REQUIRE(found_high_cardinality_state_address_update);
}

TEST_CASE("JIT fuses scaled decimal expression payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT fuses nullable BIGINT case payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
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
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
		REQUIRE(StringUtil::Contains(event.ir, "coalesce<"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT typed-tree aggregate payloads preserve all-NULL branch results", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_typed_tree_sum_null_branch AS "
	                          "SELECT i::BIGINT AS a FROM range(10000) tbl(i)"));

	const string query = "SELECT sum(CASE WHEN a < 0 THEN a ELSE NULL::BIGINT END) "
	                     "FROM jit_typed_tree_sum_null_branch";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).IsNull());

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).IsNull());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
		REQUIRE(StringUtil::Contains(event.ir, "validity=constant-null"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT generic BIGINT sum uses hugeint local accumulation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_bigint_sum_hugeint_local AS "
	                          "SELECT i, 9223372036854775807::BIGINT AS v FROM range(4096) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "37778931862957161705472");

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s "
	                        "FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
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

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=false"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT fuses multi-key perfect-hash aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_key_perfect_hash_payload AS "
	                          "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS group_flag, "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS group_status, "
	                          "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS qty_value, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS gross_value, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount_rate, "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) AS tax_rate "
	                          "FROM range(120000) tbl(i)"));

	const string query = "SELECT group_flag, group_status, "
	                     "       sum(qty_value), "
	                     "       sum(gross_value), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate)), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate) * "
	                     "           (1.00::DECIMAL(15,2) + tax_rate)), "
	                     "       sum(discount_rate), "
	                     "       count(*) "
	                     "FROM jit_multi_key_perfect_hash_payload "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "backend_source_validity=may-have-null:6"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_group_key_type=uint8:2"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_payload_type=int64:1|decimal64:5"));
		    REQUIRE(StringUtil::Contains(
		        event.reason,
		        "backend_aggregate=perfect_hash_update:1|primitive_payload_update:1|grouped_state_address_lookup:1|"
		        "generated_perfect_hash_lookup:1"));
		    REQUIRE(StringUtil::Contains(event.ir, "group_expressions=native:string-compress"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "op0=projection("));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT join-selected perfect-hash aggregate payloads survive selection views", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_selected_perfect_hash_payload AS "
	                          "SELECT (1995 + (i % 2))::INTEGER AS bucket_year, "
	                          "       (i % 5)::INTEGER AS segment_id, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS volume "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_selected_dimension AS "
	                          "SELECT i::INTEGER AS segment_id, "
	                          "       CASE WHEN i = 0 THEN 'target' ELSE 'other' END AS segment_name "
	                          "FROM range(5) tbl(i)"));

	const string query = "SELECT bucket_year, "
	                     "       sum(CASE WHEN segment_name = 'target' "
	                     "                THEN volume ELSE 0.00::DECIMAL(15,2) END) AS target_sum, "
	                     "       sum(volume) AS total_sum "
	                     "FROM jit_join_selected_perfect_hash_payload "
	                     "JOIN jit_join_selected_dimension USING (segment_id) "
	                     "GROUP BY bucket_year "
	                     "ORDER BY bucket_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		    REQUIRE(StringUtil::Contains(event.ir, "case<"));
		    REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.direct_join_output_perfect_hash_payload_update="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.direct_perfect_hash_state_update="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventJitMaterializationBoundaryCounts(event), "hash_join_probe.final_output="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                       "aggregate_update.resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT generates single typed perfect-hash aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_typed_payload_l AS "
	                          "SELECT i::BIGINT AS k, (i % 100)::BIGINT AS g, (i % 7)::BIGINT AS v "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_typed_payload_r AS "
	                          "SELECT i::BIGINT AS k, (i % 11)::BIGINT AS x "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT g, sum(v + x) AS s "
	                     "FROM jit_single_typed_payload_l JOIN jit_single_typed_payload_r USING (k) "
	                     "GROUP BY g "
	                     "ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 100);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.fused_payload_update_owns_perfect_hash_group_lookup="));
		    REQUIRE(StringUtil::Contains(EventJitMaterializationBoundaryCounts(event),
		                                 "aggregate_update.direct_perfect_hash_state_update="));
		    REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                       "aggregate_update.native_sink_update="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                       "aggregate_update.resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT gates perfect-hash aggregate updates with DuckDB scan filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_perfect_hash_payload AS "
	                          "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS group_flag, "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS group_status, "
	                          "       DATE '1998-08-30' + CAST(i % 8 AS INTEGER) AS event_date, "
	                          "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS qty_value, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS gross_value, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount_rate, "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) AS tax_rate "
	                          "FROM range(120000) tbl(i)"));

	const string query = "SELECT group_flag, group_status, "
	                     "       sum(qty_value), "
	                     "       sum(gross_value), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate)), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate) * "
	                     "           (1.00::DECIMAL(15,2) + tax_rate)), "
	                     "       sum(discount_rate), "
	                     "       count(*) "
	                     "FROM jit_filtered_perfect_hash_payload "
	                     "WHERE event_date <= DATE '1998-09-02' "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    RequireDuckDBScanFilteredSourceContract(event);
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
		    REQUIRE(StringUtil::Contains(event.ir, "aggregate_update(kind=perfect-hash"));
	    });

}

TEST_CASE("JIT perfect hash aggregate generates primitive decimal sum and count star updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_count_star AS "
	                          "SELECT (i % 4)::UTINYINT AS g1, (i % 3)::UTINYINT AS g2, "
	                          "CAST(i % 100 AS DECIMAL(15,2)) AS v FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference =
	    con.Query("SELECT g1, g2, sum(v), count(*) FROM jit_perfect_hash_count_star GROUP BY g1, g2 ORDER BY g1, g2");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 12);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
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
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_contract<"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_layout_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_update_kind=count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_compiled_primitive_sink);

	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.resolve_grouped_state_addresses="));
		REQUIRE_FALSE(
		    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "resolve_grouped_state_addresses"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT hash aggregate cast-only keys use native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
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

	bool found_hash_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate || !event.candidate_contract.OwnsSink()) {
			continue;
		}
		if (event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		found_hash_state_address_update = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "backend_aggregate=hash_update:1|primitive_payload_update:1|"
		                                           "grouped_state_address_lookup:1|native_state_address_lookup:1"));
		REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
	}
	REQUIRE(found_hash_state_address_update);

	bool found_direct_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto runtime_paths = EventJitRuntimePathCounts(event);
		if (!StringUtil::Contains(runtime_paths, "aggregate_update.direct_append_new_grouped_primitive_update=")) {
			continue;
		}
		found_direct_runtime = true;
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		auto boundaries = EventJitMaterializationBoundaryCounts(event);
		REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_append_new_grouped_primitive_update"
		                                           ".find_new.state_address_update="));
		REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.direct_append_new_grouped_primitive_update"
		                                           ".direct_append_new.primitive_update="));
		REQUIRE(StringUtil::Contains(boundaries, "aggregate_update.direct_state_update="));
	}
	REQUIRE(found_direct_runtime);
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
		if (!IsSljitRegionEvent(event)) {
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
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
	}
}
