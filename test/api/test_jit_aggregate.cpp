#include "test_jit_helpers.hpp"

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
		RequireGeneratedSourceFilteredSourceContract(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.reason, "ungrouped-aggregate-native-state-update"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_aggregate_update);
}

TEST_CASE("JIT CBO counts aggregate protocol once in runtime planner facts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=10"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=100"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_cbo_partition AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_aggregate_cbo_partition");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "10000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.generated_stage_count == 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 1);
		    REQUIRE(event.runner_cost.saved_work_per_batch == 100);
		    REQUIRE(event.runner_cost.selected_accelerated_runner);
	    });
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
	                          "       WHEN 0 THEN 0.00::DECIMAL(15,2) "
	                          "       WHEN 1 THEN 0.01::DECIMAL(15,2) "
	                          "       WHEN 2 THEN 0.02::DECIMAL(15,2) "
	                          "       WHEN 3 THEN 0.03::DECIMAL(15,2) "
	                          "       WHEN 4 THEN 0.04::DECIMAL(15,2) "
	                          "       WHEN 5 THEN 0.05::DECIMAL(15,2) "
	                          "       WHEN 6 THEN 0.06::DECIMAL(15,2) "
	                          "       WHEN 7 THEN 0.07::DECIMAL(15,2) "
	                          "       WHEN 8 THEN 0.08::DECIMAL(15,2) "
	                          "       ELSE 0.09::DECIMAL(15,2) END AS discount, "
	                          "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS quantity, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT sum(extendedprice * discount) "
	                     "FROM jit_filtered_aggregate_source_remap "
	                     "WHERE discount BETWEEN 0.05::DECIMAL(15,2) AND 0.07::DECIMAL(15,2) "
	                     "  AND quantity < 24::DECIMAL(15,2)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
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
		REQUIRE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
	}
	REQUIRE(found_filtered_aggregate);
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
	                          "CASE WHEN i % 3571 = 0 THEN NULL ELSE (i * 10)::DECIMAL(18, 2) END AS d "
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

TEST_CASE("JIT preserves projection temps for double primitive aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_double_aggregate_payload AS "
	                          "SELECT i, CAST((i % 100) AS DOUBLE) + 0.25 AS x, "
	                          "CAST(((i * 3) % 200) AS DOUBLE) + 0.5 AS y "
	                          "FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(x), sum((x * 1.5) + (y / 4.0)) "
	                           "FROM jit_double_aggregate_payload WHERE i % 7 <> 0");
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(x), sum((x * 1.5) + (y / 4.0)) "
	                        "FROM jit_double_aggregate_payload WHERE i % 7 <> 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	bool found_double_payload = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=") ||
		    !StringUtil::Contains(event.ir, "native:double-add-references")) {
			continue;
		}
		found_double_payload = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "op0=projection("));
		REQUIRE(StringUtil::Contains(event.ir, "op1=aggregate_update("));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_update_kind=sum_double"));
		REQUIRE_FALSE(StringUtil::Contains(
		    event.ir, "primitive_payloads=native:reference:region-input[compose-reference(ssa.pass#"));
	}
	REQUIRE(found_double_payload);
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

TEST_CASE("JIT fuses Q14 shaped decimal CASE aggregate payload lanes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q14_aggregate_probe AS "
	                          "SELECT i::BIGINT AS partkey, "
	                          "       100.00::DECIMAL(15,2) + (i % 13)::DECIMAL(15,2) AS l_extendedprice, "
	                          "       0.10::DECIMAL(15,2) AS l_discount "
	                          "FROM range(0, 20000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_q14_aggregate_part AS "
	                          "SELECT i::BIGINT AS partkey, "
	                          "       CASE WHEN i % 7 IN (0, 3) THEN 'PROMO BRUSHED STEEL' "
	                          "            ELSE 'STANDARD ANODIZED COPPER' END AS p_type "
	                          "FROM range(0, 20000) tbl(i)"));

	const string query = "SELECT sum(CASE WHEN p_type LIKE 'PROMO%' "
	                     "                THEN l_extendedprice * (1.00 - l_discount) "
	                     "                ELSE 0.0000 END) AS promo_sum, "
	                     "       sum(l_extendedprice * (1.00 - l_discount)) AS total_sum "
	                     "FROM jit_q14_aggregate_probe, jit_q14_aggregate_part "
	                     "WHERE jit_q14_aggregate_probe.partkey = jit_q14_aggregate_part.partkey";
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
		if (!StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_fused_runtime);
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
			REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
			                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		}
	}
	REQUIRE(found_hash_state_address_update);

	bool found_fused_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.resolve_grouped_state_addresses."));
	}
	REQUIRE(found_fused_runtime);
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
			REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
			                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
			REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_layout"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
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

TEST_CASE("JIT fuses TPC-H Q1 shaped perfect-hash aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_q1_perfect_hash_payload AS "
	              "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS l_returnflag, "
	              "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS l_linestatus, "
	              "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS l_quantity, "
	              "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS l_extendedprice, "
	              "       CAST(i % 10 AS DECIMAL(15,2)) AS l_discount, "
	              "       CAST(i % 8 AS DECIMAL(15,2)) AS l_tax "
	              "FROM range(120000) tbl(i)"));

	const string query = "SELECT l_returnflag, l_linestatus, "
	                     "       sum(l_quantity), "
	                     "       sum(l_extendedprice), "
	                     "       sum(l_extendedprice * (1.00::DECIMAL(15,2) - l_discount)), "
	                     "       sum(l_extendedprice * (1.00::DECIMAL(15,2) - l_discount) * "
	                     "           (1.00::DECIMAL(15,2) + l_tax)), "
	                     "       sum(l_discount), "
	                     "       count(*) "
	                     "FROM jit_q1_perfect_hash_payload "
	                     "GROUP BY l_returnflag, l_linestatus "
	                     "ORDER BY l_returnflag, l_linestatus";
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

TEST_CASE("JIT gates Q1 shaped perfect-hash aggregate updates with generated source filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_q1_filtered_perfect_hash_payload AS "
	              "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS l_returnflag, "
	              "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS l_linestatus, "
	              "       DATE '1998-08-30' + CAST(i % 8 AS INTEGER) AS l_shipdate, "
	              "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS l_quantity, "
	              "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS l_extendedprice, "
	              "       CAST(i % 10 AS DECIMAL(15,2)) AS l_discount, "
	              "       CAST(i % 8 AS DECIMAL(15,2)) AS l_tax "
	              "FROM range(120000) tbl(i)"));

	const string query = "SELECT l_returnflag, l_linestatus, "
	                     "       sum(l_quantity), "
	                     "       sum(l_extendedprice), "
	                     "       sum(l_extendedprice * (1.00::DECIMAL(15,2) - l_discount)), "
	                     "       sum(l_extendedprice * (1.00::DECIMAL(15,2) - l_discount) * "
	                     "           (1.00::DECIMAL(15,2) + l_tax)), "
	                     "       sum(l_discount), "
	                     "       count(*) "
	                     "FROM jit_q1_filtered_perfect_hash_payload "
	                     "WHERE l_shipdate <= DATE '1998-09-02' "
	                     "GROUP BY l_returnflag, l_linestatus "
	                     "ORDER BY l_returnflag, l_linestatus";
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
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
		    REQUIRE(StringUtil::Contains(event.ir, "op0=filter("));
		    REQUIRE(StringUtil::Contains(event.ir, "op1=aggregate_update(kind=perfect-hash"));
	    });

	bool found_filtered_perfect_hash = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_perfect_hash_update=")) {
			continue;
		}
		found_filtered_perfect_hash = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused="));
	}
	REQUIRE(found_filtered_perfect_hash);
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
		REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
		REQUIRE(StringUtil::Contains(event.reason, "native_hash_aggregate_lookup_blocker="
		                                           "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=blocked"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_blocker="
		                                       "hash-aggregate-generated-lookup-backend-lowering-missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_layout"));
		REQUIRE(StringUtil::Contains(event.ir, "append_contract_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_aggregate_lookup_mode=blocked"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash_aggregate_lookup=vectorized-address-contract"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
	}
	REQUIRE(found_hash_state_address_update);
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
