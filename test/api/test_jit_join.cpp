#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT hash join build selected ingress compiles only inside generated fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	// Exercise backend-owned filtered projection ingress into the build protocol.
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=256"));
	string lhs_key = "i";
	string rhs_key = "j";
	for (idx_t term_idx = 1; term_idx <= 60; term_idx++) {
		auto term = std::to_string(term_idx);
		lhs_key = "(" + lhs_key + " + (i * " + term + "))";
		rhs_key = "(" + rhs_key + " + (j * " + term + "))";
	}
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_hash_build_l AS SELECT " + lhs_key + "::BIGINT AS i FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_r AS SELECT i::BIGINT AS j FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_l a "
	                        "JOIN (SELECT " +
	                        rhs_key + " AS x FROM jit_hash_build_r WHERE j > 10) b ON a.i=b.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 9989);

	bool found_build_compile = false;
	bool found_build_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event)) {
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-build-generated-body-missing"));
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_PROTOCOL_REASON)) {
				found_build_compile = true;
				REQUIRE(EventExecutionMode(event) == "native");
				REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
				REQUIRE(event.runner_cost.present);
				REQUIRE(event.runner_cost.generated_stage_count > 0);
				REQUIRE(event.runner_cost.native_join_stage_count == 0);
				REQUIRE(event.runner_cost.native_hash_join_build_sink_count == 0);
				REQUIRE(event.runner_cost.generated_backend_stage_count > 0);
				REQUIRE(event.runner_cost.materialization_elision_count == 0);
				REQUIRE(event.runner_cost.saved_work_per_batch > 0);
				REQUIRE(event.code_size > 0);
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_prepare"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_hash"));
				REQUIRE(StringUtil::Contains(event.reason, "requires=hash_join_build_append"));
			}
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_prepare")) {
			found_build_runtime = true;
			REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
			REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
			REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "materialization_elision="));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.bind_sink_contract"));
			REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.reference_keys"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.filter_pushdown"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.reference_payload"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_hash"));
			REQUIRE(
			    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "hash_join_build.hash_table_append"));
		}
	}
	REQUIRE(found_build_compile);
	REQUIRE(found_build_runtime);
}

TEST_CASE("JIT CBO does not charge generated hash-build sink protocol before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	ConfigureJitCoverageCbo(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_cbo_l AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_cbo_r AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_cbo_l l "
	                        "JOIN (SELECT k + 1 AS x FROM jit_hash_build_cbo_r WHERE k > 10) r ON l.k = r.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "9988");

	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "decision" || !event.runner_cost.present ||
		    event.runner_cost.input_scope != PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE ||
		    event.runner_cost.native_hash_join_build_sink_count != 1 || event.runner_cost.generated_stage_count == 0) {
			continue;
		}
		REQUIRE_FALSE((EventStatus(event) == "skipped" && !event.has_candidate &&
		               event.blocker == EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED));
		REQUIRE(event.runner_cost.stateful_protocol_penalty == 0);
	}
}

TEST_CASE("JIT composes DuckDB string-set scans with direct hash build", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_build_l AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_build_r AS "
	                          "SELECT i::BIGINT AS k, CASE WHEN i % 4 = 0 THEN 'EUROPE' ELSE 'OTHER' END AS region "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_string_build_l l "
	                        "JOIN (SELECT k FROM jit_string_build_r WHERE region IN ('EUROPE', 'ASIA')) r USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2500");

	bool found_build_compile = false;
	bool found_build_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
		    event.candidate_traits.source_filter_count > 0) {
			found_build_compile = true;
			REQUIRE(event.runner_cost.present);
			REQUIRE(event.runner_cost.native_join_stage_count == 0);
			REQUIRE(event.runner_cost.native_hash_join_build_sink_count == 0);
			REQUIRE(event.runner_cost.generated_backend_stage_count > 0);
			REQUIRE(StringUtil::Contains(event.reason, "duckdb-scan-filtered-source-contract"));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventJitRuntimeProofCounts(event),
		                         "hash_join_build.generated_backend_work.direct_source_ingress=")) {
			found_build_runtime = true;
			REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		}
	}
	REQUIRE(found_build_compile);
	REQUIRE(found_build_runtime);
}

TEST_CASE("JIT composes mixed storage filters with direct hash build", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_build_l AS "
	                          "SELECT i::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_build_r AS "
	                          "SELECT i::BIGINT AS k, (i % 20)::INTEGER AS bucket, "
	                          "CASE WHEN i % 4 = 0 THEN 'LARGE BRASS' ELSE 'OTHER' END AS kind "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_mixed_build_l l "
	                        "JOIN (SELECT k FROM jit_mixed_build_r "
	                        "      WHERE bucket = 0 AND suffix(kind, 'BRASS')) r USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "500");

	bool found_build_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_JOIN_BUILD ||
		    event.candidate_traits.source_filter_count != 2) {
			continue;
		}
		found_build_compile = true;
		REQUIRE(event.selected_uses_scan_filters);
		REQUIRE(event.runner_cost.generated_backend_stage_count > 0);
		REQUIRE(StringUtil::Contains(event.reason, "duckdb-scan-filtered-source-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "generated_source_filter="));
	}
	REQUIRE(found_build_compile);
}

TEST_CASE("JIT executes exact-filter probe to hash-build primitive sequences", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_build_fact AS "
	                          "SELECT (i % 50000)::INTEGER AS filter_key, i::BIGINT AS join_key "
	                          "FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_build_filter AS "
	                          "SELECT i::INTEGER AS filter_key FROM range(500) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_build_probe AS "
	                          "SELECT i::BIGINT AS join_key FROM range(200000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_exact_build_probe p JOIN ("
	                        "  SELECT f.join_key FROM jit_exact_build_fact f "
	                        "  JOIN jit_exact_build_filter d USING (filter_key)"
	                        ") selected USING (join_key)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2000");

	bool found_primitive_compile = false;
	bool found_primitive_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
		    event.candidate_traits.hash_join_operator_count == 1 && event.selected_uses_scan_filters &&
		    StringUtil::Contains(event.reason, "exact_filter_proof_count=1")) {
			found_primitive_compile = true;
			REQUIRE(event.code_size == 0);
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventJitRuntimePathCounts(event),
		                         "hash_join_probe.perfect_probe.exact_source_filter=") &&
		    StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_build.selected_required_sources=")) {
			found_primitive_runtime = true;
			REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
			REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		}
	}
	REQUIRE(found_primitive_compile);
	REQUIRE(found_primitive_runtime);
}

TEST_CASE("JIT composes exact regular-prefix probes with projected hash builds", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_exact_build_fact AS "
	                          "SELECT i::BIGINT AS filter_key, i::BIGINT AS join_key, "
	                          "       (i % 97)::BIGINT AS payload FROM range(2100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_exact_build_filter AS "
	                          "SELECT (i * 500)::BIGINT AS filter_key FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_exact_build_probe AS "
	                          "SELECT i::BIGINT AS join_key FROM range(2100000) tbl(i)"));

	const string query =
	    "SELECT sum(selected.payload + selected.rhs_key) FROM jit_regular_exact_build_probe probe JOIN ("
	    "  SELECT fact.join_key, fact.payload, filter.filter_key AS rhs_key "
	    "  FROM jit_regular_exact_build_fact fact "
	    "  JOIN jit_regular_exact_build_filter filter USING (filter_key)"
	    ") selected USING (join_key)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			    return false;
		    }
		    const auto paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(paths, "hash_join_probe.regular_probe.exact_source_filter=") &&
		           (StringUtil::Contains(paths, "hash_join_probe.projected_hash_build_views=") ||
		            StringUtil::Contains(paths, "hash_join_probe.projected_hash_build_outputs="));
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "native");
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT exact prefix membership elides only equivalent unique hash probes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_prefix_probe AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v FROM range(2100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_prefix_unique AS "
	                          "SELECT (i * 500)::BIGINT AS k, (i * 11)::BIGINT AS payload "
	                          "FROM range(4096) tbl(i)"));

	const string equivalent_key_query = "SELECT count(*), sum(r.k) FROM jit_exact_prefix_probe l "
	                                    "JOIN jit_exact_prefix_unique r ON l.k = r.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(equivalent_key_query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "4096");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(equivalent_key_query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "hash_join_probe.regular_probe.exact_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.regular_probe.all_valid"));
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
	    });

	ClearJitTrace(manager, true);
	auto payload_result = con.Query("SELECT count(*), sum(r.payload) FROM jit_exact_prefix_probe l "
	                                "JOIN jit_exact_prefix_unique r ON l.k = r.k");
	REQUIRE_NO_FAIL(*payload_result);
	REQUIRE(payload_result->GetValue(0, 0).ToString() == "4096");
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
			                                   "hash_join_probe.regular_probe.exact_source_filter="));
		}
	}

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_prefix_duplicates AS "
	                          "SELECT (i * 500)::BIGINT AS k FROM range(4096) tbl(i) "
	                          "UNION ALL SELECT (i * 500)::BIGINT AS k FROM range(4096) tbl(i)"));
	ClearJitTrace(manager, true);
	auto duplicate_result = con.Query("SELECT count(*) FROM jit_exact_prefix_probe l "
	                                  "JOIN jit_exact_prefix_duplicates r ON l.k = r.k");
	REQUIRE_NO_FAIL(*duplicate_result);
	REQUIRE(duplicate_result->GetValue(0, 0).ToString() == "8192");
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
			                                   "hash_join_probe.regular_probe.exact_source_filter="));
		}
	}
}

TEST_CASE("JIT carries exact prefix proof through selected output filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_selected_probe AS "
	                          "SELECT i::BIGINT AS k, (i % 97)::BIGINT AS v FROM range(2100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_exact_selected_unique AS "
	                          "SELECT (i * 500)::BIGINT AS k FROM range(4096) tbl(i)"));

	const string query = "SELECT count(*), sum(l.v + r.k) FROM jit_exact_selected_probe l "
	                     "JOIN jit_exact_selected_unique r ON l.k = r.k WHERE l.v + r.k > 2000000";
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
		    const auto paths = EventJitRuntimePathCounts(event);
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(paths, "hash_join_probe.regular_probe.exact_source_filter=") &&
		           StringUtil::Contains(stages, "filter.selected_hash_join_selection=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventExecutionMode(event) == "native");
		    REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
	    });

	const string all_match_query = "SELECT count(*), sum(l.v + r.k) FROM jit_exact_selected_probe l "
	                               "JOIN jit_exact_selected_unique r ON l.k = r.k WHERE l.v + r.k >= r.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto all_match_reference = con.Query(all_match_query);
	REQUIRE_NO_FAIL(*all_match_reference);
	REQUIRE(all_match_reference->GetValue(0, 0).ToString() == "4096");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto all_match_result = con.Query(all_match_query);
	REQUIRE_NO_FAIL(*all_match_result);
	REQUIRE(all_match_result->GetValue(0, 0).ToString() == all_match_reference->GetValue(0, 0).ToString());
	REQUIRE(all_match_result->GetValue(1, 0).ToString() == all_match_reference->GetValue(1, 0).ToString());
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		const auto paths = EventJitRuntimePathCounts(event);
		const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       StringUtil::Contains(paths, "hash_join_probe.regular_probe.exact_source_filter=") &&
		       StringUtil::Contains(stages, "filter.selected_hash_join_selection=");
	});
}

TEST_CASE("JIT preserves source layout through generated filter projection repair", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_repair_fact AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       (CASE WHEN i % 7 = 0 THEN 'special requests ' ELSE 'ordinary ' END) "
	                          "           || i::VARCHAR AS note, "
	                          "       (i * 3)::BIGINT AS payload FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_repair_dim AS "
	                          "SELECT (i * 10)::BIGINT AS k FROM range(20000) tbl(i)"));

	const string query = "SELECT sum(group_sum) FROM ("
	                     "  SELECT f.k % 128 AS bucket, sum(f.payload) AS group_sum "
	                     "  FROM jit_source_repair_fact f JOIN jit_source_repair_dim d USING (k) "
	                     "  WHERE f.note NOT LIKE '%special%requests%' GROUP BY bucket"
	                     ") grouped";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compiled_source_join = false;
	bool found_source_join_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate && event.selected_uses_scan_filters &&
		    StringUtil::Contains(event.reason, "source_contract_input_layout=true") &&
		    StringUtil::Contains(event.reason, "HASH_JOIN:native")) {
			found_compiled_source_join = true;
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "outside operator input"));
			REQUIRE(event.runner_cost.native_join_stage_count == 1);
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    StringUtil::Contains(EventJitRuntimePathCounts(event), "hash_join_probe.")) {
			found_source_join_runtime = true;
			REQUIRE(StringUtil::Contains(EventJitRuntimeProofCounts(event), "generated_backend_work="));
		}
	}
	REQUIRE(found_compiled_source_join);
	REQUIRE(found_source_join_runtime);
}

TEST_CASE("JIT hash join build sink consumes selected probe output without native tail delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_probe_build_a AS "
	                          "SELECT i::BIGINT AS k, ((i % 512) * 1000003)::BIGINT AS b, i::BIGINT AS v "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_probe_build_b AS "
	                          "SELECT (i * 1000003)::BIGINT AS b, (i * 3 * 1000003)::BIGINT AS x "
	                          "FROM range(512) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_probe_build_c AS "
	                          "SELECT (i * 1000003)::BIGINT AS x, i::BIGINT AS y "
	                          "FROM range(2048) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_probe_build_a a "
	                        "JOIN jit_probe_build_b b USING (b) "
	                        "JOIN jit_probe_build_c c ON b.x = c.x "
	                        "WHERE a.k > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 4085);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" ||
		        EventExecutionMode(event) != "native") {
			    return false;
		    }
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return StringUtil::Contains(stages, "hash_join_probe.") &&
		           StringUtil::Contains(stages, "hash_join_build.hash_table_append=");
	    },
	    [](const ExecutionRegionEvent &event) { REQUIRE(event.jit_runtime.runtime_delegation_counts.empty()); });
}

TEST_CASE("JIT primitive sequence composes projection filter projection before hash build", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	// Exercise the build protocol from generated expression work without over-crediting protocol stages.
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=256"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,expression_rewriter'"));
	string lhs_key = "i";
	string rhs_key = "p";
	for (idx_t term_idx = 1; term_idx <= 60; term_idx++) {
		auto term = std::to_string(term_idx);
		lhs_key = "(" + lhs_key + " + (i * " + term + "))";
		rhs_key = "(" + rhs_key + " + (p * " + term + "))";
	}
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preproject_filter_l AS SELECT " + lhs_key +
	                          "::BIGINT AS k FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preproject_filter_r AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS p FROM range(4096) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_preproject_filter_l l "
	                        "JOIN (SELECT x FROM (SELECT k, " +
	                        rhs_key +
	                        " AS x FROM jit_preproject_filter_r) t WHERE k > 10) b "
	                        "ON l.k = b.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 4085);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stage_counts, "op0:projection.") &&
		           StringUtil::Contains(stage_counts, "op1:filter.selection=") &&
		           StringUtil::Contains(stage_counts, "op3:hash_join_build.reference_keys=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "op3:hash_join_build.filter_pushdown="));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts, "op2:projection."));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT composes selected source filters before hash build without delegation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_build_l AS "
	                          "SELECT i::BIGINT AS k FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_build_r AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v, (i + 1)::BIGINT AS w "
	                          "FROM range(4096) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result =
	    con.Query("SELECT sum(r.v) FROM jit_selected_build_l l "
	              "JOIN (SELECT * FROM jit_selected_build_r WHERE k > 10 AND k < 4090 AND v < w) r USING (k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 8361950);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "filter.selected_input_zero_copy=") &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "hash_join_build.hash_table_append=");
	    },
	    [](const ExecutionRegionEvent &event) { REQUIRE(EventJitRuntimeDelegationCounts(event).empty()); });
}

TEST_CASE("JIT CBO does not skip low-value hash-build sink pipelines before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_bodyless_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 32)::BIGINT AS g, "
	                          "(i * 13 % 997)::BIGINT AS payload FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_bodyless_dim AS "
	                          "SELECT g::BIGINT AS g, (g * 101)::BIGINT AS payload FROM range(32) tbl(g)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(f.i + d.payload) FROM jit_hash_bodyless_fact f "
	                        "JOIN jit_hash_bodyless_dim d ON f.g = d.g WHERE f.i % 3 = 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "4935358");

	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "decision" || !event.runner_cost.present ||
		    event.runner_cost.input_scope != PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE ||
		    event.runner_cost.native_hash_join_build_sink_count != 1 || event.runner_cost.generated_stage_count == 0) {
			continue;
		}
		REQUIRE_FALSE((EventStatus(event) == "skipped" && !event.has_candidate &&
		               event.blocker == EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED));
		REQUIRE(event.runner_cost.stateful_protocol_penalty == 0);
	}
}

TEST_CASE("JIT CBO does not cost nested-loop join protocol as native join work", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, false, false, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET nested_loop_join_threshold=1000000"));
	REQUIRE_NO_FAIL(con.Query("SET merge_join_threshold=1000000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_cost_l AS "
	                          "SELECT i::BIGINT AS i FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_cost_r AS "
	                          "SELECT i::BIGINT AS j FROM range(64) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_nested_cost_l"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_nested_cost_r"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_nested_cost_l l JOIN jit_nested_cost_r r ON l.i < r.j");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "2016");

	bool found_nested_loop_physical_cbo = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" || !event.runner_cost.present ||
		    event.runner_cost.input_scope != PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE) {
			continue;
		}
		found_nested_loop_physical_cbo = true;
		REQUIRE(event.runner_cost.native_join_stage_count == 0);
	}
	REQUIRE(found_nested_loop_physical_cbo);
}

TEST_CASE("JIT auto costs hash join regions through planner cost selection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_join_l AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_join_r AS "
	                          "SELECT i::BIGINT AS k, (i + 1)::BIGINT AS w FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(l.v + r.w) FROM jit_auto_hash_join_l l "
	                        "JOIN jit_auto_hash_join_r r ON l.k=r.k WHERE l.k BETWEEN 10 AND 9990");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE_FALSE(result->GetValue(0, 0).IsNull());

	bool found_hash_join_decision = false;
	bool found_costed_hash_join_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		const auto hash_join_shape =
		    StringUtil::Contains(event.ir, "hash-join") || StringUtil::Contains(event.reason, "hash-join");
		if (!hash_join_shape) {
			continue;
		}
		found_hash_join_decision = true;
		if (EventPhase(event) == "decision" && (EventStatus(event) == "skipped" || EventStatus(event) == "compiled")) {
			REQUIRE(event.runner_cost.present);
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON)) {
				found_costed_hash_join_probe = true;
				REQUIRE(event.runner_cost.native_join_stage_count > 0);
				REQUIRE(event.runner_cost.generated_stage_count >= event.runner_cost.native_join_stage_count);
				REQUIRE(event.runner_cost.generated_backend_stage_count >= event.runner_cost.native_join_stage_count);
				REQUIRE(event.runner_cost.admission_class != "none");
				REQUIRE_FALSE(event.runner_cost.selection_reason == "rejected_native_operator_work_uncosted");
			}
		}
		if (EventPhase(event) == "decision" && EventStatus(event) != "compiled") {
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
		}
	}
	REQUIRE(found_hash_join_decision);
	REQUIRE(found_costed_hash_join_probe);
}

TEST_CASE("JIT hash join probe generates native probe with append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_l AS "
	                          "SELECT ((i % 32) * 1000003)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(32) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT l.k + 1 AS kk, l.v, r.w FROM jit_hash_probe_l l "
	                        "JOIN jit_hash_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 65536);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 0);
	REQUIRE(result->GetValue(2, 0).GetValue<int64_t>() == 0);

	bool found_probe = false;
	bool found_runtime = false;
	bool found_probe_stage = false;
	bool found_probe_path = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (EventStatus(event) == "compiled" && EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
			found_probe = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.reason, "backend_join_key_type=int64:1"));
			REQUIRE(StringUtil::Contains(event.reason, "backend_join=hash_probe:1"));
			REQUIRE(StringUtil::Contains(event.reason, "equality_key:1"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe("));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows > 0 &&
		    event.output_rows > 0) {
			found_runtime = true;
			REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
			REQUIRE(event.jit_runtime.lazy_codegen.codegen_time_us >= 0);
			REQUIRE(event.jit_runtime.lazy_codegen.machine_codegen_time_us >= 0);
			if (HasJitRuntimePathPrefix(event, "hash_join_probe.")) {
				found_probe_path = true;
			}
			if (HasGeneratedHashJoinProbeStage(event)) {
				found_probe_stage = true;
			}
		}
	}
	REQUIRE(found_probe);
	REQUIRE(found_runtime);
	REQUIRE(found_probe_stage);
	REQUIRE(found_probe_path);

	auto explain = con.Query("EXPLAIN (ANALYZE, FORMAT JSON) "
	                         "SELECT l.k + 1 AS kk, l.v, r.w FROM jit_hash_probe_l l "
	                         "JOIN jit_hash_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*explain);
	REQUIRE(explain->RowCount() == 1);
	auto analyzed_plan = explain->GetValue(1, 0).GetValue<string>();
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"events\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"hash_join_probe_layout\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_path_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_proof_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"jit_runtime_delegation_counts\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "hash_join_probe."));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"lazy_codegen_time_us\""));
	REQUIRE(StringUtil::Contains(analyzed_plan, "\"generated_stage_runtime_breakdown\""));
}

TEST_CASE("JIT hash join lazy code is published once across parallel pipeline executors", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, false, true, 10000, 12);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_parallel_lazy_probe_l AS "
	                          "SELECT CASE WHEN i % 1024 = 0 THEN NULL ELSE ((i % 32) * 1000003)::BIGINT END AS k, "
	                          "i::BIGINT AS v FROM range(4194304) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_parallel_lazy_probe_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(32) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto expected = con.Query("SELECT sum(l.v + r.w) FROM jit_parallel_lazy_probe_l l "
	                          "JOIN jit_parallel_lazy_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*expected);

	ClearJitTrace(manager);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query("SELECT sum(l.v + r.w) FROM jit_parallel_lazy_probe_l l "
	                        "JOIN jit_parallel_lazy_probe_r r ON l.k=r.k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == expected->GetValue(0, 0));

	idx_t runtime_event_count = 0;
	idx_t lazy_codegen_event_count = 0;
	idx_t lazy_codegen_code_size = 0;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			continue;
		}
		runtime_event_count++;
		if (event.jit_runtime.lazy_codegen.code_size > 0) {
			lazy_codegen_event_count++;
			lazy_codegen_code_size += event.jit_runtime.lazy_codegen.code_size;
		}
	}
	REQUIRE(runtime_event_count > 1);
	REQUIRE(lazy_codegen_event_count == 1);
	REQUIRE(lazy_codegen_code_size > 0);
}

TEST_CASE("JIT hash join selected output appends through a primitive recipe", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_append_l AS "
	                          "SELECT ((i % 128) * 1000003)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_append_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(64) tbl(i)"));

	const string query = "WITH matched AS MATERIALIZED ("
	                     "SELECT l.k, l.v, r.w FROM jit_selected_append_l l "
	                     "JOIN jit_selected_append_r r ON l.k = r.k"
	                     ") SELECT count(*), sum(k), sum(v), sum(w) FROM matched";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           HasJitRuntimePathPrefix(event, "append_sink.hash_join_selected_append_");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(stages, "hash_join_probe.selected_append_materialization="));
		    REQUIRE(StringUtil::Contains(stages, "append_sink.selected_append_sink="));
		    REQUIRE_FALSE(StringUtil::Contains(stages, "hash_join_probe.materialize_output="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT hash join probe preserves selected reference source view", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_full_pipeline_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_base_cost=0"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_startup_margin_basis_points=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_selected_probe_l AS "
	                          "SELECT ((i % 10000) * 1000003)::BIGINT AS k, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_selected_probe_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(10000) tbl(i)"));

	const string query = "SELECT l.k, sum(l.v + r.w), sum(l.v - r.w), sum(l.v * 2), sum(r.w * 3), "
	                     "sum(l.v + 42), sum(r.w + 17) FROM jit_hash_selected_probe_l l "
	                     "JOIN jit_hash_selected_probe_r r ON l.k = r.k "
	                     "WHERE l.d > DATE '1994-01-01' GROUP BY l.k ORDER BY l.k LIMIT 3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitRuntimePathPrefix(event, "hash_join_probe.");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		    REQUIRE(HasJitRuntimePathPrefix(event, "hash_join_probe."));
	    });
}

TEST_CASE("JIT hash join filter ungrouped aggregate avoids final join materialization", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_filtered_agg_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 37)::DOUBLE AS quantity, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_filtered_agg_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, "
	                          "       ((i % 23) + 5)::DOUBLE AS threshold "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT sum(l.price)::HUGEINT AS revenue "
	                     "FROM jit_direct_filtered_agg_l l "
	                     "JOIN jit_direct_filtered_agg_r r ON l.k = r.k "
	                     "WHERE l.quantity < r.threshold OR l.k = -1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 1);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(stage_counts, "hash_join_probe.residual_predicate="));
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT perfect hash join materializes selection-only output before post-join projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_post_join_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_post_join_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_post_join_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_post_join_dim"));

	const string query = "SELECT k_i, sum(price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM ("
	                     "    SELECT CAST(k * 1000003 AS INTEGER) AS k_i, price "
	                     "    FROM ("
	                     "        SELECT f.k, f.price "
	                     "        FROM jit_perfect_post_join_fact f "
	                     "        JOIN jit_perfect_post_join_dim d ON f.k=d.k"
	                     "    ) joined"
	                     ") projected "
	                     "GROUP BY k_i";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_post_join_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_post_join_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_post_join_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_post_join_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_post_join_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_post_join_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "hash_join_probe.materialize_selection_output=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts,
		                                 "hash_join_probe.materialize_selection_output.dictionary_build_payload="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT perfect hash join grouped aggregate direct-projects input-vector keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_direct_agg_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_direct_agg_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_direct_agg_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_direct_agg_dim"));

	const string query = "SELECT f.k, sum(f.price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM jit_perfect_direct_agg_fact f "
	                     "JOIN jit_perfect_direct_agg_dim d ON f.k=d.k "
	                     "GROUP BY f.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_direct_agg_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_direct_agg_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_direct_agg_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_direct_agg_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_direct_agg_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_direct_agg_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           HasJitAggregateUpdatePath(event) && HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "perfect_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT perfect hash join grouped aggregate composes cast-chain input-vector keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=10"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_cast_direct_agg_fact AS "
	                          "SELECT (i % 1024)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_cast_direct_agg_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(1024) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_cast_direct_agg_fact"));
	REQUIRE_NO_FAIL(con.Query("ANALYZE jit_perfect_cast_direct_agg_dim"));

	const string query = "SELECT CAST(f.k AS INTEGER) AS k_i, sum(f.price)::HUGEINT AS total, count(*) AS cnt "
	                     "FROM jit_perfect_cast_direct_agg_fact f "
	                     "JOIN jit_perfect_cast_direct_agg_dim d ON f.k=d.k "
	                     "GROUP BY CAST(f.k AS INTEGER)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_cast_direct_agg_reference AS " + query));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_perfect_cast_direct_agg_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_perfect_cast_direct_agg_output "
	                      "   EXCEPT SELECT * FROM jit_perfect_cast_direct_agg_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_perfect_cast_direct_agg_reference "
	                      "   EXCEPT SELECT * FROM jit_perfect_cast_direct_agg_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           event.jit_runtime.hash_join_probe_layout == "perfect_hash_table" &&
		           HasJitAggregateUpdatePath(event) && HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "perfect_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT hash join grouped aggregate proves probe-key casts from matched build keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_matched_key_proof_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_matched_key_proof_r AS "
	                          "SELECT (i * 100003)::INTEGER AS join_k, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS shippriority "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT CAST(l.k AS INTEGER) AS orderkey, r.orderdate, r.shippriority::TINYINT AS shippriority, "
	    "       sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	    "FROM jit_matched_key_proof_l l "
	    "JOIN jit_matched_key_proof_r r ON l.k = r.join_k "
	    "WHERE l.shipdate > DATE '1994-01-01' "
	    "GROUP BY 1, 2, 3 "
	    "ORDER BY orderkey, r.orderdate, shippriority "
	    "LIMIT 64";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_matched_key_proof_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_matched_key_proof_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_matched_key_proof_output "
	                      "   EXCEPT SELECT * FROM jit_matched_key_proof_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_matched_key_proof_reference "
	                      "   EXCEPT SELECT * FROM jit_matched_key_proof_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT row-pointer aggregate accepts identity-cast build-side group keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='expression_rewriter,join_order,build_side_probe_side,"
	                          "partial_aggregate_pushdown,compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_identity_group_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       (i % 13)::BIGINT AS v "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_identity_group_r AS "
	                          "SELECT (i * 100003)::INTEGER AS k, "
	                          "       (i % 257)::INTEGER AS g, "
	                          "       'bucket_' || CAST(i % 17 AS VARCHAR) AS label "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT CAST(r.g AS INTEGER) AS g, r.label, sum(l.v) AS total "
	                     "FROM jit_identity_group_l l "
	                     "JOIN jit_identity_group_r r ON l.k = r.k "
	                     "GROUP BY 1, 2 "
	                     "ORDER BY g, r.label "
	                     "LIMIT 64";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_identity_group_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_identity_group_output AS " + query));

	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_identity_group_output "
	                      "   EXCEPT SELECT * FROM jit_identity_group_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_identity_group_reference "
	                      "   EXCEPT SELECT * FROM jit_identity_group_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects probe-side grouped keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_post_join_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 31)::INTEGER AS g, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_direct_post_join_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k FROM range(8192) tbl(i)"));

	const string query = "SELECT l.g, sum((l.p - l.d) * 2.00::DECIMAL(15,2))::HUGEINT AS revenue "
	                     "FROM jit_direct_post_join_l l "
	                     "JOIN jit_direct_post_join_r r ON l.k = r.k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY l.g ORDER BY l.g";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects split probe-side keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_split_payload_l AS "
	                          "SELECT ((i % 8192) * 1000003)::BIGINT AS k, "
	                          "       (i % 97)::BIGINT AS g, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_split_payload_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k FROM range(8192) tbl(i)"));

	const string query = "SELECT l.g::SMALLINT AS g, sum(l.p)::HUGEINT AS revenue "
	                     "FROM jit_split_payload_l l "
	                     "JOIN jit_split_payload_r r ON l.k = r.k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY g ORDER BY g";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects RHS keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_group_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_group_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       (i * 100003)::BIGINT AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS shippriority "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT r.orderkey::INTEGER AS orderkey, r.orderdate, r.shippriority::TINYINT AS shippriority, "
	    "       sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	    "FROM jit_row_pointer_group_l l "
	    "JOIN jit_row_pointer_group_r r ON l.k = r.join_k "
	    "WHERE l.shipdate > DATE '1994-01-01' "
	    "GROUP BY orderkey, r.orderdate, shippriority "
	    "ORDER BY orderkey, r.orderdate, shippriority "
	    "LIMIT 64";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate separates leading-key hash collisions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_leading_key_hash_l AS "
	                          "SELECT ((i % 512) * 100003)::BIGINT AS k, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS amount "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_leading_key_hash_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       42::BIGINT AS g0, "
	                          "       (i % 4)::BIGINT AS g1, "
	                          "       ((i // 4) % 4)::BIGINT AS g2, "
	                          "       ((i // 16) % 4)::BIGINT AS g3 "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT r.g0, r.g1, r.g2, r.g3, sum(l.amount)::HUGEINT AS total_amount "
	                     "FROM jit_leading_key_hash_l l "
	                     "JOIN jit_leading_key_hash_r r ON l.k = r.join_k "
	                     "GROUP BY r.g0, r.g1, r.g2, r.g3 "
	                     "ORDER BY r.g0, r.g1, r.g2, r.g3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate preaggregates consecutive descriptor runs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_preagg_l AS "
	                          "SELECT (((i // 4) % 2048) * 100003)::BIGINT AS k, "
	                          "       CAST(1 + (i % 29) AS DECIMAL(15,2)) AS amount "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_preagg_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       i::BIGINT AS custkey, "
	                          "       ('cust-' || CAST(i AS VARCHAR))::VARCHAR AS cname "
	                          "FROM range(2048) tbl(i)"));

	const string query = "SELECT r.custkey, r.cname, sum(l.amount)::HUGEINT AS total_amount "
	                     "FROM jit_row_pointer_preagg_l l "
	                     "JOIN jit_row_pointer_preagg_r r ON l.k = r.join_k "
	                     "GROUP BY r.custkey, r.cname "
	                     "ORDER BY r.custkey";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate preaggregates mixed input-vector descriptor runs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_row_pointer_preagg_l AS "
	                          "SELECT (((i // 4) % 2048) * 100003)::BIGINT AS k, "
	                          "       (i % 17)::BIGINT AS amount "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_row_pointer_preagg_r AS "
	                          "SELECT (i * 100003)::INTEGER AS join_k, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate, "
	                          "       (i % 32)::INTEGER AS priority "
	                          "FROM range(2048) tbl(i)"));

	const string query = "SELECT CAST(l.k AS INTEGER) AS orderkey, r.orderdate, r.priority::TINYINT AS priority, "
	                     "       sum(l.amount) AS total_amount "
	                     "FROM jit_mixed_row_pointer_preagg_l l "
	                     "JOIN jit_mixed_row_pointer_preagg_r r ON l.k = r.join_k "
	                     "GROUP BY orderkey, r.orderdate, priority "
	                     "ORDER BY orderkey, r.orderdate, priority";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 1);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
	    });
}

TEST_CASE("JIT row-pointer grouped aggregate updates complementary string-set sums directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side,partial_aggregate_pushdown,"
	                          "compressed_materialization'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_set_preagg_orders AS "
	                          "SELECT (((i // 8) % 2048) * 100003)::BIGINT AS orderkey, "
	                          "       CASE WHEN i % 5 = 0 THEN '1-URGENT' "
	                          "            WHEN i % 5 = 1 THEN '2-HIGH' "
	                          "            ELSE '3-MEDIUM' END AS orderpriority "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_set_preagg_lineitem AS "
	                          "SELECT (i * 100003)::BIGINT AS line_orderkey, "
	                          "       (i % 2)::INTEGER AS shipmode_id "
	                          "FROM range(2048) tbl(i)"));

	const string query =
	    "SELECT l.shipmode_id, "
	    "       sum(CASE WHEN o.orderpriority = '1-URGENT' OR o.orderpriority = '2-HIGH' THEN 1 ELSE 0 END) "
	    "           AS high_priority_count, "
	    "       sum(CASE WHEN o.orderpriority <> '1-URGENT' AND o.orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) "
	    "           AS low_priority_count "
	    "FROM jit_string_set_preagg_orders o "
	    "JOIN jit_string_set_preagg_lineitem l ON o.orderkey = l.line_orderkey "
	    "GROUP BY l.shipmode_id "
	    "ORDER BY l.shipmode_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
	    });
}

TEST_CASE("JIT join grouped aggregate direct-projects variable-width RHS keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_varwidth_row_pointer_group_l AS "
	                          "SELECT ((i % 8192) * 100019)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST((i % 9)::DOUBLE / 100 AS DECIMAL(15,2)) AS d, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS shipdate "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_varwidth_row_pointer_group_r AS "
	                          "SELECT (i * 100019)::BIGINT AS join_k, "
	                          "       'customer-' || (i % 97)::VARCHAR AS customer_name "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT r.customer_name, sum(l.p * (1.00::DECIMAL(15,2) - l.d))::HUGEINT AS revenue "
	                     "FROM jit_varwidth_row_pointer_group_l l "
	                     "JOIN jit_varwidth_row_pointer_group_r r ON l.k = r.join_k "
	                     "WHERE l.shipdate > DATE '1994-01-01' "
	                     "GROUP BY r.customer_name "
	                     "ORDER BY r.customer_name";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
	    });
}

static void RequireComposedJoinProjectionAggregateEvent(ExecutionRegionManager &manager) {
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
	    });
}

static void RequireSelectedHashJoinViewMaterializationHasNoElisionCredit(ExecutionRegionManager &manager) {
	bool found = false;
	for (auto &event : manager.GetEvents()) {
		if (!event.has_candidate || event.candidate_traits.selected_hash_join_view_materialization_count == 0) {
			continue;
		}
		found = true;
		REQUIRE(event.runner_cost.materialization_elision_count == 0);
	}
	REQUIRE(found);
}

TEST_CASE("JIT mapped join casts feed dense grouped counts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mapped_dense_customer AS "
	                          "SELECT i::BIGINT AS customer_id FROM range(1, 150001) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mapped_dense_orders AS "
	                          "SELECT ((i * 7919) % 100000 + 1)::BIGINT AS customer_id, i::BIGINT AS order_id "
	                          "FROM range(1500000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_mapped_dense_customer"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_mapped_dense_orders"));

	const string query = "SELECT c.customer_id, count(o.order_id) AS order_count "
	                     "FROM jit_mapped_dense_customer c "
	                     "LEFT JOIN jit_mapped_dense_orders o ON c.customer_id = o.customer_id "
	                     "GROUP BY c.customer_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mapped_dense_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mapped_dense_output AS " + query));
	auto shape = con.Query("SELECT count(*), min(order_count), max(order_count), sum(order_count) "
	                       "FROM jit_mapped_dense_output");
	REQUIRE_NO_FAIL(*shape);
	REQUIRE(shape->GetValue(0, 0).ToString() == "150000");
	REQUIRE(shape->GetValue(1, 0).ToString() == "0");
	REQUIRE(shape->GetValue(2, 0).ToString() == "15");
	REQUIRE(shape->GetValue(3, 0).ToString() == "1500000");
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_mapped_dense_output EXCEPT ALL "
	                            "   SELECT * FROM jit_mapped_dense_reference) UNION ALL "
	                            "  (SELECT * FROM jit_mapped_dense_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_mapped_dense_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(
		               runtime_paths,
		               "aggregate_update.projection_aggregate.group.input_vector_cast_key0_unchecked=1500000") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_dense_single_lane_grouped_update=1500000") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_dense_single_lane_grouped_update_flush=1500000") &&
		           !StringUtil::Contains(runtime_paths, "producer_group_output_map");
	    },
	    [](const ExecutionRegionEvent &event) { RequireHashProbeAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT two-join grouped aggregate composes mixed VARCHAR projection chain", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       (i % 8192)::BIGINT AS orderkey, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'segment_' || CAST(i % 7 AS VARCHAR) AS segment_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_orders AS "
	                          "SELECT i::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT x.segment, year(o.orderdate) AS o_year, sum(x.amount)::HUGEINT AS sum_profit "
	                     "FROM ("
	                     "  SELECT b.segment_name AS segment, "
	                     "         CAST(l.orderkey AS INTEGER) AS orderkey_i, "
	                     "         l.extendedprice * (1.00::DECIMAL(15,2) - l.discount) - "
	                     "         b.supplycost * l.quantity AS amount "
	                     "  FROM jit_two_join_l l "
	                     "  JOIN jit_two_join_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	                     ") x "
	                     "JOIN jit_two_join_orders o ON x.orderkey_i = o.orderkey "
	                     "GROUP BY x.segment, o_year "
	                     "ORDER BY x.segment, o_year";
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

	RequireComposedJoinProjectionAggregateEvent(manager);
	RequireSelectedHashJoinViewMaterializationHasNoElisionCredit(manager);
}

TEST_CASE("JIT row-pointer grouped aggregate probes compressed string keys as single fields", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_string_l AS "
	                          "SELECT ((i % 8192) * 100003)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS d "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_row_pointer_string_r AS "
	                          "SELECT (i * 100003)::BIGINT AS join_k, "
	                          "       CASE WHEN i % 2 = 0 THEN 'MAIL' ELSE 'SHIP' END AS shipmode "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT r.shipmode, sum(l.p - l.d)::HUGEINT AS revenue "
	                     "FROM jit_row_pointer_string_l l "
	                     "JOIN jit_row_pointer_string_r r ON l.k = r.join_k "
	                     "GROUP BY r.shipmode ORDER BY r.shipmode";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) { RequireHashProbeAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT two-join grouped aggregate uses selected projection aggregate backend", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_l AS "
	                          "SELECT (i % 4096)::BIGINT AS suppkey, "
	                          "       (i % 2048)::BIGINT AS partkey, "
	                          "       ((i % 8192) * 100003)::BIGINT AS orderkey, "
	                          "       (i % 2)::BIGINT AS g0, "
	                          "       (i % 3)::BIGINT AS g1, "
	                          "       (i % 5)::BIGINT AS g2, "
	                          "       (i % 7)::BIGINT AS g3, "
	                          "       (i % 11)::BIGINT AS g4, "
	                          "       (i % 13)::BIGINT AS g5, "
	                          "       (i % 17)::BIGINT AS g6, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS extendedprice, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS quantity "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_psn AS "
	                          "SELECT i::BIGINT AS ps_suppkey, "
	                          "       (i % 2048)::BIGINT AS ps_partkey, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS supplycost, "
	                          "       'segment_' || CAST(i % 7 AS VARCHAR) AS segment_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_group_orders AS "
	                          "SELECT (i * 100003)::INTEGER AS orderkey, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS orderdate "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT x.segment, year(o.orderdate) AS o_year, "
	                     "       x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6, "
	                     "       sum(x.extendedprice * (1.00::DECIMAL(15,2) - x.discount) - "
	                     "           x.supplycost * x.quantity)::HUGEINT AS total_payload "
	                     "FROM ("
	                     "  SELECT b.segment_name AS segment, "
	                     "         CAST(l.orderkey AS INTEGER) AS orderkey_i, "
	                     "         l.g0, l.g1, l.g2, l.g3, l.g4, l.g5, l.g6, "
	                     "         l.extendedprice AS extendedprice, "
	                     "         l.discount AS discount, "
	                     "         b.supplycost AS supplycost, "
	                     "         l.quantity AS quantity "
	                     "  FROM jit_wide_group_l l "
	                     "  JOIN jit_wide_group_psn b ON l.suppkey = b.ps_suppkey AND l.partkey = b.ps_partkey "
	                     ") x "
	                     "JOIN jit_wide_group_orders o ON x.orderkey_i = o.orderkey "
	                     "GROUP BY x.segment, o_year, x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6 "
	                     "ORDER BY x.segment, o_year, x.g0, x.g1, x.g2, x.g3, x.g4, x.g5, x.g6";
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

	RequireComposedJoinProjectionAggregateEvent(manager);
}

TEST_CASE("JIT two-join aggregate composes second-join projection chain", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS left_key, "
	                          "       (i % 2048)::BIGINT AS right_key, "
	                          "       ((i % 8192) * 100003)::BIGINT AS event_key, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS gross_value, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount_rate, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS qty_value "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_build AS "
	                          "SELECT i::BIGINT AS build_left_key, "
	                          "       (i % 2048)::BIGINT AS build_right_key, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS unit_cost, "
	                          "       'segment_' || CAST(i % 7 AS VARCHAR) AS segment_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_join_lookup AS "
	                          "SELECT (i * 100003)::INTEGER AS event_key, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS event_date "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT x.segment, year(o.event_date) AS event_year, "
	                     "       sum(x.gross_value * (1.00::DECIMAL(15,2) - x.discount_rate) - "
	                     "           x.unit_cost * x.qty_value)::HUGEINT AS sum_profit "
	                     "FROM ("
	                     "  SELECT b.segment_name AS segment, "
	                     "         CAST(l.event_key AS INTEGER) AS event_key_i, "
	                     "         l.gross_value AS gross_value, "
	                     "         l.discount_rate AS discount_rate, "
	                     "         b.unit_cost AS unit_cost, "
	                     "         l.qty_value AS qty_value "
	                     "  FROM jit_two_join_fact l "
	                     "  JOIN jit_two_join_build b ON l.left_key = b.build_left_key AND "
	                     "l.right_key = b.build_right_key "
	                     ") x "
	                     "JOIN jit_two_join_lookup o ON x.event_key_i = o.event_key "
	                     "GROUP BY x.segment, event_year "
	                     "ORDER BY x.segment, event_year";
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

	RequireComposedJoinProjectionAggregateEvent(manager);
}

TEST_CASE("JIT multi-join aggregate composes generic join-prefix projection chain", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_join_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS left_key, "
	                          "       (i % 2048)::BIGINT AS right_key, "
	                          "       ((i % 8192) * 100003)::BIGINT AS event_key, "
	                          "       (i % 32)::INTEGER AS region_id, "
	                          "       (100 + (i % 1000))::BIGINT AS payload_value "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_join_build AS "
	                          "SELECT i::BIGINT AS build_left_key, "
	                          "       (i % 2048)::BIGINT AS build_right_key "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_join_lookup AS "
	                          "SELECT (i * 100003)::INTEGER AS event_key "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_join_region AS "
	                          "SELECT i::INTEGER AS region_id, "
	                          "       (i * 10)::INTEGER AS region_code "
	                          "FROM range(32) tbl(i)"));

	const string query = "SELECT r.region_code, "
	                     "       sum(x.payload_value)::HUGEINT AS sum_payload "
	                     "FROM ("
	                     "  SELECT CAST(l.event_key AS INTEGER) AS event_key_i, "
	                     "         l.region_id AS region_id, "
	                     "         l.payload_value AS payload_value "
	                     "  FROM jit_multi_join_fact l "
	                     "  JOIN jit_multi_join_build b ON l.left_key = b.build_left_key AND "
	                     "l.right_key = b.build_right_key "
	                     ") x "
	                     "JOIN jit_multi_join_lookup o ON x.event_key_i = o.event_key "
	                     "JOIN jit_multi_join_region r ON x.region_id = r.region_id "
	                     "GROUP BY r.region_code "
	                     "ORDER BY r.region_code";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event) &&
		           HasHashJoinProbeRuntimePath(event) &&
		           GeneratedOperatorStageEntryCount(event, "hash_join_probe") >= 3;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireHashProbeAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(GeneratedOperatorStageExecutionCount(event, "hash_join_probe") > 0);
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
	    });
}

TEST_CASE("JIT single mark filter aggregate uses projected source grouped update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_mark_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS item_key, "
	                          "       (i % 19)::INTEGER AS group_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_mark_blocked AS "
	                          "SELECT ((i * 5) % 4096)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_single_mark_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_single_mark_blocked b"
	                     ") "
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stages, "mark_nonmatch") &&
		           HasJitFilterRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "mark_nonmatch"));
		    REQUIRE(HasJitFilterRuntimePath(event));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT mark match filter emits LHS selected boundary", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_match_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i % 7)::INTEGER AS group_id "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_match_allowed AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_mark_match_fact f "
	                     "WHERE EXISTS ("
	                     "  SELECT 1 FROM jit_mark_match_allowed a "
	                     "  WHERE a.item_key = f.item_key"
	                     ") "
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stages, "mark_match") &&
		           HasJitFilterRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "mark_match"));
		    REQUIRE(HasJitFilterRuntimePath(event));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT mark match count-star preaggregates compressed string groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_count_star_string_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       CASE i % 5 "
	                          "           WHEN 0 THEN '1-URGENT' "
	                          "           WHEN 1 THEN '2-HIGH' "
	                          "           WHEN 2 THEN '3-MEDIUM' "
	                          "           WHEN 3 THEN '4-NOT SPECIFIED' "
	                          "           ELSE '5-LOW' "
	                          "       END AS group_name "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_count_star_string_allowed AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(4096) tbl(i)"));

	const string query = "SELECT group_name, count(*) AS kept_count "
	                     "FROM jit_mark_count_star_string_fact f "
	                     "WHERE EXISTS ("
	                     "  SELECT 1 FROM jit_mark_count_star_string_allowed a "
	                     "  WHERE a.item_key = f.item_key"
	                     ") "
	                     "GROUP BY group_name "
	                     "ORDER BY group_name";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 5);

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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stages, "mark_match") &&
		           HasJitFilterRuntimePath(event) && HasJitAggregateUpdatePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                 "primitive_projected_preaggregate_count_star_groups"));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.primitive_projected_preaggregated_count_star_update"));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.primitive_pending_preaggregated_count_star_flush"));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
	RequireAllSljitMaterializationElisionRuntimeProof(manager);
}

TEST_CASE("JIT mark filter aggregate uses grouped primitive selected view", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_aggregate_tail_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       ('G' || (i % 19)::VARCHAR) AS group_name "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_aggregate_tail_blocked AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_name, sum(item_key) AS kept_sum "
	                     "FROM jit_mark_aggregate_tail_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_mark_aggregate_tail_blocked b"
	                     ") "
	                     "GROUP BY group_name "
	                     "ORDER BY group_name";
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

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stages, "mark_nonmatch") &&
		           HasJitFilterRuntimePath(event) && HasJitAggregateUpdatePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "mark_nonmatch"));
		    REQUIRE(HasJitFilterRuntimePath(event));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
	RequireAllSljitMaterializationElisionRuntimeProof(manager);
}

TEST_CASE("JIT mark filter projection preserves selected nonmatches", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_selected_projection_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i % 23)::INTEGER AS group_id "
	                          "FROM range(32768) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_selected_projection_blocked AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(512) tbl(i)"));

	const string query = "SELECT group_id, item_key + 100 AS projected_key "
	                     "FROM jit_mark_selected_projection_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_mark_selected_projection_blocked b"
	                     ")";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_selected_projection_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_selected_projection_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageCountBreakdown(event);
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && StringUtil::Contains(stages, "mark_nonmatch") &&
		           HasJitFilterRuntimePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "mark_nonmatch"));
		    REQUIRE(HasJitFilterRuntimePath(event));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_mark_selected_projection_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_selected_projection_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_mark_selected_projection_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_selected_projection_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT mark filter projection delimiter sink uses selected join input", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_supplier AS "
	                          "SELECT i::BIGINT AS suppkey, ('S' || i::VARCHAR) AS name, "
	                          "       (i % 8)::BIGINT AS nationkey "
	                          "FROM range(256) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_nation AS "
	                          "SELECT i::BIGINT AS nationkey, "
	                          "       CASE WHEN i=1 THEN 'SAUDI ARABIA' ELSE ('N' || i::VARCHAR) END AS name "
	                          "FROM range(8) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_orders AS "
	                          "SELECT i::BIGINT AS orderkey, "
	                          "       CASE WHEN i % 3 = 0 THEN 'F' ELSE 'O' END AS status "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_lineitem AS "
	                          "SELECT (i % 8192)::BIGINT AS orderkey, "
	                          "       ((i / 8192)::BIGINT % 256) AS suppkey, "
	                          "       DATE '1994-01-01' + ((i * 7) % 1000)::INTEGER AS receiptdate, "
	                          "       DATE '1994-01-01' + ((i * 5) % 1000)::INTEGER AS commitdate "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT s.name, count(*) AS numwait "
	                     "FROM jit_delim_supplier s, jit_delim_lineitem l1, jit_delim_orders o, jit_delim_nation n "
	                     "WHERE s.suppkey = l1.suppkey "
	                     "  AND o.orderkey = l1.orderkey "
	                     "  AND o.status = 'F' "
	                     "  AND l1.receiptdate > l1.commitdate "
	                     "  AND s.nationkey = n.nationkey "
	                     "  AND n.name = 'SAUDI ARABIA' "
	                     "  AND EXISTS ("
	                     "      SELECT * FROM jit_delim_lineitem l2 "
	                     "      WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey"
	                     "  ) "
	                     "  AND NOT EXISTS ("
	                     "      SELECT * FROM jit_delim_lineitem l3 "
	                     "      WHERE l3.orderkey = l1.orderkey AND l3.suppkey <> l1.suppkey "
	                     "        AND l3.receiptdate > l3.commitdate"
	                     "  ) "
	                     "GROUP BY s.name "
	                     "ORDER BY numwait DESC, s.name "
	                     "LIMIT 100";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_delim_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_delim_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitDelimJoinSinkRuntimePath(event) &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "selected_sink_batch_append");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(HasJitDelimJoinSinkRuntimePath(event));
		    REQUIRE(HasGeneratedDelimJoinSinkStage(event));
		    REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "selected_sink_batch_append"));
	    });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_delim_reference "
	                      "     EXCEPT ALL SELECT * FROM jit_delim_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_delim_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_delim_reference)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT mark filter reference aggregate slices selected projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_reference_fact AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       (i * 1000000000000)::BIGINT AS group_id "
	                          "FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_reference_blocked AS "
	                          "SELECT (i * 17)::BIGINT AS item_key "
	                          "FROM range(128) tbl(i)"));

	const string query = "SELECT group_id, count(*) AS kept_count "
	                     "FROM jit_mark_reference_fact f "
	                     "WHERE f.item_key NOT IN ("
	                     "  SELECT b.item_key FROM jit_mark_reference_blocked b"
	                     ") "
	                     "GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_reference_expected AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_mark_reference_actual AS " + query));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasJitAggregateUpdatePath(event);
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "    (SELECT * FROM jit_mark_reference_expected "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_reference_actual) "
	                      "    UNION ALL "
	                      "    (SELECT * FROM jit_mark_reference_actual "
	                      "     EXCEPT ALL SELECT * FROM jit_mark_reference_expected)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("JIT two-projection between-join chain composes primitive boundaries", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_projection_chain_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS left_key, "
	                          "       (i % 2048)::BIGINT AS right_key, "
	                          "       ((i % 8192) * 100003)::BIGINT AS event_key, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS gross_value, "
	                          "       CAST(i % 17 AS DECIMAL(15,2)) AS discount_rate, "
	                          "       CAST(1 + (i % 23) AS DECIMAL(15,2)) AS qty_value "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_projection_chain_build AS "
	                          "SELECT i::BIGINT AS build_left_key, "
	                          "       (i % 2048)::BIGINT AS build_right_key, "
	                          "       CAST(3 + (i % 31) AS DECIMAL(15,2)) AS unit_cost, "
	                          "       'segment_' || CAST(i % 7 AS VARCHAR) AS segment_name "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_two_projection_chain_lookup AS "
	                          "SELECT (i * 100003)::INTEGER AS event_key, "
	                          "       DATE '1992-01-01' + (i % 2000)::INTEGER AS event_date "
	                          "FROM range(8192) tbl(i)"));

	const string query =
	    "SELECT x.segment_alias, year(o.event_date) AS event_year, sum(x.amount)::HUGEINT AS sum_profit "
	    "FROM ("
	    "  SELECT y.segment AS segment_alias, "
	    "         CAST(y.event_key_raw AS INTEGER) AS event_key_i, "
	    "         y.gross_value * (1.00::DECIMAL(15,2) - y.discount_rate) - "
	    "         y.unit_cost * y.qty_value AS amount "
	    "  FROM ("
	    "    SELECT b.segment_name AS segment, "
	    "           l.event_key AS event_key_raw, "
	    "           l.gross_value AS gross_value, "
	    "           l.discount_rate AS discount_rate, "
	    "           b.unit_cost AS unit_cost, "
	    "           l.qty_value AS qty_value "
	    "    FROM jit_two_projection_chain_fact l "
	    "    JOIN jit_two_projection_chain_build b ON l.left_key = b.build_left_key AND "
	    "l.right_key = b.build_right_key "
	    "  ) y "
	    ") x "
	    "JOIN jit_two_projection_chain_lookup o ON x.event_key_i = o.event_key "
	    "GROUP BY x.segment_alias, event_year "
	    "ORDER BY x.segment_alias, event_year";
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

	RequireComposedJoinProjectionAggregateEvent(manager);
}

TEST_CASE("JIT hash join pair probe preserves bloom-filtered sparse misses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_bloom_l AS "
	                          "SELECT i::BIGINT AS k0, (i * 17)::BIGINT AS k1, i::BIGINT AS v "
	                          "FROM range(300000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_pair_bloom_r AS "
	                          "SELECT (i * 73)::BIGINT AS k0, (i * 73 * 17)::BIGINT AS k1, (i * 3)::BIGINT AS w "
	                          "FROM range(4096) tbl(i)"));

	const string query = "SELECT count(*), sum(l.v + r.w) FROM jit_hash_pair_bloom_l l "
	                     "JOIN jit_hash_pair_bloom_r r ON l.k0 = r.k0 AND l.k1 = r.k1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "4096");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasGeneratedHashJoinProbeStage(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT hash join probe consumes generated-filter selected reference input", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_full_vector_selected_l AS "
	                          "SELECT ((i % 10000) * 1000003)::BIGINT AS k, i::BIGINT AS v, "
	                          "DATE '1992-01-01' + (i % 2000)::INTEGER AS d FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_full_vector_selected_r AS "
	                          "SELECT (i * 1000003)::BIGINT AS k, (i * 3)::BIGINT AS w "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT l.k, sum(l.v + r.w), sum(l.v - r.w), sum(l.v * 2), sum(r.w * 3), "
	                     "sum(l.v + 42), sum(r.w + 17) FROM jit_hash_full_vector_selected_l l "
	                     "JOIN jit_hash_full_vector_selected_r r ON l.k = r.k "
	                     "WHERE l.d > DATE '1994-01-01' GROUP BY l.k ORDER BY l.k LIMIT 3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasGeneratedHashJoinProbeStage(event) &&
		           StageNameContains(event.generated_stage_runtime, "filter.selection");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto runtime_breakdown = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(StringUtil::Contains(runtime_breakdown, "op0:filter.selection="));
		    REQUIRE(HasGeneratedAggregateUpdateStage(event));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT selected hash join generated filter remaps pre-join projection sources", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,join_order,build_side_probe_side'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_filter_map_a AS "
	                          "SELECT i::BIGINT AS k, (i % 100)::BIGINT AS v FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_selected_filter_map_b AS "
	                          "SELECT i::INTEGER AS k, (i * 3)::BIGINT AS w FROM range(4096) tbl(i)"));

	const string query = "SELECT count(*), sum(v + w) FROM ("
	                     "  SELECT a.ki, a.v, b.w "
	                     "  FROM (SELECT CAST(k AS INTEGER) AS ki, v FROM jit_selected_filter_map_a) a "
	                     "  JOIN jit_selected_filter_map_b b ON a.ki = b.k"
	                     ") joined WHERE v + w > 1000";
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
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "filter.selected_hash_join_selection=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto stages = EventGeneratedStageRuntimeBreakdown(event);
		    REQUIRE(StringUtil::Contains(stages, "filter.selected_hash_join_selection="));
	    });
}

TEST_CASE("JIT hash join chain probe preserves bloom-filtered sparse misses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_bloom_l AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v FROM range(8192) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_chain_bloom_r AS "
	                          "SELECT ((i % 64) * 128 + 7)::BIGINT AS k, i::BIGINT AS w, "
	                          "(i % 2 = 0) AS keep FROM range(512) tbl(i)"));

	const string query = "SELECT count(*), sum(r.w) FROM jit_hash_chain_bloom_l l "
	                     "JOIN (SELECT k, w FROM jit_hash_chain_bloom_r WHERE keep) r ON l.k = r.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "256");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && HasGeneratedHashJoinProbeStage(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.jit_runtime.hash_join_probe_layout == "regular_hash_table");
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}
