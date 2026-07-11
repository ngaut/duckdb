#include "test_jit_helpers.hpp"

#include <cmath>

using namespace duckdb;

static idx_t CompiledTypedExpressionProjectionCodeSize(ExecutionRegionManager &manager) {
	idx_t code_size = 0;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || event.candidate_traits.projection_count == 0 ||
		    !StringUtil::Contains(event.ir, "typed-expression-tree")) {
			continue;
		}
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(code_size == 0);
		code_size = ExecutionRegionEventProfileCodeSize(event);
	}
	REQUIRE(code_size > 0);
	return code_size;
}

TEST_CASE("JIT auto compiles decimal projection chains through fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_projection_chain AS "
	                          "SELECT i, CAST(i AS DECIMAL(15,2)) AS d FROM range(10000) t(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT x + CAST(1 AS DECIMAL(15,2)) AS y "
	                        "FROM (SELECT d * CAST(2 AS DECIMAL(15,2)) AS x, i "
	                        "      FROM jit_decimal_projection_chain WHERE i < 5) p");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);

	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		if (!IsCompiledSljitRegionEvent(event) || EventExecutionMode(event) != "native" ||
		    event.candidate_traits.projection_count == 0) {
			return false;
		}
		RequireGeneratedMachineCodeRegion(event);
		RequireGeneratedSourceFilterContract(event);
		REQUIRE(StringUtil::Contains(event.ir, "projection(native:expression-tree"));
		REQUIRE(!StringUtil::Contains(event.ir, "op3=projection(native"));
		return true;
	});
}

TEST_CASE("JIT canonicalizes type-preserving arithmetic identities before lowering", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_identity_projection AS "
	                          "SELECT i, CAST(i % 1000 AS DECIMAL(18,10)) AS d FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT (d * CAST(1 AS DECIMAL(1,0))) * CAST(1 AS DECIMAL(1,0)) + "
	                        "CAST(10 AS DECIMAL(18,10)) AS y "
	                        "FROM jit_decimal_identity_projection WHERE i < 4");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"10.0000000000", "11.0000000000", "12.0000000000", "13.0000000000"}));

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.projection_count == 0) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE_FALSE(StringUtil::Contains(event.ir, ".multiply("));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(18,10),physical=INT64"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT omits flat nullable typed-tree path for small generated regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_typed_tree_small AS "
	                          "SELECT i::BIGINT AS i, (i % 32)::BIGINT AS g FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_typed_tree_large AS "
	                          "SELECT i::BIGINT AS i, (i % 32)::BIGINT AS g FROM range(20000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_typed_tree_small_out AS "
	                          "SELECT (i * 3) - g AS adjusted FROM jit_typed_tree_small"));
	auto small_code_size = CompiledTypedExpressionProjectionCodeSize(manager);

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_typed_tree_large_out AS "
	                          "SELECT (i * 3) - g AS adjusted FROM jit_typed_tree_large"));
	auto large_code_size = CompiledTypedExpressionProjectionCodeSize(manager);

	REQUIRE(large_code_size > small_code_size);
}

TEST_CASE("JIT lowers date year intrinsic as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_date_year_native(d DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_date_year_native VALUES "
	                          "(DATE '1992-02-29'), (DATE '-0001-01-01'), "
	                          "(DATE 'infinity'), (NULL)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT year(d) AS y FROM jit_date_year_native");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1992);
	REQUIRE(result->GetValue(0, 1).GetValue<int64_t>() == -1);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).IsNull());

	RequireNativeSljitIr(manager, "date_year", [&](const ExecutionRegionEvent &event) {
		REQUIRE(event.candidate_contract.abi == ExecutionRegionABI::FULL_PIPELINE);
		REQUIRE(event.candidate_traits.projection_count > 0);
	});
}

TEST_CASE("JIT lowers compressed scalar intrinsics as native projections", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_compress_native AS "
	                          "SELECT CASE WHEN range=10 THEN NULL ELSE (range + 1992)::BIGINT END y "
	                          "FROM range(11)"));

	ClearJitTrace(manager, true);
	auto ints = con.Query("SELECT y FROM jit_compress_native ORDER BY y NULLS LAST");
	REQUIRE_NO_FAIL(*ints);
	REQUIRE(ints->RowCount() == 11);
	REQUIRE(ints->GetValue(0, 10).IsNull());
	RequireNoUnsupportedReason(manager, "__internal_decompress_integral");
	RequireNativeSljitIr(manager, "integral_decompress");
}

TEST_CASE("JIT lowers fixed-prefix substring as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_substring_projection("
	                          "id INTEGER, s VARCHAR, amount DECIMAL(15,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_substring_projection VALUES "
	                          "(1, '13-0001', 10.00), "
	                          "(2, '13-0002', 2.50), "
	                          "(3, '31-0003', 4.00), "
	                          "(4, '漢字-alpha', 7.00), "
	                          "(5, '漢a-beta', 11.00), "
	                          "(6, 'éa-tail', 13.00), "
	                          "(7, NULL, 17.00)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_string_substring_projection_out AS "
	                          "SELECT substring(s, 1, 2) AS prefix, count(*) AS c, sum(amount) AS total "
	                          "FROM jit_string_substring_projection "
	                          "GROUP BY 1"));

	auto result = con.Query("SELECT c, total FROM jit_string_substring_projection_out WHERE prefix = '13'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2);
	REQUIRE(result->GetValue(1, 0).ToString() == "12.50");
	result = con.Query("SELECT c, total FROM jit_string_substring_projection_out WHERE prefix = '漢字'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).ToString() == "7.00");
	result = con.Query("SELECT c, total FROM jit_string_substring_projection_out WHERE prefix = '漢a'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).ToString() == "11.00");
	result = con.Query("SELECT c, total FROM jit_string_substring_projection_out WHERE prefix = 'éa'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).ToString() == "13.00");
	result = con.Query("SELECT c, total FROM jit_string_substring_projection_out WHERE prefix IS NULL");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).ToString() == "17.00");

	RequireNativeSljitIr(manager, "native:string-substring:prefix=2", [](const ExecutionRegionEvent &event) {
		REQUIRE(event.candidate_traits.projection_count > 0);
		REQUIRE(event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
		REQUIRE(StringUtil::Contains(event.reason, "backend_group_key_type=varchar"));
	});
	RequireNoUnsupportedReason(manager, "function=substring");
}

TEST_CASE("JIT lowers string predicates without aggregate sink dependence", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_native(id INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_native VALUES "
	                          "(1, 'EUROPE BRASS'), "
	                          "(2, 'forest green part'), "
	                          "(3, 'ordinary special shipping requests here'), "
	                          "(4, 'special only'), "
	                          "(5, NULL)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("WITH t AS MATERIALIZED (SELECT id, s FROM jit_string_native) "
	                        "SELECT id FROM t "
	                        "WHERE suffix(s, 'BRASS') OR contains(s, 'green') OR s LIKE '%special%requests%'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	RequireNoUnsupportedReason(manager, "function=suffix");
	RequireNoUnsupportedReason(manager, "function=contains");
	RequireNoUnsupportedReason(manager, "function=~~");
	RequireNativeSljitIr(manager, "string_suffix");
	RequireNativeSljitIr(manager, "string_contains");
	RequireNativeSljitIr(manager, "string_like");

	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='in_clause'"));
	ClearJitTrace(manager, true);
	result = con.Query("CREATE TEMP TABLE jit_string_projected_predicates AS "
	                   "WITH t AS MATERIALIZED (SELECT id, s FROM jit_string_native) "
	                   "SELECT id, "
	                   "       s LIKE '%special%requests%' AS like_hit, "
	                   "       s IN ('EUROPE BRASS', 'forest green part', "
	                   "             'ordinary special shipping requests here', NULL) AS in_hit, "
	                   "       s NOT IN ('EUROPE BRASS', 'forest green part', "
	                   "                 'ordinary special shipping requests here', NULL) AS not_in_hit "
	                   "FROM t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers=''"));
	result = con.Query("SELECT count(*) FILTER (WHERE like_hit), "
	                   "       count(*) FILTER (WHERE like_hit IS NULL), "
	                   "       count(*) FILTER (WHERE in_hit), "
	                   "       count(*) FILTER (WHERE in_hit IS NULL), "
	                   "       count(*) FILTER (WHERE not_in_hit), "
	                   "       count(*) FILTER (WHERE not_in_hit IS NULL) "
	                   "FROM jit_string_projected_predicates");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(2, 0).GetValue<int64_t>() == 3);
	REQUIRE(result->GetValue(3, 0).GetValue<int64_t>() == 2);
	REQUIRE(result->GetValue(4, 0).GetValue<int64_t>() == 0);
	REQUIRE(result->GetValue(5, 0).GetValue<int64_t>() == 2);
	RequireNativeSljitIr(manager, "string_like", [](const ExecutionRegionEvent &event) {
		REQUIRE(event.candidate_traits.projection_count > 0);
	});
	RequireNativeSljitIr(manager, "in_list", [](const ExecutionRegionEvent &event) {
		REQUIRE(event.candidate_traits.projection_count > 0);
	});

	ClearJitTrace(manager, true);
	result = con.Query("WITH t AS MATERIALIZED (SELECT id, s FROM jit_string_native) "
	                   "SELECT id FROM t "
	                   "WHERE s = 'EUROPE BRASS' OR s = 'forest green part' OR s = 'special only'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.filter_count > 0 &&
		           StringUtil::Contains(event.ir, "EUROPE BRASS");
	    },
	    [](const ExecutionRegionEvent &event) { REQUIRE(ExecutionRegionEventProfileCodeSize(event) < 1000); });

	ClearJitTrace(manager, true);
	result = con.Query("WITH t AS MATERIALIZED (SELECT id, s FROM jit_string_native) "
	                   "SELECT id FROM t WHERE s LIKE 'ordinary%special%ship_ing%'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	for (auto &event : manager.GetEvents()) {
		const auto generated_percent_only_like =
		    IsCompiledSljitRegionEvent(event) && StringUtil::Contains(event.ir, "string_like");
		REQUIRE_FALSE(generated_percent_only_like);
	}
}

TEST_CASE("JIT lowers decimal CASE payloads with string prefix conditions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_shape("
	                          "id INTEGER, category_name VARCHAR, "
	                          "base_amount DECIMAL(15,2), rebate_rate DECIMAL(15,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_decimal_case_shape VALUES "
	                          "(1, 'PRIORITY BRUSHED STEEL', 100.00, 0.10), "
	                          "(2, 'STANDARD BRUSHED STEEL', 50.00, 0.05), "
	                          "(3, 'PRIORITY ANODIZED COPPER', 200.00, 0.25), "
	                          "(4, NULL, 30.00, 0.10)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_decimal_case_shape_out AS "
	                        "SELECT id, "
	                        "       CASE WHEN prefix(category_name, 'PRIORITY') "
	                        "            THEN base_amount * (1.00 - rebate_rate) "
	                        "            ELSE 0.0000 END AS matched_revenue, "
	                        "       base_amount * (1.00 - rebate_rate) AS total_revenue "
	                        "FROM jit_decimal_case_shape");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT matched_revenue, total_revenue FROM jit_decimal_case_shape_out ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"90.0000", "0.0000", "150.0000", "0.0000"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"90.0000", "47.5000", "150.0000", "27.0000"}));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.projection_count > 0 &&
		           StringUtil::Contains(event.ir, "string_prefix") &&
		           StringUtil::Contains(event.ir, "case<logical=DECIMAL") &&
		           StringUtil::Contains(event.ir, "logical=DECIMAL(18,4),physical=INT64");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	    });
	RequireNoUnsupportedReason(manager, "root_kind=case;logical_type=DECIMAL(18,4);required=value");
}

TEST_CASE("JIT CASE branch fast path respects selected hash join sources", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_join_probe AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       100.00::DECIMAL(15,2) + (i % 13)::DECIMAL(15,2) AS base_amount, "
	                          "       0.10::DECIMAL(15,2) AS rebate_rate "
	                          "FROM range(0, 20000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_join_dim AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       CASE WHEN i % 7 IN (0, 3) THEN 'PRIORITY BRUSHED STEEL' "
	                          "            ELSE 'STANDARD ANODIZED COPPER' END AS category_name "
	                          "FROM range(0, 20000) tbl(i)"));

	const string aggregate_sql = "SELECT sum(CASE WHEN category_name LIKE 'PRIORITY%' "
	                             "                THEN base_amount * (1.00 - rebate_rate) "
	                             "                ELSE 0.0000 END) AS matched_sum "
	                             "FROM jit_decimal_case_join_probe, jit_decimal_case_join_dim "
	                             "WHERE jit_decimal_case_join_probe.item_key = jit_decimal_case_join_dim.item_key";

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto expected = con.Query(aggregate_sql);
	REQUIRE_NO_FAIL(*expected);
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));

	ClearJitTrace(manager, true);
	auto actual = con.Query(aggregate_sql);
	REQUIRE_NO_FAIL(*actual);
	REQUIRE(actual->GetValue(0, 0).ToString() == expected->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.operator_count > 0 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "hash_join_probe") &&
		           StringUtil::Contains(event.ir, "string_prefix") &&
		           StringUtil::Contains(event.ir, "case<logical=DECIMAL");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });
}

TEST_CASE("JIT lowers long string predicates through packed native comparisons", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_packed(id INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_packed VALUES "
	                          "(1, 'abcdefghijklmnop'), "
	                          "(2, 'prefix-abcdefghijkl-zzz'), "
	                          "(3, 'xxx-abcdefghijkl-suffix'), "
	                          "(4, 'ABCDEFGHIJKL-rest'), "
	                          "(5, 'mnopqrstuvwx-rest'), "
	                          "(6, 'ordinary value'), "
	                          "(7, NULL)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_string_packed_output AS "
	                          "WITH t AS MATERIALIZED (SELECT id, s FROM jit_string_packed) "
	                          "SELECT id FROM t "
	                          "WHERE s = 'abcdefghijklmnop' "
	                          "OR prefix(s, 'prefix-abcdefghijkl') "
	                          "OR suffix(s, 'abcdefghijkl-suffix') "
	                          "OR substring(s, 1, 12) IN ('ABCDEFGHIJKL', 'mnopqrstuvwx')"));
	auto result = con.Query("SELECT id FROM jit_string_packed_output ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));

	RequireNativeSljitIr(manager, "compare_equal");
	RequireNativeSljitIr(manager, "string_prefix");
	RequireNativeSljitIr(manager, "string_suffix");
	RequireNativeSljitIr(manager, "string_substring");
	RequireNoUnsupportedReason(manager, "function=prefix");
	RequireNoUnsupportedReason(manager, "function=suffix");
	RequireNoUnsupportedReason(manager, "function=substring");
}

TEST_CASE("JIT owns retained full-layout table scan filters as generated source filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_generated_source_filter AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM jit_generated_source_filter WHERE i > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "49994945");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 0 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
	RequireNativeSljitIr(manager, "filter(");
}

TEST_CASE("JIT owns pruned table scan filters through source input layout", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_generated_pruned_source_filter AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(DATE '1998-01-01' + ((i % 10)::INTEGER)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM jit_generated_pruned_source_filter WHERE d <= DATE '1998-01-05'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "24985000");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 0 &&
		           StringUtil::Contains(event.reason, "source_contract_filter_prune_required=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
}

TEST_CASE("JIT owns date range source filters through source input layout", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_generated_date_range_source_filter AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(DATE '1995-09-01' + ((i % 60)::INTEGER)) AS d "
	                          "FROM range(20000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(i) FROM jit_generated_date_range_source_filter "
	                        "WHERE d >= DATE '1995-09-10' AND d < DATE '1995-09-20'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "33411690");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedSourceFilterContract(event); });
}

TEST_CASE("JIT orders generated source filters by generic predicate cost", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_ordered_source_filter AS "
	                          "SELECT i::BIGINT AS i, "
	                          "CASE WHEN i % 11 = 0 THEN 'MAIL' ELSE 'AIR' END AS s, "
	                          "(DATE '1994-01-01' + ((i % 2000)::INTEGER)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_ordered_source_filter "
	                        "WHERE s IN ('MAIL', 'SHIP') AND d < DATE '1995-01-01' AND i > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "165");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count == 3 &&
		           StringUtil::Contains(event.reason, "generated_source_filter=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    auto generated_filter = event.reason.find("generated_source_filter=");
		    auto date_filter = event.reason.find("logical=DATE", generated_filter);
		    auto integer_filter = event.reason.find("logical=BIGINT", generated_filter);
		    auto string_filter = event.reason.find("in_list<logical=BOOLEAN", generated_filter);
		    REQUIRE(generated_filter != string::npos);
		    REQUIRE(date_filter != string::npos);
		    REQUIRE(integer_filter != string::npos);
		    REQUIRE(string_filter != string::npos);
		    REQUIRE(date_filter < integer_filter);
		    REQUIRE(date_filter < string_filter);
		    REQUIRE(integer_filter < string_filter);
	    });
}

TEST_CASE("JIT owns large complex scan filters through source input layout", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_large_complex_source_filter AS "
	                          "SELECT i::BIGINT AS i, "
	                          "(DATE '1998-01-01' + ((i % 31)::INTEGER)) AS d, "
	                          "CAST(i % 1000 AS DECIMAL(15,2)) AS v "
	                          "FROM range(1100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(v) FROM jit_large_complex_source_filter "
	                        "WHERE i >= 1000 AND i < 900000 AND d <= DATE '1998-01-15'");
	REQUIRE_NO_FAIL(*result);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 1 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
}

TEST_CASE("JIT preserves ordered sink partitions for sparse generated source filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_sparse_parallel_source_filter AS "
	                          "SELECT i::BIGINT AS i, "
	                          "CASE WHEN i % 10000 = 0 THEN 'EUROPE' ELSE 'OTHER' END AS region "
	                          "FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_sparse_parallel_source_filter_output AS "
	                          "SELECT i + 1 AS i "
	                          "FROM jit_sparse_parallel_source_filter "
	                          "WHERE region = 'EUROPE'"));
	auto result = con.Query("SELECT count(*), sum(i) FROM jit_sparse_parallel_source_filter_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 49500100);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 0 &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::MATERIALIZATION;
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedSourceFilterContract(event); });
	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		return IsSljitRegionEvent(event) && EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		       event.sink_next_batch_invocation_count > 0;
	});
}

TEST_CASE("JIT auto planner cost skips source-only string filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_string_source AS "
	                          "SELECT i::INTEGER AS id, "
	                          "CASE WHEN i % 10 = 0 THEN 'ordinary special shipping requests here' "
	                          "ELSE 'regular text' END AS s "
	                          "FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_string_source_filtered AS "
	                          "SELECT id FROM jit_auto_string_source "
	                          "WHERE s NOT LIKE '%special%requests%'"));
	auto result = con.Query("SELECT count(*) FROM jit_auto_string_source_filtered");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 900000);

	bool found_source_string_filter_decision = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" ||
		    !StringUtil::Contains(event.ir, ".string_like(")) {
			continue;
		}
		found_source_string_filter_decision = true;
		REQUIRE_FALSE(IsCompiledSljitRegionEvent(event));
		REQUIRE(EventExecutionMode(event) == "unsupported");
	}
	REQUIRE(found_source_string_filter_decision);
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return event.backend_name == "sljit" && EventPhase(event) == "decision" &&
		           StringUtil::Contains(event.ir, ".string_like(");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventStatus(event) != "compiled");
		    REQUIRE(EventExecutionMode(event) == "unsupported");
	    });
}

TEST_CASE("JIT auto preserves cheap source string equality filters under aggressive CBO", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_string_equality_source AS "
	                          "SELECT i::INTEGER AS id, "
	                          "CASE WHEN i % 4 = 0 THEN 'EUROPE' ELSE 'OTHER' END AS region "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_auto_string_equality_output AS "
	                        "SELECT id + 1 AS id FROM jit_auto_string_equality_source WHERE region = 'EUROPE'");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT count(*), sum(id) FROM jit_auto_string_equality_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2500);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 12497500);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.candidate_traits.source_filter_count > 0 &&
		           event.candidate_traits.projection_count > 0;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedSourceFilterContract(event);
		    RequireGeneratedMachineCodeRegion(event);
	    });
	RequireNoUnsupportedReason(manager, "source filter references must be local to one scan column");
}

TEST_CASE("JIT lowers signed INT128 predicates as native filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_int128_predicate("
	                          "id INTEGER, d DECIMAL(38,2), d2 DECIMAL(38,2), h HUGEINT, h2 HUGEINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_int128_predicate VALUES "
	                          "(1, 100.00, 100.00, -5, -5), "
	                          "(2, 2.00, 3.00, 5, 6), "
	                          "(3, NULL, 10.00, NULL, 0), "
	                          "(4, -1.00, -1.00, 0, -1)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT id FROM jit_int128_predicate "
	                        "WHERE ((d > CAST(3 AS DECIMAL(38,2)) OR CAST(0 AS HUGEINT) > h) "
	                        "       AND d2 = d AND h = h2) "
	                        "ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && EventExecutionMode(event) == "native" &&
		           event.candidate_traits.filter_count > 0 &&
		           StringUtil::Contains(event.ir, "logical=DECIMAL(38,2),physical=INT128") &&
		           StringUtil::Contains(event.ir, "logical=HUGEINT,physical=INT128");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	for (auto &event : manager.GetEvents()) {
		const bool unsupported_int128_reason =
		    StringUtil::Contains(event.reason, "region IR node is unsupported by SLJIT native contract lowering") &&
		    (StringUtil::Contains(event.reason, "logical=DECIMAL(38,2),physical=INT128") ||
		     StringUtil::Contains(event.reason, "logical=HUGEINT,physical=INT128"));
		REQUIRE_FALSE(unsupported_int128_reason);
	}
}

TEST_CASE("JIT lowers scalar casts and arithmetic as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_cast_arithmetic AS "
	                          "SELECT i::INTEGER AS i, (i + 1)::DOUBLE AS d, (i + 2)::DOUBLE AS e "
	                          "FROM range(10) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT i::UTINYINT AS u, d + e AS add_ref, d - 2.0 AS sub_const, "
	                        "       10.0 - d AS sub_left_const, d * e AS mul_ref, d / 2.0 AS half, d / e AS ratio "
	                        "FROM jit_cast_arithmetic WHERE i < 5 ORDER BY i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 4).GetValue<uint8_t>() == 4);
	REQUIRE(result->GetValue(1, 4).GetValue<double>() == 11.0);
	REQUIRE(result->GetValue(2, 4).GetValue<double>() == 3.0);
	REQUIRE(result->GetValue(3, 4).GetValue<double>() == 5.0);
	REQUIRE(result->GetValue(4, 4).GetValue<double>() == 30.0);
	REQUIRE(result->GetValue(5, 4).GetValue<double>() == 2.5);
	REQUIRE(result->GetValue(6, 4).GetValue<double>() == 5.0 / 6.0);

	RequireNativeSljitIr(manager, "signed-to-unsigned-cast");
	RequireNativeSljitIr(manager, "double-add-references");
	RequireNativeSljitIr(manager, "double-subtract-constant");
	RequireNativeSljitIr(manager, "double-multiply-references");
	RequireNativeSljitIr(manager, "double-divide-constant");
	RequireNativeSljitIr(manager, "double-divide-references");
}

TEST_CASE("JIT lowers casted numeric double division as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_casted_double_division("
	                          "id INTEGER, d DECIMAL(38,2), n BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_casted_double_division VALUES "
	                          "(1, 100.00, 4), "
	                          "(2, 12.50, 5), "
	                          "(3, NULL, 2), "
	                          "(4, 7.00, NULL), "
	                          "(5, 8.00, 0)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_casted_double_division_result AS "
	                        "SELECT id, CAST(d AS DOUBLE) / CAST(n AS DOUBLE) AS ratio "
	                        "FROM jit_casted_double_division");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT ratio FROM jit_casted_double_division_result ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 0).GetValue<double>() == 25.0);
	REQUIRE(result->GetValue(0, 1).GetValue<double>() == 2.5);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).IsNull());
	REQUIRE(std::isinf(result->GetValue(0, 4).GetValue<double>()));

	RequireNativeSljitIr(manager, "double-divide-references", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(38,2),physical=INT128"));
		REQUIRE(StringUtil::Contains(event.ir, "logical=BIGINT,physical=INT64"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});

	ClearJitTrace(manager, true);
	result = con.Query("CREATE TEMP TABLE jit_casted_double_constant_division_result AS "
	                   "SELECT id, CAST(d AS DOUBLE) / 7.0 AS ratio "
	                   "FROM jit_casted_double_division");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT ratio FROM jit_casted_double_constant_division_result ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 0).GetValue<double>() == 100.0 / 7.0);
	REQUIRE(result->GetValue(0, 1).GetValue<double>() == 12.5 / 7.0);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).GetValue<double>() == 1.0);
	REQUIRE(result->GetValue(0, 4).GetValue<double>() == 8.0 / 7.0);

	RequireNativeSljitIr(manager, "double-divide-constant", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(38,2),physical=INT128"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});
}

TEST_CASE("JIT lowers casted numeric double comparison predicates as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,top_n'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_casted_double_predicate("
	                          "id INTEGER, d64 DECIMAL(15,2), d128 DECIMAL(38,2), x DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_casted_double_predicate VALUES "
	                          "(1, 10.50, 100.25, 101.0), "
	                          "(2, 12.00, 300.00, 11.0), "
	                          "(3, NULL, NULL, 5.0), "
	                          "(4, 7.00, 42.00, NULL)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT id FROM jit_casted_double_predicate "
	                        "WHERE CAST(d64 AS DOUBLE) < x "
	                        "ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
	RequireNativeSljitIr(manager, "native:boolean-predicate", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "compare_less_than"));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(15,2),physical=INT64"));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DOUBLE,physical=DOUBLE"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});

	ClearJitTrace(manager, true);
	result = con.Query("SELECT id FROM jit_casted_double_predicate "
	                   "WHERE CAST(d128 AS DOUBLE) >= 100.25 "
	                   "ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
	REQUIRE(result->GetValue(0, 1).GetValue<int32_t>() == 2);
	RequireNativeSljitIr(manager, "native:boolean-predicate", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "compare_greater_than_or_equal"));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(38,2),physical=INT128"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});
}

TEST_CASE("JIT lowers decimal arithmetic comparison predicates as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,top_n'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_arith_predicate("
	                          "id INTEGER, a DECIMAL(13,2), b DECIMAL(13,2), c DECIMAL(13,2))"));
	// a + b widens to DECIMAL(14,2); the binder aligns the comparison by casting c to DECIMAL(14,2). That cast keeps
	// the same scale and only widens the declared width, so the generated path must own it as an INT64 passthrough
	// instead of delegating the whole compound-arithmetic predicate to native.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_decimal_arith_predicate VALUES "
	                          "(1, 10.00, 5.00, 12.00), "
	                          "(2, 3.00, 2.00, 10.00), "
	                          "(3, NULL, 5.00, 1.00), "
	                          "(4, 8.00, 8.00, NULL), "
	                          "(5, -5.00, 3.00, -10.00)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT id FROM jit_decimal_arith_predicate "
	                        "WHERE a + b > c "
	                        "ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
	REQUIRE(result->GetValue(0, 1).GetValue<int32_t>() == 5);
	RequireNativeSljitIr(manager, "native:typed-expression-tree", [](const ExecutionRegionEvent &event) {
		// The same-scale widening cast (c -> DECIMAL(14,2)) is lowered inside the generated tree instead of forcing
		// the whole compound-arithmetic predicate to delegate to native.
		REQUIRE(StringUtil::Contains(event.ir, "cast:DECIMAL(14,2)"));
		REQUIRE(StringUtil::Contains(event.ir, "DECIMAL(13,2)"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});
}

TEST_CASE("JIT lowers integer modulo-by-constant with magic-multiply strength reduction", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,top_n'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_modulo_predicate(id INTEGER, a BIGINT)"));
	// Negative dividends exercise the truncated-toward-zero remainder sign (-14 % 7 == 0, -7 % 7 == 0), and the NULL
	// must propagate to a NULL (excluded) predicate. Correctness here proves the magic-multiply reduction, not a
	// hardware divide, produces native-identical remainders.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_modulo_predicate VALUES "
	                          "(1, 14), (2, -14), (3, 7), (4, -7), (5, 10), (6, NULL), (7, 0)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT id FROM jit_modulo_predicate WHERE a % 7 = 0 ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
	REQUIRE(result->GetValue(0, 1).GetValue<int32_t>() == 2);
	REQUIRE(result->GetValue(0, 2).GetValue<int32_t>() == 3);
	REQUIRE(result->GetValue(0, 3).GetValue<int32_t>() == 4);
	REQUIRE(result->GetValue(0, 4).GetValue<int32_t>() == 7);
	RequireNativeSljitIr(manager, "native:typed-expression-tree", [](const ExecutionRegionEvent &event) {
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});

	// Exercise full eight-row selector groups plus a tail with positive and negative remainders.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_modulo_selector AS "
	                          "SELECT (i - 1031)::BIGINT AS a FROM range(2063) tbl(i)"));
	const string selector_query = "SELECT count(*), sum(a) FROM jit_modulo_selector WHERE a % 7 = 3";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto selector_reference = con.Query(selector_query);
	REQUIRE_NO_FAIL(*selector_reference);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto selector_result = con.Query(selector_query);
	REQUIRE_NO_FAIL(*selector_result);
	REQUIRE(selector_result->GetValue(0, 0) == selector_reference->GetValue(0, 0));
	REQUIRE(selector_result->GetValue(1, 0) == selector_reference->GetValue(1, 0));
	RequireNativeSljitIr(manager, "native:typed-expression-tree", [](const ExecutionRegionEvent &event) {
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});

	// Modulo fused inside compound arithmetic still lowers as generated code and stays native-identical.
	ClearJitTrace(manager, true);
	result = con.Query("SELECT id FROM jit_modulo_predicate WHERE a IS NOT NULL AND (a * 3 - 1) % 5 = 2 ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	auto reference = con.Query("SELECT id FROM jit_modulo_predicate WHERE a IS NOT NULL AND (a * 3 - 1) % 5 = 2 "
	                           "ORDER BY id");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(result->RowCount() == reference->RowCount());

	// Truncating integer division ("//") shares the same magic-multiply reduction and must truncate toward zero for
	// negative dividends (-14 // 4 == -3, not the floor -4).
	ClearJitTrace(manager, true);
	result = con.Query("SELECT id, a // 4 AS q FROM jit_modulo_predicate WHERE a IS NOT NULL ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 6);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 3);  // 14 // 4
	REQUIRE(result->GetValue(1, 1).GetValue<int64_t>() == -3); // -14 // 4 truncates toward zero
	REQUIRE(result->GetValue(1, 3).GetValue<int64_t>() == -1); // -7 // 4
	RequireNativeSljitIr(manager, "native:typed-expression-tree", [](const ExecutionRegionEvent &event) {
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});

	// A variable (non-constant) divisor cannot prove a non-zero denominator, so it stays native and must be correct.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_modulo_variable(id INTEGER, a BIGINT, d BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_modulo_variable VALUES (1, 14, 7), (2, 10, 4)"));
	result = con.Query("SELECT id FROM jit_modulo_variable WHERE d <> 0 AND a % d = 0 ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
}

TEST_CASE("JIT lowers int-times-decimal arithmetic through a scale-zero cast", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='filter_pushdown,top_n'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_int_decimal(id INTEGER, qty INTEGER, price DECIMAL(13,2))"));
	// The binder aligns qty * price by casting qty to DECIMAL(x,0); that scale-zero cast is a sign-extended INT64
	// passthrough the generated tree must own so the whole product lowers instead of delegating. Negatives and a NULL
	// exercise sign handling and null propagation.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_int_decimal VALUES "
	                          "(1, 10, 5.00), (2, -3, 4.00), (3, 7, -2.50), (4, 0, 9.00), (5, NULL, 1.00)"));

	ClearJitTrace(manager, true);
	// qty*price: 50.00, -12.00, -17.50, 0.00, NULL -> only id 1 (50.00) passes > 6.00.
	auto result = con.Query("SELECT id FROM jit_int_decimal WHERE qty * price > 6.00 ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int32_t>() == 1);
	RequireNativeSljitIr(manager, "native:typed-expression-tree", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "cast:DECIMAL"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});
}
