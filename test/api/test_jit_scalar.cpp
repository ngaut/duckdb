#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT force compiles decimal projection chains through fused regions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
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
		RequireNativeFusedRegion(event);
		RequireDuckDBScanFilteredSourceContract(event);
		REQUIRE(StringUtil::Contains(event.ir, "projection(native:expression-tree"));
		REQUIRE(!StringUtil::Contains(event.ir, "op3=projection(native"));
		return true;
	});
}

TEST_CASE("JIT canonicalizes type-preserving arithmetic identities before lowering", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
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
		RequireCompiledGeneratedRegion(event);
		REQUIRE_FALSE(StringUtil::Contains(event.ir, ".multiply("));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(18,10),physical=INT64"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT lowers date year intrinsic as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
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

	ConfigureSljit(con, "force", true, true, true, 10000);
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

TEST_CASE("JIT lowers string predicates without aggregate sink dependence", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
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
}

TEST_CASE("JIT lowers long string predicates through packed native comparisons", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
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
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" || EventRequestedPolicy(event) != "auto" ||
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
		    return event.backend_name == "sljit" && EventTarget(event) == "region" && EventPhase(event) == "decision" &&
		           EventRequestedPolicy(event) == "auto" && StringUtil::Contains(event.ir, ".string_like(");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(EventStatus(event) != "compiled");
		    REQUIRE(EventExecutionMode(event) == "unsupported");
	    });
}

TEST_CASE("JIT auto prunes protocol-only source string equality before CBO", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_string_equality AS "
	                          "SELECT i::INTEGER AS id, "
	                          "CASE WHEN i % 10 = 0 THEN 'target' ELSE 'other' END AS s "
	                          "FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_auto_string_equality_output AS "
	                        "SELECT id FROM jit_auto_string_equality WHERE s = 'target'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	result = con.Query("SELECT count(*) FROM jit_auto_string_equality_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100000);

	bool found_protocol_only_prune = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" || EventRequestedPolicy(event) != "auto" ||
		    event.blocker != "no_executable_region_work" || !StringUtil::Contains(event.ir, "compare_equal") ||
		    !StringUtil::Contains(event.ir, "materialization-append-sink")) {
			continue;
		}
		found_protocol_only_prune = true;
		REQUIRE(EventStatus(event) == "unsupported");
		REQUIRE(EventExecutionMode(event) == "unsupported");
		REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		REQUIRE_FALSE(event.runner_cost.present);
		REQUIRE(event.code_size == 0);
		REQUIRE(StringUtil::Contains(event.ir, "compare_equal"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, ".string_like("));
	}
	REQUIRE(found_protocol_only_prune);
}

TEST_CASE("JIT force preserves DuckDB scan ownership for cheap source string equality filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_force_string_equality_source AS "
	                          "SELECT i::INTEGER AS id, "
	                          "CASE WHEN i % 4 = 0 THEN 'EUROPE' ELSE 'OTHER' END AS region "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_force_string_equality_output AS "
	                        "SELECT id + 1 AS id FROM jit_force_string_equality_source WHERE region = 'EUROPE'");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT count(*), sum(id) FROM jit_force_string_equality_output");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2500);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 12497500);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) &&
		           StringUtil::Contains(event.reason, "vectorized table scan filters") &&
		           StringUtil::Contains(event.ir, "uses_scan_filters=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_uses_scan_filters);
		    RequireNativeFusedRegion(event);
	    });
	RequireNoUnsupportedReason(manager, "source filter references must be local to one scan column");
}

TEST_CASE("JIT lowers signed INT128 predicates as native filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
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
	    [](const ExecutionRegionEvent &event) { RequireNativeFusedRegion(event); });

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

	ConfigureSljit(con, "force", true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_cast_arithmetic AS "
	                          "SELECT i::INTEGER AS i, (i + 1)::DOUBLE AS d, (i + 2)::DOUBLE AS e "
	                          "FROM range(10) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT i::UTINYINT AS u, d + e AS add_ref, d - 2.0 AS sub_const, "
	                        "       10.0 - d AS sub_left_const, d * e AS mul_ref, d / 2.0 AS half "
	                        "FROM jit_cast_arithmetic WHERE i < 5 ORDER BY i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 4).GetValue<uint8_t>() == 4);
	REQUIRE(result->GetValue(1, 4).GetValue<double>() == 11.0);
	REQUIRE(result->GetValue(2, 4).GetValue<double>() == 3.0);
	REQUIRE(result->GetValue(3, 4).GetValue<double>() == 5.0);
	REQUIRE(result->GetValue(4, 4).GetValue<double>() == 30.0);
	REQUIRE(result->GetValue(5, 4).GetValue<double>() == 2.5);

	RequireNativeSljitIr(manager, "signed-to-unsigned-cast");
	RequireNativeSljitIr(manager, "double-add-references");
	RequireNativeSljitIr(manager, "double-subtract-constant");
	RequireNativeSljitIr(manager, "double-multiply-references");
	RequireNativeSljitIr(manager, "double-divide-constant");
}

TEST_CASE("JIT lowers casted numeric double division as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_casted_double_division("
	                          "id INTEGER, d DECIMAL(38,2), n BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_casted_double_division VALUES "
	                          "(1, 100.00, 4), "
	                          "(2, 12.50, 5), "
	                          "(3, NULL, 2), "
	                          "(4, 7.00, NULL)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("CREATE TEMP TABLE jit_casted_double_division_result AS "
	                        "SELECT id, CAST(d AS DOUBLE) / CAST(n AS DOUBLE) AS ratio "
	                        "FROM jit_casted_double_division");
	REQUIRE_NO_FAIL(*result);
	result = con.Query("SELECT ratio FROM jit_casted_double_division_result ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<double>() == 25.0);
	REQUIRE(result->GetValue(0, 1).GetValue<double>() == 2.5);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).IsNull());

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
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<double>() == 100.0 / 7.0);
	REQUIRE(result->GetValue(0, 1).GetValue<double>() == 12.5 / 7.0);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).GetValue<double>() == 1.0);

	RequireNativeSljitIr(manager, "double-divide-constant", [](const ExecutionRegionEvent &event) {
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL(38,2),physical=INT128"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sljit-expression-lowering-unsupported"));
	});
}

TEST_CASE("JIT lowers casted numeric double comparison predicates as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
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
