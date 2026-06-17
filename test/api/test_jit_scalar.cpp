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
		if (!IsCompiledSljitRegionEvent(event) || event.execution_mode != "native" ||
		    !StringUtil::Contains(event.candidate_shape, "projection")) {
			return false;
		}
		RequireNativeFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "decimal"));
		return true;
	});
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
		REQUIRE(StringUtil::Contains(event.candidate_shape, "projection"));
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
	auto result = con.Query("SELECT id FROM jit_string_native "
	                        "WHERE suffix(s, 'BRASS') OR contains(s, 'green') OR s LIKE '%special%requests%' "
	                        "ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	RequireNoUnsupportedReason(manager, "function=suffix");
	RequireNoUnsupportedReason(manager, "function=contains");
	RequireNoUnsupportedReason(manager, "function=~~");
	RequireNativeSljitIr(manager, "string_suffix");
	RequireNativeSljitIr(manager, "string_contains");
	RequireNativeSljitIr(manager, "string_like");
}

TEST_CASE("JIT lowers scalar casts and arithmetic as generated code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_cast_arithmetic AS "
	                          "SELECT i::INTEGER AS i, (i + 1)::DOUBLE AS d FROM range(10) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT i::UTINYINT AS u, d / 2.0 AS half FROM jit_cast_arithmetic WHERE i < 5");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 5);

	RequireNativeSljitIr(manager, "signed-to-unsigned-cast");
	RequireNativeSljitIr(manager, "double");
}
