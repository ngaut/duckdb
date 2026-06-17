#include "test_jit_helpers.hpp"

using namespace duckdb;

TEST_CASE("JIT native hash join build uses operator protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_l AS SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE jit_hash_build_r AS SELECT i::BIGINT AS j, (i + 1)::BIGINT AS x FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT a.i, b.j FROM jit_hash_build_l a "
	                        "JOIN (SELECT j, x FROM jit_hash_build_r WHERE j > 10) b ON a.i=b.x "
	                        "WHERE a.i < 20 ORDER BY a.i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14, 15, 16, 17, 18, 19}));

	bool found_build_contract = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || event.execution_mode != "native" ||
		    !StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON)) {
			continue;
		}
		found_build_contract = true;
		RequireCompiledFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
	}
	REQUIRE(found_build_contract);
}

TEST_CASE("JIT native hash join probe compiles with append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_l AS "
	                          "SELECT (i % 32)::BIGINT AS k, i::BIGINT AS v FROM range(2048) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_r AS "
	                          "SELECT i::BIGINT AS k, (i * 3)::BIGINT AS w FROM range(32) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT l.k, l.v, r.w FROM jit_hash_probe_l l "
	                        "JOIN jit_hash_probe_r r ON l.k=r.k WHERE l.v < 8 ORDER BY l.v");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 8);
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4, 5, 6, 7}));

	bool found_probe = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
			found_probe = true;
			RequireNativeFusedRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=1"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows > 0) {
			found_runtime = true;
		}
	}
	REQUIRE(found_probe);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT native nested loop join probe compiles with append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_probe_l AS "
	                          "SELECT CAST(i AS DECIMAL(38,2)) AS x FROM range(4) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nested_probe_r AS "
	                          "SELECT CAST(1.500000000000 AS DECIMAL(38,12)) AS y"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT l.x, r.y FROM jit_nested_probe_l l "
	                        "JOIN jit_nested_probe_r r ON CAST(l.x AS DECIMAL(38,12)) > r.y ORDER BY l.x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2);

	bool found_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || event.execution_mode != "native" ||
		    !StringUtil::Contains(event.reason, JIT_NESTED_LOOP_JOIN_PROBE_EXECUTABLE_REASON)) {
			continue;
		}
		found_probe = true;
		RequireNativeFusedRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "nested_loop_join_probe("));
		REQUIRE(StringUtil::Contains(event.ir, "native:decimal128-scale-up"));
		REQUIRE(StringUtil::Contains(event.ir, "native_nested_loop_join_probe_contract_status=ready"));
	}
	REQUIRE(found_probe);
}

TEST_CASE("JIT delimiter join sink uses native sink protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "force", false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_outer(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_delim_outer SELECT i, i % 5 FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_delim_inner(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_delim_inner SELECT i, i % 5 FROM range(1000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT k FROM jit_delim_outer o "
	                        "WHERE EXISTS (SELECT 1 FROM jit_delim_inner i WHERE i.g=o.g AND i.k>o.k) "
	                        "ORDER BY k LIMIT 3");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 3);

	bool found_delim_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			continue;
		}
		found_delim_sink = true;
		RequireCompiledFusedOperatorProtocolRegion(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "delimiter join sink protocol"));
		REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-aggregate-native-state-update-boundary"));
	}
	REQUIRE(found_delim_sink);
}
