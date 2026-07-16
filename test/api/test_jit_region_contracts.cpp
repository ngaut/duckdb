#include "test_jit_helpers.hpp"
#include "sljit_full_pipeline_recipe_binding.hpp"

using namespace duckdb;

TEST_CASE("JIT recipe binders preserve the output recipe on admission failure", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops;
	vector<LogicalType> source_output_types;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	SljitFullPipelineRecipeBinding binding(ops, source_output_types, source_min_values, source_max_values, false);

	SljitFullPipelineRecipe recipe;
	recipe.direct_aggregate_consumer.probe_step_idx = 17;
	recipe.direct_aggregate_consumer.terminal_step_idx = 23;
	recipe.direct_aggregate_consumer.probe_input_filter_idx = 29;
	recipe.direct_aggregate_consumer.hash_join_idx = 31;
	recipe.direct_aggregate_consumer.aggregate_idx = 37;
	recipe.uses_extended_source_fetch_budget = true;

	auto require_unchanged = [&]() {
		REQUIRE(recipe.primitive_sequence.Count() == 0);
		REQUIRE(recipe.direct_aggregate_consumer.probe_step_idx == 17);
		REQUIRE(recipe.direct_aggregate_consumer.terminal_step_idx == 23);
		REQUIRE(recipe.direct_aggregate_consumer.probe_input_filter_idx == 29);
		REQUIRE(recipe.direct_aggregate_consumer.hash_join_idx == 31);
		REQUIRE(recipe.direct_aggregate_consumer.aggregate_idx == 37);
		REQUIRE(recipe.uses_extended_source_fetch_budget);
	};

	SljitSourceFilterAggregateFacts source_filter_aggregate;
	REQUIRE_FALSE(binding.TryMakeSourceFilterAggregateRecipe(source_filter_aggregate, recipe));
	require_unchanged();

	SljitJoinFilterAggregateFacts join_filter_aggregate;
	REQUIRE_FALSE(binding.TryMakeJoinFilterAggregateRecipe(join_filter_aggregate, recipe));
	require_unchanged();

	SljitSourceHashJoinBuildSinkFacts source_hash_join_build;
	REQUIRE_FALSE(binding.TryMakeSourceHashJoinBuildSinkRecipe(source_hash_join_build, recipe));
	require_unchanged();

	SljitHashJoinAppendSinkFacts hash_join_append;
	REQUIRE_FALSE(binding.TryMakeHashJoinAppendSinkRecipe(hash_join_append, recipe));
	require_unchanged();

	SljitHashJoinBuildSinkFacts hash_join_build;
	REQUIRE_FALSE(binding.TryMakeHashJoinBuildSinkRecipe(hash_join_build, recipe));
	require_unchanged();
}

TEST_CASE("JIT recipe publication validates explicit direct terminal ownership", "[api][jit]") {
	SljitFullPipelinePrimitiveSequence sequence;
	sequence.Add(SljitFullPipelinePrimitiveStep::SourceFetch());
	SljitHashJoinProbeSelectionPrimitive probe;
	probe.hash_join_idx = 3;
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(probe));
	SljitPostJoinProjectionAggregatePrimitive terminal;
	terminal.post_join_projection.hash_join_idx = 3;
	terminal.aggregate_idx = 5;
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(terminal));

	auto contract = SljitMakeHashJoinDirectAggregateConsumerContract(1, 2, 3, 5);
	auto recipe = SljitMakeFullPipelinePrimitiveRecipe(false, sequence, contract);
	auto recipe_plan = SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
	REQUIRE(recipe_plan.Kind() == SljitFullPipelineRecipePlanKind::PRIMITIVE_RECIPE);
	REQUIRE(recipe_plan.HasRecipe());

	contract.aggregate_idx = 7;
	REQUIRE_THROWS(SljitMakeFullPipelinePrimitiveRecipe(false, sequence, contract));

	SljitHashJoinDirectAggregateConsumerContract partial_contract;
	partial_contract.probe_step_idx = 1;
	REQUIRE_THROWS(SljitMakeFullPipelinePrimitiveRecipe(false, sequence, partial_contract));

	auto native_only_plan = SljitMakeFullPipelineNativeOnlyPlan("full_pipeline.recipe.native_only.test");
	REQUIRE(native_only_plan.Kind() == SljitFullPipelineRecipePlanKind::NATIVE_ONLY);
	REQUIRE_FALSE(native_only_plan.HasRecipe());
	REQUIRE_THROWS(SljitMakeFullPipelineNativeOnlyPlan(string()));
}

TEST_CASE("JIT table-function sources use the generic source contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	ClearJitTrace(manager);
	auto result = con.Query("SELECT sum(i * 31 + (i % 97)) FROM range(100000) AS t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "155003249685");

	bool found_source_contract = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.source_kind == ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    event.candidate_contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
			found_source_contract = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(event.runner_cost.source_contract_input_rows == 100000);
			REQUIRE_FALSE(event.runner_cost.source_contract_output_cardinality_unknown);
			REQUIRE(StringUtil::Contains(event.ir, "table_scan_contract<function=range"));
			REQUIRE(StringUtil::Contains(event.ir, "in_out_function=true"));
			REQUIRE(StringUtil::Contains(event.reason, "table-function source contract"));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    event.source_contract_output_rows > 0) {
			found_runtime = true;
			RequireNativeGeneratedRuntimeWork(event);
		}
	}
	REQUIRE(found_source_contract);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT table scan source contract fuses with generated projection and append sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_scan_contract(i BIGINT, j BIGINT, p DECIMAL(15,2), d DECIMAL(15,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_scan_contract SELECT i, i % 3, i::DECIMAL(15,2), "
	                          "(i % 10)::DECIMAL(15,2) / 100 FROM range(1000) AS t(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT p * d AS revenue FROM jit_scan_contract WHERE j=1 AND i>=10 AND i<20");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);

	bool found_source_contract = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (EventStatus(event) == "compiled" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    StringUtil::Contains(event.ir, "table_scan_contract<function=seq_scan")) {
			found_source_contract = true;
			RequireGeneratedMachineCodeRegion(event);
			RequireGeneratedSourceFilterContract(event);
			REQUIRE(StringUtil::Contains(event.ir, "source_contract<status=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "append sink contract"));
			REQUIRE(StringUtil::Contains(event.reason, "sink_contract_status=ready"));
		}
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		    EventExecutionMode(event) == "native" &&
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		    event.source_contract_output_rows > 0) {
			found_runtime = true;
		}
	}
	REQUIRE(found_source_contract);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT aggregate state scans are native source contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_stateful_perfect_hash AS "
	                          "SELECT (i % 4)::INTEGER AS k, i::BIGINT AS v FROM range(1000) tbl(i)"));

	ClearJitTrace(manager);
	auto perfect = con.Query("SELECT k, s FROM (SELECT k, sum(v) AS s FROM jit_stateful_perfect_hash GROUP BY k) g "
	                         "WHERE s >= 0 ORDER BY k");
	REQUIRE_NO_FAIL(*perfect);
	REQUIRE(perfect->RowCount() == 4);

	bool found_perfect_scan = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || event.ir.empty() ||
		    event.candidate_traits.source_kind != ExecutionRegionSourceKind::STATEFUL_OPERATOR ||
		    !StringUtil::Contains(event.ir, "function=perfect_hash_aggregate_scan")) {
			continue;
		}
		found_perfect_scan = true;
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "perfect-hash-aggregate-native-state-scan"));
		REQUIRE(event.candidate_contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
		REQUIRE(event.candidate_contract.state_scan_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
		REQUIRE(event.candidate_contract.OwnsSource());
		REQUIRE(event.candidate_contract.OwnsStateScan());
	}
	REQUIRE(found_perfect_scan);

	ClearJitTrace(manager);
	auto ungrouped = con.Query("SELECT s FROM (SELECT count(*) AS s FROM range(1000) AS t(i)) g WHERE s >= 0");
	REQUIRE_NO_FAIL(*ungrouped);
	REQUIRE(ungrouped->GetValue(0, 0).ToString() == "1000");

	bool found_ungrouped_scan = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || event.ir.empty() ||
		    event.candidate_traits.source_kind != ExecutionRegionSourceKind::STATEFUL_OPERATOR ||
		    !StringUtil::Contains(event.ir, "function=ungrouped_aggregate_scan")) {
			continue;
		}
		found_ungrouped_scan = true;
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "ungrouped-aggregate-native-state-scan"));
		REQUIRE(event.candidate_contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
		REQUIRE(event.candidate_contract.state_scan_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT);
	}
	REQUIRE(found_ungrouped_scan);
}

TEST_CASE("JIT sort and top-n state scans are source contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);

	auto require_sort_source = [&](const string &query, const string &function_name, const string &capability) {
		ClearJitTrace(manager);
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() > 0);

		bool found_contract = false;
		for (auto &event : manager.GetEvents()) {
			if (!IsSljitRegionEvent(event) || event.ir.empty() ||
			    !StringUtil::Contains(event.ir, "function=" + function_name)) {
				continue;
			}
			found_contract = true;
			REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability=" + capability));
			REQUIRE(event.candidate_contract.OwnsSource());
			REQUIRE(event.candidate_contract.OwnsStateScan());
		}
		REQUIRE(found_contract);
	};

	require_sort_source("SELECT i + 1 FROM (SELECT i FROM range(1000) t(i) ORDER BY i DESC) s WHERE i >= 998",
	                    "order_by_scan", "order-by-native-state-scan");
	require_sort_source("SELECT i + 1 FROM (SELECT i FROM range(1000) t(i) ORDER BY i LIMIT 10) s WHERE i >= 8",
	                    "top_n_scan", "top-n-native-state-scan");
}

TEST_CASE("JIT aggregate sinks expose ready native state-update contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_region_aggregate_boundary AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT sum(i) FROM jit_region_aggregate_boundary");
	REQUIRE_NO_FAIL(*result);

	bool found_aggregate_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		found_aggregate_update = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(event.candidate_contract.missing_contract_count == 0);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	}
	REQUIRE(found_aggregate_update);
}
