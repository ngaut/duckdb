#include "test_jit_helpers.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_hash_join_probe_execution_contract.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"

using namespace duckdb;

TEST_CASE("JIT MARK boundary binds strategy and output layout once", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops(3);
	auto &join = ops[0];
	join.kind = SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	join.hash_join_probe.plan.output_mode = ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	// Hash joins own their input schema in the probe plan, not the generic operator field.
	join.hash_join_probe.plan.input_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	join.output_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	join.hash_join_probe.plan.operator_info.hash_join_contract.lhs_output_column_indices = {1, 0};
	ops[1].kind = SljitNativeRegionOpKind::FILTER;
	ops[1].filter = make_uniq<SljitExecutableFilter>();
	auto &filter = ops[1].filter->expression;
	filter.plan.kind = SljitNativeRegionExpressionKind::PREDICATE;
	filter.plan.return_type = LogicalType::BOOLEAN;
	filter.input_source_indices = {2};
	ops[2].kind = SljitNativeRegionOpKind::PROJECTION;
	ops[2].projections.emplace_back();
	auto &projection = ops[2].projections[0].plan;
	projection.kind = SljitNativeRegionExpressionKind::REFERENCE;
	for (bool invert : {false, true}) {
		filter.plan.predicate = make_uniq<SljitNativePredicate>();
		filter.plan.predicate->return_type = LogicalType::BOOLEAN;
		filter.plan.predicate->kind = invert ? SljitNativePredicateKind::NOT : SljitNativePredicateKind::REFERENCE;
		filter.plan.predicate->source_index = 0;
		if (invert) {
			filter.plan.predicate->child = make_uniq<SljitNativePredicate>();
			filter.plan.predicate->child->kind = SljitNativePredicateKind::REFERENCE;
			filter.plan.predicate->child->return_type = LogicalType::BOOLEAN;
			filter.plan.predicate->child->source_index = 0;
		}
		for (bool reads_marker : {false, true}) {
			projection.source_index = reads_marker ? 2 : 1;
			projection.return_type = join.output_types[projection.source_index];
			for (bool perfect : {false, true}) {
				join.hash_join_probe.plan.perfect_hash_probe = perfect;
				for (bool residual : {false, true}) {
					join.hash_join_probe.plan.residual_predicate = residual;
					SljitMarkProbeFilterBoundaryPrimitive bound;
					REQUIRE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
					REQUIRE(bound.filter_mode ==
					        (invert ? SljitMarkProbeFilterMode::NON_MATCHES : SljitMarkProbeFilterMode::MATCHES));
					REQUIRE(bound.output_layout->ColumnCount() == (reads_marker ? 3 : 2));
					REQUIRE(bound.output_layout->Column(1).type == LogicalType::VARCHAR);
					REQUIRE(bound.output_layout->Column(0).source_index == 1);
					REQUIRE(bound.output_layout->Column(1).source_index == 0);
					if (reads_marker) {
						REQUIRE_FALSE(bound.output_layout->Column(2).HasSource());
					}
					REQUIRE(bound.strategy == (!reads_marker && !perfect && !residual
					                               ? SljitMarkProbeFilterBoundaryStrategy::FILTERED_SELECTION
					                               : SljitMarkProbeFilterBoundaryStrategy::MATERIALIZE));
					REQUIRE(bound.marker_mode == (!reads_marker ? SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER
					                              : invert ? SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS
					                                       : SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE));
				}
			}
		}
		join.hash_join_probe.plan.perfect_hash_probe = false;
		join.hash_join_probe.plan.residual_predicate = false;
		SljitMarkProbeFilterBoundaryPrimitive bound;
		REQUIRE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, DConstants::INVALID_INDEX, true, bound));
		REQUIRE(bound.strategy == SljitMarkProbeFilterBoundaryStrategy::FILTERED_MATERIALIZED);
		REQUIRE(bound.marker_mode == (invert ? SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_FALSE
		                                     : SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE));
		REQUIRE(bound.output_layout->Types() == join.output_types);
		REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, false, 2, true, bound));
		REQUIRE(bound.strategy == SljitMarkProbeFilterBoundaryStrategy::FILTERED_MATERIALIZED);
	}
	SljitMarkProbeFilterBoundaryPrimitive bound;
	REQUIRE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, false, DConstants::INVALID_INDEX, false, bound));
	REQUIRE(bound.marker_mode == SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS);
	join.output_types.back() = LogicalType::BIGINT;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	REQUIRE(bound.output_layout->Column(2).type == LogicalType::BOOLEAN);
	join.output_types.back() = LogicalType::BOOLEAN;
	join.hash_join_probe.plan.input_types[1] = LogicalType::VARCHAR;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	join.hash_join_probe.plan.input_types[1] = LogicalType::BIGINT;
	join.hash_join_probe.plan.operator_info.hash_join_contract.lhs_output_column_indices[0] = 9;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	join.hash_join_probe.plan.operator_info.hash_join_contract.lhs_output_column_indices[0] = 1;
	filter.input_source_indices[0] = 0;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 9, false, bound));
}

TEST_CASE("JIT batch layouts validate types and retain producer identity", "[api][jit]") {
	const vector<LogicalType> types {LogicalType::INTEGER, LogicalType::VARCHAR};
	vector<SljitBatchLayoutColumn> columns {{1, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        {0, LogicalType::INTEGER, LogicalType::BIGINT},
	                                        {1, LogicalType::VARCHAR, LogicalType::VARCHAR}};
	auto layout = make_shared_ptr<SljitBatchLayout>(3, types, columns);
	REQUIRE(layout->ProducerIndex() == 3);
	REQUIRE(layout->ColumnCount() == 3);
	REQUIRE(layout->SourceColumns() == vector<idx_t> {1, 0, 1});
	REQUIRE(layout->Types() == vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR});
	REQUIRE_FALSE(layout->PreservesSourceColumnOrdinals());
	columns[0].source_index = 0;
	REQUIRE(layout->Column(0).source_index == 1);
	REQUIRE_THROWS(SljitBatchLayout(3, types, columns));
	columns[0].source_index = 2;
	REQUIRE_THROWS(SljitBatchLayout(3, types, columns));
	columns[0] = {0, LogicalType::DATE, LogicalType::DATE};
	REQUIRE_THROWS(SljitBatchLayout(3, types, columns));
	REQUIRE_THROWS(SljitBatchLayout(DConstants::INVALID_INDEX, types, {}));
	SljitBatchLayout computed(4, types, {{DConstants::INVALID_INDEX, LogicalType(), LogicalType::BOOLEAN}});
	REQUIRE_FALSE(computed.Column(0).HasSource());
	REQUIRE(computed.Types() == vector<LogicalType> {LogicalType::BOOLEAN});
	REQUIRE_THROWS(computed.SourceColumns());

	SljitJoinProjectionAggregateDescriptor descriptor;
	REQUIRE(descriptor.ProducerLayoutMatches(nullptr));
	descriptor.producer_layout = layout.get();
	REQUIRE(descriptor.ProducerLayoutMatches(layout.get()));
	REQUIRE_FALSE(descriptor.ProducerLayoutMatches(nullptr));
	const auto another_producer = make_shared_ptr<SljitBatchLayout>(
	    4, types,
	    vector<SljitBatchLayoutColumn> {{1, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                    {0, LogicalType::INTEGER, LogicalType::BIGINT},
	                                    {1, LogicalType::VARCHAR, LogicalType::VARCHAR}});
	REQUIRE_FALSE(descriptor.ProducerLayoutMatches(another_producer.get()));
	descriptor.ClearBuiltState();
	REQUIRE(descriptor.ProducerLayoutMatches(nullptr));
}

TEST_CASE("JIT elided probes bind one typed layout and immutable input plan", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops(2);
	auto &projection = ops[0];
	projection.kind = SljitNativeRegionOpKind::PROJECTION;
	projection.input_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	projection.output_types = {LogicalType::INTEGER, LogicalType::VARCHAR};
	projection.projections.resize(2);
	auto &cast = projection.projections[0].plan;
	cast.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
	cast.source_index = 1;
	cast.return_type = LogicalType::INTEGER;
	cast.cast_source_width = SljitNativeSignedIntegerWidth::INT64;
	cast.cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	cast.cast_chain.source_type = LogicalType::BIGINT;
	cast.cast_chain.steps.emplace_back();
	cast.cast_chain.steps.back().target_type = LogicalType::INTEGER;
	cast.cast_chain.steps.back().source_alias = "probe_key";
	auto &reference = projection.projections[1].plan;
	reference.kind = SljitNativeRegionExpressionKind::REFERENCE;
	reference.source_index = 0;
	reference.return_type = LogicalType::VARCHAR;

	ops[1].kind = SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	auto &plan = ops[1].hash_join_probe.plan;
	plan.output_mode = ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	plan.input_types = projection.output_types;
	plan.keys.emplace_back();
	plan.keys[0].key_kind = SljitNativeHashJoinKeyKind::INT32;
	plan.keys[0].key_type = LogicalType::INTEGER;
	plan.keys[0].key_input_index = 0;
	plan.operator_info.hash_join_keys.emplace_back();
	plan.operator_info.hash_join_keys[0].input_index = 0;
	plan.residual_predicate = true;
	auto &sources = plan.operator_info.hash_join_contract.residual_sources;
	sources.emplace_back();
	sources[0].type = LogicalType::VARCHAR;
	sources[0].input_index = 1;
	sources.emplace_back();
	sources[1].kind = ExecutionHashJoinResidualSourceKind::BUILD;
	sources[1].source_index = 1;
	sources[1].input_index = 3;
	sources[1].type = LogicalType::INTEGER;

	SljitHashJoinProbeSelectionPrimitive primitive;
	REQUIRE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, primitive));
	REQUIRE(primitive.input_plan);
	REQUIRE(primitive.output_layout->ProducerIndex() == 0);
	REQUIRE(primitive.output_layout->SourceColumns() == vector<idx_t> {1, 0});
	REQUIRE_FALSE(primitive.source_key0_int64_to_int32_unchecked);
	auto copy = primitive;
	REQUIRE(copy.input_plan.get() == primitive.input_plan.get());
	REQUIRE(copy.output_layout.get() == primitive.output_layout.get());
	primitive.input_plan.reset();
	primitive.output_layout.reset();
	cast.source_index = 0;
	cast.cast_chain.steps.clear();
	plan.keys[0].key_input_index = 7;
	sources[0].input_index = 9;
	auto &bound = SljitBindHashJoinProbeExecutionPlan(ops[1], copy.input_plan.get(),
	                                                  SljitHashJoinProbeOutputContract::SELECTED_VIEW);
	REQUIRE(bound.input_types == projection.input_types);
	REQUIRE(bound.keys[0].key_input_index == 1);
	REQUIRE(bound.operator_info.hash_join_keys[0].input_index == 1);
	REQUIRE(bound.operator_info.hash_join_contract.residual_sources[0].input_index == 0);
	REQUIRE(bound.operator_info.hash_join_contract.residual_sources[1].input_index == 3);
	REQUIRE(bound.keys[0].source_cast.steps[0].source_alias == "probe_key");
	REQUIRE_THROWS(bound.keys[0].source_cast.ThrowCastError(3000000000));
	REQUIRE_THROWS(SljitBindHashJoinProbeExecutionPlan(ops[1], copy.input_plan.get(),
	                                                   SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT));
	auto published_layout = copy.output_layout.get();
	REQUIRE_FALSE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, copy));
	REQUIRE(copy.output_layout.get() == published_layout);

	cast.source_index = 1;
	cast.cast_chain = bound.keys[0].source_cast;
	plan.keys[0].key_input_index = 0;
	sources[0].input_index = 0; // A residual cannot read the unexecuted narrowing as a VARCHAR.
	REQUIRE_FALSE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, copy));
	REQUIRE(copy.output_layout.get() == published_layout);
	sources[0].input_index = 1;
	REQUIRE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {Value(), Value::BIGINT(0)},
	                                                 {Value(), Value::BIGINT(40)}, copy));
	REQUIRE(copy.source_key0_int64_to_int32_unchecked);
	projection.output_types[1] = LogicalType::BIGINT;
	REQUIRE_FALSE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, primitive));

	projection.input_types = {LogicalType::INTEGER, LogicalType::VARCHAR};
	projection.output_types[1] = LogicalType::VARCHAR;
	cast.kind = SljitNativeRegionExpressionKind::REFERENCE;
	cast.source_index = 0;
	reference.source_index = 1;
	plan.residual_predicate = false;
	REQUIRE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, primitive));
	REQUIRE(primitive.output_layout->PreservesSourceColumnOrdinals());
	REQUIRE_FALSE(primitive.input_plan);
	REQUIRE_FALSE(primitive.source_key0_int64_to_int32_unchecked);
}

TEST_CASE("JIT execution suppression follows nested context scopes", "[api][jit]") {
	DuckDB db;
	Connection first(db);
	Connection second(db);

	REQUIRE_FALSE(first.context->IsCompiledExecutionSuppressed());
	REQUIRE_FALSE(second.context->IsCompiledExecutionSuppressed());
	{
		ExecutionRegionSuppressionGuard first_guard(*first.context);
		REQUIRE(first.context->IsCompiledExecutionSuppressed());
		REQUIRE_FALSE(second.context->IsCompiledExecutionSuppressed());
		{
			ExecutionRegionSuppressionGuard second_guard(*second.context);
			REQUIRE(first.context->IsCompiledExecutionSuppressed());
			REQUIRE(second.context->IsCompiledExecutionSuppressed());
		}
		REQUIRE(first.context->IsCompiledExecutionSuppressed());
		REQUIRE_FALSE(second.context->IsCompiledExecutionSuppressed());
	}
	REQUIRE_FALSE(first.context->IsCompiledExecutionSuppressed());
	REQUIRE_FALSE(second.context->IsCompiledExecutionSuppressed());
}

TEST_CASE("JIT recipe binders preserve the output recipe on admission failure", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops;
	vector<LogicalType> source_output_types;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	SljitFullPipelineRecipeBinding binding(ops, source_output_types, source_min_values, source_max_values);

	SljitFullPipelineRecipe recipe;
	recipe.direct_aggregate_consumer.probe_step_idx = 17;
	recipe.direct_aggregate_consumer.terminal_step_idx = 23;
	recipe.direct_aggregate_consumer.probe_input_filter_idx = 29;
	recipe.direct_aggregate_consumer.hash_join_idx = 31;
	recipe.direct_aggregate_consumer.aggregate_idx = 37;

	auto require_unchanged = [&]() {
		REQUIRE(recipe.primitive_sequence.Count() == 0);
		REQUIRE(recipe.direct_aggregate_consumer.probe_step_idx == 17);
		REQUIRE(recipe.direct_aggregate_consumer.terminal_step_idx == 23);
		REQUIRE(recipe.direct_aggregate_consumer.probe_input_filter_idx == 29);
		REQUIRE(recipe.direct_aggregate_consumer.hash_join_idx == 31);
		REQUIRE(recipe.direct_aggregate_consumer.aggregate_idx == 37);
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

	SljitHashJoinBuildSinkFacts hash_join_build;
	REQUIRE_FALSE(binding.TryMakeHashJoinBuildSinkRecipe(hash_join_build, recipe));
	require_unchanged();

	SljitGeneratedFilterProjectionNativeTailFacts generated_filter_tail;
	REQUIRE_FALSE(binding.TryMakeGeneratedFilterProjectionNativeTailRecipe(generated_filter_tail, recipe));
	require_unchanged();

	SljitProjectionFilterProjectionNativeTailFacts projection_filter_tail;
	REQUIRE_FALSE(binding.TryMakeProjectionFilterProjectionNativeTailRecipe(projection_filter_tail, recipe));
	require_unchanged();

	auto &projection_aggregate = binding.ProjectionAggregateRecipes();
	SljitMarkFilterProjectionNativeTailFacts mark_filter_tail;
	REQUIRE_FALSE(projection_aggregate.TryMakeMarkFilterProjectionNativeTailRecipe(mark_filter_tail, recipe));
	require_unchanged();

	SljitFullPipelineProjectionAggregateShape projection_aggregate_shape;
	SljitProjectionAggregatePrefixFacts projection_aggregate_prefix;
	REQUIRE_FALSE(projection_aggregate.TryMakeSourceProjectionAggregateTailRecipe(projection_aggregate_shape,
	                                                                              projection_aggregate_prefix, recipe));
	require_unchanged();

	REQUIRE_FALSE(projection_aggregate.TryMakeJoinDirectProjectionAggregateRecipe(projection_aggregate_shape,
	                                                                              projection_aggregate_prefix, recipe));
	require_unchanged();
	REQUIRE_FALSE(projection_aggregate.TryMakeJoinProjectionAggregateTailRecipe(projection_aggregate_shape,
	                                                                            projection_aggregate_prefix, recipe));
	require_unchanged();
	REQUIRE_FALSE(projection_aggregate.TryMakeMarkFilterProjectionAggregateRecipe(projection_aggregate_shape,
	                                                                              projection_aggregate_prefix, recipe));
	require_unchanged();
	REQUIRE_FALSE(projection_aggregate.TryMakeMarkFilterNativeTailRecipe(projection_aggregate_prefix, recipe));
	require_unchanged();
}

TEST_CASE("JIT recipe publication validates explicit direct terminal ownership", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops;
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
	auto recipe = SljitFinalizeFullPipelinePrimitiveRecipe(ops, sequence, contract);
	auto recipe_plan = SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
	REQUIRE(recipe_plan.Kind() == SljitFullPipelineRecipePlanKind::PRIMITIVE_RECIPE);
	REQUIRE(recipe_plan.HasRecipe());

	contract.aggregate_idx = 7;
	REQUIRE_THROWS(SljitFinalizeFullPipelinePrimitiveRecipe(ops, sequence, contract));

	SljitHashJoinDirectAggregateConsumerContract partial_contract;
	partial_contract.probe_step_idx = 1;
	REQUIRE_THROWS(SljitFinalizeFullPipelinePrimitiveRecipe(ops, sequence, partial_contract));

	auto native_only_plan = SljitMakeFullPipelineNativeOnlyPlan("full_pipeline.recipe.native_only.test");
	REQUIRE(native_only_plan.Kind() == SljitFullPipelineRecipePlanKind::NATIVE_ONLY);
	REQUIRE_FALSE(native_only_plan.HasRecipe());
	REQUIRE_THROWS(SljitMakeFullPipelineNativeOnlyPlan(string()));
}

TEST_CASE("JIT primitive steps keep operator ownership in typed payloads", "[api][jit]") {
	auto native_tail = SljitFullPipelinePrimitiveStep::NativeTailDelegation(7);
	REQUIRE(native_tail.kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION);
	REQUIRE(native_tail.native_tail_start_idx == 7);

	SljitHashJoinProbeSelectionPrimitive selection;
	selection.hash_join_idx = 11;
	auto probe = SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(selection);
	REQUIRE(probe.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION);
	REQUIRE(probe.hash_join_probe_selection.hash_join_idx == 11);
}

TEST_CASE("JIT table-function sources use the generic source contract", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);
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
		    event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
		REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
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
		REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	}
	REQUIRE(found_ungrouped_scan);
}

TEST_CASE("JIT sort and top-n state scans are source contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, 10000);

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
			REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
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

	ConfigureSljitForCoverage(con, true, true, 10000);
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
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	}
	REQUIRE(found_aggregate_update);
}
