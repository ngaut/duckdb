#include "test_jit_helpers.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_hash_join_probe_execution_contract.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_join_projection_aggregate_input_binding.hpp"
#include "sljit_projection_perfect_hash_aggregate_descriptor.hpp"
#include "sljit_region_kernel.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"
#include "duckdb/execution/execution_contract.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

namespace {
class JitSinkKindTestOperator : public PhysicalOperator {
public:
	JitSinkKindTestOperator(PhysicalPlan &plan, ExecutionRegionSinkKind kind_p)
	    : PhysicalOperator(plan, PhysicalOperatorType::NESTED_LOOP_JOIN, {LogicalType::BIGINT}, 1), kind(kind_p) {
	}

	ExecutionContract GetExecutionContract(ExecutionRegionOperatorSlot, bool) const override {
		ExecutionContract result;
		result.sink.SetKind(kind);
		return result;
	}

private:
	ExecutionRegionSinkKind kind;
};

class JitProofTestRuntime : public ExecutionRegionRuntime {
public:
	idx_t MaxChunks() const override {
		return 1;
	}
	idx_t MaxThreads() const override {
		return 1;
	}
	Allocator &GetAllocator() override {
		return Allocator::DefaultAllocator();
	}
	ExecutionRegionLocalState &LocalState() override {
		if (local_state) {
			return *local_state;
		}
		throw InternalException("unused proof-test local state");
	}
	SourceResultType FetchSourceContract(DataChunk *&) override {
		throw InternalException("unused proof-test source");
	}
	SourceResultType FetchPrimitiveAggregateStateSourceContract(ExecutionAggregateStateScanBatch *&) override {
		throw InternalException("unused proof-test aggregate source");
	}
	ExecutionOperatorRuntime &ExecutionOperators() override {
		throw InternalException("unused proof-test operators");
	}
	bool TraceRuntime() const override {
		return true;
	}
	void RecordGeneratedStageRuntime(ExecutionRegionStageId, int64_t, idx_t) override {
		timings++;
	}
	void RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof proof, idx_t count) override {
		if (count > 0) {
			proofs |= ExecutionRegionJitRuntimeProofBit(proof);
		}
	}
	bool Has(ExecutionRegionJitRuntimeProof proof) const {
		return ExecutionRegionJitRuntimeProofRequired(proofs, proof);
	}

	idx_t timings = 0;
	ExecutionRegionJitRuntimeProofMask proofs = 0;
	optional_ptr<ExecutionRegionLocalState> local_state;
};
} // namespace

TEST_CASE("Hash join repartition respects workspace and radix limits", "[api][jit]") {
	JitTestDatabase test;
	PhysicalPlan plan(Allocator::Get(test.context));
	PhysicalDummyScan left(plan, {LogicalType::BIGINT}, 1);
	PhysicalDummyScan right(plan, {LogicalType::BIGINT}, 1);
	LogicalComparisonJoin logical(JoinType::INNER);
	logical.types = {LogicalType::BIGINT, LogicalType::BIGINT};
	vector<JoinCondition> conditions;
	conditions.emplace_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0),
	                        make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0), ExpressionType::COMPARE_EQUAL);
	PhysicalHashJoin join(plan, logical, left, right, std::move(conditions), JoinType::INNER, 1);
	constexpr idx_t MB = 1024 * 1024;
	for (idx_t initial_bits : {idx_t(0), idx_t(4), RadixPartitioning::MAX_RADIX_BITS}) {
		auto table = join.InitializeHashTable(test.context, initial_bits);
		table->FinishInitWithLayout(
		    JoinHashTable::BuildRowLayout(join.condition_types, join.payload_columns.col_types, JoinType::INNER, {}));
		REQUIRE_FALSE(table->SetRepartitionRadixBits(0, 1024 * MB, 1000000, MB));
		REQUIRE(table->GetRadixBits() == initial_bits);
		// Even a single two-way split must fit before the layout is changed.
		REQUIRE_FALSE(table->SetRepartitionRadixBits(MB, 1024 * MB, 1000000, MB));
		REQUIRE(table->GetRadixBits() == initial_bits);
		const auto changed = table->SetRepartitionRadixBits(16 * MB, 1024 * MB, 1000000, 2 * MB);
		if (initial_bits == RadixPartitioning::MAX_RADIX_BITS) {
			REQUIRE_FALSE(changed);
			REQUIRE(table->GetRadixBits() == initial_bits);
		} else {
			REQUIRE(changed);
			const auto fanout = RadixPartitioning::NumberOfPartitions(table->GetRadixBits() - initial_bits);
			REQUIRE(fanout == 8);
			REQUIRE(fanout * 2 * MB <= 16 * MB);
		}
	}
}

TEST_CASE("Delimiter contracts preserve blocked non-hash build sinks", "[api][jit]") {
	JitTestDatabase test;
	PhysicalPlan plan(Allocator::Get(test.context));
	vector<unique_ptr<Expression>> groups;
	groups.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	PhysicalHashAggregate distinct(plan, test.context, {LogicalType::BIGINT}, {}, std::move(groups), 1);
	for (auto kind : {ExecutionRegionSinkKind::NONE, ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD,
	                  ExecutionRegionSinkKind::SORT}) {
		JitSinkKindTestOperator child(plan, kind);
		PhysicalDelimJoin delim(plan, PhysicalOperatorType::RIGHT_DELIM_JOIN, {LogicalType::BIGINT}, child, distinct,
		                        {}, 1, optional_idx(1));
		for (bool diagnostics : {false, true}) {
			auto contract = delim.GetExecutionContract(ExecutionRegionOperatorSlot::SINK, diagnostics);
			REQUIRE(contract.sink.Kind() == ExecutionRegionSinkKind::DELIM_JOIN_SINK);
			REQUIRE(contract.sink.Native().status == ExecutionRegionStateContractStatus::BLOCKED);
			REQUIRE(StringUtil::Contains(contract.sink.Native().blocker, "right-build-contract-kind-mismatch"));
			REQUIRE_FALSE(contract.sink.DelimJoin().build.contract.present);
			REQUIRE(contract.sink.DelimJoin().distinct.contract.present);
		}
	}
}

TEST_CASE("JIT timers and delegation cannot manufacture execution proof", "[api][jit]") {
	JitProofTestRuntime runtime;
	auto start = SljitRegionStageStart(runtime);
	RecordSljitRegionStageRuntimePath(runtime, 0, SljitNativeRegionOpKind::AGGREGATE_UPDATE, "native_sink_update",
	                                  start);
	RecordSljitRegionRuntimeDelegation(runtime, SljitNativeRegionOpKind::AGGREGATE_UPDATE, "native_sink_update", 10);
	REQUIRE(runtime.timings == 1);
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::DELEGATED_RUNTIME_WORK));
	REQUIRE_FALSE(runtime.Has(ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK));
	REQUIRE_FALSE(runtime.Has(ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK));
	REQUIRE_FALSE(runtime.Has(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
	RecordSljitRegionMaterializationElision(runtime, SljitNativeRegionOpKind::HASH_JOIN_PROBE, "selected_view", 10);
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK));
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK));
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION));
	REQUIRE_FALSE(runtime.Has(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
	RecordSljitRegionGeneratedWork(runtime, SljitNativeRegionOpKind::HASH_JOIN_PROBE, 0);
	REQUIRE_FALSE(runtime.Has(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
}

TEST_CASE("JIT join aggregate inputs own their schema and bound access", "[api][jit]") {
	SljitExecutableRegionOp projection;
	projection.SetKind(SljitNativeRegionOpKind::PROJECTION);
	projection.output_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT,
	                           LogicalType::BIGINT};
	projection.Projections().resize(5);
	for (idx_t idx = 0; idx < 5; idx++) {
		auto &plan = projection.Projections()[idx].plan;
		plan.kind = SljitNativeRegionExpressionKind::REFERENCE;
		plan.return_type = projection.output_types[idx];
		plan.source_index = idx == 1 ? 1 : 0;
	}
	auto &cast = projection.Projections()[2].plan;
	cast.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
	cast.cast_source_width = SljitNativeSignedIntegerWidth::INT64;
	cast.cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	cast.cast_chain.source_type = LogicalType::BIGINT;
	cast.cast_chain.steps.emplace_back();
	cast.cast_chain.steps.back().target_type = LogicalType::INTEGER;
	cast.cast_chain.steps.back().source_alias = "join_payload";
	vector<SljitCastErrorIdentity> errors;
	BindSljitCastErrorSlots(cast.cast_chain, errors);
	projection.Projections()[3].plan.source_index = 2;
	auto &mixed = projection.Projections()[4].plan;
	mixed.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
	mixed.source_index = 0;
	mixed.right_source_index = 2;

	ExecutionHashJoinProbeBinding binding;
	binding.layout_kind = ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	binding.lhs_output_column_indices = {1, 0};
	binding.rhs_output_column_count = 1;
	binding.output_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT};
	const vector<LogicalType> input_types {LogicalType::VARCHAR, LogicalType::BIGINT};
	vector<SljitExecutableRegionExpression> prepared;
	idx_t owned_code_size = 0;
	REQUIRE(SljitTryPrepareJoinAggregateInputs(binding.lhs_output_column_indices, binding.output_types, input_types,
	                                           projection, nullptr, nullptr, prepared, owned_code_size));
	SljitJoinProjectionAggregateDescriptor descriptor;
	descriptor.BorrowProjection(projection);
	for (idx_t idx = 0; idx < 5; idx++) {
		SljitJoinProjectionAggregateInputSource source;
		source.projection_idx = idx;
		source.type = projection.output_types[idx];
		descriptor.input_sources.push_back(std::move(source));
	}
	REQUIRE(SljitTryBindJoinProjectionAggregateInputs(binding, input_types, descriptor, prepared));
	REQUIRE(descriptor.InputTypes() == projection.output_types);
	REQUIRE(descriptor.input_sources[0].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT);
	REQUIRE(descriptor.input_sources[0].input_idx == 1);
	REQUIRE(descriptor.input_sources[1].input_idx == 0);
	REQUIRE(descriptor.input_sources[2].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_EXPRESSION);
	REQUIRE(descriptor.input_sources[2].expression->plan.source_index == 1);
	REQUIRE(descriptor.input_sources[3].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT);
	REQUIRE(descriptor.input_sources[3].input_idx == 0);
	REQUIRE(descriptor.input_sources[4].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_MIXED_EXPRESSION);
	REQUIRE(descriptor.input_sources[4].expression_sources.size() == 2);
	REQUIRE(descriptor.input_sources[4].expression_sources[0].input_idx == 1);
	REQUIRE(descriptor.input_sources[4].expression_sources[1].IsBuildInput());
	REQUIRE(descriptor.input_sources[4].expression->plan.right_source_index == 1);
	cast.cast_chain.steps.clear();
	REQUIRE(descriptor.input_sources[2].expression->plan.cast_chain.steps[0].source_alias.empty());
	REQUIRE(descriptor.input_sources[2].expression->plan.cast_chain.steps[0].error_binding == 0);
	auto preserved_cast = descriptor.input_sources[2].expression->plan.cast_chain;
	BindSljitCastErrorSlots(preserved_cast, errors);
	REQUIRE(errors[0].source_alias == "join_payload");
	REQUIRE_THROWS(preserved_cast.ThrowCastError(3000000000, errors));
	descriptor.EnsureInput(Allocator::DefaultAllocator());
	REQUIRE(descriptor.input.chunk.GetTypes() == projection.output_types);

	SljitJoinProjectionAggregateDescriptor sparse;
	SljitJoinProjectionAggregateInputSource source;
	source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
	source.input_idx = 1;
	source.type = LogicalType::BIGINT;
	REQUIRE(SljitTryAssignJoinProjectionAggregateInputAt(sparse, projection, 3, std::move(source)));
	REQUIRE(sparse.input_sources[0].kind == SljitJoinProjectionAggregateInputKind::UNUSED);
	REQUIRE(sparse.InputTypes() ==
	        vector<LogicalType> {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT});
	SljitJoinProjectionAggregateInputSource conflict;
	conflict.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
	conflict.input_idx = 1;
	conflict.type = LogicalType::VARCHAR;
	REQUIRE_FALSE(SljitTryAssignJoinProjectionAggregateInputAt(sparse, projection, 3, std::move(conflict)));
	REQUIRE_FALSE(SljitTryBindJoinProjectionAggregateInputs(binding, {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                                        sparse, prepared));
	REQUIRE(sparse.input_sources.empty());
	REQUIRE_FALSE(sparse.Ready());
	SljitJoinProjectionAggregateDescriptor count_star;
	REQUIRE(SljitTryBindJoinProjectionAggregateInputs(binding, input_types, count_star, prepared));
	count_star.EnsureInput(Allocator::DefaultAllocator());
	REQUIRE(count_star.input.ColumnCount() == 0);
	SljitJoinProjectionAggregateDescriptor unsupported_mixed;
	unsupported_mixed.BorrowProjection(projection);
	SljitJoinProjectionAggregateInputSource mixed_source;
	mixed_source.projection_idx = 4;
	mixed_source.type = LogicalType::BIGINT;
	unsupported_mixed.input_sources.push_back(std::move(mixed_source));
	binding.layout_kind = ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	REQUIRE_FALSE(SljitTryBindJoinProjectionAggregateInputs(binding, input_types, unsupported_mixed, prepared));
	REQUIRE(unsupported_mixed.Blocker() == "input_layout_mixed_storage");
	REQUIRE(unsupported_mixed.input_sources.empty());
}

TEST_CASE("JIT join aggregate layout preserves reordered producer casts and aliases", "[api][jit]") {
	const vector<LogicalType> input_types {LogicalType::BIGINT, LogicalType::VARCHAR};
	SljitExecutableRegionOp producer;
	producer.SetKind(SljitNativeRegionOpKind::PROJECTION);
	producer.input_types = input_types;
	producer.output_types = {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT};
	producer.Projections().resize(3);
	for (idx_t idx = 0; idx < 3; idx++) {
		auto &plan = producer.Projections()[idx].plan;
		plan.kind = SljitNativeRegionExpressionKind::REFERENCE;
		plan.return_type = producer.output_types[idx];
		plan.source_index = idx == 0 ? 1 : 0;
	}
	auto &cast = producer.Projections()[1].plan;
	cast.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
	cast.cast_source_width = SljitNativeSignedIntegerWidth::INT64;
	cast.cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	cast.cast_chain.source_type = LogicalType::BIGINT;
	cast.cast_chain.steps.emplace_back();
	cast.cast_chain.steps.back().target_type = LogicalType::INTEGER;
	cast.cast_chain.steps.back().source_alias = "producer_key";
	SljitRegionInstanceBindings instance_bindings;
	BindSljitCastErrorSlots(cast.cast_chain, instance_bindings.cast_errors);
	SljitBatchLayout layout(0, input_types,
	                        {{1, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         {0, LogicalType::BIGINT, LogicalType::INTEGER},
	                         {0, LogicalType::BIGINT, LogicalType::BIGINT}});
	ExecutionHashJoinProbeBinding binding;
	binding.lhs_output_column_indices = {2, 1, 0};
	binding.rhs_output_column_count = 1;
	binding.output_types = {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT};
	SljitExecutableRegionOp projection;
	projection.SetKind(SljitNativeRegionOpKind::PROJECTION);
	projection.output_types = binding.output_types;
	projection.Projections().resize(4);
	SljitJoinProjectionAggregateDescriptor descriptor;
	descriptor.producer_layout = &layout;
	descriptor.BorrowProjection(projection);
	for (idx_t idx = 0; idx < 4; idx++) {
		auto &plan = projection.Projections()[idx].plan;
		plan.kind = SljitNativeRegionExpressionKind::REFERENCE;
		plan.source_index = idx;
		plan.return_type = binding.output_types[idx];
		SljitJoinProjectionAggregateInputSource source;
		source.projection_idx = idx;
		source.type = plan.return_type;
		descriptor.input_sources.push_back(std::move(source));
	}
	vector<SljitExecutableRegionExpression> prepared;
	idx_t owned_code_size = 0;
	REQUIRE(SljitTryPrepareJoinAggregateInputs(binding.lhs_output_column_indices, binding.output_types, input_types,
	                                           projection, &layout, &producer, prepared, owned_code_size));
	REQUIRE(owned_code_size > 0);
	REQUIRE(SljitTryBindJoinProjectionAggregateInputs(binding, input_types, descriptor, prepared));
	REQUIRE(descriptor.input_sources[1].expression->vector.Function() == prepared[1].vector.Function());
	SljitJoinProjectionAggregateDescriptor second;
	SljitJoinProjectionAggregateInputSource second_cast;
	second_cast.projection_idx = 1;
	second_cast.type = LogicalType::INTEGER;
	second.input_sources.push_back(std::move(second_cast));
	REQUIRE(SljitTryBindJoinProjectionAggregateInputs(binding, input_types, second, prepared));
	REQUIRE(second.input_sources[0].expression->vector.Function() == prepared[1].vector.Function());
	REQUIRE(second.input_sources[0].adapter.get() != descriptor.input_sources[1].adapter.get());
	REQUIRE(descriptor.input_sources[0].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT);
	REQUIRE(descriptor.input_sources[0].input_idx == 0);
	REQUIRE_FALSE(descriptor.input_sources[0].adapter);
	REQUIRE(descriptor.input_sources[1].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_EXPRESSION);
	REQUIRE(descriptor.input_sources[1].adapter);
	REQUIRE(descriptor.input_sources[2].input_idx == 1);
	REQUIRE(descriptor.input_sources[3].kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT);
	REQUIRE(descriptor.input_sources[3].input_idx == 0);
	REQUIRE_FALSE(descriptor.input_sources[3].adapter);
	idx_t input_idx;
	const LogicalType *source_type = nullptr;
	SljitExecutableRegionExpression resolved;
	REQUIRE(SljitTryResolveProducerProbeInput(descriptor, &producer, 2, input_idx, source_type, resolved));
	REQUIRE(input_idx == 0);
	REQUIRE(SljitProjectionIsSingleSourceReferenceLike(resolved.plan));
	REQUIRE(SljitTryResolveProducerProbeInput(descriptor, &producer, 1, input_idx, source_type, resolved));
	REQUIRE(resolved.plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST);

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), input_types);
	input.data[0].SetValue(0, Value::BIGINT(42));
	input.data[0].SetValue(1, Value(LogicalType::BIGINT));
	input.SetChildCardinality(2);
	Vector result(LogicalType::INTEGER);
	auto &bound_cast = descriptor.input_sources[1];
	auto artifact = make_shared_ptr<SljitNativeRegionArtifact>(
	    "sljit", vector<SljitExecutableRegionOp>(), vector<SljitExecutableScanFilter>(),
	    ExecutionRegionScanFilterMode::NONE, SljitFullPipelineRecipePlan());
	SljitNativeRegionKernel kernel(std::move(artifact), std::move(instance_bindings));
	auto local_state = kernel.CreateLocalState(Allocator::DefaultAllocator());
	JitProofTestRuntime runtime;
	runtime.local_state = local_state.get();
	REQUIRE(SljitTryExecuteProjectionExpressionToBatch(*bound_cast.expression, input, result, 0, 2, nullptr,
	                                                   *bound_cast.adapter, &runtime));
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK));
	REQUIRE(runtime.Has(ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK));
	REQUIRE(result.GetValue(0) == Value::INTEGER(42));
	REQUIRE(result.GetValue(1).IsNull());
	input.data[0].SetValue(0, Value::BIGINT(3000000000));
	REQUIRE_THROWS_AS(SljitTryExecuteProjectionExpressionToBatch(*bound_cast.expression, input, result, 0, 1, nullptr,
	                                                             *bound_cast.adapter, &runtime),
	                  ConversionException);
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto native = con.Query("SELECT CAST(x AS INTEGER) FROM (VALUES (42::BIGINT), (NULL::BIGINT)) t(x)");
	REQUIRE(CHECK_COLUMN(native, 0, {Value::INTEGER(42), Value(LogicalType::INTEGER)}));
	REQUIRE(con.Query("SELECT CAST(3000000000::BIGINT AS INTEGER)")->HasError());
}

TEST_CASE("JIT MARK boundary binds strategy and output layout once", "[api][jit]") {
	vector<SljitExecutableRegionOp> ops(3);
	auto &join = ops[0];
	join.SetKind(SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	join.HashJoinProbe().plan.output_mode = ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	// Hash joins own their input schema in the probe plan, not the generic operator field.
	join.HashJoinProbe().plan.input_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	join.output_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	join.HashJoinProbe().plan.operator_info.HashJoin().contract.lhs_output_column_indices = {1, 0};
	ops[1].SetKind(SljitNativeRegionOpKind::FILTER);
	ops[1].Filter() = make_uniq<SljitExecutableFilter>();
	auto &filter = ops[1].Filter()->expression;
	filter.plan.kind = SljitNativeRegionExpressionKind::PREDICATE;
	filter.plan.return_type = LogicalType::BOOLEAN;
	filter.input_source_indices = {2};
	ops[2].SetKind(SljitNativeRegionOpKind::PROJECTION);
	ops[2].Projections().emplace_back();
	auto &projection = ops[2].Projections()[0].plan;
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
				join.HashJoinProbe().plan.perfect_hash_probe = perfect;
				for (bool residual : {false, true}) {
					join.HashJoinProbe().plan.residual_predicate = residual;
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
		join.HashJoinProbe().plan.perfect_hash_probe = false;
		join.HashJoinProbe().plan.residual_predicate = false;
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
	join.HashJoinProbe().plan.input_types[1] = LogicalType::VARCHAR;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	join.HashJoinProbe().plan.input_types[1] = LogicalType::BIGINT;
	join.HashJoinProbe().plan.operator_info.HashJoin().contract.lhs_output_column_indices[0] = 9;
	REQUIRE_FALSE(SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, 0, 1, true, 2, false, bound));
	join.HashJoinProbe().plan.operator_info.HashJoin().contract.lhs_output_column_indices[0] = 1;
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
	projection.SetKind(SljitNativeRegionOpKind::PROJECTION);
	projection.input_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	projection.output_types = {LogicalType::INTEGER, LogicalType::VARCHAR};
	projection.Projections().resize(2);
	auto &cast = projection.Projections()[0].plan;
	cast.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
	cast.source_index = 1;
	cast.return_type = LogicalType::INTEGER;
	cast.cast_source_width = SljitNativeSignedIntegerWidth::INT64;
	cast.cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	cast.cast_chain.source_type = LogicalType::BIGINT;
	cast.cast_chain.steps.emplace_back();
	cast.cast_chain.steps.back().target_type = LogicalType::INTEGER;
	cast.cast_chain.steps.back().source_alias = "probe_key";
	vector<SljitCastErrorIdentity> errors;
	BindSljitCastErrorSlots(cast.cast_chain, errors);
	auto &reference = projection.Projections()[1].plan;
	reference.kind = SljitNativeRegionExpressionKind::REFERENCE;
	reference.source_index = 0;
	reference.return_type = LogicalType::VARCHAR;

	ops[1].SetKind(SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	auto &plan = ops[1].HashJoinProbe().plan;
	plan.output_mode = ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	plan.input_types = projection.output_types;
	plan.keys.emplace_back();
	plan.keys[0].key_kind = SljitNativeHashJoinKeyKind::INT32;
	plan.keys[0].key_type = LogicalType::INTEGER;
	plan.keys[0].key_input_index = 0;
	plan.operator_info.HashJoin().keys.emplace_back();
	plan.operator_info.HashJoin().keys[0].input_index = 0;
	plan.residual_predicate = true;
	auto &sources = plan.operator_info.HashJoin().contract.residual_sources;
	sources.emplace_back();
	sources[0].type = LogicalType::VARCHAR;
	sources[0].input_index = 1;
	sources.emplace_back();
	sources[1].kind = ExecutionHashJoinResidualSourceKind::BUILD;
	sources[1].source_index = 1;
	sources[1].input_index = 3;
	sources[1].type = LogicalType::INTEGER;

	SljitHashJoinProbeSelectionPrimitive primitive;
	REQUIRE_FALSE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {}, {}, primitive));
	REQUIRE_FALSE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {Value(), Value::BIGINT(0)},
	                                                       {Value(), Value::BIGINT(3000000000)}, primitive));
	REQUIRE(SljitTryBindElidedHashJoinProbeSelection(ops, 0, 1, {Value(), Value::BIGINT(0)},
	                                                 {Value(), Value::BIGINT(40)}, primitive));
	REQUIRE(primitive.input_plan);
	REQUIRE(primitive.output_layout->ProducerIndex() == 0);
	REQUIRE(primitive.output_layout->SourceColumns() == vector<idx_t> {1, 0});
	REQUIRE(primitive.source_key0_int64_to_int32_unchecked);
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
	REQUIRE(bound.operator_info.HashJoin().keys[0].input_index == 1);
	REQUIRE(bound.operator_info.HashJoin().contract.residual_sources[0].input_index == 0);
	REQUIRE(bound.operator_info.HashJoin().contract.residual_sources[1].input_index == 3);
	REQUIRE(bound.keys[0].source_cast.steps[0].source_alias.empty());
	REQUIRE(bound.keys[0].source_cast.steps[0].error_binding == 0);
	auto preserved_cast = bound.keys[0].source_cast;
	BindSljitCastErrorSlots(preserved_cast, errors);
	REQUIRE(errors[0].source_alias == "probe_key");
	REQUIRE_THROWS(preserved_cast.ThrowCastError(3000000000, errors));
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
	ops.resize(6);
	ops[3].SetKind(SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	ops[5].SetKind(SljitNativeRegionOpKind::AGGREGATE_UPDATE);
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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "155003249685");

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
	REQUIRE(ungrouped->Collection().GetValue(0, 0).ToString() == "1000");

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
