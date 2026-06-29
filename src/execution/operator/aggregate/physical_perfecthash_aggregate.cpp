#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                                           vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           const vector<unique_ptr<BaseStatistics>> &group_stats,
                                                           vector<idx_t> required_bits_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::PERFECT_HASH_GROUP_BY, std::move(types_p),
                       estimated_cardinality),
      groups(std::move(groups_p)), aggregates(std::move(aggregates_p)), required_bits(std::move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = *stats;
		D_ASSERT(NumericStats::HasMin(nstats));
		group_minima.push_back(NumericStats::Min(nstats));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->GetReturnType());
	}

	vector<BoundAggregateExpression *> bindings;
	vector<LogicalType> payload_types_filters;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.IsDistinct());
		D_ASSERT(aggr.Function().HasStateCombineCallback());
		for (auto &child : aggr.GetChildren()) {
			payload_types.push_back(child->GetReturnType());
		}
		if (aggr.GetFilter()) {
			payload_types_filters.push_back(aggr.GetFilter()->GetReturnType());
		}
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
	aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		aggregate_input_idx += aggr.GetChildren().size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.GetFilter()) {
			auto &filter_ref = *aggr.GetFilter();
			auto &bound_ref_expr = filter_ref.Cast<BoundReferenceExpression>();
			auto it = filter_indexes.find(filter_ref);
			if (it == filter_indexes.end()) {
				filter_indexes[filter_ref] = bound_ref_expr.Index();
				bound_ref_expr.IndexMutable() = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}
}

unique_ptr<PerfectAggregateHashTable> PhysicalPerfectHashAggregate::CreateHT(Allocator &allocator,
                                                                             ClientContext &context) const {
	return make_uniq<PerfectAggregateHashTable>(context, allocator, group_types, payload_types, aggregate_objects,
	                                            group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PerfectHashAggregateGlobalState : public GlobalSinkState {
public:
	PerfectHashAggregateGlobalState(const PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(Allocator::Get(context), context)) {
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
};

class PerfectHashAggregateLocalState : public LocalSinkState {
public:
	PerfectHashAggregateLocalState(const PhysicalPerfectHashAggregate &op, ExecutionContext &context)
	    : op(op), ht(op.CreateHT(Allocator::Get(context.client), context.client)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	//! The local aggregate hash table
	const PhysicalPerfectHashAggregate &op;
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

class PerfectHashAggregateExecutionRegionSinkState : public ExecutionAggregateUpdateState {
public:
	PerfectHashAggregateExecutionRegionSinkState(ExecutionContext &context_p, const PhysicalPerfectHashAggregate &op_p,
	                                             PerfectHashAggregateLocalState &local_state_p)
	    : context(context_p), op(op_p), local_state(local_state_p) {
	}

	SinkResultType Sink(DataChunk &input) override {
		DataChunk &group_chunk = local_state.group_chunk;
		DataChunk &aggregate_input_chunk = local_state.aggregate_input_chunk;

		for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
			auto &group = op.groups[group_idx];
			if (group->GetExpressionClass() != ExpressionClass::BOUND_REF) {
				throw InternalException("execution region perfect hash aggregate group is not a bound reference");
			}
			auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
			if (bound_ref_expr.Index() >= input.ColumnCount()) {
				throw InternalException("execution region perfect hash aggregate group index out of range");
			}
			group_chunk.data[group_idx].Reference(input.data[bound_ref_expr.Index()]);
		}
		idx_t aggregate_input_idx = 0;
		for (auto &aggregate : op.aggregates) {
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			if (aggr.IsDistinct() || aggr.GetFilter() || aggr.GetOrderBys()) {
				throw InternalException(
				    "execution region perfect hash aggregate update received unsupported aggregate");
			}
			for (auto &child_expr : aggr.GetChildren()) {
				if (child_expr->GetExpressionClass() != ExpressionClass::BOUND_REF) {
					throw InternalException("execution region perfect hash aggregate payload is not a bound reference");
				}
				auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
				if (bound_ref_expr.Index() >= input.ColumnCount()) {
					throw InternalException("execution region perfect hash aggregate payload index out of range");
				}
				aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.Index()]);
			}
		}

		group_chunk.Verify(context.client.db);
		aggregate_input_chunk.Verify(context.client.db);
		D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());
		local_state.ht->AddChunk(group_chunk, aggregate_input_chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

private:
	ExecutionContext &context;
	const PhysicalPerfectHashAggregate &op;
	PerfectHashAggregateLocalState &local_state;
};

class PerfectHashAggregateStateAddressState : public ExecutionGroupedAggregateStateAddressState {
public:
	PerfectHashAggregateStateAddressState(ExecutionContext &context_p, const PhysicalPerfectHashAggregate &op_p,
	                                      PerfectHashAggregateLocalState &local_state_p)
	    : context(context_p), op(op_p), local_state(local_state_p) {
	}

	void ResolveStateAddresses(DataChunk &input, Vector &addresses,
	                           optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) override {
		(void)recorder;
		DataChunk &group_chunk = local_state.group_chunk;
		for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
			auto &group = op.groups[group_idx];
			if (group->GetExpressionClass() != ExpressionClass::BOUND_REF) {
				throw InternalException("execution region perfect hash aggregate group is not a bound reference");
			}
			auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
			if (bound_ref_expr.Index() >= input.ColumnCount()) {
				throw InternalException("execution region perfect hash aggregate group index out of range");
			}
			group_chunk.data[group_idx].Reference(input.data[bound_ref_expr.Index()]);
		}
		group_chunk.SetChildCardinality(input.size());
		group_chunk.Verify(context.client.db);

		auto layout = local_state.ht->GetStateLayout();
		local_state.ht->ResolveStateAddresses(group_chunk, addresses, layout.aggregate_state_offset);
	}

private:
	ExecutionContext &context;
	const PhysicalPerfectHashAggregate &op;
	PerfectHashAggregateLocalState &local_state;
};

static ExecutionPrimitiveAggregateUpdateBinding
BuildPerfectHashAggregatePrimitiveUpdateBinding(const PhysicalPerfectHashAggregate &op,
                                                const ExecutionRegionSinkInfo &sink_info) {
	ExecutionPrimitiveAggregateUpdateBinding result;
	auto &contract = sink_info.aggregate_contract;
	if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < sink_info.aggregates.size()) {
		result.blocker = "perfect-hash-aggregate-primitive-update-grouped-state-layout";
		return result;
	}
	result.lanes.reserve(sink_info.aggregates.size());
	for (auto &aggregate_info : sink_info.aggregates) {
		if (aggregate_info.aggregate_index >= op.aggregates.size() ||
		    aggregate_info.aggregate_index >= contract.grouped_state_offsets.size()) {
			result.blocker = "perfect-hash-aggregate-primitive-update-aggregate-index-out-of-range";
			return result;
		}
		auto &aggregate = op.aggregates[aggregate_info.aggregate_index]->Cast<BoundAggregateExpression>();
		auto &function = aggregate.Function();
		if (!function.HasPrimitiveUpdateABI()) {
			result.blocker = "perfect-hash-aggregate-primitive-update-abi-missing";
			return result;
		}
		auto &abi = function.GetPrimitiveUpdateABI();
		if (!AggregatePrimitiveUpdateKindIsSupported(abi.kind)) {
			result.blocker = "perfect-hash-aggregate-primitive-update-kind-unsupported";
			return result;
		}
		const auto requires_payload = AggregatePrimitiveUpdateRequiresPayload(abi.kind);
		if (requires_payload) {
			if (aggregate_info.child_count != 1 || aggregate_info.child_types.empty()) {
				result.blocker = "perfect-hash-aggregate-primitive-update-requires-one-payload";
				return result;
			}
			if (AggregatePrimitiveUpdateRequiresTypedPayload(abi.kind) &&
			    aggregate_info.child_types[0].InternalType() != abi.input_type) {
				result.blocker = "perfect-hash-aggregate-primitive-update-payload-type-mismatch";
				return result;
			}
		} else if (aggregate_info.child_count != 0) {
			result.blocker = "perfect-hash-aggregate-primitive-update-requires-no-payload";
			return result;
		}
		if (function.HasStateSizeCallback() && function.GetStateSizeCallback()(function) != abi.state_size) {
			result.blocker = "perfect-hash-aggregate-primitive-update-state-size-mismatch";
			return result;
		}
		auto value_size = AggregatePrimitiveUpdateStateValueSize(abi.kind);
		if (value_size == 0 || abi.state_value_offset + value_size > abi.state_size) {
			result.blocker = "perfect-hash-aggregate-primitive-update-state-layout-invalid";
			return result;
		}
		if (AggregatePrimitiveUpdateHasStateIsSet(abi.kind) &&
		    abi.state_is_set_offset + sizeof(bool) > abi.state_size) {
			result.blocker = "perfect-hash-aggregate-primitive-update-state-layout-invalid";
			return result;
		}

		ExecutionPrimitiveAggregateUpdateLane lane;
		lane.ready = true;
		lane.kind = abi.kind;
		lane.aggregate_index = aggregate_info.aggregate_index;
		lane.payload_index = requires_payload ? aggregate_info.payload_index : DConstants::INVALID_INDEX;
		lane.payload_type = requires_payload ? aggregate_info.child_types[0].InternalType() : PhysicalType::INVALID;
		lane.state_size = abi.state_size;
		lane.state_offset = contract.grouped_state_offsets[aggregate_info.aggregate_index];
		lane.state_value_offset = abi.state_value_offset;
		lane.state_is_set_offset = abi.state_is_set_offset;
		lane.blocker.clear();
		result.lanes.push_back(lane);
	}
	result.ready = !result.lanes.empty() && result.lanes.size() == sink_info.aggregates.size();
	result.blocker = result.ready ? string() : "perfect-hash-aggregate-primitive-update-empty";
	return result;
}

static string ValidatePerfectHashAggregateExecutionSink(const ExecutionRegionSinkInfo &sink_info) {
	if (sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return "perfect-hash-aggregate-runtime-kind-mismatch";
	}
	auto &contract = sink_info.aggregate_contract;
	if (contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return contract.native_state_update_contract.blocker.empty()
		           ? "perfect-hash-aggregate-state-update-contract-not-ready"
		           : contract.native_state_update_contract.blocker;
	}
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, sink_info.aggregates, sink_info.groups);
	if (!blocker.empty()) {
		return blocker;
	}
	return string();
}

unique_ptr<GlobalSinkState> PhysicalPerfectHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PerfectHashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalPerfectHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PerfectHashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalPerfectHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		group_chunk.data[group_idx].Reference(chunk.data[bound_ref_expr.Index()]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.GetChildren()) {
			D_ASSERT(child_expr->GetExpressionType() == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.Index()]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.GetFilter()) {
			auto it = filter_indexes.find(*aggr.GetFilter());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}

	group_chunk.Verify(context.client.db);
	aggregate_input_chunk.Verify(context.client.db);
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

bool PhysicalPerfectHashAggregate::BindExecutionSink(ExecutionContext &context, DataChunk &input,
                                                     OperatorSinkInput &sink_input,
                                                     const ExecutionRegionSinkInfo &sink_info,
                                                     ExecutionSinkBinding &binding) const {
	(void)input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	auto blocker = ValidatePerfectHashAggregateExecutionSink(sink_info);
	if (!blocker.empty()) {
		binding.blocker = blocker;
		binding.aggregate_update.blocker = blocker;
		return false;
	}

	auto &local_state = sink_input.local_state.Cast<PerfectHashAggregateLocalState>();
	binding.ready = true;
	binding.aggregate_update.ready = true;
	binding.aggregate_update.state =
	    make_shared_ptr<PerfectHashAggregateExecutionRegionSinkState>(context, *this, local_state);
	binding.aggregate_update.grouped_state.ready = true;
	binding.aggregate_update.grouped_state.state =
	    make_shared_ptr<PerfectHashAggregateStateAddressState>(context, *this, local_state);
	binding.aggregate_update.grouped_state.aggregate_state_offsets = sink_info.aggregate_contract.grouped_state_offsets;
	auto state_layout = local_state.ht->GetStateLayout();
	binding.aggregate_update.grouped_state.perfect_hash_layout.ready = state_layout.data && state_layout.group_is_set;
	binding.aggregate_update.grouped_state.perfect_hash_layout.data = state_layout.data;
	binding.aggregate_update.grouped_state.perfect_hash_layout.group_is_set = state_layout.group_is_set;
	binding.aggregate_update.grouped_state.perfect_hash_layout.total_groups = state_layout.total_groups;
	binding.aggregate_update.grouped_state.perfect_hash_layout.tuple_size = state_layout.tuple_size;
	binding.aggregate_update.grouped_state.perfect_hash_layout.aggregate_state_offset =
	    state_layout.aggregate_state_offset;
	binding.aggregate_update.grouped_state.perfect_hash_layout.blocker =
	    binding.aggregate_update.grouped_state.perfect_hash_layout.ready ? "none" : "perfect-hash-state-layout-missing";
	binding.aggregate_update.grouped_state.blocker.clear();
	binding.aggregate_update.primitive = BuildPerfectHashAggregatePrimitiveUpdateBinding(*this, sink_info);
	binding.aggregate_update.blocker.clear();
	binding.blocker.clear();
	return true;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalPerfectHashAggregate::Combine(ExecutionContext &context,
                                                            OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	auto &gstate = input.global_state.Cast<PerfectHashAggregateGlobalState>();

	lock_guard<mutex> l(gstate.lock);
	gstate.ht->Combine(*lstate.ht);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PerfectHashAggregateState : public GlobalSourceState {
public:
	PerfectHashAggregateState() : ht_scan_position(0) {
	}

	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

static SourceResultType ScanPerfectHashAggregateState(DataChunk &chunk, OperatorSourceInput &input,
                                                      const PhysicalPerfectHashAggregate &op) {
	auto &state = input.global_state.Cast<PerfectHashAggregateState>();
	auto &gstate = op.sink_state->Cast<PerfectHashAggregateGlobalState>();

	{
		ExecutionOperatorStageTimer timer(input.stage_recorder,
		                                  "source_contract.perfect_hash_aggregate_state_scan.scan");
		gstate.ht->Scan(state.ht_scan_position, chunk);
	}

	return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

unique_ptr<GlobalSourceState> PhysicalPerfectHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PerfectHashAggregateState>();
}

bool PhysicalPerfectHashAggregate::SupportsExecutionSourceContract(
    const ExecutionRegionOpenRequest &open_request) const {
	return open_request.UsesSourceContract();
}

SourceResultType PhysicalPerfectHashAggregate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                               OperatorSourceInput &input) const {
	(void)context;
	return ScanPerfectHashAggregateState(chunk, input, *this);
}

SourceResultType
PhysicalPerfectHashAggregate::GetExecutionSourceContractDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                     OperatorSourceInput &input) const {
	(void)context;
	return ScanPerfectHashAggregateState(chunk, input, *this);
}

InsertionOrderPreservingMap<string> PhysicalPerfectHashAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;

	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (aggregate.GetFilter()) {
			aggregate_info += " Filter: " + aggregate.GetFilter()->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
