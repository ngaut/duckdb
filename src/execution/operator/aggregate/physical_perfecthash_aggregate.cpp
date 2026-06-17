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

	idx_t FindOrCreateAggregateStates(DataChunk &input, const vector<idx_t> &group_input_indices,
	                                  Vector &addresses_out) override {
		if (group_input_indices.size() != op.groups.size()) {
			throw InternalException("execution region perfect hash aggregate state lookup group binding mismatch");
		}
		DataChunk &group_chunk = local_state.group_chunk;
		group_chunk.Reset();
		for (idx_t group_idx = 0; group_idx < group_input_indices.size(); group_idx++) {
			auto input_idx = group_input_indices[group_idx];
			if (input_idx >= input.ColumnCount()) {
				throw InternalException("execution region perfect hash aggregate state lookup group index out of range");
			}
			group_chunk.data[group_idx].Reference(input.data[input_idx]);
		}
		group_chunk.SetChildCardinality(input.size());
		group_chunk.Verify(context.client.db);
		return local_state.ht->FindOrCreateAggregateStates(group_chunk, addresses_out);
	}

	void FinishNativeAggregateUpdate() override {
	}

private:
	ExecutionContext &context;
	const PhysicalPerfectHashAggregate &op;
	PerfectHashAggregateLocalState &local_state;
};

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
	if (contract.distinct_aggregate_count != 0 || contract.aggregate_filter_count != 0 ||
	    contract.aggregate_order_count != 0) {
		return "perfect-hash-aggregate-state-update-unsupported-aggregate-semantics";
	}
	if (contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY) {
		return contract.native_grouped_state_contract.blocker.empty() ? "perfect-hash-aggregate-grouped-state-not-ready"
		                                                              : contract.native_grouped_state_contract.blocker;
	}
	for (auto &group : sink_info.groups) {
		if (!group.supported_reference) {
			return group.reason.empty() ? "perfect-hash-aggregate-state-update-group-reference" : group.reason;
		}
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (!aggregate.reason.empty()) {
			return aggregate.reason;
		}
		if (aggregate.distinct || aggregate.has_filter || aggregate.has_order_bys || aggregate.order_dependent) {
			return "perfect-hash-aggregate-state-update-unsupported-aggregate-semantics";
		}
		if (!aggregate.has_state_update || !aggregate.payload_expressions_ready ||
		    !aggregate.supported_payload_references) {
			return "perfect-hash-aggregate-state-update-payload-contract";
		}
	}
	return "none";
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
	if (blocker != "none") {
		binding.blocker = blocker;
		binding.aggregate_update.blocker = blocker;
		return false;
	}

	auto &local_state = sink_input.local_state.Cast<PerfectHashAggregateLocalState>();
	binding.ready = true;
	binding.aggregate_update.ready = true;
	binding.aggregate_update.state =
	    make_shared_ptr<PerfectHashAggregateExecutionRegionSinkState>(context, *this, local_state);
	binding.aggregate_update.blocker = "none";
	binding.blocker = "none";
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

	gstate.ht->Scan(state.ht_scan_position, chunk);

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
