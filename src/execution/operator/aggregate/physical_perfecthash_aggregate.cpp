#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"

#include "duckdb/execution/jit/aggregate_runtime.hpp"
#include "duckdb/execution/jit/runtime.hpp"
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

static void
ValidateJitNativePerfectHashAggregateGroupBindings(const PhysicalPerfectHashAggregate &op,
                                                   const vector<JitGroupedAggregateGroupBinding> &group_bindings) {
	if (group_bindings.size() != op.groups.size()) {
		throw InternalException("JIT native perfect hash aggregate group binding count %llu does not match group count %llu",
		                        static_cast<unsigned long long>(group_bindings.size()),
		                        static_cast<unsigned long long>(op.groups.size()));
	}
	for (idx_t binding_idx = 0; binding_idx < group_bindings.size(); binding_idx++) {
		auto &binding = group_bindings[binding_idx];
		if (binding.group_index != binding_idx) {
			throw InternalException("JIT native perfect hash aggregate requires dense group bindings");
		}
	}
}

static void
ValidateJitNativePerfectHashAggregateStateRequests(const PhysicalPerfectHashAggregate &op,
                                                   const vector<JitNativeGroupedAggregateStateRequest> &requests) {
	if (requests.size() != op.aggregates.size()) {
		throw InternalException(
		    "JIT native perfect hash aggregate update request count %llu does not match aggregate count %llu",
		    static_cast<unsigned long long>(requests.size()),
		    static_cast<unsigned long long>(op.aggregates.size()));
	}
	if (!op.filter_indexes.empty()) {
		throw InternalException("JIT native perfect hash aggregate update does not support aggregate filters");
	}
	for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
		auto &request = requests[request_idx];
		if (request.aggregate_index != request_idx) {
			throw InternalException("JIT native perfect hash aggregate update requires dense aggregate state requests");
		}
		if (request.aggregate_index >= op.aggregates.size()) {
			throw InternalException(
			    "JIT native perfect hash aggregate request references aggregate index %llu beyond %llu",
			    static_cast<unsigned long long>(request.aggregate_index),
			    static_cast<unsigned long long>(op.aggregates.size()));
		}
		auto &aggregate = op.aggregates[request.aggregate_index]->Cast<BoundAggregateExpression>();
		if (aggregate.GetFilter()) {
			throw InternalException("JIT native perfect hash aggregate update does not support aggregate filters");
		}
	}
}

static void ValidateJitNativePerfectHashAggregateShape(
    const PhysicalPerfectHashAggregate &op, const vector<JitGroupedAggregateGroupBinding> &group_bindings,
    const vector<JitNativeGroupedAggregateStateRequest> &requests) {
	ValidateJitNativePerfectHashAggregateGroupBindings(op, group_bindings);
	ValidateJitNativePerfectHashAggregateStateRequests(op, requests);
}

static void BindJitNativePerfectHashAggregateStateLayout(
    const PerfectAggregateHashTable &ht, const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
    JitNativeGroupedAggregateStateSet &bound_states) {
	auto &layout = ht.GetLayout();
	auto &offsets = layout.GetOffsets();
	if (offsets.size() < requested_states.size()) {
		throw InternalException("JIT native perfect hash aggregate update cannot bind grouped aggregate state offsets");
	}
	const auto base_offset = layout.GetAggrOffset();
	bound_states.states.clear();
	bound_states.states.reserve(requested_states.size());
	for (auto &request : requested_states) {
		auto state_offset = offsets[request.aggregate_index];
		if (state_offset < base_offset) {
			throw InternalException("JIT native perfect hash aggregate state offset is before aggregate payload area");
		}
		JitNativeGroupedAggregateState state;
		state.aggregate_index = request.aggregate_index;
		state.update_kind = request.update_kind;
		state.aggregate_state_offset = state_offset - base_offset;
		bound_states.states.push_back(state);
	}
}

void JitBindNativePerfectHashAggregateStates(
    ExecutionContext &context, OperatorSinkInput &input, DataChunk &payload_chunk,
    const vector<JitGroupedAggregateGroupBinding> &group_bindings,
    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
    JitNativeGroupedAggregateStateSet &bound_states) {
	auto &local_state = input.local_state.Cast<PerfectHashAggregateLocalState>();
	auto &op = local_state.op;
	ValidateJitNativePerfectHashAggregateShape(op, group_bindings, requested_states);

	auto &group_chunk = local_state.group_chunk;
	group_chunk.Reset();
	for (idx_t binding_idx = 0; binding_idx < group_bindings.size(); binding_idx++) {
		auto &binding = group_bindings[binding_idx];
		if (binding.input_index >= payload_chunk.ColumnCount()) {
			throw InternalException(
			    "JIT native perfect hash aggregate group binding references input column %llu beyond %llu",
			    static_cast<unsigned long long>(binding.input_index),
			    static_cast<unsigned long long>(payload_chunk.ColumnCount()));
		}
		group_chunk.data[binding.group_index].Reference(payload_chunk.data[binding.input_index]);
	}
	group_chunk.SetChildCardinality(payload_chunk.size());
	group_chunk.Verify(context.client.db);

	local_state.ht->FindOrCreateAggregateStates(group_chunk, bound_states.aggregate_addresses);
	BindJitNativePerfectHashAggregateStateLayout(*local_state.ht, requested_states, bound_states);
	bound_states.count = payload_chunk.size();
}

void JitBindNativePerfectHashAggregateStateLayout(
    ExecutionContext &context, OperatorSinkInput &input,
    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
    JitNativeGroupedAggregateStateSet &bound_states, JitNativePerfectHashAggregateStateLayout &state_layout) {
	(void)context;
	auto &local_state = input.local_state.Cast<PerfectHashAggregateLocalState>();
	auto &op = local_state.op;
	ValidateJitNativePerfectHashAggregateStateRequests(op, requested_states);
	BindJitNativePerfectHashAggregateStateLayout(*local_state.ht, requested_states, bound_states);
	auto layout = local_state.ht->GetStateLayout();
	state_layout.data = layout.data;
	state_layout.group_is_set = layout.group_is_set;
	state_layout.total_groups = layout.total_groups;
	state_layout.tuple_size = layout.tuple_size;
	state_layout.aggregate_state_offset = layout.aggregate_state_offset;
	bound_states.count = 0;
}

SinkResultType JitFinishNativePerfectHashAggregateUpdate(ExecutionContext &context, OperatorSinkInput &input,
                                                         idx_t count) {
	(void)context;
	(void)input;
	(void)count;
	return SinkResultType::NEED_MORE_INPUT;
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

bool PhysicalPerfectHashAggregate::SupportsJitNativeSource(const JitPreparedPipeline &jit_prepared_pipeline) const {
	return jit_prepared_pipeline.RequiresNativeSource();
}

SourceResultType PhysicalPerfectHashAggregate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                               OperatorSourceInput &input) const {
	(void)context;
	return ScanPerfectHashAggregateState(chunk, input, *this);
}

SourceResultType PhysicalPerfectHashAggregate::GetJitNativeSourceDataInternal(ExecutionContext &context, DataChunk &chunk,
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
