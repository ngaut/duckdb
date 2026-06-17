#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sorting/sort.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<BoundOrderByNode> orders,
                             vector<idx_t> projections, idx_t estimated_cardinality, bool is_index_sort_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::ORDER_BY, std::move(types), estimated_cardinality),
      orders(std::move(orders)), projections(std::move(projections)), is_index_sort(is_index_sort_p) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalSinkState : public GlobalSinkState {
public:
	OrderGlobalSinkState(const PhysicalOrder &op, ClientContext &context)
	    : sort(context, op.orders, op.children[0].get().types, op.projections, op.is_index_sort),
	      state(sort.GetGlobalSinkState(context)) {
	}

public:
	Sort sort;
	unique_ptr<GlobalSinkState> state;
};

class OrderLocalSinkState : public LocalSinkState {
public:
	OrderLocalSinkState() {
	}

public:
	unique_ptr<LocalSinkState> state;
};

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &) const {
	return make_uniq<OrderLocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOrder::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<OrderGlobalSinkState>(*this, context);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();
	if (!lstate.state) {
		lstate.state = gstate.sort.GetLocalSinkState(context);
	}
	OperatorSinkInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.Sink(context, chunk, sort_input);
}

class OrderExecutionRegionSinkState : public ExecutionOrderedSinkState {
public:
	OrderExecutionRegionSinkState(ExecutionContext &context_p, Sort &sort_p, GlobalSinkState &global_state_p,
	                              LocalSinkState &local_state_p, InterruptState &interrupt_state_p)
	    : context(context_p), sort(sort_p), global_state(global_state_p), local_state(local_state_p),
	      interrupt_state(interrupt_state_p) {
	}

	SinkResultType Sink(DataChunk &order_keys, DataChunk &payload) override {
		OperatorSinkInput sort_input {global_state, local_state, interrupt_state};
		return sort.SinkPreparedOrderKeys(context, order_keys, payload, sort_input);
	}

private:
	ExecutionContext &context;
	Sort &sort;
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

static string ValidateOrderedExecutionSink(const ExecutionRegionSinkInfo &sink_info,
                                           ExecutionRegionOperatorKind expected_kind) {
	if (sink_info.kind != ExecutionRegionSinkKind::SORT) {
		return "ordered-sink-runtime-kind-mismatch";
	}
	if (sink_info.native_sink_contract.status != ExecutionRegionStateContractStatus::READY) {
		return sink_info.native_sink_contract.blocker.empty() ? "ordered-sink-contract-not-ready"
		                                                      : sink_info.native_sink_contract.blocker;
	}
	if (!sink_info.order_contract.present) {
		return "ordered-sink-runtime-missing-order-contract";
	}
	if (sink_info.order_contract.kind != expected_kind) {
		return "ordered-sink-runtime-operator-kind-mismatch";
	}
	if (!sink_info.order_contract.all_order_keys_ready) {
		return sink_info.order_contract.order_key_blocker.empty() ? "ordered-sink-runtime-order-key-not-ready"
		                                                          : sink_info.order_contract.order_key_blocker;
	}
	return "none";
}

SinkResultType ExecutionSinkOrdered(const ExecutionOrderedSinkBinding &binding, DataChunk &order_keys,
                                    DataChunk &payload) {
	if (!binding.ready || !binding.state) {
		throw InternalException("Execution region ordered sink binding is incomplete");
	}
	if (order_keys.ColumnCount() != binding.order_key_types.size()) {
		throw InternalException("Execution region ordered sink order-key count mismatch");
	}
	if (payload.ColumnCount() != binding.payload_types.size()) {
		throw InternalException("Execution region ordered sink payload column count mismatch");
	}
	return binding.state->Sink(order_keys, payload);
}

bool PhysicalOrder::BindExecutionSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
                                      const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) const {
	(void)input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	auto blocker = ValidateOrderedExecutionSink(sink_info, ExecutionRegionOperatorKind::ORDER_BY);
	if (blocker != "none") {
		binding.blocker = blocker;
		binding.ordered_sink.blocker = blocker;
		return false;
	}

	auto &gstate = sink_input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = sink_input.local_state.Cast<OrderLocalSinkState>();
	if (!lstate.state) {
		lstate.state = gstate.sort.GetLocalSinkState(context);
	}
	binding.ready = true;
	binding.ordered_sink.ready = true;
	binding.ordered_sink.state = make_shared_ptr<OrderExecutionRegionSinkState>(
	    context, gstate.sort, *gstate.state, *lstate.state, sink_input.interrupt_state);
	binding.ordered_sink.order_key_types.reserve(sink_info.order_contract.order_keys.size());
	for (auto &key : sink_info.order_contract.order_keys) {
		binding.ordered_sink.order_key_types.push_back(key.type);
	}
	binding.ordered_sink.payload_types = sink_info.order_contract.payload_types;
	binding.ordered_sink.blocker = "none";
	binding.blocker = "none";
	return true;
}

SinkCombineResultType PhysicalOrder::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();
	if (!lstate.state) {
		return SinkCombineResultType::FINISHED;
	}
	OperatorSinkCombineInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.Combine(context, sort_input);
}

SinkFinalizeType PhysicalOrder::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	OperatorSinkFinalizeInput sort_input {*gstate.state, input.interrupt_state};
	return gstate.sort.Finalize(context, sort_input);
}

ProgressData PhysicalOrder::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate_p,
                                            const ProgressData source_progress) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSinkState>();
	return gstate.sort.GetSinkProgress(context, *gstate.state, source_progress);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class OrderGlobalSourceState : public GlobalSourceState {
public:
	explicit OrderGlobalSourceState(ClientContext &context, OrderGlobalSinkState &sink)
	    : sort(sink.sort), state(sort.GetGlobalSourceState(context, *sink.state)) {
	}

public:
	idx_t MaxThreads() override {
		return state->MaxThreads();
	}

public:
	Sort &sort;
	unique_ptr<GlobalSourceState> state;
};

class OrderLocalSourceState : public LocalSourceState {
public:
	explicit OrderLocalSourceState(ExecutionContext &context, OrderGlobalSourceState &gstate)
	    : state(gstate.sort.GetLocalSourceState(context, *gstate.state)) {
	}

public:
	unique_ptr<LocalSourceState> state;
};

unique_ptr<LocalSourceState> PhysicalOrder::GetLocalSourceState(ExecutionContext &context,
                                                                GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	return make_uniq<OrderLocalSourceState>(context, gstate);
}

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<OrderGlobalSourceState>(context, sink_state->Cast<OrderGlobalSinkState>());
}

bool PhysicalOrder::SupportsExecutionSourceContract(const ExecutionRegionOpenRequest &open_request) const {
	return open_request.UsesSourceContract();
}

SourceResultType PhysicalOrder::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSourceState>();
	auto &lstate = input.local_state.Cast<OrderLocalSourceState>();
	OperatorSourceInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.GetData(context, chunk, sort_input);
}

SourceResultType PhysicalOrder::GetExecutionSourceContractDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                       OperatorSourceInput &input) const {
	return GetDataInternal(context, chunk, input);
}

OperatorPartitionData PhysicalOrder::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                      GlobalSourceState &gstate_p, LocalSourceState &lstate_p,
                                                      const OperatorPartitionInfo &partition_info) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	auto &lstate = lstate_p.Cast<OrderLocalSourceState>();
	if (partition_info.RequiresPartitionColumns()) {
		throw InternalException("PhysicalOrder::GetPartitionData: partition columns not supported");
	}
	return gstate.sort.GetPartitionData(context, chunk, *gstate.state, *lstate.state, partition_info);
}

ProgressData PhysicalOrder::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	return gstate.sort.GetProgress(context, *gstate.state);
}

InsertionOrderPreservingMap<string> PhysicalOrder::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->ToString() + " ";
		orders_info += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	result["__order_by__"] = orders_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
