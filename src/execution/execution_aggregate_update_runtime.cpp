#include "duckdb/execution/execution_operator_runtime.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

namespace {

class ExecutionOperatorSinkAggregateUpdateState : public ExecutionAggregateUpdateState {
public:
	ExecutionOperatorSinkAggregateUpdateState(ExecutionContext &context_p, const PhysicalOperator &op_p,
	                                          OperatorSinkInput &sink_input)
	    : context(context_p), op(op_p), global_state(sink_input.global_state), local_state(sink_input.local_state),
	      interrupt_state(sink_input.interrupt_state) {
	}

	SinkResultType Sink(DataChunk &input) override {
		OperatorSinkInput sink_input {global_state, local_state, interrupt_state};
		return op.Sink(context, input, sink_input);
	}

private:
	ExecutionContext &context;
	const PhysicalOperator &op;
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

} // namespace

shared_ptr<ExecutionAggregateUpdateState> ExecutionOperatorSinkAggregateUpdate(ExecutionContext &context,
                                                                               const PhysicalOperator &op,
                                                                               OperatorSinkInput &sink_input) {
	return make_shared_ptr<ExecutionOperatorSinkAggregateUpdateState>(context, op, sink_input);
}

bool ExecutionBindPrimitiveAggregateUpdateLane(const BoundAggregateExpression &aggregate,
                                               const ExecutionRegionAggregateInput &info,
                                               ExecutionPrimitiveAggregateUpdateLane &lane, string &blocker) {
	auto &function = aggregate.Function();
	if (!function.HasPrimitiveUpdateABI()) {
		blocker = "primitive-update-abi-missing";
		return false;
	}
	auto &abi = function.GetPrimitiveUpdateABI();
	const auto kind = info.primitive_update_kind;
	if (!AggregatePrimitiveUpdateKindIsSupported(kind)) {
		blocker = "primitive-update-kind-unsupported";
		return false;
	}
	// The contract's update may differ from the function's own (a filtered count(*) counts its CASE payload); the
	// state it writes must be the function's
	if (AggregatePrimitiveUpdateStateValueSize(kind) != AggregatePrimitiveUpdateStateValueSize(abi.kind) ||
	    AggregatePrimitiveUpdateHasStateIsSet(kind) != AggregatePrimitiveUpdateHasStateIsSet(abi.kind) ||
	    AggregatePrimitiveUpdateUsesInt64State(kind) != AggregatePrimitiveUpdateUsesInt64State(abi.kind)) {
		blocker = "primitive-update-state-kind-mismatch";
		return false;
	}
	const auto requires_payload = AggregatePrimitiveUpdateRequiresPayload(kind);
	if (requires_payload) {
		if (info.child_count != 1 || info.child_types.empty()) {
			blocker = "primitive-update-requires-one-payload";
			return false;
		}
		if (AggregatePrimitiveUpdateRequiresTypedPayload(kind) &&
		    info.child_types[0].InternalType() != abi.input_type) {
			blocker = "primitive-update-payload-type-mismatch";
			return false;
		}
	} else if (info.child_count != 0) {
		blocker = "primitive-update-requires-no-payload";
		return false;
	}
	AggregateStateInput state_input(function, aggregate.BindInfo().get());
	if (function.HasStateSizeCallback() && function.GetStateSizeCallback()(state_input) != abi.state_size) {
		blocker = "primitive-update-state-size-mismatch";
		return false;
	}
	auto value_size = AggregatePrimitiveUpdateStateValueSize(kind);
	if (value_size == 0 || abi.state_value_offset + value_size > abi.state_size ||
	    (AggregatePrimitiveUpdateHasStateIsSet(kind) && abi.state_is_set_offset + sizeof(bool) > abi.state_size)) {
		blocker = "primitive-update-state-layout-invalid";
		return false;
	}
	lane = ExecutionPrimitiveAggregateUpdateLane();
	lane.ready = true;
	lane.kind = kind;
	lane.aggregate_index = info.aggregate_index;
	lane.payload_type = requires_payload ? info.child_types[0].InternalType() : PhysicalType::INVALID;
	lane.state_size = abi.state_size;
	lane.state_value_offset = abi.state_value_offset;
	lane.state_is_set_offset = abi.state_is_set_offset;
	return true;
}

} // namespace duckdb
