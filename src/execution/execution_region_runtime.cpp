#include "duckdb/execution/execution_region_runtime.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

const ExecutionPrimitiveAggregateUpdateLane *
ExecutionPrimitiveAggregateUpdateBinding::FindLane(idx_t aggregate_index) const {
	for (auto &lane : lanes) {
		if (lane.aggregate_index == aggregate_index) {
			return &lane;
		}
	}
	return nullptr;
}

idx_t ExecutionAggregateUpdateState::FindOrCreateAggregateStates(DataChunk &input,
                                                                 const vector<idx_t> &group_input_indices,
                                                                 Vector &addresses_out) {
	(void)input;
	(void)group_input_indices;
	(void)addresses_out;
	throw InternalException("execution aggregate update state does not expose grouped state addresses");
}

void ExecutionAggregateUpdateState::FinishNativeAggregateUpdate() {
	throw InternalException("execution aggregate update state does not expose native aggregate finalization");
}

SinkResultType ExecutionSinkAppend(const ExecutionAppendSinkBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution append sink binding is incomplete");
	}
	return binding.state->Append(input);
}

SinkResultType ExecutionSinkDelimJoin(const ExecutionDelimJoinSinkBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution delimiter join sink binding is incomplete");
	}
	return binding.state->Sink(input);
}

SinkResultType ExecutionSinkAggregateUpdate(const ExecutionAggregateUpdateBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution aggregate update binding is incomplete");
	}
	return binding.state->Sink(input);
}

idx_t ExecutionFindOrCreateAggregateStates(const ExecutionAggregateUpdateBinding &binding,
                                           DataChunk &input,
                                           const vector<idx_t> &group_input_indices,
                                           Vector &addresses_out) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution aggregate update binding is incomplete");
	}
	return binding.state->FindOrCreateAggregateStates(input, group_input_indices, addresses_out);
}

void ExecutionFinishAggregateUpdate(const ExecutionAggregateUpdateBinding &binding) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution aggregate update binding is incomplete");
	}
	binding.state->FinishNativeAggregateUpdate();
}

OperatorResultType ExecutionOperatorProject(const ExecutionProjectionBinding &binding, DataChunk &input,
                                            DataChunk &output) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution projection operator binding is incomplete");
	}
	return binding.state->Project(input, output);
}

ExecutionOperatorRuntime::~ExecutionOperatorRuntime() {
}

ExecutionRegionRuntime::~ExecutionRegionRuntime() {
}

} // namespace duckdb
