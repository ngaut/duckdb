#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

PhysicalBatchCollector::PhysicalBatchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalResultCollector(physical_plan, data) {
}

ExecutionContract PhysicalBatchCollector::GetExecutionContract() const {
	return BuildExecutionResultCollectorSinkContract();
}

class BatchCollectorExecutionRegionSinkState : public ExecutionAppendSinkState {
public:
	explicit BatchCollectorExecutionRegionSinkState(BatchCollectorLocalState &state_p) : state(state_p) {
	}

	SinkResultType Append(DataChunk &input) override {
		state.data.Append(input, state.partition_info.batch_index.GetIndex());
		return SinkResultType::NEED_MORE_INPUT;
	}

private:
	BatchCollectorLocalState &state;
};

bool PhysicalBatchCollector::BindExecutionSink(ExecutionContext &context, DataChunk &input,
                                               OperatorSinkInput &sink_input, const ExecutionRegionSinkInfo &sink_info,
                                               ExecutionSinkBinding &binding) const {
	(void)context;
	(void)input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	if (sink_info.kind != ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
	    sink_info.native_sink_contract.status != ExecutionRegionStateContractStatus::READY) {
		binding.blocker = sink_info.native_sink_contract.blocker.empty() ? "result-collector-sink-contract-not-ready"
		                                                                 : sink_info.native_sink_contract.blocker;
		return false;
	}
	auto &state = sink_input.local_state.Cast<BatchCollectorLocalState>();
	binding.ready = true;
	binding.append_sink.ready = true;
	binding.append_sink.state = make_shared_ptr<BatchCollectorExecutionRegionSinkState>(state);
	binding.append_sink.blocker.clear();
	binding.blocker.clear();
	return true;
}

SinkResultType PhysicalBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BatchCollectorLocalState>();
	state.data.Append(chunk, state.partition_info.batch_index.GetIndex());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBatchCollector::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<BatchCollectorGlobalState>();
	auto &state = input.local_state.Cast<BatchCollectorLocalState>();

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BatchCollectorGlobalState>();
	auto collection = gstate.data.FetchCollection();
	D_ASSERT(collection);
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, IdentifiersToStrings(names),
	                                                 std::move(collection), context.GetClientProperties());
	gstate.result = std::move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchCollectorLocalState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BatchCollectorGlobalState>(context, *this);
}

unique_ptr<QueryResult> PhysicalBatchCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BatchCollectorGlobalState>();
	D_ASSERT(gstate.result);
	return std::move(gstate.result);
}

} // namespace duckdb
