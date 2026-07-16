#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

PhysicalBufferedBatchCollector::PhysicalBufferedBatchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalResultCollector(physical_plan, data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedBatchCollectorGlobalState : public GlobalSinkState {
public:
	weak_ptr<ClientContext> context;
	shared_ptr<BufferedData> buffered_data;
};

BufferedBatchCollectorLocalState::BufferedBatchCollectorLocalState() {
}

ExecutionContract PhysicalBufferedBatchCollector::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                                       bool render_diagnostics) const {
	return BuildExecutionResultCollectorSinkContract();
}

class BufferedBatchCollectorExecutionRegionSinkState : public ExecutionAppendSinkState {
public:
	BufferedBatchCollectorExecutionRegionSinkState(BufferedBatchCollectorGlobalState &global_state_p,
	                                               BufferedBatchCollectorLocalState &local_state_p,
	                                               InterruptState &interrupt_state_p)
	    : global_state(global_state_p), local_state(local_state_p), interrupt_state(interrupt_state_p) {
	}

	SinkResultType Append(DataChunk &input) override {
		local_state.current_batch = local_state.partition_info.batch_index.GetIndex();
		auto batch = local_state.partition_info.batch_index.GetIndex();
		auto min_batch_index = local_state.partition_info.min_batch_index.GetIndex();
		auto &buffered_data = global_state.buffered_data->Cast<BatchedBufferedData>();
		buffered_data.UpdateMinBatchIndex(min_batch_index);
		if (buffered_data.ShouldBlockBatch(batch)) {
			buffered_data.BlockSink(interrupt_state, batch);
			return SinkResultType::BLOCKED;
		}
		buffered_data.Append(input, batch);
		return SinkResultType::NEED_MORE_INPUT;
	}

private:
	BufferedBatchCollectorGlobalState &global_state;
	BufferedBatchCollectorLocalState &local_state;
	InterruptState &interrupt_state;
};

bool PhysicalBufferedBatchCollector::BindExecutionSink(ExecutionContext &context, DataChunk &input,
                                                       OperatorSinkInput &sink_input,
                                                       const ExecutionRegionSinkInfo &sink_info,
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
	auto &global_state = sink_input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &local_state = sink_input.local_state.Cast<BufferedBatchCollectorLocalState>();
	binding.ready = true;
	binding.append_sink.ready = true;
	binding.append_sink.state = make_shared_ptr<BufferedBatchCollectorExecutionRegionSinkState>(
	    global_state, local_state, sink_input.interrupt_state);
	binding.append_sink.blocker.clear();
	binding.blocker.clear();
	return true;
}

SinkResultType PhysicalBufferedBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
	auto batch = lstate.partition_info.batch_index.GetIndex();
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();

	auto &buffered_data = gstate.buffered_data->Cast<BatchedBufferedData>();
	buffered_data.UpdateMinBatchIndex(min_batch_index);

	if (buffered_data.ShouldBlockBatch(batch)) {
		auto callback_state = input.interrupt_state;
		buffered_data.BlockSink(callback_state, batch);
		return SinkResultType::BLOCKED;
	}

	// FIXME: if we want to make this more accurate, we should grab a reservation on the buffer space
	// while we're unlocked some other thread could also append, causing us to potentially cross our buffer size

	buffered_data.Append(chunk, batch);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkNextBatchType PhysicalBufferedBatchCollector::NextBatch(ExecutionContext &context,
                                                            OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	auto batch = lstate.current_batch;
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto new_index = lstate.partition_info.batch_index.GetIndex();

	auto &buffered_data = gstate.buffered_data->Cast<BatchedBufferedData>();
	buffered_data.CompleteBatch(batch);
	lstate.current_batch = new_index;
	// FIXME: this can move from the buffer to the read queue, increasing the 'read_queue_byte_count'
	// We might want to block here if 'read_queue_byte_count' has already reached the ReadQueueCapacity()
	// So we don't completely disregard the 'streaming_buffer_size' that was set
	buffered_data.UpdateMinBatchIndex(min_batch_index);
	return SinkNextBatchType::READY;
}

SinkCombineResultType PhysicalBufferedBatchCollector::Combine(ExecutionContext &context,
                                                              OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto &buffered_data = gstate.buffered_data->Cast<BatchedBufferedData>();

	// FIXME: this can move from the buffer to the read queue, increasing the 'read_queue_byte_count'
	// We might want to block here if 'read_queue_byte_count' has already reached the ReadQueueCapacity()
	// So we don't completely disregard the 'streaming_buffer_size' that was set
	buffered_data.UpdateMinBatchIndex(min_batch_index);
	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalBufferedBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<BufferedBatchCollectorLocalState>();
	return std::move(state);
}

unique_ptr<GlobalSinkState> PhysicalBufferedBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<BufferedBatchCollectorGlobalState>();
	state->context = context.shared_from_this();
	state->buffered_data = make_shared_ptr<BatchedBufferedData>(context);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalBufferedBatchCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BufferedBatchCollectorGlobalState>();
	auto cc = gstate.context.lock();
	auto result = make_uniq<StreamQueryResult>(statement_type, properties, types, IdentifiersToStrings(names),
	                                           cc->GetClientProperties(), gstate.buffered_data);
	return std::move(result);
}

} // namespace duckdb
