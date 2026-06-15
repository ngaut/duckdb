#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

PhysicalBufferedCollector::PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                     bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	//! This is weak to avoid creating a cyclical reference
	weak_ptr<ClientContext> context;
	shared_ptr<BufferedData> buffered_data;
};

class BufferedCollectorLocalState : public LocalSinkState {};

JitOperatorDescriptor PhysicalBufferedCollector::GetJitOperatorDescriptor() const {
	return BuildJitResultCollectorAppendDescriptor();
}

bool PhysicalBufferedCollector::BindJitNativeSink(ExecutionContext &context, DataChunk &input,
                                                  OperatorSinkInput &sink_input,
                                                  const JitRegionSinkInfo &sink_info,
                                                  JitNativeSinkBinding &binding) const {
	(void)context;
	(void)input;
	binding = JitNativeSinkBinding();
	binding.kind = sink_info.kind;
	if (sink_info.kind != JitRegionSinkKind::RESULT_COLLECTOR_APPEND) {
		binding.blocker = "result-collector-native-runtime-kind-mismatch";
		return false;
	}
	auto &gstate = sink_input.global_state.Cast<BufferedCollectorGlobalState>();
	binding.ready = true;
	binding.result_collector_append.ready = true;
	binding.result_collector_append.kind = JitNativeResultCollectorAppendKind::SIMPLE_BUFFERED_DATA;
	binding.result_collector_append.simple_buffered_data = &gstate.buffered_data->Cast<SimpleBufferedData>();
	binding.result_collector_append.interrupt_state = &sink_input.interrupt_state;
	binding.result_collector_append.blocker = "none";
	binding.blocker = "none";
	return true;
}

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedCollectorLocalState>();
	(void)lstate;

	lock_guard<mutex> l(gstate.glock);
	auto &buffered_data = gstate.buffered_data->Cast<SimpleBufferedData>();

	if (buffered_data.BufferIsFull()) {
		auto callback_state = input.interrupt_state;
		buffered_data.BlockSink(callback_state);
		return SinkResultType::BLOCKED;
	}
	buffered_data.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBufferedCollector::Combine(ExecutionContext &context,
                                                         OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalBufferedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<BufferedCollectorGlobalState>();
	state->context = context.shared_from_this();
	state->buffered_data = make_shared_ptr<SimpleBufferedData>(context);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalBufferedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<BufferedCollectorLocalState>();
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalBufferedCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BufferedCollectorGlobalState>();
	lock_guard<mutex> l(gstate.glock);
	// FIXME: maybe we want to check if the execution was successful before creating the StreamQueryResult ?
	auto cc = gstate.context.lock();
	auto result = make_uniq<StreamQueryResult>(statement_type, properties, types, IdentifiersToStrings(names),
	                                           cc->GetClientProperties(), gstate.buffered_data);
	return std::move(result);
}

bool PhysicalBufferedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalBufferedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
