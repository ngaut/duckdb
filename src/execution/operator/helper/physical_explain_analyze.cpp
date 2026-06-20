#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class ExplainAnalyzeStateGlobalState : public GlobalSinkState {
public:
	string analyzed_plan;
};

ExecutionContract PhysicalExplainAnalyze::GetExecutionContract() const {
	ExecutionContract result;
	result.sink.kind = ExecutionRegionSinkKind::MATERIALIZATION;
	result.sink.reason = "DuckDB explain analyze materialization sink contract";
	result.sink.reason += ";operator=EXPLAIN_ANALYZE";
	result.sink.reason += ";input_columns=" + std::to_string(children.empty() ? 0 : children[0].get().types.size());
	result.sink.native_sink_contract.status = ExecutionRegionStateContractStatus::READY;
	result.sink.native_sink_contract.required_capability = "materialization-append-sink";
	result.sink.native_sink_contract.contract_version = "v1";
	result.sink.native_sink_contract.blocker.clear();
	result.sink.reason += ";sink_contract_status=ready";
	result.sink.reason += ";sink_required_capability=materialization-append-sink";
	result.sink.reason += ";sink_contract_version=v1";
	result.sink.reason += ";sink_contract_blocker=none";
	result.sink.fields = BuildExecutionContractFields(result.sink.reason);
	return FinalizeExecutionContract(std::move(result));
}

class ExplainAnalyzeExecutionRegionSinkState : public ExecutionAppendSinkState {
public:
	SinkResultType Append(DataChunk &input) override {
		(void)input;
		return SinkResultType::NEED_MORE_INPUT;
	}
};

bool PhysicalExplainAnalyze::BindExecutionSink(ExecutionContext &context, DataChunk &input,
                                               OperatorSinkInput &sink_input, const ExecutionRegionSinkInfo &sink_info,
                                               ExecutionSinkBinding &binding) const {
	(void)context;
	(void)input;
	(void)sink_input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	if (sink_info.kind != ExecutionRegionSinkKind::MATERIALIZATION ||
	    sink_info.native_sink_contract.status != ExecutionRegionStateContractStatus::READY) {
		binding.blocker = sink_info.native_sink_contract.blocker.empty() ? "explain-analyze-sink-contract-not-ready"
		                                                                 : sink_info.native_sink_contract.blocker;
		return false;
	}
	binding.ready = true;
	binding.append_sink.ready = true;
	binding.append_sink.state = make_shared_ptr<ExplainAnalyzeExecutionRegionSinkState>();
	binding.append_sink.blocker.clear();
	binding.blocker.clear();
	return true;
}

SinkResultType PhysicalExplainAnalyze::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalExplainAnalyze::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ExplainAnalyzeStateGlobalState>();
	auto &profiler = QueryProfiler::Get(context);
	profiler.FinalizeMetrics();
	gstate.analyzed_plan = profiler.ToString(format);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalExplainAnalyze::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ExplainAnalyzeStateGlobalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalExplainAnalyze::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<ExplainAnalyzeStateGlobalState>();

	chunk.data[0].Append(Value("analyzed_plan"));
	chunk.data[1].Append(Value(gstate.analyzed_plan));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
