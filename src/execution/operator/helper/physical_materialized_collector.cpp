#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/result_set_manager.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                             bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel) {
}

class MaterializedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;
};

class MaterializedCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

ExecutionContract PhysicalMaterializedCollector::GetExecutionContract() const {
	return BuildExecutionResultCollectorSinkContract();
}

class MaterializedCollectorExecutionRegionSinkState : public ExecutionAppendSinkState {
public:
	explicit MaterializedCollectorExecutionRegionSinkState(MaterializedCollectorLocalState &state_p) : state(state_p) {
	}

	SinkResultType Append(DataChunk &input) override {
		state.collection->Append(state.append_state, input);
		return SinkResultType::NEED_MORE_INPUT;
	}

private:
	MaterializedCollectorLocalState &state;
};

bool PhysicalMaterializedCollector::BindExecutionSink(ExecutionContext &context, DataChunk &input,
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
	auto &state = sink_input.local_state.Cast<MaterializedCollectorLocalState>();
	binding.ready = true;
	binding.append_sink.ready = true;
	binding.append_sink.state = make_shared_ptr<MaterializedCollectorExecutionRegionSinkState>(state);
	binding.append_sink.blocker = "none";
	binding.blocker = "none";
	return true;
}

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	lstate.collection->Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalMaterializedCollector::Combine(ExecutionContext &context,
                                                             OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<MaterializedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<MaterializedCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalMaterializedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<MaterializedCollectorLocalState>();
	state->collection = CreateCollection(context.client);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<MaterializedCollectorGlobalState>();
	if (!gstate.collection) {
		gstate.collection = CreateCollection(*gstate.context);
	}
	auto result =
	    make_uniq<MaterializedQueryResult>(statement_type, properties, IdentifiersToStrings(names),
	                                       std::move(gstate.collection), gstate.context->GetClientProperties());
	return std::move(result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalMaterializedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
