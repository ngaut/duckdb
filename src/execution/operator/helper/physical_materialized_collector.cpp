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

JitOperatorDescriptor PhysicalMaterializedCollector::GetJitOperatorDescriptor() const {
	return BuildJitResultCollectorAppendDescriptor();
}

bool PhysicalMaterializedCollector::BindJitNativeSink(ExecutionContext &context, DataChunk &input,
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
	auto &state = sink_input.local_state.Cast<MaterializedCollectorLocalState>();
	binding.ready = true;
	binding.result_collector_append.ready = true;
	binding.result_collector_append.kind = JitNativeResultCollectorAppendKind::COLUMN_DATA_COLLECTION;
	binding.result_collector_append.collection = state.collection.get();
	binding.result_collector_append.append_state = &state.append_state;
	binding.result_collector_append.blocker = "none";
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
