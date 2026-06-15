#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), memory_type(data.memory_type),
      plan(data.physical_plan->Root()), names(data.names) {
	types = data.types;
}

unique_ptr<PhysicalOperator> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                         PreparedStatementData &data) {
	auto &physical_plan = *data.physical_plan;
	auto &root = physical_plan.Root();

	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
		// Not an order-preserving plan: use the parallel materialized collector.
		if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
			return make_uniq<PhysicalBufferedCollector>(physical_plan, data, true);
		}
		return make_uniq<PhysicalMaterializedCollector>(physical_plan, data, true);
	}

	if (!PhysicalPlanGenerator::UseBatchIndex(context, root)) {
		// Order-preserving plan, and we cannot use the batch index: use single-threaded result collector.
		if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
			return make_uniq<PhysicalBufferedCollector>(physical_plan, data, false);
		}
		return make_uniq<PhysicalMaterializedCollector>(physical_plan, data, false);
	}

	// Order-preserving plan, and we can use the batch index: use a batch collector.
	if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
		return make_uniq<PhysicalBufferedBatchCollector>(physical_plan, data);
	}
	return make_uniq<PhysicalBatchCollector>(physical_plan, data);
}

JitOperatorDescriptor PhysicalResultCollector::BuildJitResultCollectorAppendDescriptor() const {
	JitOperatorDescriptor result;
	result.has_sink = true;
	result.sink.kind = JitRegionSinkKind::RESULT_COLLECTOR_APPEND;
	result.sink.reason = "DuckDB result collector native append protocol";
	result.sink.reason += ";operator=RESULT_COLLECTOR";
	result.sink.reason += ";output_columns=" + std::to_string(types.size());
	result.sink.reason += ";native_result_collector_append_contract=ready";
	result.sink.reason += ";native_result_collector_append_required_capability=result-collector-native-append";
	result.sink.reason += ";native_result_collector_append_blocker=none";
	result.sink.fields = BuildJitDescriptorProtocolFields(result.sink.reason);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

SinkResultType JitAppendNativeResultCollector(const JitNativeResultCollectorAppendBinding &binding, DataChunk &input) {
	if (!binding.ready) {
		throw InternalException("JIT native result collector append requires a ready binding");
	}
	switch (binding.kind) {
	case JitNativeResultCollectorAppendKind::COLUMN_DATA_COLLECTION:
		if (!binding.collection || !binding.append_state) {
			throw InternalException("JIT native materialized result collector append binding is incomplete");
		}
		binding.collection->Append(*binding.append_state, input);
		return SinkResultType::NEED_MORE_INPUT;
	case JitNativeResultCollectorAppendKind::BATCHED_DATA_COLLECTION:
		if (!binding.batched_data) {
			throw InternalException("JIT native batched result collector append binding is incomplete");
		}
		binding.batched_data->Append(input, binding.batch_index);
		return SinkResultType::NEED_MORE_INPUT;
	case JitNativeResultCollectorAppendKind::SIMPLE_BUFFERED_DATA:
		if (!binding.simple_buffered_data || !binding.interrupt_state) {
			throw InternalException("JIT native simple buffered result collector append binding is incomplete");
		}
		if (binding.simple_buffered_data->BufferIsFull()) {
			binding.simple_buffered_data->BlockSink(*binding.interrupt_state);
			return SinkResultType::BLOCKED;
		}
		binding.simple_buffered_data->Append(input);
		return SinkResultType::NEED_MORE_INPUT;
	case JitNativeResultCollectorAppendKind::BATCHED_BUFFERED_DATA:
		if (!binding.batched_buffered_data || !binding.interrupt_state || !binding.current_batch) {
			throw InternalException("JIT native batched buffered result collector append binding is incomplete");
		}
		*binding.current_batch = binding.batch_index;
		binding.batched_buffered_data->UpdateMinBatchIndex(binding.min_batch_index);
		if (binding.batched_buffered_data->ShouldBlockBatch(binding.batch_index)) {
			binding.batched_buffered_data->BlockSink(*binding.interrupt_state, binding.batch_index);
			return SinkResultType::BLOCKED;
		}
		binding.batched_buffered_data->Append(input, binding.batch_index);
		return SinkResultType::NEED_MORE_INPUT;
	default:
		throw InternalException("JIT native result collector append binding has no append kind");
	}
}

vector<const_reference<PhysicalOperator>> PhysicalResultCollector::GetChildren() const {
	return {plan};
}

void PhysicalResultCollector::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	D_ASSERT(children.empty());

	// single operator: the operator becomes the data source of the current pipeline
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(plan);
}

unique_ptr<ColumnDataCollection> PhysicalResultCollector::CreateCollection(ClientContext &context) const {
	switch (memory_type) {
	case QueryResultMemoryType::IN_MEMORY:
		return make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	case QueryResultMemoryType::BUFFER_MANAGED:
		// Use the DatabaseInstance BufferManager because the query result can outlive the ClientContext
		return make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(*context.db), types,
		                                       ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	default:
		throw NotImplementedException("PhysicalResultCollector::CreateCollection for %s",
		                              EnumUtil::ToString(memory_type));
	}
}

} // namespace duckdb
