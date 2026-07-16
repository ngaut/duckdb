//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_materialized_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

class PhysicalMaterializedCollector : public PhysicalResultCollector {
public:
	PhysicalMaterializedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, bool parallel);

	bool parallel;

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	ExecutionContract GetExecutionContract(ExecutionRegionOperatorSlot slot, bool render_diagnostics) const override;
	bool BindExecutionSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
	                       const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool ParallelSink() const override;
	bool SinkOrderDependent() const override;
};

} // namespace duckdb
