//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_buffered_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

class PhysicalBufferedCollector : public PhysicalResultCollector {
public:
	PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, bool parallel);

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
	bool IsStreaming() const override {
		return true;
	}
};

} // namespace duckdb
