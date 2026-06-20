//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_explain_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/enums/explain_format.hpp"

namespace duckdb {

class PhysicalExplainAnalyze : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXPLAIN_ANALYZE;

public:
	PhysicalExplainAnalyze(PhysicalPlan &physical_plan, vector<LogicalType> types, ExplainFormat format)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXPLAIN_ANALYZE, std::move(types), 1), format(format) {
	}

public:
	ExplainFormat format;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink Interface
	ExecutionContract GetExecutionContract() const override;
	bool BindExecutionSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
	                       const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
