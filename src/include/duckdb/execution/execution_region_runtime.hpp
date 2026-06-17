//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

class Allocator;

class ExecutionOperatorRuntime {
public:
	virtual ~ExecutionOperatorRuntime();

	virtual ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                                 const ExecutionRegionOperatorInfo &operator_info,
	                                                 ExecutionOperatorBinding &binding) = 0;
	virtual bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                      ExecutionSinkBinding &binding) = 0;
	virtual SinkResultType RecordSinkResult(DataChunk &chunk, SinkResultType sink_result) = 0;
	virtual SinkResultType RecordSinkResult(idx_t input_rows, SinkResultType sink_result) = 0;
};

struct ExecutionRegionRuntimeMetrics {
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	string generated_stage_runtime_breakdown;
};

class ExecutionRegionRuntime {
public:
	virtual ~ExecutionRegionRuntime();

	virtual idx_t MaxChunks() const = 0;
	virtual Allocator &GetAllocator() = 0;
	virtual SourceResultType FetchSourceContract(DataChunk *&result) = 0;
	virtual ExecutionOperatorRuntime &ExecutionOperators() = 0;
	virtual bool TraceRuntime() const = 0;
	virtual void RecordGeneratedStageRuntime(const string &stage, int64_t runtime_time_us) = 0;
	virtual void Defer(string reason) = 0;
	virtual const string &DeferredReason() const = 0;
};

} // namespace duckdb
