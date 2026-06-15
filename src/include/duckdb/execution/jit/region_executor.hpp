//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/region_executor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

namespace duckdb {

class DataChunk;
class JitRegionKernel;
enum class PipelineExecuteResult;
struct OperatorSinkInput;
struct OperatorSourceInput;
class PipelineExecutor;

class JitRegionExecutor {
public:
	static bool TryExecuteFullPipeline(PipelineExecutor &executor, idx_t max_chunks, PipelineExecuteResult &result);
	static bool TryExecute(PipelineExecutor &executor, DataChunk &input, DataChunk &result, idx_t initial_idx,
	                       OperatorResultType &operator_result);
	static bool HasSourcePrefixKernel(PipelineExecutor &executor);
	static bool TryExecuteSourcePrefix(PipelineExecutor &executor, DataChunk &source_chunk, DataChunk *&result,
	                                   SourceResultType source_result, int64_t source_fetch_time_us,
	                                   idx_t &next_operator_idx);
	static bool TryExecutePreparedSourceReference(PipelineExecutor &executor, DataChunk &source_chunk,
	                                             DataChunk *&result, SourceResultType source_result,
	                                             int64_t source_fetch_time_us, idx_t &next_operator_idx);

private:
	static OperatorResultType ExecuteReferenceOperatorInterval(PipelineExecutor &executor, DataChunk &input,
	                                                           DataChunk &result, idx_t start_operator_idx,
	                                                           idx_t end_operator_idx);
	static void VerifyRegionResult(const DataChunk &actual, OperatorResultType actual_result,
	                               const DataChunk &expected, OperatorResultType expected_result);
	static bool ExecuteFallback(PipelineExecutor &executor, JitRegionKernel &kernel, DataChunk &input,
	                            DataChunk &result, idx_t initial_idx, idx_t input_count_before,
	                            OperatorResultType &operator_result, string decline_reason, string fallback_reason,
	                            int64_t decline_time_us = 0);
};

} // namespace duckdb
