//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <limits>
#include <utility>

namespace duckdb {

static bool SljitSinkResultStopsPipeline(SinkResultType sink_result) {
	return sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED;
}

static bool SljitNativeSinkResultStopsExecution(ExecutionRegionRuntime &runtime, SinkResultType sink_result,
                                                ExecutionRegionResult &result) {
	if (!SljitSinkResultStopsPipeline(sink_result)) {
		return false;
	}
	if (sink_result == SinkResultType::BLOCKED) {
		// A blocked append sink queues its chunks in the executor and reports NEED_MORE_INPUT
		throw InternalException("SLJIT kernel received a blocked sink result");
	}
	result = ExecutionRegionResult::FINISHED;
	return true;
}

static bool SljitStopFullPipeline(ExecutionRegionResult &result, ExecutionRegionResult stop_result) {
	result = stop_result;
	return true;
}

} // namespace duckdb
