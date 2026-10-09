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

//! A native operator the kernel delegates to is not ready, although the runner found every one ready before the first
//! source fetch and a running pipeline never changes runners: an invariant violation, not a fallback
[[noreturn]] static void SljitThrowNativeOperatorNotReady(const string &reason) {
	throw InternalException("SLJIT kernel delegates to a native operator that is not ready: %s",
	                        reason.empty() ? string("no reason given") : reason);
}

} // namespace duckdb
