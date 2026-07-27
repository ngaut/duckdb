//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_trace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>
#include <type_traits>

namespace duckdb {

int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start);

const char *SljitRegionOpKindName(SljitNativeRegionOpKind kind);
string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind);
string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind, const string &phase);

std::chrono::steady_clock::time_point SljitRegionStageStart(ExecutionRegionRuntime &runtime);
void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                   std::chrono::steady_clock::time_point start);
void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                   const char *phase, std::chrono::steady_clock::time_point start);
void RecordSljitRegionRuntimePath(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind, const char *path,
                                  idx_t count = 1);
//! Record the descriptive path and the two typed proof categories required for
//! a backend-owned materialization-elision path.
void RecordSljitRegionMaterializationElision(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                             const char *path, idx_t count = 1);
void RecordSljitRegionRuntimeDelegation(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                        const char *delegation, idx_t row_count);
void RecordSljitRegionStageRuntimePath(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                       const char *phase, std::chrono::steady_clock::time_point start);
void RecordSljitRegionStageRuntimeWithSuffix(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                             SljitNativeRegionOpKind kind, const char *suffix,
                                             std::chrono::steady_clock::time_point start);
void RecordSljitDirectAppendProfile(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                    const DirectAppendProfile &profile);

class SljitRegionStageAccumulator {
public:
	SljitRegionStageAccumulator(ExecutionRegionRuntime &runtime_p, idx_t op_idx, SljitNativeRegionOpKind kind,
	                            const char *phase);

	void Add(std::chrono::steady_clock::time_point start);
	void Flush();

private:
	ExecutionRegionRuntime &runtime;
	string stage;
	int64_t runtime_time_us = 0;
	idx_t count = 0;
	bool enabled;
};

class SljitRegionStageRecorder : public ExecutionOperatorStageRecorder {
public:
	SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, string stage_prefix_p);
	SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, idx_t op_idx, SljitNativeRegionOpKind kind,
	                         const char *phase);

	void RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) override;
	int64_t RecordedRuntimeTimeUs() const;

private:
	ExecutionRegionRuntime &runtime;
	string stage_prefix;
	int64_t recorded_runtime_time_us = 0;
};

void RecordSljitRegionUnattributedRuntime(ExecutionRegionRuntime &runtime, const string &stage_name,
                                          std::chrono::steady_clock::time_point start,
                                          const SljitRegionStageRecorder &recorder);

template <class EXECUTE>
static auto ExecuteSljitRegionRecordedOperation(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                SljitNativeRegionOpKind kind, const char *stage_name,
                                                std::chrono::steady_clock::time_point stage_start, EXECUTE execute)
    -> decltype(execute(optional_ptr<ExecutionOperatorStageRecorder>())) {
	using Result = decltype(execute(optional_ptr<ExecutionOperatorStageRecorder>()));
	if (!runtime.TraceRuntime()) {
		if constexpr (std::is_void<Result>::value) {
			execute(nullptr);
			return;
		} else {
			return execute(nullptr);
		}
	}

	auto trace_stage_name = SljitRegionStageName(op_idx, kind, stage_name);
	SljitRegionStageRecorder recorder(runtime, trace_stage_name);
	if constexpr (std::is_void<Result>::value) {
		execute(&recorder);
		RecordSljitRegionUnattributedRuntime(runtime, trace_stage_name, stage_start, recorder);
	} else {
		auto result = execute(&recorder);
		RecordSljitRegionUnattributedRuntime(runtime, trace_stage_name, stage_start, recorder);
		return result;
	}
}

} // namespace duckdb
