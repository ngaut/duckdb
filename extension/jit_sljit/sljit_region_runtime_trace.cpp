//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_trace.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_runtime_trace.hpp"

#include <utility>

namespace duckdb {

int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

const char *SljitRegionOpKindName(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::FILTER:
		return "filter";
	case SljitNativeRegionOpKind::PROJECTION:
		return "projection";
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return "hash_join_probe";
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return "hash_join_build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order_sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append_sink";
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return "aggregate_update";
	default:
		return "unknown";
	}
}

string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind) {
	return "op" + std::to_string(op_idx) + ":" + SljitRegionOpKindName(kind);
}

string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind, const string &phase) {
	return SljitRegionStageName(op_idx, kind) + "." + phase;
}

static bool SljitRegionOpKindHasBackendWork(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
	case SljitNativeRegionOpKind::ORDER_SINK:
	case SljitNativeRegionOpKind::APPEND_SINK:
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return true;
	default:
		return false;
	}
}

std::chrono::steady_clock::time_point SljitRegionStageStart(ExecutionRegionRuntime &runtime) {
	return runtime.TraceRuntime() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                   std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind), SljitRegionElapsedMicros(start));
}

void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                   const char *phase, std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind, phase), SljitRegionElapsedMicros(start));
}

void RecordSljitRegionRuntimePath(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind, const char *path,
                                  idx_t count) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	auto runtime_path = string(SljitRegionOpKindName(kind)) + "." + path;
	runtime.RecordJitRuntimePath(runtime_path.c_str(), count);
}

void RecordSljitRegionMaterializationElision(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                             const char *path, idx_t count) {
	RecordSljitRegionRuntimePath(runtime, kind, path, count);
	RecordSljitRegionBackendWork(runtime, count);
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION, count);
}

void RecordSljitRegionBackendWork(ExecutionRegionRuntime &runtime, idx_t count) {
	if (!runtime.TraceRuntime() || count == 0) {
		return;
	}
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK, count);
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK, count);
}

void RecordSljitRegionGeneratedWork(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind, idx_t count) {
	if (!runtime.TraceRuntime() || count == 0) {
		return;
	}
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK, count);
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::ACCELERATED_STAGE_WORK, count);
	if (SljitRegionOpKindHasBackendWork(kind)) {
		runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::BACKEND_OWNED_WORK, count);
	}
}

void RecordSljitRegionRuntimeDelegation(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                        const char *delegation, idx_t row_count) {
	if (!runtime.TraceRuntime() || row_count == 0) {
		return;
	}
	auto runtime_delegation = string(SljitRegionOpKindName(kind)) + "." + delegation;
	runtime.RecordJitRuntimeDelegation(runtime_delegation.c_str(), row_count);
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::DELEGATED_RUNTIME_WORK, row_count);
}

void RecordSljitRegionStageRuntimePath(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                       const char *phase, std::chrono::steady_clock::time_point start) {
	RecordSljitRegionStageRuntime(runtime, op_idx, kind, phase, start);
	RecordSljitRegionRuntimePath(runtime, kind, phase);
}

void RecordSljitRegionStageRuntimeWithSuffix(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                             SljitNativeRegionOpKind kind, const char *suffix,
                                             std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind) + suffix, SljitRegionElapsedMicros(start));
}

SljitRegionStageRecorder::SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, string stage_prefix_p)
    : runtime(runtime_p), stage_prefix(std::move(stage_prefix_p)) {
}

SljitRegionStageRecorder::SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, idx_t op_idx,
                                                   SljitNativeRegionOpKind kind, const char *phase)
    : runtime(runtime_p) {
	if (runtime.TraceRuntime()) {
		stage_prefix = SljitRegionStageName(op_idx, kind, phase);
	}
}

void SljitRegionStageRecorder::RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	recorded_runtime_time_us += runtime_time_us;
	runtime.RecordGeneratedStageRuntime(stage_prefix + "." + stage.name, runtime_time_us);
}

int64_t SljitRegionStageRecorder::RecordedRuntimeTimeUs() const {
	return recorded_runtime_time_us;
}

void RecordSljitRegionUnattributedRuntime(ExecutionRegionRuntime &runtime, const string &stage_name,
                                          std::chrono::steady_clock::time_point start,
                                          const SljitRegionStageRecorder &recorder) {
	auto runtime_us = SljitRegionElapsedMicros(start);
	auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
	if (unattributed_runtime_us > 0) {
		runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
	}
}

} // namespace duckdb
