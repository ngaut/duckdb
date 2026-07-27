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
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return "nested_loop_join_probe";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return "nested_loop_join_build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order_sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append_sink";
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return "delim_join_sink";
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

static bool SljitRegionOpKindProvesGeneratedBackendWork(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
	case SljitNativeRegionOpKind::ORDER_SINK:
	case SljitNativeRegionOpKind::APPEND_SINK:
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
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
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK, count);
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION, count);
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
	runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK);
	if (SljitRegionOpKindProvesGeneratedBackendWork(kind)) {
		runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK);
	}
}

void RecordSljitRegionStageRuntimeWithSuffix(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                             SljitNativeRegionOpKind kind, const char *suffix,
                                             std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind) + suffix, SljitRegionElapsedMicros(start));
}

static void RecordSljitDirectAppendProfileStage(ExecutionRegionRuntime &runtime, const string &stage_prefix,
                                                const char *stage_name, int64_t runtime_time_us, idx_t count = 1) {
	if (!runtime.TraceRuntime() || runtime_time_us <= 0 || count == 0) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(stage_prefix + "." + stage_name, runtime_time_us, count);
}

void RecordSljitDirectAppendProfile(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                    const DirectAppendProfile &profile) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	auto prepare_prefix = SljitRegionStageName(op_idx, kind, "append_prepare");
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_finalize_row_group",
	                                    profile.prepare_finalize_row_group_time_us);
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_new_row_group",
	                                    profile.prepare_new_row_group_time_us);
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_fixed_column_prepare",
	                                    profile.prepare_fixed_column_time_us, profile.prepare_fixed_column_count);

	auto commit_prefix = SljitRegionStageName(op_idx, kind, "append_commit");
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_format",
	                                    profile.commit_source_format_time_us,
	                                    profile.commit_source_append_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_append",
	                                    profile.commit_source_append_time_us,
	                                    profile.commit_source_append_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_fixed_column_commit",
	                                    profile.commit_fixed_column_time_us, profile.commit_fixed_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_distinct_lock",
	                                    profile.commit_distinct_lock_time_us);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_distinct_stats",
	                                    profile.commit_source_distinct_stats_time_us,
	                                    profile.commit_source_distinct_stats_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_target_distinct_stats",
	                                    profile.commit_target_distinct_stats_time_us,
	                                    profile.commit_target_distinct_stats_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_provided_distinct_count",
	                                    profile.commit_provided_distinct_count_time_us,
	                                    profile.commit_provided_distinct_count_column_count);
}

SljitRegionStageAccumulator::SljitRegionStageAccumulator(ExecutionRegionRuntime &runtime_p, idx_t op_idx,
                                                         SljitNativeRegionOpKind kind, const char *phase)
    : runtime(runtime_p), enabled(runtime.TraceRuntime()) {
	if (enabled) {
		stage = SljitRegionStageName(op_idx, kind, phase);
	}
}

void SljitRegionStageAccumulator::Add(std::chrono::steady_clock::time_point start) {
	if (!enabled) {
		return;
	}
	runtime_time_us += SljitRegionElapsedMicros(start);
	count++;
}

void SljitRegionStageAccumulator::Flush() {
	if (!enabled || count == 0) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(stage, runtime_time_us, count);
	runtime_time_us = 0;
	count = 0;
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
