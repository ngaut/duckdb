//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_fixed_fused_runtime.hpp"
#include "sljit_direct_projection_fixed_materialization_runtime.hpp"
#include "sljit_direct_projection_fixed_source_runtime.hpp"
#include "sljit_direct_projection_fixed_stats_runtime.hpp"
#include "sljit_direct_projection_floating_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

namespace duckdb {

enum class SljitDirectProjectionMaterializerKind : uint8_t { NONE, FLOATING_FUSED, FIXED_FUSED, FIXED_SCALAR };

struct SljitDirectProjectionCandidate {
	SljitDirectProjectionMaterializerKind kind = SljitDirectProjectionMaterializerKind::NONE;
	SljitDirectProjectionStatsMode stats_mode = SljitDirectProjectionStatsMode::NONE;
	const char *shape_changed_message = nullptr;

	bool IsSet() const {
		return kind != SljitDirectProjectionMaterializerKind::NONE;
	}

	const char *ShapeChangedMessage() const {
		return shape_changed_message ? shape_changed_message
		                             : "SLJIT direct append source shape changed after preflight";
	}
};

class SljitDirectProjectionStageTimers {
public:
	SljitDirectProjectionStageTimers(ExecutionRegionRuntime &runtime, idx_t projection_idx,
	                                 SljitNativeRegionOpKind projection_kind, idx_t sink_idx,
	                                 SljitNativeRegionOpKind sink_kind, const SljitDirectProjectionCandidate &candidate)
	    : prepare(runtime, sink_idx, sink_kind, "append_prepare"),
	      floating_source_prepare(runtime, projection_idx, projection_kind, "append_floating_source_prepare"),
	      floating_run(runtime, projection_idx, projection_kind, "append_floating_run"),
	      floating_generated(runtime, projection_idx, projection_kind, "materialize_generated"),
	      floating_stats(runtime, projection_idx, projection_kind, "append_floating_finish_stats"),
	      fixed_generated(runtime, projection_idx, projection_kind, "materialize_fixed_generated"),
	      fixed_fused_generated(runtime, projection_idx, projection_kind, "materialize_fixed_fused_generated"),
	      fixed_stats(runtime, projection_idx, projection_kind, "append_fixed_stats"),
	      commit(runtime, sink_idx, sink_kind, "append_commit") {
		switch (candidate.kind) {
		case SljitDirectProjectionMaterializerKind::FLOATING_FUSED:
			generated = &floating_generated;
			stats = &floating_stats;
			break;
		case SljitDirectProjectionMaterializerKind::FIXED_FUSED:
			generated = &fixed_fused_generated;
			stats = &fixed_stats;
			break;
		case SljitDirectProjectionMaterializerKind::FIXED_SCALAR:
			generated = &fixed_generated;
			stats = &fixed_stats;
			break;
		default:
			break;
		}
	}

	void AddPrepare(std::chrono::steady_clock::time_point start) {
		prepare.Add(start);
	}

	void AddFloatingSourcePrepare(std::chrono::steady_clock::time_point start) {
		floating_source_prepare.Add(start);
	}

	void AddFloatingRun(std::chrono::steady_clock::time_point start) {
		floating_run.Add(start);
	}

	void AddGenerated(std::chrono::steady_clock::time_point start) {
		D_ASSERT(generated);
		generated->Add(start);
	}

	void AddStats(std::chrono::steady_clock::time_point start) {
		D_ASSERT(stats);
		stats->Add(start);
	}

	void AddCommit(std::chrono::steady_clock::time_point start) {
		commit.Add(start);
	}

	void Flush() {
		prepare.Flush();
		floating_source_prepare.Flush();
		floating_run.Flush();
		floating_generated.Flush();
		floating_stats.Flush();
		fixed_generated.Flush();
		fixed_fused_generated.Flush();
		fixed_stats.Flush();
		commit.Flush();
	}

private:
	SljitRegionStageAccumulator prepare;
	SljitRegionStageAccumulator floating_source_prepare;
	SljitRegionStageAccumulator floating_run;
	SljitRegionStageAccumulator floating_generated;
	SljitRegionStageAccumulator floating_stats;
	SljitRegionStageAccumulator fixed_generated;
	SljitRegionStageAccumulator fixed_fused_generated;
	SljitRegionStageAccumulator fixed_stats;
	SljitRegionStageAccumulator commit;
	SljitRegionStageAccumulator *generated = nullptr;
	SljitRegionStageAccumulator *stats = nullptr;
};

template <class PROJECTION_SCRATCH>
static bool SljitTrySelectDirectProjectionCandidate(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                    SljitExecutableRegionOp &op, DataChunk &input,
                                                    optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
                                                    PROJECTION_SCRATCH &projection_scratch,
                                                    SljitDirectProjectionCandidate &candidate) {
	candidate = SljitDirectProjectionCandidate();
	const bool use_floating_direct_append =
	    op.flat_fused_floating_projection.Function() && op.flat_fused_floating_projection_plan.covers_all_projections;
	auto preflight_stage_start = SljitRegionStageStart(runtime);
	if (use_floating_direct_append) {
		if (!PrepareFlatFusedFloatingProjectionSources(op, input, nullptr, input.size(), projection_scratch, false)) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "append_floating_preflight", preflight_stage_start);
		candidate.kind = SljitDirectProjectionMaterializerKind::FLOATING_FUSED;
		candidate.stats_mode = op.flat_fused_floating_projection_plan.stats_mode;
		candidate.shape_changed_message = "SLJIT direct append source shape changed after direct-append preflight";
		return true;
	}

	preflight_stage_start = SljitRegionStageStart(runtime);
	if (TryPrepareFlatFusedFixedProjectionSources(op, input, 0, input.size(), source_cache, projection_scratch)) {
		if (!SljitTryDirectMaterializeFixedProjection(op, input, nullptr, source_cache, &projection_scratch.fused)) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "append_fixed_preflight", preflight_stage_start);
		candidate.kind = SljitDirectProjectionMaterializerKind::FIXED_FUSED;
		candidate.stats_mode = SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS;
		candidate.shape_changed_message =
		    "SLJIT fixed fused direct projection source shape changed after direct-append preflight";
		return true;
	}
	if (SljitTryDirectMaterializeFixedProjection(op, input, nullptr, source_cache)) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "append_fixed_preflight", preflight_stage_start);
		candidate.kind = SljitDirectProjectionMaterializerKind::FIXED_SCALAR;
		candidate.stats_mode = SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS;
		candidate.shape_changed_message =
		    "SLJIT fixed direct projection source shape changed after direct-append preflight";
		return true;
	}
	return false;
}

template <class PROJECTION_SCRATCH>
static bool SljitTryMaterializeDirectProjectionSlice(
    ExecutionRegionRuntime &runtime, const SljitDirectProjectionCandidate &candidate, SljitExecutableRegionOp &op,
    DataChunk &input, DirectAppendSlice &slice, optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
    PROJECTION_SCRATCH &projection_scratch, SljitDirectProjectionStageTimers &stage_timers) {
	switch (candidate.kind) {
	case SljitDirectProjectionMaterializerKind::FLOATING_FUSED: {
		auto source_stage_start = SljitRegionStageStart(runtime);
		if (!PrepareFlatFusedFloatingProjectionSources(op, input, nullptr, slice.count, projection_scratch, false,
		                                               slice.source_offset)) {
			return false;
		}
		stage_timers.AddFloatingSourcePrepare(source_stage_start);

		for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
			auto &plan = op.projections[projection_idx].plan;
			if (!slice.targets[projection_idx]) {
				throw InternalException("SLJIT direct append target pointer is null");
			}
			projection_scratch.result_data[projection_idx] = slice.targets[projection_idx];
			if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
				projection_scratch.float_constants[projection_idx] = static_cast<float>(plan.double_constant);
			} else {
				projection_scratch.double_constants[projection_idx] = plan.double_constant;
			}
		}
		projection_scratch.PrepareFloatingStats(op.projections.size(),
		                                        op.flat_fused_floating_projection_plan.SinglePrecision());

		auto run_stage_start = SljitRegionStageStart(runtime);
		RunFlatFusedFloatingProjection(op, slice.count, projection_scratch);
		stage_timers.AddFloatingRun(run_stage_start);
		return true;
	}
	case SljitDirectProjectionMaterializerKind::FIXED_FUSED:
		if (!TryPrepareFlatFusedFixedProjectionSources(op, input, slice.source_offset, slice.count, source_cache,
		                                               projection_scratch)) {
			return false;
		}
		BindFlatFusedFixedProjectionTargets(op, slice, projection_scratch);
		RunFlatFusedFixedProjection(op, slice.count, projection_scratch);
		return SljitTryDirectMaterializeFixedProjection(op, input, &slice, source_cache, &projection_scratch.fused);
	case SljitDirectProjectionMaterializerKind::FIXED_SCALAR:
		return SljitTryDirectMaterializeFixedProjection(op, input, &slice, source_cache);
	default:
		return false;
	}
}

template <class PROJECTION_SCRATCH>
static void SljitFinishDirectProjectionStats(const SljitDirectProjectionCandidate &candidate,
                                             SljitExecutableRegionOp &op, DataChunk &input, DirectAppendSlice &slice,
                                             const vector<idx_t> &source_distinct_counts,
                                             optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
                                             PROJECTION_SCRATCH &projection_scratch) {
	switch (candidate.stats_mode) {
	case SljitDirectProjectionStatsMode::NONE:
		slice.stats.clear();
		return;
	case SljitDirectProjectionStatsMode::GENERATED_FLOATING_MIN_MAX:
		if (candidate.kind != SljitDirectProjectionMaterializerKind::FLOATING_FUSED) {
			throw InternalException("SLJIT generated floating stats requested for a non-floating direct append path");
		}
		projection_scratch.FinishFloatingStats(op.projections,
		                                       op.flat_fused_floating_projection_plan.SinglePrecision());
		slice.stats = projection_scratch.direct_append_stats;
		return;
	case SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS:
		if (candidate.kind != SljitDirectProjectionMaterializerKind::FIXED_FUSED &&
		    candidate.kind != SljitDirectProjectionMaterializerKind::FIXED_SCALAR) {
			throw InternalException(
			    "SLJIT fixed postpass stats requested for a non-fixed direct projection materializer");
		}
		slice.stats.assign(op.projections.size(), DirectAppendColumnStats());
		{
			vector<DirectAppendColumnStats> fixed_source_stats(input.ColumnCount());
			for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
				TryComputeFixedDirectProjectionStats(op.projections[projection_idx], input, slice.source_offset,
				                                     slice.count, fixed_source_stats, slice.stats[projection_idx],
				                                     source_cache, source_distinct_counts);
			}
		}
		return;
	default:
		throw InternalException("Unsupported SLJIT direct projection stats mode");
	}
}

} // namespace duckdb
