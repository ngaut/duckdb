//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_sequence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <initializer_list>

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"

#include "sljit_delim_join_sink_primitive.hpp"
#include "sljit_generated_filter_primitive.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_join_projection_aggregate_update_primitive.hpp"
#include "sljit_projection_chain_runtime.hpp"

namespace duckdb {

enum class SljitFullPipelinePrimitiveKind : uint8_t {
	INVALID,
	SOURCE_FETCH,
	SOURCE_BATCH_BOUNDARY,
	GENERATED_FILTER,
	HASH_JOIN_PROBE_MATERIALIZE,
	HASH_JOIN_PROBE_SELECTION,
	MARK_PROBE_FILTER_BOUNDARY,
	PROJECTION_CHAIN,
	JOIN_PROJECTION_AGGREGATE_UPDATE,
	GROUPED_AGGREGATE_UPDATE,
	DELIM_JOIN_SINK,
	NATIVE_TAIL_HANDOFF
};

static constexpr idx_t SLJIT_FULL_PIPELINE_MAX_PRIMITIVES = 16;
static constexpr idx_t SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS = 3;

struct SljitFullPipelinePrimitiveStep {
	SljitFullPipelinePrimitiveKind kind = SljitFullPipelinePrimitiveKind::INVALID;
	std::array<idx_t, SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS> op_indices;
	idx_t op_count = 0;
	SljitHashJoinProbeMaterializePrimitive hash_join_probe_materialize;
	SljitHashJoinProbeSelectionPrimitive hash_join_probe_selection;
	SljitGeneratedFilterPrimitive generated_filter;
	SljitMarkProbeFilterBoundaryPrimitive mark_probe_filter_boundary;
	SljitProjectionChainPrimitive projection_chain;
	SljitJoinProjectionAggregateUpdatePrimitive join_projection_aggregate_update;
	SljitGroupedAggregateUpdatePrimitive grouped_aggregate_update;
	SljitDelimJoinSinkPrimitive delim_join_sink;

	idx_t Op(idx_t index) const {
		if (index >= op_count) {
			throw InternalException("SLJIT primitive step operator index is out of range");
		}
		return op_indices[index];
	}

	static SljitFullPipelinePrimitiveStep SourceFetch() {
		return Make(SljitFullPipelinePrimitiveKind::SOURCE_FETCH, {});
	}

	static SljitFullPipelinePrimitiveStep SourceBatchBoundary(idx_t op_idx) {
		return Make(SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY, {op_idx});
	}

	static SljitFullPipelinePrimitiveStep GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::GENERATED_FILTER, {primitive.filter_idx});
		step.generated_filter = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep
	HashJoinProbeMaterialize(const SljitHashJoinProbeMaterializePrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE, {primitive.hash_join_idx});
		step.hash_join_probe_materialize = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep
	HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION, {primitive.hash_join_idx});
		step.hash_join_probe_selection = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep
	MarkProbeFilterBoundary(const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY,
		                 {primitive.hash_join_idx, primitive.filter_idx});
		step.mark_probe_filter_boundary = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep ProjectionChain(const SljitProjectionChainPrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN,
		                 {primitive.first_projection_idx, primitive.final_projection_idx});
		step.projection_chain = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep
	JoinProjectionAggregateUpdate(const SljitJoinProjectionAggregateUpdatePrimitive &primitive) {
		auto step = Make(SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE,
		                 {SljitJoinProjectionAggregateUpdateFirstOpIdx(primitive),
		                  SljitJoinProjectionAggregateUpdateAggregateIdx(primitive)});
		step.join_projection_aggregate_update = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep
	GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive) {
		auto step =
		    primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT
		        ? Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE,
		               {primitive.first_projection_idx, primitive.final_projection_idx, primitive.aggregate_idx})
		        : Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE, {primitive.aggregate_idx});
		step.grouped_aggregate_update = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep DelimJoinSink(const SljitDelimJoinSinkPrimitive &primitive) {
		auto step = primitive.HasProjection()
		                ? Make(SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK,
		                       {primitive.first_projection_idx, primitive.final_projection_idx, primitive.sink_idx})
		                : Make(SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK, {primitive.sink_idx});
		step.delim_join_sink = primitive;
		return step;
	}

	static SljitFullPipelinePrimitiveStep NativeTailHandoff(idx_t op_idx) {
		return Make(SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF, {op_idx});
	}

private:
	static SljitFullPipelinePrimitiveStep Make(SljitFullPipelinePrimitiveKind kind,
	                                           std::initializer_list<idx_t> op_indices) {
		SljitFullPipelinePrimitiveStep step;
		step.kind = kind;
		step.op_indices.fill(DConstants::INVALID_INDEX);
		for (auto op_idx : op_indices) {
			if (step.op_count >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS) {
				throw InternalException("SLJIT full-pipeline primitive step exceeds the maximum operator count");
			}
			step.op_indices[step.op_count++] = op_idx;
		}
		return step;
	}
};

struct SljitFullPipelinePrimitiveSequence {
	std::array<SljitFullPipelinePrimitiveStep, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> steps;
	idx_t count = 0;

	SljitFullPipelinePrimitiveSequence() = default;

	explicit SljitFullPipelinePrimitiveSequence(std::initializer_list<SljitFullPipelinePrimitiveStep> steps) {
		for (auto step : steps) {
			Add(step);
		}
	}

	void Add(SljitFullPipelinePrimitiveStep step) {
		if (count >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES) {
			throw InternalException("SLJIT full-pipeline primitive sequence exceeds the maximum step count");
		}
		steps[count++] = step;
	}
};

} // namespace duckdb
