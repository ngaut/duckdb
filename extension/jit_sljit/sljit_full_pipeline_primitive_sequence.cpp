//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_sequence.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_primitive_sequence.hpp"

#include "duckdb/common/exception.hpp"

#include <utility>

namespace duckdb {

idx_t SljitFullPipelinePrimitiveStep::Op(idx_t index) const {
	if (index >= op_count) {
		throw InternalException("SLJIT primitive step operator index is out of range");
	}
	return op_indices[index];
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::SourceFetch() {
	return Make(SljitFullPipelinePrimitiveKind::SOURCE_FETCH, {});
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::GENERATED_FILTER, {primitive.filter_idx});
	step.generated_filter = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(const SljitHashJoinProbeMaterializePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE, {primitive.hash_join_idx});
	step.hash_join_probe_materialize = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION, {primitive.hash_join_idx});
	step.hash_join_probe_selection = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
	auto step =
	    Make(SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY, {primitive.hash_join_idx, primitive.filter_idx});
	step.mark_probe_filter_boundary = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::ProjectionChain(const SljitProjectionChainPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN,
	                 {primitive.first_projection_idx, primitive.final_projection_idx});
	step.projection_chain = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(
    const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE,
	                 {primitive.post_join_projection.hash_join_idx, primitive.aggregate_idx});
	step.post_join_projection_aggregate = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(const SljitUngroupedAggregateUpdatePrimitive &primitive) {
	auto step = primitive.strategy == SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE
	                ? Make(SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE,
	                       {primitive.filter_idx, primitive.aggregate_idx})
	                : Make(SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE, {primitive.aggregate_idx});
	step.ungrouped_aggregate_update = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive) {
	auto step = primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE
	                ? Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE,
	                       {primitive.filter_idx, primitive.aggregate_idx})
	            : primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT
	                ? Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE,
	                       {primitive.first_projection_idx, primitive.final_projection_idx, primitive.aggregate_idx})
	                : Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE, {primitive.aggregate_idx});
	step.grouped_aggregate_update = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinBuildSink(const SljitHashJoinBuildSinkPrimitive &primitive) {
	auto step = primitive.HasProjection() ? Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK,
	                                            {primitive.projection_idx, primitive.sink_idx})
	                                      : Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK,
	                                             {primitive.sink_idx});
	step.hash_join_build_sink = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::DelimJoinSink(const SljitDelimJoinSinkPrimitive &primitive) {
	auto step = primitive.HasProjection()
	                ? Make(SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK,
	                       {primitive.first_projection_idx, primitive.final_projection_idx, primitive.sink_idx})
	                : Make(SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK, {primitive.sink_idx});
	step.delim_join_sink = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::NativeTailDelegation(idx_t op_idx) {
	return Make(SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION, {op_idx});
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::Make(SljitFullPipelinePrimitiveKind kind, std::initializer_list<idx_t> op_indices) {
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

SljitFullPipelinePrimitiveSequence::SljitFullPipelinePrimitiveSequence() {
	steps.reserve(SLJIT_FULL_PIPELINE_MAX_PRIMITIVES);
}

void SljitFullPipelinePrimitiveSequence::Add(SljitFullPipelinePrimitiveStep step) {
	if (Count() >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES) {
		throw InternalException("SLJIT full-pipeline primitive sequence exceeds the maximum step count");
	}
	steps.push_back(std::move(step));
}

} // namespace duckdb
