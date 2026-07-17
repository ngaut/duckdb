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

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::SourceFetch() {
	return Make(SljitFullPipelinePrimitiveKind::SOURCE_FETCH);
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::GENERATED_FILTER);
	step.generated_filter = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(const SljitHashJoinProbeMaterializePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE);
	step.hash_join_probe_materialize = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION);
	step.hash_join_probe_selection = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY);
	step.mark_probe_filter_boundary = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::ProjectionChain(const SljitProjectionChainPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN);
	step.projection_chain = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(
    const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE);
	step.post_join_projection_aggregate = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(const SljitUngroupedAggregateUpdatePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE);
	step.ungrouped_aggregate_update = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE);
	step.grouped_aggregate_update = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::HashJoinBuildSink(const SljitHashJoinBuildSinkPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK);
	step.hash_join_build_sink = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelinePrimitiveStep::DelimJoinSink(const SljitDelimJoinSinkPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK);
	step.delim_join_sink = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::AppendSink(const SljitAppendSinkPrimitive &primitive) {
	auto step = Make(SljitFullPipelinePrimitiveKind::APPEND_SINK);
	step.append_sink = primitive;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::NativeTailDelegation(idx_t tail_start_idx) {
	auto step = Make(SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION);
	step.native_tail_start_idx = tail_start_idx;
	return step;
}

SljitFullPipelinePrimitiveStep SljitFullPipelinePrimitiveStep::Make(SljitFullPipelinePrimitiveKind kind) {
	SljitFullPipelinePrimitiveStep step;
	step.kind = kind;
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
