//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_sequence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

#include "sljit_generated_filter_primitive.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_post_join_projection_aggregate_primitive.hpp"
#include "sljit_projection_chain_primitive.hpp"
#include "sljit_ungrouped_aggregate_update_primitive.hpp"

namespace duckdb {

enum class SljitFullPipelinePrimitiveKind : uint8_t {
	INVALID,
	SOURCE_FETCH,
	GENERATED_FILTER,
	//! A generated filter on a chunk whose source proves it for every row: the rows pass as they are
	HELD_FILTER,
	HASH_JOIN_PROBE_MATERIALIZE,
	HASH_JOIN_PROBE_SELECTION,
	MARK_PROBE_FILTER_BOUNDARY,
	PROJECTION_CHAIN,
	POST_JOIN_PROJECTION_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	GROUPED_AGGREGATE_UPDATE,
	HASH_JOIN_BUILD_SINK,
	PRIMITIVE_AGGREGATE_UPDATE,
	NATIVE_TAIL_DELEGATION
};

static constexpr idx_t SLJIT_FULL_PIPELINE_MAX_PRIMITIVES = 16;

struct SljitFullPipelinePrimitiveStep {
	SljitFullPipelinePrimitiveKind kind = SljitFullPipelinePrimitiveKind::INVALID;
	SljitHashJoinProbeMaterializePrimitive hash_join_probe_materialize;
	SljitHashJoinProbeSelectionPrimitive hash_join_probe_selection;
	SljitGeneratedFilterPrimitive generated_filter;
	SljitMarkProbeFilterBoundaryPrimitive mark_probe_filter_boundary;
	SljitProjectionChainPrimitive projection_chain;
	SljitPostJoinProjectionAggregatePrimitive post_join_projection_aggregate;
	SljitUngroupedAggregateUpdatePrimitive ungrouped_aggregate_update;
	SljitGroupedAggregateUpdatePrimitive grouped_aggregate_update;
	SljitHashJoinBuildSinkPrimitive hash_join_build_sink;
	//! The aggregate a PRIMITIVE_AGGREGATE_UPDATE step updates through its generated payload update
	idx_t primitive_aggregate_idx = DConstants::INVALID_INDEX;
	idx_t native_tail_start_idx = DConstants::INVALID_INDEX;

	static SljitFullPipelinePrimitiveStep SourceFetch();
	static SljitFullPipelinePrimitiveStep GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep HeldFilter(const SljitGeneratedFilterPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	HashJoinProbeMaterialize(const SljitHashJoinProbeMaterializePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	MarkProbeFilterBoundary(const SljitMarkProbeFilterBoundaryPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep ProjectionChain(const SljitProjectionChainPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	PostJoinProjectionAggregateUpdate(const SljitPostJoinProjectionAggregatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	UngroupedAggregateUpdate(const SljitUngroupedAggregateUpdatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep HashJoinBuildSink(const SljitHashJoinBuildSinkPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep PrimitiveAggregateUpdate(idx_t aggregate_idx);
	static SljitFullPipelinePrimitiveStep NativeTailDelegation(idx_t tail_start_idx);

private:
	static SljitFullPipelinePrimitiveStep Make(SljitFullPipelinePrimitiveKind kind);
};

class SljitFullPipelinePrimitiveSequence {
public:
	SljitFullPipelinePrimitiveSequence();

	void Add(SljitFullPipelinePrimitiveStep step);

	idx_t Count() const {
		return steps.size();
	}

	const SljitFullPipelinePrimitiveStep &Step(idx_t step_idx) const {
		return steps[step_idx];
	}

	SljitFullPipelinePrimitiveStep &Step(idx_t step_idx) {
		return steps[step_idx];
	}

private:
	vector<SljitFullPipelinePrimitiveStep> steps;
};

} // namespace duckdb
