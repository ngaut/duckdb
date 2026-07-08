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
#include "duckdb/common/vector.hpp"

#include "sljit_delim_join_sink_primitive.hpp"
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
	HASH_JOIN_PROBE_MATERIALIZE,
	HASH_JOIN_PROBE_SELECTION,
	MARK_PROBE_FILTER_BOUNDARY,
	PROJECTION_CHAIN,
	POST_JOIN_PROJECTION_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	GROUPED_AGGREGATE_UPDATE,
	HASH_JOIN_BUILD_SINK,
	DELIM_JOIN_SINK,
	NATIVE_TAIL_DELEGATION
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
	SljitPostJoinProjectionAggregatePrimitive post_join_projection_aggregate;
	SljitUngroupedAggregateUpdatePrimitive ungrouped_aggregate_update;
	SljitGroupedAggregateUpdatePrimitive grouped_aggregate_update;
	SljitHashJoinBuildSinkPrimitive hash_join_build_sink;
	SljitDelimJoinSinkPrimitive delim_join_sink;

	idx_t Op(idx_t index) const;

	static SljitFullPipelinePrimitiveStep SourceFetch();
	static SljitFullPipelinePrimitiveStep GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	HashJoinProbeMaterialize(const SljitHashJoinProbeMaterializePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep MarkProbeFilterBoundary(const SljitMarkProbeFilterBoundaryPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep ProjectionChain(const SljitProjectionChainPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep
	PostJoinProjectionAggregateUpdate(const SljitPostJoinProjectionAggregatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep UngroupedAggregateUpdate(const SljitUngroupedAggregateUpdatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive);
	static SljitFullPipelinePrimitiveStep HashJoinBuildSink(const SljitHashJoinBuildSinkPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep DelimJoinSink(const SljitDelimJoinSinkPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep NativeTailDelegation(idx_t op_idx);

private:
	static SljitFullPipelinePrimitiveStep Make(SljitFullPipelinePrimitiveKind kind,
	                                           std::initializer_list<idx_t> op_indices);
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
