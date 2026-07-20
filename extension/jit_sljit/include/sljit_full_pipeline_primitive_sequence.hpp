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

#include "sljit_append_sink_primitive.hpp"
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
	APPEND_SINK,
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
	SljitDelimJoinSinkPrimitive delim_join_sink;
	SljitAppendSinkPrimitive append_sink;
	idx_t native_tail_start_idx = DConstants::INVALID_INDEX;

	static SljitFullPipelinePrimitiveStep SourceFetch();
	static SljitFullPipelinePrimitiveStep GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive);
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
	static SljitFullPipelinePrimitiveStep DelimJoinSink(const SljitDelimJoinSinkPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep AppendSink(const SljitAppendSinkPrimitive &primitive);
	static SljitFullPipelinePrimitiveStep NativeTailDelegation(idx_t tail_start_idx);

private:
	static SljitFullPipelinePrimitiveStep Make(SljitFullPipelinePrimitiveKind kind);
};

//! Single authority for whether a primitive step permits a mid-query runner
//! handoff; a kernel's capability is the conjunction over its recipe steps.
//!
//! Handoff safety for grouped aggregates additionally requires that the compiled
//! kernel store the SAME group hash the vectorized continuation would compute for
//! an identical key. When the compiled path feeds the hash table a projected group
//! representation (integral narrowing, dictionary-compressed strings), it hashes
//! that non-canonical representation, which the native runner does not reproduce.
//! A mid-query handoff would then leave one shared hash table holding two entries
//! for the same key under two different stored hashes; the final combine indexes by
//! the stored hash, so the entries land in different partitions and never merge —
//! silently splitting the group's aggregate across duplicate output rows. Any step
//! whose compiled group hash is non-canonical must therefore refuse handoff.
static bool SljitFullPipelinePrimitiveStepSupportsRunnerHandoff(const SljitFullPipelinePrimitiveStep &step) {
	if (step.kind == SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE) {
		if (step.grouped_aggregate_update.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			// Projected group inputs hash a non-canonical representation the
			// vectorized runner will not match after a handoff.
			return false;
		}
		return SljitGroupedAggregateUpdateStrategySupportsRunnerHandoff(step.grouped_aggregate_update.strategy);
	}
	if (step.kind == SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE) {
		// The post-join projection fuses the group-key projection into the aggregate,
		// so its compiled group hash is non-canonical by construction.
		return false;
	}
	return true;
}

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
