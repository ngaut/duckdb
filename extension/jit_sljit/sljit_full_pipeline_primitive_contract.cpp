//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_contract.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_primitive_contract.hpp"

namespace duckdb {

namespace {

bool SljitFullPipelinePrimitiveIsIntermediate(SljitFullPipelinePrimitiveKind kind) {
	return kind == SljitFullPipelinePrimitiveKind::GENERATED_FILTER ||
	       kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE ||
	       kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION ||
	       kind == SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY ||
	       kind == SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN;
}

bool SljitFullPipelinePrimitiveIsTerminal(SljitFullPipelinePrimitiveKind kind) {
	return kind == SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE ||
	       kind == SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE ||
	       kind == SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE ||
	       kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK ||
	       kind == SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK ||
	       kind == SljitFullPipelinePrimitiveKind::APPEND_SINK ||
	       kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION;
}

bool SljitFullPipelineHasExactFilterProbeHashBuild(const vector<SljitExecutableRegionOp> &ops,
                                                   const SljitFullPipelinePrimitiveSequence &sequence) {
	if (sequence.Count() != 3 || sequence.Step(1).kind != SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION ||
	    sequence.Step(2).kind != SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK) {
		return false;
	}
	const auto probe_idx = sequence.Step(1).hash_join_probe_selection.hash_join_idx;
	if (probe_idx >= ops.size() || ops[probe_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		throw InternalException("SLJIT bound recipe references an invalid hash join probe");
	}
	auto &probe = ops[probe_idx].hash_join_probe.plan;
	return probe.exact_source_filter_identity && probe.keys.size() == 1 && probe.equality_key_count == 1 &&
	       !probe.residual_predicate && !probe.mark_build_match &&
	       probe.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
}

void SljitCollectFusedFilterOwner(const vector<SljitExecutableRegionOp> &ops,
                                  const SljitFullPipelinePrimitiveStep &step, vector<idx_t> &owners) {
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	if (step.kind == SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE &&
	    step.ungrouped_aggregate_update.strategy ==
	        SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
		filter_idx = step.ungrouped_aggregate_update.filter_idx;
		aggregate_idx = step.ungrouped_aggregate_update.aggregate_idx;
	} else if (step.kind == SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE &&
	           step.grouped_aggregate_update.strategy ==
	               SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
		filter_idx = step.grouped_aggregate_update.filter_idx;
		aggregate_idx = step.grouped_aggregate_update.aggregate_idx;
	}
	if (filter_idx == DConstants::INVALID_INDEX) {
		return;
	}
	if (aggregate_idx >= ops.size() || ops[aggregate_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !ops[aggregate_idx].aggregate_update.filtered_update.IsExecutable()) {
		throw InternalException("SLJIT fused filter recipe has no executable aggregate owner");
	}
	owners.push_back(filter_idx);
}

} // namespace

SljitFullPipelineRecipe
SljitFinalizeFullPipelinePrimitiveRecipe(const vector<SljitExecutableRegionOp> &ops,
                                         bool uses_extended_source_fetch_budget,
                                         SljitFullPipelinePrimitiveSequence primitive_sequence,
                                         SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer) {
	if (primitive_sequence.Count() < 2 ||
	    primitive_sequence.Step(0).kind != SljitFullPipelinePrimitiveKind::SOURCE_FETCH) {
		throw InternalException("SLJIT full-pipeline recipe must start with source fetch and have a terminal");
	}

	vector<idx_t> fused_filter_owners;
	for (idx_t step_idx = 1; step_idx < primitive_sequence.Count(); step_idx++) {
		auto &step = primitive_sequence.Step(step_idx);
		const bool terminal = step_idx + 1 == primitive_sequence.Count();
		if ((terminal ? !SljitFullPipelinePrimitiveIsTerminal(step.kind)
		              : !SljitFullPipelinePrimitiveIsIntermediate(step.kind))) {
			throw InternalException("SLJIT full-pipeline recipe has invalid primitive ownership");
		}
		SljitCollectFusedFilterOwner(ops, step, fused_filter_owners);
	}
	SljitValidateHashJoinDirectAggregateConsumerContract(primitive_sequence, direct_aggregate_consumer);

	const auto &terminal = primitive_sequence.Step(primitive_sequence.Count() - 1);
	const bool selected_hash_join_sink =
	    primitive_sequence.Count() == 3 &&
	    primitive_sequence.Step(1).kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION &&
	    (terminal.kind == SljitFullPipelinePrimitiveKind::APPEND_SINK ||
	     terminal.kind == SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK);
	const bool direct_source_hash_build = terminal.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK &&
	                                      terminal.hash_join_build_sink.direct_source_ingress;

	SljitFullPipelineRecipe recipe;
	recipe.primitive_sequence = std::move(primitive_sequence);
	recipe.direct_aggregate_consumer = direct_aggregate_consumer;
	recipe.runtime_kind = selected_hash_join_sink ? SljitFullPipelineRuntimeKind::SELECTED_HASH_JOIN_SINK
	                                              : SljitFullPipelineRuntimeKind::PRIMITIVE_SEQUENCE;
	recipe.uses_extended_source_fetch_budget = uses_extended_source_fetch_budget;
	recipe.preserves_partitioned_source_chunks =
	    recipe.primitive_sequence.Step(1).kind == SljitFullPipelinePrimitiveKind::GENERATED_FILTER;
	recipe.has_scan_filter_executable_body =
	    direct_source_hash_build || SljitFullPipelineHasExactFilterProbeHashBuild(ops, recipe.primitive_sequence);
	recipe.fused_filter_owners = std::move(fused_filter_owners);
	return recipe;
}

//! A delegated aggregate hands its input chunk to the physical sink, which
//! references columns by the sink contract's declared indices and types. The
//! region graph can fuse input projections (payload compute, perfect-hash
//! group compression) into the aggregate's compiled representation — a plain
//! delegation cannot honor those, so the declared references must be sound
//! against the previous op's output layout or the tail is refused.
static bool SljitDelegatedAggregateLayoutSound(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	if (aggregate_idx == 0) {
		return true;
	}
	auto &previous_output = ops[aggregate_idx - 1].output_types;
	auto &sink_info = ops[aggregate_idx].aggregate_update.plan.sink_info;
	for (auto &group : sink_info.groups) {
		if (!group.supported_reference) {
			continue;
		}
		if (group.input_index >= previous_output.size() || previous_output[group.input_index] != group.type) {
			return false;
		}
	}
	for (auto &aggregate : sink_info.aggregates) {
		for (idx_t child_idx = 0; child_idx < aggregate.child_indices.size(); child_idx++) {
			if (aggregate.child_indices[child_idx] >= previous_output.size()) {
				return false;
			}
			if (child_idx < aggregate.child_types.size() &&
			    previous_output[aggregate.child_indices[child_idx]] != aggregate.child_types[child_idx]) {
				return false;
			}
		}
	}
	return true;
}

bool SljitNativeTailCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx) {
	if (tail_start_idx >= ops.size()) {
		return false;
	}
	auto &tail = ops[tail_start_idx];
	if (tail.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE && tail.aggregate_update.plan.UsesPrimitivePayloads()) {
		return false;
	}
	for (idx_t op_idx = tail_start_idx; op_idx < ops.size(); op_idx++) {
		if (ops[op_idx].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    !SljitDelegatedAggregateLayoutSound(ops, op_idx)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
