//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_contract.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_join_projection_aggregate_input_binding.hpp"

namespace duckdb {

namespace {

//! Resolve semantic join outputs into the probe input plus build output domain. This temporary
//! composition preserves producer expressions; it is not a second runtime column map.
static bool SljitTryBuildJoinAggregateInputProjection(const vector<idx_t> &lhs_output_columns,
                                                      const vector<LogicalType> &output_types,
                                                      const vector<LogicalType> &input_types,
                                                      optional_ptr<const SljitBatchLayout> producer_layout,
                                                      optional_ptr<const SljitExecutableRegionOp> producer,
                                                      vector<SljitNativeRegionExpressionPlan> &inputs,
                                                      vector<idx_t> &reference_sources) {
	const auto lhs_count = lhs_output_columns.size();
	if (lhs_count > output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < output_types.size(); output_idx++) {
		SljitNativeRegionExpressionPlan input;
		if (output_idx < lhs_count) {
			const auto producer_idx = lhs_output_columns[output_idx];
			if (producer_layout) {
				if (!producer || producer_idx >= producer_layout->ColumnCount() ||
				    producer_idx >= producer->projections.size()) {
					return false;
				}
				const auto &column = producer_layout->Column(producer_idx);
				if (!column.HasSource() || column.source_index >= input_types.size() ||
				    input_types[column.source_index] != column.source_type || column.type != output_types[output_idx]) {
					return false;
				}
				input = SljitCopySemanticExpressionPlan(producer->projections[producer_idx]);
				if (input.source_index != column.source_index || input.return_type != column.type) {
					return false;
				}
			} else {
				if (producer_idx >= input_types.size() || input_types[producer_idx] != output_types[output_idx]) {
					return false;
				}
				input.kind = SljitNativeRegionExpressionKind::REFERENCE;
				input.source_index = producer_idx;
				input.return_type = input_types[producer_idx];
			}
		} else {
			input.kind = SljitNativeRegionExpressionKind::REFERENCE;
			input.source_index = input_types.size() + output_idx - lhs_count;
			input.return_type = output_types[output_idx];
		}
		input.references_region_input = true;
		reference_sources.push_back(
		    input.kind == SljitNativeRegionExpressionKind::REFERENCE ? input.source_index : DConstants::INVALID_INDEX);
		inputs.push_back(std::move(input));
	}
	return true;
}

shared_ptr<SljitJoinAggregateProjection>
SljitPrepareJoinAggregateProjection(const vector<SljitExecutableRegionOp> &ops,
                                    const SljitFullPipelinePrimitiveSequence &sequence,
                                    const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	const auto &post_join = primitive.post_join_projection;
	const auto &join = ops[post_join.hash_join_idx];
	auto result = make_shared_ptr<SljitJoinAggregateProjection>();
	auto &semantic = result->semantic;
	semantic.kind = SljitNativeRegionOpKind::PROJECTION;
	if (!post_join.HasProjectionChain()) {
		if (ops[primitive.aggregate_idx].aggregate_update.plan.input_types != join.output_types) {
			return nullptr;
		}
		semantic.input_types = join.output_types;
		semantic.output_types = join.output_types;
		semantic.output_not_null.assign(join.output_types.size(), false);
		for (idx_t idx = 0; idx < join.output_types.size(); idx++) {
			SljitExecutableRegionExpression expression;
			expression.plan.kind = SljitNativeRegionExpressionKind::REFERENCE;
			expression.plan.source_index = idx;
			expression.plan.return_type = join.output_types[idx];
			semantic.projections.push_back(std::move(expression));
		}
	} else if (post_join.first_projection_idx == post_join.final_projection_idx) {
		const auto &original = ops[post_join.first_projection_idx];
		semantic.input_types = original.input_types;
		semantic.output_types = original.output_types;
		semantic.output_not_null = original.output_not_null;
		for (const auto &expression : original.projections) {
			semantic.projections.emplace_back();
			SljitBuildBorrowedProjectionExpression(expression, semantic.projections.back());
		}
	} else {
		if (!SljitBuildProjectionChainComposedProjection(ops, post_join.first_projection_idx,
		                                                 post_join.final_projection_idx, semantic)) {
			return nullptr;
		}
		result->owned_code_size += semantic.CodeSize();
	}
	optional_ptr<const SljitBatchLayout> layout;
	for (idx_t idx = 0; idx < sequence.Count(); idx++) {
		const auto &step = sequence.Step(idx);
		if (step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION &&
		    step.hash_join_probe_selection.hash_join_idx == post_join.hash_join_idx) {
			layout = step.hash_join_probe_selection.output_layout.get();
		}
	}
	optional_ptr<const SljitExecutableRegionOp> producer;
	if (layout) {
		producer = &ops[layout->ProducerIndex()];
	}
	const auto &probe = join.hash_join_probe.plan;
	const auto &input_types = producer ? producer->input_types : probe.input_types;
	if (!SljitTryPrepareJoinAggregateInputs(probe.operator_info.hash_join_contract.lhs_output_column_indices,
	                                        join.output_types, input_types, semantic, layout, producer, result->inputs,
	                                        result->owned_code_size)) {
		return nullptr;
	}
	return result;
}

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
	       kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION;
}

bool SljitFullPipelineHasExactFilterProbeHashBuild(const vector<SljitExecutableRegionOp> &ops,
                                                   const SljitFullPipelinePrimitiveSequence &sequence) {
	// Steps ahead of the final probe only shape its input: the exact probe feeding the build is the work either way.
	const auto count = sequence.Count();
	if (count < 3 || sequence.Step(count - 2).kind != SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION ||
	    sequence.Step(count - 1).kind != SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK) {
		return false;
	}
	const auto probe_idx = sequence.Step(count - 2).hash_join_probe_selection.hash_join_idx;
	if (probe_idx >= ops.size() || ops[probe_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		throw InternalException("SLJIT bound recipe references an invalid hash join probe");
	}
	auto &probe = ops[probe_idx].hash_join_probe.plan;
	return probe.exact_source_filter_binding != DConstants::INVALID_INDEX && probe.keys.size() == 1 &&
	       probe.equality_key_count == 1 && !probe.residual_predicate && !probe.mark_build_match &&
	       probe.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
}

void SljitCollectFusedFilterOwner(const vector<SljitExecutableRegionOp> &ops,
                                  const SljitFullPipelinePrimitiveStep &step, vector<idx_t> &owners) {
	// A filtered aggregate step owns its filter; it must be one its binder admits, so the step can execute it
	if (step.kind == SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE &&
	    step.ungrouped_aggregate_update.strategy ==
	        SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
		auto &primitive = step.ungrouped_aggregate_update;
		if (!SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, primitive.filter_idx,
		                                                           primitive.aggregate_idx)) {
			throw InternalException("SLJIT fused filter recipe has no executable ungrouped aggregate owner");
		}
		owners.push_back(primitive.filter_idx);
	} else if (step.kind == SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE &&
	           step.grouped_aggregate_update.strategy ==
	               SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
		auto &primitive = step.grouped_aggregate_update;
		if (!SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, primitive.filter_idx, primitive.aggregate_idx)) {
			throw InternalException("SLJIT fused filter recipe has no executable grouped aggregate owner");
		}
		owners.push_back(primitive.filter_idx);
	}
}

} // namespace

//! Composition and code generation belong to recipe construction, never execution-local binding.
bool SljitTryPrepareJoinAggregateInputs(const vector<idx_t> &lhs_output_columns,
                                        const vector<LogicalType> &output_types, const vector<LogicalType> &input_types,
                                        const SljitExecutableRegionOp &projection,
                                        optional_ptr<const SljitBatchLayout> producer_layout,
                                        optional_ptr<const SljitExecutableRegionOp> producer,
                                        vector<SljitExecutableRegionExpression> &prepared, idx_t &owned_code_size) {
	vector<SljitNativeRegionExpressionPlan> inputs;
	vector<idx_t> reference_sources;
	if (!SljitTryBuildJoinAggregateInputProjection(lhs_output_columns, output_types, input_types, producer_layout,
	                                               producer, inputs, reference_sources)) {
		return false;
	}
	for (const auto &original : projection.projections) {
		SljitExecutableRegionExpression expression;
		SljitBuildBorrowedProjectionExpression(original, expression);
		if (!SljitTryRemapExecutableExpressionInputs(reference_sources, expression)) {
			SljitNativeRegionExpressionPlan composed;
			string error;
			expression = SljitExecutableRegionExpression();
			if (!TryComposeNativeSemanticProjection(inputs, SljitCopySemanticExpressionPlan(original), composed,
			                                        false) ||
			    !SljitPrepareAndCompileExecutableRegionExpression(composed, false, expression, error)) {
				return false;
			}
			owned_code_size += expression.CodeSize();
		}
		prepared.push_back(std::move(expression));
	}
	return true;
}

SljitFullPipelineRecipe
SljitFinalizeFullPipelinePrimitiveRecipe(const vector<SljitExecutableRegionOp> &ops,
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

	auto &terminal = primitive_sequence.Step(primitive_sequence.Count() - 1);
	if (terminal.kind == SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE) {
		terminal.post_join_projection_aggregate.projection =
		    SljitPrepareJoinAggregateProjection(ops, primitive_sequence, terminal.post_join_projection_aggregate);
	}
	const bool direct_source_hash_build = terminal.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK &&
	                                      terminal.hash_join_build_sink.direct_source_ingress;

	SljitFullPipelineRecipe recipe;
	recipe.primitive_sequence = std::move(primitive_sequence);
	recipe.direct_aggregate_consumer = direct_aggregate_consumer;
	recipe.has_scan_filter_executable_body =
	    direct_source_hash_build || SljitFullPipelineHasExactFilterProbeHashBuild(ops, recipe.primitive_sequence);
	recipe.fused_filter_owners = std::move(fused_filter_owners);
	return recipe;
}

bool SljitNativeTailCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx) {
	if (tail_start_idx >= ops.size()) {
		return false;
	}
	auto &tail = ops[tail_start_idx];
	if (tail.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE && tail.aggregate_update.plan.UsesPrimitivePayloads()) {
		return false;
	}
	// Intra-tail soundness: each op's declared input references (the single
	// authority for what its execution dereferences) must be satisfied by the
	// previous op's output layout. Fused input transforms are invisible to
	// delegation, so an unsatisfied reference means the tail must be refused.
	for (idx_t op_idx = tail_start_idx + 1; op_idx < ops.size(); op_idx++) {
		if (ops[op_idx - 1].output_types.empty()) {
			continue;
		}
		if (!ops[op_idx].DeclaredInputConstraints().SatisfiedBy(ops[op_idx - 1].output_types)) {
			return false;
		}
	}
	// The tail hands DuckDB's aggregate sink the layout the ops before it produce. An aggregate whose payloads absorbed
	// a projection reads that projection's input instead of its output, and the absorbed expressions are gone from the
	// tail: matching types cannot tell (p_size * 3) % 1000 from the p_size it was computed from
	for (idx_t op_idx = tail_start_idx; op_idx < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    op.aggregate_update.plan.PayloadsWereComposedThroughProjection()) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
