//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_hash_join_projection_aggregate_input_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_dense_group_domain.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_descriptor.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_ungrouped_aggregate_payload_update_runtime.hpp"

namespace duckdb {

static void
SljitRecordJoinProjectionAggregateDescriptorShape(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                                  const SljitJoinProjectionAggregateDescriptor &descriptor,
                                                  const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                  idx_t count) {
	const auto input_prefix = descriptor.projection_idx == DConstants::INVALID_INDEX
	                              ? string("direct_join_aggregate_input.")
	                              : string("direct_projection_aggregate_input.");
	const auto group_prefix = descriptor.projection_idx == DConstants::INVALID_INDEX
	                              ? string("direct_join_aggregate_group.")
	                              : string("direct_projection_aggregate_group.");
	for (auto &source : descriptor.input_sources) {
		switch (source.kind) {
		case SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT:
			RecordSljitRegionRuntimePath(runtime, kind, (input_prefix + "projection_output").c_str(), count);
			break;
		case SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT:
			RecordSljitRegionRuntimePath(runtime, kind, (input_prefix + "hash_join_lhs_input").c_str(), count);
			break;
		default:
			RecordSljitRegionRuntimePath(runtime, kind, (input_prefix + "unknown").c_str(), count);
			break;
		}
	}
	for (auto &source : group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "input_vector").c_str(), count);
			} else if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
			           source.hash_join_condition_idx == 0 && source.unchecked_integral_cast) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "input_vector_cast_key0_unchecked").c_str(),
				                             count);
			} else if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
			           source.hash_join_condition_idx == 0) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "input_vector_cast_key0_checked").c_str(),
				                             count);
			} else if (source.unchecked_integral_cast) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "input_vector_cast_unchecked").c_str(),
				                             count);
			} else {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "input_vector_cast").c_str(), count);
			}
		} else if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "row_pointer_field").c_str(), count);
			} else if (source.unchecked_integral_cast) {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "row_pointer_field_cast_unchecked").c_str(),
				                             count);
			} else {
				RecordSljitRegionRuntimePath(runtime, kind, (group_prefix + "row_pointer_field_cast").c_str(), count);
			}
		}
	}
}

static string
SljitDescribeJoinProjectionAggregatePayloadSources(const SljitJoinProjectionAggregateDescriptor &descriptor,
                                                   DataChunk &aggregate_input) {
	string result = "payloads";
	for (auto source_idx : descriptor.payload_source_indices) {
		result += "_" + to_string(source_idx);
	}
	result += "_columns_" + to_string(aggregate_input.ColumnCount());
	result += "_inputs";
	for (auto &source : descriptor.input_sources) {
		switch (source.kind) {
		case SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT:
			result += "_projection_" + to_string(source.projection_idx);
			break;
		case SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT:
			result += "_lhs_" + to_string(source.input_idx);
			break;
		default:
			result += "_unknown";
			break;
		}
	}
	return result;
}

static bool SljitTryBuildDirectJoinOutputAggregateDenseDomain(const SljitDirectJoinOutputAggregateStrategy &strategy,
                                                              const SljitJoinProjectionAggregateDescriptor &descriptor,
                                                              ExecutionDenseGroupDomain &domain) {
	domain = ExecutionDenseGroupDomain();
	if (!strategy.source_distinct_counts || !strategy.source_min_values || !strategy.source_max_values ||
	    descriptor.group_sources.size() != 1) {
		return false;
	}
	auto &group_source = descriptor.group_sources[0];
	if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    group_source.input_vector_index >= descriptor.input_sources.size()) {
		return false;
	}
	if (group_source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE &&
	    group_source.cast_kind != ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32) {
		return false;
	}
	auto &input_source = descriptor.input_sources[group_source.input_vector_index];
	if (input_source.kind != SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
		return false;
	}
	const auto source_idx = input_source.input_idx;
	if (source_idx >= strategy.source_distinct_counts->size() || source_idx >= strategy.source_min_values->size() ||
	    source_idx >= strategy.source_max_values->size()) {
		return false;
	}
	return SljitTryBuildDenseGroupDomainFromStats(
	    group_source.target_physical_type, (*strategy.source_distinct_counts)[source_idx],
	    (*strategy.source_min_values)[source_idx], (*strategy.source_max_values)[source_idx], domain);
}

static bool SljitFlushPendingRowPointerAggregateBatch(ExecutionRegionRuntime &runtime, idx_t aggregate_idx,
                                                      SljitExecutableRegionOp &aggregate_op,
                                                      SljitJoinProjectionAggregateDescriptor &descriptor,
                                                      SljitPendingRowPointerAggregateBatch &batch) {
	const auto pending_count = batch.Count();
	if (pending_count == 0) {
		return false;
	}
	if (!batch.scratch) {
		throw InternalException("SLJIT batched direct row-pointer aggregate has no scratch state");
	}
	const auto source_key0_int64_to_int32_unchecked = batch.source_key0_int64_to_int32_unchecked;
	SljitApplyJoinProjectionGroupCastProofs(descriptor.group_sources, source_key0_int64_to_int32_unchecked);
	auto batch_group_sources = descriptor.group_sources;
	auto &payload_input = batch.PayloadInput();
	SljitApplyInputVectorGroupBatchCastProofs(payload_input, batch_group_sources, pending_count);
	SljitApplyRowPointerGroupBatchCastProofs(batch.row_pointers, batch_group_sources, pending_count);
	if (!SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
	        runtime, runtime.ExecutionOperators(), *batch.scratch, aggregate_idx, aggregate_op, payload_input,
	        batch.row_pointers, batch_group_sources, descriptor.payload_source_indices, true,
	        batch.deferred_grouped_finish, source_key0_int64_to_int32_unchecked)) {
		throw InternalException("SLJIT batched direct row-pointer aggregate update failed");
	}
	RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projection_row_pointer_grouped_update",
	                             pending_count);
	batch.Reset();
	return false;
}

static bool SljitCanPreclassifyStringSetComplementarySumBatch(SljitExecutableRegionOp &aggregate_op,
                                                              SljitJoinProjectionAggregateDescriptor &descriptor,
                                                              DataChunk &aggregate_input,
                                                              SljitStringSetComplementarySumDescriptor &classification) {
	return ExecutionRowPointerGroupKeySourcesAreRowPointerFields(descriptor.group_sources) &&
	       SljitTryBindStringSetComplementarySumDescriptor(aggregate_op, descriptor.payload_source_indices,
	                                                       classification) &&
	       SljitStringSetComplementarySumInputIsVarchar(aggregate_input, classification);
}

static void SljitAppendPreclassifiedStringSetComplementarySumBatch(
    SljitPendingRowPointerAggregateBatch &batch, const SljitStringSetComplementarySumDescriptor &classification,
    DataChunk &aggregate_input) {
	auto &classified_input = batch.preclassified_input;
	auto &target = classified_input.data[0];
	const auto old_count = classified_input.size();
	const auto append_count = aggregate_input.size();
	auto target_data = FlatVector::GetDataMutable<uint8_t>(target);
	auto &target_validity = FlatVector::ValidityMutable(target);
	UnifiedVectorFormat predicate_format;
	aggregate_input.data[classification.predicate_source_idx].ToUnifiedFormat(predicate_format);
	auto predicate_data = UnifiedVectorFormat::GetData<string_t>(predicate_format);
	auto predicate_sel = predicate_format.sel;
	auto &predicate_validity = predicate_format.validity;
	for (idx_t idx = 0; idx < append_count; idx++) {
		const auto target_idx = old_count + idx;
		const auto predicate_idx = predicate_sel->get_index(idx);
		if (!predicate_validity.RowIsValid(predicate_idx)) {
			target_validity.SetInvalid(target_idx);
			target_data[target_idx] = 0;
			continue;
		}
		target_validity.SetValid(target_idx);
		auto predicate = predicate_data[predicate_idx];
		target_data[target_idx] = SljitStringEqualsConstant(predicate, classification.constants[0]) ||
		                                  SljitStringEqualsConstant(predicate, classification.constants[1])
		                              ? 1
		                              : 0;
	}
	classified_input.SetChildCardinality(old_count + append_count);
}

static void SljitAppendPendingRowPointerAggregateBatch(
    ExecutionRegionRuntime &runtime, idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor, SljitPendingRowPointerAggregateBatch &batch,
    SljitRegionExecutionScratch &scratch, optional_ptr<bool> deferred_grouped_finish, DataChunk &aggregate_input,
    Vector &row_pointers, bool source_key0_int64_to_int32_unchecked) {
	batch.scratch = &scratch;
	batch.deferred_grouped_finish = deferred_grouped_finish;
	SljitStringSetComplementarySumDescriptor classification;
	const bool use_preclassified =
	    SljitCanPreclassifyStringSetComplementarySumBatch(aggregate_op, descriptor, aggregate_input, classification);
	if (batch.Count() != 0 && batch.uses_preclassified_input != use_preclassified) {
		SljitFlushPendingRowPointerAggregateBatch(runtime, aggregate_idx, aggregate_op, descriptor, batch);
	}
	if (use_preclassified) {
		batch.EnsurePreclassified(runtime.GetAllocator());
	} else {
		batch.Ensure(runtime.GetAllocator(), descriptor.input_types);
	}
	if (batch.Count() != 0 && batch.source_key0_int64_to_int32_unchecked != source_key0_int64_to_int32_unchecked) {
		SljitFlushPendingRowPointerAggregateBatch(runtime, aggregate_idx, aggregate_op, descriptor, batch);
		if (use_preclassified) {
			batch.EnsurePreclassified(runtime.GetAllocator());
		} else {
			batch.Ensure(runtime.GetAllocator(), descriptor.input_types);
		}
	}
	batch.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	if (batch.Count() + aggregate_input.size() > STANDARD_VECTOR_SIZE) {
		SljitFlushPendingRowPointerAggregateBatch(runtime, aggregate_idx, aggregate_op, descriptor, batch);
		batch.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
		if (use_preclassified) {
			batch.EnsurePreclassified(runtime.GetAllocator());
		} else {
			batch.Ensure(runtime.GetAllocator(), descriptor.input_types);
		}
	}
	if (use_preclassified) {
		SljitAppendPreclassifiedStringSetComplementarySumBatch(batch, classification, aggregate_input);
	} else if (batch.input.ColumnCount() == 0) {
		batch.input.SetChildCardinality(batch.Count() + aggregate_input.size());
	} else {
		batch.input.Append(aggregate_input, VectorAppendMode::ERROR_ON_NO_SPACE);
	}
	batch.row_pointers.Append(row_pointers, aggregate_input.size(), VectorAppendMode::ERROR_ON_NO_SPACE);
}

static bool SljitPostJoinProjectionUsesChain(const SljitPostJoinProjectionStrategy &post_join_projection) {
	return post_join_projection.first_projection_idx != post_join_projection.final_projection_idx;
}

static const char *
SljitDirectJoinOutputAggregateUnsupportedPrefix(const SljitPostJoinProjectionStrategy &post_join_projection) {
	if (SljitPostJoinProjectionUsesChain(post_join_projection)) {
		return "direct_projection_chain_row_pointer_aggregate_unsupported.";
	}
	return "direct_projection_row_pointer_aggregate_unsupported.";
}

static void SljitRecordDirectJoinOutputAggregateProjectionUnsupported(
    ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops,
    const SljitPostJoinProjectionStrategy &post_join_projection, const string &reason, idx_t count) {
	if (post_join_projection.trace_projection_idx == DConstants::INVALID_INDEX ||
	    post_join_projection.trace_projection_idx >= ops.size()) {
		return;
	}
	auto path = string(SljitDirectJoinOutputAggregateUnsupportedPrefix(post_join_projection)) + reason;
	RecordSljitRegionRuntimePath(runtime, ops[post_join_projection.trace_projection_idx].kind, path.c_str(), count);
}

static bool SljitFlushDirectJoinOutputAggregate(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                                SljitDirectJoinOutputAggregatePolicy &policy) {
	if (!policy.HasStrategy()) {
		return false;
	}
	auto &strategy = policy.Strategy();
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	auto &aggregate_op = ops[strategy.aggregate_idx];
	return SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, strategy.descriptor,
	                                                 strategy.pending_batch);
}

static bool SljitTryExecuteDirectJoinOutputPerfectHashAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &aggregate_input, optional_ptr<string> failure_reason) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (!aggregate_update.fused_payload_update_function || !aggregate_update.fused_payload_update_owns_group_lookup ||
	    !plan.use_primitive_payloads || !plan.use_perfect_hash_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size()) {
		return false;
	}
	if (sink_info.aggregates.empty()) {
		if (failure_reason) {
			*failure_reason = "shape";
		}
		return false;
	}
	auto &binding = SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, aggregate_input,
	                                            sink_info, "aggregate-update-runtime-binding-failed",
	                                            "SLJIT direct join-output perfect-hash aggregate update");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
		if (failure_reason) {
			*failure_reason = "binding";
		}
		return false;
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	if (!grouped_state.perfect_hash_layout.ready) {
		if (failure_reason) {
			*failure_reason = grouped_state.perfect_hash_layout.blocker.empty()
			                      ? string("perfect_hash_layout")
			                      : grouped_state.perfect_hash_layout.blocker;
		}
		return false;
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(op_idx, sink_info.aggregates, binding.aggregate_update.primitive);
	if (payload_lanes.size() != sink_info.aggregates.size()) {
		if (failure_reason) {
			*failure_reason = "payload_lanes";
		}
		return false;
	}
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	auto payload_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
	    aggregate_update.payloads, aggregate_update.fused_payload_update_function, sink_info.aggregates,
	    sink_info.groups, plan.group_expressions, sink_info.aggregate_contract, payload_lanes,
	    grouped_state.perfect_hash_layout, aggregate_input, nullptr, aggregate_input.size(), payload_scratch);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update_fused", payload_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "direct_join_output_perfect_hash_payload_update",
	                             aggregate_input.size());
	RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update_owns_perfect_hash_group_lookup",
	                             aggregate_input.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update",
	                                         aggregate_input.size());
	return true;
}

static bool SljitTryExecuteDirectJoinOutputAggregate(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitDirectJoinOutputAggregatePolicy &policy, SljitPostJoinProjectionStrategy &post_join_projection,
    DataChunk &join_input, const SelectionVector &match_selection, const SelectionVector &build_selection,
    Vector &row_pointers, DataChunk &join_output, optional_ptr<bool> deferred_grouped_finish,
    bool source_key0_int64_to_int32_unchecked = false, optional_ptr<const vector<idx_t>> output_column_map = nullptr,
    idx_t output_projection_idx = DConstants::INVALID_INDEX) {
	if (!policy.Enabled()) {
		return false;
	}
	auto &strategy = policy.Strategy();
	auto &descriptor = strategy.descriptor;
	strategy.last_failure.clear();
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	const bool has_projection_chain = post_join_projection.HasProjectionChain();
	const bool descriptor_ready =
	    has_projection_chain
	        ? SljitTryBuildPostJoinProjectionAggregateDescriptor(ops, scratch, post_join_projection,
	                                                             strategy.aggregate_idx, descriptor, output_column_map,
	                                                             output_projection_idx)
	        : SljitTryBuildSelectedJoinAggregateInputDescriptor(ops, scratch, post_join_projection.hash_join_idx,
	                                                            strategy.aggregate_idx, descriptor);
	if (!descriptor_ready) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
		                                                          descriptor.Blocker(), join_output.size());
		strategy.last_failure = descriptor.Blocker();
		strategy.disabled = true;
		return false;
	}
	descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = descriptor.input.chunk;
	aggregate_input.Reset();
	auto &aggregate_op = ops[strategy.aggregate_idx];
	if (has_projection_chain && descriptor.projection_idx == DConstants::INVALID_INDEX) {
		throw InternalException("SLJIT direct row-pointer aggregate descriptor has no projection index");
	}
	SljitApplyJoinProjectionGroupCastProofs(descriptor.group_sources, source_key0_int64_to_int32_unchecked);
	const auto aggregate_projection_idx = descriptor.projection_idx == DConstants::INVALID_INDEX
	                                          ? post_join_projection.trace_projection_idx
	                                          : descriptor.projection_idx;
	if (descriptor.output_to_projection.empty()) {
		aggregate_input.SetChildCardinality(join_output.size());
	} else if (SljitTryReferenceHashJoinProjectionAggregateInputsToChunk(
	               runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
	               descriptor.Projection(), join_input, match_selection, descriptor.input_sources, join_output.size(),
	               aggregate_input)) {
	} else {
		string materialize_failure;
		if (SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk(
		        runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx, descriptor.Projection(),
		        join_input, match_selection, build_selection, row_pointers, descriptor.input_sources,
		        join_output.size(), aggregate_input, optional_ptr<string>(&materialize_failure))) {
		} else if (!SljitJoinProjectionAggregateInputsUseOnlyProjectionOutputs(descriptor) ||
		           !SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
		               runtime, ops, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
		               descriptor.Projection(), join_input, match_selection, row_pointers, join_output, aggregate_input,
		               optional_ptr<const vector<idx_t>>(&descriptor.output_to_projection))) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection, "materialize",
			                                                          join_output.size());
			strategy.last_failure =
			    materialize_failure.empty() ? string("materialize") : string("materialize_") + materialize_failure;
			SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
			return false;
		}
	}
	if (aggregate_input.size() != join_output.size()) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection, "cardinality",
		                                                          join_output.size());
		strategy.last_failure = "cardinality";
		SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
		return false;
	}
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		if (descriptor.remapped_payloads.size() != sink_info.aggregates.size()) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
			                                                          "ungrouped_payloads", join_output.size());
			strategy.last_failure = "ungrouped_payloads";
			return false;
		}
		SljitExecuteNativeUngroupedAggregateUpdateWithPayloads(
		    runtime, runtime.ExecutionOperators(), scratch, strategy.aggregate_idx, aggregate_op, aggregate_input,
		    descriptor.remapped_payloads, "direct_join_output_ungrouped_payload_update",
		    "direct_join_output_ungrouped_payload_update", "direct_join_output_ungrouped_state_update");
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_join_output_ungrouped_update",
		                             aggregate_input.size());
		return true;
	}
	auto batch_group_sources = descriptor.group_sources;
	SljitApplyInputVectorGroupBatchCastProofs(aggregate_input, batch_group_sources, aggregate_input.size());
	SljitApplyRowPointerGroupBatchCastProofs(row_pointers, batch_group_sources, aggregate_input.size());
	SljitRecordJoinProjectionAggregateDescriptorShape(runtime, aggregate_op.kind, descriptor, batch_group_sources,
	                                                  join_output.size());
	string perfect_hash_failure;
	if (SljitTryExecuteDirectJoinOutputPerfectHashAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch,
	                                                              strategy.aggregate_idx, aggregate_op, aggregate_input,
	                                                              optional_ptr<string>(&perfect_hash_failure))) {
		return true;
	}
	if (!perfect_hash_failure.empty()) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
		                                                          string("perfect_hash_update_") + perfect_hash_failure,
		                                                          join_output.size());
	}

	if (!SljitDescriptorUsesRowPointerGroupSource(descriptor) &&
	    SljitGroupSourcesCanMaterializeFromInputVectors(aggregate_input, batch_group_sources)) {
		SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
		ExecutionDenseGroupDomain dense_domain;
		optional_ptr<const ExecutionDenseGroupDomain> dense_domain_ptr;
		if (SljitTryBuildDirectJoinOutputAggregateDenseDomain(strategy, descriptor, dense_domain)) {
			dense_domain_ptr = &dense_domain;
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projection_dense_group_domain",
			                             aggregate_input.size());
		}
		string input_vector_failure;
		if (!SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
		        runtime, runtime.ExecutionOperators(), scratch, strategy.aggregate_idx, aggregate_op, aggregate_input,
		        batch_group_sources, descriptor.payload_source_indices, true, deferred_grouped_finish,
		        source_key0_int64_to_int32_unchecked, dense_domain_ptr, optional_ptr<string>(&input_vector_failure))) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
			                                                          "input_vector_update", join_output.size());
			strategy.last_failure = input_vector_failure.empty()
			                            ? string("input_vector_update")
			                            : string("input_vector_update_") + input_vector_failure;
			strategy.last_failure +=
			    "_" + SljitDescribeJoinProjectionAggregatePayloadSources(descriptor, aggregate_input);
			return false;
		} else {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projection_input_vector_grouped_update",
			                             aggregate_input.size());
			return true;
		}
	}

	SljitAppendPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                           strategy.pending_batch, scratch, deferred_grouped_finish,
	                                           aggregate_input, row_pointers, source_key0_int64_to_int32_unchecked);
	return true;
}

} // namespace duckdb
