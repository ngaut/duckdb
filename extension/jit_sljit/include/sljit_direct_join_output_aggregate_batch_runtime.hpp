//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_batch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_input_complementary_sum_accumulator.hpp"

#include "sljit_dense_group_domain.hpp"
#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_input_vector_update_runtime.hpp"
#include "sljit_projection_aggregate_descriptor.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_row_pointer_grouped_aggregate_update_runtime.hpp"
#include "sljit_string_set_complementary_sum_runtime.hpp"

namespace duckdb {

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

static void SljitFlushPendingRowPointerAggregateBatch(ExecutionRegionRuntime &runtime, idx_t aggregate_idx,
                                                      SljitExecutableRegionOp &aggregate_op,
                                                      SljitJoinProjectionAggregateDescriptor &descriptor,
                                                      SljitPendingRowPointerAggregateBatch &batch) {
	const auto pending_count = batch.Count();
	if (pending_count == 0) {
		return;
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
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "projection_aggregate.row_pointer_grouped_update", pending_count);
	batch.Reset();
}

static void SljitFlushPendingInputVectorAggregateBatch(ExecutionRegionRuntime &runtime,
                                                       SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
                                                       SljitExecutableRegionOp &aggregate_op,
                                                       SljitDirectJoinOutputAggregateStrategy &strategy) {
	auto &batch = strategy.pending_input_vector_batch;
	const auto pending_count = batch.Count();
	if (pending_count == 0) {
		return;
	}
	auto batch_group_sources = strategy.descriptor.group_sources;
	auto &aggregate_input = batch.input;
	SljitApplyInputVectorGroupBatchCastProofs(aggregate_input, batch_group_sources, pending_count);
	ExecutionDenseGroupDomain dense_domain;
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain_ptr;
	if (SljitTryBuildDirectJoinOutputAggregateDenseDomain(strategy, strategy.descriptor, dense_domain)) {
		dense_domain_ptr = &dense_domain;
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "projection_aggregate.dense_group_domain",
		                             pending_count);
	}
	string input_vector_failure;
	if (!strategy.pending_preaggregated_groups) {
		strategy.pending_preaggregated_groups = make_shared_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>();
	}
	strategy.pending_preaggregated_scratch = &scratch;
	strategy.pending_preaggregated_deferred_grouped_finish = batch.deferred_grouped_finish;
	if (!SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
	        runtime, runtime.ExecutionOperators(), scratch, aggregate_idx, aggregate_op, aggregate_input,
	        batch_group_sources, strategy.descriptor.payload_source_indices, true, batch.deferred_grouped_finish,
	        batch.source_key0_int64_to_int32_unchecked, dense_domain_ptr, optional_ptr<string>(&input_vector_failure),
	        optional_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>(strategy.pending_preaggregated_groups.get()))) {
		throw InternalException("SLJIT batched direct input-vector aggregate update failed: %s",
		                        input_vector_failure.empty() ? "unknown" : input_vector_failure.c_str());
	}
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "projection_aggregate.input_vector_grouped_update", pending_count);
	batch.Reset();
}

static optional_ptr<const SljitStringSetComplementarySumDescriptor>
SljitGetDirectJoinOutputStringSetClassification(SljitDirectJoinOutputAggregateStrategy &strategy,
                                                SljitExecutableRegionOp &aggregate_op, DataChunk &aggregate_input) {
	auto &descriptor = strategy.descriptor;
	if (!strategy.string_set_classification_checked ||
	    strategy.string_set_classification_payload_sources != descriptor.payload_source_indices) {
		strategy.string_set_classification_checked = true;
		strategy.string_set_classification_payload_sources = descriptor.payload_source_indices;
		strategy.string_set_classification_ready = SljitTryBindStringSetComplementarySumDescriptor(
		    aggregate_op, descriptor.payload_source_indices, strategy.string_set_classification);
	}
	if (!strategy.string_set_classification_ready ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(descriptor.group_sources) ||
	    !SljitStringSetComplementarySumInputIsVarchar(aggregate_input, strategy.string_set_classification)) {
		return nullptr;
	}
	return &strategy.string_set_classification;
}

static void
SljitAppendPreclassifiedStringSetComplementarySumBatch(SljitPendingRowPointerAggregateBatch &batch,
                                                       const SljitStringSetComplementarySumDescriptor &classification,
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
		target_data[target_idx] =
		    SljitStringEqualsConstant(predicate, classification.constants[0], classification.signatures[0]) ||
		            SljitStringEqualsConstant(predicate, classification.constants[1], classification.signatures[1])
		        ? 1
		        : 0;
	}
	classified_input.SetChildCardinality(old_count + append_count);
}

static void SljitAppendPendingInputVectorAggregateBatch(
    ExecutionRegionRuntime &runtime, idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
    SljitDirectJoinOutputAggregateStrategy &strategy, SljitRegionExecutionScratch &scratch,
    optional_ptr<bool> deferred_grouped_finish, DataChunk &aggregate_input, bool source_key0_int64_to_int32_unchecked) {
	auto &batch = strategy.pending_input_vector_batch;
	batch.scratch = &scratch;
	batch.deferred_grouped_finish = deferred_grouped_finish;
	batch.Ensure(runtime.GetAllocator(), strategy.descriptor.input_types);
	if (batch.Count() != 0 && batch.source_key0_int64_to_int32_unchecked != source_key0_int64_to_int32_unchecked) {
		SljitFlushPendingInputVectorAggregateBatch(runtime, scratch, aggregate_idx, aggregate_op, strategy);
		batch.Ensure(runtime.GetAllocator(), strategy.descriptor.input_types);
	}
	batch.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	if (batch.Count() + aggregate_input.size() > STANDARD_VECTOR_SIZE) {
		SljitFlushPendingInputVectorAggregateBatch(runtime, scratch, aggregate_idx, aggregate_op, strategy);
		batch.Ensure(runtime.GetAllocator(), strategy.descriptor.input_types);
		batch.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	}
	batch.input.Append(aggregate_input, VectorAppendMode::ERROR_ON_NO_SPACE);
	if (batch.Count() == STANDARD_VECTOR_SIZE) {
		SljitFlushPendingInputVectorAggregateBatch(runtime, scratch, aggregate_idx, aggregate_op, strategy);
	}
}

static void SljitAppendPendingRowPointerAggregateBatch(
    ExecutionRegionRuntime &runtime, idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor, SljitPendingRowPointerAggregateBatch &batch,
    SljitRegionExecutionScratch &scratch, optional_ptr<bool> deferred_grouped_finish, DataChunk &aggregate_input,
    Vector &row_pointers, bool source_key0_int64_to_int32_unchecked,
    optional_ptr<const SljitStringSetComplementarySumDescriptor> classification) {
	batch.scratch = &scratch;
	batch.deferred_grouped_finish = deferred_grouped_finish;
	const bool use_preclassified = classification != nullptr;
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
		SljitAppendPreclassifiedStringSetComplementarySumBatch(batch, *classification, aggregate_input);
	} else if (batch.input.ColumnCount() == 0) {
		batch.input.SetChildCardinality(batch.Count() + aggregate_input.size());
	} else {
		batch.input.Append(aggregate_input, VectorAppendMode::ERROR_ON_NO_SPACE);
	}
	batch.row_pointers.Append(row_pointers, aggregate_input.size(), VectorAppendMode::ERROR_ON_NO_SPACE);
}

static void SljitFlushPendingDirectInputVectorAggregate(ExecutionRegionRuntime &runtime,
                                                        SljitExecutableRegionOp &aggregate_op,
                                                        SljitDirectJoinOutputAggregateStrategy &strategy) {
	if (strategy.pending_input_vector_batch.Count() != 0) {
		if (!strategy.pending_input_vector_batch.scratch) {
			throw InternalException("SLJIT batched direct input-vector aggregate has no scratch state");
		}
		SljitFlushPendingInputVectorAggregateBatch(runtime, *strategy.pending_input_vector_batch.scratch,
		                                           strategy.aggregate_idx, aggregate_op, strategy);
	}
	if (strategy.pending_preaggregated_groups && strategy.pending_preaggregated_groups->HasPending()) {
		if (!strategy.pending_preaggregated_scratch) {
			throw InternalException("SLJIT pending direct preaggregation has no scratch state");
		}
		auto &scratch = *strategy.pending_preaggregated_scratch;
		auto &binding = scratch.SinkBinding(strategy.aggregate_idx);
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
		    !binding.aggregate_update.grouped_state.state) {
			throw InternalException("SLJIT pending direct input-vector preaggregation has no grouped state");
		}
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(
		        runtime, scratch, strategy.aggregate_idx, aggregate_op, *strategy.pending_preaggregated_groups,
		        binding.aggregate_update.grouped_state, strategy.pending_preaggregated_deferred_grouped_finish)) {
			throw InternalException("SLJIT pending direct preaggregation flush failed");
		}
	}
}

static void SljitFlushDirectJoinOutputAggregate(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                                optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy_ptr) {
	if (!strategy_ptr) {
		return;
	}
	auto &strategy = *strategy_ptr;
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	auto &aggregate_op = ops[strategy.aggregate_idx];
	if (!SljitFlushJoinInputComplementarySumAccumulator(runtime, aggregate_op, strategy)) {
		throw InternalException("SLJIT join-input complementary accumulator flush failed");
	}
	SljitFlushPendingDirectInputVectorAggregate(runtime, aggregate_op, strategy);
	SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, strategy.descriptor,
	                                          strategy.pending_batch);
}

} // namespace duckdb
