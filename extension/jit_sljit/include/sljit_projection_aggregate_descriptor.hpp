//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_join_projection_aggregate_input_binding.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_ungrouped_descriptor.hpp"
#include "sljit_projection_chain_primitive.hpp"
#include "sljit_projection_perfect_hash_aggregate_descriptor.hpp"

namespace duckdb {

static bool SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    idx_t aggregate_idx, SljitJoinProjectionAggregateDescriptor &descriptor,
    const SljitJoinAggregateProjection &prepared) {
	if (aggregate_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return descriptor.Block("hash_join_binding");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return descriptor.Block("hash_join_shape");
	}
	optional_ptr<SljitExecutableRegionOp> producer_projection_op;
	if (descriptor.producer_layout) {
		const auto producer_idx = descriptor.producer_layout->ProducerIndex();
		if (producer_idx >= ops.size() || ops[producer_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
			return descriptor.Block("producer_projection");
		}
		producer_projection_op = &ops[producer_idx];
	}
	auto &aggregate_op = ops[aggregate_idx];
	bool built;
	if (aggregate_op.AggregateUpdate().plan.sink_info.Kind() == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		built = SljitTryBuildProjectionUngroupedAggregateDescriptor(binding, aggregate_op, descriptor,
		                                                            producer_projection_op);
	} else if (aggregate_op.AggregateUpdate().plan.sink_info.Kind() ==
	           ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		built = SljitTryBuildProjectionPerfectHashAggregateDescriptor(aggregate_op, descriptor);
	} else {
		built = SljitTryBuildProjectionRowPointerAggregateDescriptor(binding, ops[hash_join_idx].HashJoinProbe().plan,
		                                                             aggregate_op, descriptor, producer_projection_op);
	}
	const auto &input_types = producer_projection_op ? producer_projection_op->input_types
	                                                 : ops[hash_join_idx].HashJoinProbe().plan.input_types;
	if (!built || !SljitTryBindJoinProjectionAggregateInputs(binding, input_types, descriptor, prepared.inputs)) {
		return false;
	}
	descriptor.MarkReady();
	return true;
}

static bool SljitBuildKeyDomainFitsInt32(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
		return true;
	default:
		return false;
	}
}

static bool SljitBuildKeyDomainFitsSignedTarget(PhysicalType source_type, PhysicalType target_type) {
	switch (target_type) {
	case PhysicalType::INT8:
		switch (source_type) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			return true;
		default:
			return false;
		}
	case PhysicalType::INT16:
		switch (source_type) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::UINT8:
			return true;
		default:
			return false;
		}
	case PhysicalType::INT32:
		return SljitBuildKeyDomainFitsInt32(source_type);
	default:
		return false;
	}
}

static void SljitApplyJoinProjectionGroupCastProofs(vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                    bool source_key0_range_fits_int32) {
	for (auto &source : group_sources) {
		if (!ExecutionGroupKeyCastIsNarrowingIntegral(source.cast_kind)) {
			source.unchecked_integral_cast = false;
			continue;
		}
		const bool source_key0_range_proven = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
		                                      source_key0_range_fits_int32 && source.hash_join_condition_idx == 0;
		const bool matched_build_key_proves_range =
		    source.hash_join_condition_idx != DConstants::INVALID_INDEX &&
		    SljitBuildKeyDomainFitsSignedTarget(source.hash_join_build_key_physical_type, source.target_physical_type);
		source.unchecked_integral_cast = source_key0_range_proven || matched_build_key_proves_range;
	}
}

static bool SljitTryBuildPostJoinProjectionAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor, optional_ptr<const SljitBatchLayout> output_layout = nullptr) {
	if (descriptor.Built()) {
		if (!descriptor.ProducerLayoutMatches(output_layout)) {
			return descriptor.Block("producer_shape_changed");
		}
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	descriptor.producer_layout = output_layout;
	if (!post_join_projection.aggregate_projection) {
		return descriptor.Block("aggregate_projection_not_prepared");
	}
	auto &prepared = *post_join_projection.aggregate_projection;
	descriptor.BorrowProjection(post_join_projection.final_projection_idx, prepared.semantic);
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, post_join_projection.hash_join_idx, aggregate_idx, descriptor, prepared);
}

} // namespace duckdb
