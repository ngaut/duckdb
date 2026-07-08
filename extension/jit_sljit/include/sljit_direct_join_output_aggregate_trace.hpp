//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_trace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_descriptor.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static void
SljitRecordJoinProjectionAggregateDescriptorShape(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                                  const SljitJoinProjectionAggregateDescriptor &descriptor,
                                                  const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                  idx_t count) {
	const auto input_prefix = descriptor.projection_idx == DConstants::INVALID_INDEX
	                              ? string("join_aggregate.input.")
	                              : string("projection_aggregate.input.");
	const auto group_prefix = descriptor.projection_idx == DConstants::INVALID_INDEX
	                              ? string("join_aggregate.group.")
	                              : string("projection_aggregate.group.");
	for (auto &source : descriptor.input_sources) {
		switch (source.kind) {
		case SljitJoinProjectionAggregateInputKind::UNUSED:
			break;
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

static bool SljitPostJoinProjectionUsesChain(const SljitPostJoinProjectionStrategy &post_join_projection) {
	return post_join_projection.first_projection_idx != post_join_projection.final_projection_idx;
}

static const char *
SljitDirectJoinOutputAggregateUnsupportedPrefix(const SljitPostJoinProjectionStrategy &post_join_projection) {
	if (SljitPostJoinProjectionUsesChain(post_join_projection)) {
		return "projection_aggregate.chain_unsupported.";
	}
	return "projection_aggregate.unsupported.";
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

} // namespace duckdb
