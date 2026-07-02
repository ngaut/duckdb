//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_between_join_sidecar_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_decimal64_payload_runtime.hpp"
#include "sljit_final_projection_aggregate_descriptor.hpp"
#include "sljit_hash_join_output_reference_runtime.hpp"
#include "sljit_hash_join_rhs_projection_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static void SljitInitializeBetweenJoinCompressedPassthroughs(Allocator &allocator,
                                                             SljitExecutableRegionOp &between_join_projection_op,
                                                             SljitExecutableRegionOp &first_hash_join_op,
                                                             SljitBetweenJoinSidecars &sidecars) {
	if (sidecars.CompressedPassthroughsPlanned(between_join_projection_op.projections.size())) {
		return;
	}
	sidecars.compressed_passthrough_by_between_projection.assign(between_join_projection_op.projections.size(),
	                                                             DConstants::INVALID_INDEX);
	vector<LogicalType> passthrough_types;
	for (idx_t projection_idx = 0; projection_idx < between_join_projection_op.projections.size(); projection_idx++) {
		auto &plan = between_join_projection_op.projections[projection_idx].plan;
		if (plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
		    plan.return_type.id() != LogicalTypeId::VARCHAR ||
		    plan.source_index >= first_hash_join_op.output_types.size()) {
			continue;
		}
		auto &compressed_type = first_hash_join_op.output_types[plan.source_index];
		if (!DirectAppendSupportsFixedSizeType(compressed_type)) {
			continue;
		}
		SljitBetweenJoinCompressedPassthrough passthrough;
		passthrough.between_projection_idx = projection_idx;
		passthrough.first_join_output_source_idx = plan.source_index;
		passthrough.sidecar_idx = passthrough_types.size();
		sidecars.compressed_passthrough_by_between_projection[projection_idx] = passthrough.sidecar_idx;
		passthrough_types.push_back(compressed_type);
		sidecars.compressed_passthroughs.push_back(passthrough);
	}
	if (!passthrough_types.empty()) {
		sidecars.compressed_passthrough_batch.Ensure(allocator, passthrough_types);
	}
}

static bool SljitTryPrepareBetweenJoinSidecarAppend(SljitRegionExecutionScratch &scratch, idx_t first_hash_join_idx,
                                                    SljitDataChunkBatch &sidecar_batch, bool require_initialized,
                                                    idx_t target_offset, idx_t count,
                                                    optional_ptr<const ExecutionHashJoinProbeBinding> &binding,
                                                    const char *&blocker) {
	binding = nullptr;
	blocker = nullptr;
	if (!scratch.HasOperatorBinding(first_hash_join_idx)) {
		blocker = "first_join_binding";
		return false;
	}
	if ((require_initialized && !sidecar_batch.Initialized()) || sidecar_batch.Count() != target_offset) {
		blocker = "target_offset";
		return false;
	}
	if (target_offset + count > STANDARD_VECTOR_SIZE) {
		blocker = "capacity";
		return false;
	}
	binding = &scratch.OperatorBinding(first_hash_join_idx).hash_join_probe;
	return true;
}

static bool SljitAppendBetweenJoinCompressedPassthroughs(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t first_hash_join_idx,
    idx_t between_projection_idx, SljitExecutableRegionOp &between_join_projection_op,
    SljitExecutableRegionOp &first_hash_join_op, SljitBetweenJoinSidecars &sidecars, DataChunk &first_join_input,
    const SelectionVector &match_selection, Vector &row_pointers, idx_t target_offset, idx_t count) {
	SljitInitializeBetweenJoinCompressedPassthroughs(runtime.GetAllocator(), between_join_projection_op,
	                                                 first_hash_join_op, sidecars);
	if (sidecars.compressed_passthroughs.empty()) {
		return true;
	}
	auto record_passthrough_blocker = [&](const char *reason) {
		auto path = string("direct_between_join_compressed_passthrough_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
	};
	optional_ptr<const ExecutionHashJoinProbeBinding> binding;
	const char *blocker;
	if (!SljitTryPrepareBetweenJoinSidecarAppend(scratch, first_hash_join_idx, sidecars.compressed_passthrough_batch,
	                                             false, target_offset, count, binding, blocker)) {
		record_passthrough_blocker(blocker);
		return false;
	}
	auto stage_start = SljitRegionStageStart(runtime);
	for (auto &passthrough : sidecars.compressed_passthroughs) {
		if (passthrough.sidecar_idx >= sidecars.compressed_passthrough_batch.ColumnCount()) {
			record_passthrough_blocker("sidecar_index");
			return false;
		}
		auto &target = sidecars.compressed_passthrough_batch.chunk.data[passthrough.sidecar_idx];
		if (!SljitTryMaterializeHashJoinOutputReferenceToBatch(*binding, first_join_input, match_selection,
		                                                       row_pointers, passthrough.first_join_output_source_idx,
		                                                       target, target_offset, count)) {
			record_passthrough_blocker("source");
			return false;
		}
	}
	SljitFinishDirectProjectionBatchTargets(sidecars.compressed_passthrough_batch.chunk, target_offset + count, false);
	RecordSljitRegionStageRuntime(runtime, between_projection_idx, between_join_projection_op.kind,
	                              "between_join_compressed_passthrough_projection", stage_start);
	RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
	                             "direct_between_join_compressed_passthrough_projection", count);
	return true;
}

static bool SljitAppendBetweenJoinPrecomputedPayloads(ExecutionRegionRuntime &runtime,
                                                      SljitRegionExecutionScratch &scratch, idx_t first_hash_join_idx,
                                                      idx_t between_projection_idx, idx_t second_projection_idx,
                                                      SljitExecutableRegionOp &between_join_projection_op,
                                                      SljitBetweenJoinSidecars &sidecars, DataChunk &first_join_input,
                                                      const SelectionVector &match_selection, Vector &row_pointers,
                                                      idx_t target_offset, idx_t count) {
	if (sidecars.precomputed_payloads.empty()) {
		return true;
	}
	auto record_payload_blocker = [&](const char *reason) {
		auto path = string("direct_between_join_precomputed_payload_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
	};
	optional_ptr<const ExecutionHashJoinProbeBinding> binding;
	const char *blocker;
	if (!SljitTryPrepareBetweenJoinSidecarAppend(scratch, first_hash_join_idx, sidecars.precomputed_payload_batch, true,
	                                             target_offset, count, binding, blocker)) {
		record_payload_blocker(blocker);
		return false;
	}
	auto stage_start = SljitRegionStageStart(runtime);
	bool used_direct_decimal64_payload = false;
	for (auto &payload : sidecars.precomputed_payloads) {
		if (payload.sidecar_idx >= sidecars.precomputed_payload_batch.ColumnCount()) {
			record_payload_blocker("sidecar_index");
			return false;
		}
		string decimal64_payload_miss;
		if (SljitTryMaterializeHashJoinAllValidDecimal64ExpressionToBatch(
		        *binding, payload.first_join_expr, first_join_input, match_selection, row_pointers,
		        sidecars.precomputed_payload_batch.chunk, payload.sidecar_idx, target_offset, count,
		        payload.decimal64_discounted_amount_program, &decimal64_payload_miss)) {
			used_direct_decimal64_payload = true;
			continue;
		}
		if (!decimal64_payload_miss.empty()) {
			auto path =
			    string("direct_between_join_precomputed_payload_decimal64_unsupported.") + decimal64_payload_miss;
			RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
		}
		auto &adapter_scratch = scratch.ExpressionAdapterScratch(second_projection_idx, payload.second_projection_idx);
		if (!SljitTryMaterializeHashJoinMixedProjectionToBatch(
		        *binding, payload.first_join_expr, first_join_input, match_selection, row_pointers,
		        sidecars.precomputed_payload_batch.chunk, payload.sidecar_idx, target_offset, count, adapter_scratch)) {
			record_payload_blocker("payload_projection");
			return false;
		}
	}
	SljitFinishDirectProjectionBatchTargets(sidecars.precomputed_payload_batch.chunk, target_offset + count, false);
	RecordSljitRegionStageRuntime(runtime, between_projection_idx, between_join_projection_op.kind,
	                              "between_join_precomputed_payload_projection", stage_start);
	RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
	                             "direct_between_join_precomputed_payload_projection", count);
	if (used_direct_decimal64_payload) {
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
		                             "direct_between_join_precomputed_payload_decimal64_projection", count);
	}
	return true;
}

static bool SljitCopySecondJoinPrecomputedPayloadsToProjection(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t second_hash_join_idx,
    idx_t second_projection_idx, SljitNativeRegionOpKind second_projection_kind, SljitBetweenJoinSidecars &sidecars,
    bool precomputed_payload_omitted, idx_t expected_sidecar_count, DataChunk &direct_projection, idx_t count) {
	if (!precomputed_payload_omitted) {
		return true;
	}
	auto record_payload_blocker = [&](const char *reason) {
		auto path = string("direct_second_join_precomputed_payload_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, second_projection_kind, path.c_str(), count);
	};
	if (!sidecars.precomputed_payload_batch.Initialized() ||
	    sidecars.precomputed_payload_batch.Count() != expected_sidecar_count) {
		record_payload_blocker("sidecar_batch");
		return false;
	}
	auto stage_start = SljitRegionStageStart(runtime);
	for (auto &payload : sidecars.precomputed_payloads) {
		if (payload.sidecar_idx >= sidecars.precomputed_payload_batch.ColumnCount() ||
		    payload.second_projection_idx >= direct_projection.ColumnCount()) {
			record_payload_blocker("sidecar_index");
			return false;
		}
		SljitDirectProjectionBatchPassthrough passthrough;
		passthrough.output_idx = payload.second_projection_idx;
		passthrough.source = &sidecars.precomputed_payload_batch.chunk.data[payload.sidecar_idx];
		passthrough.selection = &scratch.FilterSelection(second_hash_join_idx);
		passthrough.trace_phase = "direct_batch_expression.precomputed_payload";
		if (!SljitTryCopyDirectProjectionPassthroughToBatch(
		        passthrough, direct_projection.data[payload.second_projection_idx], 0, count)) {
			record_payload_blocker("copy");
			return false;
		}
	}
	RecordSljitRegionStageRuntime(runtime, second_projection_idx, second_projection_kind,
	                              "second_join_precomputed_payload_passthrough", stage_start);
	RecordSljitRegionRuntimePath(runtime, second_projection_kind, "direct_second_join_precomputed_payload_passthrough",
	                             count);
	return true;
}

} // namespace duckdb
