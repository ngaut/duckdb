//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_fusion.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_region_aggregate_partial_fusion.hpp"
#include "sljit_region_aggregate_payload_fusion.hpp"
#include "sljit_region_aggregate_projection_fusion.hpp"

#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

static bool TryUsePrimitiveReferenceAggregateUpdate(const vector<LogicalType> &input_types,
                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                    bool render_diagnostics) {
	if (aggregate_update.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !aggregate_update.AggregateUpdate().CanInitializePrimitivePayloads()) {
		return false;
	}
	auto &sink = aggregate_update.AggregateUpdate().sink_info;
	if (sink.aggregates.empty()) {
		return false;
	}
	const bool grouped_state =
	    (sink.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	     sink.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) &&
	    sink.contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::READY;
	if (sink.Kind() != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE && !grouped_state) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		SljitNativeRegionExpressionPlan payload;
		if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, grouped_state,
		                                            render_diagnostics)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	const bool perfect_hash_group_lookup = grouped_state && SljitPerfectHashGroupLookupSupported(sink, payloads);

	aggregate_update.AggregateUpdate().input_types = input_types;
	aggregate_update.AggregateUpdate().payloads = std::move(payloads);
	aggregate_update.AggregateUpdate().InitializeDirectPrimitivePayloads();
	aggregate_update.AggregateUpdate().use_grouped_state_addresses = grouped_state;
	aggregate_update.AggregateUpdate().use_perfect_hash_group_lookup = perfect_hash_group_lookup;
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(), "payload_update=generated-primitive");
		if (aggregate_update.AggregateUpdate().use_perfect_hash_group_lookup) {
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
			                                     "grouped_state_lookup=generated-perfect-hash");
		} else if (aggregate_update.AggregateUpdate().use_grouped_state_addresses) {
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
			                                     "grouped_state_lookup=native-state-address");
		}
	}
	return true;
}

void FusePrimitiveAggregateUpdates(SljitNativeRegionPlan &region, const vector<LogicalType> &region_input_types,
                                   bool render_diagnostics) {
	bool changed;
	do {
		changed = false;
		auto input_types = region_input_types;
		idx_t op_idx = 0;
		while (op_idx + 1 < region.ops.size()) {
			auto &op = region.ops[op_idx];
			auto &next = region.ops[op_idx + 1];
			if (TryComposePrimitiveAggregatePayloadsThroughProjection(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryFuseNativeProjectionIntoPerfectHashAggregateUpdate(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryPartiallyFuseNativeProjectionIntoPerfectHashAggregateUpdate(input_types, op, next,
			                                                                   render_diagnostics)) {
				changed = true;
				input_types = op.output_types;
				op_idx++;
				continue;
			}
			if (TryPartiallyFuseNativeProjectionIntoRegularHashAggregateUpdate(input_types, op, next,
			                                                                   render_diagnostics)) {
				changed = true;
				input_types = op.output_types;
				op_idx++;
				continue;
			}
			input_types = op.output_types;
			op_idx++;
		}
		input_types = region_input_types;
		for (auto &op : region.ops) {
			if (TryUsePrimitiveReferenceAggregateUpdate(input_types, op, render_diagnostics)) {
				changed = true;
			}
			input_types = op.output_types;
		}
	} while (changed);
}

} // namespace duckdb
