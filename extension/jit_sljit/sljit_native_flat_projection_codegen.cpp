#include "sljit_native_flat_projection_codegen.hpp"

#include "sljit_codegen_internal.hpp"

#include "duckdb/common/exception.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

sljit_s32 SljitFlatProjectionSourcePointerRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_S3;
	case 1:
		return SLJIT_S4;
	default:
		throw InternalException("SLJIT flat projection source pointer register is out of range");
	}
}

idx_t SljitFlatProjectionSourceRegisterIndex(const vector<SljitFlatProjectionSourceRef> &sources, idx_t input_index,
                                             const char *projection_kind) {
	for (idx_t source_idx = 0; source_idx < sources.size(); source_idx++) {
		if (sources[source_idx].input_index == input_index) {
			return source_idx;
		}
	}
	throw InternalException("SLJIT flat %s projection source index is not registered", projection_kind);
}

void EmitSljitFlatProjectionLoadSharedSourcePointers(struct sljit_compiler *compiler,
                                                     const SljitFlatProjectionSharedSourcePlan &shared_plan,
                                                     sljit_s32 temp_reg) {
	for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
		auto &source = shared_plan.sources[source_idx];
		auto source_array_offset = source.right_source ? offsetof(SljitNativeVectorInput, right_source_data_array)
		                                               : offsetof(SljitNativeVectorInput, source_data_array);
		auto source_pointer_offset = SljitPointerArrayOffset(source.projection_index);
		auto source_pointer_reg = SljitFlatProjectionSourcePointerRegister(source_idx);
		sljit_emit_op1(compiler, SLJIT_MOV_P, temp_reg, 0, SLJIT_MEM1(SLJIT_S0), source_array_offset);
		sljit_emit_op1(compiler, SLJIT_MOV_P, source_pointer_reg, 0, SLJIT_MEM1(temp_reg), source_pointer_offset);
	}
}

static bool TryAddSljitFlatProjectionSource(SljitFlatProjectionSharedSourcePlan &shared_plan, idx_t input_index,
                                            idx_t projection_index, bool right_source) {
	for (auto &source : shared_plan.sources) {
		if (source.input_index == input_index) {
			return true;
		}
	}
	if (shared_plan.sources.size() >= 2) {
		return false;
	}
	SljitFlatProjectionSourceRef source;
	source.input_index = input_index;
	source.projection_index = projection_index;
	source.right_source = right_source;
	shared_plan.sources.push_back(source);
	return true;
}

bool TryPlanSljitFlatProjectionSharedSources(const vector<SljitNativeRegionExpressionPlan> &plans,
                                             const vector<idx_t> &projection_indices,
                                             SljitNativeRegionExpressionKind references_kind,
                                             idx_t min_projection_count, idx_t max_projection_count,
                                             SljitFlatProjectionSharedSourcePlan &shared_plan) {
	if (projection_indices.size() < min_projection_count ||
	    (max_projection_count != DConstants::INVALID_INDEX && projection_indices.size() > max_projection_count)) {
		return false;
	}
	shared_plan = SljitFlatProjectionSharedSourcePlan();
	for (auto projection_index : projection_indices) {
		auto &plan = plans[projection_index];
		if (!TryAddSljitFlatProjectionSource(shared_plan, plan.source_index, projection_index, false)) {
			return false;
		}
		if (plan.kind == references_kind &&
		    !TryAddSljitFlatProjectionSource(shared_plan, plan.right_source_index, projection_index, true)) {
			return false;
		}
	}
	return !shared_plan.sources.empty();
}

} // namespace duckdb
