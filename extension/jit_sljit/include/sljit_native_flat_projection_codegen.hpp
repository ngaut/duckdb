//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_flat_projection_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_codegen.hpp"

#include "duckdb/common/constants.hpp"

namespace duckdb {

struct SljitFlatProjectionSourceRef {
	idx_t input_index = DConstants::INVALID_INDEX;
	idx_t projection_index = DConstants::INVALID_INDEX;
	bool right_source = false;
};

struct SljitFlatProjectionSharedSourcePlan {
	vector<SljitFlatProjectionSourceRef> sources;
};

sljit_s32 SljitFlatProjectionSourcePointerRegister(idx_t source_idx);
idx_t SljitFlatProjectionSourceRegisterIndex(const vector<SljitFlatProjectionSourceRef> &sources, idx_t input_index,
                                             const char *projection_kind);
void EmitSljitFlatProjectionLoadSharedSourcePointers(struct sljit_compiler *compiler,
                                                     const SljitFlatProjectionSharedSourcePlan &shared_plan,
                                                     sljit_s32 temp_reg);
bool TryPlanSljitFlatProjectionSharedSources(const vector<SljitNativeRegionExpressionPlan> &plans,
                                             const vector<idx_t> &projection_indices,
                                             SljitNativeRegionExpressionKind references_kind,
                                             idx_t min_projection_count, idx_t max_projection_count,
                                             SljitFlatProjectionSharedSourcePlan &shared_plan);

} // namespace duckdb
