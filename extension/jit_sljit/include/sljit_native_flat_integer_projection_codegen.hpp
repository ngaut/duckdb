#pragma once

#include "sljit_native_codegen.hpp"

#include "sljitLir.h"

namespace duckdb {

bool ValidateNativeFlatIntegerProjectionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                   SljitNativeIntegerKind kind, string &error);
sljit_s32 SljitFlatIntegerProjectionSourceVectorRegister(idx_t source_idx);
sljit_s32 SljitFlatIntegerProjectionSourceScalarRegister(idx_t source_idx);
idx_t SljitFlatIntegerProjectionGroupSize();
sljit_s32 SljitFlatIntegerProjectionSavedRegisterCount();
sljit_s32 SljitFlatIntegerProjectionResultPointerRegister(idx_t group_idx);

} // namespace duckdb
