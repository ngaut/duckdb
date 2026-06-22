//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_codegen_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/execution_region_kernel.hpp"

#include "sljitLir.h"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle> MakeSljitCodeHandle(void *code, idx_t code_size,
                                                          vector<shared_ptr<void>> owned_data = {});

sljit_sw NativeIntegerDataScale(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerLoadOp(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerStoreOp(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerBinaryOp(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op);
sljit_s32 NativeIntegerCompareJumpType(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op);

sljit_sw NativeSignedIntegerDataScale(SljitNativeSignedIntegerWidth width);
sljit_s32 NativeSignedIntegerLoadOp(SljitNativeSignedIntegerWidth width);
sljit_s32 NativeSignedIntegerStoreOp(SljitNativeSignedIntegerWidth width);
int64_t NativeSignedIntegerMin(SljitNativeSignedIntegerWidth width);
int64_t NativeSignedIntegerMax(SljitNativeSignedIntegerWidth width);
bool NativeSignedIntegerCastNeedsRangeCheck(SljitNativeSignedIntegerWidth source_width,
                                            SljitNativeSignedIntegerWidth target_width);

sljit_sw NativeUnsignedIntegerDataScale(SljitNativeUnsignedIntegerWidth width);
sljit_s32 NativeUnsignedIntegerLoadOp(SljitNativeUnsignedIntegerWidth width);
sljit_s32 NativeUnsignedIntegerStoreOp(SljitNativeUnsignedIntegerWidth width);
int64_t NativeUnsignedIntegerMax(SljitNativeUnsignedIntegerWidth width);

} // namespace duckdb
