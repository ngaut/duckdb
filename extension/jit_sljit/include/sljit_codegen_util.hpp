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

struct ExecutionRegionCompileTimings;

class SljitCodegenTimingScope {
public:
	explicit SljitCodegenTimingScope(ExecutionRegionCompileTimings *timings);
	~SljitCodegenTimingScope();

private:
	ExecutionRegionCompileTimings *previous_timings;
};

void *GenerateSljitCode(struct sljit_compiler *compiler);
unique_ptr<ExecutionRegionCodeHandle> MakeSljitCodeHandle(void *code, idx_t code_size,
                                                          vector<shared_ptr<void>> owned_data = {});
unique_ptr<ExecutionRegionCodeHandle> FinishSljitCode(struct sljit_compiler *compiler, void *&code, string &error,
                                                      vector<shared_ptr<void>> owned_data = {});

template <class FUNCTION>
unique_ptr<ExecutionRegionCodeHandle> FinishSljitCode(struct sljit_compiler *compiler, FUNCTION &function,
                                                      string &error, vector<shared_ptr<void>> owned_data = {}) {
	void *code = nullptr;
	auto handle = FinishSljitCode(compiler, code, error, std::move(owned_data));
	if (!handle) {
		return nullptr;
	}
	function = reinterpret_cast<FUNCTION>(code);
	return handle;
}

sljit_sw NativeIntegerDataScale(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerLoadOp(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerStoreOp(SljitNativeIntegerKind kind);
sljit_s32 NativeIntegerBinaryOp(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op);
sljit_s32 NativeIntegerCompareJumpType(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op);
//! The flags a flag-setting subtraction must set for a comparison condition (without SLJIT_32) to be read from them
sljit_s32 SljitCompareConditionSetFlags(sljit_s32 condition);

//! Loads into word_reg the word of the validity mask at validity_reg that holds the bit of the row in index_reg. The
//! three registers are distinct.
void EmitLoadSljitValidityWord(struct sljit_compiler *compiler, sljit_s32 validity_reg, sljit_s32 index_reg,
                               sljit_s32 word_reg);
//! Jumps when the row's bit is clear in the word EmitLoadSljitValidityWord loaded into word_reg, which it clobbers
struct sljit_jump *EmitJumpIfSljitValidityWordBitClear(struct sljit_compiler *compiler, sljit_s32 word_reg,
                                                       sljit_s32 index_reg);
//! Jumps when the row's bit is clear in the validity mask at validity_reg. Clobbers word_reg.
struct sljit_jump *EmitJumpIfSljitValidityBitClear(struct sljit_compiler *compiler, sljit_s32 validity_reg,
                                                   sljit_s32 index_reg, sljit_s32 word_reg);
//! Loads the row's bit of the validity mask at validity_reg into target_reg: 1 when valid, 0 when NULL
void EmitLoadSljitValidityBit(struct sljit_compiler *compiler, sljit_s32 validity_reg, sljit_s32 index_reg,
                              sljit_s32 target_reg);

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
