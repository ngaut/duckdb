//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_codegen_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_util.hpp"
#include "sljit_function_types.hpp"

#include "duckdb/execution/execution_expression_ir.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

struct SljitExpressionTreeOverflowJumps {
	SljitNativeIntegerBinaryOp op = SljitNativeIntegerBinaryOp::ADD;
	vector<sljit_jump *> jumps;
};

struct SljitTypedExpressionTreeSlot {
	sljit_sw value_offset;
	sljit_sw valid_offset;
};

struct SljitTypedExpressionTreeDataPointerHoist {
	idx_t source_index = 0;
	sljit_s32 data_reg = 0;
};

enum class SljitNativeAggregateSumStateKind : uint8_t { INT64, HUGEINT };

static constexpr sljit_sw SLJIT_SELECT_TRUE_COUNT_OFFSET = 0;
static constexpr sljit_sw SLJIT_SELECT_RESULT_INDEX_OFFSET = sizeof(sljit_sw);
static constexpr sljit_sw SLJIT_SELECT_LOCAL_SIZE = 2 * sizeof(sljit_sw);

inline sljit_sw SljitPointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

void EmitInitSljitNativeVectorLoop(struct sljit_compiler *compiler);
void EmitInitSljitNativeVectorSourceArrays(struct sljit_compiler *compiler);
void EmitInitSljitNativeExpressionVectorLoop(struct sljit_compiler *compiler);
void EmitNextSljitNativeVectorLoop(struct sljit_compiler *compiler, struct sljit_label *loop);
void EmitLoadLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target);
void EmitLoadSourceIndex(struct sljit_compiler *compiler, sljit_sw sel_offset, sljit_s32 logical_index,
                         sljit_s32 target);
sljit_jump *EmitJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_offset, sljit_s32 index_reg);
void EmitLoadSelectedIndex(struct sljit_compiler *compiler);
sljit_jump *EmitSkipInvalidSourceRow(struct sljit_compiler *compiler);
void EmitSetResultRowInvalid(struct sljit_compiler *compiler);

bool TryGetSljitExpressionTreeBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op);
void AddSljitExpressionOverflowJump(vector<SljitExpressionTreeOverflowJumps> &overflows, SljitNativeIntegerBinaryOp op,
                                    sljit_jump *jump);
void EmitLoadSljitExpressionTreeLogicalIndex(struct sljit_compiler *compiler);
void EmitLoadSljitExpressionTreeSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target);
sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index);
sljit_jump *EmitJumpIfSljitExpressionTreeFlatSourceNull(struct sljit_compiler *compiler, idx_t source_index);
void EmitSljitExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op);
void EmitSljitAggregateExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op);

void EmitSljitAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                       sljit_sw saw_value_offset, sljit_s32 value_reg);
void EmitSljitAggregateCommitInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                   sljit_sw saw_value_offset);
void EmitSljitAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                              sljit_sw local_upper_offset, sljit_sw saw_value_offset,
                                              sljit_s32 value_reg);
void EmitSljitAggregateCommitHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                     sljit_sw local_upper_offset, sljit_sw saw_value_offset);

double SLJIT_FUNC SljitNativeHugeintToDouble(uint64_t lower, int64_t upper);
void SLJIT_FUNC SljitNativeAggregateHugeintCommit(SljitNativeVectorInput *input);
void SLJIT_FUNC SljitNativeRuntimeError(SljitNativeVectorInput *input);
sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op, bool single_precision = false);
bool NativeDoubleSourceHasDecimalScale(SljitNativeDoubleSourceKind kind);
bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind);
void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                 sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset, sljit_s32 target,
                                 bool single_precision = false);
sljit_s32 NativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive);
sljit_s32 NativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive);

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeValue(struct sljit_compiler *compiler,
                                                               const ExecutionExpressionIR &node, idx_t &slot_index,
                                                               vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                               const vector<idx_t> *known_valid_sources = nullptr);
void EmitSljitTypedExpressionTreeFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr);
void EmitSljitTypedExpressionTreeSelectedFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr);
void EmitSljitTypedExpressionTreeLogicalFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr);

} // namespace duckdb
