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
#include "sljit_register_layout.hpp"

#include "duckdb/execution/execution_expression_ir.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

struct SljitTypedExpressionTreeFloatCompare;

//! One checked integer operation. Its jumps reach an error stub that hands both operands to DuckDB's own operator, so
//! the error raised is exactly the one native execution raises.
struct SljitArithmeticOverflowSite {
	SljitNativeIntegerKind kind = SljitNativeIntegerKind::INT64;
	SljitNativeIntegerBinaryOp op = SljitNativeIntegerBinaryOp::ADD;
	sljit_s32 left_reg = 0;
	sljit_s32 right_reg = 0;
	sljit_s32 result_reg = 0;
	bool negation = false;
	vector<sljit_jump *> jumps;
};

struct SljitTypedExpressionTreeSlot {
	sljit_sw value_offset;
	sljit_sw valid_offset;
};

//! An sljit operand pair: a register, a stack slot, or an immediate. A chunk-local value a kernel loop carries from
//! row to row lives in a register, or a stack slot when the kernel has none free: a register never waits on the
//! store-to-load round trip a stack slot adds to every row.
struct SljitOperand {
	sljit_s32 operand;
	sljit_sw operand_w;

	static SljitOperand Register(sljit_s32 reg) {
		return {reg, 0};
	}
	static SljitOperand Stack(sljit_sw offset) {
		return {SLJIT_MEM1(SLJIT_SP), offset};
	}
	static SljitOperand Immediate(int64_t value) {
		return {SLJIT_IMM, NumericCast<sljit_sw>(value)};
	}
};

//! A loop-invariant operand a fast loop reads instead of rebuilding it for every row: a source's data pointer held in a
//! register, or a wide constant held in a stack slot (an arm64 64-bit immediate takes up to four instructions; the
//! slot is one load)
struct SljitTypedExpressionTreeLoopHoist {
	//! the source whose data pointer this holds; INVALID_INDEX for a constant or a string comparison's matches
	idx_t source_index = DConstants::INVALID_INDEX;
	int64_t constant = 0;
	//! a string comparison with a constant whose result for each dictionary entry this holds (the batch's
	//! SljitNativeVectorInput::string_matches[string_compare_index]: FALSE for a NULL entry), or nullptr
	const ExecutionExpressionIR *string_compare = nullptr;
	idx_t string_compare_index = 0;
	//! a source whose selection this holds for the batch: its own, the common one, or the identity
	idx_t selection_source = DConstants::INVALID_INDEX;
	SljitOperand location = SljitOperand::Register(0);
};

bool TryGetSljitTypedExpressionTreeSourceDataHoist(const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                   idx_t source_index, sljit_s32 &data_reg);
//! The hoisted slot of a constant, or the constant as an immediate
SljitOperand SljitTypedExpressionTreeConstantOperand(const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                                     int64_t value);
//! Gives each wide constant the fast emitters of these expressions use a stack slot past local_size
void AppendSljitTypedExpressionTreeConstantHoists(const vector<reference<const ExecutionExpressionIR>> &expressions,
                                                  vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                  sljit_sw &local_size);
//! The comparisons of a string source with a constant in these expressions, in the order their matches are numbered
vector<reference<const ExecutionExpressionIR>>
CollectSljitTypedExpressionTreeStringCompares(const vector<reference<const ExecutionExpressionIR>> &expressions);
//! The sources the predicate and payloads read only through comparisons with a constant whose NULL result decides as
//! FALSE would: under AND and OR in the predicate, or in a CASE condition
vector<idx_t>
CollectSljitTypedExpressionTreeStringCompareOnlySources(const ExecutionExpressionIR &predicate,
                                                        const vector<reference<const ExecutionExpressionIR>> &payloads);
//! Gives each string comparison with a constant a stack slot past local_size for the matches of its dictionary
void AppendSljitTypedExpressionTreeStringCompareHoists(
    const vector<reference<const ExecutionExpressionIR>> &expressions,
    vector<SljitTypedExpressionTreeLoopHoist> &hoists, sljit_sw &local_size);
//! Gives each source a stack slot past local_size for the selection a selected row reads it through
void AppendSljitTypedExpressionTreeSelectionHoists(const vector<idx_t> &sources,
                                                   vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                   sljit_sw &local_size);
//! The hoist of a string comparison's matches, or nullptr
const SljitTypedExpressionTreeLoopHoist *
FindSljitTypedExpressionTreeStringCompareHoist(const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                               const ExecutionExpressionIR &node);
//! Stores the hoisted constants and string matches into their slots; data pointers are loaded by the kernel that
//! owns their sources
//! The sign-extending load of a narrow signed integer column, and its element scale
void GetSljitTypedExpressionTreeNarrowSourceLoad(const ExecutionExpressionIR &reference, sljit_s32 &load_op,
                                                 sljit_sw &scale);

void EmitSljitTypedExpressionTreeConstantHoists(struct sljit_compiler *compiler,
                                                const vector<SljitTypedExpressionTreeLoopHoist> &hoists);
//! Stores into the stack slot the selection a batch's logical rows are read through: its execute selection, else the
//! identity
void EmitSljitLogicalSelectionHoist(struct sljit_compiler *compiler, sljit_sw slot_offset);
//! SLJIT_S3 = the logical row of loop row SLJIT_S1, through the selection EmitSljitLogicalSelectionHoist stored
void EmitLoadSljitHoistedLogicalIndex(struct sljit_compiler *compiler, sljit_sw slot_offset);
enum class SljitTypedExpressionTreeFastIndexMode : uint8_t { FLAT, LOGICAL, SELECTED };

SljitTypedExpressionTreeSlot AllocateSljitTypedExpressionTreeSlot(idx_t &slot_index);
void EmitStoreSljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           sljit_s32 value_reg, sljit_s32 valid_reg);
void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &source,
                                          const SljitTypedExpressionTreeSlot &target);
void EmitStoreSljitTypedExpressionTreeBool(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           bool value);
int64_t SljitTypedExpressionTreeConstantValue(const ExecutionExpressionIR &node);
//! The multiplier of the signed magic-number reduction of division by a constant divisor >= 2
int64_t SljitSignedDivisionMultiplier(int64_t divisor);
//! The unsigned divisibility test by a constant divisor >= 2 [Granlund and Montgomery]: u is a multiple of the divisor
//! exactly when rotate_right(u * inverse, shift) <= limit, where inverse is the inverse of the divisor's odd part
//! modulo 2^64, shift its count of trailing zero bits, and limit floor((2^64 - 1) / divisor)
struct SljitRemainderTestConstants {
	int64_t inverse;
	idx_t shift;
	int64_t limit;
};
SljitRemainderTestConstants ComputeSljitRemainderTestConstants(int64_t divisor);
//! `n % d = c` or `n % d <> c` for a constant divisor d >= 2 and a constant 0 <= c < d, which a predicate jump tests
//! by divisibility instead of computing the remainder
bool SljitTypedExpressionTreeIsRemainderTest(const ExecutionExpressionIR &node);
void EmitSljitTypedExpressionTreeModulo(struct sljit_compiler *compiler, int64_t divisor,
                                        SljitNativeIntegerKind binary_kind, sljit_s32 n_reg, sljit_s32 result_reg,
                                        const vector<SljitTypedExpressionTreeLoopHoist> *hoists = nullptr);
void EmitSljitTypedExpressionTreeIntegerDivide(struct sljit_compiler *compiler, int64_t divisor,
                                               SljitNativeIntegerKind binary_kind, sljit_s32 n_reg,
                                               sljit_s32 result_reg,
                                               const vector<SljitTypedExpressionTreeLoopHoist> *hoists = nullptr);
SljitIntegerOverflowChecks SljitTypedExpressionTreeOverflowChecks(const ExecutionExpressionIR &node);
//! Converts the INTEGER/BIGINT or DECIMAL64 `source` value in value_reg (sign-extended) to DOUBLE bits in place,
//! through FR0 and FR1; clobbers R1
void EmitSljitTypedExpressionTreeConvertToDouble(struct sljit_compiler *compiler, const ExecutionExpressionIR &source,
                                                 sljit_s32 value_reg);
//! Combines the DOUBLE bits in left_reg and right_reg into target_reg through FR0 and FR1
void EmitSljitTypedExpressionTreeDoubleBinary(struct sljit_compiler *compiler, ExecutionExpressionBinaryOp op,
                                              sljit_s32 target_reg, sljit_s32 left_reg, sljit_s32 right_reg);
//! Negates the value in value_reg in place: flips a DOUBLE's sign, or subtracts an integer from zero, held in R1
void EmitSljitTypedExpressionTreeNegate(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        sljit_s32 value_reg, vector<SljitArithmeticOverflowSite> &overflows);
void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label);
void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                               const SljitTypedExpressionTreeSlot &slot);
void EmitLoadSljitTypedExpressionTreeFastSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                                     sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *hoists);
//! Jumps when the source's row, read as index_mode reads it, is NULL. Clobbers R0-R2.
sljit_jump *EmitJumpIfSljitTypedExpressionTreeFastSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                             SljitTypedExpressionTreeFastIndexMode index_mode,
                                                             const vector<SljitTypedExpressionTreeLoopHoist> *hoists);
SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeStringPrefix(struct sljit_compiler *compiler,
                                                                      const ExecutionExpressionIR &node,
                                                                      idx_t &slot_index);
void EmitSljitTypedExpressionTreeFastStringPrefixReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     SljitTypedExpressionTreeFastIndexMode index_mode,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);
void EmitSljitTypedExpressionTreeFastStringCompareReg(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &node,
                                                      SljitTypedExpressionTreeFastIndexMode index_mode,
                                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);
vector<sljit_jump *> EmitSljitTypedExpressionTreeStringPrefixJumpIfTrue(struct sljit_compiler *compiler,
                                                                        const ExecutionExpressionIR &node);
vector<sljit_jump *> EmitSljitTypedExpressionTreeStringCompareJumpIfTrue(struct sljit_compiler *compiler,
                                                                         const ExecutionExpressionIR &node);
SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeStringCompare(struct sljit_compiler *compiler,
                                                                       const ExecutionExpressionIR &node,
                                                                       idx_t &slot_index);
//! Loads a DOUBLE/FLOAT comparison's column value at the row (a FLOAT widened) into FR0
void EmitLoadSljitTypedExpressionTreeFloatValue(struct sljit_compiler *compiler,
                                                const SljitTypedExpressionTreeFloatCompare &compare, sljit_s32 data_reg,
                                                sljit_s32 index_reg);
//! Jumps when the comparison of the value in FR0 with the constant is `when`: the constant is no NaN, so one compare
//! decides. Reads the constant from its hoisted slot, else materializes it in FR1.
sljit_jump *EmitSljitTypedExpressionTreeFloatCompareJump(struct sljit_compiler *compiler,
                                                         const SljitTypedExpressionTreeFloatCompare &compare, bool when,
                                                         const vector<SljitTypedExpressionTreeLoopHoist> *hoists);
vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler,
                                                               const ExecutionExpressionIR &node, idx_t &slot_index,
                                                               vector<SljitArithmeticOverflowSite> &overflows,
                                                               const vector<idx_t> *known_valid_sources = nullptr);
SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeConjunction(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        idx_t &slot_index, vector<SljitArithmeticOverflowSite> &overflows,
                                        const vector<idx_t> *known_valid_sources = nullptr);
SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeCoalesce(struct sljit_compiler *compiler,
                                                                  const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                  vector<SljitArithmeticOverflowSite> &overflows,
                                                                  const vector<idx_t> *known_valid_sources = nullptr);
SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeCase(struct sljit_compiler *compiler,
                                                              const ExecutionExpressionIR &node, idx_t &slot_index,
                                                              vector<SljitArithmeticOverflowSite> &overflows,
                                                              const vector<idx_t> *known_valid_sources = nullptr);

//! DOUBLE adds each row to the state's running value in source order
enum class SljitNativeAggregateSumStateKind : uint8_t { INT64, HUGEINT, DOUBLE };

static constexpr sljit_sw SLJIT_DATE_NEGATIVE_INFINITY_DAYS = -2147483647;
static constexpr sljit_sw SLJIT_DATE_POSITIVE_INFINITY_DAYS = 2147483647;
static constexpr sljit_sw SLJIT_SELECT_TRUE_COUNT_OFFSET = 0;

//! What a generated predicate's row loop knows about its chunk, checked once before the loop
struct SljitPredicateRowShape {
	//! no source has a validity mask
	bool sources_all_valid = false;
	//! neither the rows nor any source carries a selection: a row's logical and source indices are its position
	bool flat = false;
};
static constexpr sljit_sw SLJIT_SELECT_RESULT_INDEX_OFFSET = sizeof(sljit_sw);
static constexpr sljit_sw SLJIT_SELECT_LOCAL_SIZE = 2 * sizeof(sljit_sw);

//! The scratch register argument of sljit_emit_enter for a kernel that evaluates typed expression trees: a comparison
//! of a DOUBLE/FLOAT column with a constant takes the floating-point scratch registers FR0 and FR1
inline sljit_s32 SljitTypedExpressionTreeEnterScratches(sljit_s32 scratches) {
	const auto float_registers = MaxValue<sljit_s32>((scratches >> 8) & 0xff, 2);
	return (scratches & ~sljit_s32(0xff00)) | SLJIT_ENTER_FLOAT(float_registers);
}

inline bool SljitNativeIntegerKindPreservesSourceDateInfinity(SljitNativeIntegerKind kind) {
	return kind == SljitNativeIntegerKind::DATE;
}

inline sljit_sw SljitPointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

void EmitInitSljitNativeVectorLoop(struct sljit_compiler *compiler);
void EmitInitSljitNativeVectorSourceArrays(struct sljit_compiler *compiler);
void EmitInitSljitNativeExpressionVectorLoop(struct sljit_compiler *compiler);
void EmitLoadSljitNativeSourceValidity(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target);
void EmitNextSljitNativeVectorLoop(struct sljit_compiler *compiler, struct sljit_label *loop);
//! A bottom-tested latch: steps the row and repeats the loop while rows remain, so each row takes one branch; returns
//! the exit jump
struct sljit_jump *EmitSljitNativeVectorLatch(struct sljit_compiler *compiler, struct sljit_label *loop);
void EmitLoadLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target);
void EmitLoadSourceIndex(struct sljit_compiler *compiler, sljit_sw sel_offset, sljit_s32 logical_index,
                         sljit_s32 target);
sljit_jump *EmitJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_offset, sljit_s32 index_reg);
void EmitLoadSelectedIndex(struct sljit_compiler *compiler);
sljit_jump *EmitSkipInvalidSourceRow(struct sljit_compiler *compiler);
void EmitSetResultRowInvalid(struct sljit_compiler *compiler);
void EmitSljitDateYearFromDays(struct sljit_compiler *compiler, sljit_s32 days_reg, sljit_s32 target_reg);

bool TryGetSljitExpressionTreeBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op);
//! Emits result = left <op> right with the given checks; the result is always a sign-extended machine word. The error
//! stub reads both operands back from their registers: a checked sum or difference may overwrite one of them, which
//! the stub recovers by inverting the operation, but a checked product must not.
void EmitSljitCheckedIntegerBinaryOp(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                     SljitNativeIntegerBinaryOp op, sljit_s32 result_reg, sljit_s32 left_reg,
                                     sljit_s32 right_reg, const SljitIntegerOverflowChecks &checks,
                                     vector<SljitArithmeticOverflowSite> &overflows);
void EmitLoadSljitExpressionTreeLogicalIndex(struct sljit_compiler *compiler);
void EmitLoadSljitExpressionTreeSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target);
sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index);
sljit_jump *EmitJumpIfSljitExpressionTreeFlatSourceNull(struct sljit_compiler *compiler, idx_t source_index);
//! Emits one error stub per site, which records the error in the input; returns the stubs' exit jumps.
vector<sljit_jump *> EmitSljitArithmeticOverflowStubs(struct sljit_compiler *compiler,
                                                      const vector<SljitArithmeticOverflowSite> &overflows);

void EmitSljitAggregateAccumulateInt64(struct sljit_compiler *compiler, const SljitOperand &sum,
                                       const SljitOperand &saw_value, sljit_s32 value_reg);
void EmitSljitAggregateCommitInt64(struct sljit_compiler *compiler, const SljitOperand &sum,
                                   const SljitOperand &saw_value);
void EmitSljitAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                              sljit_sw local_upper_offset, sljit_sw saw_value_offset,
                                              sljit_s32 value_reg);
void EmitSljitAggregateCommitHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                     sljit_sw local_upper_offset, sljit_sw saw_value_offset);
void EmitSljitAggregateAccumulateSumState(struct sljit_compiler *compiler, SljitNativeAggregateSumStateKind state_kind,
                                          sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                          sljit_sw saw_value_offset, sljit_s32 value_reg);
void EmitSljitAggregateCommitSumState(struct sljit_compiler *compiler, SljitNativeAggregateSumStateKind state_kind,
                                      sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                      sljit_sw saw_value_offset);

double SLJIT_FUNC SljitNativeHugeintToDouble(uint64_t lower, int64_t upper);
double SLJIT_FUNC SljitNativeDecimal64ToDoubleCall(int64_t value, uint64_t scale);
double SLJIT_FUNC SljitNativeDecimal128ToDoubleCall(uint64_t lower, int64_t upper, uint64_t scale);
void SLJIT_FUNC SljitNativeAggregateHugeintCommit(SljitNativeVectorInput *input);
void SLJIT_FUNC SljitNativeRuntimeError(SljitNativeVectorInput *input);
//! Raises DuckDB's error for a perfect-hash group id at or beyond the table's group count
void SLJIT_FUNC SljitNativePerfectHashGroupOutOfRange(SljitNativeVectorInput *input, sljit_uw group,
                                                      sljit_uw total_groups);
sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op, bool single_precision = false);
bool NativeDoubleSourceHasDecimalScale(SljitNativeDoubleSourceKind kind);
bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind);
//! value_reg (not R0/R1) holds a raw DECIMAL64 value; R0/R1 are scratch and a slow conversion clobbers every scratch
void EmitSljitDecimal64ToDouble(struct sljit_compiler *compiler, sljit_s32 value_reg, const SljitOperand &factor,
                                const SljitOperand &scale, sljit_s32 target);
//! R0/R1 hold a raw DECIMAL128 value's lower and upper words
void EmitSljitDecimal128ToDouble(struct sljit_compiler *compiler, const SljitOperand &scale, sljit_s32 target);
//! Loads the operand at base_reg[index_reg] of a source that converts without a helper call
//! (!NativeDoubleSourceUsesHelper); clobbers R2
void EmitLoadNativeDoubleOperandAt(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                   sljit_s32 base_reg, sljit_s32 index_reg, sljit_s32 target,
                                   bool single_precision = false);
//! Loads a double operand; a decimal source converts exactly as DuckDB casts it, so its loader may call a helper.
//! decimal_offset is the input's SljitNativeDecimalSourceScale for that source.
void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                 sljit_sw data_offset, sljit_s32 index_reg, sljit_sw decimal_offset, sljit_s32 target,
                                 bool single_precision = false);
sljit_s32 NativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive);
sljit_s32 NativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive);

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeValue(struct sljit_compiler *compiler,
                                                               const ExecutionExpressionIR &node, idx_t &slot_index,
                                                               vector<SljitArithmeticOverflowSite> &overflows,
                                                               const vector<idx_t> *known_valid_sources = nullptr);
//! Evaluates a fast-path node over a row without NULLs into R2. Given null_exits, the node may be one that
//! SljitTypedExpressionTreeFastNullExitSupported admits: a row whose value is NULL takes one of the jumps added to it
void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                              const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists = nullptr,
                                              vector<sljit_jump *> *null_exits = nullptr);
//! Emits a predicate over rows without NULLs as jumps taken when it evaluates to `when`; control falls through
//! otherwise. A comparison jumps on its own flags and a conjunction short-circuits on the child that decides it, so no
//! boolean is materialized; a flat-row remainder test checks divisibility
vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastJumps(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when,
                                      idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                      SljitTypedExpressionTreeFastIndexMode index_mode,
                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);
//! EmitSljitTypedExpressionTreeFastJumps over rows that may hold NULLs, for a predicate that decides NULL as FALSE
//! (SljitTypedExpressionTreeNullDecidesFalse): each leaf first checks its sources and counts as FALSE on a NULL
vector<sljit_jump *> EmitSljitTypedExpressionTreeNullableFastJumps(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);
//! EmitSljitTypedExpressionTreeFastJumps over flat rows, taken when the predicate is true
vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastJumpsIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                            idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                            const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists = nullptr);
void EmitSljitTypedExpressionTreeSelectedFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists = nullptr, vector<sljit_jump *> *null_exits = nullptr);
void EmitSljitTypedExpressionTreeLogicalFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists = nullptr, vector<sljit_jump *> *null_exits = nullptr);

} // namespace duckdb
