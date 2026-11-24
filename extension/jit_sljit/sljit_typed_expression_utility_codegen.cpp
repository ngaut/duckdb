//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_utility_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include <algorithm>
#include <array>
#include <cstring>

namespace duckdb {

SljitTypedExpressionTreeSlot AllocateSljitTypedExpressionTreeSlot(idx_t &slot_index) {
	const auto value_offset = NumericCast<sljit_sw>(slot_index++ * sizeof(sljit_sw) * 2);
	return SljitTypedExpressionTreeSlot {value_offset, value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))};
}

void EmitStoreSljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           sljit_s32 value_reg, sljit_s32 valid_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.value_offset, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.valid_offset, valid_reg, 0);
}

void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &source,
                                          const SljitTypedExpressionTreeSlot &target) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, target, SLJIT_R2, SLJIT_R3);
}

void EmitStoreSljitTypedExpressionTreeBool(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           bool value) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, value ? 1 : 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

//! A double's bits, as the trees hold a DOUBLE value or a floating-point comparison's constant
static int64_t SljitDoubleBits(double value) {
	int64_t bits;
	memcpy(&bits, &value, sizeof(bits));
	return bits;
}

int64_t SljitTypedExpressionTreeConstantValue(const ExecutionExpressionIR &node) {
	if (node.constant.IsNull()) {
		return 0;
	}
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return node.constant.GetValueUnsafe<bool>() ? 1 : 0;
	}
	if (node.return_type.id() == LogicalTypeId::UTINYINT && node.physical_type == PhysicalType::UINT8) {
		return node.constant.GetValueUnsafe<uint8_t>();
	}
	if (node.return_type.id() == LogicalTypeId::USMALLINT && node.physical_type == PhysicalType::UINT16) {
		return node.constant.GetValueUnsafe<uint16_t>();
	}
	if (node.return_type.id() == LogicalTypeId::UINTEGER && node.physical_type == PhysicalType::UINT32) {
		return node.constant.GetValueUnsafe<uint32_t>();
	}
	if (node.return_type.id() == LogicalTypeId::UBIGINT && node.physical_type == PhysicalType::UINT64) {
		// the register holds the value's bits
		return static_cast<int64_t>(node.constant.GetValueUnsafe<uint64_t>());
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return node.constant.GetValueUnsafe<int32_t>();
	}
	if (SljitTypedExpressionTreeIsDoubleNode(node)) {
		return SljitDoubleBits(node.constant.GetValueUnsafe<double>());
	}
	if (SljitTypedExpressionTreeIsNarrowDecimal128Node(node)) {
		// The narrow-DECIMAL128 plan proved |value| <= INT64_MAX, so the hugeint payload narrows exactly.
		int64_t value;
		if (!Hugeint::TryCast<int64_t>(node.constant.GetValueUnsafe<hugeint_t>(), value)) {
			throw InternalException("SLJIT narrow DECIMAL128 constant does not fit INT64");
		}
		return value;
	}
	return node.constant.GetValueUnsafe<int64_t>();
}

SljitIntegerOverflowChecks SljitTypedExpressionTreeOverflowChecks(const ExecutionExpressionIR &node) {
	SljitIntegerOverflowChecks checks;
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		D_ASSERT(node.unary_op == ExecutionExpressionUnaryOp::NEGATE);
		// a DECIMAL's range is symmetric, so only an integer's minimum has no negation
		checks.overflow = node.arithmetic_overflow_check && !SljitTypedExpressionTreeIsDecimal64Node(node);
		checks.negation = true;
		return checks;
	}
	if (!node.arithmetic_overflow_check) {
		return checks;
	}
	checks.overflow = true;
	if (SljitTypedExpressionTreeIsDecimal64Node(node)) {
		checks.range = true;
		if (!TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, checks.result_min, checks.result_max)) {
			throw InternalException("SLJIT typed expression-tree binary node missing decimal64 result range");
		}
	}
	return checks;
}

void EmitSljitTypedExpressionTreeConvertToDouble(struct sljit_compiler *compiler, const ExecutionExpressionIR &source,
                                                 sljit_s32 value_reg) {
	sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_SW, SLJIT_FR0, 0, value_reg, 0);
	if (SljitTypedExpressionTreeIsDecimal64Node(source)) {
		const auto scale = DecimalType::GetScale(source.return_type);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM,
		               static_cast<sljit_sw>(SljitDoubleBits(NumericHelper::DOUBLE_POWERS_OF_TEN[scale])));
		sljit_emit_fcopy(compiler, SLJIT_COPY_TO_F64, SLJIT_FR1, SLJIT_R1);
		sljit_emit_fop2(compiler, SLJIT_DIV_F64, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
	}
	sljit_emit_fcopy(compiler, SLJIT_COPY_FROM_F64, SLJIT_FR0, value_reg);
}

void EmitSljitTypedExpressionTreeDoubleBinary(struct sljit_compiler *compiler, ExecutionExpressionBinaryOp op,
                                              sljit_s32 target_reg, sljit_s32 left_reg, sljit_s32 right_reg) {
	sljit_s32 fop;
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		fop = SLJIT_ADD_F64;
		break;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		fop = SLJIT_SUB_F64;
		break;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		fop = SLJIT_MUL_F64;
		break;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree DOUBLE operator");
	}
	sljit_emit_fcopy(compiler, SLJIT_COPY_TO_F64, SLJIT_FR0, left_reg);
	sljit_emit_fcopy(compiler, SLJIT_COPY_TO_F64, SLJIT_FR1, right_reg);
	sljit_emit_fop2(compiler, fop, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
	sljit_emit_fcopy(compiler, SLJIT_COPY_FROM_F64, SLJIT_FR0, target_reg);
}

void EmitSljitTypedExpressionTreeNegate(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        sljit_s32 value_reg, vector<SljitArithmeticOverflowSite> &overflows) {
	if (SljitTypedExpressionTreeIsDoubleNode(node)) {
		sljit_emit_op2(compiler, SLJIT_XOR, value_reg, 0, value_reg, 0, SLJIT_IMM,
		               static_cast<sljit_sw>(NumericLimits<int64_t>::Minimum()));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	EmitSljitCheckedIntegerBinaryOp(compiler, SljitTypedExpressionTreeIntegerKind(node),
	                                SljitNativeIntegerBinaryOp::SUBTRACT, value_reg, SLJIT_R1, value_reg,
	                                SljitTypedExpressionTreeOverflowChecks(node), overflows);
}

bool TryGetSljitTypedExpressionTreeSourceDataHoist(const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                   idx_t source_index, sljit_s32 &data_reg) {
	if (!data_hoists) {
		return false;
	}
	for (auto &hoist : *data_hoists) {
		if (hoist.source_index == source_index) {
			data_reg = hoist.location.operand;
			return true;
		}
	}
	return false;
}

SljitOperand SljitTypedExpressionTreeConstantOperand(const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                                     int64_t value) {
	if (hoists) {
		for (auto &hoist : *hoists) {
			if (hoist.source_index == DConstants::INVALID_INDEX && !hoist.string_compare &&
			    hoist.selection_source == DConstants::INVALID_INDEX && hoist.constant == value) {
				return hoist.location;
			}
		}
	}
	return SljitOperand::Immediate(value);
}

bool SljitTypedExpressionTreeIsRemainderTest(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY ||
	    (node.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL &&
	     node.binary_op != ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL) ||
	    !node.left || !node.right) {
		return false;
	}
	auto &modulo = *node.left;
	if (modulo.binary_op != ExecutionExpressionBinaryOp::MODULO ||
	    !SljitTypedExpressionTreeConstantDivisorReductionSupported(modulo) ||
	    node.right->kind != ExecutionExpressionIRKind::CONSTANT || node.right->constant.IsNull()) {
		return false;
	}
	const auto remainder = SljitTypedExpressionTreeConstantValue(*node.right);
	return remainder >= 0 && remainder < SljitTypedExpressionTreeConstantValue(*modulo.right);
}

static void AddSljitTypedExpressionTreeConstantHoist(int64_t value, vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                     sljit_sw &local_size) {
	for (auto &hoist : hoists) {
		if (hoist.source_index == DConstants::INVALID_INDEX && !hoist.string_compare &&
		    hoist.selection_source == DConstants::INVALID_INDEX && hoist.constant == value) {
			return;
		}
	}
	SljitTypedExpressionTreeLoopHoist hoist;
	hoist.constant = value;
	hoist.location = SljitOperand::Stack(local_size);
	local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	hoists.push_back(hoist);
}

static void CollectSljitTypedExpressionTreeConstantHoists(const ExecutionExpressionIR &node,
                                                          vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                          sljit_sw &local_size) {
	SljitTypedExpressionTreeFloatCompare float_compare;
	if (TryReadSljitTypedExpressionTreeFloatCompareConstant(node, float_compare)) {
		AddSljitTypedExpressionTreeConstantHoist(SljitDoubleBits(float_compare.constant), hoists, local_size);
		return;
	}
	if (SljitTypedExpressionTreeIsRemainderTest(node)) {
		auto constants = ComputeSljitRemainderTestConstants(SljitTypedExpressionTreeConstantValue(*node.left->right));
		AddSljitTypedExpressionTreeConstantHoist(constants.inverse, hoists, local_size);
		AddSljitTypedExpressionTreeConstantHoist(constants.limit, hoists, local_size);
	}
	if (SljitTypedExpressionTreeConstantDivisorReductionSupported(node)) {
		AddSljitTypedExpressionTreeConstantHoist(
		    SljitSignedDivisionMultiplier(SljitTypedExpressionTreeConstantValue(*node.right)), hoists, local_size);
	}
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			CollectSljitTypedExpressionTreeConstantHoists(*child, hoists, local_size);
		}
	}
	for (auto &child : node.children) {
		if (child) {
			CollectSljitTypedExpressionTreeConstantHoists(*child, hoists, local_size);
		}
	}
}

void EmitLoadSljitTypedExpressionTreeFloatValue(struct sljit_compiler *compiler,
                                                const SljitTypedExpressionTreeFloatCompare &compare, sljit_s32 data_reg,
                                                sljit_s32 index_reg) {
	if (compare.single_precision) {
		sljit_emit_fop1(compiler, SLJIT_MOV_F32, SLJIT_FR0, 0, SLJIT_MEM2(data_reg, index_reg), 2);
		sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_F32, SLJIT_FR0, 0, SLJIT_FR0, 0);
		return;
	}
	sljit_emit_fop1(compiler, SLJIT_MOV_F64, SLJIT_FR0, 0, SLJIT_MEM2(data_reg, index_reg), 3);
}

sljit_jump *EmitSljitTypedExpressionTreeFloatCompareJump(struct sljit_compiler *compiler,
                                                         const SljitTypedExpressionTreeFloatCompare &compare, bool when,
                                                         const vector<SljitTypedExpressionTreeLoopHoist> *hoists) {
	// a comparison's negation pairs with it in the lowest bit of the jump type
	const auto type = NativeDoubleCompareAgainstNumberJumpType(compare.op) ^ (when ? 0 : 1);
	auto constant = SljitTypedExpressionTreeConstantOperand(hoists, SljitDoubleBits(compare.constant));
	if (constant.operand == SLJIT_IMM) {
		sljit_emit_fset64(compiler, SLJIT_FR1, compare.constant);
		return sljit_emit_fcmp(compiler, type, SLJIT_FR0, 0, SLJIT_FR1, 0);
	}
	return sljit_emit_fcmp(compiler, type, SLJIT_FR0, 0, constant.operand, constant.operand_w);
}

void AppendSljitTypedExpressionTreeConstantHoists(const vector<reference<const ExecutionExpressionIR>> &expressions,
                                                  vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                  sljit_sw &local_size) {
	for (auto &expression : expressions) {
		CollectSljitTypedExpressionTreeConstantHoists(expression.get(), hoists, local_size);
	}
}

static void CollectSljitTypedExpressionTreeStringCompares(const ExecutionExpressionIR &node,
                                                          vector<reference<const ExecutionExpressionIR>> &result) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
		result.push_back(node);
		return;
	}
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			CollectSljitTypedExpressionTreeStringCompares(*child, result);
		}
	}
	for (auto &child : node.children) {
		if (child) {
			CollectSljitTypedExpressionTreeStringCompares(*child, result);
		}
	}
}

vector<reference<const ExecutionExpressionIR>>
CollectSljitTypedExpressionTreeStringCompares(const vector<reference<const ExecutionExpressionIR>> &expressions) {
	vector<reference<const ExecutionExpressionIR>> result;
	for (auto &expression : expressions) {
		CollectSljitTypedExpressionTreeStringCompares(expression.get(), result);
	}
	return result;
}

//! Splits the sources a node reads into those it reads at a string comparison whose NULL result decides as FALSE
//! would, and the rest. A filter decides NULL as FALSE, and so does a CASE condition; AND and OR pass that on to their
//! children.
static void CollectSljitTypedExpressionTreeStringCompareReads(const ExecutionExpressionIR &node,
                                                              bool null_decides_false, vector<idx_t> &compared,
                                                              vector<idx_t> &other) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	if (null_decides_false &&
	    TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
		compared.push_back(source_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		other.push_back(node.ref_index);
		return;
	}
	const bool children_decide_false = node.kind == ExecutionExpressionIRKind::CONJUNCTION && null_decides_false;
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx++) {
		// the even children of a CASE are its conditions
		const bool condition = node.kind == ExecutionExpressionIRKind::CASE && child_idx % 2 == 0;
		CollectSljitTypedExpressionTreeStringCompareReads(*node.children[child_idx], children_decide_false || condition,
		                                                  compared, other);
	}
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			CollectSljitTypedExpressionTreeStringCompareReads(*child, false, compared, other);
		}
	}
}

vector<idx_t> CollectSljitTypedExpressionTreeStringCompareOnlySources(
    const ExecutionExpressionIR &predicate, const vector<reference<const ExecutionExpressionIR>> &payloads) {
	vector<idx_t> compared;
	vector<idx_t> other;
	CollectSljitTypedExpressionTreeStringCompareReads(predicate, true, compared, other);
	for (auto &payload : payloads) {
		CollectSljitTypedExpressionTreeStringCompareReads(payload.get(), false, compared, other);
	}
	vector<idx_t> result;
	for (auto source_index : compared) {
		if (std::find(other.begin(), other.end(), source_index) == other.end() &&
		    std::find(result.begin(), result.end(), source_index) == result.end()) {
			result.push_back(source_index);
		}
	}
	return result;
}

//! The selection of a source without one: row i of a vector reads entry i
static const sel_t *SljitIdentitySelection() {
	static const auto identity = []() {
		std::array<sel_t, STANDARD_VECTOR_SIZE> result;
		for (idx_t row_idx = 0; row_idx < result.size(); row_idx++) {
			result[row_idx] = NumericCast<sel_t>(row_idx);
		}
		return result;
	}();
	return identity.data();
}

void EmitSljitLogicalSelectionHoist(struct sljit_compiler *compiler, sljit_sw slot_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto has_execute_selection = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_IMM, reinterpret_cast<sljit_sw>(SljitIdentitySelection()));
	sljit_set_label(has_execute_selection, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), slot_offset, SLJIT_R0, 0);
}

void EmitLoadSljitHoistedLogicalIndex(struct sljit_compiler *compiler, sljit_sw slot_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), slot_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
}

void AppendSljitTypedExpressionTreeSelectionHoists(const vector<idx_t> &sources,
                                                   vector<SljitTypedExpressionTreeLoopHoist> &hoists,
                                                   sljit_sw &local_size) {
	for (auto source_index : sources) {
		SljitTypedExpressionTreeLoopHoist hoist;
		hoist.selection_source = source_index;
		hoist.location = SljitOperand::Stack(local_size);
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		hoists.push_back(hoist);
	}
}

void AppendSljitTypedExpressionTreeStringCompareHoists(
    const vector<reference<const ExecutionExpressionIR>> &expressions,
    vector<SljitTypedExpressionTreeLoopHoist> &hoists, sljit_sw &local_size) {
	auto compares = CollectSljitTypedExpressionTreeStringCompares(expressions);
	for (idx_t compare_idx = 0; compare_idx < compares.size(); compare_idx++) {
		SljitTypedExpressionTreeLoopHoist hoist;
		hoist.string_compare = &compares[compare_idx].get();
		hoist.string_compare_index = compare_idx;
		hoist.location = SljitOperand::Stack(local_size);
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		hoists.push_back(hoist);
	}
}

const SljitTypedExpressionTreeLoopHoist *
FindSljitTypedExpressionTreeStringCompareHoist(const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                               const ExecutionExpressionIR &node) {
	if (!hoists) {
		return nullptr;
	}
	for (auto &hoist : *hoists) {
		if (hoist.string_compare == &node) {
			return &hoist;
		}
	}
	return nullptr;
}

void EmitSljitTypedExpressionTreeConstantHoists(struct sljit_compiler *compiler,
                                                const vector<SljitTypedExpressionTreeLoopHoist> &hoists) {
	vector<reference<const SljitTypedExpressionTreeLoopHoist>> string_compares;
	for (auto &hoist : hoists) {
		if (hoist.selection_source != DConstants::INVALID_INDEX) {
			// as EmitLoadSljitExpressionTreeSourceIndex resolves it: the source's own selection, else the common one,
			// else the identity
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_IMM,
			               reinterpret_cast<sljit_sw>(SljitIdentitySelection()));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S4, 0);
			auto no_selection_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
			               SljitPointerArrayOffset(hoist.selection_source));
			auto own_selection = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			sljit_set_label(no_selection_array, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, source_common_sel));
			auto no_common_selection = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			sljit_set_label(own_selection, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_R0, 0);
			sljit_set_label(no_common_selection, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.location.operand, hoist.location.operand_w, SLJIT_R1, 0);
		} else if (hoist.string_compare) {
			string_compares.push_back(hoist);
		} else if (hoist.source_index == DConstants::INVALID_INDEX) {
			sljit_emit_op1(compiler, SLJIT_MOV, hoist.location.operand, hoist.location.operand_w, SLJIT_IMM,
			               NumericCast<sljit_sw>(hoist.constant));
		}
	}
	if (string_compares.empty()) {
		return;
	}
	// a batch without matches for a comparison (its source is no small dictionary) compares its rows
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, string_matches));
	for (auto &hoist_ref : string_compares) {
		auto &hoist = hoist_ref.get();
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_IMM, 0);
		auto no_matches = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
		               SljitPointerArrayOffset(hoist.string_compare_index));
		sljit_set_label(no_matches, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.location.operand, hoist.location.operand_w, SLJIT_R1, 0);
	}
}

void EmitLoadSljitTypedExpressionTreeFastSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                                     sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *hoists) {
	if (index_mode == SljitTypedExpressionTreeFastIndexMode::FLAT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
		return;
	}
	if (index_mode == SljitTypedExpressionTreeFastIndexMode::LOGICAL) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
		return;
	}
	if (hoists) {
		for (auto &hoist : *hoists) {
			if (hoist.selection_source == source_index) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, hoist.location.operand, hoist.location.operand_w);
				sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(target, SLJIT_S3), 2);
				return;
			}
		}
	}
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, target);
}

sljit_jump *EmitJumpIfSljitTypedExpressionTreeFastSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                             SljitTypedExpressionTreeFastIndexMode index_mode,
                                                             const vector<SljitTypedExpressionTreeLoopHoist> *hoists) {
	// the index first: resolving a selection without a hoist clobbers R0
	EmitLoadSljitTypedExpressionTreeFastSourceIndex(compiler, source_index, SLJIT_R1, index_mode, hoists);
	EmitLoadSljitNativeSourceValidity(compiler, source_index, SLJIT_R0);
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto source_is_null = EmitJumpIfSljitValidityBitClear(compiler, SLJIT_R0, SLJIT_R1, SLJIT_R2);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto jump : jumps) {
		sljit_set_label(jump, label);
	}
}

void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                               const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

} // namespace duckdb
