#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/negate.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"

#include "sljitLir.h"

#include <cstddef>
#include <exception>

namespace duckdb {

template <class OVERFLOW_CHECK, class T>
static void RunNativeOverflowCheck(int64_t left, int64_t right) {
	OVERFLOW_CHECK::template Operation<T, T, T>(static_cast<T>(left), static_cast<T>(right));
}

template <class T>
static void RunNativeIntegerOverflowCheck(SljitNativeIntegerBinaryOp op, int64_t left, int64_t right) {
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return RunNativeOverflowCheck<AddOperatorOverflowCheck, T>(left, right);
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return RunNativeOverflowCheck<SubtractOperatorOverflowCheck, T>(left, right);
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return RunNativeOverflowCheck<MultiplyOperatorOverflowCheck, T>(left, right);
	}
}

static void RunNativeDecimal64OverflowCheck(SljitNativeIntegerBinaryOp op, int64_t left, int64_t right) {
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return RunNativeOverflowCheck<DecimalAddOverflowCheck, int64_t>(left, right);
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return RunNativeOverflowCheck<DecimalSubtractOverflowCheck, int64_t>(left, right);
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return RunNativeOverflowCheck<DecimalMultiplyOverflowCheck, int64_t>(left, right);
	}
}

static void RunNativeDateOverflowCheck(SljitNativeIntegerBinaryOp op, int64_t left, int64_t right) {
	// date plans keep the date on the left and the day count on the right
	const auto date = date_t(static_cast<int32_t>(left));
	const auto days = static_cast<int32_t>(right);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		AddOperator::Operation<date_t, int32_t, date_t>(date, days);
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		SubtractOperator::Operation<date_t, int32_t, date_t>(date, days);
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		break;
	}
	throw InternalException("SLJIT DATE arithmetic only adds or subtracts days");
}

static constexpr sljit_sw SLJIT_OVERFLOW_NEGATION = 1 << 16;

static sljit_sw SljitArithmeticOverflowOperation(const SljitArithmeticOverflowSite &site) {
	return (site.negation ? SLJIT_OVERFLOW_NEGATION : 0) | static_cast<sljit_sw>(site.kind) << 8 |
	       static_cast<sljit_sw>(site.op);
}

//! A negation is the subtraction of its operand from zero; DuckDB's negation rejects the operand
static void RunNativeNegationCheck(SljitNativeIntegerKind kind, int64_t operand) {
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		NegateOperator::Operation<int32_t, int32_t>(static_cast<int32_t>(operand));
		return;
	case SljitNativeIntegerKind::INT64:
		NegateOperator::Operation<int64_t, int64_t>(operand);
		return;
	default:
		throw InternalException("SLJIT negation overflow check for an unsupported integer kind");
	}
}

//! Re-runs the operation that overflowed through DuckDB's own operator, which raises the error native execution
//! raises. An operator that accepts the operands means the generated check disagrees with DuckDB.
static void SLJIT_FUNC SljitNativeArithmeticOverflow(SljitNativeVectorInput *input, sljit_sw operation) {
	try {
		const auto kind = static_cast<SljitNativeIntegerKind>((operation >> 8) & 0xFF);
		const auto op = static_cast<SljitNativeIntegerBinaryOp>(operation & 0xFF);
		const auto left = input->overflow_value;
		const auto right = input->overflow_right_value;
		if (operation & SLJIT_OVERFLOW_NEGATION) {
			RunNativeNegationCheck(kind, right);
		} else {
			switch (kind) {
			case SljitNativeIntegerKind::INT32:
				RunNativeIntegerOverflowCheck<int32_t>(op, left, right);
				break;
			case SljitNativeIntegerKind::INT64:
				RunNativeIntegerOverflowCheck<int64_t>(op, left, right);
				break;
			case SljitNativeIntegerKind::DECIMAL64:
				RunNativeDecimal64OverflowCheck(op, left, right);
				break;
			case SljitNativeIntegerKind::DATE:
				RunNativeDateOverflowCheck(op, left, right);
				break;
			default:
				throw InternalException("SLJIT arithmetic overflow check for an unsupported integer kind");
			}
		}
		throw InternalException("SLJIT arithmetic overflow check fired on operands %lld and %lld that DuckDB accepts",
		                        left, right);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

bool TryGetSljitExpressionTreeBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		native_op = SljitNativeIntegerBinaryOp::ADD;
		return true;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		native_op = SljitNativeIntegerBinaryOp::SUBTRACT;
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		native_op = SljitNativeIntegerBinaryOp::MULTIPLY;
		return true;
	default:
		return false;
	}
}

void EmitSljitCheckedIntegerBinaryOp(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                     SljitNativeIntegerBinaryOp op, sljit_s32 result_reg, sljit_s32 left_reg,
                                     sljit_s32 right_reg, const SljitIntegerOverflowChecks &checks,
                                     vector<SljitArithmeticOverflowSite> &overflows) {
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	// a 32-bit operation leaves the register's upper half undefined
	const auto widen_result = (binary_op & SLJIT_32) != 0;
	if (!checks.overflow && !checks.range) {
		sljit_emit_op2(compiler, binary_op, result_reg, 0, left_reg, 0, right_reg, 0);
		if (widen_result) {
			sljit_emit_op1(compiler, SLJIT_MOV_S32, result_reg, 0, result_reg, 0);
		}
		return;
	}
	D_ASSERT(left_reg != right_reg || result_reg != left_reg);
	D_ASSERT(op != SljitNativeIntegerBinaryOp::MULTIPLY || (result_reg != left_reg && result_reg != right_reg));
	SljitArithmeticOverflowSite site;
	site.kind = kind;
	site.op = op;
	site.left_reg = left_reg;
	site.right_reg = right_reg;
	site.result_reg = result_reg;
	site.negation = checks.negation;
	sljit_emit_op2(compiler, checks.overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op, result_reg, 0, left_reg, 0,
	               right_reg, 0);
	if (checks.overflow) {
		site.jumps.push_back(sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	}
	if (widen_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, result_reg, 0, result_reg, 0);
	}
	if (checks.range) {
		site.jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, result_reg, 0, SLJIT_IMM,
		                                    NumericCast<sljit_sw>(checks.result_min)));
		site.jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, result_reg, 0, SLJIT_IMM,
		                                    NumericCast<sljit_sw>(checks.result_max)));
	}
	overflows.push_back(std::move(site));
}

vector<sljit_jump *> EmitSljitArithmeticOverflowStubs(struct sljit_compiler *compiler,
                                                      const vector<SljitArithmeticOverflowSite> &overflows) {
	vector<sljit_jump *> stub_exits;
	for (auto &site : overflows) {
		SetSljitJumpLabels(site.jumps, sljit_emit_label(compiler));
		// an operand the (possibly wrapped) result overwrote comes back by inverting the sum or difference
		const auto add = NativeIntegerBinaryOp(site.kind, SljitNativeIntegerBinaryOp::ADD);
		const auto subtract = NativeIntegerBinaryOp(site.kind, SljitNativeIntegerBinaryOp::SUBTRACT);
		if (site.result_reg == site.left_reg) {
			// left = result - right, or result + right for a difference
			sljit_emit_op2(compiler, site.op == SljitNativeIntegerBinaryOp::ADD ? subtract : add, site.left_reg, 0,
			               site.result_reg, 0, site.right_reg, 0);
		} else if (site.result_reg == site.right_reg) {
			// right = result - left, or left - result for a difference
			if (site.op == SljitNativeIntegerBinaryOp::ADD) {
				sljit_emit_op2(compiler, subtract, site.right_reg, 0, site.result_reg, 0, site.left_reg, 0);
			} else {
				sljit_emit_op2(compiler, subtract, site.right_reg, 0, site.left_reg, 0, site.result_reg, 0);
			}
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_value),
		               site.left_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, overflow_right_value), site.right_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, SljitArithmeticOverflowOperation(site));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS2V(P, W), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeArithmeticOverflow));
		stub_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}
	return stub_exits;
}

} // namespace duckdb
