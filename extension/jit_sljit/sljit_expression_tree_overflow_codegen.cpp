#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"

#include "sljitLir.h"

#include <exception>

namespace duckdb {

static void SljitNativeTreeOverflow(SljitNativeVectorInput *input, const char *message) {
	try {
		throw OutOfRangeException("%s", message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeTreeAddOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in addition of DECIMAL");
}

static void SLJIT_FUNC SljitNativeTreeSubtractOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in subtract of DECIMAL");
}

static void SLJIT_FUNC SljitNativeTreeMultiplyOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in multiplication of DECIMAL");
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

void AddSljitExpressionOverflowJump(vector<SljitExpressionTreeOverflowJumps> &overflows, SljitNativeIntegerBinaryOp op,
                                    sljit_jump *jump) {
	for (auto &entry : overflows) {
		if (entry.op == op) {
			entry.jumps.push_back(jump);
			return;
		}
	}
	SljitExpressionTreeOverflowJumps entry;
	entry.op = op;
	entry.jumps.push_back(jump);
	overflows.push_back(std::move(entry));
}

void EmitSljitExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeTreeAddOverflow));
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeSubtractOverflow));
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeMultiplyOverflow));
		return;
	default:
		throw InternalException("Unknown SLJIT expression-tree overflow operator");
	}
}

} // namespace duckdb
