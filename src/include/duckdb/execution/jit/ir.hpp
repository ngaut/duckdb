//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/ir.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/common.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct JitExpressionIR {
	JitExpressionIRKind kind = JitExpressionIRKind::CONSTANT;
	LogicalType return_type;
	PhysicalType physical_type = PhysicalType::INVALID;
	JitExpressionValidityKind validity = JitExpressionValidityKind::UNKNOWN;
	JitExpressionSourceKind source = JitExpressionSourceKind::UNKNOWN;
	JitExpressionExceptionKind exception_behavior = JitExpressionExceptionKind::UNKNOWN;
	Value constant;
	idx_t ref_index = 0;
	JitExpressionUnaryOp unary_op = JitExpressionUnaryOp::NOT;
	JitExpressionBinaryOp binary_op = JitExpressionBinaryOp::ADD;
	JitExpressionConjunctionOp conjunction_op = JitExpressionConjunctionOp::AND;
	JitExpressionIntrinsicKind intrinsic = JitExpressionIntrinsicKind::NONE;
	bool try_cast = false;
	bool not_in = false;
	bool not_between = false;
	bool lower_inclusive = true;
	bool upper_inclusive = true;
	unique_ptr<JitExpressionIR> left;
	unique_ptr<JitExpressionIR> right;
	unique_ptr<JitExpressionIR> else_node;
	vector<unique_ptr<JitExpressionIR>> children;

	unique_ptr<JitExpressionIR> Copy() const;
};

struct JitExpressionFragment {
	idx_t expression_index = 0;
	LogicalType return_type;
	unique_ptr<JitExpressionIR> root;
	string reason;
	string ir;
};

} // namespace duckdb
