//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_expression_ir.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_common.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExecutionExpressionIR {
	ExecutionExpressionIRKind kind = ExecutionExpressionIRKind::CONSTANT;
	LogicalType return_type;
	PhysicalType physical_type = PhysicalType::INVALID;
	ExecutionExpressionValidityKind validity = ExecutionExpressionValidityKind::UNKNOWN;
	ExecutionExpressionSourceKind source = ExecutionExpressionSourceKind::UNKNOWN;
	ExecutionExpressionExceptionKind exception_behavior = ExecutionExpressionExceptionKind::UNKNOWN;
	optional_idx query_location;
	Value constant;
	idx_t ref_index = 0;
	ExecutionExpressionUnaryOp unary_op = ExecutionExpressionUnaryOp::NOT;
	ExecutionExpressionBinaryOp binary_op = ExecutionExpressionBinaryOp::ADD;
	ExecutionExpressionConjunctionOp conjunction_op = ExecutionExpressionConjunctionOp::AND;
	ExecutionExpressionIntrinsicKind intrinsic = ExecutionExpressionIntrinsicKind::NONE;
	bool arithmetic_overflow_check = true;
	bool try_cast = false;
	bool not_in = false;
	bool not_between = false;
	bool lower_inclusive = true;
	bool upper_inclusive = true;
	unique_ptr<ExecutionExpressionIR> left;
	unique_ptr<ExecutionExpressionIR> right;
	unique_ptr<ExecutionExpressionIR> else_node;
	vector<unique_ptr<ExecutionExpressionIR>> children;

	unique_ptr<ExecutionExpressionIR> Copy() const;
};

enum class ExecutionExpressionIRMode : uint8_t { COMPACT, TRACE };

struct ExecutionExpressionTraits {
	bool root_is_reference = false;
	bool has_arithmetic_binary = false;
	bool has_conjunction = false;
	idx_t predicate_expression_count = 0;
	idx_t control_expression_count = 0;
	idx_t expression_cost = 0;
};

struct ExecutionExpressionFragment {
	ExecutionExpressionFragment() = default;
	ExecutionExpressionFragment(const ExecutionExpressionFragment &other);
	ExecutionExpressionFragment &operator=(const ExecutionExpressionFragment &other);
	ExecutionExpressionFragment(ExecutionExpressionFragment &&other) noexcept = default;
	ExecutionExpressionFragment &operator=(ExecutionExpressionFragment &&other) noexcept = default;

	idx_t expression_index = 0;
	LogicalType return_type;
	ExecutionExpressionTraits traits;
	unique_ptr<ExecutionExpressionIR> root;
	string reason;
	string ir;
};

DUCKDB_API string DescribeExecutionExpressionIR(const ExecutionExpressionIR &node);

} // namespace duckdb
