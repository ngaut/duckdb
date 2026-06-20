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
	bool has_integer_arithmetic_result = false;
	bool has_non_integer_arithmetic_result = false;
	bool has_comparison_binary = false;
	bool has_integer_comparison_operands = false;
	bool has_non_integer_comparison_operands = false;
	bool has_conjunction = false;
	idx_t expression_node_count = 0;
	idx_t reference_expression_count = 0;
	idx_t predicate_expression_count = 0;
	idx_t control_expression_count = 0;
	idx_t arithmetic_binary_count = 0;
	idx_t integer_arithmetic_result_count = 0;
	idx_t non_integer_arithmetic_result_count = 0;
	idx_t comparison_binary_count = 0;
	idx_t integer_comparison_operand_count = 0;
	idx_t non_integer_comparison_operand_count = 0;
	idx_t conjunction_count = 0;
	idx_t string_predicate_count = 0;
	idx_t high_cost_string_predicate_count = 0;
	idx_t string_like_count = 0;
	idx_t string_contains_count = 0;
	idx_t string_prefix_count = 0;
	idx_t string_suffix_count = 0;
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
