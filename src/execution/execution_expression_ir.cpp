#include "duckdb/execution/execution_region_lowering.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/cost_model.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static bool ExecutionExpressionIsIntegralType(const LogicalType &type) {
	return type.IsIntegral();
}

static bool ExecutionExpressionIsSignedIntegerType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsUnsignedIntegerType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsCompressibleIntegralType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsFloatingType(const LogicalType &type) {
	return type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE;
}

static bool ExecutionExpressionIsArithmeticType(const LogicalType &type) {
	return ExecutionExpressionIsIntegralType(type) || ExecutionExpressionIsFloatingType(type) ||
	       type.id() == LogicalTypeId::DECIMAL;
}

static bool ExecutionExpressionIsNegatableType(const LogicalType &type) {
	return ExecutionExpressionIsArithmeticType(type) || type.id() == LogicalTypeId::DECIMAL;
}

static bool ExecutionExpressionIsScalarType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::INTERVAL:
		return true;
	default:
		return ExecutionExpressionIsArithmeticType(type);
	}
}

static bool ExecutionExpressionConstantValueMatchesType(const Value &value, const LogicalType &type) {
	return value.IsNull() || value.type() == type;
}

static bool ExecutionExpressionIsComparableType(const LogicalType &type) {
	return ExecutionExpressionIsScalarType(type);
}

static bool ExecutionExpressionIsSameOrNullType(const LogicalType &left, const LogicalType &right) {
	if (left == right) {
		return true;
	}
	return left.id() == LogicalTypeId::SQLNULL || right.id() == LogicalTypeId::SQLNULL;
}

static bool ExecutionExpressionIsComparisonOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM:
	case ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsArithmeticOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
	case ExecutionExpressionBinaryOp::MULTIPLY:
	case ExecutionExpressionBinaryOp::INTEGER_DIVIDE:
	case ExecutionExpressionBinaryOp::MODULO:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionBinaryOperandsAreIntegral(const ExecutionExpressionIR &node) {
	return node.left && node.right && ExecutionExpressionIsIntegralType(node.left->return_type) &&
	       ExecutionExpressionIsIntegralType(node.right->return_type);
}

static bool TryInvertExecutionComparisonOp(ExecutionExpressionBinaryOp input, ExecutionExpressionBinaryOp &result) {
	switch (input) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		result = ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		result = ExecutionExpressionBinaryOp::COMPARE_EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		result = ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		result = ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		result = ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		result = ExecutionExpressionBinaryOp::COMPARE_LESSTHAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		result = ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		result = ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM;
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsNullConstant(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && node.constant.IsNull();
}

static unique_ptr<ExecutionExpressionIR> BuildExecutionNullCheckIR(unique_ptr<ExecutionExpressionIR> child,
                                                                   ExecutionExpressionUnaryOp op) {
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::UNARY;
	node->return_type = LogicalType::BOOLEAN;
	node->unary_op = op;
	node->left = std::move(child);
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryCanonicalizeExecutionNullDistinctComparison(ExecutionExpressionBinaryOp op, unique_ptr<ExecutionExpressionIR> left,
                                               unique_ptr<ExecutionExpressionIR> right) {
	if (op != ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM &&
	    op != ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM) {
		return nullptr;
	}
	auto left_is_null = ExecutionExpressionIsNullConstant(*left);
	auto right_is_null = ExecutionExpressionIsNullConstant(*right);
	if (!left_is_null && !right_is_null) {
		return nullptr;
	}
	if (left_is_null && right_is_null) {
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value::BOOLEAN(op == ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM);
		return node;
	}
	auto null_check_op = op == ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM
	                         ? ExecutionExpressionUnaryOp::IS_NOT_NULL
	                         : ExecutionExpressionUnaryOp::IS_NULL;
	if (left_is_null) {
		return BuildExecutionNullCheckIR(std::move(right), null_check_op);
	}
	return BuildExecutionNullCheckIR(std::move(left), null_check_op);
}

static string ExecutionUnaryOpToString(ExecutionExpressionUnaryOp op) {
	switch (op) {
	case ExecutionExpressionUnaryOp::NOT:
		return "not";
	case ExecutionExpressionUnaryOp::IS_NULL:
		return "is_null";
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		return "is_not_null";
	case ExecutionExpressionUnaryOp::NEGATE:
		return "negate";
	default:
		return "unknown";
	}
}

static string ExecutionBinaryOpToString(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		return "add";
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return "subtract";
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return "multiply";
	case ExecutionExpressionBinaryOp::DIVIDE:
		return "divide";
	case ExecutionExpressionBinaryOp::INTEGER_DIVIDE:
		return "integer_divide";
	case ExecutionExpressionBinaryOp::MODULO:
		return "modulo";
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		return "compare_equal";
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		return "compare_not_equal";
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		return "compare_less_than";
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		return "compare_greater_than";
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return "compare_less_than_or_equal";
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return "compare_greater_than_or_equal";
	case ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		return "compare_distinct_from";
	case ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return "compare_not_distinct_from";
	default:
		return "unknown";
	}
}

static string ExecutionExpressionTypeDescriptor(const LogicalType &type, PhysicalType physical_type) {
	return "logical=" + type.ToString() + ",physical=" + TypeIdToString(physical_type);
}

static string ExecutionExpressionTypeDescriptor(const LogicalType &type) {
	return ExecutionExpressionTypeDescriptor(type, type.InternalType());
}

static string ExecutionExpressionValidityDescriptor(const ExecutionExpressionIR &node) {
	return "validity=" + string(ExecutionExpressionValidityKindToString(node.validity));
}

static string ExecutionExpressionSourceDescriptor(const ExecutionExpressionIR &node) {
	if (node.source == ExecutionExpressionSourceKind::VECTOR) {
		return "source=vector#" + std::to_string(node.ref_index);
	}
	return "source=" + string(ExecutionExpressionSourceKindToString(node.source));
}

static string ExecutionExpressionExceptionDescriptor(const ExecutionExpressionIR &node) {
	return "exceptions=" + string(ExecutionExpressionExceptionKindToString(node.exception_behavior));
}

static string ExecutionExpressionArithmeticDescriptor(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY || ExecutionExpressionIsComparisonOp(node.binary_op)) {
		return string();
	}
	return ",overflow_check=" + string(node.arithmetic_overflow_check ? "true" : "false");
}

string DescribeExecutionExpressionIR(const ExecutionExpressionIR &node) {
	string result = string(ExecutionExpressionIRKindToString(node.kind)) + "<" +
	                ExecutionExpressionTypeDescriptor(node.return_type, node.physical_type) + "," +
	                ExecutionExpressionValidityDescriptor(node) + "," + ExecutionExpressionSourceDescriptor(node) +
	                "," + ExecutionExpressionExceptionDescriptor(node) + ExecutionExpressionArithmeticDescriptor(node) +
	                ">";
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		result += "(" + node.constant.ToString() + ")";
		break;
	case ExecutionExpressionIRKind::REFERENCE:
		result += "(#" + std::to_string(node.ref_index) + ")";
		break;
	case ExecutionExpressionIRKind::UNARY:
		result += "." + ExecutionUnaryOpToString(node.unary_op) + "(" + DescribeExecutionExpressionIR(*node.left) + ")";
		break;
	case ExecutionExpressionIRKind::BINARY:
		result += "." + ExecutionBinaryOpToString(node.binary_op) + "(" + DescribeExecutionExpressionIR(*node.left) +
		          "," + DescribeExecutionExpressionIR(*node.right) + ")";
		break;
	case ExecutionExpressionIRKind::CAST:
		result +=
		    ".try=" + string(node.try_cast ? "true" : "false") + "(" + DescribeExecutionExpressionIR(*node.left) + ")";
		break;
	case ExecutionExpressionIRKind::CONJUNCTION:
	case ExecutionExpressionIRKind::COALESCE:
	case ExecutionExpressionIRKind::CONSTANT_OR_NULL:
	case ExecutionExpressionIRKind::IN_LIST:
	case ExecutionExpressionIRKind::BETWEEN:
		result += "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeExecutionExpressionIR(*node.children[i]);
		}
		result += ")";
		break;
	case ExecutionExpressionIRKind::CASE:
		result += "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeExecutionExpressionIR(*node.children[i]);
		}
		result += ",else=" + DescribeExecutionExpressionIR(*node.else_node) + ")";
		break;
	case ExecutionExpressionIRKind::INTRINSIC:
		result += "." + string(ExecutionExpressionIntrinsicKindToString(node.intrinsic)) + "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeExecutionExpressionIR(*node.children[i]);
		}
		result += ")";
		break;
	default:
		break;
	}
	return result;
}

static string GetExecutionExpressionReason(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return "core-ir:constant";
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return "core-ir:reference";
	}
	if (node.kind == ExecutionExpressionIRKind::CAST) {
		return "core-ir:cast";
	}
	if (node.kind == ExecutionExpressionIRKind::CASE) {
		return "core-ir:case";
	}
	if (node.kind == ExecutionExpressionIRKind::COALESCE) {
		return "core-ir:coalesce";
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		return "core-ir:constant-or-null";
	}
	if (node.kind == ExecutionExpressionIRKind::IN_LIST) {
		return "core-ir:in-list";
	}
	if (node.kind == ExecutionExpressionIRKind::BETWEEN) {
		return "core-ir:between";
	}
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC) {
		return string("core-ir:intrinsic:") + ExecutionExpressionIntrinsicKindToString(node.intrinsic);
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION ||
	    (node.kind == ExecutionExpressionIRKind::UNARY && node.unary_op == ExecutionExpressionUnaryOp::NOT)) {
		return "core-ir:boolean";
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.unary_op == ExecutionExpressionUnaryOp::NEGATE) {
		return "core-ir:arithmetic";
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		return "core-ir:null-check";
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && ExecutionExpressionIsComparisonOp(node.binary_op)) {
		return "core-ir:comparison";
	}
	return "core-ir:arithmetic";
}

static bool ExecutionExpressionConstantEqualsInteger(const ExecutionExpressionIR &node, int64_t expected) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT || node.constant.IsNull()) {
		return false;
	}
	auto &type = node.constant.type();
	const bool is_decimal = type.id() == LogicalTypeId::DECIMAL;
	int64_t scaled_expected = expected;
	if (is_decimal) {
		const auto scale = DecimalType::GetScale(type);
		if (scale >= NumericHelper::CACHED_POWERS_OF_TEN) {
			return false;
		}
		scaled_expected *= NumericHelper::POWERS_OF_TEN[scale];
	}
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return node.constant.GetValueUnsafe<int8_t>() == scaled_expected;
	case PhysicalType::INT16:
		return node.constant.GetValueUnsafe<int16_t>() == scaled_expected;
	case PhysicalType::INT32:
		return node.constant.GetValueUnsafe<int32_t>() == scaled_expected;
	case PhysicalType::INT64:
		return node.constant.GetValueUnsafe<int64_t>() == scaled_expected;
	case PhysicalType::UINT8:
		return scaled_expected >= 0 && node.constant.GetValueUnsafe<uint8_t>() == uint64_t(scaled_expected);
	case PhysicalType::UINT16:
		return scaled_expected >= 0 && node.constant.GetValueUnsafe<uint16_t>() == uint64_t(scaled_expected);
	case PhysicalType::UINT32:
		return scaled_expected >= 0 && node.constant.GetValueUnsafe<uint32_t>() == uint64_t(scaled_expected);
	case PhysicalType::UINT64:
		return scaled_expected >= 0 && node.constant.GetValueUnsafe<uint64_t>() == uint64_t(scaled_expected);
	default:
		return false;
	}
}

static bool ExecutionExpressionTypeMatchesParent(const ExecutionExpressionIR &parent,
                                                 const ExecutionExpressionIR &child) {
	return parent.return_type == child.return_type;
}

static unique_ptr<ExecutionExpressionIR>
NormalizeExecutionExpressionArithmeticIdentity(unique_ptr<ExecutionExpressionIR> node) {
	D_ASSERT(node);
	if (node->kind != ExecutionExpressionIRKind::BINARY || !node->left || !node->right) {
		return node;
	}
	switch (node->binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
		if (ExecutionExpressionConstantEqualsInteger(*node->right, 0) &&
		    ExecutionExpressionTypeMatchesParent(*node, *node->left)) {
			return std::move(node->left);
		}
		if (ExecutionExpressionConstantEqualsInteger(*node->left, 0) &&
		    ExecutionExpressionTypeMatchesParent(*node, *node->right)) {
			return std::move(node->right);
		}
		break;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		if (ExecutionExpressionConstantEqualsInteger(*node->right, 0) &&
		    ExecutionExpressionTypeMatchesParent(*node, *node->left)) {
			return std::move(node->left);
		}
		break;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		if (ExecutionExpressionConstantEqualsInteger(*node->right, 1) &&
		    ExecutionExpressionTypeMatchesParent(*node, *node->left)) {
			return std::move(node->left);
		}
		if (ExecutionExpressionConstantEqualsInteger(*node->left, 1) &&
		    ExecutionExpressionTypeMatchesParent(*node, *node->right)) {
			return std::move(node->right);
		}
		break;
	default:
		break;
	}
	return node;
}

static unique_ptr<ExecutionExpressionIR> NormalizeExecutionExpressionIR(unique_ptr<ExecutionExpressionIR> node) {
	if (!node) {
		return nullptr;
	}
	if (node->left) {
		node->left = NormalizeExecutionExpressionIR(std::move(node->left));
	}
	if (node->right) {
		node->right = NormalizeExecutionExpressionIR(std::move(node->right));
	}
	if (node->else_node) {
		node->else_node = NormalizeExecutionExpressionIR(std::move(node->else_node));
	}
	for (auto &child : node->children) {
		child = NormalizeExecutionExpressionIR(std::move(child));
	}
	if (node->kind == ExecutionExpressionIRKind::CAST && !node->try_cast && node->left &&
	    node->left->kind == ExecutionExpressionIRKind::CONSTANT) {
		Value cast_value;
		string error;
		if (node->left->constant.DefaultTryCastAs(node->return_type, cast_value, &error) &&
		    ExecutionExpressionConstantValueMatchesType(cast_value, node->return_type)) {
			auto constant = make_uniq<ExecutionExpressionIR>();
			constant->kind = ExecutionExpressionIRKind::CONSTANT;
			constant->return_type = node->return_type;
			constant->constant = std::move(cast_value);
			return constant;
		}
	}
	node = NormalizeExecutionExpressionArithmeticIdentity(std::move(node));
	return node;
}

static ExecutionExpressionValidityKind GetExecutionExpressionValidity(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		return node.constant.IsNull() ? ExecutionExpressionValidityKind::CONSTANT_NULL
		                              : ExecutionExpressionValidityKind::CONSTANT_VALID;
	case ExecutionExpressionIRKind::REFERENCE:
		return ExecutionExpressionValidityKind::SOURCE;
	case ExecutionExpressionIRKind::UNARY:
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::IS_NULL:
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			return ExecutionExpressionValidityKind::NOT_NULL;
		default:
			return ExecutionExpressionValidityKind::CHILD;
		}
	case ExecutionExpressionIRKind::BINARY:
		return ExecutionExpressionValidityKind::CHILDREN_NULL_PROPAGATING;
	case ExecutionExpressionIRKind::CAST:
		return node.try_cast ? ExecutionExpressionValidityKind::CHILD_OR_CAST_FAILURE
		                     : ExecutionExpressionValidityKind::CHILD;
	case ExecutionExpressionIRKind::CONJUNCTION:
		return ExecutionExpressionValidityKind::THREE_VALUED_BOOLEAN;
	case ExecutionExpressionIRKind::COALESCE:
		return ExecutionExpressionValidityKind::FIRST_VALID_CHILD;
	case ExecutionExpressionIRKind::CONSTANT_OR_NULL:
		return ExecutionExpressionValidityKind::CONSTANT_PLUS_NULL_GUARDS;
	case ExecutionExpressionIRKind::IN_LIST:
		return ExecutionExpressionValidityKind::SQL_IN_LIST;
	case ExecutionExpressionIRKind::BETWEEN:
		return ExecutionExpressionValidityKind::SQL_BETWEEN;
	case ExecutionExpressionIRKind::CASE:
		return ExecutionExpressionValidityKind::SELECTED_BRANCH;
	case ExecutionExpressionIRKind::INTRINSIC:
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::STRING_PREFIX ||
		    node.intrinsic == ExecutionExpressionIntrinsicKind::STRING_SUFFIX ||
		    node.intrinsic == ExecutionExpressionIntrinsicKind::STRING_CONTAINS ||
		    node.intrinsic == ExecutionExpressionIntrinsicKind::STRING_LIKE ||
		    node.intrinsic == ExecutionExpressionIntrinsicKind::STRING_SUBSTRING) {
			return ExecutionExpressionValidityKind::CHILDREN_NULL_PROPAGATING;
		}
		return ExecutionExpressionValidityKind::CHILD;
	default:
		return ExecutionExpressionValidityKind::UNKNOWN;
	}
}

static ExecutionExpressionSourceKind GetExecutionExpressionSource(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		return ExecutionExpressionSourceKind::CONSTANT;
	case ExecutionExpressionIRKind::REFERENCE:
		return ExecutionExpressionSourceKind::VECTOR;
	default:
		return ExecutionExpressionSourceKind::DERIVED;
	}
}

static ExecutionExpressionExceptionKind GetExecutionExpressionExceptionBehavior(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CAST:
		return node.try_cast ? ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR
		                     : ExecutionExpressionExceptionKind::CAST;
	case ExecutionExpressionIRKind::BINARY:
		if (ExecutionExpressionIsComparisonOp(node.binary_op) || !node.arithmetic_overflow_check) {
			return ExecutionExpressionExceptionKind::NONE;
		}
		return ExecutionExpressionExceptionKind::ARITHMETIC;
	case ExecutionExpressionIRKind::INTRINSIC:
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::ERROR) {
			return ExecutionExpressionExceptionKind::ERROR;
		}
		return ExecutionExpressionExceptionKind::NONE;
	case ExecutionExpressionIRKind::UNARY:
		if (node.unary_op == ExecutionExpressionUnaryOp::NEGATE) {
			return ExecutionExpressionExceptionKind::ARITHMETIC;
		}
		break;
	default:
		break;
	}
	return ExecutionExpressionExceptionKind::NONE;
}

static void MergeChildExecutionExpressionTraits(ExecutionExpressionTraits &target,
                                                const ExecutionExpressionTraits &source) {
	target.has_arithmetic_binary = target.has_arithmetic_binary || source.has_arithmetic_binary;
	target.has_integer_arithmetic_result = target.has_integer_arithmetic_result || source.has_integer_arithmetic_result;
	target.has_non_integer_arithmetic_result =
	    target.has_non_integer_arithmetic_result || source.has_non_integer_arithmetic_result;
	target.has_comparison_binary = target.has_comparison_binary || source.has_comparison_binary;
	target.has_integer_comparison_operands =
	    target.has_integer_comparison_operands || source.has_integer_comparison_operands;
	target.has_non_integer_comparison_operands =
	    target.has_non_integer_comparison_operands || source.has_non_integer_comparison_operands;
	target.has_conjunction = target.has_conjunction || source.has_conjunction;
	target.expression_node_count += source.expression_node_count;
	target.reference_expression_count += source.reference_expression_count;
	target.predicate_expression_count += source.predicate_expression_count;
	target.control_expression_count += source.control_expression_count;
	target.arithmetic_binary_count += source.arithmetic_binary_count;
	target.integer_arithmetic_result_count += source.integer_arithmetic_result_count;
	target.non_integer_arithmetic_result_count += source.non_integer_arithmetic_result_count;
	target.comparison_binary_count += source.comparison_binary_count;
	target.integer_comparison_operand_count += source.integer_comparison_operand_count;
	target.non_integer_comparison_operand_count += source.non_integer_comparison_operand_count;
	target.conjunction_count += source.conjunction_count;
	target.string_predicate_count += source.string_predicate_count;
	target.high_cost_string_predicate_count += source.high_cost_string_predicate_count;
	target.string_like_count += source.string_like_count;
	target.string_contains_count += source.string_contains_count;
	target.string_prefix_count += source.string_prefix_count;
	target.string_suffix_count += source.string_suffix_count;
}

static void AnnotateExecutionExpressionStringPredicate(ExecutionExpressionTraits &traits,
                                                       ExecutionExpressionIntrinsicKind intrinsic) {
	switch (intrinsic) {
	case ExecutionExpressionIntrinsicKind::STRING_LIKE:
		traits.predicate_expression_count++;
		traits.string_predicate_count++;
		traits.high_cost_string_predicate_count++;
		traits.string_like_count++;
		break;
	case ExecutionExpressionIntrinsicKind::STRING_CONTAINS:
		traits.predicate_expression_count++;
		traits.string_predicate_count++;
		traits.high_cost_string_predicate_count++;
		traits.string_contains_count++;
		break;
	case ExecutionExpressionIntrinsicKind::STRING_PREFIX:
		traits.predicate_expression_count++;
		traits.string_predicate_count++;
		traits.string_prefix_count++;
		break;
	case ExecutionExpressionIntrinsicKind::STRING_SUFFIX:
		traits.predicate_expression_count++;
		traits.string_predicate_count++;
		traits.string_suffix_count++;
		break;
	default:
		break;
	}
}

static ExecutionExpressionTraits AnnotateExecutionExpressionIR(ExecutionExpressionIR &node) {
	ExecutionExpressionTraits traits;
	if (node.left) {
		MergeChildExecutionExpressionTraits(traits, AnnotateExecutionExpressionIR(*node.left));
	}
	if (node.right) {
		MergeChildExecutionExpressionTraits(traits, AnnotateExecutionExpressionIR(*node.right));
	}
	if (node.else_node) {
		MergeChildExecutionExpressionTraits(traits, AnnotateExecutionExpressionIR(*node.else_node));
	}
	for (auto &child : node.children) {
		MergeChildExecutionExpressionTraits(traits, AnnotateExecutionExpressionIR(*child));
	}
	node.physical_type = node.return_type.InternalType();
	node.validity = GetExecutionExpressionValidity(node);
	node.source = GetExecutionExpressionSource(node);
	node.exception_behavior = GetExecutionExpressionExceptionBehavior(node);

	traits.expression_node_count++;
	traits.root_is_reference = node.kind == ExecutionExpressionIRKind::REFERENCE;
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		traits.reference_expression_count++;
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY) {
		if (ExecutionExpressionIsArithmeticOp(node.binary_op)) {
			traits.has_arithmetic_binary = true;
			traits.arithmetic_binary_count++;
			if (ExecutionExpressionIsIntegralType(node.return_type)) {
				traits.has_integer_arithmetic_result = true;
				traits.integer_arithmetic_result_count++;
			} else {
				traits.has_non_integer_arithmetic_result = true;
				traits.non_integer_arithmetic_result_count++;
			}
		}
		if (ExecutionExpressionIsComparisonOp(node.binary_op)) {
			traits.has_comparison_binary = true;
			traits.predicate_expression_count++;
			traits.comparison_binary_count++;
			if (ExecutionExpressionBinaryOperandsAreIntegral(node)) {
				traits.has_integer_comparison_operands = true;
				traits.integer_comparison_operand_count++;
			} else {
				traits.has_non_integer_comparison_operands = true;
				traits.non_integer_comparison_operand_count++;
			}
		}
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		traits.has_conjunction = true;
		traits.predicate_expression_count++;
		traits.control_expression_count++;
		traits.conjunction_count++;
	}
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC) {
		AnnotateExecutionExpressionStringPredicate(traits, node.intrinsic);
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY &&
	    (node.unary_op == ExecutionExpressionUnaryOp::NOT || node.unary_op == ExecutionExpressionUnaryOp::IS_NULL ||
	     node.unary_op == ExecutionExpressionUnaryOp::IS_NOT_NULL)) {
		traits.control_expression_count++;
	}
	if (node.kind == ExecutionExpressionIRKind::CASE || node.kind == ExecutionExpressionIRKind::COALESCE ||
	    node.kind == ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		traits.control_expression_count++;
	}
	if (node.kind == ExecutionExpressionIRKind::IN_LIST || node.kind == ExecutionExpressionIRKind::BETWEEN) {
		traits.predicate_expression_count++;
		traits.control_expression_count++;
	}
	return traits;
}

static bool TryGetExecutionExpressionArithmeticOp(const BoundFunctionExpression &expression,
                                                  ExecutionExpressionBinaryOp &op) {
	if (expression.GetChildren().size() != 2) {
		return false;
	}
	auto name = expression.Function().GetName().GetIdentifierName();
	if (name == "+") {
		if (!ExecutionExpressionIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::ADD;
		return true;
	}
	if (name == "-") {
		if (!ExecutionExpressionIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::SUBTRACT;
		return true;
	}
	if (name == "*") {
		if (!ExecutionExpressionIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::MULTIPLY;
		return true;
	}
	if (name == "/") {
		if (!ExecutionExpressionIsFloatingType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::DIVIDE;
		return true;
	}
	if (name == "//") {
		if (!ExecutionExpressionIsIntegralType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::INTEGER_DIVIDE;
		return true;
	}
	if (name == "%") {
		if (!ExecutionExpressionIsIntegralType(expression.GetReturnType())) {
			return false;
		}
		op = ExecutionExpressionBinaryOp::MODULO;
		return true;
	}
	return false;
}

static bool TryGetExecutionExpressionComparisonOp(ExpressionType type, ExecutionExpressionBinaryOp &op) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		op = ExecutionExpressionBinaryOp::COMPARE_EQUAL;
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		op = ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL;
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		op = ExecutionExpressionBinaryOp::COMPARE_LESSTHAN;
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		op = ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN;
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		op = ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO;
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		op = ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO;
		return true;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		op = ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM;
		return true;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		op = ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM;
		return true;
	default:
		return false;
	}
}

static unique_ptr<ExecutionExpressionIR> TryBuildExecutionExpressionIR(const Expression &expression);

static unique_ptr<ExecutionExpressionIR> BuildExecutionExpressionBooleanConstant(bool value) {
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::CONSTANT;
	node->return_type = LogicalType::BOOLEAN;
	node->constant = Value::BOOLEAN(value);
	return node;
}

static bool ExecutionExpressionIsUnsignedIntegerCompressionType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsIntegralCompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_compress_integral_");
}

static bool ExecutionExpressionIsIntegralDecompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_decompress_integral_");
}

static bool ExecutionExpressionIsStringCompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_compress_string_");
}

static bool ExecutionExpressionIsStringDecompressFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "__internal_decompress_string";
}

static bool ExecutionExpressionIsStringCompressionStorageType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionExpressionIsStringPrefixFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "prefix";
}

static bool ExecutionExpressionIsStringSuffixFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "suffix";
}

static bool ExecutionExpressionIsStringContainsFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "contains" && function.GetChildren().size() == 2 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::VARCHAR &&
	       function.GetChildren()[1]->GetReturnType().id() == LogicalTypeId::VARCHAR;
}

static bool ExecutionExpressionIsStringLikeFunction(const BoundFunctionExpression &function) {
	auto name = function.Function().GetName().GetIdentifierName();
	return (name == "~~" || name == "!~~") && function.GetChildren().size() == 2 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::VARCHAR &&
	       function.GetChildren()[1]->GetReturnType().id() == LogicalTypeId::VARCHAR;
}

static bool ExecutionExpressionIsStringSubstringFunction(const BoundFunctionExpression &function) {
	auto name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
	return name == "substring" || name == "substr";
}

static bool ExecutionExpressionIsOptionalTableFilterFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "__internal_tablefilter_optional";
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionOptionalTableFilterIR(const BoundFunctionExpression &function,
                                                 const LogicalType &return_type) {
	if (!ExecutionExpressionIsOptionalTableFilterFunction(function) || function.GetChildren().size() != 1 ||
	    return_type.id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	return BuildExecutionExpressionBooleanConstant(true);
}

static unique_ptr<ExecutionExpressionIR>
BuildExecutionExpressionStringMatchIR(ExecutionExpressionIntrinsicKind intrinsic,
                                      unique_ptr<ExecutionExpressionIR> source,
                                      unique_ptr<ExecutionExpressionIR> pattern, const LogicalType &return_type) {
	if (pattern->kind == ExecutionExpressionIRKind::CONSTANT && pattern->constant.IsNull()) {
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value(LogicalType::BOOLEAN);
		return node;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = intrinsic;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(pattern));
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionStringMatchIR(const BoundFunctionExpression &function, const LogicalType &return_type,
                                         ExecutionExpressionIntrinsicKind intrinsic) {
	if (function.GetChildren().size() != 2 || return_type.id() != LogicalTypeId::BOOLEAN ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    function.GetChildren()[1]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	auto source = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	auto pattern = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
	if (!source || !pattern || source->return_type.id() != LogicalTypeId::VARCHAR ||
	    pattern->return_type.id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	return BuildExecutionExpressionStringMatchIR(intrinsic, std::move(source), std::move(pattern), return_type);
}

static bool ExecutionExpressionLikePatternIsPercentOnly(const string &pattern) {
	for (auto character : pattern) {
		if (character == '_') {
			return false;
		}
	}
	return true;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionStringLikeIR(const BoundFunctionExpression &function, const LogicalType &return_type) {
	if (!ExecutionExpressionIsStringLikeFunction(function) || return_type.id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	auto source = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	auto pattern = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
	if (!source || !pattern || source->return_type.id() != LogicalTypeId::VARCHAR ||
	    pattern->return_type.id() != LogicalTypeId::VARCHAR || pattern->kind != ExecutionExpressionIRKind::CONSTANT) {
		return nullptr;
	}
	if (pattern->constant.IsNull()) {
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value(LogicalType::BOOLEAN);
		return node;
	}
	if (!ExecutionExpressionLikePatternIsPercentOnly(StringValue::Get(pattern->constant))) {
		return nullptr;
	}
	auto like = BuildExecutionExpressionStringMatchIR(ExecutionExpressionIntrinsicKind::STRING_LIKE, std::move(source),
	                                                  std::move(pattern), return_type);
	if (function.Function().GetName().GetIdentifierName() == "~~") {
		return like;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::UNARY;
	node->return_type = return_type;
	node->unary_op = ExecutionExpressionUnaryOp::NOT;
	node->left = std::move(like);
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionStringSubstringIR(const BoundFunctionExpression &function, const LogicalType &return_type) {
	if (!ExecutionExpressionIsStringSubstringFunction(function) || return_type.id() != LogicalTypeId::VARCHAR ||
	    (function.GetChildren().size() != 2 && function.GetChildren().size() != 3) ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    !function.GetChildren()[1]->GetReturnType().IsIntegral()) {
		return nullptr;
	}
	if (function.GetChildren().size() == 3 && !function.GetChildren()[2]->GetReturnType().IsIntegral()) {
		return nullptr;
	}
	auto source = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	auto offset = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
	if (!source || !offset || source->return_type.id() != LogicalTypeId::VARCHAR || !offset->return_type.IsIntegral()) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::STRING_SUBSTRING;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(offset));
	if (function.GetChildren().size() == 3) {
		auto length = TryBuildExecutionExpressionIR(*function.GetChildren()[2]);
		if (!length || !length->return_type.IsIntegral()) {
			return nullptr;
		}
		node->children.push_back(std::move(length));
	}
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionIntegralCompressIR(const BoundFunctionExpression &function, const LogicalType &return_type) {
	if (!ExecutionExpressionIsIntegralCompressFunction(function) || function.GetChildren().size() != 2 ||
	    !ExecutionExpressionIsUnsignedIntegerCompressionType(return_type)) {
		return nullptr;
	}
	auto &source_expr = *function.GetChildren()[0];
	auto &minimum_expr = *function.GetChildren()[1];
	if (!ExecutionExpressionIsCompressibleIntegralType(source_expr.GetReturnType()) ||
	    source_expr.GetReturnType() != minimum_expr.GetReturnType()) {
		return nullptr;
	}
	auto source = TryBuildExecutionExpressionIR(source_expr);
	auto minimum = TryBuildExecutionExpressionIR(minimum_expr);
	if (!source || !minimum || minimum->kind != ExecutionExpressionIRKind::CONSTANT || minimum->constant.IsNull() ||
	    source->return_type != minimum->return_type) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(minimum));
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionIntegralDecompressIR(const BoundFunctionExpression &function,
                                                const LogicalType &return_type) {
	if (!ExecutionExpressionIsIntegralDecompressFunction(function) || function.GetChildren().size() != 2 ||
	    !ExecutionExpressionIsCompressibleIntegralType(return_type)) {
		return nullptr;
	}
	auto &source_expr = *function.GetChildren()[0];
	auto &minimum_expr = *function.GetChildren()[1];
	if (!ExecutionExpressionIsUnsignedIntegerCompressionType(source_expr.GetReturnType()) ||
	    minimum_expr.GetReturnType() != return_type) {
		return nullptr;
	}
	auto source = TryBuildExecutionExpressionIR(source_expr);
	auto minimum = TryBuildExecutionExpressionIR(minimum_expr);
	if (!source || !minimum || minimum->kind != ExecutionExpressionIRKind::CONSTANT || minimum->constant.IsNull() ||
	    minimum->return_type != return_type) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::INTEGRAL_DECOMPRESS;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(minimum));
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionStringCompressIR(const BoundFunctionExpression &function, const LogicalType &return_type) {
	if (!ExecutionExpressionIsStringCompressFunction(function) || function.GetChildren().size() != 1 ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    !ExecutionExpressionIsUnsignedIntegerCompressionType(return_type)) {
		return nullptr;
	}
	auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	if (!child || child->return_type.id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::STRING_COMPRESS;
	node->children.push_back(std::move(child));
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionStringDecompressIR(const BoundFunctionExpression &function, const LogicalType &return_type) {
	if (!ExecutionExpressionIsStringDecompressFunction(function) || function.GetChildren().size() != 1 ||
	    return_type.id() != LogicalTypeId::VARCHAR ||
	    !ExecutionExpressionIsStringCompressionStorageType(function.GetChildren()[0]->GetReturnType())) {
		return nullptr;
	}
	auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	if (!child || !ExecutionExpressionIsStringCompressionStorageType(child->return_type)) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::STRING_DECOMPRESS;
	node->children.push_back(std::move(child));
	return node;
}

static bool ExecutionExpressionIsDateYearFunction(const BoundFunctionExpression &function,
                                                  const LogicalType &return_type) {
	return function.Function().GetName().GetIdentifierName() == "year" && function.GetChildren().size() == 1 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::DATE &&
	       return_type.id() == LogicalTypeId::BIGINT;
}

static unique_ptr<ExecutionExpressionIR> TryBuildExecutionExpressionDateYearIR(const BoundFunctionExpression &function,
                                                                               const LogicalType &return_type) {
	if (!ExecutionExpressionIsDateYearFunction(function, return_type)) {
		return nullptr;
	}
	auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	if (!child || child->return_type.id() != LogicalTypeId::DATE) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::DATE_YEAR;
	node->children.push_back(std::move(child));
	return node;
}

static unique_ptr<ExecutionExpressionIR> TryBuildExecutionExpressionErrorIR(const BoundFunctionExpression &function,
                                                                            const LogicalType &return_type) {
	if (function.Function().GetName().GetIdentifierName() != "error") {
		return nullptr;
	}
	if (function.GetChildren().size() != 1 || !ExecutionExpressionIsScalarType(return_type)) {
		return nullptr;
	}
	auto message = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
	if (!message || message->return_type.id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = ExecutionExpressionIntrinsicKind::ERROR;
	node->children.push_back(std::move(message));
	return node;
}

static bool ExecutionExpressionIsErrorIntrinsic(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::INTRINSIC &&
	       node.intrinsic == ExecutionExpressionIntrinsicKind::ERROR;
}

static bool ExecutionExpressionBranchTypeMatches(const ExecutionExpressionIR &node, const LogicalType &target_type) {
	return node.return_type == target_type ||
	       (ExecutionExpressionIsErrorIntrinsic(node) && node.return_type.id() == LogicalTypeId::SQLNULL);
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionInListIR(ExpressionType type, const LogicalType &return_type,
                                    const vector<unique_ptr<Expression>> &children) {
	if (type != ExpressionType::COMPARE_IN && type != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	if (children.size() < 2) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::IN_LIST;
	node->return_type = return_type;
	node->not_in = type == ExpressionType::COMPARE_NOT_IN;
	for (auto &child_expr : children) {
		auto child = TryBuildExecutionExpressionIR(*child_expr);
		if (!child || !ExecutionExpressionIsComparableType(child->return_type)) {
			return nullptr;
		}
		if (!node->children.empty() &&
		    !ExecutionExpressionIsSameOrNullType(node->children[0]->return_type, child->return_type)) {
			return nullptr;
		}
		node->children.push_back(std::move(child));
	}
	return node;
}

static unique_ptr<ExecutionExpressionIR>
TryBuildExecutionExpressionBetweenIR(ExpressionType type, const BoundFunctionExpression &expression) {
	if (type != ExpressionType::COMPARE_BETWEEN && type != ExpressionType::COMPARE_NOT_BETWEEN) {
		return nullptr;
	}
	if (expression.GetChildren().size() != 3 || expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	auto node = make_uniq<ExecutionExpressionIR>();
	node->kind = ExecutionExpressionIRKind::BETWEEN;
	node->return_type = expression.GetReturnType();
	node->not_between = type == ExpressionType::COMPARE_NOT_BETWEEN;
	node->lower_inclusive = BoundBetweenExpression::LowerInclusive(expression);
	node->upper_inclusive = BoundBetweenExpression::UpperInclusive(expression);
	for (auto &child_expr : expression.GetChildren()) {
		auto child = TryBuildExecutionExpressionIR(*child_expr);
		if (!child || !ExecutionExpressionIsComparableType(child->return_type)) {
			return nullptr;
		}
		if (!node->children.empty() &&
		    !ExecutionExpressionIsSameOrNullType(node->children[0]->return_type, child->return_type)) {
			return nullptr;
		}
		node->children.push_back(std::move(child));
	}
	return node;
}

static unique_ptr<ExecutionExpressionIR> TryBuildExecutionExpressionIR(const Expression &expression) {
	if (expression.HasParameter()) {
		return nullptr;
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT: {
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto &constant = expression.Cast<BoundConstantExpression>();
		if (!ExecutionExpressionConstantValueMatchesType(constant.GetValue(), expression.GetReturnType())) {
			return nullptr;
		}
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CONSTANT;
		node->return_type = expression.GetReturnType();
		node->constant = constant.GetValue();
		return node;
	}
	case ExpressionClass::BOUND_REF: {
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto &ref = expression.Cast<BoundReferenceExpression>();
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::REFERENCE;
		node->return_type = expression.GetReturnType();
		node->ref_index = ref.Index();
		return node;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expression.Cast<BoundOperatorExpression>();
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_COALESCE: {
			if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
				return nullptr;
			}
			auto node = make_uniq<ExecutionExpressionIR>();
			node->kind = ExecutionExpressionIRKind::COALESCE;
			node->return_type = expression.GetReturnType();
			for (auto &child_expr : op.GetChildren()) {
				auto child = TryBuildExecutionExpressionIR(*child_expr);
				if (!child || child->return_type != expression.GetReturnType()) {
					return nullptr;
				}
				node->children.push_back(std::move(child));
			}
			return node;
		}
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return TryBuildExecutionExpressionInListIR(expression.GetExpressionType(), expression.GetReturnType(),
			                                           op.GetChildren());
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			if (op.GetChildren().size() != 1) {
				return nullptr;
			}
			auto child = TryBuildExecutionExpressionIR(*op.GetChildren()[0]);
			if (!child) {
				return nullptr;
			}
			auto node = make_uniq<ExecutionExpressionIR>();
			node->kind = ExecutionExpressionIRKind::UNARY;
			node->return_type = expression.GetReturnType();
			switch (expression.GetExpressionType()) {
			case ExpressionType::OPERATOR_NOT:
				if (child->return_type.id() != LogicalTypeId::BOOLEAN) {
					return nullptr;
				}
				if (child->kind == ExecutionExpressionIRKind::UNARY &&
				    child->unary_op == ExecutionExpressionUnaryOp::NOT) {
					return std::move(child->left);
				}
				if (child->kind == ExecutionExpressionIRKind::UNARY &&
				    child->unary_op == ExecutionExpressionUnaryOp::IS_NULL) {
					child->unary_op = ExecutionExpressionUnaryOp::IS_NOT_NULL;
					return child;
				}
				if (child->kind == ExecutionExpressionIRKind::UNARY &&
				    child->unary_op == ExecutionExpressionUnaryOp::IS_NOT_NULL) {
					child->unary_op = ExecutionExpressionUnaryOp::IS_NULL;
					return child;
				}
				if (child->kind == ExecutionExpressionIRKind::BINARY &&
				    TryInvertExecutionComparisonOp(child->binary_op, child->binary_op)) {
					return child;
				}
				node->unary_op = ExecutionExpressionUnaryOp::NOT;
				break;
			case ExpressionType::OPERATOR_IS_NULL:
				node->unary_op = ExecutionExpressionUnaryOp::IS_NULL;
				break;
			case ExpressionType::OPERATOR_IS_NOT_NULL:
				node->unary_op = ExecutionExpressionUnaryOp::IS_NOT_NULL;
				break;
			default:
				throw InternalException("Invalid execution expression IR unary operator build");
			}
			node->left = std::move(child);
			return node;
		}
		default:
			return nullptr;
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (auto optional_filter =
		        TryBuildExecutionExpressionOptionalTableFilterIR(function, expression.GetReturnType())) {
			return optional_filter;
		}
		if (auto intrinsic = TryBuildExecutionExpressionStringLikeIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (ExecutionExpressionIsStringPrefixFunction(function)) {
			return TryBuildExecutionExpressionStringMatchIR(function, expression.GetReturnType(),
			                                                ExecutionExpressionIntrinsicKind::STRING_PREFIX);
		}
		if (ExecutionExpressionIsStringSuffixFunction(function)) {
			return TryBuildExecutionExpressionStringMatchIR(function, expression.GetReturnType(),
			                                                ExecutionExpressionIntrinsicKind::STRING_SUFFIX);
		}
		if (ExecutionExpressionIsStringContainsFunction(function)) {
			return TryBuildExecutionExpressionStringMatchIR(function, expression.GetReturnType(),
			                                                ExecutionExpressionIntrinsicKind::STRING_CONTAINS);
		}
		if (auto intrinsic = TryBuildExecutionExpressionStringSubstringIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionIntegralCompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionIntegralDecompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionStringCompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionStringDecompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionDateYearIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildExecutionExpressionErrorIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (function.Function().GetName().GetIdentifierName() == "constant_or_null") {
			if (function.GetChildren().size() < 2 || !ExecutionExpressionIsScalarType(expression.GetReturnType())) {
				return nullptr;
			}
			auto node = make_uniq<ExecutionExpressionIR>();
			node->kind = ExecutionExpressionIRKind::CONSTANT_OR_NULL;
			node->return_type = expression.GetReturnType();
			for (auto &child_expr : function.GetChildren()) {
				auto child = TryBuildExecutionExpressionIR(*child_expr);
				if (!child || !ExecutionExpressionIsScalarType(child->return_type)) {
					return nullptr;
				}
				node->children.push_back(std::move(child));
			}
			return node;
		}
		auto in_list = TryBuildExecutionExpressionInListIR(expression.GetExpressionType(), expression.GetReturnType(),
		                                                   function.GetChildren());
		if (in_list) {
			return in_list;
		}
		auto between = TryBuildExecutionExpressionBetweenIR(expression.GetExpressionType(), function);
		if (between) {
			return between;
		}
		if (function.IsOperator() && function.GetChildren().size() == 1 &&
		    function.Function().GetName().GetIdentifierName() == "-" &&
		    ExecutionExpressionIsNegatableType(expression.GetReturnType())) {
			auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return nullptr;
			}
			auto node = make_uniq<ExecutionExpressionIR>();
			node->kind = ExecutionExpressionIRKind::UNARY;
			node->return_type = expression.GetReturnType();
			node->unary_op = ExecutionExpressionUnaryOp::NEGATE;
			node->left = std::move(child);
			return node;
		}
		ExecutionExpressionBinaryOp op;
		bool is_comparison = TryGetExecutionExpressionComparisonOp(expression.GetExpressionType(), op);
		bool is_arithmetic = TryGetExecutionExpressionArithmeticOp(function, op);
		if (!is_comparison && !is_arithmetic) {
			return nullptr;
		}
		if (function.GetChildren().size() != 2) {
			return nullptr;
		}
		auto left = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
		auto right = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
		if (!left || !right) {
			return nullptr;
		}
		if (is_comparison &&
		    (op == ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM ||
		     op == ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM) &&
		    (ExecutionExpressionIsNullConstant(*left) || ExecutionExpressionIsNullConstant(*right))) {
			return TryCanonicalizeExecutionNullDistinctComparison(op, std::move(left), std::move(right));
		}
		if (is_comparison && (!ExecutionExpressionIsComparableType(left->return_type) ||
		                      !ExecutionExpressionIsComparableType(right->return_type))) {
			return nullptr;
		}
		if (is_comparison && !ExecutionExpressionIsSameOrNullType(left->return_type, right->return_type)) {
			return nullptr;
		}
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::BINARY;
		node->return_type = expression.GetReturnType();
		node->binary_op = op;
		node->arithmetic_overflow_check =
		    !is_arithmetic || function.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR;
		node->left = std::move(left);
		node->right = std::move(right);
		return node;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		if (!ExecutionExpressionIsScalarType(cast.source_type()) ||
		    !ExecutionExpressionIsScalarType(cast.TargetType())) {
			return nullptr;
		}
		if (!cast.IsTryCast() && cast.CanThrow() &&
		    (!ExecutionExpressionIsSignedIntegerType(cast.source_type()) ||
		     (!ExecutionExpressionIsSignedIntegerType(cast.TargetType()) &&
		      !ExecutionExpressionIsUnsignedIntegerType(cast.TargetType())))) {
			return nullptr;
		}
		auto child = TryBuildExecutionExpressionIR(cast.Child());
		if (!child) {
			return nullptr;
		}
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CAST;
		node->return_type = cast.TargetType();
		node->query_location = expression.GetQueryLocation();
		node->try_cast = cast.IsTryCast();
		node->left = std::move(child);
		return node;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CONJUNCTION;
		node->return_type = expression.GetReturnType();
		switch (expression.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			node->conjunction_op = ExecutionExpressionConjunctionOp::AND;
			break;
		case ExpressionType::CONJUNCTION_OR:
			node->conjunction_op = ExecutionExpressionConjunctionOp::OR;
			break;
		default:
			return nullptr;
		}
		for (auto &child_expr : conjunction.GetChildren()) {
			auto child = TryBuildExecutionExpressionIR(*child_expr);
			if (!child || child->return_type.id() != LogicalTypeId::BOOLEAN) {
				return nullptr;
			}
			node->children.push_back(std::move(child));
		}
		return node;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expression.Cast<BoundCaseExpression>();
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto node = make_uniq<ExecutionExpressionIR>();
		node->kind = ExecutionExpressionIRKind::CASE;
		node->return_type = expression.GetReturnType();
		for (auto &case_check : case_expr.CaseChecks()) {
			auto when_node = TryBuildExecutionExpressionIR(*case_check.when_expr);
			auto then_node = TryBuildExecutionExpressionIR(*case_check.then_expr);
			if (!when_node || when_node->return_type.id() != LogicalTypeId::BOOLEAN || !then_node ||
			    !ExecutionExpressionBranchTypeMatches(*then_node, expression.GetReturnType())) {
				return nullptr;
			}
			node->children.push_back(std::move(when_node));
			node->children.push_back(std::move(then_node));
		}
		auto else_node = TryBuildExecutionExpressionIR(case_expr.Else());
		if (!else_node || !ExecutionExpressionBranchTypeMatches(*else_node, expression.GetReturnType())) {
			return nullptr;
		}
		node->else_node = std::move(else_node);
		return node;
	}
	default:
		return nullptr;
	}
}

static string ExecutionExpressionBool(bool value) {
	return value ? "true" : "false";
}

static string DescribeExecutionExpressionSubject(const Expression &expression) {
	string result = "class=" + ExpressionClassToString(expression.GetExpressionClass());
	result += ";type=" + ExpressionTypeToString(expression.GetExpressionType());
	result += ";return=" + expression.GetReturnType().ToString();
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expression.Cast<BoundFunctionExpression>();
		result += ";function=" + function.Function().GetName().GetIdentifierName();
		result += ";is_operator=" + ExecutionExpressionBool(function.IsOperator());
		result += ";children=" + std::to_string(function.GetChildren().size());
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expression.Cast<BoundOperatorExpression>();
		result += ";children=" + std::to_string(op.GetChildren().size());
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		result += ";source=" + cast.source_type().ToString();
		result += ";target=" + cast.TargetType().ToString();
		result += ";try_cast=" + ExecutionExpressionBool(cast.IsTryCast());
		result += ";can_throw=" + ExecutionExpressionBool(cast.CanThrow());
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		result += ";children=" + std::to_string(conjunction.GetChildren().size());
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expression.Cast<BoundCaseExpression>();
		result += ";case_checks=" + std::to_string(case_expr.CaseChecks().size());
		break;
	}
	default:
		break;
	}
	return result;
}

static string BuildExecutionExpressionFailureReason(const Expression &expression);

static string BuildExecutionExpressionChildFailureReason(const string &reason, idx_t child_idx,
                                                         const Expression &child) {
	auto child_ir = TryBuildExecutionExpressionIR(child);
	if (child_ir) {
		return string();
	}
	return "reason=" + reason + ";child=" + std::to_string(child_idx) + ";child_reason=(" +
	       BuildExecutionExpressionFailureReason(child) + ")";
}

static string BuildExecutionExpressionReasonWithSubject(const string &reason, const Expression &expression) {
	return "reason=" + reason + ";" + DescribeExecutionExpressionSubject(expression);
}

static string BuildExecutionExpressionInListFailureReason(const string &reason_prefix, ExpressionType type,
                                                          const LogicalType &return_type,
                                                          const vector<unique_ptr<Expression>> &children,
                                                          const Expression &expression) {
	if (type != ExpressionType::COMPARE_IN && type != ExpressionType::COMPARE_NOT_IN) {
		return string();
	}
	if (children.size() < 2) {
		return BuildExecutionExpressionReasonWithSubject(reason_prefix + "_needs_at_least_two_children", expression);
	}
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child_expr = *children[child_idx];
		auto child = TryBuildExecutionExpressionIR(child_expr);
		if (!child) {
			return BuildExecutionExpressionChildFailureReason(reason_prefix + "_child_unsupported", child_idx,
			                                                  child_expr);
		}
		if (!ExecutionExpressionIsComparableType(child->return_type)) {
			auto result =
			    BuildExecutionExpressionReasonWithSubject(reason_prefix + "_child_not_comparable", expression);
			result += ";child=" + std::to_string(child_idx);
			result += ";child_type=" + child->return_type.ToString();
			return result;
		}
		if (child_idx > 0) {
			auto first_child = TryBuildExecutionExpressionIR(*children[0]);
			D_ASSERT(first_child);
			if (!ExecutionExpressionIsSameOrNullType(first_child->return_type, child->return_type)) {
				auto result = BuildExecutionExpressionReasonWithSubject(reason_prefix + "_type_mismatch", expression);
				result += ";left_type=" + first_child->return_type.ToString();
				result += ";right_type=" + child->return_type.ToString();
				result += ";child=" + std::to_string(child_idx);
				return result;
			}
		}
	}
	return BuildExecutionExpressionReasonWithSubject(reason_prefix + "_unknown", expression) +
	       ";return=" + return_type.ToString();
}

static string BuildExecutionExpressionBetweenFailureReason(ExpressionType type, const BoundFunctionExpression &function,
                                                           const Expression &expression) {
	if (type != ExpressionType::COMPARE_BETWEEN && type != ExpressionType::COMPARE_NOT_BETWEEN) {
		return string();
	}
	if (function.GetChildren().size() != 3) {
		return BuildExecutionExpressionReasonWithSubject("between_needs_three_children", expression);
	}
	if (function.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return BuildExecutionExpressionReasonWithSubject("between_return_not_boolean", expression);
	}
	for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
		auto &child_expr = *function.GetChildren()[child_idx];
		auto child = TryBuildExecutionExpressionIR(child_expr);
		if (!child) {
			return BuildExecutionExpressionChildFailureReason("between_child_unsupported", child_idx, child_expr);
		}
		if (!ExecutionExpressionIsComparableType(child->return_type)) {
			auto result = BuildExecutionExpressionReasonWithSubject("between_child_not_comparable", expression);
			result += ";child=" + std::to_string(child_idx);
			result += ";child_type=" + child->return_type.ToString();
			return result;
		}
		if (child_idx > 0) {
			auto first_child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			D_ASSERT(first_child);
			if (!ExecutionExpressionIsSameOrNullType(first_child->return_type, child->return_type)) {
				auto result = BuildExecutionExpressionReasonWithSubject("between_type_mismatch", expression);
				result += ";left_type=" + first_child->return_type.ToString();
				result += ";right_type=" + child->return_type.ToString();
				result += ";child=" + std::to_string(child_idx);
				return result;
			}
		}
	}
	return BuildExecutionExpressionReasonWithSubject("between_unknown", expression);
}

static string BuildExecutionExpressionFailureReason(const Expression &expression) {
	if (expression.HasParameter()) {
		return BuildExecutionExpressionReasonWithSubject("has_parameter", expression);
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT: {
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return BuildExecutionExpressionReasonWithSubject("constant_type_not_scalar", expression);
		}
		auto &constant = expression.Cast<BoundConstantExpression>();
		if (!ExecutionExpressionConstantValueMatchesType(constant.GetValue(), expression.GetReturnType())) {
			auto result = BuildExecutionExpressionReasonWithSubject("constant_value_type_mismatch", expression);
			result += ";value_type=" + constant.GetValue().type().ToString();
			return result;
		}
		break;
	}
	case ExpressionClass::BOUND_REF:
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return BuildExecutionExpressionReasonWithSubject("reference_type_not_scalar", expression);
		}
		break;
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expression.Cast<BoundOperatorExpression>();
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_COALESCE: {
			if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("coalesce_return_not_scalar", expression);
			}
			for (idx_t child_idx = 0; child_idx < op.GetChildren().size(); child_idx++) {
				auto &child_expr = *op.GetChildren()[child_idx];
				auto child = TryBuildExecutionExpressionIR(child_expr);
				if (!child) {
					return BuildExecutionExpressionChildFailureReason("coalesce_child_unsupported", child_idx,
					                                                  child_expr);
				}
				if (child->return_type != expression.GetReturnType()) {
					auto result = BuildExecutionExpressionReasonWithSubject("coalesce_child_type_mismatch", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child->return_type.ToString();
					return result;
				}
			}
			break;
		}
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return BuildExecutionExpressionInListFailureReason(
			    "in_list", expression.GetExpressionType(), expression.GetReturnType(), op.GetChildren(), expression);
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			if (op.GetChildren().size() != 1) {
				return BuildExecutionExpressionReasonWithSubject("unary_operator_needs_one_child", expression);
			}
			auto child = TryBuildExecutionExpressionIR(*op.GetChildren()[0]);
			if (!child) {
				return BuildExecutionExpressionChildFailureReason("unary_child_unsupported", 0, *op.GetChildren()[0]);
			}
			if (expression.GetExpressionType() == ExpressionType::OPERATOR_NOT &&
			    child->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildExecutionExpressionReasonWithSubject("not_child_not_boolean", expression);
				result += ";child_type=" + child->return_type.ToString();
				return result;
			}
			break;
		}
		default:
			return BuildExecutionExpressionReasonWithSubject("bound_operator_type_unsupported", expression);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expression.Cast<BoundFunctionExpression>();
		auto function_name = function.Function().GetName().GetIdentifierName();
		if (ExecutionExpressionIsOptionalTableFilterFunction(function)) {
			if (function.GetChildren().size() != 1) {
				return BuildExecutionExpressionReasonWithSubject("optional_table_filter_needs_one_child", expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildExecutionExpressionReasonWithSubject("optional_table_filter_return_not_boolean",
				                                                 expression);
			}
			break;
		}
		if (ExecutionExpressionIsStringLikeFunction(function)) {
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildExecutionExpressionReasonWithSubject("string_like_return_not_boolean", expression);
			}
			auto source = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			if (!source) {
				return BuildExecutionExpressionChildFailureReason("string_like_source_unsupported", 0,
				                                                  *function.GetChildren()[0]);
			}
			auto pattern = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
			if (!pattern) {
				return BuildExecutionExpressionChildFailureReason("string_like_pattern_unsupported", 1,
				                                                  *function.GetChildren()[1]);
			}
			if (pattern->kind != ExecutionExpressionIRKind::CONSTANT) {
				return BuildExecutionExpressionReasonWithSubject("string_like_pattern_not_constant", expression);
			}
			if (!pattern->constant.IsNull() &&
			    !ExecutionExpressionLikePatternIsPercentOnly(StringValue::Get(pattern->constant))) {
				return BuildExecutionExpressionReasonWithSubject("string_like_pattern_has_unsupported_wildcard",
				                                                 expression);
			}
			break;
		}
		if (ExecutionExpressionIsStringPrefixFunction(function) ||
		    ExecutionExpressionIsStringSuffixFunction(function) ||
		    ExecutionExpressionIsStringContainsFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildExecutionExpressionReasonWithSubject("string_match_needs_two_children", expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildExecutionExpressionReasonWithSubject("string_match_return_not_boolean", expression);
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				if (child_expr.GetReturnType().id() != LogicalTypeId::VARCHAR) {
					auto result =
					    BuildExecutionExpressionReasonWithSubject("string_match_child_not_varchar", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child_expr.GetReturnType().ToString();
					return result;
				}
				auto child = TryBuildExecutionExpressionIR(child_expr);
				if (!child) {
					return BuildExecutionExpressionChildFailureReason("string_match_child_unsupported", child_idx,
					                                                  child_expr);
				}
			}
			break;
		}
		if (ExecutionExpressionIsStringSubstringFunction(function)) {
			if (function.GetChildren().size() != 2 && function.GetChildren().size() != 3) {
				return BuildExecutionExpressionReasonWithSubject("string_substring_needs_two_or_three_children",
				                                                 expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				return BuildExecutionExpressionReasonWithSubject("string_substring_return_not_varchar", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
				auto result =
				    BuildExecutionExpressionReasonWithSubject("string_substring_source_not_varchar", expression);
				result += ";source_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			for (idx_t child_idx = 1; child_idx < function.GetChildren().size(); child_idx++) {
				if (!function.GetChildren()[child_idx]->GetReturnType().IsIntegral()) {
					auto result =
					    BuildExecutionExpressionReasonWithSubject("string_substring_bound_not_integral", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + function.GetChildren()[child_idx]->GetReturnType().ToString();
					return result;
				}
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				auto child = TryBuildExecutionExpressionIR(child_expr);
				if (!child) {
					return BuildExecutionExpressionChildFailureReason("string_substring_child_unsupported", child_idx,
					                                                  child_expr);
				}
			}
			break;
		}
		if (ExecutionExpressionIsIntegralCompressFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildExecutionExpressionReasonWithSubject("integral_compress_needs_two_children", expression);
			}
			if (!ExecutionExpressionIsUnsignedIntegerCompressionType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("integral_compress_return_not_unsigned_integer",
				                                                 expression);
			}
			auto &source_expr = *function.GetChildren()[0];
			auto &minimum_expr = *function.GetChildren()[1];
			if (!ExecutionExpressionIsCompressibleIntegralType(source_expr.GetReturnType())) {
				auto result =
				    BuildExecutionExpressionReasonWithSubject("integral_compress_source_not_integral", expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				return result;
			}
			if (source_expr.GetReturnType() != minimum_expr.GetReturnType()) {
				auto result =
				    BuildExecutionExpressionReasonWithSubject("integral_compress_minimum_type_mismatch", expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				result += ";minimum_type=" + minimum_expr.GetReturnType().ToString();
				return result;
			}
			auto source = TryBuildExecutionExpressionIR(source_expr);
			if (!source) {
				return BuildExecutionExpressionChildFailureReason("integral_compress_source_unsupported", 0,
				                                                  source_expr);
			}
			auto minimum = TryBuildExecutionExpressionIR(minimum_expr);
			if (!minimum) {
				return BuildExecutionExpressionChildFailureReason("integral_compress_minimum_unsupported", 1,
				                                                  minimum_expr);
			}
			if (minimum->kind != ExecutionExpressionIRKind::CONSTANT || minimum->constant.IsNull()) {
				return BuildExecutionExpressionReasonWithSubject("integral_compress_minimum_not_constant", expression);
			}
			break;
		}
		if (ExecutionExpressionIsIntegralDecompressFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildExecutionExpressionReasonWithSubject("integral_decompress_needs_two_children", expression);
			}
			if (!ExecutionExpressionIsCompressibleIntegralType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("integral_decompress_return_not_integral", expression);
			}
			auto &source_expr = *function.GetChildren()[0];
			auto &minimum_expr = *function.GetChildren()[1];
			if (!ExecutionExpressionIsUnsignedIntegerCompressionType(source_expr.GetReturnType())) {
				auto result = BuildExecutionExpressionReasonWithSubject(
				    "integral_decompress_source_not_unsigned_integer", expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				return result;
			}
			if (minimum_expr.GetReturnType() != expression.GetReturnType()) {
				auto result =
				    BuildExecutionExpressionReasonWithSubject("integral_decompress_minimum_type_mismatch", expression);
				result += ";return_type=" + expression.GetReturnType().ToString();
				result += ";minimum_type=" + minimum_expr.GetReturnType().ToString();
				return result;
			}
			auto source = TryBuildExecutionExpressionIR(source_expr);
			if (!source) {
				return BuildExecutionExpressionChildFailureReason("integral_decompress_source_unsupported", 0,
				                                                  source_expr);
			}
			auto minimum = TryBuildExecutionExpressionIR(minimum_expr);
			if (!minimum) {
				return BuildExecutionExpressionChildFailureReason("integral_decompress_minimum_unsupported", 1,
				                                                  minimum_expr);
			}
			if (minimum->kind != ExecutionExpressionIRKind::CONSTANT || minimum->constant.IsNull()) {
				return BuildExecutionExpressionReasonWithSubject("integral_decompress_minimum_not_constant",
				                                                 expression);
			}
			break;
		}
		if (ExecutionExpressionIsStringCompressFunction(function)) {
			if (function.GetChildren().size() != 1) {
				return BuildExecutionExpressionReasonWithSubject("string_compress_needs_one_child", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
				auto result =
				    BuildExecutionExpressionReasonWithSubject("string_compress_child_not_varchar", expression);
				result += ";child_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			if (!ExecutionExpressionIsUnsignedIntegerCompressionType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("string_compress_return_not_unsigned_integer",
				                                                 expression);
			}
			auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildExecutionExpressionChildFailureReason("string_compress_child_unsupported", 0,
				                                                  *function.GetChildren()[0]);
			}
			break;
		}
		if (function_name == "year") {
			if (function.GetChildren().size() != 1) {
				return BuildExecutionExpressionReasonWithSubject("date_year_needs_one_child", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::DATE) {
				auto result = BuildExecutionExpressionReasonWithSubject("date_year_child_not_date", expression);
				result += ";child_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BIGINT) {
				return BuildExecutionExpressionReasonWithSubject("date_year_return_not_bigint", expression);
			}
			auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildExecutionExpressionChildFailureReason("date_year_child_unsupported", 0,
				                                                  *function.GetChildren()[0]);
			}
			break;
		}
		if (function_name == "constant_or_null") {
			if (function.GetChildren().size() < 2) {
				return BuildExecutionExpressionReasonWithSubject("constant_or_null_needs_two_children", expression);
			}
			if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("constant_or_null_return_not_scalar", expression);
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				auto child = TryBuildExecutionExpressionIR(child_expr);
				if (!child) {
					return BuildExecutionExpressionChildFailureReason("constant_or_null_child_unsupported", child_idx,
					                                                  child_expr);
				}
				if (!ExecutionExpressionIsScalarType(child->return_type)) {
					auto result =
					    BuildExecutionExpressionReasonWithSubject("constant_or_null_child_not_scalar", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child->return_type.ToString();
					return result;
				}
			}
			break;
		}
		auto in_list = BuildExecutionExpressionInListFailureReason(
		    "in_list", expression.GetExpressionType(), expression.GetReturnType(), function.GetChildren(), expression);
		if (!in_list.empty()) {
			return in_list;
		}
		auto between =
		    BuildExecutionExpressionBetweenFailureReason(expression.GetExpressionType(), function, expression);
		if (!between.empty()) {
			return between;
		}
		if (function.IsOperator() && function.GetChildren().size() == 1 && function_name == "-") {
			if (!ExecutionExpressionIsNegatableType(expression.GetReturnType())) {
				return BuildExecutionExpressionReasonWithSubject("negate_return_not_supported", expression);
			}
			auto child = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildExecutionExpressionChildFailureReason("negate_child_unsupported", 0,
				                                                  *function.GetChildren()[0]);
			}
			break;
		}
		ExecutionExpressionBinaryOp binary_op;
		bool is_comparison = TryGetExecutionExpressionComparisonOp(expression.GetExpressionType(), binary_op);
		bool is_arithmetic = TryGetExecutionExpressionArithmeticOp(function, binary_op);
		if (!is_comparison && !is_arithmetic) {
			return BuildExecutionExpressionReasonWithSubject("function_or_operator_unsupported", expression);
		}
		if (function.GetChildren().size() != 2) {
			return BuildExecutionExpressionReasonWithSubject("binary_function_needs_two_children", expression);
		}
		auto left = TryBuildExecutionExpressionIR(*function.GetChildren()[0]);
		auto right = TryBuildExecutionExpressionIR(*function.GetChildren()[1]);
		if (!left) {
			return BuildExecutionExpressionChildFailureReason("binary_left_unsupported", 0, *function.GetChildren()[0]);
		}
		if (!right) {
			return BuildExecutionExpressionChildFailureReason("binary_right_unsupported", 1,
			                                                  *function.GetChildren()[1]);
		}
		if (is_comparison && (!ExecutionExpressionIsComparableType(left->return_type) ||
		                      !ExecutionExpressionIsComparableType(right->return_type))) {
			auto result = BuildExecutionExpressionReasonWithSubject("comparison_child_not_comparable", expression);
			result += ";left_type=" + left->return_type.ToString();
			result += ";right_type=" + right->return_type.ToString();
			return result;
		}
		if (is_comparison && !ExecutionExpressionIsSameOrNullType(left->return_type, right->return_type)) {
			auto result = BuildExecutionExpressionReasonWithSubject("comparison_type_mismatch", expression);
			result += ";left_type=" + left->return_type.ToString();
			result += ";right_type=" + right->return_type.ToString();
			return result;
		}
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		if (!ExecutionExpressionIsScalarType(cast.source_type()) ||
		    !ExecutionExpressionIsScalarType(cast.TargetType())) {
			return BuildExecutionExpressionReasonWithSubject("cast_type_not_scalar", expression);
		}
		if (!cast.IsTryCast() && cast.CanThrow() &&
		    (!ExecutionExpressionIsSignedIntegerType(cast.source_type()) ||
		     (!ExecutionExpressionIsSignedIntegerType(cast.TargetType()) &&
		      !ExecutionExpressionIsUnsignedIntegerType(cast.TargetType())))) {
			return BuildExecutionExpressionReasonWithSubject("throwing_cast_requires_signed_source_and_integer_target",
			                                                 expression);
		}
		auto child = TryBuildExecutionExpressionIR(cast.Child());
		if (!child) {
			return BuildExecutionExpressionChildFailureReason("cast_child_unsupported", 0, cast.Child());
		}
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		    expression.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			return BuildExecutionExpressionReasonWithSubject("conjunction_type_unsupported", expression);
		}
		for (idx_t child_idx = 0; child_idx < conjunction.GetChildren().size(); child_idx++) {
			auto &child_expr = *conjunction.GetChildren()[child_idx];
			auto child = TryBuildExecutionExpressionIR(child_expr);
			if (!child) {
				return BuildExecutionExpressionChildFailureReason("conjunction_child_unsupported", child_idx,
				                                                  child_expr);
			}
			if (child->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildExecutionExpressionReasonWithSubject("conjunction_child_not_boolean", expression);
				result += ";child=" + std::to_string(child_idx);
				result += ";child_type=" + child->return_type.ToString();
				return result;
			}
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expression.Cast<BoundCaseExpression>();
		if (!ExecutionExpressionIsScalarType(expression.GetReturnType())) {
			return BuildExecutionExpressionReasonWithSubject("case_return_not_scalar", expression);
		}
		for (idx_t case_idx = 0; case_idx < case_expr.CaseChecks().size(); case_idx++) {
			auto &case_check = case_expr.CaseChecks()[case_idx];
			auto when_node = TryBuildExecutionExpressionIR(*case_check.when_expr);
			if (!when_node) {
				return BuildExecutionExpressionChildFailureReason("case_when_unsupported", case_idx,
				                                                  *case_check.when_expr);
			}
			if (when_node->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildExecutionExpressionReasonWithSubject("case_when_not_boolean", expression);
				result += ";case=" + std::to_string(case_idx);
				result += ";when_type=" + when_node->return_type.ToString();
				return result;
			}
			auto then_node = TryBuildExecutionExpressionIR(*case_check.then_expr);
			if (!then_node) {
				return BuildExecutionExpressionChildFailureReason("case_then_unsupported", case_idx,
				                                                  *case_check.then_expr);
			}
			if (!ExecutionExpressionBranchTypeMatches(*then_node, expression.GetReturnType())) {
				auto result = BuildExecutionExpressionReasonWithSubject("case_then_type_mismatch", expression);
				result += ";case=" + std::to_string(case_idx);
				result += ";then_type=" + then_node->return_type.ToString();
				return result;
			}
		}
		auto else_node = TryBuildExecutionExpressionIR(case_expr.Else());
		if (!else_node) {
			return BuildExecutionExpressionChildFailureReason("case_else_unsupported", 0, case_expr.Else());
		}
		if (!ExecutionExpressionBranchTypeMatches(*else_node, expression.GetReturnType())) {
			auto result = BuildExecutionExpressionReasonWithSubject("case_else_type_mismatch", expression);
			result += ";else_type=" + else_node->return_type.ToString();
			return result;
		}
		break;
	}
	default:
		return BuildExecutionExpressionReasonWithSubject("expression_class_unsupported", expression);
	}
	return BuildExecutionExpressionReasonWithSubject("unknown_lowering_failure", expression);
}

string DescribeExecutionExpressionLoweringFailure(const Expression &expression) {
	return "core expression lowering unsupported;" + BuildExecutionExpressionFailureReason(expression);
}

unique_ptr<ExecutionExpressionFragment>
TryLowerExecutionExpression(const Expression &expression, idx_t expression_index, ExecutionExpressionIRMode mode) {
	auto root = TryBuildExecutionExpressionIR(expression);
	if (!root) {
		return nullptr;
	}
	root = NormalizeExecutionExpressionIR(std::move(root));
	auto traits = AnnotateExecutionExpressionIR(*root);
	traits.expression_cost = DuckDBCostModel::ExpressionCost(expression);
	auto result = make_uniq<ExecutionExpressionFragment>();
	result->expression_index = expression_index;
	result->return_type = expression.GetReturnType();
	result->traits = traits;
	result->reason = GetExecutionExpressionReason(*root);
	if (mode == ExecutionExpressionIRMode::TRACE) {
		result->ir = "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*root);
	}
	result->root = std::move(root);
	return result;
}

const ExecutionExpressionFragment *ExecutionExpressionAnalysisCache::Get(const Expression &expression,
                                                                         ExecutionExpressionIRMode mode) {
	auto &entry = entries[&expression];
	if (!entry.attempted) {
		entry.fragment = TryLowerExecutionExpression(expression, 0, ExecutionExpressionIRMode::COMPACT);
		entry.attempted = true;
	}
	if (!entry.fragment) {
		return nullptr;
	}
	if (mode == ExecutionExpressionIRMode::TRACE && entry.fragment->ir.empty() && entry.fragment->root) {
		entry.fragment->ir = "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*entry.fragment->root);
	}
	return entry.fragment.get();
}

unique_ptr<ExecutionExpressionFragment> ExecutionExpressionAnalysisCache::Copy(const Expression &expression,
                                                                               idx_t expression_index,
                                                                               ExecutionExpressionIRMode mode) {
	auto fragment = Get(expression, mode);
	if (!fragment) {
		return nullptr;
	}
	auto result = make_uniq<ExecutionExpressionFragment>(*fragment);
	result->expression_index = expression_index;
	if (mode == ExecutionExpressionIRMode::COMPACT) {
		result->ir.clear();
	} else if (result->ir.empty() && result->root) {
		result->ir = "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*result->root);
	}
	return result;
}

} // namespace duckdb
