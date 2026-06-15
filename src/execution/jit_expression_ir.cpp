#include "duckdb/execution/jit/lowering.hpp"

#include "duckdb/common/string_util.hpp"
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

static bool JitIsIntegralType(const LogicalType &type) {
	return type.IsIntegral();
}

static bool JitIsSignedIntegerType(const LogicalType &type) {
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

static bool JitIsUnsignedIntegerType(const LogicalType &type) {
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

static bool JitIsCompressibleIntegralType(const LogicalType &type) {
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

static bool JitIsFloatingType(const LogicalType &type) {
	return type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE;
}

static bool JitIsArithmeticType(const LogicalType &type) {
	return JitIsIntegralType(type) || JitIsFloatingType(type) || type.id() == LogicalTypeId::DECIMAL;
}

static bool JitIsNegatableType(const LogicalType &type) {
	return JitIsArithmeticType(type) || type.id() == LogicalTypeId::DECIMAL;
}

static bool JitIsScalarType(const LogicalType &type) {
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
		return JitIsArithmeticType(type);
	}
}

static bool JitConstantValueMatchesType(const Value &value, const LogicalType &type) {
	return value.IsNull() || value.type() == type;
}

static bool JitIsComparableType(const LogicalType &type) {
	return JitIsScalarType(type);
}

static bool JitIsSameOrNullType(const LogicalType &left, const LogicalType &right) {
	if (left == right) {
		return true;
	}
	return left.id() == LogicalTypeId::SQLNULL || right.id() == LogicalTypeId::SQLNULL;
}

static bool JitIsComparisonOp(JitExpressionBinaryOp op) {
	switch (op) {
	case JitExpressionBinaryOp::COMPARE_EQUAL:
	case JitExpressionBinaryOp::COMPARE_NOTEQUAL:
	case JitExpressionBinaryOp::COMPARE_LESSTHAN:
	case JitExpressionBinaryOp::COMPARE_GREATERTHAN:
	case JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
	case JitExpressionBinaryOp::COMPARE_DISTINCT_FROM:
	case JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool TryInvertJitComparisonOp(JitExpressionBinaryOp input, JitExpressionBinaryOp &result) {
	switch (input) {
	case JitExpressionBinaryOp::COMPARE_EQUAL:
		result = JitExpressionBinaryOp::COMPARE_NOTEQUAL;
		return true;
	case JitExpressionBinaryOp::COMPARE_NOTEQUAL:
		result = JitExpressionBinaryOp::COMPARE_EQUAL;
		return true;
	case JitExpressionBinaryOp::COMPARE_LESSTHAN:
		result = JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO;
		return true;
	case JitExpressionBinaryOp::COMPARE_GREATERTHAN:
		result = JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO;
		return true;
	case JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		result = JitExpressionBinaryOp::COMPARE_GREATERTHAN;
		return true;
	case JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		result = JitExpressionBinaryOp::COMPARE_LESSTHAN;
		return true;
	case JitExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		result = JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM;
		return true;
	case JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		result = JitExpressionBinaryOp::COMPARE_DISTINCT_FROM;
		return true;
	default:
		return false;
	}
}

static bool JitIsNullConstant(const JitExpressionIR &node) {
	return node.kind == JitExpressionIRKind::CONSTANT && node.constant.IsNull();
}

static unique_ptr<JitExpressionIR> BuildJitNullCheckIR(unique_ptr<JitExpressionIR> child, JitExpressionUnaryOp op) {
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::UNARY;
	node->return_type = LogicalType::BOOLEAN;
	node->unary_op = op;
	node->left = std::move(child);
	return node;
}

static unique_ptr<JitExpressionIR> TryCanonicalizeJitNullDistinctComparison(JitExpressionBinaryOp op,
                                                                            unique_ptr<JitExpressionIR> left,
                                                                            unique_ptr<JitExpressionIR> right) {
	if (op != JitExpressionBinaryOp::COMPARE_DISTINCT_FROM && op != JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM) {
		return nullptr;
	}
	auto left_is_null = JitIsNullConstant(*left);
	auto right_is_null = JitIsNullConstant(*right);
	if (!left_is_null && !right_is_null) {
		return nullptr;
	}
	if (left_is_null && right_is_null) {
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value::BOOLEAN(op == JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM);
		return node;
	}
	auto null_check_op = op == JitExpressionBinaryOp::COMPARE_DISTINCT_FROM ? JitExpressionUnaryOp::IS_NOT_NULL
	                                                                        : JitExpressionUnaryOp::IS_NULL;
	if (left_is_null) {
		return BuildJitNullCheckIR(std::move(right), null_check_op);
	}
	return BuildJitNullCheckIR(std::move(left), null_check_op);
}

static string JitUnaryOpToString(JitExpressionUnaryOp op) {
	switch (op) {
	case JitExpressionUnaryOp::NOT:
		return "not";
	case JitExpressionUnaryOp::IS_NULL:
		return "is_null";
	case JitExpressionUnaryOp::IS_NOT_NULL:
		return "is_not_null";
	case JitExpressionUnaryOp::NEGATE:
		return "negate";
	default:
		return "unknown";
	}
}

static string JitBinaryOpToString(JitExpressionBinaryOp op) {
	switch (op) {
	case JitExpressionBinaryOp::ADD:
		return "add";
	case JitExpressionBinaryOp::SUBTRACT:
		return "subtract";
	case JitExpressionBinaryOp::MULTIPLY:
		return "multiply";
	case JitExpressionBinaryOp::DIVIDE:
		return "divide";
	case JitExpressionBinaryOp::INTEGER_DIVIDE:
		return "integer_divide";
	case JitExpressionBinaryOp::MODULO:
		return "modulo";
	case JitExpressionBinaryOp::COMPARE_EQUAL:
		return "compare_equal";
	case JitExpressionBinaryOp::COMPARE_NOTEQUAL:
		return "compare_not_equal";
	case JitExpressionBinaryOp::COMPARE_LESSTHAN:
		return "compare_less_than";
	case JitExpressionBinaryOp::COMPARE_GREATERTHAN:
		return "compare_greater_than";
	case JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return "compare_less_than_or_equal";
	case JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return "compare_greater_than_or_equal";
	case JitExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		return "compare_distinct_from";
	case JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return "compare_not_distinct_from";
	default:
		return "unknown";
	}
}

static string JitTypeDescriptor(const LogicalType &type, PhysicalType physical_type) {
	return "logical=" + type.ToString() + ",physical=" + TypeIdToString(physical_type);
}

static string JitTypeDescriptor(const LogicalType &type) {
	return JitTypeDescriptor(type, type.InternalType());
}

static string JitExpressionValidityDescriptor(const JitExpressionIR &node) {
	return "validity=" + string(JitExpressionValidityKindToString(node.validity));
}

static string JitExpressionSourceDescriptor(const JitExpressionIR &node) {
	if (node.source == JitExpressionSourceKind::VECTOR) {
		return "source=vector#" + std::to_string(node.ref_index);
	}
	return "source=" + string(JitExpressionSourceKindToString(node.source));
}

static string JitExpressionExceptionDescriptor(const JitExpressionIR &node) {
	return "exceptions=" + string(JitExpressionExceptionKindToString(node.exception_behavior));
}

static string DescribeJitExpressionIR(const JitExpressionIR &node) {
	string result = string(JitExpressionIRKindToString(node.kind)) + "<" +
	                JitTypeDescriptor(node.return_type, node.physical_type) + "," + JitExpressionValidityDescriptor(node) +
	                "," + JitExpressionSourceDescriptor(node) + "," +
	                JitExpressionExceptionDescriptor(node) + ">";
	switch (node.kind) {
	case JitExpressionIRKind::CONSTANT:
		result += "(" + node.constant.ToString() + ")";
		break;
	case JitExpressionIRKind::REFERENCE:
		result += "(#" + std::to_string(node.ref_index) + ")";
		break;
	case JitExpressionIRKind::UNARY:
		result += "." + JitUnaryOpToString(node.unary_op) + "(" + DescribeJitExpressionIR(*node.left) + ")";
		break;
	case JitExpressionIRKind::BINARY:
		result += "." + JitBinaryOpToString(node.binary_op) + "(" + DescribeJitExpressionIR(*node.left) + "," +
		          DescribeJitExpressionIR(*node.right) + ")";
		break;
	case JitExpressionIRKind::CAST:
		result += ".try=" + string(node.try_cast ? "true" : "false") + "(" + DescribeJitExpressionIR(*node.left) +
		          ")";
		break;
	case JitExpressionIRKind::CONJUNCTION:
	case JitExpressionIRKind::COALESCE:
	case JitExpressionIRKind::CONSTANT_OR_NULL:
	case JitExpressionIRKind::IN_LIST:
	case JitExpressionIRKind::BETWEEN:
		result += "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeJitExpressionIR(*node.children[i]);
		}
		result += ")";
		break;
	case JitExpressionIRKind::CASE:
		result += "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeJitExpressionIR(*node.children[i]);
		}
		result += ",else=" + DescribeJitExpressionIR(*node.else_node) + ")";
		break;
	case JitExpressionIRKind::INTRINSIC:
		result += "." + string(JitExpressionIntrinsicKindToString(node.intrinsic)) + "(";
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += DescribeJitExpressionIR(*node.children[i]);
		}
		result += ")";
		break;
	default:
		break;
	}
	return result;
}

static string GetJitExpressionReason(const JitExpressionIR &node) {
	if (node.kind == JitExpressionIRKind::CONSTANT) {
		return "core-ir:constant";
	}
	if (node.kind == JitExpressionIRKind::REFERENCE) {
		return "core-ir:reference";
	}
	if (node.kind == JitExpressionIRKind::CAST) {
		return "core-ir:cast";
	}
	if (node.kind == JitExpressionIRKind::CASE) {
		return "core-ir:case";
	}
	if (node.kind == JitExpressionIRKind::COALESCE) {
		return "core-ir:coalesce";
	}
	if (node.kind == JitExpressionIRKind::CONSTANT_OR_NULL) {
		return "core-ir:constant-or-null";
	}
	if (node.kind == JitExpressionIRKind::IN_LIST) {
		return "core-ir:in-list";
	}
	if (node.kind == JitExpressionIRKind::BETWEEN) {
		return "core-ir:between";
	}
	if (node.kind == JitExpressionIRKind::INTRINSIC) {
		return string("core-ir:intrinsic:") + JitExpressionIntrinsicKindToString(node.intrinsic);
	}
	if (node.kind == JitExpressionIRKind::CONJUNCTION ||
	    (node.kind == JitExpressionIRKind::UNARY && node.unary_op == JitExpressionUnaryOp::NOT)) {
		return "core-ir:boolean";
	}
	if (node.kind == JitExpressionIRKind::UNARY && node.unary_op == JitExpressionUnaryOp::NEGATE) {
		return "core-ir:arithmetic";
	}
	if (node.kind == JitExpressionIRKind::UNARY) {
		return "core-ir:null-check";
	}
	if (node.kind == JitExpressionIRKind::BINARY && JitIsComparisonOp(node.binary_op)) {
		return "core-ir:comparison";
	}
	return "core-ir:arithmetic";
}

static unique_ptr<JitExpressionIR> NormalizeJitExpressionIR(unique_ptr<JitExpressionIR> node) {
	if (!node) {
		return nullptr;
	}
	if (node->left) {
		node->left = NormalizeJitExpressionIR(std::move(node->left));
	}
	if (node->right) {
		node->right = NormalizeJitExpressionIR(std::move(node->right));
	}
	if (node->else_node) {
		node->else_node = NormalizeJitExpressionIR(std::move(node->else_node));
	}
	for (auto &child : node->children) {
		child = NormalizeJitExpressionIR(std::move(child));
	}
	if (node->kind == JitExpressionIRKind::CAST && !node->try_cast && node->left &&
	    node->left->kind == JitExpressionIRKind::CONSTANT) {
		Value cast_value;
		string error;
		if (node->left->constant.DefaultTryCastAs(node->return_type, cast_value, &error) &&
		    JitConstantValueMatchesType(cast_value, node->return_type)) {
			auto constant = make_uniq<JitExpressionIR>();
			constant->kind = JitExpressionIRKind::CONSTANT;
			constant->return_type = node->return_type;
			constant->constant = std::move(cast_value);
			return constant;
		}
	}
	return node;
}

static JitExpressionValidityKind GetJitExpressionValidity(const JitExpressionIR &node) {
	switch (node.kind) {
	case JitExpressionIRKind::CONSTANT:
		return node.constant.IsNull() ? JitExpressionValidityKind::CONSTANT_NULL : JitExpressionValidityKind::CONSTANT_VALID;
	case JitExpressionIRKind::REFERENCE:
		return JitExpressionValidityKind::SOURCE;
	case JitExpressionIRKind::UNARY:
		switch (node.unary_op) {
		case JitExpressionUnaryOp::IS_NULL:
		case JitExpressionUnaryOp::IS_NOT_NULL:
			return JitExpressionValidityKind::NOT_NULL;
		default:
			return JitExpressionValidityKind::CHILD;
		}
	case JitExpressionIRKind::BINARY:
		return JitExpressionValidityKind::CHILDREN_NULL_PROPAGATING;
	case JitExpressionIRKind::CAST:
		return node.try_cast ? JitExpressionValidityKind::CHILD_OR_CAST_FAILURE : JitExpressionValidityKind::CHILD;
	case JitExpressionIRKind::CONJUNCTION:
		return JitExpressionValidityKind::THREE_VALUED_BOOLEAN;
	case JitExpressionIRKind::COALESCE:
		return JitExpressionValidityKind::FIRST_VALID_CHILD;
	case JitExpressionIRKind::CONSTANT_OR_NULL:
		return JitExpressionValidityKind::CONSTANT_PLUS_NULL_GUARDS;
	case JitExpressionIRKind::IN_LIST:
		return JitExpressionValidityKind::SQL_IN_LIST;
	case JitExpressionIRKind::BETWEEN:
		return JitExpressionValidityKind::SQL_BETWEEN;
	case JitExpressionIRKind::CASE:
		return JitExpressionValidityKind::SELECTED_BRANCH;
	case JitExpressionIRKind::INTRINSIC:
		if (node.intrinsic == JitExpressionIntrinsicKind::STRING_PREFIX ||
		    node.intrinsic == JitExpressionIntrinsicKind::STRING_SUFFIX ||
		    node.intrinsic == JitExpressionIntrinsicKind::STRING_CONTAINS ||
		    node.intrinsic == JitExpressionIntrinsicKind::STRING_LIKE ||
		    node.intrinsic == JitExpressionIntrinsicKind::STRING_SUBSTRING) {
			return JitExpressionValidityKind::CHILDREN_NULL_PROPAGATING;
		}
		return JitExpressionValidityKind::CHILD;
	default:
		return JitExpressionValidityKind::UNKNOWN;
	}
}

static JitExpressionSourceKind GetJitExpressionSource(const JitExpressionIR &node) {
	switch (node.kind) {
	case JitExpressionIRKind::CONSTANT:
		return JitExpressionSourceKind::CONSTANT;
	case JitExpressionIRKind::REFERENCE:
		return JitExpressionSourceKind::VECTOR;
	default:
		return JitExpressionSourceKind::DERIVED;
	}
}

static JitExpressionExceptionKind GetJitExpressionExceptionBehavior(const JitExpressionIR &node) {
	switch (node.kind) {
	case JitExpressionIRKind::CAST:
		return node.try_cast ? JitExpressionExceptionKind::NULL_ON_CAST_ERROR : JitExpressionExceptionKind::CAST;
	case JitExpressionIRKind::BINARY:
		return JitIsComparisonOp(node.binary_op) ? JitExpressionExceptionKind::NONE
		                                         : JitExpressionExceptionKind::ARITHMETIC;
	case JitExpressionIRKind::INTRINSIC:
		return JitExpressionExceptionKind::NONE;
	case JitExpressionIRKind::UNARY:
		if (node.unary_op == JitExpressionUnaryOp::NEGATE) {
			return JitExpressionExceptionKind::ARITHMETIC;
		}
		break;
	default:
		break;
	}
	return JitExpressionExceptionKind::NONE;
}

static void AnnotateJitExpressionIR(JitExpressionIR &node) {
	if (node.left) {
		AnnotateJitExpressionIR(*node.left);
	}
	if (node.right) {
		AnnotateJitExpressionIR(*node.right);
	}
	if (node.else_node) {
		AnnotateJitExpressionIR(*node.else_node);
	}
	for (auto &child : node.children) {
		AnnotateJitExpressionIR(*child);
	}
	node.physical_type = node.return_type.InternalType();
	node.validity = GetJitExpressionValidity(node);
	node.source = GetJitExpressionSource(node);
	node.exception_behavior = GetJitExpressionExceptionBehavior(node);
}

static bool TryGetJitArithmeticOp(const BoundFunctionExpression &expression, JitExpressionBinaryOp &op) {
	if (expression.GetChildren().size() != 2) {
		return false;
	}
	auto name = expression.Function().GetName().GetIdentifierName();
	if (name == "+") {
		if (!JitIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::ADD;
		return true;
	}
	if (name == "-") {
		if (!JitIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::SUBTRACT;
		return true;
	}
	if (name == "*") {
		if (!JitIsArithmeticType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::MULTIPLY;
		return true;
	}
	if (name == "/") {
		if (!JitIsFloatingType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::DIVIDE;
		return true;
	}
	if (name == "//") {
		if (!JitIsIntegralType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::INTEGER_DIVIDE;
		return true;
	}
	if (name == "%") {
		if (!JitIsIntegralType(expression.GetReturnType())) {
			return false;
		}
		op = JitExpressionBinaryOp::MODULO;
		return true;
	}
	return false;
}

static bool TryGetJitComparisonOp(ExpressionType type, JitExpressionBinaryOp &op) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		op = JitExpressionBinaryOp::COMPARE_EQUAL;
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		op = JitExpressionBinaryOp::COMPARE_NOTEQUAL;
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		op = JitExpressionBinaryOp::COMPARE_LESSTHAN;
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		op = JitExpressionBinaryOp::COMPARE_GREATERTHAN;
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		op = JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO;
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		op = JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO;
		return true;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		op = JitExpressionBinaryOp::COMPARE_DISTINCT_FROM;
		return true;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		op = JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM;
		return true;
	default:
		return false;
	}
}

static unique_ptr<JitExpressionIR> TryBuildJitExpressionIR(const Expression &expression);

static unique_ptr<JitExpressionIR> BuildJitBooleanConstant(bool value) {
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::CONSTANT;
	node->return_type = LogicalType::BOOLEAN;
	node->constant = Value::BOOLEAN(value);
	return node;
}

static bool JitIsUnsignedIntegerCompressionType(const LogicalType &type) {
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

static bool JitIsIntegralCompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_compress_integral_");
}

static bool JitIsIntegralDecompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_decompress_integral_");
}

static bool JitIsStringCompressFunction(const BoundFunctionExpression &function) {
	return StringUtil::StartsWith(function.Function().GetName().GetIdentifierName(), "__internal_compress_string_");
}

static bool JitIsStringPrefixFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "prefix";
}

static bool JitIsStringSuffixFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "suffix";
}

static bool JitIsStringContainsFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "contains" && function.GetChildren().size() == 2 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::VARCHAR &&
	       function.GetChildren()[1]->GetReturnType().id() == LogicalTypeId::VARCHAR;
}

static bool JitIsStringLikeFunction(const BoundFunctionExpression &function) {
	auto name = function.Function().GetName().GetIdentifierName();
	return (name == "~~" || name == "!~~") && function.GetChildren().size() == 2 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::VARCHAR &&
	       function.GetChildren()[1]->GetReturnType().id() == LogicalTypeId::VARCHAR;
}

static bool JitIsStringSubstringFunction(const BoundFunctionExpression &function) {
	auto name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
	return name == "substring" || name == "substr";
}

static bool JitIsOptionalTableFilterFunction(const BoundFunctionExpression &function) {
	return function.Function().GetName().GetIdentifierName() == "__internal_tablefilter_optional";
}

static unique_ptr<JitExpressionIR> TryBuildJitOptionalTableFilterIR(const BoundFunctionExpression &function,
                                                                    const LogicalType &return_type) {
	if (!JitIsOptionalTableFilterFunction(function) || function.GetChildren().size() != 1 ||
	    return_type.id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	return BuildJitBooleanConstant(true);
}

static unique_ptr<JitExpressionIR> BuildJitStringMatchIR(JitExpressionIntrinsicKind intrinsic,
                                                         unique_ptr<JitExpressionIR> source,
                                                         unique_ptr<JitExpressionIR> pattern,
                                                         const LogicalType &return_type) {
	if (pattern->kind == JitExpressionIRKind::CONSTANT && pattern->constant.IsNull()) {
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value(LogicalType::BOOLEAN);
		return node;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = intrinsic;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(pattern));
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitStringMatchIR(const BoundFunctionExpression &function,
                                                            const LogicalType &return_type,
                                                            JitExpressionIntrinsicKind intrinsic) {
	if (function.GetChildren().size() != 2 || return_type.id() != LogicalTypeId::BOOLEAN ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    function.GetChildren()[1]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	auto source = TryBuildJitExpressionIR(*function.GetChildren()[0]);
	auto pattern = TryBuildJitExpressionIR(*function.GetChildren()[1]);
	if (!source || !pattern || source->return_type.id() != LogicalTypeId::VARCHAR ||
	    pattern->return_type.id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	return BuildJitStringMatchIR(intrinsic, std::move(source), std::move(pattern), return_type);
}

static bool JitLikePatternIsPercentOnly(const string &pattern) {
	for (auto character : pattern) {
		if (character == '_') {
			return false;
		}
	}
	return true;
}

static unique_ptr<JitExpressionIR> TryBuildJitStringLikeIR(const BoundFunctionExpression &function,
                                                           const LogicalType &return_type) {
	if (!JitIsStringLikeFunction(function) || return_type.id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	auto source = TryBuildJitExpressionIR(*function.GetChildren()[0]);
	auto pattern = TryBuildJitExpressionIR(*function.GetChildren()[1]);
	if (!source || !pattern || source->return_type.id() != LogicalTypeId::VARCHAR ||
	    pattern->return_type.id() != LogicalTypeId::VARCHAR || pattern->kind != JitExpressionIRKind::CONSTANT) {
		return nullptr;
	}
	if (pattern->constant.IsNull()) {
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CONSTANT;
		node->return_type = LogicalType::BOOLEAN;
		node->constant = Value(LogicalType::BOOLEAN);
		return node;
	}
	if (!JitLikePatternIsPercentOnly(StringValue::Get(pattern->constant))) {
		return nullptr;
	}
	auto like = BuildJitStringMatchIR(JitExpressionIntrinsicKind::STRING_LIKE, std::move(source), std::move(pattern),
	                                 return_type);
	if (function.Function().GetName().GetIdentifierName() == "~~") {
		return like;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::UNARY;
	node->return_type = return_type;
	node->unary_op = JitExpressionUnaryOp::NOT;
	node->left = std::move(like);
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitStringSubstringIR(const BoundFunctionExpression &function,
                                                                const LogicalType &return_type) {
	if (!JitIsStringSubstringFunction(function) || return_type.id() != LogicalTypeId::VARCHAR ||
	    (function.GetChildren().size() != 2 && function.GetChildren().size() != 3) ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    !function.GetChildren()[1]->GetReturnType().IsIntegral()) {
		return nullptr;
	}
	if (function.GetChildren().size() == 3 && !function.GetChildren()[2]->GetReturnType().IsIntegral()) {
		return nullptr;
	}
	auto source = TryBuildJitExpressionIR(*function.GetChildren()[0]);
	auto offset = TryBuildJitExpressionIR(*function.GetChildren()[1]);
	if (!source || !offset || source->return_type.id() != LogicalTypeId::VARCHAR ||
	    !offset->return_type.IsIntegral()) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = JitExpressionIntrinsicKind::STRING_SUBSTRING;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(offset));
	if (function.GetChildren().size() == 3) {
		auto length = TryBuildJitExpressionIR(*function.GetChildren()[2]);
		if (!length || !length->return_type.IsIntegral()) {
			return nullptr;
		}
		node->children.push_back(std::move(length));
	}
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitIntegralCompressIR(const BoundFunctionExpression &function,
                                                                 const LogicalType &return_type) {
	if (!JitIsIntegralCompressFunction(function) || function.GetChildren().size() != 2 ||
	    !JitIsUnsignedIntegerCompressionType(return_type)) {
		return nullptr;
	}
	auto &source_expr = *function.GetChildren()[0];
	auto &minimum_expr = *function.GetChildren()[1];
	if (!JitIsCompressibleIntegralType(source_expr.GetReturnType()) ||
	    source_expr.GetReturnType() != minimum_expr.GetReturnType()) {
		return nullptr;
	}
	auto source = TryBuildJitExpressionIR(source_expr);
	auto minimum = TryBuildJitExpressionIR(minimum_expr);
	if (!source || !minimum || minimum->kind != JitExpressionIRKind::CONSTANT || minimum->constant.IsNull() ||
	    source->return_type != minimum->return_type) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = JitExpressionIntrinsicKind::INTEGRAL_COMPRESS;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(minimum));
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitIntegralDecompressIR(const BoundFunctionExpression &function,
                                                                   const LogicalType &return_type) {
	if (!JitIsIntegralDecompressFunction(function) || function.GetChildren().size() != 2 ||
	    !JitIsCompressibleIntegralType(return_type)) {
		return nullptr;
	}
	auto &source_expr = *function.GetChildren()[0];
	auto &minimum_expr = *function.GetChildren()[1];
	if (!JitIsUnsignedIntegerCompressionType(source_expr.GetReturnType()) || minimum_expr.GetReturnType() != return_type) {
		return nullptr;
	}
	auto source = TryBuildJitExpressionIR(source_expr);
	auto minimum = TryBuildJitExpressionIR(minimum_expr);
	if (!source || !minimum || minimum->kind != JitExpressionIRKind::CONSTANT || minimum->constant.IsNull() ||
	    minimum->return_type != return_type) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = JitExpressionIntrinsicKind::INTEGRAL_DECOMPRESS;
	node->children.push_back(std::move(source));
	node->children.push_back(std::move(minimum));
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitStringCompressIR(const BoundFunctionExpression &function,
                                                               const LogicalType &return_type) {
	if (!JitIsStringCompressFunction(function) || function.GetChildren().size() != 1 ||
	    function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR ||
	    !JitIsUnsignedIntegerCompressionType(return_type)) {
		return nullptr;
	}
	auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
	if (!child || child->return_type.id() != LogicalTypeId::VARCHAR) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = JitExpressionIntrinsicKind::STRING_COMPRESS;
	node->children.push_back(std::move(child));
	return node;
}

static bool JitIsDateYearFunction(const BoundFunctionExpression &function, const LogicalType &return_type) {
	return function.Function().GetName().GetIdentifierName() == "year" && function.GetChildren().size() == 1 &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::DATE &&
	       return_type.id() == LogicalTypeId::BIGINT;
}

static unique_ptr<JitExpressionIR> TryBuildJitDateYearIR(const BoundFunctionExpression &function,
                                                         const LogicalType &return_type) {
	if (!JitIsDateYearFunction(function, return_type)) {
		return nullptr;
	}
	auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
	if (!child || child->return_type.id() != LogicalTypeId::DATE) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::INTRINSIC;
	node->return_type = return_type;
	node->intrinsic = JitExpressionIntrinsicKind::DATE_YEAR;
	node->children.push_back(std::move(child));
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitInListIR(ExpressionType type, const LogicalType &return_type,
                                                       const vector<unique_ptr<Expression>> &children) {
	if (type != ExpressionType::COMPARE_IN && type != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	if (children.size() < 2) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::IN_LIST;
	node->return_type = return_type;
	node->not_in = type == ExpressionType::COMPARE_NOT_IN;
	for (auto &child_expr : children) {
		auto child = TryBuildJitExpressionIR(*child_expr);
		if (!child || !JitIsComparableType(child->return_type)) {
			return nullptr;
		}
		if (!node->children.empty() && !JitIsSameOrNullType(node->children[0]->return_type, child->return_type)) {
			return nullptr;
		}
		node->children.push_back(std::move(child));
	}
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitBetweenIR(ExpressionType type,
                                                        const BoundFunctionExpression &expression) {
	if (type != ExpressionType::COMPARE_BETWEEN && type != ExpressionType::COMPARE_NOT_BETWEEN) {
		return nullptr;
	}
	if (expression.GetChildren().size() != 3 || expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return nullptr;
	}
	auto node = make_uniq<JitExpressionIR>();
	node->kind = JitExpressionIRKind::BETWEEN;
	node->return_type = expression.GetReturnType();
	node->not_between = type == ExpressionType::COMPARE_NOT_BETWEEN;
	node->lower_inclusive = BoundBetweenExpression::LowerInclusive(expression);
	node->upper_inclusive = BoundBetweenExpression::UpperInclusive(expression);
	for (auto &child_expr : expression.GetChildren()) {
		auto child = TryBuildJitExpressionIR(*child_expr);
		if (!child || !JitIsComparableType(child->return_type)) {
			return nullptr;
		}
		if (!node->children.empty() && !JitIsSameOrNullType(node->children[0]->return_type, child->return_type)) {
			return nullptr;
		}
		node->children.push_back(std::move(child));
	}
	return node;
}

static unique_ptr<JitExpressionIR> TryBuildJitExpressionIR(const Expression &expression) {
	if (expression.HasParameter()) {
		return nullptr;
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT: {
		if (!JitIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto &constant = expression.Cast<BoundConstantExpression>();
		if (!JitConstantValueMatchesType(constant.GetValue(), expression.GetReturnType())) {
			return nullptr;
		}
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CONSTANT;
		node->return_type = expression.GetReturnType();
		node->constant = constant.GetValue();
		return node;
	}
	case ExpressionClass::BOUND_REF: {
		if (!JitIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto &ref = expression.Cast<BoundReferenceExpression>();
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::REFERENCE;
		node->return_type = expression.GetReturnType();
		node->ref_index = ref.Index();
		return node;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expression.Cast<BoundOperatorExpression>();
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_COALESCE: {
			if (!JitIsScalarType(expression.GetReturnType())) {
				return nullptr;
			}
			auto node = make_uniq<JitExpressionIR>();
			node->kind = JitExpressionIRKind::COALESCE;
			node->return_type = expression.GetReturnType();
			for (auto &child_expr : op.GetChildren()) {
				auto child = TryBuildJitExpressionIR(*child_expr);
				if (!child || child->return_type != expression.GetReturnType()) {
					return nullptr;
				}
				node->children.push_back(std::move(child));
			}
			return node;
		}
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return TryBuildJitInListIR(expression.GetExpressionType(), expression.GetReturnType(), op.GetChildren());
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			if (op.GetChildren().size() != 1) {
				return nullptr;
			}
			auto child = TryBuildJitExpressionIR(*op.GetChildren()[0]);
			if (!child) {
				return nullptr;
			}
			auto node = make_uniq<JitExpressionIR>();
			node->kind = JitExpressionIRKind::UNARY;
			node->return_type = expression.GetReturnType();
			switch (expression.GetExpressionType()) {
				case ExpressionType::OPERATOR_NOT:
					if (child->return_type.id() != LogicalTypeId::BOOLEAN) {
						return nullptr;
					}
					if (child->kind == JitExpressionIRKind::UNARY && child->unary_op == JitExpressionUnaryOp::NOT) {
						return std::move(child->left);
					}
					if (child->kind == JitExpressionIRKind::UNARY &&
					    child->unary_op == JitExpressionUnaryOp::IS_NULL) {
						child->unary_op = JitExpressionUnaryOp::IS_NOT_NULL;
						return child;
					}
					if (child->kind == JitExpressionIRKind::UNARY &&
					    child->unary_op == JitExpressionUnaryOp::IS_NOT_NULL) {
						child->unary_op = JitExpressionUnaryOp::IS_NULL;
						return child;
					}
					if (child->kind == JitExpressionIRKind::BINARY &&
					    TryInvertJitComparisonOp(child->binary_op, child->binary_op)) {
						return child;
					}
					node->unary_op = JitExpressionUnaryOp::NOT;
					break;
			case ExpressionType::OPERATOR_IS_NULL:
				node->unary_op = JitExpressionUnaryOp::IS_NULL;
				break;
			case ExpressionType::OPERATOR_IS_NOT_NULL:
				node->unary_op = JitExpressionUnaryOp::IS_NOT_NULL;
				break;
			default:
				throw InternalException("Invalid JIT IR unary operator build");
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
		if (auto optional_filter = TryBuildJitOptionalTableFilterIR(function, expression.GetReturnType())) {
			return optional_filter;
		}
		if (auto intrinsic = TryBuildJitStringLikeIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (JitIsStringPrefixFunction(function)) {
			return TryBuildJitStringMatchIR(function, expression.GetReturnType(),
			                               JitExpressionIntrinsicKind::STRING_PREFIX);
		}
		if (JitIsStringSuffixFunction(function)) {
			return TryBuildJitStringMatchIR(function, expression.GetReturnType(),
			                               JitExpressionIntrinsicKind::STRING_SUFFIX);
		}
		if (JitIsStringContainsFunction(function)) {
			return TryBuildJitStringMatchIR(function, expression.GetReturnType(),
			                               JitExpressionIntrinsicKind::STRING_CONTAINS);
		}
		if (auto intrinsic = TryBuildJitStringSubstringIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildJitIntegralCompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildJitIntegralDecompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildJitStringCompressIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (auto intrinsic = TryBuildJitDateYearIR(function, expression.GetReturnType())) {
			return intrinsic;
		}
		if (function.Function().GetName().GetIdentifierName() == "constant_or_null") {
			if (function.GetChildren().size() < 2 || !JitIsScalarType(expression.GetReturnType())) {
				return nullptr;
			}
			auto node = make_uniq<JitExpressionIR>();
			node->kind = JitExpressionIRKind::CONSTANT_OR_NULL;
			node->return_type = expression.GetReturnType();
			for (auto &child_expr : function.GetChildren()) {
				auto child = TryBuildJitExpressionIR(*child_expr);
				if (!child || !JitIsScalarType(child->return_type)) {
					return nullptr;
				}
				node->children.push_back(std::move(child));
			}
			return node;
		}
		auto in_list =
		    TryBuildJitInListIR(expression.GetExpressionType(), expression.GetReturnType(), function.GetChildren());
		if (in_list) {
			return in_list;
		}
		auto between = TryBuildJitBetweenIR(expression.GetExpressionType(), function);
		if (between) {
			return between;
		}
		if (function.IsOperator() && function.GetChildren().size() == 1 &&
		    function.Function().GetName().GetIdentifierName() == "-" && JitIsNegatableType(expression.GetReturnType())) {
			auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return nullptr;
			}
			auto node = make_uniq<JitExpressionIR>();
			node->kind = JitExpressionIRKind::UNARY;
			node->return_type = expression.GetReturnType();
			node->unary_op = JitExpressionUnaryOp::NEGATE;
			node->left = std::move(child);
			return node;
		}
		JitExpressionBinaryOp op;
		bool is_comparison = TryGetJitComparisonOp(expression.GetExpressionType(), op);
		bool is_arithmetic = TryGetJitArithmeticOp(function, op);
		if (!is_comparison && !is_arithmetic) {
			return nullptr;
		}
		if (function.GetChildren().size() != 2) {
			return nullptr;
		}
		auto left = TryBuildJitExpressionIR(*function.GetChildren()[0]);
		auto right = TryBuildJitExpressionIR(*function.GetChildren()[1]);
		if (!left || !right) {
			return nullptr;
		}
		if (is_comparison &&
		    (op == JitExpressionBinaryOp::COMPARE_DISTINCT_FROM ||
		     op == JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM) &&
		    (JitIsNullConstant(*left) || JitIsNullConstant(*right))) {
			return TryCanonicalizeJitNullDistinctComparison(op, std::move(left), std::move(right));
		}
		if (is_comparison && (!JitIsComparableType(left->return_type) || !JitIsComparableType(right->return_type))) {
			return nullptr;
		}
		if (is_comparison && !JitIsSameOrNullType(left->return_type, right->return_type)) {
			return nullptr;
		}
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::BINARY;
		node->return_type = expression.GetReturnType();
		node->binary_op = op;
		node->left = std::move(left);
		node->right = std::move(right);
		return node;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		if (!JitIsScalarType(cast.source_type()) || !JitIsScalarType(cast.TargetType())) {
			return nullptr;
		}
		if (!cast.IsTryCast() && cast.CanThrow() &&
		    (!JitIsSignedIntegerType(cast.source_type()) ||
		     (!JitIsSignedIntegerType(cast.TargetType()) && !JitIsUnsignedIntegerType(cast.TargetType())))) {
			return nullptr;
		}
		auto child = TryBuildJitExpressionIR(cast.Child());
		if (!child) {
			return nullptr;
		}
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CAST;
		node->return_type = cast.TargetType();
		node->try_cast = cast.IsTryCast();
		node->left = std::move(child);
		return node;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CONJUNCTION;
		node->return_type = expression.GetReturnType();
		switch (expression.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			node->conjunction_op = JitExpressionConjunctionOp::AND;
			break;
		case ExpressionType::CONJUNCTION_OR:
			node->conjunction_op = JitExpressionConjunctionOp::OR;
			break;
		default:
			return nullptr;
		}
		for (auto &child_expr : conjunction.GetChildren()) {
			auto child = TryBuildJitExpressionIR(*child_expr);
			if (!child || child->return_type.id() != LogicalTypeId::BOOLEAN) {
				return nullptr;
			}
			node->children.push_back(std::move(child));
		}
		return node;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expression.Cast<BoundCaseExpression>();
		if (!JitIsScalarType(expression.GetReturnType())) {
			return nullptr;
		}
		auto node = make_uniq<JitExpressionIR>();
		node->kind = JitExpressionIRKind::CASE;
		node->return_type = expression.GetReturnType();
		for (auto &case_check : case_expr.CaseChecks()) {
			auto when_node = TryBuildJitExpressionIR(*case_check.when_expr);
			auto then_node = TryBuildJitExpressionIR(*case_check.then_expr);
			if (!when_node || when_node->return_type.id() != LogicalTypeId::BOOLEAN || !then_node ||
			    then_node->return_type != expression.GetReturnType()) {
				return nullptr;
			}
			node->children.push_back(std::move(when_node));
			node->children.push_back(std::move(then_node));
		}
		auto else_node = TryBuildJitExpressionIR(case_expr.Else());
		if (!else_node || else_node->return_type != expression.GetReturnType()) {
			return nullptr;
		}
		node->else_node = std::move(else_node);
		return node;
	}
	default:
		return nullptr;
	}
}

static string JitExpressionBool(bool value) {
	return value ? "true" : "false";
}

static string DescribeJitExpressionSubject(const Expression &expression) {
	string result = "class=" + ExpressionClassToString(expression.GetExpressionClass());
	result += ";type=" + ExpressionTypeToString(expression.GetExpressionType());
	result += ";return=" + expression.GetReturnType().ToString();
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expression.Cast<BoundFunctionExpression>();
		result += ";function=" + function.Function().GetName().GetIdentifierName();
		result += ";is_operator=" + JitExpressionBool(function.IsOperator());
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
		result += ";try_cast=" + JitExpressionBool(cast.IsTryCast());
		result += ";can_throw=" + JitExpressionBool(cast.CanThrow());
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

static string BuildJitExpressionFailureReason(const Expression &expression);

static string BuildJitExpressionChildFailureReason(const string &reason, idx_t child_idx, const Expression &child) {
	auto child_ir = TryBuildJitExpressionIR(child);
	if (child_ir) {
		return string();
	}
	return "reason=" + reason + ";child=" + std::to_string(child_idx) + ";child_reason=(" +
	       BuildJitExpressionFailureReason(child) + ")";
}

static string BuildJitExpressionReasonWithSubject(const string &reason, const Expression &expression) {
	return "reason=" + reason + ";" + DescribeJitExpressionSubject(expression);
}

static string BuildJitInListFailureReason(const string &reason_prefix, ExpressionType type,
                                          const LogicalType &return_type,
                                          const vector<unique_ptr<Expression>> &children,
                                          const Expression &expression) {
	if (type != ExpressionType::COMPARE_IN && type != ExpressionType::COMPARE_NOT_IN) {
		return string();
	}
	if (children.size() < 2) {
		return BuildJitExpressionReasonWithSubject(reason_prefix + "_needs_at_least_two_children", expression);
	}
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child_expr = *children[child_idx];
		auto child = TryBuildJitExpressionIR(child_expr);
		if (!child) {
			return BuildJitExpressionChildFailureReason(reason_prefix + "_child_unsupported", child_idx, child_expr);
		}
		if (!JitIsComparableType(child->return_type)) {
			auto result = BuildJitExpressionReasonWithSubject(reason_prefix + "_child_not_comparable", expression);
			result += ";child=" + std::to_string(child_idx);
			result += ";child_type=" + child->return_type.ToString();
			return result;
		}
		if (child_idx > 0) {
			auto first_child = TryBuildJitExpressionIR(*children[0]);
			D_ASSERT(first_child);
			if (!JitIsSameOrNullType(first_child->return_type, child->return_type)) {
				auto result = BuildJitExpressionReasonWithSubject(reason_prefix + "_type_mismatch", expression);
				result += ";left_type=" + first_child->return_type.ToString();
				result += ";right_type=" + child->return_type.ToString();
				result += ";child=" + std::to_string(child_idx);
				return result;
			}
		}
	}
	return BuildJitExpressionReasonWithSubject(reason_prefix + "_unknown", expression) + ";return=" +
	       return_type.ToString();
}

static string BuildJitBetweenFailureReason(ExpressionType type, const BoundFunctionExpression &function,
                                           const Expression &expression) {
	if (type != ExpressionType::COMPARE_BETWEEN && type != ExpressionType::COMPARE_NOT_BETWEEN) {
		return string();
	}
	if (function.GetChildren().size() != 3) {
		return BuildJitExpressionReasonWithSubject("between_needs_three_children", expression);
	}
	if (function.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return BuildJitExpressionReasonWithSubject("between_return_not_boolean", expression);
	}
	for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
		auto &child_expr = *function.GetChildren()[child_idx];
		auto child = TryBuildJitExpressionIR(child_expr);
		if (!child) {
			return BuildJitExpressionChildFailureReason("between_child_unsupported", child_idx, child_expr);
		}
		if (!JitIsComparableType(child->return_type)) {
			auto result = BuildJitExpressionReasonWithSubject("between_child_not_comparable", expression);
			result += ";child=" + std::to_string(child_idx);
			result += ";child_type=" + child->return_type.ToString();
			return result;
		}
		if (child_idx > 0) {
			auto first_child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			D_ASSERT(first_child);
			if (!JitIsSameOrNullType(first_child->return_type, child->return_type)) {
				auto result = BuildJitExpressionReasonWithSubject("between_type_mismatch", expression);
				result += ";left_type=" + first_child->return_type.ToString();
				result += ";right_type=" + child->return_type.ToString();
				result += ";child=" + std::to_string(child_idx);
				return result;
			}
		}
	}
	return BuildJitExpressionReasonWithSubject("between_unknown", expression);
}

static string BuildJitExpressionFailureReason(const Expression &expression) {
	if (expression.HasParameter()) {
		return BuildJitExpressionReasonWithSubject("has_parameter", expression);
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT: {
		if (!JitIsScalarType(expression.GetReturnType())) {
			return BuildJitExpressionReasonWithSubject("constant_type_not_scalar", expression);
		}
		auto &constant = expression.Cast<BoundConstantExpression>();
		if (!JitConstantValueMatchesType(constant.GetValue(), expression.GetReturnType())) {
			auto result = BuildJitExpressionReasonWithSubject("constant_value_type_mismatch", expression);
			result += ";value_type=" + constant.GetValue().type().ToString();
			return result;
		}
		break;
	}
	case ExpressionClass::BOUND_REF:
		if (!JitIsScalarType(expression.GetReturnType())) {
			return BuildJitExpressionReasonWithSubject("reference_type_not_scalar", expression);
		}
		break;
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expression.Cast<BoundOperatorExpression>();
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_COALESCE: {
			if (!JitIsScalarType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("coalesce_return_not_scalar", expression);
			}
			for (idx_t child_idx = 0; child_idx < op.GetChildren().size(); child_idx++) {
				auto &child_expr = *op.GetChildren()[child_idx];
				auto child = TryBuildJitExpressionIR(child_expr);
				if (!child) {
					return BuildJitExpressionChildFailureReason("coalesce_child_unsupported", child_idx, child_expr);
				}
				if (child->return_type != expression.GetReturnType()) {
					auto result = BuildJitExpressionReasonWithSubject("coalesce_child_type_mismatch", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child->return_type.ToString();
					return result;
				}
			}
			break;
		}
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return BuildJitInListFailureReason("in_list", expression.GetExpressionType(), expression.GetReturnType(),
			                                   op.GetChildren(), expression);
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			if (op.GetChildren().size() != 1) {
				return BuildJitExpressionReasonWithSubject("unary_operator_needs_one_child", expression);
			}
			auto child = TryBuildJitExpressionIR(*op.GetChildren()[0]);
			if (!child) {
				return BuildJitExpressionChildFailureReason("unary_child_unsupported", 0, *op.GetChildren()[0]);
			}
			if (expression.GetExpressionType() == ExpressionType::OPERATOR_NOT &&
			    child->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildJitExpressionReasonWithSubject("not_child_not_boolean", expression);
				result += ";child_type=" + child->return_type.ToString();
				return result;
			}
			break;
		}
		default:
			return BuildJitExpressionReasonWithSubject("bound_operator_type_unsupported", expression);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expression.Cast<BoundFunctionExpression>();
		auto function_name = function.Function().GetName().GetIdentifierName();
		if (JitIsOptionalTableFilterFunction(function)) {
			if (function.GetChildren().size() != 1) {
				return BuildJitExpressionReasonWithSubject("optional_table_filter_needs_one_child", expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildJitExpressionReasonWithSubject("optional_table_filter_return_not_boolean", expression);
			}
			break;
		}
		if (JitIsStringLikeFunction(function)) {
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildJitExpressionReasonWithSubject("string_like_return_not_boolean", expression);
			}
			auto source = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			if (!source) {
				return BuildJitExpressionChildFailureReason("string_like_source_unsupported", 0,
				                                            *function.GetChildren()[0]);
			}
			auto pattern = TryBuildJitExpressionIR(*function.GetChildren()[1]);
			if (!pattern) {
				return BuildJitExpressionChildFailureReason("string_like_pattern_unsupported", 1,
				                                            *function.GetChildren()[1]);
			}
			if (pattern->kind != JitExpressionIRKind::CONSTANT) {
				return BuildJitExpressionReasonWithSubject("string_like_pattern_not_constant", expression);
			}
			if (!pattern->constant.IsNull() && !JitLikePatternIsPercentOnly(StringValue::Get(pattern->constant))) {
				return BuildJitExpressionReasonWithSubject("string_like_pattern_has_unsupported_wildcard",
				                                           expression);
			}
			break;
		}
		if (JitIsStringPrefixFunction(function) || JitIsStringSuffixFunction(function) ||
		    JitIsStringContainsFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildJitExpressionReasonWithSubject("string_match_needs_two_children", expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				return BuildJitExpressionReasonWithSubject("string_match_return_not_boolean", expression);
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				if (child_expr.GetReturnType().id() != LogicalTypeId::VARCHAR) {
					auto result = BuildJitExpressionReasonWithSubject("string_match_child_not_varchar", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child_expr.GetReturnType().ToString();
					return result;
				}
				auto child = TryBuildJitExpressionIR(child_expr);
				if (!child) {
					return BuildJitExpressionChildFailureReason("string_match_child_unsupported", child_idx,
					                                            child_expr);
				}
			}
			break;
		}
		if (JitIsStringSubstringFunction(function)) {
			if (function.GetChildren().size() != 2 && function.GetChildren().size() != 3) {
				return BuildJitExpressionReasonWithSubject("string_substring_needs_two_or_three_children",
				                                           expression);
			}
			if (expression.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				return BuildJitExpressionReasonWithSubject("string_substring_return_not_varchar", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
				auto result = BuildJitExpressionReasonWithSubject("string_substring_source_not_varchar", expression);
				result += ";source_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			for (idx_t child_idx = 1; child_idx < function.GetChildren().size(); child_idx++) {
				if (!function.GetChildren()[child_idx]->GetReturnType().IsIntegral()) {
					auto result = BuildJitExpressionReasonWithSubject("string_substring_bound_not_integral",
					                                                   expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + function.GetChildren()[child_idx]->GetReturnType().ToString();
					return result;
				}
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				auto child = TryBuildJitExpressionIR(child_expr);
				if (!child) {
					return BuildJitExpressionChildFailureReason("string_substring_child_unsupported", child_idx,
					                                            child_expr);
				}
			}
			break;
		}
		if (JitIsIntegralCompressFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildJitExpressionReasonWithSubject("integral_compress_needs_two_children", expression);
			}
			if (!JitIsUnsignedIntegerCompressionType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("integral_compress_return_not_unsigned_integer",
				                                           expression);
			}
			auto &source_expr = *function.GetChildren()[0];
			auto &minimum_expr = *function.GetChildren()[1];
			if (!JitIsCompressibleIntegralType(source_expr.GetReturnType())) {
				auto result = BuildJitExpressionReasonWithSubject("integral_compress_source_not_integral", expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				return result;
			}
			if (source_expr.GetReturnType() != minimum_expr.GetReturnType()) {
				auto result = BuildJitExpressionReasonWithSubject("integral_compress_minimum_type_mismatch", expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				result += ";minimum_type=" + minimum_expr.GetReturnType().ToString();
				return result;
			}
			auto source = TryBuildJitExpressionIR(source_expr);
			if (!source) {
				return BuildJitExpressionChildFailureReason("integral_compress_source_unsupported", 0, source_expr);
			}
			auto minimum = TryBuildJitExpressionIR(minimum_expr);
			if (!minimum) {
				return BuildJitExpressionChildFailureReason("integral_compress_minimum_unsupported", 1, minimum_expr);
			}
			if (minimum->kind != JitExpressionIRKind::CONSTANT || minimum->constant.IsNull()) {
				return BuildJitExpressionReasonWithSubject("integral_compress_minimum_not_constant", expression);
			}
			break;
		}
		if (JitIsIntegralDecompressFunction(function)) {
			if (function.GetChildren().size() != 2) {
				return BuildJitExpressionReasonWithSubject("integral_decompress_needs_two_children", expression);
			}
			if (!JitIsCompressibleIntegralType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("integral_decompress_return_not_integral", expression);
			}
			auto &source_expr = *function.GetChildren()[0];
			auto &minimum_expr = *function.GetChildren()[1];
			if (!JitIsUnsignedIntegerCompressionType(source_expr.GetReturnType())) {
				auto result = BuildJitExpressionReasonWithSubject("integral_decompress_source_not_unsigned_integer",
				                                                  expression);
				result += ";source_type=" + source_expr.GetReturnType().ToString();
				return result;
			}
			if (minimum_expr.GetReturnType() != expression.GetReturnType()) {
				auto result = BuildJitExpressionReasonWithSubject("integral_decompress_minimum_type_mismatch",
				                                                  expression);
				result += ";return_type=" + expression.GetReturnType().ToString();
				result += ";minimum_type=" + minimum_expr.GetReturnType().ToString();
				return result;
			}
			auto source = TryBuildJitExpressionIR(source_expr);
			if (!source) {
				return BuildJitExpressionChildFailureReason("integral_decompress_source_unsupported", 0, source_expr);
			}
			auto minimum = TryBuildJitExpressionIR(minimum_expr);
			if (!minimum) {
				return BuildJitExpressionChildFailureReason("integral_decompress_minimum_unsupported", 1,
				                                            minimum_expr);
			}
			if (minimum->kind != JitExpressionIRKind::CONSTANT || minimum->constant.IsNull()) {
				return BuildJitExpressionReasonWithSubject("integral_decompress_minimum_not_constant", expression);
			}
			break;
		}
		if (JitIsStringCompressFunction(function)) {
			if (function.GetChildren().size() != 1) {
				return BuildJitExpressionReasonWithSubject("string_compress_needs_one_child", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
				auto result = BuildJitExpressionReasonWithSubject("string_compress_child_not_varchar", expression);
				result += ";child_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			if (!JitIsUnsignedIntegerCompressionType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("string_compress_return_not_unsigned_integer",
				                                           expression);
			}
			auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildJitExpressionChildFailureReason("string_compress_child_unsupported", 0,
				                                            *function.GetChildren()[0]);
			}
			break;
		}
		if (function_name == "year") {
			if (function.GetChildren().size() != 1) {
				return BuildJitExpressionReasonWithSubject("date_year_needs_one_child", expression);
			}
			if (function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::DATE) {
				auto result = BuildJitExpressionReasonWithSubject("date_year_child_not_date", expression);
				result += ";child_type=" + function.GetChildren()[0]->GetReturnType().ToString();
				return result;
			}
			if (expression.GetReturnType().id() != LogicalTypeId::BIGINT) {
				return BuildJitExpressionReasonWithSubject("date_year_return_not_bigint", expression);
			}
			auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildJitExpressionChildFailureReason("date_year_child_unsupported", 0,
				                                            *function.GetChildren()[0]);
			}
			break;
		}
		if (function_name == "constant_or_null") {
			if (function.GetChildren().size() < 2) {
				return BuildJitExpressionReasonWithSubject("constant_or_null_needs_two_children", expression);
			}
			if (!JitIsScalarType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("constant_or_null_return_not_scalar", expression);
			}
			for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
				auto &child_expr = *function.GetChildren()[child_idx];
				auto child = TryBuildJitExpressionIR(child_expr);
				if (!child) {
					return BuildJitExpressionChildFailureReason("constant_or_null_child_unsupported", child_idx,
					                                            child_expr);
				}
				if (!JitIsScalarType(child->return_type)) {
					auto result = BuildJitExpressionReasonWithSubject("constant_or_null_child_not_scalar", expression);
					result += ";child=" + std::to_string(child_idx);
					result += ";child_type=" + child->return_type.ToString();
					return result;
				}
			}
			break;
		}
		auto in_list = BuildJitInListFailureReason("in_list", expression.GetExpressionType(), expression.GetReturnType(),
		                                          function.GetChildren(), expression);
		if (!in_list.empty()) {
			return in_list;
		}
		auto between = BuildJitBetweenFailureReason(expression.GetExpressionType(), function, expression);
		if (!between.empty()) {
			return between;
		}
		if (function.IsOperator() && function.GetChildren().size() == 1 && function_name == "-") {
			if (!JitIsNegatableType(expression.GetReturnType())) {
				return BuildJitExpressionReasonWithSubject("negate_return_not_supported", expression);
			}
			auto child = TryBuildJitExpressionIR(*function.GetChildren()[0]);
			if (!child) {
				return BuildJitExpressionChildFailureReason("negate_child_unsupported", 0, *function.GetChildren()[0]);
			}
			break;
		}
		JitExpressionBinaryOp binary_op;
		bool is_comparison = TryGetJitComparisonOp(expression.GetExpressionType(), binary_op);
		bool is_arithmetic = TryGetJitArithmeticOp(function, binary_op);
		if (!is_comparison && !is_arithmetic) {
			return BuildJitExpressionReasonWithSubject("function_or_operator_unsupported", expression);
		}
		if (function.GetChildren().size() != 2) {
			return BuildJitExpressionReasonWithSubject("binary_function_needs_two_children", expression);
		}
		auto left = TryBuildJitExpressionIR(*function.GetChildren()[0]);
		auto right = TryBuildJitExpressionIR(*function.GetChildren()[1]);
		if (!left) {
			return BuildJitExpressionChildFailureReason("binary_left_unsupported", 0, *function.GetChildren()[0]);
		}
		if (!right) {
			return BuildJitExpressionChildFailureReason("binary_right_unsupported", 1, *function.GetChildren()[1]);
		}
		if (is_comparison && (!JitIsComparableType(left->return_type) || !JitIsComparableType(right->return_type))) {
			auto result = BuildJitExpressionReasonWithSubject("comparison_child_not_comparable", expression);
			result += ";left_type=" + left->return_type.ToString();
			result += ";right_type=" + right->return_type.ToString();
			return result;
		}
		if (is_comparison && !JitIsSameOrNullType(left->return_type, right->return_type)) {
			auto result = BuildJitExpressionReasonWithSubject("comparison_type_mismatch", expression);
			result += ";left_type=" + left->return_type.ToString();
			result += ";right_type=" + right->return_type.ToString();
			return result;
		}
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		if (!JitIsScalarType(cast.source_type()) || !JitIsScalarType(cast.TargetType())) {
			return BuildJitExpressionReasonWithSubject("cast_type_not_scalar", expression);
		}
		if (!cast.IsTryCast() && cast.CanThrow() &&
		    (!JitIsSignedIntegerType(cast.source_type()) ||
		     (!JitIsSignedIntegerType(cast.TargetType()) && !JitIsUnsignedIntegerType(cast.TargetType())))) {
			return BuildJitExpressionReasonWithSubject("throwing_cast_requires_signed_source_and_integer_target",
			                                           expression);
		}
		auto child = TryBuildJitExpressionIR(cast.Child());
		if (!child) {
			return BuildJitExpressionChildFailureReason("cast_child_unsupported", 0, cast.Child());
		}
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		    expression.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			return BuildJitExpressionReasonWithSubject("conjunction_type_unsupported", expression);
		}
		for (idx_t child_idx = 0; child_idx < conjunction.GetChildren().size(); child_idx++) {
			auto &child_expr = *conjunction.GetChildren()[child_idx];
			auto child = TryBuildJitExpressionIR(child_expr);
			if (!child) {
				return BuildJitExpressionChildFailureReason("conjunction_child_unsupported", child_idx, child_expr);
			}
			if (child->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildJitExpressionReasonWithSubject("conjunction_child_not_boolean", expression);
				result += ";child=" + std::to_string(child_idx);
				result += ";child_type=" + child->return_type.ToString();
				return result;
			}
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expression.Cast<BoundCaseExpression>();
		if (!JitIsScalarType(expression.GetReturnType())) {
			return BuildJitExpressionReasonWithSubject("case_return_not_scalar", expression);
		}
		for (idx_t case_idx = 0; case_idx < case_expr.CaseChecks().size(); case_idx++) {
			auto &case_check = case_expr.CaseChecks()[case_idx];
			auto when_node = TryBuildJitExpressionIR(*case_check.when_expr);
			if (!when_node) {
				return BuildJitExpressionChildFailureReason("case_when_unsupported", case_idx, *case_check.when_expr);
			}
			if (when_node->return_type.id() != LogicalTypeId::BOOLEAN) {
				auto result = BuildJitExpressionReasonWithSubject("case_when_not_boolean", expression);
				result += ";case=" + std::to_string(case_idx);
				result += ";when_type=" + when_node->return_type.ToString();
				return result;
			}
			auto then_node = TryBuildJitExpressionIR(*case_check.then_expr);
			if (!then_node) {
				return BuildJitExpressionChildFailureReason("case_then_unsupported", case_idx, *case_check.then_expr);
			}
			if (then_node->return_type != expression.GetReturnType()) {
				auto result = BuildJitExpressionReasonWithSubject("case_then_type_mismatch", expression);
				result += ";case=" + std::to_string(case_idx);
				result += ";then_type=" + then_node->return_type.ToString();
				return result;
			}
		}
		auto else_node = TryBuildJitExpressionIR(case_expr.Else());
		if (!else_node) {
			return BuildJitExpressionChildFailureReason("case_else_unsupported", 0, case_expr.Else());
		}
		if (else_node->return_type != expression.GetReturnType()) {
			auto result = BuildJitExpressionReasonWithSubject("case_else_type_mismatch", expression);
			result += ";else_type=" + else_node->return_type.ToString();
			return result;
		}
		break;
	}
	default:
		return BuildJitExpressionReasonWithSubject("expression_class_unsupported", expression);
	}
	return BuildJitExpressionReasonWithSubject("unknown_lowering_failure", expression);
}

string DescribeJitExpressionLoweringFailure(const Expression &expression) {
	return "core expression lowering unsupported;" + BuildJitExpressionFailureReason(expression);
}

unique_ptr<JitExpressionFragment> TryLowerJitExpression(const Expression &expression, idx_t expression_index) {
	auto root = TryBuildJitExpressionIR(expression);
	if (!root) {
		return nullptr;
	}
	root = NormalizeJitExpressionIR(std::move(root));
	AnnotateJitExpressionIR(*root);
	auto result = make_uniq<JitExpressionFragment>();
	result->expression_index = expression_index;
	result->return_type = expression.GetReturnType();
	result->reason = GetJitExpressionReason(*root);
	result->ir = "duckdb.expr typed-vector-ir;" + DescribeJitExpressionIR(*root);
	result->root = std::move(root);
	return result;
}

} // namespace duckdb
