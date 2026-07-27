//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_description.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"

#include <cstring>

namespace duckdb {

static string DescribeSljitExpressionSourceIndexList(const vector<idx_t> &source_indices) {
	string result;
	for (idx_t source_idx = 0; source_idx < source_indices.size(); source_idx++) {
		if (source_idx > 0) {
			result += "|";
		}
		result += std::to_string(source_indices[source_idx]);
	}
	return result;
}

static string DescribeSljitExpressionTreeNode(const ExecutionExpressionIR &node) {
	string result;
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		return "ref#" + std::to_string(node.ref_index) + ":" + node.return_type.ToString();
	case ExecutionExpressionIRKind::CONSTANT:
		return "const:" + node.return_type.ToString();
	case ExecutionExpressionIRKind::INTRINSIC:
		result = "intrinsic" + std::to_string(static_cast<uint8_t>(node.intrinsic)) + ":" + node.return_type.ToString();
		break;
	case ExecutionExpressionIRKind::BINARY:
		result = "binary" + std::to_string(static_cast<uint8_t>(node.binary_op)) + ":" + node.return_type.ToString();
		break;
	case ExecutionExpressionIRKind::CAST:
		result = "cast:" + node.return_type.ToString();
		break;
	case ExecutionExpressionIRKind::CASE:
		result = "case:" + node.return_type.ToString();
		break;
	default:
		result = "node" + std::to_string(static_cast<uint8_t>(node.kind)) + ":" + node.return_type.ToString();
		break;
	}
	result += "(";
	bool needs_separator = false;
	auto append_child = [&](const unique_ptr<ExecutionExpressionIR> &child) {
		if (!child) {
			return;
		}
		if (needs_separator) {
			result += ",";
		}
		result += DescribeSljitExpressionTreeNode(*child);
		needs_separator = true;
	};
	append_child(node.left);
	append_child(node.right);
	for (auto &child : node.children) {
		append_child(child);
	}
	append_child(node.else_node);
	result += ")";
	return result;
}

static bool SljitExpressionHasSemanticRangeCheck(SljitNativeIntegerKind kind) {
	return kind == SljitNativeIntegerKind::DECIMAL64 || kind == SljitNativeIntegerKind::DATE;
}

class SljitSemanticExpressionKey {
public:
	void String(const char *name, const string &value) {
		result += name;
		result += "=" + std::to_string(value.size()) + ":";
		result += value;
		result += ";";
	}

	template <class T>
	void Number(const char *name, T value) {
		result += name;
		result += "=";
		result += std::to_string(value);
		result += ";";
	}

	void Bool(const char *name, bool value) {
		Number(name, value ? 1 : 0);
	}

	void Double(const char *name, double value) {
		static_assert(sizeof(value) == sizeof(uint64_t), "double semantic key requires an IEEE-754-sized value");
		uint64_t bits;
		memcpy(&bits, &value, sizeof(bits));
		Number(name, bits);
	}

	void Type(const char *name, const LogicalType &type) {
		String(name, type.ToString());
	}

	void ValueField(const char *name, const Value &value) {
		Type(name, value.type());
		String(name, value.ToSQLString());
	}

	string Take() {
		return std::move(result);
	}

private:
	string result;
};

static string BuildSljitExpressionTreeSemanticKey(const ExecutionExpressionIR &node);

static void AppendSljitExpressionTreeChild(SljitSemanticExpressionKey &key, const char *name,
                                           const unique_ptr<ExecutionExpressionIR> &child) {
	key.Bool(name, child != nullptr);
	if (child) {
		key.String(name, BuildSljitExpressionTreeSemanticKey(*child));
	}
}

static string BuildSljitExpressionTreeSemanticKey(const ExecutionExpressionIR &node) {
	SljitSemanticExpressionKey key;
	key.Number("kind", static_cast<uint8_t>(node.kind));
	key.Type("return_type", node.return_type);
	key.Number("physical_type", static_cast<uint8_t>(node.physical_type));
	key.Number("validity", static_cast<uint8_t>(node.validity));
	key.Number("source", static_cast<uint8_t>(node.source));
	key.Number("exception", static_cast<uint8_t>(node.exception_behavior));
	key.Bool("query_location", node.query_location.IsValid());
	if (node.query_location.IsValid()) {
		key.Number("query_location_value", node.query_location.GetIndex());
	}
	key.ValueField("constant", node.constant);
	key.Number("ref", node.ref_index);
	key.Number("unary", static_cast<uint8_t>(node.unary_op));
	key.Number("binary", static_cast<uint8_t>(node.binary_op));
	key.Number("conjunction", static_cast<uint8_t>(node.conjunction_op));
	key.Number("intrinsic", static_cast<uint8_t>(node.intrinsic));
	key.Bool("overflow", node.arithmetic_overflow_check);
	key.Bool("try_cast", node.try_cast);
	key.Bool("not_in", node.not_in);
	key.Bool("not_between", node.not_between);
	key.Bool("lower_inclusive", node.lower_inclusive);
	key.Bool("upper_inclusive", node.upper_inclusive);
	AppendSljitExpressionTreeChild(key, "left", node.left);
	AppendSljitExpressionTreeChild(key, "right", node.right);
	AppendSljitExpressionTreeChild(key, "else", node.else_node);
	key.Number("child_count", node.children.size());
	for (auto &child : node.children) {
		AppendSljitExpressionTreeChild(key, "child", child);
	}
	return key.Take();
}

static string BuildSljitPredicateSemanticKey(const SljitNativePredicate &predicate) {
	SljitSemanticExpressionKey key;
	key.Number("kind", static_cast<uint8_t>(predicate.kind));
	key.Type("return_type", predicate.return_type);
	key.Bool("constant_value", predicate.constant_value);
	key.Bool("constant_null", predicate.constant_is_null);
	key.Number("conjunction", static_cast<uint8_t>(predicate.conjunction_op));
	key.Number("source", predicate.source_index);
	key.Number("right_source", predicate.right_source_index);
	key.Number("constant", predicate.constant);
	key.Number("int128_lower", predicate.int128_constant_lower);
	key.Number("int128_upper", predicate.int128_constant_upper);
	key.Bool("constant_left", predicate.constant_on_left);
	key.Number("integer_kind", static_cast<uint8_t>(predicate.integer_kind));
	key.Number("double_source_kind", static_cast<uint8_t>(predicate.double_source_kind));
	key.Number("double_right_source_kind", static_cast<uint8_t>(predicate.double_right_source_kind));
	key.Double("double_constant", predicate.double_constant);
	key.Double("double_source_scale", predicate.double_source_scale);
	key.Double("double_right_source_scale", predicate.double_right_source_scale);
	key.Number("compare", static_cast<uint8_t>(predicate.compare_op));
	key.Number("null_check", static_cast<uint8_t>(predicate.null_check_op));
	key.Number("constant_count", predicate.constants.size());
	for (auto value : predicate.constants) {
		key.Number("constant_item", value);
	}
	key.Bool("list_null", predicate.list_has_null);
	key.Bool("not_in", predicate.not_in);
	key.Number("lower", predicate.lower);
	key.Number("upper", predicate.upper);
	key.Bool("lower_inclusive", predicate.lower_inclusive);
	key.Bool("upper_inclusive", predicate.upper_inclusive);
	key.Bool("not_between", predicate.not_between);
	key.String("string_constant", predicate.string_constant);
	key.Number("string_constant_count", predicate.string_constants.size());
	for (auto &value : predicate.string_constants) {
		key.String("string_constant_item", value);
	}
	key.Number("substring_length", predicate.substring_length);
	key.Bool("guard_null", predicate.guard_has_null_constant);
	for (auto value : predicate.guard_source_indices) {
		key.Number("guard_source", value);
	}
	for (auto value : predicate.source_indices) {
		key.Number("source_item", value);
	}
	for (auto value : predicate.source_not_null) {
		key.Bool("source_not_null", value);
	}
	key.Bool("has_child", predicate.child != nullptr);
	if (predicate.child) {
		key.String("child", BuildSljitPredicateSemanticKey(*predicate.child));
	}
	key.Number("child_count", predicate.children.size());
	for (auto &child : predicate.children) {
		key.Bool("child_present", child != nullptr);
		if (child) {
			key.String("child_item", BuildSljitPredicateSemanticKey(*child));
		}
	}
	return key.Take();
}

static string DescribeNativeRegionExpressionInternal(const SljitNativeRegionExpressionPlan &expr,
                                                     bool include_diagnostics) {
	string result;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		result = "native:reference";
		result += expr.references_region_input ? ":region-input" : ":projection-local";
		result += ":source=" + std::to_string(expr.source_index);
		break;
	case SljitNativeRegionExpressionKind::CONSTANT:
		result = "native:constant<" + expr.return_type.ToString() + ">(" + expr.constant_value.ToString() + ")";
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		result = NativeIntegerBinaryReason(expr.integer_kind, expr.binary_op);
		if (!expr.check_arithmetic_overflow) {
			result += ":no-overflow";
		}
		if (SljitExpressionHasSemanticRangeCheck(expr.integer_kind) && !expr.check_result_range) {
			result += ":in-range";
		}
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		result = NativeIntegerBinaryReferenceReason(expr.integer_kind, expr.binary_op);
		if (!expr.check_arithmetic_overflow) {
			result += ":no-overflow";
		}
		if (SljitExpressionHasSemanticRangeCheck(expr.integer_kind) && !expr.check_result_range) {
			result += ":in-range";
		}
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		result = NativeDoubleBinaryReason(expr.double_binary_op);
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		result = NativeDoubleBinaryReferenceReason(expr.double_binary_op);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		result = NativeIntegerCompareReason(expr.integer_kind);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		result = NativeIntegerCompareReferenceReason(expr.integer_kind);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		result = NativeIntegerCastReason(expr.cast_source_width, expr.cast_target_width, expr.try_cast);
		break;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		result = "native:signed-to-unsigned-cast:" + NativeSignedIntegerTypeName(expr.cast_source_width) + "->" +
		         NativeUnsignedIntegerTypeName(expr.unsigned_cast_target_width) +
		         (expr.try_cast ? ":try" : ":throwing");
		break;
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		result = "native:decimal64-to-double:scale=" + std::to_string(expr.double_constant);
		break;
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		result = "native:decimal128-scale-up:factor=" + std::to_string(expr.constant);
		break;
	case SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY:
		result = "native:decimal128-widening-multiply:" + NativeSignedIntegerTypeName(expr.cast_source_width) + "x" +
		         NativeSignedIntegerTypeName(expr.right_cast_source_width);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		result = NativeIntegerCoalesceReason(expr.signed_integer_width);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		result = NativeIntegerInListReason(expr.integer_kind, expr.not_in);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		result = NativeIntegerBetweenReason(expr.integer_kind, expr.not_between);
		break;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		result = "native:constant-or-null";
		break;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		result = "native:string-compress:" + std::to_string(expr.string_compress_target_size);
		break;
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		result = "native:string-decompress:" + std::to_string(expr.string_decompress_source_size);
		break;
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
		result = "native:string-substring:prefix=" + std::to_string(expr.string_substring_length);
		break;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		result = "native:integral-compress";
		break;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		result = "native:integral-decompress";
		break;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		result = "native:date-year";
		break;
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		result = "native:error-guarded-reference:" + std::to_string(expr.guarded_value_size);
		break;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		result = NativeNullCheckReason(expr.null_check_op);
		break;
	case SljitNativeRegionExpressionKind::PREDICATE:
		result = "native:boolean-predicate";
		break;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		result = "native:expression-tree:sources=" +
		         DescribeSljitExpressionSourceIndexList(expr.expression_tree_source_indices);
		if (expr.expression_tree) {
			result += ":tree=" + DescribeSljitExpressionTreeNode(*expr.expression_tree);
		}
		break;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		result = "native:typed-expression-tree:sources=" +
		         DescribeSljitExpressionSourceIndexList(expr.expression_tree_source_indices);
		if (expr.expression_tree) {
			result += ":tree=" + DescribeSljitExpressionTreeNode(*expr.expression_tree);
		}
		break;
	default:
		result = "native:unknown";
		break;
	}
	if (include_diagnostics && !expr.ir.empty()) {
		result += "[" + expr.ir + "]";
	}
	return result;
}

string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr) {
	return DescribeNativeRegionExpressionInternal(expr, true);
}

string DescribeNativeRegionExpressionSemantic(const SljitNativeRegionExpressionPlan &expr) {
	SljitSemanticExpressionKey key;
	key.Number("kind", static_cast<uint8_t>(expr.kind));
	key.Number("integer_kind", static_cast<uint8_t>(expr.integer_kind));
	key.Type("return_type", expr.return_type);
	key.ValueField("constant_value", expr.constant_value);
	key.Number("source", expr.source_index);
	key.Number("right_source", expr.right_source_index);
	key.Number("constant", expr.constant);
	key.Number("result_min", expr.result_min);
	key.Number("result_max", expr.result_max);
	key.Bool("constant_left", expr.constant_on_left);
	key.Bool("overflow", expr.check_arithmetic_overflow);
	key.Bool("result_range", expr.check_result_range);
	key.Number("binary", static_cast<uint8_t>(expr.binary_op));
	key.Number("double_binary", static_cast<uint8_t>(expr.double_binary_op));
	key.Number("double_source_kind", static_cast<uint8_t>(expr.double_source_kind));
	key.Number("double_right_source_kind", static_cast<uint8_t>(expr.double_right_source_kind));
	key.Double("double_source_scale", expr.double_source_scale);
	key.Double("double_right_source_scale", expr.double_right_source_scale);
	key.Number("compare", static_cast<uint8_t>(expr.compare_op));
	key.Number("cast_source", static_cast<uint8_t>(expr.cast_source_width));
	key.Number("right_cast_source", static_cast<uint8_t>(expr.right_cast_source_width));
	key.Number("cast_target", static_cast<uint8_t>(expr.cast_target_width));
	key.Number("unsigned_source", static_cast<uint8_t>(expr.unsigned_source_width));
	key.Number("unsigned_target", static_cast<uint8_t>(expr.unsigned_cast_target_width));
	key.Bool("query_location", expr.query_location.IsValid());
	if (expr.query_location.IsValid()) {
		key.Number("query_location_value", expr.query_location.GetIndex());
	}
	key.Number("string_compress_size", expr.string_compress_target_size);
	key.Number("string_decompress_size", expr.string_decompress_source_size);
	key.Number("substring_length", expr.string_substring_length);
	key.Number("guard_source", expr.guard_source_index);
	key.Number("guard_compare", static_cast<uint8_t>(expr.guard_compare_op));
	key.Number("guard_constant", expr.guard_constant);
	key.Bool("guard_constant_left", expr.guard_constant_on_left);
	key.Number("guarded_value_size", expr.guarded_value_size);
	key.String("error", expr.error_message);
	key.Bool("try_cast", expr.try_cast);
	key.Double("double_constant", expr.double_constant);
	key.Number("signed_width", static_cast<uint8_t>(expr.signed_integer_width));
	key.Number("coalesce_rhs", static_cast<uint8_t>(expr.coalesce_rhs_kind));
	key.Bool("coalesce_null", expr.coalesce_constant_is_null);
	key.Number("null_check", static_cast<uint8_t>(expr.null_check_op));
	for (auto value : expr.constants) {
		key.Number("constant_item", value);
	}
	key.Number("lower", expr.lower);
	key.Number("upper", expr.upper);
	key.Bool("list_null", expr.list_has_null);
	key.Bool("not_in", expr.not_in);
	key.Bool("not_between", expr.not_between);
	key.Bool("lower_inclusive", expr.lower_inclusive);
	key.Bool("upper_inclusive", expr.upper_inclusive);
	key.ValueField("constant_or_null", expr.constant_or_null.constant);
	key.Bool("constant_or_null_guard", expr.constant_or_null.guard_has_null_constant);
	for (auto value : expr.constant_or_null.guard_source_indices) {
		key.Number("constant_or_null_source", value);
	}
	key.Bool("references_region_input", expr.references_region_input);
	key.Bool("flat_nullable_fast_path", expr.emit_flat_nullable_fast_path);
	key.Bool("predicate", expr.predicate != nullptr);
	if (expr.predicate) {
		key.String("predicate_value", BuildSljitPredicateSemanticKey(*expr.predicate));
	}
	key.Bool("expression_tree", expr.expression_tree != nullptr);
	if (expr.expression_tree) {
		key.String("expression_tree_value", BuildSljitExpressionTreeSemanticKey(*expr.expression_tree));
	}
	for (auto source_index : expr.expression_tree_source_indices) {
		key.Number("expression_tree_source", source_index);
	}
	return key.Take();
}

string DescribeNativeRegionExpressionList(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	string result;
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (expr_idx > 0) {
			result += ",";
		}
		result += DescribeNativeRegionExpression(expressions[expr_idx]);
	}
	return result;
}

string DescribeNativeRegionExpressionListSemantic(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	string result;
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (expr_idx > 0) {
			result += ",";
		}
		result += DescribeNativeRegionExpressionSemantic(expressions[expr_idx]);
	}
	return result;
}

} // namespace duckdb
