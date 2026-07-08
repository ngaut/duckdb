//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_projection_sources.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

static bool TryMapNativeProjectionSourceIndex(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                              idx_t &source_index) {
	if (source_index >= input_projection.size()) {
		return false;
	}
	auto &source = input_projection[source_index];
	if (source.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		return false;
	}
	source_index = source.source_index;
	return true;
}

static bool TryMapNativeExpressionTreeSourceIndicesThroughProjection(
    const vector<SljitNativeRegionExpressionPlan> &input_projection, SljitNativeRegionExpressionPlan &expr) {
	if (!expr.expression_tree) {
		return true;
	}
	for (auto &source_index : expr.expression_tree_source_indices) {
		if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
			return false;
		}
	}
	return true;
}

static bool
TryMapNativePredicateSourcesThroughProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                              SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return true;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		return TryMapNativeProjectionSourceIndex(input_projection, predicate.source_index);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		return TryMapNativeProjectionSourceIndex(input_projection, predicate.source_index) &&
		       TryMapNativeProjectionSourceIndex(input_projection, predicate.right_source_index);
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
				return false;
			}
		}
		return predicate.child && TryMapNativePredicateSourcesThroughProjection(input_projection, *predicate.child);
	case SljitNativePredicateKind::NOT:
		return predicate.child && TryMapNativePredicateSourcesThroughProjection(input_projection, *predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (!child || !TryMapNativePredicateSourcesThroughProjection(input_projection, *child)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeReference(idx_t source_index, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = source_index;
	return result;
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeConstant(const Value &constant, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::CONSTANT;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = constant.IsNull() ? ExecutionExpressionValidityKind::CONSTANT_NULL
	                                     : ExecutionExpressionValidityKind::CONSTANT_VALID;
	result->source = ExecutionExpressionSourceKind::CONSTANT;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->constant = constant;
	return result;
}

static Value SljitSignedIntegerValue(SljitNativeSignedIntegerWidth width, int64_t value) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return Value::TINYINT(NumericCast<int8_t>(value));
	case SljitNativeSignedIntegerWidth::INT16:
		return Value::SMALLINT(NumericCast<int16_t>(value));
	case SljitNativeSignedIntegerWidth::INT32:
		return Value::INTEGER(NumericCast<int32_t>(value));
	case SljitNativeSignedIntegerWidth::INT64:
		return Value::BIGINT(value);
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

static LogicalType SljitSignedIntegerWidthType(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return LogicalType::TINYINT;
	case SljitNativeSignedIntegerWidth::INT16:
		return LogicalType::SMALLINT;
	case SljitNativeSignedIntegerWidth::INT32:
		return LogicalType::INTEGER;
	case SljitNativeSignedIntegerWidth::INT64:
		return LogicalType::BIGINT;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

static LogicalType SljitUnsignedIntegerWidthType(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return LogicalType::UTINYINT;
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return LogicalType::USMALLINT;
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return LogicalType::UINTEGER;
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeIntrinsic(ExecutionExpressionIntrinsicKind intrinsic,
                                                                const LogicalType &return_type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::INTRINSIC;
	result->return_type = return_type;
	result->physical_type = return_type.InternalType();
	result->validity = ExecutionExpressionValidityKind::CHILD;
	result->source = ExecutionExpressionSourceKind::DERIVED;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->intrinsic = intrinsic;
	return result;
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeCast(unique_ptr<ExecutionExpressionIR> child,
                                                           const LogicalType &target_type, optional_idx query_location,
                                                           bool try_cast) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::CAST;
	result->return_type = target_type;
	result->physical_type = target_type.InternalType();
	result->validity =
	    try_cast ? ExecutionExpressionValidityKind::CHILD_OR_CAST_FAILURE : ExecutionExpressionValidityKind::CHILD;
	result->source = ExecutionExpressionSourceKind::DERIVED;
	result->exception_behavior =
	    try_cast ? ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR : ExecutionExpressionExceptionKind::CAST;
	result->query_location = query_location;
	result->try_cast = try_cast;
	result->left = std::move(child);
	return result;
}

static void ExpandSljitExpressionTreeSources(ExecutionExpressionIR &node, const vector<idx_t> &source_indices) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		D_ASSERT(node.ref_index < source_indices.size());
		node.ref_index = source_indices[node.ref_index];
		return;
	}
	if (node.left) {
		ExpandSljitExpressionTreeSources(*node.left, source_indices);
	}
	if (node.right) {
		ExpandSljitExpressionTreeSources(*node.right, source_indices);
	}
	if (node.else_node) {
		ExpandSljitExpressionTreeSources(*node.else_node, source_indices);
	}
	for (auto &child : node.children) {
		ExpandSljitExpressionTreeSources(*child, source_indices);
	}
}

unique_ptr<ExecutionExpressionIR> CopySljitExpressionPlanAsInputTree(const SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return MakeSljitTreeReference(expr.source_index, expr.return_type);
	case SljitNativeRegionExpressionKind::CONSTANT:
		return MakeSljitTreeConstant(expr.constant_value, expr.return_type);
	case SljitNativeRegionExpressionKind::INTEGER_CAST: {
		auto source_type = SljitSignedIntegerWidthType(expr.cast_source_width);
		auto target_type = SljitSignedIntegerWidthType(expr.cast_target_width);
		if (expr.return_type != target_type) {
			return nullptr;
		}
		auto child = MakeSljitTreeReference(expr.source_index, source_type);
		return MakeSljitTreeCast(std::move(child), expr.return_type, expr.query_location, expr.try_cast);
	}
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST: {
		auto source_type = SljitSignedIntegerWidthType(expr.cast_source_width);
		auto target_type = SljitUnsignedIntegerWidthType(expr.unsigned_cast_target_width);
		if (expr.return_type != target_type) {
			return nullptr;
		}
		auto child = MakeSljitTreeReference(expr.source_index, source_type);
		return MakeSljitTreeCast(std::move(child), expr.return_type, expr.query_location, expr.try_cast);
	}
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS: {
		auto source_type = SljitSignedIntegerWidthType(expr.cast_source_width);
		auto target_type = SljitUnsignedIntegerWidthType(expr.unsigned_cast_target_width);
		if (expr.return_type != target_type) {
			return nullptr;
		}
		auto child = MakeSljitTreeReference(expr.source_index, source_type);
		auto constant = MakeSljitTreeConstant(SljitSignedIntegerValue(expr.cast_source_width, expr.constant),
		                                      source_type);
		auto result = MakeSljitTreeIntrinsic(ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS, expr.return_type);
		result->children.push_back(std::move(child));
		result->children.push_back(std::move(constant));
		return result;
	}
	case SljitNativeRegionExpressionKind::DATE_YEAR: {
		if (expr.return_type.id() != LogicalTypeId::BIGINT || expr.return_type.InternalType() != PhysicalType::INT64) {
			return nullptr;
		}
		auto child = MakeSljitTreeReference(expr.source_index, LogicalType::DATE);
		auto result = MakeSljitTreeIntrinsic(ExecutionExpressionIntrinsicKind::DATE_YEAR, expr.return_type);
		result->children.push_back(std::move(child));
		return result;
	}
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING: {
		if (expr.return_type.id() != LogicalTypeId::VARCHAR) {
			return nullptr;
		}
		auto child = MakeSljitTreeReference(expr.source_index, LogicalType::VARCHAR);
		auto start = MakeSljitTreeConstant(Value::BIGINT(1), LogicalType::BIGINT);
		auto length = MakeSljitTreeConstant(Value::BIGINT(NumericCast<int64_t>(expr.string_substring_length)),
		                                    LogicalType::BIGINT);
		auto result = MakeSljitTreeIntrinsic(ExecutionExpressionIntrinsicKind::STRING_SUBSTRING, expr.return_type);
		result->children.push_back(std::move(child));
		result->children.push_back(std::move(start));
		result->children.push_back(std::move(length));
		return result;
	}
	default:
		if (expr.expression_tree) {
			auto result = expr.expression_tree->Copy();
			ExpandSljitExpressionTreeSources(*result, expr.expression_tree_source_indices);
			return result;
		}
		return nullptr;
	}
}

bool TryMapNativeProjectionExpressionSources(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                             SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (expr.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
			       TryMapNativeProjectionSourceIndex(input_projection, expr.guard_source_index) &&
			       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
			for (auto &source_index : expr.constant_or_null.guard_source_indices) {
				if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
					return false;
				}
			}
			return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
		       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
		       TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index) &&
		       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		if (!TryMapNativeProjectionSourceIndex(input_projection, expr.source_index)) {
			return false;
		}
		if (expr.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			return TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index) &&
			       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (!expr.predicate || !TryMapNativePredicateSourcesThroughProjection(input_projection, *expr.predicate)) {
			return false;
		}
		FinalizeSljitNativePredicateSourceIndices(*expr.predicate);
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	default:
		return false;
	}
}

} // namespace duckdb
