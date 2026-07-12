//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_expression_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_expression_codegen.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_native_double_source_helpers.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static string NativeRegionIntegerBinaryOverflowMessage(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	if (kind == SljitNativeIntegerKind::DATE) {
		return "Date out of range";
	}
	if (kind != SljitNativeIntegerKind::DECIMAL64) {
		return NativeIntegerBinaryOverflowMessage(op);
	}
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "Overflow in addition of DECIMAL";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "Overflow in subtract of DECIMAL";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "Overflow in multiplication of DECIMAL";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

bool SljitCompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
                                                    string &error) {
	auto &semantic = expr.plan;
	auto build_vector = [&](auto build) {
		SljitNativeVectorFunction function = nullptr;
		auto code = build(function);
		auto compiled = SljitCompiledFunction<SljitNativeVectorFunction>::TryCreate(std::move(code), function);
		if (!compiled.IsExecutable()) {
			return false;
		}
		expr.vector = std::move(compiled);
		return true;
	};
	auto build_flat = [&](auto build) {
		SljitNativeVectorFunction function = nullptr;
		auto code = build(function);
		auto compiled = SljitCompiledFunction<SljitNativeVectorFunction>::TryCreate(std::move(code), function);
		if (!compiled.IsExecutable()) {
			return false;
		}
		expr.flat = std::move(compiled);
		return true;
	};
	auto build_select = [&](auto build) {
		SljitNativeVectorFunction function = nullptr;
		auto code = build(function);
		auto compiled = SljitCompiledFunction<SljitNativeVectorFunction>::TryCreate(std::move(code), function);
		if (!compiled.IsExecutable()) {
			return false;
		}
		expr.select = std::move(compiled);
		return true;
	};
	auto build_predicate = [&](auto build) {
		SljitNativePredicateFunction function = nullptr;
		auto code = build(function);
		auto compiled = SljitCompiledFunction<SljitNativePredicateFunction>::TryCreate(std::move(code), function);
		if (!compiled.IsExecutable()) {
			return false;
		}
		expr.predicate = std::move(compiled);
		return true;
	};
	auto build_predicate_select = [&](auto build) {
		SljitNativePredicateFunction function = nullptr;
		auto code = build(function);
		auto compiled = SljitCompiledFunction<SljitNativePredicateFunction>::TryCreate(std::move(code), function);
		if (!compiled.IsExecutable()) {
			return false;
		}
		expr.predicate_select = std::move(compiled);
		return true;
	};
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return true;
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (require_boolean) {
			error = "SLJIT constant projection cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		if (!build_vector([&](SljitNativeVectorFunction &function) {
			    return BuildSljitNativeIntegerBinaryConstant(
			        semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, function, error,
			        semantic.check_arithmetic_overflow, semantic.check_result_range, semantic.result_min,
			        semantic.result_max);
		    })) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			if (!build_flat([&](SljitNativeVectorFunction &function) {
				    return BuildSljitNativeFlatIntegerBinaryConstant(semantic.integer_kind, semantic.binary_op,
				                                                     semantic.constant_on_left, function, error);
			    })) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		if (!build_vector([&](SljitNativeVectorFunction &function) {
			    return BuildSljitNativeIntegerBinaryReferences(
			        semantic.integer_kind, semantic.binary_op, function, error, semantic.check_arithmetic_overflow,
			        semantic.check_result_range, semantic.result_min, semantic.result_max);
		    })) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			if (!build_flat([&](SljitNativeVectorFunction &function) {
				    return BuildSljitNativeFlatIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op,
				                                                       function, error);
			    })) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		if (!build_vector([&](SljitNativeVectorFunction &function) {
			    return BuildSljitNativeDoubleBinaryConstant(semantic.double_binary_op, semantic.double_source_kind,
			                                                semantic.constant_on_left, single_precision, function,
			                                                error);
		    })) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind)) {
			if (!build_flat([&](SljitNativeVectorFunction &function) {
				    return BuildSljitNativeFlatDoubleBinaryConstant(
				        semantic.double_binary_op, semantic.double_source_kind, semantic.constant_on_left,
				        single_precision, function, error);
			    })) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		if (!build_vector([&](SljitNativeVectorFunction &function) {
			    return BuildSljitNativeDoubleBinaryReferences(semantic.double_binary_op, semantic.double_source_kind,
			                                                  semantic.double_right_source_kind, single_precision,
			                                                  function, error);
		    })) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind) &&
		    IsDirectNativeFloatingSource(semantic.double_right_source_kind)) {
			if (!build_flat([&](SljitNativeVectorFunction &function) {
				    return BuildSljitNativeFlatDoubleBinaryReferences(
				        semantic.double_binary_op, semantic.double_source_kind, semantic.double_right_source_kind,
				        single_precision, function, error);
			    })) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeIntegerSelectConstant(semantic.integer_kind, semantic.compare_op,
				                                             semantic.constant_on_left, function, error);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerCompareConstant(semantic.integer_kind, semantic.compare_op,
			                                              semantic.constant_on_left, function, error);
		});
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op, function,
				                                               error);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op, function,
			                                                error);
		});
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		expr.overflow_message =
		    NativeIntegerCastOverflowMessage(semantic.cast_source_width, semantic.cast_target_width);
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerCast(semantic.cast_source_width, semantic.cast_target_width,
			                                   semantic.try_cast, function, error);
		});
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		expr.overflow_message = NativeSignedToUnsignedIntegerCastOverflowMessage(semantic.cast_source_width,
		                                                                         semantic.unsigned_cast_target_width);
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeSignedToUnsignedIntegerCast(
			    semantic.cast_source_width, semantic.unsigned_cast_target_width, semantic.try_cast, function, error);
		});
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		if (require_boolean) {
			error = "SLJIT decimal64-to-double cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		if (require_boolean) {
			error = "SLJIT decimal128 scale-up cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY:
		if (require_boolean) {
			error = "SLJIT decimal128 widening multiply cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeDecimal128WideningMultiply(semantic.cast_source_width,
			                                                  semantic.right_cast_source_width, function, error);
		});
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerCoalesce(semantic.signed_integer_width, semantic.coalesce_rhs_kind,
			                                       semantic.coalesce_constant_is_null, function, error);
		});
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeIntegerInListSelect(semantic.integer_kind, semantic.constants.size(),
				                                           semantic.list_has_null, semantic.not_in, function, error);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(),
			                                     semantic.list_has_null, semantic.not_in, function, error);
		});
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeIntegerBetweenSelect(semantic.integer_kind, semantic.lower, semantic.upper,
				                                            semantic.lower_inclusive, semantic.upper_inclusive,
				                                            semantic.not_between, function, error);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
			                                      semantic.lower_inclusive, semantic.upper_inclusive,
			                                      semantic.not_between, function, error);
		});
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		if (require_boolean) {
			error = "SLJIT constant_or_null cannot lower as a predicate";
			return false;
		}
		return build_predicate([&](SljitNativePredicateFunction &function) {
			return BuildSljitNativeConstantOrNull(semantic.constant_or_null.guard_source_indices, function, error);
		});
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		if (require_boolean) {
			error = "SLJIT string compression cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeStringCompress(semantic.string_compress_target_size, function, error);
		});
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT string decompression cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeStringDecompress(semantic.string_decompress_source_size, function, error);
		});
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
		if (require_boolean) {
			error = "SLJIT string substring cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (require_boolean) {
			error = "SLJIT integral compression cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegralCompress(semantic.cast_source_width, semantic.unsigned_cast_target_width,
			                                        function, error);
		});
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT integral decompression cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeIntegralDecompress(semantic.unsigned_source_width, semantic.cast_target_width,
			                                          function, error);
		});
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		if (require_boolean) {
			error = "SLJIT date-year cannot lower as a predicate";
			return false;
		}
		return build_vector(
		    [&](SljitNativeVectorFunction &function) { return BuildSljitNativeDateYear(function, error); });
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		if (require_boolean) {
			error = "SLJIT error-guarded reference cannot lower as a predicate";
			return false;
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeErrorGuardedReference(semantic.guarded_value_size, semantic.guard_compare_op,
			                                             semantic.guard_constant_on_left, function, error);
		});
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeNullCheckSelect(semantic.null_check_op, function, error);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeNullCheck(semantic.null_check_op, function, error);
		});
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (require_boolean) {
			return build_predicate_select([&](SljitNativePredicateFunction &function) {
				return BuildSljitNativePredicate(*semantic.predicate, false, function, error);
			});
		}
		return build_predicate([&](SljitNativePredicateFunction &function) {
			return BuildSljitNativePredicate(*semantic.predicate, true, function, error);
		});
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		if (require_boolean) {
			error = "SLJIT expression tree cannot lower as a predicate";
			return false;
		}
		if (!semantic.expression_tree) {
			error = "SLJIT expression tree plan is missing its typed expression IR";
			return false;
		}
		expr.overflow_message = "Overflow in arithmetic expression";
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeExpressionTree(*semantic.expression_tree, function, error);
		});
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		if (require_boolean && semantic.return_type.id() != LogicalTypeId::BOOLEAN) {
			error = "SLJIT typed expression tree predicate must return BOOLEAN";
			return false;
		}
		if (!semantic.expression_tree) {
			error = "SLJIT typed expression tree plan is missing its typed expression IR";
			return false;
		}
		expr.overflow_message = "Overflow in arithmetic expression";
		if (require_boolean) {
			return build_select([&](SljitNativeVectorFunction &function) {
				return BuildSljitNativeTypedExpressionTreeSelect(*semantic.expression_tree, function, error,
				                                                 semantic.emit_flat_nullable_fast_path);
			});
		}
		return build_vector([&](SljitNativeVectorFunction &function) {
			return BuildSljitNativeTypedExpressionTree(*semantic.expression_tree, semantic.integer_kind, function,
			                                           error, semantic.emit_flat_nullable_fast_path);
		});
	default:
		throw InternalException("Unknown SLJIT native region expression kind");
	}
}

bool SljitPrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                                      SljitExecutableRegionExpression &expr, string &error,
                                                      const vector<bool> *input_not_null) {
	SljitPrepareExecutableRegionExpression(plan, expr, input_not_null);
	return SljitCompilePreparedExecutableRegionExpression(expr, require_boolean, error);
}

} // namespace duckdb
