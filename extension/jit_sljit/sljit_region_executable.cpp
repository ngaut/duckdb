#include "sljit_region_executable.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_region_codegen.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static idx_t AddSljitExecutableInputSource(vector<idx_t> &input_sources, idx_t input_source_index) {
	for (idx_t source_idx = 0; source_idx < input_sources.size(); source_idx++) {
		if (input_sources[source_idx] == input_source_index) {
			return source_idx;
		}
	}
	input_sources.push_back(input_source_index);
	return input_sources.size() - 1;
}

static void RemapSljitPredicateToExecutableInputs(SljitNativePredicate &predicate, vector<idx_t> &input_sources) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return;
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
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index);
		predicate.right_source_index = AddSljitExecutableInputSource(input_sources, predicate.right_source_index);
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			source_index = AddSljitExecutableInputSource(input_sources, source_index);
		}
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources);
		}
		return;
	case SljitNativePredicateKind::NOT:
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources);
		}
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child) {
				RemapSljitPredicateToExecutableInputs(*child, input_sources);
			}
		}
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

static void SetDenseSljitPredicateSourceIndices(SljitNativePredicate &predicate, idx_t source_count) {
	predicate.source_indices.clear();
	predicate.source_indices.reserve(source_count);
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		predicate.source_indices.push_back(source_idx);
	}
}

static void RemapSljitConstantOrNullToExecutableInputs(SljitNativeConstantOrNull &constant_or_null,
                                                       vector<idx_t> &input_sources) {
	for (auto &source_index : constant_or_null.guard_source_indices) {
		source_index = AddSljitExecutableInputSource(input_sources, source_index);
	}
}

static void PrepareExecutableRegionExpressionInputs(SljitExecutableRegionExpression &expr) {
	auto &semantic = expr.plan;
	expr.input_source_indices.clear();
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (semantic.predicate) {
			RemapSljitPredicateToExecutableInputs(*semantic.predicate, expr.input_source_indices);
			SetDenseSljitPredicateSourceIndices(*semantic.predicate, expr.input_source_indices.size());
		}
		return;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		RemapSljitConstantOrNullToExecutableInputs(semantic.constant_or_null, expr.input_source_indices);
		return;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		expr.input_source_indices = semantic.expression_tree_source_indices;
		return;
	default:
		return;
	}
}

static void RemapSljitExpressionTreeToCombinedInputs(ExecutionExpressionIR &node, const vector<idx_t> &local_sources,
                                                     vector<idx_t> &combined_sources) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (node.ref_index >= local_sources.size()) {
			throw InternalException("SLJIT expression-tree reference source is out of range");
		}
		node.ref_index = AddSljitExecutableInputSource(combined_sources, local_sources[node.ref_index]);
		return;
	}
	if (node.left) {
		RemapSljitExpressionTreeToCombinedInputs(*node.left, local_sources, combined_sources);
	}
	if (node.right) {
		RemapSljitExpressionTreeToCombinedInputs(*node.right, local_sources, combined_sources);
	}
	if (node.else_node) {
		RemapSljitExpressionTreeToCombinedInputs(*node.else_node, local_sources, combined_sources);
	}
	for (auto &child : node.children) {
		if (child) {
			RemapSljitExpressionTreeToCombinedInputs(*child, local_sources, combined_sources);
		}
	}
}

static string NativeRegionIntegerBinaryOverflowMessage(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
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

static void PrepareExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                              SljitExecutableRegionExpression &expr) {
	expr.plan = CopySljitNativeRegionExpression(plan, false, false);
	PrepareExecutableRegionExpressionInputs(expr);
}

static bool CompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
                                                      string &error) {
	auto &semantic = expr.plan;
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
		expr.code = BuildSljitNativeIntegerBinaryConstant(
		    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.function, error,
		    semantic.check_result_range, semantic.result_min, semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op, expr.function,
		                                                    error, semantic.check_result_range, semantic.result_min,
		                                                    semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		expr.code = BuildSljitNativeDoubleBinaryConstant(semantic.double_binary_op, semantic.double_source_kind,
		                                                 semantic.constant_on_left, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		expr.code = BuildSljitNativeDoubleBinaryReferences(semantic.double_binary_op, semantic.double_source_kind,
		                                                   semantic.double_right_source_kind, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerSelectConstant(
			    semantic.integer_kind, semantic.compare_op, semantic.constant_on_left, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerCompareConstant(semantic.integer_kind, semantic.compare_op,
		                                                   semantic.constant_on_left, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op,
			                                                           expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code =
		    BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		expr.overflow_message =
		    NativeIntegerCastOverflowMessage(semantic.cast_source_width, semantic.cast_target_width);
		expr.code = BuildSljitNativeIntegerCast(semantic.cast_source_width, semantic.cast_target_width,
		                                        semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		expr.overflow_message = NativeSignedToUnsignedIntegerCastOverflowMessage(semantic.cast_source_width,
		                                                                         semantic.unsigned_cast_target_width);
		expr.code = BuildSljitNativeSignedToUnsignedIntegerCast(
		    semantic.cast_source_width, semantic.unsigned_cast_target_width, semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
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
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		expr.code = BuildSljitNativeIntegerCoalesce(semantic.signed_integer_width, semantic.coalesce_rhs_kind,
		                                            semantic.coalesce_constant_is_null, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerInListSelect(semantic.integer_kind, semantic.constants.size(),
			                                                       semantic.list_has_null, semantic.not_in,
			                                                       expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(),
		                                          semantic.list_has_null, semantic.not_in, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerBetweenSelect(
			    semantic.integer_kind, semantic.lower, semantic.upper, semantic.lower_inclusive,
			    semantic.upper_inclusive, semantic.not_between, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
		                                           semantic.lower_inclusive, semantic.upper_inclusive,
		                                           semantic.not_between, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		if (require_boolean) {
			error = "SLJIT constant_or_null cannot lower as a predicate";
			return false;
		}
		expr.predicate_code = BuildSljitNativeConstantOrNull(semantic.constant_or_null.guard_source_indices,
		                                                     expr.predicate_function, error);
		return expr.predicate_code != nullptr;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		if (require_boolean) {
			error = "SLJIT string compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeStringCompress(semantic.string_compress_target_size, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT string decompression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeStringDecompress(semantic.string_decompress_source_size, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (require_boolean) {
			error = "SLJIT integral compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralCompress(semantic.cast_source_width, semantic.unsigned_cast_target_width,
		                                             expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT integral decompression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralDecompress(semantic.unsigned_source_width, semantic.cast_target_width,
		                                               expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		if (require_boolean) {
			error = "SLJIT date-year cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeDateYear(expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		if (require_boolean) {
			error = "SLJIT error-guarded reference cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeErrorGuardedReference(semantic.guarded_value_size, semantic.guard_compare_op,
		                                                  semantic.guard_constant_on_left, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeNullCheckSelect(semantic.null_check_op, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeNullCheck(semantic.null_check_op, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (require_boolean) {
			expr.predicate_select_code =
			    BuildSljitNativePredicate(*semantic.predicate, false, expr.predicate_select_function, error);
			return expr.predicate_select_code != nullptr;
		}
		expr.predicate_code = BuildSljitNativePredicate(*semantic.predicate, true, expr.predicate_function, error);
		return expr.predicate_code != nullptr;
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
		expr.code = BuildSljitNativeExpressionTree(*semantic.expression_tree, expr.function, error);
		return expr.code != nullptr;
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
			expr.select_code =
			    BuildSljitNativeTypedExpressionTreeSelect(*semantic.expression_tree, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code =
		    BuildSljitNativeTypedExpressionTree(*semantic.expression_tree, semantic.integer_kind, expr.function, error);
		return expr.code != nullptr;
	default:
		throw InternalException("Unknown SLJIT native region expression kind");
	}
}

static bool PrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                        bool require_boolean, SljitExecutableRegionExpression &expr,
                                                        string &error) {
	PrepareExecutableRegionExpression(plan, expr);
	return CompilePreparedExecutableRegionExpression(expr, require_boolean, error);
}

static unique_ptr<ExecutionExpressionIR>
SljitPayloadReferenceExpressionTree(const SljitNativeRegionExpressionPlan &plan) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = plan.return_type;
	result->physical_type = plan.return_type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = 0;
	return result;
}

static bool NormalizeSljitFilteredAggregatePayloadExpression(SljitExecutableRegionExpression &payload,
                                                             vector<idx_t> &local_sources) {
	if (payload.plan.expression_tree) {
		local_sources = payload.input_source_indices.empty() ? payload.plan.expression_tree_source_indices
		                                                     : payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index == DConstants::INVALID_INDEX) {
		return false;
	}
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	local_sources.clear();
	local_sources.push_back(payload.plan.source_index);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

static bool TryBuildFilteredAggregateUpdate(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &aggregate_op,
                                            string &error) {
	if (filter_op.kind != SljitNativeRegionOpKind::FILTER ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return true;
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	if (aggregate_update.filtered_update.IsExecutable() || !aggregate_update.plan.use_primitive_payloads ||
	    aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    aggregate_update.payloads.empty() ||
	    aggregate_update.payloads.size() != aggregate_update.plan.sink_info.aggregates.size()) {
		return true;
	}
	if (filter_op.filter.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
	    !filter_op.filter.plan.expression_tree) {
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateKindCanGenerate(primitive_kind)) {
			return true;
		}
	}

	SljitExecutableFilteredAggregateUpdate filtered_update;
	filtered_update.filter.plan = CopySljitNativeRegionExpression(filter_op.filter.plan, true, false);
	filtered_update.payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		SljitExecutableRegionExpression filtered_payload;
		filtered_payload.plan = CopySljitNativeRegionExpression(payload.plan, true, false);
		filtered_update.payloads.push_back(std::move(filtered_payload));
	}
	if (!filtered_update.filter.plan.expression_tree) {
		return true;
	}

	vector<idx_t> combined_sources;
	auto &filter_sources = filter_op.filter.input_source_indices.empty()
	                           ? filter_op.filter.plan.expression_tree_source_indices
	                           : filter_op.filter.input_source_indices;
	RemapSljitExpressionTreeToCombinedInputs(*filtered_update.filter.plan.expression_tree, filter_sources,
	                                         combined_sources);
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateUsesPayloadExpression(primitive_kind)) {
			continue;
		}
		vector<idx_t> payload_sources;
		if (!NormalizeSljitFilteredAggregatePayloadExpression(filtered_update.payloads[payload_idx], payload_sources)) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*filtered_update.payloads[payload_idx].plan.expression_tree,
		                                         payload_sources, combined_sources);
	}
	filtered_update.input_source_indices = combined_sources;
	filtered_update.filter.input_source_indices = combined_sources;
	filtered_update.filter.plan.expression_tree_source_indices = combined_sources;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	codegen_payloads.reserve(filtered_update.payloads.size());
	for (auto &payload : filtered_update.payloads) {
		payload.input_source_indices = combined_sources;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(CopySljitNativeRegionExpression(payload.plan, true, false));
	}

	SljitNativeAggregateUpdateFunction function = nullptr;
	string filtered_error;
	auto code = BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
	    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
	    function, filtered_error);

	if (code && function) {
		filtered_update.code = std::move(code);
		filtered_update.function = function;
		aggregate_update.filtered_update = std::move(filtered_update);
		return true;
	}
	if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
		error = filtered_error;
		return false;
	}
	return true;
}

static void BuildExecutableAggregateUpdateMetadata(const SljitNativeAggregateUpdatePlan &op,
                                                   SljitExecutableAggregateUpdate &executable) {
	executable.plan.sink_info = op.sink_info;
	executable.plan.input_types = op.input_types;
	executable.plan.use_primitive_payloads = op.use_primitive_payloads;
	executable.plan.use_grouped_state_addresses = op.use_grouped_state_addresses;
	executable.plan.use_perfect_hash_group_lookup = op.use_perfect_hash_group_lookup;
	executable.payloads.reserve(op.payloads.size());
	for (auto &payload : op.payloads) {
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = CopySljitNativeRegionExpression(payload, true, false);
		executable.payloads.push_back(std::move(executable_payload));
	}
}

static bool BuildExecutableAggregateUpdatePayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                      SljitExecutableAggregateUpdate &executable, string &error) {
	if (op.use_primitive_payloads && !op.use_grouped_state_addresses && op.payloads.size() > 1) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(op.payloads, op.sink_info.aggregates,
		                                                                         fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	if (op.use_primitive_payloads && op.use_perfect_hash_group_lookup && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.groups, op.sink_info.aggregate_contract, fused_function,
		    fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			executable.fused_payload_update_owns_group_lookup = true;
			return true;
		}
		if (!fused_error.empty()) {
			error = fused_error;
			return false;
		}
	}
	if (op.use_primitive_payloads && op.use_grouped_state_addresses && op.payloads.size() > 1) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	executable.payload_update_code.reserve(op.payloads.size());
	executable.payload_update_functions.reserve(op.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &payload = op.payloads[payload_idx];
		if (payload_idx >= op.sink_info.aggregates.size()) {
			error = "SLJIT aggregate update payload has no aggregate contract";
			return false;
		}
		auto primitive_kind = op.sink_info.aggregates[payload_idx].primitive_update_kind;
		SljitNativeAggregateUpdateFunction function = nullptr;
		unique_ptr<ExecutionRegionCodeHandle> code;
		if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (op.use_grouped_state_addresses) {
				code = BuildSljitNativeGroupedCountStar(function, error);
			} else {
				code = BuildSljitNativeUngroupedCountStar(function, error);
			}
			if (!code || !function) {
				if (error.empty()) {
					error = "SLJIT count-star aggregate update has no native primitive reducer";
				}
				return false;
			}
			executable.payload_update_code.push_back(std::move(code));
			executable.payload_update_functions.push_back(function);
			continue;
		}
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (op.use_grouped_state_addresses) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeGroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeGroupedSumHugeintReference(payload.integer_kind, function, error);
				} else {
					error = "SLJIT grouped aggregate reference reducer has no primitive state kind";
					return false;
				}
			} else {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
					code = BuildSljitNativeUngroupedSumDoubleReference(payload.double_source_kind, function, error);
				} else {
					error = "SLJIT aggregate reference reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-constant reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
			    payload.integer_kind, payload.binary_op, payload.constant_on_left, function, error,
			    payload.check_result_range, payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-reference reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(payload.integer_kind, payload.binary_op,
			                                                                function, error, payload.check_result_range,
			                                                                payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-constant reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(
			    payload.double_binary_op, payload.double_source_kind, payload.constant_on_left, function, error);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-reference reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
			    payload.double_binary_op, payload.double_source_kind, payload.double_right_source_kind, function,
			    error);
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64ExpressionTree(*payload.expression_tree, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintExpressionTree(*payload.expression_tree, function, error);
				} else {
					error = "SLJIT aggregate expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate typed expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code =
					    BuildSljitNativeUngroupedSumInt64TypedExpressionTree(*payload.expression_tree, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(*payload.expression_tree, function,
					                                                              error);
				} else {
					error = "SLJIT aggregate typed expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		default:
			break;
		}
		if (!code || !function) {
			if (error.empty()) {
				error = "SLJIT aggregate update payload has no native primitive reducer";
			}
			return false;
		}
		executable.payload_update_code.push_back(std::move(code));
		executable.payload_update_functions.push_back(function);
	}
	return true;
}

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error, bool build_filter_code = true,
                                    bool build_aggregate_update_payload_code = true) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.output_types = op.output_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		PrepareExecutableRegionExpression(op.filter, executable.filter);
		if (!build_filter_code) {
			return true;
		}
		return CompilePreparedExecutableRegionExpression(executable.filter, true, error);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.hash_join_probe.plan = CopySljitNativeHashJoinProbePlan(op.hash_join_probe, false);
		if (op.hash_join_probe.residual_predicate &&
		    !PrepareAndCompileExecutableRegionExpression(op.hash_join_probe.residual_filter, true,
		                                                 executable.hash_join_probe.residual_filter, error)) {
			return false;
		}
		return ValidateSljitHashJoinProbe(op.hash_join_probe.keys, op.hash_join_probe.equality_key_count,
		                                  op.hash_join_probe.output_mode, error);
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.hash_join_build.plan.sink_info = op.hash_join_build.sink_info;
		executable.hash_join_build.plan.input_types = op.hash_join_build.input_types;
		return true;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		executable.nested_loop_join_probe.plan.operator_index = op.nested_loop_join_probe.operator_index;
		executable.nested_loop_join_probe.plan.input_types = op.nested_loop_join_probe.input_types;
		executable.nested_loop_join_probe.plan.condition_types = op.nested_loop_join_probe.condition_types;
		executable.nested_loop_join_probe.plan.join_type = op.nested_loop_join_probe.join_type;
		executable.nested_loop_join_probe.plan.operator_info = op.nested_loop_join_probe.operator_info;
		executable.nested_loop_join_probe.plan.conditions.reserve(op.nested_loop_join_probe.conditions.size());
		executable.nested_loop_join_probe.lhs_conditions.reserve(op.nested_loop_join_probe.conditions.size());
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
			condition_plan.type = condition.type;
			condition_plan.comparison_type = condition.comparison_type;
			condition_plan.value_kind = condition.value_kind;
			condition_plan.lhs_condition = CopySljitNativeRegionExpression(condition.lhs_condition, false, false);
			executable.nested_loop_join_probe.plan.conditions.push_back(std::move(condition_plan));

			SljitExecutableRegionExpression executable_condition;
			if (!PrepareAndCompileExecutableRegionExpression(condition.lhs_condition, false, executable_condition,
			                                                 error)) {
				return false;
			}
			executable.nested_loop_join_probe.lhs_conditions.push_back(std::move(executable_condition));
		}
		executable.nested_loop_join_probe.code = BuildSljitNestedLoopJoinProbe(
		    executable.nested_loop_join_probe.plan, executable.nested_loop_join_probe.function, error);
		return executable.nested_loop_join_probe.code != nullptr &&
		       executable.nested_loop_join_probe.function != nullptr;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		executable.nested_loop_join_build.plan.sink_info = op.nested_loop_join_build.sink_info;
		executable.nested_loop_join_build.plan.input_types = op.nested_loop_join_build.input_types;
		executable.nested_loop_join_build.plan.condition_types = op.nested_loop_join_build.condition_types;
		executable.nested_loop_join_build.rhs_conditions.reserve(op.nested_loop_join_build.rhs_conditions.size());
		for (auto &condition : op.nested_loop_join_build.rhs_conditions) {
			SljitExecutableRegionExpression executable_condition;
			if (!PrepareAndCompileExecutableRegionExpression(condition, false, executable_condition, error)) {
				return false;
			}
			executable.nested_loop_join_build.rhs_conditions.push_back(std::move(executable_condition));
		}
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		executable.order_sink.plan.sink_info = op.order_sink.sink_info;
		executable.order_sink.plan.input_types = op.order_sink.input_types;
		executable.order_sink.plan.key_types = op.order_sink.key_types;
		executable.order_sink.order_keys.reserve(op.order_sink.order_keys.size());
		for (auto &order_key : op.order_sink.order_keys) {
			SljitExecutableRegionExpression executable_order_key;
			if (!PrepareAndCompileExecutableRegionExpression(order_key, false, executable_order_key, error)) {
				return false;
			}
			executable.order_sink.order_keys.push_back(std::move(executable_order_key));
		}
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK:
		executable.append_sink.plan.sink_info = op.append_sink.sink_info;
		executable.append_sink.plan.input_types = op.append_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		if (op.delim_join_sink.sink_info.kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			error = "SLJIT delimiter join sink executable is missing delimiter sink info";
			return false;
		}
		executable.delim_join_sink.plan.sink_info = op.delim_join_sink.sink_info;
		executable.delim_join_sink.plan.input_types = op.delim_join_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		BuildExecutableAggregateUpdateMetadata(op.aggregate_update, executable.aggregate_update);
		if (!build_aggregate_update_payload_code) {
			return true;
		}
		return BuildExecutableAggregateUpdatePayloadCode(op.aggregate_update, executable.aggregate_update, error);
	case SljitNativeRegionOpKind::PROJECTION:
		executable.projections.reserve(op.projections.size());
		for (auto &projection : op.projections) {
			SljitExecutableRegionExpression executable_projection;
			if (!PrepareAndCompileExecutableRegionExpression(projection, false, executable_projection, error)) {
				return false;
			}
			executable.projections.push_back(std::move(executable_projection));
		}
		return true;
	default:
		throw InternalException("Unknown SLJIT native region operator kind");
	}
}

static bool SljitCanDeferAggregateUpdatePayloadCode(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx == 0 || op_idx + 1 != ops.size() || ops[op_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[op_idx - 1].kind == SljitNativeRegionOpKind::FILTER;
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error) {
	executable.ops.reserve(region.ops.size());
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		SljitExecutableRegionOp executable_op;
		auto defer_aggregate_payload_code = SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx);
		const auto defer_filter_code = op.kind == SljitNativeRegionOpKind::FILTER && op_idx + 1 < region.ops.size() &&
		                               SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx + 1);
		if (!BuildExecutableRegionOp(op, executable_op, error, !defer_filter_code, !defer_aggregate_payload_code)) {
			return false;
		}
		executable.ops.push_back(std::move(executable_op));
		if (defer_aggregate_payload_code) {
			auto &aggregate_update_op = executable.ops[op_idx];
			if (!TryBuildFilteredAggregateUpdate(executable.ops[op_idx - 1], aggregate_update_op, error)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable() &&
			    !CompilePreparedExecutableRegionExpression(executable.ops[op_idx - 1].filter, true, error)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable()) {
				if (!BuildExecutableAggregateUpdatePayloadCode(op.aggregate_update,
				                                               aggregate_update_op.aggregate_update, error)) {
					return false;
				}
			}
		}
	}
	return true;
}

} // namespace duckdb
