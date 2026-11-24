//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_artifact_key.cpp
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include <type_traits>

namespace duckdb {

// Database-local, versioned structural identity. Diagnostic renderers never participate. Each field is its name and
// its value in a fixed width, or with its length in front, so equal keys hold equal fields in the same order.
class SljitArtifactKeyWriter {
public:
	template <class T>
	void Field(const char *name, const T &value) {
		Name(name);
		if constexpr (std::is_enum<T>::value || std::is_integral<T>::value) {
			Fixed(static_cast<int64_t>(value));
		} else if constexpr (std::is_floating_point<T>::value) {
			Fixed(static_cast<double>(value));
		} else if constexpr (std::is_same<T, string>::value || std::is_same<T, LogicalType>::value ||
		                     std::is_same<T, Value>::value) {
			Append(value);
		} else if constexpr (std::is_convertible<const T &, const char *>::value) {
			const char *text = value;
			Bytes(text, strlen(text));
		} else {
			static_assert(std::is_same<T, std::vector<bool>::const_reference>::value, "unsupported key field type");
			Fixed(static_cast<bool>(value));
		}
	}
	template <class T>
	void Field(const char *name, const vector<T> &values) {
		Field(name, values.size());
		for (auto &value : values) {
			Field("entry", value);
		}
	}
	void Field(const char *name, const vector<bool> &values) {
		Field(name, values.size());
		for (bool value : values) {
			Field("entry", value);
		}
	}
	//! A child's fields, closed by a byte no field name starts with
	template <class APPEND>
	void Nested(const char *name, APPEND &&append) {
		Name(name);
		append();
		key.push_back(char(0xFF));
	}
	string Take() {
		return std::move(key);
	}

private:
	void Name(const char *name) {
		key.append(name, strlen(name) + 1);
	}
	template <class T>
	void Fixed(T value) {
		key.append(const_char_ptr_cast(&value), sizeof(T));
	}
	void Bytes(const char *data, idx_t size) {
		Fixed(static_cast<uint64_t>(size));
		key.append(data, size);
	}
	void Append(const string &value) {
		Bytes(value.data(), value.size());
	}
	void Append(const LogicalType &type) {
		auto &info = type.GetTypeInfo();
		const bool plain = info.alias.empty() && !info.extension_info;
		if (plain && info.type == LogicalTypeInfoType::INVALID_TYPE_INFO) {
			Fixed(uint8_t(0));
			Fixed(static_cast<uint8_t>(type.id()));
		} else if (plain && info.type == LogicalTypeInfoType::DECIMAL_TYPE_INFO) {
			Fixed(uint8_t(1));
			Fixed(DecimalType::GetWidth(type));
			Fixed(DecimalType::GetScale(type));
		} else {
			Fixed(uint8_t(2));
			Serialized(type);
		}
	}
	void Append(const Value &value) {
		Append(value.type());
		Fixed(value.IsNull());
		if (!value.IsNull()) {
			Serialized(value);
		}
	}
	template <class T>
	void Serialized(const T &value) {
		MemoryStream stream;
		BinarySerializer serializer(stream);
		serializer.Begin();
		value.Serialize(serializer);
		serializer.End();
		Bytes(const_char_ptr_cast(stream.GetData()), stream.GetPosition());
	}

private:
	string key;
};

static void AppendSljitExpressionTreeSemanticKey(SljitArtifactKeyWriter &key, const ExecutionExpressionIR &node);

static void AppendSljitExpressionTreeChild(SljitArtifactKeyWriter &key, const char *name,
                                           const unique_ptr<ExecutionExpressionIR> &child) {
	key.Field(name, child != nullptr);
	if (child) {
		key.Nested(name, [&]() { AppendSljitExpressionTreeSemanticKey(key, *child); });
	}
}

static void AppendSljitExpressionTreeSemanticKey(SljitArtifactKeyWriter &key, const ExecutionExpressionIR &node) {
	key.Field("kind", static_cast<uint8_t>(node.kind));
	key.Field("return_type", node.return_type);
	key.Field("physical_type", static_cast<uint8_t>(node.physical_type));
	key.Field("validity", static_cast<uint8_t>(node.validity));
	key.Field("source", static_cast<uint8_t>(node.source));
	key.Field("exception", static_cast<uint8_t>(node.exception_behavior));
	D_ASSERT(!node.query_location.IsValid() && node.cast_source_alias.empty());
	key.Field("cast_error_binding", node.cast_error_binding);
	key.Field("constant", node.constant);
	key.Field("ref", node.ref_index);
	key.Field("unary", static_cast<uint8_t>(node.unary_op));
	key.Field("binary", static_cast<uint8_t>(node.binary_op));
	key.Field("conjunction", static_cast<uint8_t>(node.conjunction_op));
	key.Field("intrinsic", static_cast<uint8_t>(node.intrinsic));
	key.Field("overflow", node.arithmetic_overflow_check);
	key.Field("try_cast", node.try_cast);
	key.Field("not_in", node.not_in);
	key.Field("not_between", node.not_between);
	key.Field("lower_inclusive", node.lower_inclusive);
	key.Field("upper_inclusive", node.upper_inclusive);
	AppendSljitExpressionTreeChild(key, "left", node.left);
	AppendSljitExpressionTreeChild(key, "right", node.right);
	AppendSljitExpressionTreeChild(key, "else", node.else_node);
	key.Field("child_count", node.children.size());
	for (auto &child : node.children) {
		AppendSljitExpressionTreeChild(key, "child", child);
	}
}

static void AppendSljitPredicateSemanticKey(SljitArtifactKeyWriter &key, const SljitNativePredicate &predicate) {
	key.Field("kind", static_cast<uint8_t>(predicate.kind));
	key.Field("return_type", predicate.return_type);
	key.Field("constant_value", predicate.constant_value);
	key.Field("constant_null", predicate.constant_is_null);
	key.Field("conjunction", static_cast<uint8_t>(predicate.conjunction_op));
	key.Field("source", predicate.source_index);
	key.Field("right_source", predicate.right_source_index);
	key.Field("constant", predicate.constant);
	key.Field("int128_lower", predicate.int128_constant_lower);
	key.Field("int128_upper", predicate.int128_constant_upper);
	key.Field("constant_left", predicate.constant_on_left);
	key.Field("integer_kind", static_cast<uint8_t>(predicate.integer_kind));
	key.Field("double_source_kind", static_cast<uint8_t>(predicate.double_source_kind));
	key.Field("double_right_source_kind", static_cast<uint8_t>(predicate.double_right_source_kind));
	key.Field("double_constant", predicate.double_constant);
	key.Field("double_source_decimal_scale", predicate.double_source_decimal_scale);
	key.Field("double_right_source_decimal_scale", predicate.double_right_source_decimal_scale);
	key.Field("compare", static_cast<uint8_t>(predicate.compare_op));
	key.Field("null_check", static_cast<uint8_t>(predicate.null_check_op));
	key.Field("constant_count", predicate.constants.size());
	for (auto value : predicate.constants) {
		key.Field("constant_item", value);
	}
	key.Field("list_null", predicate.list_has_null);
	key.Field("not_in", predicate.not_in);
	key.Field("lower", predicate.lower);
	key.Field("upper", predicate.upper);
	key.Field("lower_inclusive", predicate.lower_inclusive);
	key.Field("upper_inclusive", predicate.upper_inclusive);
	key.Field("not_between", predicate.not_between);
	key.Field("string_constant", predicate.string_constant);
	key.Field("string_constant_count", predicate.string_constants.size());
	for (auto &value : predicate.string_constants) {
		key.Field("string_constant_item", value);
	}
	key.Field("substring_length", predicate.substring_length);
	key.Field("guard_null", predicate.guard_has_null_constant);
	for (auto value : predicate.guard_source_indices) {
		key.Field("guard_source", value);
	}
	for (auto value : predicate.source_indices) {
		key.Field("source_item", value);
	}
	for (auto value : predicate.source_not_null) {
		key.Field("source_not_null", value);
	}
	key.Field("has_child", predicate.child != nullptr);
	if (predicate.child) {
		key.Nested("child", [&]() { AppendSljitPredicateSemanticKey(key, *predicate.child); });
	}
	key.Field("child_count", predicate.children.size());
	for (auto &child : predicate.children) {
		key.Field("child_present", child != nullptr);
		if (child) {
			key.Nested("child_item", [&]() { AppendSljitPredicateSemanticKey(key, *child); });
		}
	}
}

static void AppendSljitExpressionArtifactKey(SljitArtifactKeyWriter &key, const SljitNativeRegionExpressionPlan &expr) {
	key.Field("kind", static_cast<uint8_t>(expr.kind));
	key.Field("integer_kind", static_cast<uint8_t>(expr.integer_kind));
	key.Field("return_type", expr.return_type);
	key.Field("constant_value", expr.constant_value);
	key.Field("source", expr.source_index);
	key.Field("right_source", expr.right_source_index);
	key.Field("constant", expr.constant);
	key.Field("result_min", expr.result_min);
	key.Field("result_max", expr.result_max);
	key.Field("constant_left", expr.constant_on_left);
	key.Field("overflow", expr.check_arithmetic_overflow);
	key.Field("result_range", expr.check_result_range);
	key.Field("binary", static_cast<uint8_t>(expr.binary_op));
	key.Field("double_binary", static_cast<uint8_t>(expr.double_binary_op));
	key.Field("double_source_kind", static_cast<uint8_t>(expr.double_source_kind));
	key.Field("double_right_source_kind", static_cast<uint8_t>(expr.double_right_source_kind));
	key.Field("double_source_decimal_scale", expr.double_source_decimal_scale);
	key.Field("double_right_source_decimal_scale", expr.double_right_source_decimal_scale);
	key.Field("compare", static_cast<uint8_t>(expr.compare_op));
	key.Field("cast_source", static_cast<uint8_t>(expr.cast_source_width));
	key.Field("right_cast_source", static_cast<uint8_t>(expr.right_cast_source_width));
	key.Field("cast_target", static_cast<uint8_t>(expr.cast_target_width));
	key.Field("unsigned_source", static_cast<uint8_t>(expr.unsigned_source_width));
	key.Field("unsigned_target", static_cast<uint8_t>(expr.unsigned_cast_target_width));
	key.Field("cast_chain_source", expr.cast_chain.source_type);
	key.Field("cast_chain_steps", expr.cast_chain.steps.size());
	for (auto &step : expr.cast_chain.steps) {
		key.Field("cast_chain_target", step.target_type);
		D_ASSERT(!step.query_location.IsValid() && step.source_alias.empty());
		key.Field("cast_error_binding", step.error_binding);
	}
	key.Field("string_compress_size", expr.string_compress_target_size);
	key.Field("string_decompress_size", expr.string_decompress_source_size);
	key.Field("substring_length", expr.string_substring_length);
	key.Field("guard_source", expr.guard_source_index);
	key.Field("guard_compare", static_cast<uint8_t>(expr.guard_compare_op));
	key.Field("guard_constant", expr.guard_constant);
	key.Field("guard_constant_left", expr.guard_constant_on_left);
	key.Field("guarded_value_size", expr.guarded_value_size);
	key.Field("error", expr.error_message);
	key.Field("try_cast", expr.try_cast);
	key.Field("double_constant", expr.double_constant);
	key.Field("signed_width", static_cast<uint8_t>(expr.signed_integer_width));
	key.Field("coalesce_rhs", static_cast<uint8_t>(expr.coalesce_rhs_kind));
	key.Field("coalesce_null", expr.coalesce_constant_is_null);
	key.Field("null_check", static_cast<uint8_t>(expr.null_check_op));
	for (auto value : expr.constants) {
		key.Field("constant_item", value);
	}
	key.Field("lower", expr.lower);
	key.Field("upper", expr.upper);
	key.Field("list_null", expr.list_has_null);
	key.Field("not_in", expr.not_in);
	key.Field("not_between", expr.not_between);
	key.Field("lower_inclusive", expr.lower_inclusive);
	key.Field("upper_inclusive", expr.upper_inclusive);
	key.Field("references_region_input", expr.references_region_input);
	key.Field("flat_nullable_fast_path", expr.emit_flat_nullable_fast_path);
	key.Field("predicate", expr.predicate != nullptr);
	if (expr.predicate) {
		key.Nested("predicate_value", [&]() { AppendSljitPredicateSemanticKey(key, *expr.predicate); });
	}
	key.Field("expression_tree", expr.expression_tree != nullptr);
	if (expr.expression_tree) {
		key.Nested("expression_tree_value",
		           [&]() { AppendSljitExpressionTreeSemanticKey(key, *expr.expression_tree); });
	}
	for (auto source_index : expr.expression_tree_source_indices) {
		key.Field("expression_tree_source", source_index);
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionExpressionFragment &fragment) {
	key.Field("index", fragment.expression_index);
	key.Field("return_type", fragment.return_type);
	key.Field("root", fragment.root != nullptr);
	if (fragment.root) {
		key.Nested("root_value", [&]() { AppendSljitExpressionTreeSemanticKey(key, *fragment.root); });
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionNativeGroupedStateContract &contract) {
	key.Field("status", contract.status);
	key.Field("required_capability", contract.required_capability);
	key.Field("contract_version", contract.contract_version);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionNativeOperatorContract &contract) {
	key.Field("status", contract.status);
	key.Field("required_capability", contract.required_capability);
	key.Field("contract_version", contract.contract_version);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionPrimitiveAggregateStateLane &contract) {
	key.Field("aggregate_index", contract.aggregate_index);
	key.Field("source_output_index", contract.source_output_index);
	key.Field("return_type", contract.return_type);
	key.Field("primitive_update_kind", contract.primitive_update_kind);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionNativeStateScanContract &contract) {
	key.Field("status", contract.status);
	key.Field("required_capability", contract.required_capability);
	key.Field("contract_version", contract.contract_version);
	key.Field("primitive_aggregate_batch_ready", contract.primitive_aggregate_batch_ready);
	key.Field("primitive_aggregate_lanes_count", contract.primitive_aggregate_lanes.size());
	for (auto &entry : contract.primitive_aggregate_lanes) {
		key.Nested("primitive_aggregate_lanes_entry", [&]() { AppendContractKey(key, entry); });
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionHashJoinResidualSource &contract) {
	key.Field("kind", contract.kind);
	key.Field("source_index", contract.source_index);
	key.Field("input_index", contract.input_index);
	key.Field("not_null", contract.not_null);
	key.Field("type", contract.type);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionHashJoinContract &contract) {
	key.Field("present", contract.present);
	key.Field("join_type", contract.join_type);
	key.Field("condition_count", contract.condition_count);
	key.Field("equality_condition_count", contract.equality_condition_count);
	key.Field("non_equality_condition_count", contract.non_equality_condition_count);
	key.Field("null_equal_condition_count", contract.null_equal_condition_count);
	key.Field("condition_types", contract.condition_types);
	key.Field("rhs_condition_types", contract.rhs_condition_types);
	key.Field("comparison_types", contract.comparison_types);
	key.Field("payload_column_count", contract.payload_column_count);
	key.Field("payload_column_indices", contract.payload_column_indices);
	key.Field("payload_types", contract.payload_types);
	key.Field("lhs_output_column_count", contract.lhs_output_column_count);
	key.Field("lhs_output_column_indices", contract.lhs_output_column_indices);
	key.Field("lhs_output_types", contract.lhs_output_types);
	key.Field("rhs_output_column_count", contract.rhs_output_column_count);
	key.Field("rhs_output_column_indices", contract.rhs_output_column_indices);
	key.Field("rhs_output_types", contract.rhs_output_types);
	key.Field("lhs_probe_column_count", contract.lhs_probe_column_count);
	key.Field("lhs_probe_column_indices", contract.lhs_probe_column_indices);
	key.Field("lhs_probe_types", contract.lhs_probe_types);
	key.Field("lhs_output_in_probe_count", contract.lhs_output_in_probe_count);
	key.Field("delim_type_count", contract.delim_type_count);
	key.Field("correlated_mark_counts_required", contract.correlated_mark_counts_required);
	key.Field("residual_predicate", contract.residual_predicate);
	key.Field("residual_info", contract.residual_info);
	key.Field("residual_expression_ready", contract.residual_expression_ready);
	key.Nested("residual_expression", [&]() { AppendContractKey(key, contract.residual_expression); });
	key.Field("residual_sources_count", contract.residual_sources.size());
	for (auto &entry : contract.residual_sources) {
		key.Nested("residual_sources_entry", [&]() { AppendContractKey(key, entry); });
	}
	key.Field("filter_pushdown", contract.filter_pushdown);
	key.Field("filter_pushdown_condition_count", contract.filter_pushdown_condition_count);
	key.Field("filter_pushdown_probe_count", contract.filter_pushdown_probe_count);
	key.Field("build_side_has_filter", contract.build_side_has_filter);
	key.Field("source_produces_rows", contract.source_produces_rows);
	key.Field("regular_hash_table_layout_ready", contract.regular_hash_table_layout_ready);
	key.Field("perfect_hash_probe_shape_ready", contract.perfect_hash_probe_shape_ready);
	key.Field("native_probe_shape_ready", contract.native_probe_shape_ready);
	key.Field("build_sink_shape_ready", contract.build_sink_shape_ready);
	key.Field("native_probe_output_mode", contract.native_probe_output_mode);
	key.Field("output_reaches_dictionary_narrowing_sink", contract.output_reaches_dictionary_narrowing_sink);
	key.Field("payload_dict_index_widths", contract.payload_dict_index_widths);
	key.Field("layout_offsets", contract.layout_offsets);
	key.Field("tuple_size", contract.tuple_size);
	key.Field("pointer_offset", contract.pointer_offset);
	key.Nested("native_probe_contract", [&]() { AppendContractKey(key, contract.native_probe_contract); });
	key.Nested("native_build_contract", [&]() { AppendContractKey(key, contract.native_build_contract); });
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionAggregateContract &contract) {
	key.Field("present", contract.present);
	key.Field("kind", contract.kind);
	key.Field("group_count", contract.group_count);
	key.Field("group_types", contract.group_types);
	key.Field("aggregate_count", contract.aggregate_count);
	key.Field("aggregate_functions", contract.aggregate_functions);
	key.Field("aggregate_return_types", contract.aggregate_return_types);
	key.Field("aggregate_child_counts", contract.aggregate_child_counts);
	key.Field("aggregate_types", contract.aggregate_types);
	key.Field("aggregate_filter_count", contract.aggregate_filter_count);
	key.Field("aggregate_order_count", contract.aggregate_order_count);
	key.Field("payload_type_count", contract.payload_type_count);
	key.Field("payload_types", contract.payload_types);
	key.Field("grouping_set_count", contract.grouping_set_count);
	key.Field("grouping_function_count", contract.grouping_function_count);
	key.Field("radix_table_count", contract.radix_table_count);
	key.Field("distinct_aggregate_count", contract.distinct_aggregate_count);
	key.Field("distinct_table_count", contract.distinct_table_count);
	key.Field("distinct_child_count", contract.distinct_child_count);
	key.Field("input_group_type_count", contract.input_group_type_count);
	key.Field("input_group_types", contract.input_group_types);
	key.Field("non_distinct_filter_count", contract.non_distinct_filter_count);
	key.Field("distinct_filter_count", contract.distinct_filter_count);
	key.Field("perfect_required_bits_count", contract.perfect_required_bits_count);
	key.Field("perfect_required_bits_total", contract.perfect_required_bits_total);
	key.Field("perfect_required_bits", contract.perfect_required_bits);
	key.Field("perfect_group_minima_count", contract.perfect_group_minima_count);
	key.Field("perfect_group_minima", contract.perfect_group_minima);
	key.Field("grouped_state_layout_ready", contract.grouped_state_layout_ready);
	key.Field("grouped_state_offsets", contract.grouped_state_offsets);
	key.Nested("native_grouped_state_contract",
	           [&]() { AppendContractKey(key, contract.native_grouped_state_contract); });
	key.Nested("native_state_update_contract",
	           [&]() { AppendContractKey(key, contract.native_state_update_contract); });
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionOrderContract &contract) {
	key.Field("present", contract.present);
	key.Field("kind", contract.kind);
	key.Field("order_count", contract.order_count);
	key.Field("payload_type_count", contract.payload_type_count);
	key.Field("payload_types", contract.payload_types);
	key.Field("projection_count", contract.projection_count);
	key.Field("projection_ids", contract.projection_ids);
	key.Field("has_limit", contract.has_limit);
	key.Field("limit", contract.limit);
	key.Field("offset", contract.offset);
	key.Field("dynamic_filter", contract.dynamic_filter);
	key.Field("is_index_sort", contract.is_index_sort);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionAggregateInput &contract) {
	key.Field("aggregate_index", contract.aggregate_index);
	key.Field("payload_index", contract.payload_index);
	key.Field("child_count", contract.child_count);
	key.Field("function_name", contract.function_name);
	key.Field("return_type", contract.return_type);
	key.Field("child_types", contract.child_types);
	key.Field("child_indices", contract.child_indices);
	key.Field("child_expressions_count", contract.child_expressions.size());
	for (auto &entry : contract.child_expressions) {
		key.Nested("child_expressions_entry", [&]() { AppendContractKey(key, entry); });
	}
	key.Field("distinct", contract.distinct);
	key.Field("has_filter", contract.has_filter);
	key.Field("has_order_bys", contract.has_order_bys);
	key.Field("order_dependent", contract.order_dependent);
	key.Field("has_state_update", contract.has_state_update);
	key.Field("primitive_update_ready", contract.primitive_update_ready);
	key.Field("primitive_update_kind", contract.primitive_update_kind);
	key.Field("primitive_update_input_type", contract.primitive_update_input_type);
	key.Field("primitive_update_state_size", contract.primitive_update_state_size);
	key.Field("primitive_update_state_value_offset", contract.primitive_update_state_value_offset);
	key.Field("primitive_update_state_is_set_offset", contract.primitive_update_state_is_set_offset);
	key.Field("supported_payload_references", contract.supported_payload_references);
	key.Field("payload_expressions_ready", contract.payload_expressions_ready);
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionHashJoinKeyInput &contract) {
	key.Field("key_index", contract.key_index);
	key.Field("input_index", contract.input_index);
	key.Field("type", contract.type);
	key.Field("supported_reference", contract.supported_reference);
	key.Field("expression_ready", contract.expression_ready);
	key.Nested("expression", [&]() { AppendContractKey(key, contract.expression); });
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionGroupInput &contract) {
	key.Field("group_index", contract.group_index);
	key.Field("input_index", contract.input_index);
	key.Field("type", contract.type);
	key.Field("supported_reference", contract.supported_reference);
	key.Field("expression_ready", contract.expression_ready);
	key.Nested("expression", [&]() { AppendContractKey(key, contract.expression); });
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionOperatorInfo &contract) {
	if (contract.Kind() != ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
		throw InternalException("SLJIT probe artifact requires a hash join contract");
	}
	key.Field("kind", contract.Kind());
	key.Nested("hash_join_contract", [&]() { AppendContractKey(key, contract.HashJoin().contract); });
	key.Field("hash_join_keys_count", contract.HashJoin().keys.size());
	for (auto &entry : contract.HashJoin().keys) {
		key.Nested("hash_join_keys_entry", [&]() { AppendContractKey(key, entry); });
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionAggregateSinkInfo &contract) {
	if (contract.Kind() == ExecutionRegionSinkKind::NONE) {
		throw InternalException("SLJIT aggregate artifact requires an aggregate operator kind");
	}
	key.Field("kind", contract.Kind());
	key.Nested("aggregate_contract", [&]() { AppendContractKey(key, contract.contract); });
	key.Field("aggregates_count", contract.aggregates.size());
	for (auto &entry : contract.aggregates) {
		key.Nested("aggregates_entry", [&]() { AppendContractKey(key, entry); });
	}
	key.Field("groups_count", contract.groups.size());
	for (auto &entry : contract.groups) {
		key.Nested("groups_entry", [&]() { AppendContractKey(key, entry); });
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionHashJoinInfo &contract) {
	key.Field("kind", ExecutionRegionSinkKind::HASH_JOIN_BUILD);
	key.Nested("hash_join_contract", [&]() { AppendContractKey(key, contract.contract); });
	key.Field("hash_join_keys_count", contract.keys.size());
	for (auto &entry : contract.keys) {
		key.Nested("hash_join_keys_entry", [&]() { AppendContractKey(key, entry); });
	}
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionOrderSinkInfo &contract) {
	key.Field("kind", ExecutionRegionSinkKind::SORT);
	key.Nested("order_contract", [&]() { AppendContractKey(key, contract.contract); });
	key.Nested("native_sink_contract", [&]() { AppendContractKey(key, contract.native_contract); });
}

static void AppendContractKey(SljitArtifactKeyWriter &key, const ExecutionRegionSinkInfo &contract) {
	key.Field("kind", contract.Kind());
	switch (contract.Kind()) {
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		key.Nested("native_sink_contract", [&]() { AppendContractKey(key, contract.Native()); });
		break;
	default:
		throw InternalException("SLJIT artifact contains an unsupported sink contract");
	}
}

static void AppendExpressionListKey(SljitArtifactKeyWriter &key,
                                    const vector<SljitNativeRegionExpressionPlan> &expressions) {
	key.Field("count", expressions.size());
	for (auto &expression : expressions) {
		key.Nested("expression", [&]() { AppendSljitExpressionArtifactKey(key, expression); });
	}
}

static void AppendCastChainKey(SljitArtifactKeyWriter &key, const SljitIntegerCastChain &chain) {
	key.Field("source_type", chain.source_type);
	key.Field("count", chain.steps.size());
	for (auto &step : chain.steps) {
		key.Field("target_type", step.target_type);
		D_ASSERT(!step.query_location.IsValid() && step.source_alias.empty());
		key.Field("cast_error_binding", step.error_binding);
	}
}

static void AppendOpKey(SljitArtifactKeyWriter &key, const SljitNativeRegionOpPlan &op) {
	key.Field("kind", op.Kind());
	key.Field("operator_index", op.operator_index);
	key.Field("input_types", op.input_types);
	key.Field("output_types", op.output_types);
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::FILTER:
		key.Nested("filter", [&]() { AppendSljitExpressionArtifactKey(key, op.Filter()); });
		break;
	case SljitNativeRegionOpKind::PROJECTION:
		key.Nested("projections", [&]() { AppendExpressionListKey(key, op.Projections()); });
		break;
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
		auto &probe = op.HashJoinProbe();
		key.Field("probe_operator_index", probe.operator_index);
		key.Field("key_count", probe.keys.size());
		for (auto &entry : probe.keys) {
			key.Field("key_input_index", entry.key_input_index);
			key.Field("key_layout_offset", entry.key_layout_offset);
			key.Field("key_type", entry.key_type);
			key.Field("key_kind", entry.key_kind);
			key.Field("comparison_type", entry.comparison_type);
			key.Field("equality_key", entry.equality_key);
			key.Field("null_equal", entry.null_equal);
			key.Field("source_known_not_null", entry.source_known_not_null);
			key.Nested("source_cast", [&]() { AppendCastChainKey(key, entry.source_cast); });
		}
		key.Field("equality_key_count", probe.equality_key_count);
		key.Field("mark_build_match", probe.mark_build_match);
		key.Field("mark_build_match_after_residual", probe.mark_build_match_after_residual);
		key.Field("residual_predicate", probe.residual_predicate);
		key.Field("perfect_hash_probe", probe.perfect_hash_probe);
		key.Field("exact_source_filter_binding", probe.exact_source_filter_binding);
		key.Field("found_match_offset", probe.found_match_offset);
		key.Field("pointer_offset", probe.pointer_offset);
		key.Field("output_mode", probe.output_mode);
		key.Field("probe_input_types", probe.input_types);
		key.Field("residual_source_types", probe.residual_source_types);
		key.Field("residual_source_not_null", probe.residual_source_not_null);
		key.Nested("residual_filter", [&]() { AppendSljitExpressionArtifactKey(key, probe.residual_filter); });
		key.Nested("operator", [&]() { AppendContractKey(key, probe.operator_info); });
		break;
	}
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		key.Nested("sink", [&]() { AppendContractKey(key, op.HashJoinBuild().sink_info); });
		key.Field("build_input_types", op.HashJoinBuild().input_types);
		break;
	case SljitNativeRegionOpKind::APPEND_SINK:
		key.Nested("sink", [&]() { AppendContractKey(key, op.AppendSink().sink_info); });
		key.Field("append_input_types", op.AppendSink().input_types);
		break;
	case SljitNativeRegionOpKind::ORDER_SINK:
		key.Nested("sink", [&]() { AppendContractKey(key, op.OrderSink().sink_info); });
		key.Field("order_input_types", op.OrderSink().input_types);
		break;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
		auto &aggregate = op.AggregateUpdate();
		key.Nested("sink", [&]() { AppendContractKey(key, aggregate.sink_info); });
		key.Field("aggregate_input_types", aggregate.input_types);
		key.Nested("payloads", [&]() { AppendExpressionListKey(key, aggregate.payloads); });
		key.Nested("groups", [&]() { AppendExpressionListKey(key, aggregate.group_expressions); });
		key.Field("payload_binding_state", aggregate.payload_binding_state);
		key.Field("use_grouped_state_addresses", aggregate.use_grouped_state_addresses);
		key.Field("use_perfect_hash_group_lookup", aggregate.use_perfect_hash_group_lookup);
		break;
	}
	}
}

string BuildSljitExpressionArtifactKey(const SljitNativeRegionExpressionPlan &expr) {
	SljitArtifactKeyWriter key;
	AppendSljitExpressionArtifactKey(key, expr);
	return key.Take();
}

string BuildSljitRegionArtifactSemanticKey(const SljitNativeRegionPlan &region) {
	SljitArtifactKeyWriter key;
	key.Field("schema", "sljit-artifact-v10");
	key.Field("cast_error_count", region.cast_errors.size());
	key.Field("scan_filter_mode", region.scan_filter_mode);
	key.Field("source_types", region.source_output_types);
	key.Field("source_not_null", region.source_not_null);
	key.Field("scan_filter_count", region.scan_filters.size());
	for (auto &filter : region.scan_filters) {
		key.Field("filter_index", filter.filter_index);
		key.Field("input_type", filter.input_type);
		key.Field("input_not_null", filter.input_not_null);
		key.Nested("expression", [&]() { AppendSljitExpressionArtifactKey(key, filter.filter); });
	}
	key.Field("state_source", region.aggregate_state_source.function_name);
	key.Nested("state_scan", [&]() { AppendContractKey(key, region.aggregate_state_source.state_scan_contract); });
	key.Field("op_count", region.ops.size());
	for (auto &op : region.ops) {
		key.Nested("op", [&]() { AppendOpKey(key, op); });
	}
	key.Field("native_continuation_source_op_count", region.native_continuation_source_ops.size());
	for (auto &op : region.native_continuation_source_ops) {
		key.Nested("native_continuation_source_op", [&]() { AppendOpKey(key, op); });
	}
	return key.Take();
}

string BuildSljitRegionArtifactBindingKey(const SljitNativeRegionPlan &region) {
	SljitArtifactKeyWriter key;
	key.Field("schema", "sljit-binding-v4");
	key.Field("distinct", region.source_distinct_counts);
	key.Field("min", region.source_min_values);
	key.Field("max", region.source_max_values);
	return key.Take();
}

string ComposeSljitRegionArtifactCacheKey(const string &semantic_key, const string &binding_key) {
	SljitArtifactKeyWriter key;
	key.Field("schema", "sljit-cache-v2");
	key.Field("semantic", semantic_key);
	key.Field("binding", binding_key);
	return key.Take();
}

} // namespace duckdb
