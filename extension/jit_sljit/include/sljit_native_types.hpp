//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_expression_ir.hpp"

namespace duckdb {

class Vector;

enum class SljitNativeIntegerBinaryOp : uint8_t { ADD, SUBTRACT, MULTIPLY };
enum class SljitNativeDoubleBinaryOp : uint8_t { ADD, SUBTRACT, MULTIPLY, DIVIDE };
enum class SljitNativeDoubleSourceKind : uint8_t {
	FLOAT,
	DOUBLE,
	INT64_TO_DOUBLE,
	DECIMAL64_TO_DOUBLE,
	INT128_TO_DOUBLE,
	DECIMAL128_TO_DOUBLE
};
enum class SljitNativeIntegerCompareOp : uint8_t {
	EQUAL,
	NOT_EQUAL,
	LESS_THAN,
	GREATER_THAN,
	LESS_THAN_OR_EQUAL,
	GREATER_THAN_OR_EQUAL
};
enum class SljitNativeIntegerKind : uint8_t { INT8, UINT8, INT32, INT64, DECIMAL64 };
enum class SljitNativeSignedIntegerWidth : uint8_t { INT8, INT16, INT32, INT64 };
enum class SljitNativeUnsignedIntegerWidth : uint8_t { UINT8, UINT16, UINT32 };
enum class SljitNativeHashJoinKeyKind : uint8_t {
	INT8,
	INT16,
	INT32,
	INT64,
	INT128,
	UINT8,
	UINT16,
	UINT32,
	UINT64,
	UINT128
};
enum class SljitNativeNestedLoopJoinValueKind : uint8_t { INT32, INT64, INT128, DOUBLE };
enum class SljitNativeCoalesceRhsKind : uint8_t { CONSTANT, REFERENCE };
enum class SljitNativeNullCheckOp : uint8_t { IS_NULL, IS_NOT_NULL };
enum class SljitNativePredicateKind : uint8_t {
	CONSTANT,
	REFERENCE,
	NOT,
	CONJUNCTION,
	CONSTANT_OR_NULL,
	INTEGER_COMPARE_CONSTANT,
	INTEGER_COMPARE_REFERENCES,
	DOUBLE_COMPARE_CONSTANT,
	DOUBLE_COMPARE_REFERENCES,
	INT128_COMPARE_CONSTANT,
	INT128_COMPARE_REFERENCES,
	INTEGER_IN_LIST,
	INTEGER_BETWEEN,
	STRING_EQUAL_CONSTANT,
	STRING_IN_LIST_CONSTANT,
	STRING_PREFIX_CONSTANT,
	STRING_SUFFIX_CONSTANT,
	STRING_CONTAINS_CONSTANT,
	STRING_LIKE_CONSTANT,
	STRING_SUBSTRING_IN_LIST_CONSTANT,
	NULL_CHECK
};

struct SljitNativePredicate {
	SljitNativePredicateKind kind = SljitNativePredicateKind::CONSTANT;
	LogicalType return_type;
	bool constant_value = false;
	bool constant_is_null = false;
	ExecutionExpressionConjunctionOp conjunction_op = ExecutionExpressionConjunctionOp::AND;
	idx_t source_index = 0;
	idx_t right_source_index = 0;
	int64_t constant = 0;
	uint64_t int128_constant_lower = 0;
	int64_t int128_constant_upper = 0;
	bool constant_on_left = false;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	SljitNativeDoubleSourceKind double_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
	SljitNativeDoubleSourceKind double_right_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
	double double_constant = 0;
	double double_source_scale = 1;
	double double_right_source_scale = 1;
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeNullCheckOp null_check_op = SljitNativeNullCheckOp::IS_NULL;
	vector<int64_t> constants;
	bool list_has_null = false;
	bool not_in = false;
	int64_t lower = 0;
	int64_t upper = 0;
	bool lower_inclusive = true;
	bool upper_inclusive = true;
	bool not_between = false;
	string string_constant;
	vector<string> string_constants;
	idx_t substring_length = 0;
	bool guard_has_null_constant = false;
	vector<idx_t> guard_source_indices;
	vector<idx_t> source_indices;
	unique_ptr<SljitNativePredicate> child;
	vector<unique_ptr<SljitNativePredicate>> children;

	unique_ptr<SljitNativePredicate> Copy() const;
};

struct SljitNativeConstantOrNull {
	Value constant;
	vector<idx_t> guard_source_indices;
	bool guard_has_null_constant = false;
};

struct SljitNativeVectorInput {
	const_data_ptr_t source_data = nullptr;
	const_data_ptr_t right_source_data = nullptr;
	const_data_ptr_t *source_data_array = nullptr;
	const_data_ptr_t *right_source_data_array = nullptr;
	const_data_ptr_t *group_data_array = nullptr;
	data_ptr_t *result_data_array = nullptr;
	const_data_ptr_t floating_constants = nullptr;
	data_ptr_t floating_stats_min = nullptr;
	data_ptr_t floating_stats_max = nullptr;
	const sel_t *execute_sel = nullptr;
	const sel_t *source_sel = nullptr;
	const sel_t *right_source_sel = nullptr;
	const sel_t **source_sel_array = nullptr;
	const sel_t **right_source_sel_array = nullptr;
	const sel_t **group_sel_array = nullptr;
	const validity_t *source_validity = nullptr;
	const validity_t *right_source_validity = nullptr;
	const validity_t **source_validity_array = nullptr;
	const validity_t **right_source_validity_array = nullptr;
	const validity_t **group_validity_array = nullptr;
	const int64_t *constants = nullptr;
	int64_t constant = 0;
	double double_constant = 0;
	double source_double_scale = 1;
	double right_source_double_scale = 1;
	data_ptr_t result_data = nullptr;
	Vector *result_vector = nullptr;
	validity_t *result_validity = nullptr;
	sel_t *true_sel = nullptr;
	sel_t *false_sel = nullptr;
	idx_t selected_count = 0;
	const char *overflow_message = nullptr;
	const char *error_message = nullptr;
	optional_idx query_location;
	int64_t overflow_value = 0;
	idx_t string_decompress_source_size = 0;
	idx_t active_source_index = 0;
	idx_t active_result_index = 0;
	idx_t count = 0;
	bool expression_tree_flat_no_selection = false;
	bool expression_tree_flat_all_valid = false;
	int64_t *aggregate_int64_value = nullptr;
	hugeint_t *aggregate_hugeint_value = nullptr;
	double *aggregate_double_value = nullptr;
	int64_t **aggregate_int64_values = nullptr;
	hugeint_t **aggregate_hugeint_values = nullptr;
	int64_t aggregate_local_int64 = 0;
	hugeint_t aggregate_local_hugeint;
	bool *aggregate_state_is_set = nullptr;
	bool **aggregate_state_is_sets = nullptr;
	idx_t *aggregate_row_count = nullptr;
	idx_t **aggregate_row_counts = nullptr;
	const uintptr_t *aggregate_state_addresses = nullptr;
	idx_t aggregate_state_offset = 0;
	idx_t aggregate_state_value_offset = 0;
	idx_t aggregate_state_is_set_offset = 0;
	data_ptr_t perfect_hash_state_data = nullptr;
	bool *perfect_hash_group_is_set = nullptr;
	idx_t perfect_hash_total_groups = 0;
	idx_t perfect_hash_tuple_size = 0;
	idx_t perfect_hash_aggregate_state_offset = 0;
	bool has_error = false;
	std::exception_ptr error;
};

struct SljitNativePredicateInput {
	const_data_ptr_t *source_data = nullptr;
	const sel_t **source_sel = nullptr;
	const validity_t **source_validity = nullptr;
	const sel_t *execute_sel = nullptr;
	data_ptr_t result_data = nullptr;
	validity_t *result_validity = nullptr;
	sel_t *true_sel = nullptr;
	sel_t *false_sel = nullptr;
	idx_t selected_count = 0;
	idx_t count = 0;
	std::exception_ptr error;
};

struct SljitNativeHashJoinProbeInput {
	const_data_ptr_t *source_data = nullptr;
	const sel_t **source_sel = nullptr;
	const validity_t **source_validity = nullptr;
	idx_t count = 0;
	const_data_ptr_t entries = nullptr;
	uint64_t bitmask = 0;
	uint64_t pointer_mask = 0;
	bool use_salt = false;
	bool rhs_keys_have_validity = false;
	bool chains_longer_than_one = false;
	bool dictionary_emission = false;
	idx_t key_offset = 0;
	idx_t pointer_offset = 0;
	const data_ptr_t *aux_next_ptrs = nullptr;
	sel_t *match_sel = nullptr;
	sel_t *build_sel = nullptr;
	data_ptr_t *row_pointers = nullptr;
	idx_t output_capacity = 0;
	uint64_t perfect_min = 0;
	uint64_t perfect_max = 0;
	const validity_t *perfect_validity = nullptr;
	idx_t selected_count = 0;
	idx_t input_offset = 0;
	data_ptr_t resume_row_pointer = nullptr;
	bool finished = false;
};

struct SljitNativeNestedLoopJoinProbeInput {
	const_data_ptr_t left_data = nullptr;
	const_data_ptr_t right_data = nullptr;
	const sel_t *left_sel = nullptr;
	const sel_t *right_sel = nullptr;
	const validity_t *left_validity = nullptr;
	const validity_t *right_validity = nullptr;
	idx_t left_count = 0;
	idx_t right_count = 0;
	idx_t left_offset = 0;
	idx_t right_offset = 0;
	idx_t output_capacity = 0;
	sel_t *left_match_sel = nullptr;
	sel_t *right_match_sel = nullptr;
	idx_t selected_count = 0;
	bool right_chunk_finished = false;
};

} // namespace duckdb
