//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
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
//! The operator that compares the operands in swapped order alike: `a op b` holds exactly when `b flip(op) a` does
inline SljitNativeIntegerCompareOp FlipNativeIntegerCompareOp(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	default:
		return op;
	}
}
enum class SljitNativeIntegerKind : uint8_t { INT8, UINT8, UINT16, UINT32, INT32, INT64, DECIMAL64, DATE };
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
	UINT128,
	STRING
};

static inline bool SljitSignedAffineGroupPhysicalType(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static inline bool SljitTryGetHashJoinKeyKind(PhysicalType physical_type, SljitNativeHashJoinKeyKind &kind) {
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		kind = SljitNativeHashJoinKeyKind::UINT8;
		return true;
	case PhysicalType::INT8:
		kind = SljitNativeHashJoinKeyKind::INT8;
		return true;
	case PhysicalType::UINT16:
		kind = SljitNativeHashJoinKeyKind::UINT16;
		return true;
	case PhysicalType::INT16:
		kind = SljitNativeHashJoinKeyKind::INT16;
		return true;
	case PhysicalType::UINT32:
		kind = SljitNativeHashJoinKeyKind::UINT32;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeHashJoinKeyKind::INT32;
		return true;
	case PhysicalType::UINT64:
		kind = SljitNativeHashJoinKeyKind::UINT64;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeHashJoinKeyKind::INT64;
		return true;
	case PhysicalType::UINT128:
		kind = SljitNativeHashJoinKeyKind::UINT128;
		return true;
	case PhysicalType::INT128:
		kind = SljitNativeHashJoinKeyKind::INT128;
		return true;
	case PhysicalType::VARCHAR:
		// generated string hashing mixes the inline bytes as little-endian words, as DuckDB's Hash(string_t) does
		kind = SljitNativeHashJoinKeyKind::STRING;
		return !DUCKDB_IS_BIG_ENDIAN;
	default:
		return false;
	}
}

static inline bool SljitTryGetHashJoinKeyKind(const LogicalType &type, SljitNativeHashJoinKeyKind &kind) {
	return SljitTryGetHashJoinKeyKind(type.InternalType(), kind);
}

static inline bool SljitHashJoinKeyKindMatchesPhysicalType(SljitNativeHashJoinKeyKind key_kind,
                                                           PhysicalType physical_type) {
	SljitNativeHashJoinKeyKind physical_key_kind;
	return SljitTryGetHashJoinKeyKind(physical_type, physical_key_kind) && key_kind == physical_key_kind;
}

static inline bool SljitHashJoinKeyCanUseInt64SourceForInt32Key(idx_t key_idx, SljitNativeHashJoinKeyKind key_kind,
                                                                PhysicalType source_type) {
	return key_idx == 0 && key_kind == SljitNativeHashJoinKeyKind::INT32 && source_type == PhysicalType::INT64;
}

static inline bool SljitHashJoinKeyKindIs128(SljitNativeHashJoinKeyKind kind) {
	return kind == SljitNativeHashJoinKeyKind::INT128 || kind == SljitNativeHashJoinKeyKind::UINT128;
}

//! Keys that fit one machine word: generated code loads, hashes and orders them with single register operations
static inline bool SljitHashJoinKeyKindIsWord(SljitNativeHashJoinKeyKind kind) {
	return !SljitHashJoinKeyKindIs128(kind) && kind != SljitNativeHashJoinKeyKind::STRING;
}

template <class DISPATCH>
static bool SljitDispatchHashJoinKeyKind(SljitNativeHashJoinKeyKind kind, DISPATCH &dispatch) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		dispatch.template Execute<int8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		dispatch.template Execute<int16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		dispatch.template Execute<int32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		dispatch.template Execute<int64_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT128:
		dispatch.template Execute<hugeint_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		dispatch.template Execute<uint8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		dispatch.template Execute<uint16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		dispatch.template Execute<uint32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		dispatch.template Execute<uint64_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT128:
		dispatch.template Execute<uhugeint_t>();
		return true;
	case SljitNativeHashJoinKeyKind::STRING:
		dispatch.template Execute<string_t>();
		return true;
	default:
		return false;
	}
}

enum class SljitHashJoinProbeLayoutKind : int8_t {
	RUNTIME = -1,
	NO_CHAIN = 0,
	NO_CHAIN_SALT = 1,
	CHAIN_DIRECT = 2,
	CHAIN_DICTIONARY = 3,
	CHAIN_SALT_DIRECT = 4,
	CHAIN_SALT_DICTIONARY = 5
};

static inline SljitHashJoinProbeLayoutKind
SljitHashJoinProbeLayoutKindFromFlags(bool use_salt, bool chains_longer_than_one, bool dictionary_emission) {
	if (!chains_longer_than_one) {
		return use_salt ? SljitHashJoinProbeLayoutKind::NO_CHAIN_SALT : SljitHashJoinProbeLayoutKind::NO_CHAIN;
	}
	if (use_salt) {
		return dictionary_emission ? SljitHashJoinProbeLayoutKind::CHAIN_SALT_DICTIONARY
		                           : SljitHashJoinProbeLayoutKind::CHAIN_SALT_DIRECT;
	}
	return dictionary_emission ? SljitHashJoinProbeLayoutKind::CHAIN_DICTIONARY
	                           : SljitHashJoinProbeLayoutKind::CHAIN_DIRECT;
}

static inline bool SljitHashJoinProbeLayoutUsesSalt(SljitHashJoinProbeLayoutKind kind) {
	return kind == SljitHashJoinProbeLayoutKind::NO_CHAIN_SALT ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_SALT_DIRECT ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_SALT_DICTIONARY;
}

static inline bool SljitHashJoinProbeLayoutChainsLongerThanOne(SljitHashJoinProbeLayoutKind kind) {
	return kind == SljitHashJoinProbeLayoutKind::CHAIN_DIRECT ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_DICTIONARY ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_SALT_DIRECT ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_SALT_DICTIONARY;
}

static inline bool SljitHashJoinProbeLayoutUsesDictionaryEmission(SljitHashJoinProbeLayoutKind kind) {
	return kind == SljitHashJoinProbeLayoutKind::CHAIN_DICTIONARY ||
	       kind == SljitHashJoinProbeLayoutKind::CHAIN_SALT_DICTIONARY;
}

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
	//! the scale of a decimal double source, 0 for any other source kind
	uint32_t double_source_decimal_scale = 0;
	uint32_t double_right_source_decimal_scale = 0;
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
	vector<bool> source_not_null;
	unique_ptr<SljitNativePredicate> child;
	vector<unique_ptr<SljitNativePredicate>> children;

	unique_ptr<SljitNativePredicate> Copy() const;
};

struct SljitPerfectHashDictionaryGroupRuntime {
	idx_t *contributions = nullptr;
	sel_t *active_indices = nullptr;
	idx_t *active_count = nullptr;
};

struct SljitCastErrorIdentity {
	QueryLocation query_location;
	string source_alias;
};

//! Lowering moves the error identity into an execution-local slot before artifact construction.
struct SljitIntegerCastStep {
	LogicalType target_type;
	QueryLocation query_location;
	string source_alias;
	idx_t error_binding = DConstants::INVALID_INDEX;
};

//! The bound casts one native integer cast stands for, innermost first. Native code checks only the final range; when
//! that check fails, the chain replays the value through DuckDB's casts and raises the error of the step that fails.
struct SljitIntegerCastChain {
	LogicalType source_type;
	vector<SljitIntegerCastStep> steps;

	[[noreturn]] void ThrowCastError(int64_t source_value, const vector<SljitCastErrorIdentity> &errors) const;
};

class ExecutionRegionRuntime;
const vector<SljitCastErrorIdentity> &SljitCastErrorBindings(ExecutionRegionRuntime &runtime);

//! Cold error handling resolves the current query's identity; generated code retains no query metadata.
struct SljitBoundIntegerCastChain {
	optional_ptr<const SljitIntegerCastChain> chain;
	optional_ptr<ExecutionRegionRuntime> runtime;

	[[noreturn]] void ThrowCastError(int64_t source_value) const;
};

//! The checks DuckDB's operator applies to one integer operation: the hardware overflow flag and, for kinds whose
//! values are narrower than their registers (DECIMAL64, DATE), the result range.
struct SljitIntegerOverflowChecks {
	bool overflow = false;
	bool range = false;
	int64_t result_min = 0;
	int64_t result_max = 0;
	//! the operation subtracts a value from zero to negate it: DuckDB reports its overflow as a failed negation
	bool negation = false;
};

//! A decimal source's scale and the power of ten its fast-path conversion divides by. DuckDB converts a decimal to
//! DOUBLE as `raw / 10^scale` only when the raw value is exactly representable (|raw| <= 2^53) and 10^scale is exact
//! (scale <= 22); every other value takes its correctly rounded decimal conversion.
struct SljitNativeDecimalSourceScale {
	double factor = 1;
	uint64_t scale = 0;
};

SljitNativeDecimalSourceScale SljitNativeDecimalSourceScaleFor(uint32_t scale);
//! DuckDB's own DECIMAL -> DOUBLE cast of a raw decimal value
double SljitNativeDecimalToDouble(int64_t value, uint32_t scale);
double SljitNativeDecimalToDouble(hugeint_t value, uint32_t scale);

struct SljitNativeVectorInput {
	const_data_ptr_t source_data = nullptr;
	const_data_ptr_t right_source_data = nullptr;
	const_data_ptr_t *source_data_array = nullptr;
	const_data_ptr_t *right_source_data_array = nullptr;
	const_data_ptr_t *group_data_array = nullptr;
	SljitPerfectHashDictionaryGroupRuntime *perfect_hash_dictionary_groups = nullptr;
	data_ptr_t *result_data_array = nullptr;
	const sel_t *execute_sel = nullptr;
	const sel_t *source_sel = nullptr;
	const sel_t *right_source_sel = nullptr;
	const sel_t **source_sel_array = nullptr;
	const sel_t **right_source_sel_array = nullptr;
	const sel_t *source_common_sel = nullptr;
	const sel_t **group_sel_array = nullptr;
	const validity_t *source_validity = nullptr;
	const validity_t *right_source_validity = nullptr;
	const validity_t **source_validity_array = nullptr;
	const validity_t **right_source_validity_array = nullptr;
	const validity_t **group_validity_array = nullptr;
	const int64_t *constants = nullptr;
	//! per string comparison with a constant, the match of each entry of the dictionary its source is (nonzero when
	//! the entry equals the constant), or nullptr where the rows are compared one by one
	const uint8_t *const *string_matches = nullptr;
	int64_t constant = 0;
	double double_constant = 0;
	SljitNativeDecimalSourceScale source_decimal;
	SljitNativeDecimalSourceScale right_source_decimal;
	data_ptr_t result_data = nullptr;
	Vector *result_vector = nullptr;
	validity_t *result_validity = nullptr;
	sel_t *true_sel = nullptr;
	sel_t *false_sel = nullptr;
	idx_t selected_count = 0;
	const char *error_message = nullptr;
	//! the casts a native integer cast stands for, which raise its error
	SljitBoundIntegerCastChain cast_chain;
	//! the value a failed cast saw, or the left operand of an overflowing integer operation
	int64_t overflow_value = 0;
	//! the right operand of an overflowing integer operation
	int64_t overflow_right_value = 0;
	idx_t string_decompress_source_size = 0;
	idx_t active_source_index = 0;
	idx_t active_result_index = 0;
	idx_t count = 0;
	bool expression_tree_flat_no_selection = false;
	bool expression_tree_flat_all_valid = false;
	bool expression_tree_all_valid = false;
	// Perfect-hash group representation is independent from the payload expression representation above.
	bool perfect_hash_group_flat_all_valid = false;
	bool perfect_hash_group_all_valid = false;
	bool perfect_hash_inputs_flat_no_selection = false;
	bool perfect_hash_inputs_all_valid = false;
	bool group_selection_all_present = false;
	int64_t *aggregate_int64_value = nullptr;
	hugeint_t *aggregate_hugeint_value = nullptr;
	double *aggregate_double_value = nullptr;
	int64_t **aggregate_int64_values = nullptr;
	hugeint_t **aggregate_hugeint_values = nullptr;
	double **aggregate_double_values = nullptr;
	hugeint_t aggregate_local_hugeint;
	bool *aggregate_state_is_set = nullptr;
	bool **aggregate_state_is_sets = nullptr;
	idx_t *aggregate_row_count = nullptr;
	idx_t **aggregate_row_counts = nullptr;
	const uintptr_t *aggregate_state_addresses = nullptr;
	const sel_t *aggregate_state_address_sel = nullptr;
	bool aggregate_state_addresses_by_loop_index = false;
	idx_t aggregate_state_offset = 0;
	idx_t aggregate_state_value_offset = 0;
	idx_t aggregate_state_is_set_offset = 0;
	data_ptr_t perfect_hash_state_data = nullptr;
	bool *perfect_hash_group_is_set = nullptr;
	idx_t perfect_hash_total_groups = 0;
	idx_t perfect_hash_tuple_size = 0;
	idx_t perfect_hash_aggregate_state_offset = 0;
	bool has_error = false;
	//! No source has a selection of its own: every one reads the logical row, and no row is NULL. It sits in the
	//! padding before error: a field that moves the hot fields above costs the generated loops cycles
	bool expression_tree_logical_all_valid = false;
	std::exception_ptr error;
	//! Groups whose aggregate states the perfect-hash update marked set on the group's first touch
	bool *perfect_hash_group_states_marked = nullptr;
};

struct SljitNativePrimitiveRunLaneInput {
	const_data_ptr_t payload_data = nullptr;
	const validity_t *payload_validity = nullptr;
	int64_t *output_int64_values = nullptr;
	hugeint_t *output_hugeint_values = nullptr;
	uint8_t *output_value_is_set = nullptr;
};

// Fixed-storage streaming ABI for generated consecutive-group run aggregation. The caller owns all buffers and may
// resume after flushing output by preserving input_offset and resetting output_count.
struct SljitNativePrimitiveRunInput {
	const_data_ptr_t group_data = nullptr;
	SljitNativePrimitiveRunLaneInput *lane_inputs = nullptr;

	// The tuned single-lane specialization binds these fields directly. Multi-lane
	// kernels use lane_inputs and share only output_row_counts across lanes.
	const_data_ptr_t payload_data = nullptr;
	const validity_t *payload_validity = nullptr;
	int64_t group_cast_constant = 0;
	data_ptr_t output_group_data = nullptr;
	int64_t *output_int64_values = nullptr;
	hugeint_t *output_hugeint_values = nullptr;
	uint8_t *output_value_is_set = nullptr;
	int64_t *output_shared_int64_values = nullptr;
	idx_t *output_shared_valid_counts = nullptr;
	idx_t *output_row_counts = nullptr;
	idx_t input_count = 0;
	idx_t input_offset = 0;
	idx_t output_count = 0;
	idx_t output_capacity = 0;
	uint8_t output_groups_strictly_increasing = 0;
};

struct SljitNativePreaggregatedPrimitiveLaneInput {
	const int64_t *int64_values = nullptr;
	const hugeint_t *hugeint_values = nullptr;
	const uint8_t *value_is_set = nullptr;
	idx_t state_offset = 0;
};

// Backend-private ABI for publishing preaggregated primitive deltas into
// core-owned aggregate states. The address array and semantic state offsets are
// supplied by the core; generated code never observes hash-table row layout.
struct SljitNativePreaggregatedPrimitiveUpdateInput {
	const uintptr_t *addresses = nullptr;
	const sel_t *address_sel = nullptr;
	const sel_t *execute_sel = nullptr;
	const SljitNativePreaggregatedPrimitiveLaneInput *lane_inputs = nullptr;
	idx_t count = 0;
};

struct SljitNativePredicateInput {
	const_data_ptr_t *source_data = nullptr;
	const sel_t **source_sel = nullptr;
	const validity_t **source_validity = nullptr;
	const sel_t *execute_sel = nullptr;
	data_ptr_t result_data = nullptr;
	validity_t *result_validity = nullptr;
	//! the matching rows' logical indices, when the predicate selects instead of materializing a result
	sel_t *true_sel = nullptr;
	idx_t selected_count = 0;
	idx_t count = 0;
	bool sources_all_valid = false;
	//! neither the rows (execute_sel) nor any source (source_sel) carries a selection
	bool rows_flat = false;
	//! A partial-SIMD predicate whose residual reads one dictionary source: the residual's result for each dictionary
	//! entry (1 passes; false and NULL do not), indexed through that source's selection; nullptr evaluates the residual
	//! row by row
	const uint8_t *residual_dictionary = nullptr;
	std::exception_ptr error;
};

struct SljitNativeRegularHashJoinProbeInput {
	const_data_ptr_t *source_data = nullptr;
	const sel_t **source_sel = nullptr;
	const validity_t **source_validity = nullptr;
	bool source_key0_int64_to_int32 = false;
	bool source_key0_int64_to_int32_unchecked = false;
	//! source_key0_int64_to_int32: the elided cast, which raises a failed narrowing's error
	SljitBoundIntegerCastChain source_key0_cast;
	idx_t count = 0;
	const uint64_t *entries = nullptr;
	uint64_t bitmask = 0;
	uint64_t bucket_shift = 0;
	uint64_t pointer_mask = 0;
	SljitHashJoinProbeLayoutKind layout_kind = SljitHashJoinProbeLayoutKind::NO_CHAIN;
	bool rhs_keys_have_validity = false;
	idx_t pointer_offset = 0;
	const data_ptr_t *aux_next_ptrs = nullptr;
	const uint64_t *bloom_filter_bits = nullptr;
	uint64_t bloom_filter_bitmask = 0;
	sel_t *match_sel = nullptr;
	data_ptr_t *row_pointers = nullptr;
	idx_t output_capacity = 0;
	idx_t selected_count = 0;
	idx_t input_offset = 0;
	data_ptr_t resume_row_pointer = nullptr;
	bool finished = false;
	std::exception_ptr error;
};

struct SljitNativePerfectHashJoinProbeInput {
	const_data_ptr_t source_data = nullptr;
	const sel_t *source_sel = nullptr;
	const validity_t *source_validity = nullptr;
	bool source_key0_int64_to_int32 = false;
	bool source_key0_int64_to_int32_unchecked = false;
	//! source_key0_int64_to_int32: the elided cast, which raises a failed narrowing's error
	SljitBoundIntegerCastChain source_key0_cast;
	idx_t count = 0;
	sel_t *match_sel = nullptr;
	sel_t *build_sel = nullptr;
	hugeint_t perfect_min_128;
	hugeint_t perfect_max_128;
	uhugeint_t perfect_min_u128;
	uhugeint_t perfect_max_u128;
	uint64_t perfect_min = 0;
	uint64_t perfect_max = 0;
	const validity_t *perfect_validity = nullptr;
	idx_t selected_count = 0;
	idx_t input_offset = 0;
	bool finished = false;
	std::exception_ptr error;
};

} // namespace duckdb
