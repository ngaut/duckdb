//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct PrefixRangeFunctionData;
struct PerfectHashJoinFunctionData;

enum class FastInternalFilterOperationType : uint8_t { SIGNED_NUMERIC_RANGE, PREFIX_RANGE, PERFECT_HASH_JOIN };

//! One analyzed operation in a fully supported internal expression filter. The
//! expression tree is immutable for the lifetime of the state, so retaining the
//! bound function data avoids rediscovering the same shape for every vector.
struct FastInternalFilterOperation {
	FastInternalFilterOperationType type;

	bool range_empty = false;
	bool range_has_lower = false;
	bool range_has_upper = false;
	int64_t range_lower = 0;
	int64_t range_upper = 0;
	optional_ptr<const PrefixRangeFunctionData> prefix_range_data;
	optional_ptr<const PerfectHashJoinFunctionData> perfect_hash_join_data;
};

//! Thread-local state for executing a table filter
struct TableFilterState {
public:
	virtual ~TableFilterState() = default;

public:
	static unique_ptr<TableFilterState> Initialize(ClientContext &context, const TableFilter &filter);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	ClientContext &GetContext() {
		D_ASSERT(executor);
		return executor->GetContext();
	}

	unique_ptr<ExpressionExecutor> executor;
	bool fast_string_equality_filter_initialized = false;
	bool fast_string_equality_filter_supported = false;
	vector<string> fast_string_equality_constants;
	vector<uint8_t> fast_dictionary_matches;
	const void *fast_dictionary_matches_entry = nullptr;
	idx_t fast_dictionary_matches_count = 0;

	bool exact_prefilter_residual_initialized = false;
	bool exact_prefilter_present = false;
	unique_ptr<Expression> exact_prefilter_residual_expression;
	unique_ptr<ExpressionExecutor> exact_prefilter_residual_executor;

	bool fast_internal_filter_initialized = false;
	bool fast_internal_filter_supported = false;
	PhysicalType fast_internal_filter_type = PhysicalType::INVALID;
	vector<FastInternalFilterOperation> fast_internal_filter_operations;

	bool fast_signed_numeric_range_filter_initialized = false;
	bool fast_signed_numeric_range_filter_supported = false;
	bool fast_signed_numeric_range_filter_always_false = false;
	bool fast_signed_numeric_range_has_lower = false;
	bool fast_signed_numeric_range_has_upper = false;
	int64_t fast_signed_numeric_range_lower = 0;
	int64_t fast_signed_numeric_range_upper = 0;
	PhysicalType fast_signed_numeric_range_type = PhysicalType::INVALID;

	bool fast_prefix_range_filter_initialized = false;
	bool fast_prefix_range_filter_supported = false;
	bool fast_prefix_range_filter_covers_filter = false;
	const PrefixRangeFunctionData *fast_prefix_range_filter_data = nullptr;
	bool fast_prefix_range_residual_coverage_initialized = false;
	bool fast_prefix_range_residual_covers_filter = false;

	bool constant_filter_null_effect_initialized = false;
	bool constant_filter_filters_nulls = false;
	bool constant_filter_filters_valid_values = false;

	SelectionVector fast_filter_sel;
};

} // namespace duckdb
