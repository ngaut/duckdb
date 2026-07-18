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
struct BloomFilterFunctionData;
struct ExecutionPerfectHashJoinFilterLayout;
struct UnifiedVectorFormat;
class Vector;

//! Backend-neutral executable selection state for one immutable table filter.
//! The storage scan owns invocation and selection semantics; an accelerator
//! backend only supplies the predicate implementation.
class DUCKDB_API TableFilterKernelState {
public:
	virtual ~TableFilterKernelState();

	//! Returns true when the kernel executed. A false result asks the storage
	//! scan to use its canonical ExpressionExecutor fallback.
	virtual bool TrySelect(Vector &vector, UnifiedVectorFormat &format, SelectionVector &selection, idx_t scan_count,
	                       idx_t &approved_tuple_count) = 0;
};

//! Query-plan-owned provider for thread-local table-filter kernels.
class DUCKDB_API TableFilterKernelProvider {
public:
	virtual ~TableFilterKernelProvider();

	virtual bool HasTableFilterKernels() const;
	virtual bool HasTableFilterKernel(idx_t filter_index) const;
	virtual unique_ptr<TableFilterKernelState> CreateTableFilterKernelState(idx_t filter_index) const;
};

//! Thread-local execution policy for an optional filter. The wrapper owns this policy; internal filter primitives
//! only implement matching.
struct FilterSelectivityState {
	enum class Status : uint8_t { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	FilterSelectivityState(idx_t n_vectors_to_check, float selectivity_threshold);

	void Update(idx_t accepted, idx_t processed);
	bool IsActive() const;
	double GetSelectivity() const;

	const idx_t n_vectors_to_check;
	const float selectivity_threshold;
	idx_t tuples_accepted;
	idx_t tuples_processed;
	idx_t vectors_processed;
	Status status;
	idx_t pause_multiplier;
};

enum class FastInternalFilterOperationType : uint8_t {
	SIGNED_NUMERIC_RANGE,
	PREFIX_RANGE,
	PERFECT_HASH_JOIN,
	BLOOM_FILTER
};

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
	optional_ptr<const BloomFilterFunctionData> bloom_filter_data;
	unique_ptr<FilterSelectivityState> selectivity;
};

//! Immutable storage-scan dispatch derived from the analyzed filter operations.
//! ExpressionFilterState has the same thread-local lifetime as the immutable
//! table filter, so compression functions can consume this without rebinding
//! operator-lifetime layout and fusion invariants for every vector.
struct FastInternalFilterScanPlan {
	idx_t primary_operation_count = 0;
	optional_ptr<const ExecutionPerfectHashJoinFilterLayout> perfect_hash_join_layout;
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
	unique_ptr<TableFilterKernelState> kernel;
	bool fast_string_equality_filter_initialized = false;
	bool fast_string_equality_filter_supported = false;
	vector<string> fast_string_equality_constants;
	vector<uint8_t> fast_dictionary_matches;
	//! The cached bitmap is keyed by the dictionary's stable identity, never by the
	//! buffer address: a recycled allocation with an equal entry count would
	//! otherwise replay a stale bitmap against a different dictionary's layout.
	string fast_dictionary_matches_dictionary_id;
	idx_t fast_dictionary_matches_count = 0;

	bool fast_internal_filter_initialized = false;
	bool fast_internal_filter_supported = false;
	PhysicalType fast_internal_filter_type = PhysicalType::INVALID;
	vector<FastInternalFilterOperation> fast_internal_filter_operations;
	FastInternalFilterScanPlan fast_internal_filter_scan_plan;
	unique_ptr<Expression> fast_internal_filter_residual_expression;
	unique_ptr<ExpressionExecutor> fast_internal_filter_residual_executor;

	bool fast_signed_numeric_range_filter_initialized = false;
	bool fast_signed_numeric_range_filter_supported = false;
	bool fast_signed_numeric_range_filter_always_false = false;
	bool fast_signed_numeric_range_has_lower = false;
	bool fast_signed_numeric_range_has_upper = false;
	int64_t fast_signed_numeric_range_lower = 0;
	int64_t fast_signed_numeric_range_upper = 0;
	PhysicalType fast_signed_numeric_range_type = PhysicalType::INVALID;

	bool constant_filter_null_effect_initialized = false;
	bool constant_filter_filters_nulls = false;
	bool constant_filter_filters_valid_values = false;

	SelectionVector fast_filter_sel;
};

} // namespace duckdb
