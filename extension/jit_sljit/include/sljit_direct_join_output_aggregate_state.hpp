//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_string_set_complementary_sum_runtime.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch;
struct SljitPendingPreaggregatedPrimitiveGroupBatch;

struct SljitComplementarySumRHSField {
	ExecutionHashJoinRHSFixedColumnSource source;
	idx_t compressed_size = 0;
	std::array<std::array<data_t, sizeof(uhugeint_t)>, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT>
	    compressed_constants {};
};

enum class SljitComplementarySumPredicateStorage : uint8_t { REGULAR_ROW_POINTER, PERFECT_HASH_DICTIONARY };

struct SljitJoinInputRowPointerComplementarySumPlan {
	SljitDeferredBuildState build_state;
	SljitStringSetComplementarySumDescriptor classification;
	SljitComplementarySumPredicateStorage predicate_storage =
	    SljitComplementarySumPredicateStorage::REGULAR_ROW_POINTER;
	SljitComplementarySumRHSField predicate_field;
	idx_t perfect_hash_rhs_output_idx = DConstants::INVALID_INDEX;
	idx_t group_input_vector_idx = DConstants::INVALID_INDEX;
	idx_t join_input_group_column_idx = DConstants::INVALID_INDEX;
	LogicalType join_input_group_type;
	bool pipeline_accumulator_checked = false;
	bool pipeline_accumulator_enabled = false;
	bool blocker_recorded = false;
};

struct SljitJoinInputRowPointerComplementarySumAccumulator {
	explicit SljitJoinInputRowPointerComplementarySumAccumulator(PhysicalType physical_type_p,
	                                                             bool predicate_all_valid_p = false)
	    : physical_type(physical_type_p), predicate_all_valid(predicate_all_valid_p) {
	}
	virtual ~SljitJoinInputRowPointerComplementarySumAccumulator() = default;
	virtual bool Empty() const = 0;
	virtual void Reset() = 0;

	PhysicalType physical_type;
	bool predicate_all_valid;
};

//! Rows a direct join-output aggregate collects with their group row pointers until they update the aggregate. The
//! columns use one of two layouts, each its own type: the descriptor's payload sources, or one string-set class per row
//! in place of those sources. A consumer of one layout never receives the other
template <class LAYOUT>
struct SljitPendingRowPointerBatch {
	SljitPendingRowPointerBatch() : row_pointers(LogicalType::POINTER) {
	}

	idx_t Count() const {
		return initialized ? columns.size() : 0;
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &descriptor_input_types) {
		if (!initialized) {
			columns.Initialize(allocator, LAYOUT::ColumnTypes(descriptor_input_types));
			row_pointers.Initialize(VectorDataInitialization::UNINITIALIZED, STANDARD_VECTOR_SIZE);
			FlatVector::SetSize(row_pointers, 0);
			initialized = true;
		}
	}

	void Reset() {
		if (!initialized) {
			return;
		}
		columns.Reset();
		FlatVector::SetSize(row_pointers, 0);
		source_key0_int64_to_int32_unchecked = false;
	}

	DataChunk columns;
	Vector row_pointers;
	bool initialized = false;
	bool source_key0_int64_to_int32_unchecked = false;
	optional_ptr<SljitRegionExecutionScratch> scratch;
};

//! The descriptor's payload sources, as the aggregate input slots hold them
struct SljitRowPointerPayloadLayout {
	static vector<LogicalType> ColumnTypes(const vector<LogicalType> &descriptor_input_types) {
		return descriptor_input_types;
	}
};

//! One class per row: whether the row's string matches the complementary sum's constants (NULL for a NULL string)
struct SljitRowPointerStringSetClassLayout {
	static vector<LogicalType> ColumnTypes(const vector<LogicalType> &) {
		return {LogicalType::UTINYINT};
	}
};

using SljitPendingRowPointerPayloadBatch = SljitPendingRowPointerBatch<SljitRowPointerPayloadLayout>;
using SljitPendingRowPointerClassBatch = SljitPendingRowPointerBatch<SljitRowPointerStringSetClassLayout>;

//! A strategy's pending row-pointer rows: at most one of the two batches holds rows
struct SljitPendingRowPointerAggregateBatches {
	SljitPendingRowPointerPayloadBatch payload;
	SljitPendingRowPointerClassBatch classes;
};

struct SljitPendingInputVectorAggregateBatch {
	idx_t Count() const {
		return initialized ? input.size() : 0;
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &input_types) {
		if (!initialized) {
			input.Initialize(allocator, input_types);
			initialized = true;
		}
	}

	void Reset() {
		if (!initialized) {
			return;
		}
		input.Reset();
		source_key0_int64_to_int32_unchecked = false;
	}

	DataChunk input;
	bool initialized = false;
	bool source_key0_int64_to_int32_unchecked = false;
	optional_ptr<SljitRegionExecutionScratch> scratch;
};

struct SljitDirectJoinOutputAggregateStrategy {
	SljitDirectJoinOutputAggregateStrategy(idx_t aggregate_idx_p, const vector<idx_t> &source_distinct_counts_p,
	                                       const vector<Value> &source_min_values_p,
	                                       const vector<Value> &source_max_values_p)
	    : aggregate_idx(aggregate_idx_p), source_distinct_counts(&source_distinct_counts_p),
	      source_min_values(&source_min_values_p), source_max_values(&source_max_values_p) {
	}

	idx_t aggregate_idx;
	bool disabled = false;
	optional_ptr<const vector<idx_t>> source_distinct_counts;
	optional_ptr<const vector<Value>> source_min_values;
	optional_ptr<const vector<Value>> source_max_values;
	string last_failure;
	SljitJoinProjectionAggregateDescriptor descriptor;
	SljitPendingInputVectorAggregateBatch pending_input_vector_batch;
	shared_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch> pending_preaggregated_groups;
	optional_ptr<SljitRegionExecutionScratch> pending_preaggregated_scratch;
	SljitPendingRowPointerAggregateBatches pending_batch;
	SljitJoinInputRowPointerComplementarySumPlan join_input_complementary_sum_plan;
	unique_ptr<SljitJoinInputRowPointerComplementarySumAccumulator> join_input_complementary_sum_accumulator;
	optional_ptr<SljitRegionExecutionScratch> join_input_complementary_sum_scratch;
	vector<idx_t> string_set_classification_payload_sources;
	SljitStringSetComplementarySumDescriptor string_set_classification;
	bool string_set_classification_checked = false;
	bool string_set_classification_ready = false;
};

} // namespace duckdb
