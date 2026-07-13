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

struct SljitJoinInputRowPointerComplementarySumPlan {
	SljitDeferredBuildState build_state;
	SljitStringSetComplementarySumDescriptor classification;
	SljitComplementarySumRHSField predicate_field;
	idx_t group_input_vector_idx = DConstants::INVALID_INDEX;
	idx_t join_input_group_column_idx = DConstants::INVALID_INDEX;
	LogicalType join_input_group_type;
	bool pipeline_accumulator_checked = false;
	bool pipeline_accumulator_enabled = false;
	bool blocker_recorded = false;
};

struct SljitJoinInputRowPointerComplementarySumAccumulator {
	explicit SljitJoinInputRowPointerComplementarySumAccumulator(PhysicalType physical_type_p)
	    : physical_type(physical_type_p) {
	}
	virtual ~SljitJoinInputRowPointerComplementarySumAccumulator() = default;
	virtual bool Empty() const = 0;
	virtual void Reset() = 0;

	PhysicalType physical_type;
};

struct SljitPendingRowPointerAggregateBatch {
	SljitPendingRowPointerAggregateBatch() : row_pointers(LogicalType::POINTER) {
	}

	idx_t Count() const {
		if (!initialized) {
			return 0;
		}
		return uses_preclassified_input ? preclassified_input.size() : input.size();
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &input_types) {
		EnsureRowPointers();
		if (!input_initialized) {
			input.Initialize(allocator, input_types);
			input_initialized = true;
		}
		uses_preclassified_input = false;
		initialized = true;
	}

	void EnsurePreclassified(Allocator &allocator) {
		EnsureRowPointers();
		if (!preclassified_input_initialized) {
			vector<LogicalType> input_types {LogicalType::UTINYINT};
			preclassified_input.Initialize(allocator, input_types);
			preclassified_input_initialized = true;
		}
		uses_preclassified_input = true;
		initialized = true;
	}

	void Reset() {
		if (!initialized) {
			return;
		}
		if (input_initialized) {
			input.Reset();
		}
		if (preclassified_input_initialized) {
			preclassified_input.Reset();
		}
		FlatVector::SetSize(row_pointers, 0);
		uses_preclassified_input = false;
		source_key0_int64_to_int32_unchecked = false;
	}

	DataChunk &PayloadInput() {
		return uses_preclassified_input ? preclassified_input : input;
	}

private:
	void EnsureRowPointers() {
		if (row_pointers_initialized) {
			return;
		}
		row_pointers.Initialize(VectorDataInitialization::UNINITIALIZED, STANDARD_VECTOR_SIZE);
		FlatVector::SetSize(row_pointers, 0);
		row_pointers_initialized = true;
	}

public:
	DataChunk input;
	DataChunk preclassified_input;
	Vector row_pointers;
	bool initialized = false;
	bool input_initialized = false;
	bool preclassified_input_initialized = false;
	bool row_pointers_initialized = false;
	bool uses_preclassified_input = false;
	bool source_key0_int64_to_int32_unchecked = false;
	optional_ptr<SljitRegionExecutionScratch> scratch;
	optional_ptr<bool> deferred_grouped_finish;
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
	optional_ptr<bool> deferred_grouped_finish;
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
	optional_ptr<bool> pending_preaggregated_deferred_grouped_finish;
	SljitPendingRowPointerAggregateBatch pending_batch;
	SljitJoinInputRowPointerComplementarySumPlan join_input_complementary_sum_plan;
	unique_ptr<SljitJoinInputRowPointerComplementarySumAccumulator> join_input_complementary_sum_accumulator;
	optional_ptr<SljitRegionExecutionScratch> join_input_complementary_sum_scratch;
	optional_ptr<bool> join_input_complementary_sum_deferred_grouped_finish;
	vector<idx_t> string_set_classification_payload_sources;
	SljitStringSetComplementarySumDescriptor string_set_classification;
	bool string_set_classification_checked = false;
	bool string_set_classification_ready = false;
};

} // namespace duckdb
