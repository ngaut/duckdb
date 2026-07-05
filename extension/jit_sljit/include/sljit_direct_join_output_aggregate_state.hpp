//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_projection_aggregate_state.hpp"

#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch;

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

struct SljitDirectJoinOutputAggregateStrategy;

struct SljitDirectJoinOutputAggregatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	SljitDirectJoinOutputAggregateStrategy MakeStrategy() const;
};

struct SljitDirectJoinOutputAggregateStrategy {
	explicit SljitDirectJoinOutputAggregateStrategy(idx_t aggregate_idx_p) : aggregate_idx(aggregate_idx_p) {
	}

	idx_t aggregate_idx;
	bool disabled = false;
	optional_ptr<const vector<idx_t>> source_distinct_counts;
	optional_ptr<const vector<Value>> source_min_values;
	optional_ptr<const vector<Value>> source_max_values;
	string last_failure;
	SljitJoinProjectionAggregateDescriptor descriptor;
	SljitPendingRowPointerAggregateBatch pending_batch;
};

inline SljitDirectJoinOutputAggregateStrategy SljitDirectJoinOutputAggregatePrimitive::MakeStrategy() const {
	return SljitDirectJoinOutputAggregateStrategy(aggregate_idx);
}

static bool SljitAggregateUpdateHasDedicatedCompiledBackend(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return op.aggregate_update.plan.use_primitive_payloads;
}

static bool SljitCanBindDirectJoinOutputAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t aggregate_idx) {
	return aggregate_idx < ops.size() && SljitAggregateUpdateHasDedicatedCompiledBackend(ops[aggregate_idx]);
}

static SljitDirectJoinOutputAggregatePrimitive
SljitBindDirectJoinOutputAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	if (!SljitCanBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx)) {
		throw InternalException("SLJIT direct join output aggregate primitive cannot bind requested aggregate");
	}
	SljitDirectJoinOutputAggregatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	return primitive;
}

struct SljitDirectJoinOutputAggregatePolicy {
	SljitDirectJoinOutputAggregatePolicy() {
	}

	explicit SljitDirectJoinOutputAggregatePolicy(SljitDirectJoinOutputAggregateStrategy &strategy_p)
	    : strategy(&strategy_p) {
	}

	bool Enabled() const {
		return strategy != nullptr && !strategy->disabled;
	}

	bool HasStrategy() const {
		return strategy != nullptr;
	}

	SljitDirectJoinOutputAggregateStrategy &Strategy() {
		return *strategy;
	}

	optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy;
};

} // namespace duckdb
