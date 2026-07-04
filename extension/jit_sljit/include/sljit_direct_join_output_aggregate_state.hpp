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
		return initialized ? input.size() : 0;
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &input_types) {
		if (initialized) {
			return;
		}
		input.Initialize(allocator, input_types);
		row_pointers.Initialize(VectorDataInitialization::UNINITIALIZED, STANDARD_VECTOR_SIZE);
		FlatVector::SetSize(row_pointers, 0);
		initialized = true;
	}

	void Reset() {
		if (!initialized) {
			return;
		}
		input.Reset();
		FlatVector::SetSize(row_pointers, 0);
		source_key0_int64_to_int32_unchecked = false;
	}

	DataChunk input;
	Vector row_pointers;
	bool initialized = false;
	bool source_key0_int64_to_int32_unchecked = false;
	optional_ptr<SljitRegionExecutionScratch> scratch;
	optional_ptr<bool> deferred_grouped_finish;
};

enum class SljitDirectJoinOutputAggregateUpdateSchedule : uint8_t {
	PENDING_ROW_POINTER_BATCH,
	IMMEDIATE_ROW_POINTER_UPDATE
};

struct SljitDirectJoinOutputAggregateStrategy;

struct SljitDirectJoinOutputAggregatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	SljitDirectJoinOutputAggregateUpdateSchedule update_schedule =
	    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH;

	SljitDirectJoinOutputAggregateStrategy MakeStrategy() const;
};

struct SljitDirectJoinOutputAggregateStrategy {
	SljitDirectJoinOutputAggregateStrategy(idx_t aggregate_idx_p,
	                                       SljitDirectJoinOutputAggregateUpdateSchedule update_schedule_p)
	    : aggregate_idx(aggregate_idx_p), update_schedule(update_schedule_p) {
	}

	bool UsesPendingBatch() const {
		return update_schedule == SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH;
	}

	idx_t aggregate_idx;
	SljitDirectJoinOutputAggregateUpdateSchedule update_schedule;
	bool disabled = false;
	optional_ptr<const vector<idx_t>> source_distinct_counts;
	optional_ptr<const vector<Value>> source_min_values;
	optional_ptr<const vector<Value>> source_max_values;
	string last_failure;
	SljitJoinProjectionAggregateDescriptor descriptor;
	SljitPendingRowPointerAggregateBatch pending_batch;
};

inline SljitDirectJoinOutputAggregateStrategy SljitDirectJoinOutputAggregatePrimitive::MakeStrategy() const {
	return SljitDirectJoinOutputAggregateStrategy(aggregate_idx, update_schedule);
}

static bool SljitAggregateUpdateHasDedicatedCompiledBackend(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	if (op.aggregate_update.plan.sink_info.aggregate_contract.distinct_count_pointer_keys) {
		return true;
	}
	return op.aggregate_update.plan.use_primitive_payloads;
}

static bool SljitAggregateUpdateCanUseSelectedJoinPerfectHashBackend(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	return plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	       plan.use_primitive_payloads && plan.use_perfect_hash_group_lookup &&
	       aggregate_update.fused_payload_update_owns_group_lookup &&
	       aggregate_update.fused_payload_update_function != nullptr;
}

static bool SljitCanBindDirectJoinOutputAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t aggregate_idx) {
	return aggregate_idx < ops.size() && SljitAggregateUpdateHasDedicatedCompiledBackend(ops[aggregate_idx]);
}

static SljitDirectJoinOutputAggregatePrimitive
SljitBindDirectJoinOutputAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx,
                                            SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) {
	if (!SljitCanBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx)) {
		throw InternalException("SLJIT direct join output aggregate primitive cannot bind requested aggregate");
	}
	SljitDirectJoinOutputAggregatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	primitive.update_schedule = update_schedule;
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

	bool UsesPendingBatch() const {
		return strategy != nullptr && strategy->UsesPendingBatch();
	}

	SljitDirectJoinOutputAggregateStrategy &Strategy() {
		return *strategy;
	}

	optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy;
};

} // namespace duckdb
