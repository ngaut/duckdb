//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_execution_scratch_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

#include <array>

namespace duckdb {

struct SljitFlatHashJoinKeySourceDataDispatch {
	Vector &source;
	const_data_ptr_t data = nullptr;

	template <class T>
	void Execute() {
		data = reinterpret_cast<const_data_ptr_t>(FlatVector::GetData<T>(source));
	}
};

static const_data_ptr_t SljitFlatHashJoinKeySourceData(Vector &source, SljitNativeHashJoinKeyKind kind) {
	SljitFlatHashJoinKeySourceDataDispatch dispatch {source};
	if (!SljitDispatchHashJoinKeyKind(kind, dispatch)) {
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
	return dispatch.data;
}

struct SljitHashJoinProbeSourceScratch {
	void Resize(idx_t key_count) {
		sources.Resize(key_count);
	}

	bool Prepare(DataChunk &input, const SljitNativeHashJoinProbePlan &plan) {
		if (sources.Count() != plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe key scratch width mismatch");
		}
		bool source_key0_int64_to_int32 = false;
		for (idx_t key_idx = 0; key_idx < plan.keys.size(); key_idx++) {
			auto &key = plan.keys[key_idx];
			auto &source_vector = input.data[key.key_input_index];
			if (SljitHashJoinKeyCanUseInt64SourceForInt32Key(key_idx, key.key_kind,
			                                                 source_vector.GetType().InternalType())) {
				if (source_vector.GetVectorType() == VectorType::FLAT_VECTOR &&
				    sources.PrepareFlatSource(
				        input, key.key_input_index, key_idx,
				        reinterpret_cast<const_data_ptr_t>(FlatVector::GetData<int64_t>(source_vector)), input.size(),
				        "SLJIT native hash join probe key source is out of range", key.source_known_not_null)) {
					source_key0_int64_to_int32 = true;
					continue;
				}
				auto &format = sources.PrepareFormat(input, key.key_input_index, key_idx,
				                                     "SLJIT native hash join probe key source is out of range");
				sources.SetData(key_idx,
				                reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format)));
				source_key0_int64_to_int32 = true;
			} else {
				if (source_vector.GetVectorType() == VectorType::FLAT_VECTOR &&
				    sources.PrepareFlatSource(input, key.key_input_index, key_idx,
				                              SljitFlatHashJoinKeySourceData(source_vector, key.key_kind), input.size(),
				                              "SLJIT native hash join probe key source is out of range",
				                              key.source_known_not_null)) {
					continue;
				}
				auto &format = sources.PrepareFormat(input, key.key_input_index, key_idx,
				                                     "SLJIT native hash join probe key source is out of range");
				sources.SetData(key_idx, NativeHashJoinKeySourceData(format, key.key_kind));
			}
			sources.FinishSource(key_idx, nullptr, input.size(), key.source_known_not_null);
		}
		return source_key0_int64_to_int32;
	}

	const_data_ptr_t *DataArray() {
		return sources.DataArray();
	}

	const sel_t **SelectionArrayOrNull() {
		return sources.SelectionArrayOrNull();
	}

	const validity_t **ValidityArrayOrNull() {
		return sources.ValidityArrayOrNull();
	}

	bool HasCommonSelection() const {
		return sources.HasCommonSelection();
	}

private:
	SljitSourceVectorScratch sources;
};

struct SljitDirectAggregateUpdateTracker {
	explicit SljitDirectAggregateUpdateTracker(idx_t miss_limit_p) : miss_limit(miss_limit_p) {
	}

	bool Disabled() const {
		return disabled;
	}

	void Record(bool updated) {
		if (updated) {
			misses = 0;
			return;
		}
		if (++misses >= miss_limit) {
			disabled = true;
		}
	}

	bool disabled = false;
	idx_t misses = 0;
	idx_t miss_limit;
};

struct SljitPreaggregatedGroupContinuationState {
	void Clear() {
		ready = false;
		physical_type = PhysicalType::INVALID;
		state_address = 0;
	}

	bool ready = false;
	PhysicalType physical_type = PhysicalType::INVALID;
	std::array<uint8_t, sizeof(hugeint_t)> key {};
	uintptr_t state_address = 0;
};

enum class SljitBoundGroupedAggregateStrategy : uint8_t {
	UNBOUND,
	PERFECT_HASH_FUSED,
	GROUPED_STATE_FUSED,
	GROUPED_STATE_PER_PAYLOAD
};

struct SljitBoundGroupedPrimitiveAggregateUpdate {
	bool ready = false;
	idx_t op_idx = DConstants::INVALID_INDEX;
	optional_ptr<SljitExecutableRegionOp> op;
	optional_ptr<const vector<SljitAggregatePayloadDescriptor>> payload_descriptors;
	optional_ptr<const vector<const ExecutionPrimitiveAggregateUpdateLane *>> payload_lanes;
	optional_ptr<const vector<SljitGroupedReductionLaneBinding>> reduction_lanes;
	optional_ptr<SljitAggregatePayloadAdapterScratch> payload_scratch;
	optional_ptr<ExecutionGroupedAggregateStateAddressBinding> grouped_state;
	SljitBoundGroupedAggregateStrategy strategy = SljitBoundGroupedAggregateStrategy::UNBOUND;
};

struct SljitAggregateOperatorScratch {
	unique_ptr<Vector> state_addresses;
	unique_ptr<DataChunk> preaggregated_groups;
	unique_ptr<DataChunk> preaggregated_group_slice;
	unique_ptr<Vector> preaggregated_row_pointers;
	SljitPreaggregatedGroupContinuationState preaggregated_group_continuation;
	SljitPreaggregatedPrimitiveAggregateScratch preaggregate_scratch;
	SljitPreaggregatedPrimitiveAggregateScratch preaggregate_scratch_slice;
	SljitAggregatePayloadAdapterScratch payload_scratch;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> payload_lanes;
	bool payload_lanes_ready = false;
	vector<SljitGroupedReductionLaneBinding> grouped_reduction_lanes;
	bool grouped_reduction_lanes_ready = false;
	SljitDirectAggregateUpdateTracker direct_new {8};
	SljitDirectAggregateUpdateTracker direct_append_new {2};
	SljitDirectAggregateUpdateTracker row_pointer_preaggregate {8};
	SljitBoundGroupedPrimitiveAggregateUpdate bound_grouped_update;
};

struct SljitAggregateUpdateScratchState {
	void Resize(idx_t count) {
		operators.resize(count);
	}

	Vector &StateAddresses(idx_t op_idx) {
		auto &state = Operator(op_idx);
		if (!state.state_addresses) {
			throw InternalException("SLJIT aggregate update has no grouped state-address scratch");
		}
		return *state.state_addresses;
	}

	SljitAggregatePayloadAdapterScratch &PayloadScratch(idx_t op_idx) {
		return Operator(op_idx).payload_scratch;
	}

	SljitBoundGroupedPrimitiveAggregateUpdate &BoundGroupedUpdate(idx_t op_idx) {
		return Operator(op_idx).bound_grouped_update;
	}

	DataChunk &PreaggregatedGroups(idx_t op_idx) {
		auto &state = Operator(op_idx);
		if (!state.preaggregated_groups) {
			throw InternalException("SLJIT aggregate update has no preaggregated group scratch");
		}
		return *state.preaggregated_groups;
	}

	DataChunk &PreaggregatedGroupSlice(idx_t op_idx) {
		auto &state = Operator(op_idx);
		if (!state.preaggregated_group_slice) {
			throw InternalException("SLJIT aggregate update has no preaggregated group slice scratch");
		}
		return *state.preaggregated_group_slice;
	}

	Vector &PreaggregatedRowPointers(idx_t op_idx) {
		auto &state = Operator(op_idx);
		if (!state.preaggregated_row_pointers) {
			throw InternalException("SLJIT aggregate update has no preaggregated row-pointer scratch");
		}
		return *state.preaggregated_row_pointers;
	}

	SljitPreaggregatedGroupContinuationState &PreaggregatedGroupContinuation(idx_t op_idx) {
		return Operator(op_idx).preaggregated_group_continuation;
	}

	SljitPreaggregatedPrimitiveAggregateScratch &PreaggregateScratch(idx_t op_idx) {
		return Operator(op_idx).preaggregate_scratch;
	}

	SljitPreaggregatedPrimitiveAggregateScratch &PreaggregateScratchSlice(idx_t op_idx) {
		return Operator(op_idx).preaggregate_scratch_slice;
	}

	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &
	PayloadLanes(idx_t op_idx, const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
	             const ExecutionPrimitiveAggregateUpdateBinding &primitive) {
		auto &state = Operator(op_idx);
		if (state.payload_lanes_ready) {
			return state.payload_lanes;
		}
		auto &lanes = state.payload_lanes;
		lanes.assign(payload_descriptors.size(), nullptr);
		for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
			auto aggregate_index = payload_descriptors[payload_idx].aggregate_index;
			auto lane = primitive.FindLane(aggregate_index);
			if (!lane) {
				throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregate_index));
			}
			lanes[payload_idx] = lane;
		}
		state.payload_lanes_ready = true;
		return lanes;
	}

	const vector<SljitGroupedReductionLaneBinding> &
	GroupedReductionLanes(idx_t op_idx, const ExecutionRegionAggregateContract &contract,
	                      const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
	                      const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		auto &state = Operator(op_idx);
		if (state.grouped_reduction_lanes_ready) {
			return state.grouped_reduction_lanes;
		}
		auto &lanes = state.grouped_reduction_lanes;
		if (!SljitTryBindGroupedReductionLanes(contract, payload_descriptors, payload_lanes, lanes)) {
			throw InternalException("SLJIT grouped aggregate reduction lane binding failed");
		}
		state.grouped_reduction_lanes_ready = true;
		return lanes;
	}

	bool DirectNewDisabled(idx_t op_idx) const {
		return Operator(op_idx).direct_new.Disabled();
	}

	void RecordDirectNewResult(idx_t op_idx, bool updated) {
		Operator(op_idx).direct_new.Record(updated);
	}

	bool DirectAppendNewDisabled(idx_t op_idx) const {
		return Operator(op_idx).direct_append_new.Disabled();
	}

	void RecordDirectAppendNewResult(idx_t op_idx, bool updated) {
		Operator(op_idx).direct_append_new.Record(updated);
	}

	bool RowPointerPreaggregateDisabled(idx_t op_idx) const {
		return Operator(op_idx).row_pointer_preaggregate.Disabled();
	}

	void RecordRowPointerPreaggregateResult(idx_t op_idx, bool updated) {
		Operator(op_idx).row_pointer_preaggregate.Record(updated);
	}

	void Initialize(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		if (op_idx >= operators.size() || operators[op_idx]) {
			throw InternalException("SLJIT aggregate update scratch initialized more than once");
		}
		operators[op_idx] = make_uniq<SljitAggregateOperatorScratch>();
		if (!op.aggregate_update.plan.use_grouped_state_addresses) {
			return;
		}
		auto &state = *operators[op_idx];
		state.state_addresses = make_uniq<Vector>(LogicalType::POINTER);
		state.preaggregated_row_pointers = make_uniq<Vector>(LogicalType::POINTER);
		auto &groups = op.aggregate_update.plan.sink_info.groups;
		if (groups.empty()) {
			return;
		}
		vector<LogicalType> group_types;
		group_types.reserve(groups.size());
		for (auto &group : groups) {
			group_types.push_back(group.type);
		}
		SljitInitializeScratchChunk(allocator, group_types, state.preaggregated_groups);
		SljitInitializeScratchChunk(allocator, group_types, state.preaggregated_group_slice);
	}

private:
	SljitAggregateOperatorScratch &Operator(idx_t op_idx) {
		if (op_idx >= operators.size() || !operators[op_idx]) {
			throw InternalException("SLJIT aggregate update has no operator scratch");
		}
		return *operators[op_idx];
	}

	const SljitAggregateOperatorScratch &Operator(idx_t op_idx) const {
		if (op_idx >= operators.size() || !operators[op_idx]) {
			throw InternalException("SLJIT aggregate update has no operator scratch");
		}
		return *operators[op_idx];
	}

	vector<unique_ptr<SljitAggregateOperatorScratch>> operators;
};

} // namespace duckdb
