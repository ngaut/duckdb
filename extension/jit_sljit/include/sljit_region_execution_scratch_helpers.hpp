//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_execution_scratch_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

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
	SljitDirectAggregateUpdateTracker(idx_t miss_limit_p, const char *scratch_name_p)
	    : miss_limit(miss_limit_p), scratch_name(scratch_name_p) {
	}

	void Resize(idx_t count) {
		disabled.resize(count);
		misses.resize(count);
	}

	bool Disabled(idx_t op_idx) const {
		return op_idx >= disabled.size() || disabled[op_idx];
	}

	void Record(idx_t op_idx, bool updated) {
		if (op_idx >= disabled.size() || op_idx >= misses.size()) {
			throw InternalException("SLJIT aggregate update has no %s scratch", scratch_name);
		}
		if (updated) {
			misses[op_idx] = 0;
			return;
		}
		if (++misses[op_idx] >= miss_limit) {
			disabled[op_idx] = true;
		}
	}

	vector<bool> disabled;
	vector<idx_t> misses;
	idx_t miss_limit;
	const char *scratch_name;
};

struct SljitAggregateUpdateScratchState {
	void Resize(idx_t count) {
		state_addresses.resize(count);
		preaggregated_groups.resize(count);
		preaggregated_group_slices.resize(count);
		preaggregated_row_pointers.resize(count);
		preaggregate_scratch.resize(count);
		preaggregate_scratch_slices.resize(count);
		payload_scratch.resize(count);
		payload_lanes.resize(count);
		payload_lanes_ready.resize(count);
		direct_new.Resize(count);
		direct_append_new.Resize(count);
		row_pointer_preaggregate.Resize(count);
	}

	Vector &StateAddresses(idx_t op_idx) {
		return SljitCheckedScratchPtr(state_addresses, op_idx,
		                              "SLJIT aggregate update has no grouped state-address scratch");
	}

	SljitAggregatePayloadAdapterScratch &PayloadScratch(idx_t op_idx) {
		return SljitCheckedScratchSlot(payload_scratch, op_idx,
		                               "SLJIT aggregate update has no payload-adapter scratch");
	}

	DataChunk &PreaggregatedGroups(idx_t op_idx) {
		return SljitCheckedScratchPtr(preaggregated_groups, op_idx,
		                              "SLJIT aggregate update has no preaggregated group scratch");
	}

	DataChunk &PreaggregatedGroupSlice(idx_t op_idx) {
		return SljitCheckedScratchPtr(preaggregated_group_slices, op_idx,
		                              "SLJIT aggregate update has no preaggregated group slice scratch");
	}

	Vector &PreaggregatedRowPointers(idx_t op_idx) {
		return SljitCheckedScratchPtr(preaggregated_row_pointers, op_idx,
		                              "SLJIT aggregate update has no preaggregated row-pointer scratch");
	}

	SljitPreaggregatedPrimitiveAggregateScratch &PreaggregateScratch(idx_t op_idx) {
		return SljitCheckedScratchSlot(preaggregate_scratch, op_idx,
		                               "SLJIT aggregate update has no preaggregate scratch");
	}

	SljitPreaggregatedPrimitiveAggregateScratch &PreaggregateScratchSlice(idx_t op_idx) {
		return SljitCheckedScratchSlot(preaggregate_scratch_slices, op_idx,
		                               "SLJIT aggregate update has no preaggregate slice scratch");
	}

	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &
	PayloadLanes(idx_t op_idx, const vector<ExecutionRegionAggregateInput> &aggregates,
	             const ExecutionPrimitiveAggregateUpdateBinding &primitive) {
		if (op_idx >= payload_lanes.size() || op_idx >= payload_lanes_ready.size()) {
			throw InternalException("SLJIT aggregate update has no payload-lane scratch");
		}
		if (payload_lanes_ready[op_idx]) {
			return payload_lanes[op_idx];
		}
		auto &lanes = payload_lanes[op_idx];
		lanes.assign(aggregates.size(), nullptr);
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto aggregate_index = aggregate.aggregate_index;
			auto lane = primitive.FindLane(aggregate_index);
			if (!lane) {
				throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregate_index));
			}
			lanes[payload_idx] = lane;
		}
		payload_lanes_ready[op_idx] = true;
		return lanes;
	}

	bool DirectNewDisabled(idx_t op_idx) const {
		return direct_new.Disabled(op_idx);
	}

	void RecordDirectNewResult(idx_t op_idx, bool updated) {
		direct_new.Record(op_idx, updated);
	}

	bool DirectAppendNewDisabled(idx_t op_idx) const {
		return direct_append_new.Disabled(op_idx);
	}

	void RecordDirectAppendNewResult(idx_t op_idx, bool updated) {
		direct_append_new.Record(op_idx, updated);
	}

	bool RowPointerPreaggregateDisabled(idx_t op_idx) const {
		return row_pointer_preaggregate.Disabled(op_idx);
	}

	void RecordRowPointerPreaggregateResult(idx_t op_idx, bool updated) {
		row_pointer_preaggregate.Record(op_idx, updated);
	}

	void Initialize(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		if (!op.aggregate_update.plan.use_grouped_state_addresses) {
			return;
		}
		state_addresses[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		preaggregated_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		auto &groups = op.aggregate_update.plan.sink_info.groups;
		if (groups.empty()) {
			return;
		}
		vector<LogicalType> group_types;
		group_types.reserve(groups.size());
		for (auto &group : groups) {
			group_types.push_back(group.type);
		}
		SljitInitializeScratchChunk(allocator, group_types, preaggregated_groups[op_idx]);
		SljitInitializeScratchChunk(allocator, group_types, preaggregated_group_slices[op_idx]);
	}

private:
	vector<unique_ptr<Vector>> state_addresses;
	vector<unique_ptr<DataChunk>> preaggregated_groups;
	vector<unique_ptr<DataChunk>> preaggregated_group_slices;
	vector<unique_ptr<Vector>> preaggregated_row_pointers;
	vector<SljitPreaggregatedPrimitiveAggregateScratch> preaggregate_scratch;
	vector<SljitPreaggregatedPrimitiveAggregateScratch> preaggregate_scratch_slices;
	vector<SljitAggregatePayloadAdapterScratch> payload_scratch;
	vector<vector<const ExecutionPrimitiveAggregateUpdateLane *>> payload_lanes;
	vector<bool> payload_lanes_ready;
	SljitDirectAggregateUpdateTracker direct_new {8, "direct-new"};
	SljitDirectAggregateUpdateTracker direct_append_new {2, "direct-append-new"};
	SljitDirectAggregateUpdateTracker row_pointer_preaggregate {8, "row-pointer-preaggregate"};
};

} // namespace duckdb
