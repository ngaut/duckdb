//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/radix_partitioned_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/execution/progress_data.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb {

class GroupedAggregateHashTable;
class TupleDataRowLocationRemap;
struct ExecutionPrimitiveAggregateUpdateLane;
struct ExecutionRegionSinkInfo;
struct ExecutionOperatorStageRecorder;
struct AggregatePartition;

class RadixPartitionedHashTable {
public:
	RadixPartitionedHashTable(GroupingSet &grouping_set, const GroupedAggregateData &op,
	                          TupleDataValidityType group_validity);
	unique_ptr<GroupedAggregateHashTable> CreateHT(ClientContext &context, const idx_t capacity,
	                                               const idx_t radix_bits) const;

public:
	GroupingSet &grouping_set;
	//! The indices specified in the groups_count that do not appear in the grouping_set
	unsafe_vector<idx_t> null_groups;
	const GroupedAggregateData &op;
	vector<LogicalType> group_types;
	//! The GROUPING values that belong to this hash table
	vector<Value> grouping_values;
	//! Whether there are no NULLs in the groups
	const TupleDataValidityType group_validity;

public:
	//! Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	void ResetGlobalSinkState(ClientContext &context, GlobalSinkState &gstate) const;
	void ResetLocalSinkState(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const;

	void Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, DataChunk &aggregate_input_chunk,
	          const unsafe_vector<idx_t> &filter) const;
	void SinkSelected(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                  DataChunk &aggregate_input_chunk, const unsafe_vector<idx_t> &filter,
	                  const SelectionVector &selection, idx_t count) const;
	bool TrySinkGroupsFast(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                       const SelectionVector *selection = nullptr, idx_t count = DConstants::INVALID_INDEX,
	                       optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                       idx_t estimated_input_count = 0, idx_t distinct_key_cardinality_upper_bound = 0) const;
	void ResolveStateAddresses(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                           Vector &addresses_out, optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                           bool prefer_fast_lookup = false) const;
	bool ReserveGroups(ExecutionContext &context, OperatorSinkInput &input, idx_t group_count,
	                   optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) const;
	bool TryUpdateNewPrimitiveGroups(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                                 const ExecutionRegionSinkInfo &sink_info,
	                                 const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                 optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                 bool finish = true,
	                                 optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryUpdateNewPrimitiveGroupsWithPayloadInput(
	    ExecutionContext &context, DataChunk &groups, DataChunk &payload_input,
	    const vector<idx_t> &payload_source_indices, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryAppendNewPrimitiveGroups(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                                 const ExecutionRegionSinkInfo &sink_info,
	                                 const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                 optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                 bool finish = true,
	                                 optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryUpdateNewGroupsWithSelectedStateAddresses(
	    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryUpdateGroupKeysWithSelectedStateAddresses(
	    ExecutionContext &context, DataChunk &groups, OperatorSinkInput &input,
	    const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryFindOrCreateRowPointerGroupStateTargets(
	    ExecutionContext &context, DataChunk &payload_input, Vector &row_pointers, idx_t count,
	    OperatorSinkInput &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	    const ExecutionRegionSinkInfo &sink_info, ExecutionGroupedAggregateStateTargetBatch &targets,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) const;
	bool TryFindOrCreateInputVectorGroupStateTargets(
	    ExecutionContext &context, DataChunk &payload_input, idx_t count, OperatorSinkInput &input,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateTargetBatch &targets,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryUpdateInputVectorGroupCountOne(ExecutionContext &context, DataChunk &payload_input, idx_t count,
	                                       OperatorSinkInput &input,
	                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	                                       const ExecutionRegionSinkInfo &sink_info,
	                                       const ExecutionPrimitiveAggregateUpdateLane &lane,
	                                       optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                       bool finish = true,
	                                       optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryUpdateRowPointerGroupPrimitivePayloads(ExecutionContext &context, DataChunk &payload_input,
	                                               Vector &row_pointers, idx_t count,
	                                               const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	                                               const vector<idx_t> &payload_source_indices,
	                                               OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
	                                               const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                               optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                               bool finish = true) const;
	bool TryAppendNewGroupsWithStateAddresses(
	    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryAppendNewGroupKeysWithStateAddresses(
	    ExecutionContext &context, DataChunk &groups, OperatorSinkInput &input,
	    const ExecutionRegionSinkInfo &sink_info, ExecutionGroupedAggregateStateAddressUpdateFunction update_function,
	    void *update_state, optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) const;
	bool TryResolveNewGroupAddresses(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
	                                 const ExecutionRegionSinkInfo &sink_info, Vector &addresses_out,
	                                 optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                 bool finish = true) const;
	void RecordDirectStateAddressUpdates(ExecutionContext &context, OperatorSinkInput &input, idx_t count) const;
	void FinishStateUpdates(ExecutionContext &context, OperatorSinkInput &input,
	                        optional_ptr<TupleDataRowLocationRemap> row_location_remap = nullptr) const;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
	             optional_ptr<TupleDataRowLocationRemap> row_location_remap = nullptr) const;
	void Finalize(ClientContext &context, GlobalSinkState &gstate,
	              optional_ptr<TupleDataRowLocationRemap> state_remap = nullptr) const;

public:
	//! Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const;
	void ResetGlobalSourceState(ClientContext &context, GlobalSourceState &gstate) const;
	void ResetLocalSourceState(ExecutionContext &context, LocalSourceState &lstate) const;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, GlobalSinkState &sink,
	                         OperatorSourceInput &input) const;

	ProgressData GetProgress(ClientContext &context, GlobalSinkState &sink_p, GlobalSourceState &gstate) const;
	optional_idx FinalizedCount(GlobalSinkState &sink) const;

	shared_ptr<TupleDataLayout> GetLayoutPtr() const;
	const TupleDataLayout &GetLayout() const;
	idx_t MaxThreads(GlobalSinkState &sink) const;
	static void SetMultiScan(GlobalSinkState &sink);

private:
	void SetGroupingValues();
	void PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk) const;
	void PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk, const SelectionVector &selection,
	                        idx_t count) const;

	shared_ptr<TupleDataLayout> layout_ptr;
};

} // namespace duckdb
