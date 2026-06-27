//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_operator_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/function/aggregate_primitive_update.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <chrono>

namespace duckdb {

class ColumnDataCollection;
class DataChunk;
class GlobalSinkState;
class InterruptState;
class LocalSinkState;
class Vector;
class Allocator;
class SelectionVector;
struct ColumnDataScanState;
struct ExecutionContext;
struct OperatorSinkInput;
struct PartitionedTupleDataAppendState;
struct JoinFilterLocalState;
struct JoinFilterPushdownInfo;

enum class ExecutionOperatorBindResult : uint8_t { READY, DEFERRED, INVALID };
enum class ExecutionOperatorReadinessStatus : uint8_t { READY, NOT_READY, INVALID };

struct ExecutionOperatorReadiness {
	ExecutionOperatorReadinessStatus status = ExecutionOperatorReadinessStatus::INVALID;
	ExecutionRegionOperatorContractKind kind = ExecutionRegionOperatorContractKind::NONE;
	string blocker;

	bool Ready() const {
		return status == ExecutionOperatorReadinessStatus::READY;
	}
};

struct ExecutionPrimitiveAggregateUpdateLane;

struct ExecutionAppendSinkState {
	virtual ~ExecutionAppendSinkState() {
	}
	virtual SinkResultType Append(DataChunk &input) = 0;
	virtual ExecutionOperatorBindResult PrepareDirectAppend(const vector<LogicalType> &types, idx_t count,
	                                                        DirectAppendReservation &reservation, string &blocker,
	                                                        optional_ptr<DirectAppendProfile> profile = nullptr);
	virtual SinkResultType CommitDirectAppend(const DirectAppendReservation &reservation,
	                                          optional_ptr<DirectAppendProfile> profile = nullptr);
};

struct ExecutionOrderedSinkState {
	virtual ~ExecutionOrderedSinkState() {
	}
	virtual SinkResultType Sink(DataChunk &order_keys, DataChunk &payload) = 0;
};

struct ExecutionDelimJoinSinkState {
	virtual ~ExecutionDelimJoinSinkState() {
	}
	virtual SinkResultType Sink(DataChunk &input) = 0;
};

struct ExecutionAggregateUpdateState {
	virtual ~ExecutionAggregateUpdateState() {
	}
	virtual SinkResultType Sink(DataChunk &input) = 0;
};

struct ExecutionOperatorStageRecorder {
	virtual ~ExecutionOperatorStageRecorder() {
	}
	virtual void RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) = 0;
};

class ExecutionOperatorStageTimer {
public:
	ExecutionOperatorStageTimer(optional_ptr<ExecutionOperatorStageRecorder> recorder, ExecutionRegionStageId stage);
	ExecutionOperatorStageTimer(optional_ptr<ExecutionOperatorStageRecorder> recorder, const char *stage_name);
	~ExecutionOperatorStageTimer();
	ExecutionOperatorStageTimer(const ExecutionOperatorStageTimer &) = delete;
	ExecutionOperatorStageTimer &operator=(const ExecutionOperatorStageTimer &) = delete;

private:
	optional_ptr<ExecutionOperatorStageRecorder> recorder;
	ExecutionRegionStageId stage;
	std::chrono::steady_clock::time_point start;
};

struct ExecutionGroupedAggregateStateAddressState {
	virtual ~ExecutionGroupedAggregateStateAddressState() {
	}
	virtual void ResolveStateAddresses(DataChunk &input, Vector &addresses,
	                                   optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) = 0;
	virtual bool TryUpdateNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                                const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)lanes;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateNewGroupsWithPayloadInput(
	    DataChunk &groups, DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
	    const ExecutionRegionSinkInfo &sink_info, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr) {
		(void)groups;
		(void)payload_input;
		(void)payload_source_indices;
		(void)sink_info;
		(void)lanes;
		(void)recorder;
		(void)finish;
		(void)precomputed_hashes;
		return false;
	}
	virtual bool TryAppendNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                                const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)lanes;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateExistingGroupsWithStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateExistingGroupsWithSelectedStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateNewGroupsWithStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateNewGroupsWithSelectedStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr) {
		(void)input;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		(void)precomputed_hashes;
		return false;
	}
	virtual bool TryUpdateNewGroupsWithRowPointerKeys(
	    DataChunk &payload_input, Vector &row_pointers, idx_t count,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)payload_input;
		(void)row_pointers;
		(void)count;
		(void)group_sources;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryUpdateNewGroupsWithRowPointerKeysPayloadInput(
	    DataChunk &payload_input, Vector &row_pointers, idx_t count,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
	    const ExecutionRegionSinkInfo &sink_info, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)payload_input;
		(void)row_pointers;
		(void)count;
		(void)group_sources;
		(void)payload_source_indices;
		(void)sink_info;
		(void)lanes;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryAppendNewGroupsWithStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)update_function;
		(void)update_state;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual bool TryResolveNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, Vector &addresses,
	                                 optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                 bool finish = true) {
		(void)input;
		(void)sink_info;
		(void)addresses;
		(void)recorder;
		(void)finish;
		return false;
	}
	virtual void FinishStateUpdates() {
	}
};

struct ExecutionPerfectAggregateStateAddressLayout {
	bool ready = false;
	data_ptr_t data = nullptr;
	bool *group_is_set = nullptr;
	idx_t total_groups = 0;
	idx_t tuple_size = 0;
	idx_t aggregate_state_offset = 0;
	string blocker;
};

struct ExecutionPrimitiveAggregateUpdateLane {
	bool ready = false;
	AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
	idx_t aggregate_index = DConstants::INVALID_INDEX;
	idx_t payload_index = DConstants::INVALID_INDEX;
	PhysicalType payload_type = PhysicalType::INVALID;
	idx_t state_size = 0;
	idx_t state_offset = 0;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;
	int64_t *sum_int64_value = nullptr;
	hugeint_t *sum_hugeint_value = nullptr;
	double *sum_double_value = nullptr;
	bool *state_is_set = nullptr;
	idx_t *row_count = nullptr;
	string blocker;
};

struct ExecutionPrimitiveAggregateUpdateBinding {
	bool ready = false;
	vector<ExecutionPrimitiveAggregateUpdateLane> lanes;
	string blocker;

	const ExecutionPrimitiveAggregateUpdateLane *FindLane(idx_t aggregate_index) const;
};

struct ExecutionGroupedAggregateStateAddressBinding {
	bool ready = false;
	shared_ptr<ExecutionGroupedAggregateStateAddressState> state;
	vector<idx_t> aggregate_state_offsets;
	ExecutionHashAggregateLookupLayout hash_lookup_layout;
	ExecutionPerfectAggregateStateAddressLayout perfect_hash_layout;
	string blocker;
};

struct ExecutionHashJoinProbeBinding {
	bool ready = false;
	JoinHashTable *hash_table = nullptr;
	ExecutionHashJoinProbeLayoutKind layout_kind = ExecutionHashJoinProbeLayoutKind::NONE;
	ExecutionHashJoinTableLayout table_layout;
	ExecutionPerfectHashJoinTableLayout perfect_layout;
	bool empty_build_side = false;
	vector<idx_t> probe_key_input_indices;
	vector<idx_t> lhs_output_column_indices;
	idx_t rhs_output_column_count = 0;
	vector<idx_t> lhs_probe_column_indices;
	vector<LogicalType> lhs_probe_types;
	vector<LogicalType> output_types;
	vector<ExecutionHashJoinResidualSource> residual_sources;
	ExecutionHashJoinProbeOutputMode output_mode = ExecutionHashJoinProbeOutputMode::NONE;
	bool correlated_mark_counts_required = false;
	idx_t correlated_mark_group_count = 0;
	string blocker;
};

struct ExecutionHashJoinBuildBinding {
	bool ready = false;
	JoinHashTable *hash_table = nullptr;
	PartitionedTupleDataAppendState *append_state = nullptr;
	DataChunk *join_keys = nullptr;
	DataChunk *payload_chunk = nullptr;
	const JoinFilterPushdownInfo *filter_pushdown = nullptr;
	JoinFilterLocalState *local_filter_state = nullptr;
	bool skip_filter_pushdown = false;
	vector<column_t> key_input_indices;
	vector<LogicalType> key_types;
	vector<column_t> payload_input_indices;
	vector<LogicalType> payload_types;
	string blocker;
};

struct ExecutionNestedLoopJoinProbeBinding {
	bool ready = false;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	vector<ExecutionRegionComparisonType> comparison_types;
	vector<LogicalType> condition_types;
	vector<LogicalType> output_types;
	bool empty_build_side = false;
	ColumnDataCollection *right_condition_data = nullptr;
	ColumnDataCollection *right_payload_data = nullptr;
	ColumnDataScanState *condition_scan_state = nullptr;
	ColumnDataScanState *payload_scan_state = nullptr;
	DataChunk *right_condition = nullptr;
	DataChunk *right_payload = nullptr;
	idx_t *left_tuple = nullptr;
	idx_t *right_tuple = nullptr;
	bool *fetch_next_left = nullptr;
	bool *fetch_next_right = nullptr;
	string blocker;
};

struct ExecutionNestedLoopJoinBuildBinding {
	bool ready = false;
	ColumnDataCollection *right_condition_data = nullptr;
	ColumnDataCollection *right_payload_data = nullptr;
	mutex *lock = nullptr;
	vector<LogicalType> condition_types;
	string blocker;
};

struct ExecutionAppendSinkBinding {
	bool ready = false;
	shared_ptr<ExecutionAppendSinkState> state;
	string blocker;
};

struct ExecutionOrderedSinkBinding {
	bool ready = false;
	shared_ptr<ExecutionOrderedSinkState> state;
	vector<LogicalType> order_key_types;
	vector<LogicalType> payload_types;
	string blocker;
};

struct ExecutionDelimJoinSinkBinding {
	bool ready = false;
	shared_ptr<ExecutionDelimJoinSinkState> state;
	string blocker;
};

struct ExecutionAggregateUpdateBinding {
	bool ready = false;
	shared_ptr<ExecutionAggregateUpdateState> state;
	ExecutionPrimitiveAggregateUpdateBinding primitive;
	ExecutionGroupedAggregateStateAddressBinding grouped_state;
	string blocker;
};

struct ExecutionOperatorBinding {
	bool ready = false;
	ExecutionRegionOperatorContractKind kind = ExecutionRegionOperatorContractKind::NONE;
	ExecutionHashJoinProbeBinding hash_join_probe;
	ExecutionNestedLoopJoinProbeBinding nested_loop_join_probe;
	string blocker;
};

struct ExecutionSinkBinding {
	bool ready = false;
	ExecutionRegionSinkKind kind = ExecutionRegionSinkKind::NONE;
	ExecutionHashJoinBuildBinding hash_join_build;
	ExecutionNestedLoopJoinBuildBinding nested_loop_join_build;
	ExecutionAppendSinkBinding append_sink;
	ExecutionOrderedSinkBinding ordered_sink;
	ExecutionDelimJoinSinkBinding delim_join_sink;
	ExecutionAggregateUpdateBinding aggregate_update;
	string blocker;
};

DUCKDB_API void ExecutionMaterializeHashJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                  Vector &row_pointers, const SelectionVector &match_sel, idx_t count,
                                                  DataChunk &result,
                                                  optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);

DUCKDB_API bool ExecutionTryDirectGatherHashJoinRHSFixedColumn(const ExecutionHashJoinProbeBinding &binding,
                                                               Vector &row_pointers, idx_t count,
                                                               idx_t rhs_output_idx, Vector &result);

DUCKDB_API bool ExecutionGetHashJoinRHSFixedColumnSource(const ExecutionHashJoinProbeBinding &binding,
                                                         idx_t rhs_output_idx,
                                                         ExecutionHashJoinRHSFixedColumnSource &source);

DUCKDB_API bool ExecutionMaterializeHashJoinProbeProjectionSources(
    const ExecutionHashJoinProbeBinding &binding, DataChunk &input, Vector &row_pointers,
    const SelectionVector &match_sel, idx_t count, const vector<uint8_t> &referenced_columns, DataChunk &result,
    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
    optional_ptr<const SelectionVector> perfect_build_sel = nullptr);

DUCKDB_API void
ExecutionMaterializePerfectHashJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                         const SelectionVector &probe_sel, const SelectionVector &build_sel,
                                         idx_t count, DataChunk &result,
                                         optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);

DUCKDB_API void
ExecutionMaterializeHashJoinProbeLeftUnmatched(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                               const SelectionVector &unmatched_sel, idx_t count, DataChunk &result,
                                               optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);

DUCKDB_API void
ExecutionMaterializeHashJoinResidualSources(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                            Vector &row_pointers, const SelectionVector &match_sel, idx_t count,
                                            DataChunk &residual_sources,
                                            optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);

DUCKDB_API bool ExecutionBindHashJoinBuild(ExecutionContext &context, OperatorSinkInput &input, DataChunk &chunk,
                                           const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding);

DUCKDB_API void ExecutionHashJoinBuildReferenceKeys(const ExecutionHashJoinBuildBinding &binding, DataChunk &input);
DUCKDB_API void ExecutionHashJoinBuildApplyFilterPushdown(const ExecutionHashJoinBuildBinding &binding);
DUCKDB_API void ExecutionHashJoinBuildReferencePayload(const ExecutionHashJoinBuildBinding &binding, DataChunk &input);
DUCKDB_API idx_t ExecutionHashJoinBuildPrepare(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                               Vector &hash_values, SelectionVector &build_sel,
                                               optional_ptr<const SelectionVector> &build_selection);
DUCKDB_API void ExecutionHashJoinBuildHash(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                           Vector &hash_values, const SelectionVector &build_selection,
                                           idx_t build_count);
DUCKDB_API void ExecutionHashJoinBuildAppend(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                             const SelectionVector &build_selection, idx_t build_count);
DUCKDB_API SinkResultType ExecutionSinkHashJoinBuild(const ExecutionHashJoinBuildBinding &binding, DataChunk &input,
                                                     optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);

DUCKDB_API bool ExecutionNestedLoopJoinProbeStartInput(const ExecutionNestedLoopJoinProbeBinding &binding);

DUCKDB_API bool ExecutionNestedLoopJoinProbeAdvanceRight(const ExecutionNestedLoopJoinProbeBinding &binding);

DUCKDB_API void ExecutionMaterializeNestedLoopJoinProbe(const ExecutionNestedLoopJoinProbeBinding &binding,
                                                        DataChunk &input, const SelectionVector &left_sel,
                                                        const SelectionVector &right_sel, idx_t count,
                                                        DataChunk &result);

DUCKDB_API SinkResultType ExecutionSinkNestedLoopJoinBuild(const ExecutionNestedLoopJoinBuildBinding &binding,
                                                           DataChunk &input, DataChunk &right_condition);

DUCKDB_API SinkResultType ExecutionSinkAppend(const ExecutionAppendSinkBinding &binding, DataChunk &input);
DUCKDB_API ExecutionOperatorBindResult ExecutionPrepareDirectAppend(
    const ExecutionAppendSinkBinding &binding, const vector<LogicalType> &types, idx_t count,
    DirectAppendReservation &reservation, string &blocker, optional_ptr<DirectAppendProfile> profile = nullptr);
DUCKDB_API SinkResultType ExecutionCommitDirectAppend(const ExecutionAppendSinkBinding &binding,
                                                      const DirectAppendReservation &reservation,
                                                      optional_ptr<DirectAppendProfile> profile = nullptr);

DUCKDB_API SinkResultType ExecutionSinkOrdered(const ExecutionOrderedSinkBinding &binding, DataChunk &order_keys,
                                               DataChunk &payload);

DUCKDB_API SinkResultType ExecutionSinkDelimJoin(const ExecutionDelimJoinSinkBinding &binding, DataChunk &input);

} // namespace duckdb
