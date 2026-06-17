//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_operator_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/function/aggregate_primitive_update.hpp"

namespace duckdb {

class ColumnDataCollection;
class DataChunk;
class GlobalSinkState;
class InterruptState;
class LocalSinkState;
class Vector;
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

struct ExecutionAppendSinkState {
	virtual ~ExecutionAppendSinkState() {
	}
	virtual SinkResultType Append(DataChunk &input) = 0;
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
	virtual idx_t FindOrCreateAggregateStates(DataChunk &input, const vector<idx_t> &group_input_indices,
	                                          Vector &addresses_out);
	virtual void FinishNativeAggregateUpdate();
};

struct ExecutionPrimitiveAggregateUpdateLane {
	bool ready = false;
	AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
	idx_t aggregate_index = DConstants::INVALID_INDEX;
	idx_t payload_index = DConstants::INVALID_INDEX;
	PhysicalType payload_type = PhysicalType::INVALID;
	int64_t *sum_int64_value = nullptr;
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

struct ExecutionProjectionOperatorState {
	virtual ~ExecutionProjectionOperatorState() {
	}
	virtual OperatorResultType Project(DataChunk &input, DataChunk &output) = 0;
};

struct ExecutionProjectionBinding {
	bool ready = false;
	shared_ptr<ExecutionProjectionOperatorState> state;
	vector<LogicalType> output_types;
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
	string blocker;
};

struct ExecutionOperatorBinding {
	bool ready = false;
	ExecutionRegionOperatorContractKind kind = ExecutionRegionOperatorContractKind::NONE;
	ExecutionProjectionBinding projection;
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
                                                  DataChunk &result);

DUCKDB_API void ExecutionMaterializePerfectHashJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                         const SelectionVector &probe_sel,
                                                         const SelectionVector &build_sel, idx_t count,
                                                         DataChunk &result);

DUCKDB_API void ExecutionMaterializeHashJoinProbeLeftUnmatched(const ExecutionHashJoinProbeBinding &binding,
                                                               DataChunk &input, const SelectionVector &unmatched_sel,
                                                               idx_t count, DataChunk &result);

DUCKDB_API void ExecutionMaterializeHashJoinResidualSources(const ExecutionHashJoinProbeBinding &binding,
                                                            DataChunk &input, Vector &row_pointers,
                                                            const SelectionVector &match_sel, idx_t count,
                                                            DataChunk &residual_sources);

DUCKDB_API bool ExecutionBindHashJoinBuild(ExecutionContext &context, OperatorSinkInput &input, DataChunk &chunk,
                                           const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding);

DUCKDB_API SinkResultType ExecutionSinkHashJoinBuild(const ExecutionHashJoinBuildBinding &binding, DataChunk &input);

DUCKDB_API bool ExecutionNestedLoopJoinProbeStartInput(const ExecutionNestedLoopJoinProbeBinding &binding);

DUCKDB_API bool ExecutionNestedLoopJoinProbeAdvanceRight(const ExecutionNestedLoopJoinProbeBinding &binding);

DUCKDB_API void ExecutionMaterializeNestedLoopJoinProbe(const ExecutionNestedLoopJoinProbeBinding &binding,
                                                        DataChunk &input, const SelectionVector &left_sel,
                                                        const SelectionVector &right_sel, idx_t count,
                                                        DataChunk &result);

DUCKDB_API SinkResultType ExecutionSinkNestedLoopJoinBuild(const ExecutionNestedLoopJoinBuildBinding &binding,
                                                           DataChunk &input, DataChunk &right_condition);

DUCKDB_API SinkResultType ExecutionSinkAppend(const ExecutionAppendSinkBinding &binding, DataChunk &input);

DUCKDB_API SinkResultType ExecutionSinkOrdered(const ExecutionOrderedSinkBinding &binding, DataChunk &order_keys,
                                               DataChunk &payload);

DUCKDB_API SinkResultType ExecutionSinkDelimJoin(const ExecutionDelimJoinSinkBinding &binding, DataChunk &input);

DUCKDB_API SinkResultType ExecutionSinkAggregateUpdate(const ExecutionAggregateUpdateBinding &binding,
                                                       DataChunk &input);

DUCKDB_API idx_t ExecutionFindOrCreateAggregateStates(const ExecutionAggregateUpdateBinding &binding,
                                                      DataChunk &input,
                                                      const vector<idx_t> &group_input_indices,
                                                      Vector &addresses_out);

DUCKDB_API void ExecutionFinishAggregateUpdate(const ExecutionAggregateUpdateBinding &binding);

DUCKDB_API OperatorResultType ExecutionOperatorProject(const ExecutionProjectionBinding &binding, DataChunk &input,
                                                       DataChunk &output);

} // namespace duckdb
