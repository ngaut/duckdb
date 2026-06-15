//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/operator_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/execution/jit/join_runtime.hpp"
#include "duckdb/execution/jit/region.hpp"

namespace duckdb {

class DataChunk;
class BatchedDataCollection;
class BatchedBufferedData;
class ColumnDataCollection;
class InterruptState;
class SimpleBufferedData;
struct ColumnDataAppendState;
struct ExecutionContext;
struct OperatorSinkInput;
struct PartitionedTupleDataAppendState;
struct JoinFilterLocalState;
struct JoinFilterPushdownInfo;

enum class JitNativeResultCollectorAppendKind : uint8_t {
	NONE,
	COLUMN_DATA_COLLECTION,
	BATCHED_DATA_COLLECTION,
	SIMPLE_BUFFERED_DATA,
	BATCHED_BUFFERED_DATA
};

struct JitNativeHashJoinProbeBinding {
	bool ready = false;
	JoinHashTable *hash_table = nullptr;
	JitNativeHashJoinTableLayout table_layout;
	vector<idx_t> probe_key_input_indices;
	vector<idx_t> lhs_output_column_indices;
	idx_t rhs_output_column_count = 0;
	vector<idx_t> lhs_probe_column_indices;
	vector<LogicalType> lhs_probe_types;
	vector<LogicalType> output_types;
	JitRegionHashJoinProbeOutputMode output_mode = JitRegionHashJoinProbeOutputMode::NONE;
	bool correlated_mark_counts_required = false;
	idx_t correlated_mark_group_count = 0;
	string blocker;
};

struct JitNativeHashJoinBuildBinding {
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

struct JitNativeResultCollectorAppendBinding {
	bool ready = false;
	JitNativeResultCollectorAppendKind kind = JitNativeResultCollectorAppendKind::NONE;
	ColumnDataCollection *collection = nullptr;
	ColumnDataAppendState *append_state = nullptr;
	BatchedDataCollection *batched_data = nullptr;
	SimpleBufferedData *simple_buffered_data = nullptr;
	BatchedBufferedData *batched_buffered_data = nullptr;
	InterruptState *interrupt_state = nullptr;
	idx_t batch_index = 0;
	idx_t min_batch_index = 0;
	idx_t *current_batch = nullptr;
	string blocker;
};

struct JitNativeOperatorBinding {
	bool ready = false;
	JitRegionOperatorKind kind = JitRegionOperatorKind::NONE;
	JitNativeHashJoinProbeBinding hash_join_probe;
	string blocker;
};

struct JitNativeSinkBinding {
	bool ready = false;
	JitRegionSinkKind kind = JitRegionSinkKind::NONE;
	JitNativeHashJoinBuildBinding hash_join_build;
	JitNativeResultCollectorAppendBinding result_collector_append;
	string blocker;
};

DUCKDB_API void JitMaterializeNativeHashJoinProbe(const JitNativeHashJoinProbeBinding &binding, DataChunk &input,
                                                  Vector &row_pointers, const SelectionVector &match_sel, idx_t count,
                                                  DataChunk &result);

DUCKDB_API bool JitBindNativeHashJoinBuild(ExecutionContext &context, OperatorSinkInput &input, DataChunk &chunk,
                                           const JitRegionSinkInfo &sink_info, JitNativeSinkBinding &binding);

DUCKDB_API SinkResultType JitAppendNativeHashJoinBuild(const JitNativeHashJoinBuildBinding &binding, DataChunk &input);

DUCKDB_API SinkResultType JitAppendNativeResultCollector(const JitNativeResultCollectorAppendBinding &binding,
                                                         DataChunk &input);

} // namespace duckdb
