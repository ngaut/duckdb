//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/aggregate_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/jit/common.hpp"

namespace duckdb {

class DataChunk;
struct ExecutionContext;
struct OperatorSinkInput;

struct JitUngroupedAggregatePayloadBinding {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
};

struct JitGroupedAggregatePayloadBinding {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
};

struct JitGroupedAggregateGroupBinding {
	idx_t group_index = 0;
	idx_t input_index = 0;
};

struct JitNativeUngroupedAggregateState {
	idx_t aggregate_index = 0;
	JitAggregateUpdateKind update_kind = JitAggregateUpdateKind::NONE;
	data_ptr_t state = nullptr;
	idx_t *count = nullptr;
};

struct JitNativeGroupedAggregateStateRequest {
	idx_t aggregate_index = 0;
	JitAggregateUpdateKind update_kind = JitAggregateUpdateKind::NONE;
};

struct JitNativeGroupedAggregateState {
	idx_t aggregate_index = 0;
	JitAggregateUpdateKind update_kind = JitAggregateUpdateKind::NONE;
	idx_t aggregate_state_offset = 0;
};

struct JitNativeGroupedAggregateStateSet {
	JitNativeGroupedAggregateStateSet();

	Vector aggregate_addresses;
	vector<JitNativeGroupedAggregateState> states;
	idx_t count = 0;
};

struct JitNativePerfectHashAggregateStateLayout {
	data_ptr_t data = nullptr;
	bool *group_is_set = nullptr;
	idx_t total_groups = 0;
	idx_t tuple_size = 0;
	idx_t aggregate_state_offset = 0;
};

DUCKDB_API void
JitBindNativeUngroupedAggregateStates(OperatorSinkInput &input,
                                      const vector<JitNativeUngroupedAggregateState> &requested_states,
                                      vector<JitNativeUngroupedAggregateState> &bound_states);

DUCKDB_API void JitBindNativeHashAggregateStates(
    ExecutionContext &context, OperatorSinkInput &input, DataChunk &payload_chunk,
    const vector<JitGroupedAggregateGroupBinding> &group_bindings,
    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
    JitNativeGroupedAggregateStateSet &bound_states);

DUCKDB_API SinkResultType JitFinishNativeHashAggregateUpdate(ExecutionContext &context, OperatorSinkInput &input,
                                                             idx_t count);

DUCKDB_API void
JitBindNativePerfectHashAggregateStates(ExecutionContext &context, OperatorSinkInput &input, DataChunk &payload_chunk,
                                        const vector<JitGroupedAggregateGroupBinding> &group_bindings,
                                        const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
                                        JitNativeGroupedAggregateStateSet &bound_states);

DUCKDB_API void JitBindNativePerfectHashAggregateStateLayout(
    ExecutionContext &context, OperatorSinkInput &input,
    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
    JitNativeGroupedAggregateStateSet &bound_states, JitNativePerfectHashAggregateStateLayout &state_layout);

DUCKDB_API SinkResultType JitFinishNativePerfectHashAggregateUpdate(ExecutionContext &context, OperatorSinkInput &input,
                                                                    idx_t count);

} // namespace duckdb
