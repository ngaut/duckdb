//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/compiled_contract.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/common.hpp"

namespace duckdb {

enum class JitCompiledProtocolKind : uint8_t {
	NONE,
	SCAN_CURSOR,
	FILTER_STAGE,
	PROJECTION_STAGE,
	HASH_JOIN_BUILD,
	HASH_JOIN_PROBE_CURSOR,
	AGGREGATE_LOOKUP,
	AGGREGATE_UPDATE,
	SINK_CURSOR,
	STATE_SCAN_CURSOR
};

enum class JitCompiledDrainKind : uint8_t {
	NONE,
	ONE_INPUT_ONE_OUTPUT,
	ZERO_OR_ONE_OUTPUT,
	ZERO_OR_MANY_OUTPUT,
	STATE_DRAIN
};

struct JitCompiledStageContract {
	JitRegionStageKind stage = JitRegionStageKind::OPERATOR_BOUNDARY;
	JitCompiledProtocolKind protocol = JitCompiledProtocolKind::NONE;
	JitRegionStageExecutionKind execution = JitRegionStageExecutionKind::NONE;
	JitCompiledDrainKind drain = JitCompiledDrainKind::NONE;
	string required_capability;
	string blocker;
	string ir;
};

struct JitCompiledOperatorContract {
	bool present = false;
	bool has_source = false;
	bool has_operator = false;
	bool has_sink = false;
	bool has_state_scan = false;
	bool has_resumable_output = false;
	bool executor_boundary_free = false;
	vector<JitCompiledStageContract> stages;
	string ir;
};

} // namespace duckdb
