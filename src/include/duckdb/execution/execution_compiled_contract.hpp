//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_compiled_contract.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

enum class ExecutionCompiledContractKind : uint8_t {
	NONE,
	SCAN_CURSOR,
	FILTER_STAGE,
	PROJECTION_STAGE,
	HASH_JOIN_BUILD,
	HASH_JOIN_PROBE_CURSOR,
	NESTED_LOOP_JOIN_BUILD,
	NESTED_LOOP_JOIN_PROBE_CURSOR,
	AGGREGATE_LOOKUP,
	AGGREGATE_UPDATE,
	AGGREGATE_DISTINCT_SINK,
	SINK_CURSOR,
	STATE_SCAN_CURSOR
};

enum class ExecutionCompiledDrainKind : uint8_t {
	NONE,
	ONE_INPUT_ONE_OUTPUT,
	ZERO_OR_ONE_OUTPUT,
	ZERO_OR_MANY_OUTPUT,
	STATE_DRAIN
};

DUCKDB_API const char *ExecutionCompiledContractKindToString(ExecutionCompiledContractKind kind);
DUCKDB_API const char *ExecutionCompiledDrainKindToString(ExecutionCompiledDrainKind kind);

struct ExecutionCompiledStageContract {
	ExecutionRegionStageKind stage = ExecutionRegionStageKind::OPERATOR_BOUNDARY;
	ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE;
	ExecutionRegionStageExecutionKind execution = ExecutionRegionStageExecutionKind::NONE;
	ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE;
	bool executable_work = false;
	string required_capability;
	string blocker;
	string ir;
};

struct ExecutionCompiledOperatorContract {
	vector<ExecutionCompiledStageContract> stages;
	string ir;

	bool Present() const {
		return !stages.empty();
	}

	bool HasSource() const {
		for (auto &stage : stages) {
			if (stage.stage == ExecutionRegionStageKind::SOURCE) {
				return true;
			}
		}
		return false;
	}

	bool HasOperator() const {
		for (auto &stage : stages) {
			if (stage.stage == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
			    stage.stage == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
			    stage.stage == ExecutionRegionStageKind::OPERATOR_BOUNDARY) {
				return true;
			}
		}
		return false;
	}

	bool HasSink() const {
		for (auto &stage : stages) {
			switch (stage.stage) {
			case ExecutionRegionStageKind::HASH_JOIN_BUILD:
			case ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD:
			case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::HASH_AGGREGATE_DISTINCT_SINK:
			case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::APPEND_SINK:
			case ExecutionRegionStageKind::SORT_SINK:
			case ExecutionRegionStageKind::DELIM_JOIN_SINK:
			case ExecutionRegionStageKind::SINK_BOUNDARY:
				return true;
			default:
				break;
			}
		}
		return false;
	}

	bool HasStateScan() const {
		for (auto &stage : stages) {
			if (stage.operation == ExecutionCompiledContractKind::STATE_SCAN_CURSOR) {
				return true;
			}
		}
		return false;
	}

	bool HasZeroOrManyOutput() const {
		for (auto &stage : stages) {
			if (stage.drain == ExecutionCompiledDrainKind::ZERO_OR_MANY_OUTPUT) {
				return true;
			}
		}
		return false;
	}

	bool HasExecutableWork() const {
		for (auto &stage : stages) {
			if (stage.executable_work && stage.execution == ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
				return true;
			}
		}
		return false;
	}

	bool HasNativeContract(ExecutionCompiledContractKind operation) const {
		for (auto &stage : stages) {
			if (stage.operation == operation && stage.execution == ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
				return true;
			}
		}
		return false;
	}
};

} // namespace duckdb
