//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_contract.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_compiled_contract.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class Expression;
class PhysicalTableScan;

struct ExecutionSourceContractCapability {
	ExecutionRegionSourceKind kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE;
	bool supports_source_contract_input_layout = false;
	bool uses_storage_scan = false;

	bool IsReady() const {
		return execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}
};

DUCKDB_API ExecutionSourceContractCapability GetExecutionSourceContractCapability(const PhysicalTableScan &scan);

struct ExecutionSourceFilterContract {
	idx_t filter_index = 0;
	idx_t scan_column_index = 0;
	idx_t table_column_index = 0;
	optional_ptr<const Expression> expression;
	string reason;
};

struct ExecutionSourceContract {
	ExecutionRegionSourceKind kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE;
	string function_name;
	vector<ExecutionRegionContractField> fields;
	idx_t estimated_source_cardinality = 0;
	bool estimated_source_cardinality_exact = false;
	bool finalized_source_cardinality_required = false;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	vector<ExecutionSourceFilterContract> filters;
	vector<ExecutionRegionExactFilterProof> exact_filter_proofs;
	ExecutionRegionTableScanContract table_scan_contract;
	ExecutionRegionHashJoinContract hash_join_contract;
	ExecutionRegionNestedLoopJoinContract nested_loop_join_contract;
	ExecutionRegionAggregateContract aggregate_contract;
	ExecutionRegionOrderContract order_contract;
	vector<ExecutionRegionAggregateInput> aggregates;
	vector<ExecutionRegionHashJoinKeyInput> hash_join_keys;
	vector<ExecutionRegionGroupInput> groups;
	ExecutionSourceProtocolContract source_contract;
	ExecutionRegionNativeStateScanContract native_state_scan_contract;
	string reason;
};

struct ExecutionTransformContract {
	optional_ptr<const Expression> filter_expression;
	vector<optional_ptr<const Expression>> projection_expressions;

	bool HasFilterExpression() const {
		return filter_expression;
	}
	bool HasProjectionExpressions() const {
		return !projection_expressions.empty();
	}
};

struct ExecutionContract {
	ExecutionCompiledOperatorContract compiled_contract;

	string source_boundary_reason;
	ExecutionSourceContract source;

	ExecutionTransformContract transform;

	ExecutionRegionOperatorInfo operator_info;

	ExecutionRegionSinkInfo sink;

	bool HasSource() const {
		return source.kind != ExecutionRegionSourceKind::NONE;
	}
	bool HasOperator() const {
		return operator_info.kind != ExecutionRegionOperatorContractKind::NONE;
	}
	bool HasSink() const {
		return sink.kind != ExecutionRegionSinkKind::NONE;
	}
};

DUCKDB_API ExecutionContract FinalizeExecutionContract(ExecutionContract descriptor);
DUCKDB_API vector<ExecutionRegionContractField> BuildExecutionContractFields(const string &reason);

} // namespace duckdb
