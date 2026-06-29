//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_graph.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/execution_contract.hpp"

namespace duckdb {

class Expression;
class Pipeline;

enum class ExecutionRegionOperatorSlot : uint8_t { SOURCE, OPERATOR, SINK };

struct ExecutionRegionOperatorEntry {
	bool present = false;
	ExecutionRegionOperatorSlot slot = ExecutionRegionOperatorSlot::OPERATOR;
	idx_t operator_index = DConstants::INVALID_INDEX;
	string operator_name;
	vector<LogicalType> output_types;
	idx_t estimated_cardinality = 0;
	ExecutionRegionOperatorKind operator_kind = ExecutionRegionOperatorKind::GENERIC;
	optional_ptr<const Expression> filter_expression;
	vector<optional_ptr<const Expression>> projection_expressions;
	string source_boundary_reason;
	ExecutionSourceContract source_payload;
	ExecutionRegionOperatorInfo operator_payload;
	ExecutionRegionSinkInfo sink_payload;
	ExecutionCompiledOperatorContract source_contract;
	ExecutionCompiledOperatorContract operator_contract;
	ExecutionCompiledOperatorContract sink_contract;
	bool has_generated_expression = false;
	bool has_native_operator_work = false;

	bool HasSourceContract() const {
		return source_contract.Present();
	}
	bool HasOperatorContract() const {
		return operator_contract.Present();
	}
	bool HasSinkContract() const {
		return sink_contract.Present();
	}
	bool UsesSourceContract() const {
		return source_contract.HasNativeContract(ExecutionCompiledContractKind::SCAN_CURSOR) ||
		       source_contract.HasNativeContract(ExecutionCompiledContractKind::STATE_SCAN_CURSOR);
	}
	bool HasNativeOperator() const {
		return operator_contract.HasNativeContract(ExecutionCompiledContractKind::HASH_JOIN_PROBE_CURSOR) ||
		       operator_contract.HasNativeContract(ExecutionCompiledContractKind::NESTED_LOOP_JOIN_PROBE_CURSOR) ||
		       operator_contract.HasNativeContract(ExecutionCompiledContractKind::AGGREGATE_LOOKUP);
	}
	bool HasNativeSink() const {
		return sink_contract.HasNativeContract(ExecutionCompiledContractKind::HASH_JOIN_BUILD) ||
		       sink_contract.HasNativeContract(ExecutionCompiledContractKind::NESTED_LOOP_JOIN_BUILD) ||
		       sink_contract.HasNativeContract(ExecutionCompiledContractKind::AGGREGATE_UPDATE) ||
		       sink_contract.HasNativeContract(ExecutionCompiledContractKind::SINK_CURSOR);
	}
	bool IsScanSource() const {
		return operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN ||
		       operator_kind == ExecutionRegionOperatorKind::SCAN_SOURCE;
	}
	bool IsFilter() const {
		return operator_kind == ExecutionRegionOperatorKind::FILTER;
	}
	bool IsProjection() const {
		return operator_kind == ExecutionRegionOperatorKind::PROJECTION;
	}
	bool IsSortSink() const {
		return operator_kind == ExecutionRegionOperatorKind::ORDER_BY ||
		       operator_kind == ExecutionRegionOperatorKind::TOP_N;
	}
	bool IsMaterializationSink() const {
		return operator_kind == ExecutionRegionOperatorKind::CTE ||
		       operator_kind == ExecutionRegionOperatorKind::RESULT_COLLECTOR ||
		       operator_kind == ExecutionRegionOperatorKind::EXPLAIN_ANALYZE;
	}
};

struct ExecutionRegionGraph {
	ExecutionRegionOperatorEntry source;
	vector<ExecutionRegionOperatorEntry> operators;
	ExecutionRegionOperatorEntry sink;
	bool has_generated_expression = false;
	bool has_native_operator_work = false;

	bool HasSource() const {
		return source.present;
	}
	bool HasSink() const {
		return sink.present;
	}
	bool Empty() const {
		return !HasSource() && operators.empty() && !HasSink();
	}
	idx_t OperatorCount() const {
		return operators.size();
	}
	bool HasGeneratedExpression() const {
		return has_generated_expression;
	}
	bool HasNativeOperatorWork() const {
		return has_native_operator_work;
	}
};

DUCKDB_API unique_ptr<ExecutionRegionGraph> BuildExecutionRegionGraph(Pipeline &pipeline,
                                                                      bool render_diagnostics = false);
DUCKDB_API string DescribeExecutionRegionGraphShape(const ExecutionRegionGraph &graph);

} // namespace duckdb
