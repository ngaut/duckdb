//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_lowering.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_graph.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

class Expression;
class ExecutionRegionBackendPlan;

enum class ExecutionRegionPipelineInventoryMode : uint8_t { ADMISSION, TRACE };

struct ExecutionRegionNodeLowering {
	string label;
	string operator_name;
	ExecutionRegionOperatorKind operator_kind = ExecutionRegionOperatorKind::GENERIC;
	ExecutionRegionLoweringKind kind = ExecutionRegionLoweringKind::BOUNDARY;
	string reason;
};

struct ExecutionRegionLoweringPlan {
	void AddNode(string label, string operator_name, ExecutionRegionLoweringKind kind, string reason);
	void AddNode(string label, string operator_name, ExecutionRegionOperatorKind operator_kind,
	             ExecutionRegionLoweringKind kind, string reason);
	void AddFusionBlocker(string reason);
	void SetCompiledExecutionMode(ExecutionRegionExecutionMode execution_mode);
	void SetRegionExecutionForm(ExecutionRegionForm execution_form);
	void SetSourceFilterOwnership(ExecutionRegionSourceFilterOwnershipKind source_filter_ownership);
	void SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetOperatorStageIR(string stage_ir);
	idx_t NativeCount() const;
	idx_t BoundaryCount() const;
	ExecutionRegionExecutionMode ExpectedCompiledExecutionMode() const;
	ExecutionRegionForm ExpectedRegionExecutionForm() const;
	ExecutionRegionSourceFilterOwnershipKind SourceFilterOwnership() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	string EventReason() const;

	vector<ExecutionRegionNodeLowering> nodes;
	vector<string> fusion_blockers;
	string shape_key;
	shared_ptr<ExecutionRegionBackendPlan> backend_plan;
	ExecutionRegionExecutionMode compiled_execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	ExecutionRegionForm region_execution_form = ExecutionRegionForm::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSourceFilterOwnershipKind source_filter_ownership = ExecutionRegionSourceFilterOwnershipKind::NONE;
	string operator_stage_ir;
};

DUCKDB_API unique_ptr<ExecutionExpressionFragment> TryLowerExecutionExpression(const Expression &expression,
                                                                   idx_t expression_index = 0);
DUCKDB_API string DescribeExecutionExpressionLoweringFailure(const Expression &expression);
DUCKDB_API unique_ptr<ExecutionRegionPipelineInventory> TryInspectExecutionRegionPipeline(const ExecutionRegionGraph &descriptor,
                                                                              ExecutionRegionPipelineInventoryMode mode);
DUCKDB_API unique_ptr<ExecutionRegionIR> TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor);

} // namespace duckdb
