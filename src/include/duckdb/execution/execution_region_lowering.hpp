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

enum class ExecutionRegionIRMode : uint8_t { COMPACT, TRACE };

class ExecutionExpressionAnalysisCache {
public:
	const ExecutionExpressionFragment *Get(const Expression &expression,
	                                       ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::COMPACT);
	unique_ptr<ExecutionExpressionFragment> Copy(const Expression &expression, idx_t expression_index,
	                                             ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::COMPACT);

private:
	struct Entry {
		bool attempted = false;
		unique_ptr<ExecutionExpressionFragment> fragment;
	};

private:
	unordered_map<const Expression *, Entry> entries;
};

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
	void SetFullyFused(bool fully_fused);
	void SetUsesScanFilters(bool uses_scan_filters);
	void SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetOperatorStageIR(string stage_ir);
	idx_t NativeCount() const;
	idx_t BoundaryCount() const;
	ExecutionRegionExecutionMode ExpectedCompiledExecutionMode() const;
	bool IsFullyFused() const;
	bool UsesScanFilters() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	string CompactEventReason() const;
	string EventReason() const;

	vector<ExecutionRegionNodeLowering> nodes;
	vector<string> fusion_blockers;
	shared_ptr<ExecutionRegionBackendPlan> backend_plan;
	ExecutionRegionExecutionMode compiled_execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	idx_t native_count = 0;
	idx_t boundary_count = 0;
	bool fully_fused = false;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;
	string operator_stage_ir;
};

DUCKDB_API unique_ptr<ExecutionExpressionFragment>
TryLowerExecutionExpression(const Expression &expression, idx_t expression_index = 0,
                            ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::TRACE);
DUCKDB_API string DescribeExecutionExpressionLoweringFailure(const Expression &expression);
DUCKDB_API unique_ptr<ExecutionRegionIR>
TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor, ExecutionRegionIRMode mode,
                        ExecutionExpressionAnalysisCache *expression_cache = nullptr);

} // namespace duckdb
