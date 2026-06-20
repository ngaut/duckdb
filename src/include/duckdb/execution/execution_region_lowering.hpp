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
	void SetRegionExecutionForm(ExecutionRegionForm execution_form);
	void SetExecutionBody(ExecutionRegionExecutionBody execution_body);
	void SetUsesScanFilters(bool uses_scan_filters);
	void SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetOperatorStageIR(string stage_ir);
	void SetGeneratedExpressionCost(idx_t expression_cost);
	void SetGeneratedStageCount(idx_t stage_count);
	idx_t NativeCount() const;
	idx_t BoundaryCount() const;
	ExecutionRegionExecutionMode ExpectedCompiledExecutionMode() const;
	ExecutionRegionForm ExpectedRegionExecutionForm() const;
	ExecutionRegionExecutionBody ExpectedExecutionBody() const;
	bool UsesScanFilters() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	string CompactEventReason() const;
	string EventReason() const;

	vector<ExecutionRegionNodeLowering> nodes;
	vector<string> fusion_blockers;
	string shape_key;
	shared_ptr<ExecutionRegionBackendPlan> backend_plan;
	ExecutionRegionExecutionMode compiled_execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	ExecutionRegionForm region_execution_form = ExecutionRegionForm::NONE;
	ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;
	idx_t generated_expression_cost = 0;
	idx_t generated_stage_count = 0;
	string operator_stage_ir;
};

inline void AppendExecutionRegionShapeKeyPart(string &key, const string &label, const string &value) {
	if (value.empty()) {
		return;
	}
	key += ":";
	key += label;
	key += ":";
	key += value;
}

inline string BuildExecutionRegionShapeKey(const string &backend_name, const ExecutionRegionSignature &signature) {
	string result = backend_name + ":" + signature.context + ":" + signature.shape;
	if (!signature.feature_shape.empty()) {
		result += ":";
		result += signature.feature_shape;
	}
	return result;
}

inline string BuildExecutionRegionContextShapeKey(const ExecutionRegionSignature &signature, const string &shape_key) {
	auto result = shape_key;
	AppendExecutionRegionShapeKeyPart(result, "context", signature.context_feature_shape);
	AppendExecutionRegionShapeKeyPart(result, "contract", signature.contract_shape);
	return result;
}

DUCKDB_API unique_ptr<ExecutionExpressionFragment>
TryLowerExecutionExpression(const Expression &expression, idx_t expression_index = 0,
                            ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::TRACE);
DUCKDB_API string DescribeExecutionExpressionLoweringFailure(const Expression &expression);
DUCKDB_API unique_ptr<ExecutionRegionPipelineInventory>
TryInspectExecutionRegionPipeline(const ExecutionRegionGraph &descriptor,
                                  ExecutionExpressionAnalysisCache *expression_cache = nullptr);
DUCKDB_API string RenderExecutionRegionPipelineInventoryIR(const ExecutionRegionPipelineInventory &inventory);
DUCKDB_API unique_ptr<ExecutionRegionIR>
TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor, ExecutionRegionIRMode mode,
                        const ExecutionRegionPipelineInventory *inventory = nullptr,
                        ExecutionExpressionAnalysisCache *expression_cache = nullptr);

} // namespace duckdb
