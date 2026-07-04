//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_composition.hpp"
#include "sljit_region_plan.hpp"

namespace duckdb {

struct SljitProjectionGraphLowering {
	SljitProjectionGraphLowering(const vector<LogicalType> &input_types_p, bool render_diagnostics_p)
	    : input_type_count(input_types_p.size()), render_diagnostics(render_diagnostics_p),
	      current_types(input_types_p) {
	}

	idx_t input_type_count;
	bool render_diagnostics;
	vector<LogicalType> current_types;
	vector<SljitNativeRegionOpPlan> native_ops;
};

struct SljitSourceContractPlan {
	bool uses_scan_filters = false;
	bool requires_source_contract_input_layout = false;

	void Merge(const SljitSourceContractPlan &other) {
		uses_scan_filters = uses_scan_filters || other.uses_scan_filters;
		requires_source_contract_input_layout =
		    requires_source_contract_input_layout || other.requires_source_contract_input_layout;
	}
};

struct SljitRegionNodePlan {
	ExecutionRegionLoweringKind kind = ExecutionRegionLoweringKind::BOUNDARY;
	string reason;
	vector<SljitNativeRegionOpPlan> native_ops;
	SljitSourceContractPlan source_contract;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool requires_source_contract = false;
};

struct SljitSourceFilterPlan {
	vector<SljitNativeRegionOpPlan> native_ops;
	SljitSourceContractPlan source_contract;
};

bool TryLowerNativeRegionExpression(const ExecutionExpressionFragment &fragment, bool require_boolean,
                                    SljitNativeRegionExpressionPlan &expr, string &error, bool render_diagnostics);
bool TryReadNativeRegionExpression(const ExecutionExpressionIR &root, bool require_boolean,
                                   SljitNativeRegionExpressionPlan &expr);
bool TryReadNativeRegionPredicateExpression(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr);
bool TryReadNativeScalarIntrinsicRegionExpression(const ExecutionExpressionIR &root,
                                                  SljitNativeRegionExpressionPlan &expr);
bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root, SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan &expression);
bool TryBuildSljitNativeTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                SljitNativeRegionExpressionPlan &expr);
bool TryBuildSljitNativeAnyExpressionTreePlan(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr);
void AttachSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr);
unique_ptr<ExecutionExpressionIR> CopySljitExpressionPlanAsInputTree(const SljitNativeRegionExpressionPlan &expr);
bool TryMapNativeProjectionExpressionSources(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                             SljitNativeRegionExpressionPlan &expr);
void FuseAdjacentNativeProjections(SljitNativeRegionPlan &region, bool render_diagnostics);
void FusePrimitiveAggregateUpdates(SljitNativeRegionPlan &region, const vector<LogicalType> &region_input_types,
                                   bool render_diagnostics);
void DisableSljitRegionFlatNullableFastPath(SljitNativeRegionPlan &region);
bool SljitNativeRegionHasExecutableBodyGap(const SljitNativeRegionPlan &region, string &blocker);
void AddSljitNativeRegionCapabilityFacts(ExecutionRegionLoweringPlan &lowering_plan,
                                         const SljitNativeRegionPlan &native_region);

bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan);
SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(SljitRegionNodePlan &node_plan);
SljitRegionNodePlan SljitNativeNode(SljitNativeRegionOpPlan &&native_op, string reason);
SljitRegionNodePlan SljitNativeNode(vector<SljitNativeRegionOpPlan> native_ops, string reason);
SljitRegionNodePlan SljitRegionBoundaryNode(string reason);
SljitRegionNodePlan SljitNodeBlockerBoundary(const ExecutionRegionNode &node, const char *fallback);
string SljitBlockerOrReason(const string &blocker, const char *reason);
void AppendSljitReasonPart(string &reason, const string &part, bool render_diagnostics);
SljitNativeRegionExpressionPlan SljitNativeReferenceExpression(idx_t source_index, const LogicalType &type, string ir,
                                                               bool references_region_input);
SljitRegionNodePlan SljitBlockedContractBoundary(const string &blocker, const char *reason);

SljitRegionNodePlan PlanSljitFilterNode(const ExecutionRegionNode &node, string &error, bool render_diagnostics);
SljitRegionNodePlan PlanSljitProjectionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                            string &error, bool render_diagnostics);
string SljitSourceBoundaryReason(const ExecutionRegionNode &node, bool render_diagnostics);
void AppendSljitSourceFilterFacts(string &reason, const ExecutionRegionNode &node,
                                  const ExecutionRegionTableScanContract &contract, bool include_input_columns);
bool TryPlanSljitSourceFilters(const ExecutionRegionNode &node, SljitSourceFilterPlan &plan, string &error,
                               bool render_diagnostics);
SljitRegionNodePlan PlanSljitSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                        ExecutionRegionSourceExecutionKind source_execution, bool render_diagnostics);
bool SljitCanExecuteSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract);
	SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const ExecutionRegionNode &node,
	                                                       const vector<LogicalType> &input_types,
	                                                       const vector<bool> &input_not_null, bool render_diagnostics);
SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                             const vector<LogicalType> &input_types,
                                                             bool render_diagnostics);
SljitRegionNodePlan PlanSljitHashJoinBuildSinkNode(const ExecutionRegionNode &node, bool render_diagnostics);
SljitRegionNodePlan PlanSljitNestedLoopJoinBuildSinkNode(const ExecutionRegionNode &node, bool render_diagnostics);
SljitRegionNodePlan PlanSljitAggregateUpdateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics);
SljitRegionNodePlan PlanSljitHashAggregateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics);
SljitRegionNodePlan PlanSljitSinkNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                      bool render_diagnostics);
SljitRegionNodePlan PlanSljitFullPipelineSinkNode(const ExecutionRegionNode &node,
                                                  const vector<LogicalType> &input_types, bool render_diagnostics);
void AddSljitFullPipelineSinkBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                      const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan,
                                      const ExecutionRegionContract &contract);
void AddSljitOperatorContractBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                      const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan);

const char *SljitNativeRegionOpKindName(SljitNativeRegionOpKind kind);
const char *SljitHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode);
const char *SljitHashJoinComparisonToString(ExecutionRegionComparisonType comparison_type);
const char *SljitNestedLoopJoinValueKindToString(SljitNativeNestedLoopJoinValueKind kind);
string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr);
string DescribeNativeRegionExpressionList(const vector<SljitNativeRegionExpressionPlan> &expressions);
string DescribeSljitHashJoinProbeKeys(const vector<SljitNativeHashJoinProbeKeyPlan> &keys, const char *separator);
void AppendSljitHashJoinProbeMarkOffsets(string &result, const char *name, const SljitNativeHashJoinProbePlan &probe);

} // namespace duckdb
