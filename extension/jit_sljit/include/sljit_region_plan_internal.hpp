//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

struct SljitRegionNodePlan {
	ExecutionRegionLoweringKind kind = ExecutionRegionLoweringKind::BOUNDARY;
	string reason;
	vector<SljitNativeRegionOpPlan> native_ops;
	bool uses_scan_filters = false;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool requires_source_contract = false;
};

bool TryLowerNativeRegionExpression(const ExecutionExpressionFragment &fragment, bool require_boolean,
                                    SljitNativeRegionExpressionPlan &expr, string &error, bool render_diagnostics);
bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root, SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan &expression);
void FuseAdjacentNativeProjections(SljitNativeRegionPlan &region, bool render_diagnostics);
void FusePrimitiveAggregateUpdates(SljitNativeRegionPlan &region, const vector<LogicalType> &region_input_types,
                                   bool render_diagnostics);
bool SljitNativeRegionOpGeneratesCode(const SljitNativeRegionOpPlan &op);

bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan);
const SljitNativeRegionOpPlan &SljitRegionNodeFirstNativeOp(const SljitRegionNodePlan &node_plan);
SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(SljitRegionNodePlan &node_plan);
bool SljitRegionNodeHasSingleNativeOp(const SljitRegionNodePlan &node_plan);
void AppendSljitRegionNodeNativeOps(SljitNativeRegionPlan &region, SljitRegionNodePlan &node_plan);
SljitRegionNodePlan SljitNativeNode(SljitNativeRegionOpPlan &&native_op, string reason);
SljitRegionNodePlan SljitNativeNode(vector<SljitNativeRegionOpPlan> native_ops, string reason);
SljitRegionNodePlan SljitRegionBoundaryNode(string reason);
SljitRegionNodePlan SljitNodeBlockerBoundary(const ExecutionRegionNode &node, const char *fallback);
string SljitBlockerOrReason(const string &blocker, const char *reason);
void AppendSljitReasonPart(string &reason, const string &part, bool render_diagnostics);
SljitRegionNodePlan SljitBlockedContractBoundary(const string &blocker, const char *reason);
SljitRegionNodePlan SljitUnsupportedExpressionBoundaryNode(const string &error);

SljitRegionNodePlan PlanSljitFilterNode(const ExecutionRegionNode &node, string &error, bool render_diagnostics);
SljitRegionNodePlan PlanSljitProjectionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                            string &error, bool render_diagnostics);
string SljitSourceBoundaryReason(const ExecutionRegionNode &node, bool render_diagnostics);
SljitRegionNodePlan PlanSljitSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                        ExecutionRegionSourceExecutionKind source_execution, bool render_diagnostics);
bool SljitCanExecuteSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract);
SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                       const vector<LogicalType> &input_types, bool render_diagnostics);
SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                             const vector<LogicalType> &input_types,
                                                             bool render_diagnostics);
SljitRegionNodePlan PlanSljitSinkNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                      bool render_diagnostics);
SljitRegionNodePlan PlanSljitFullPipelineSinkNode(const ExecutionRegionNode &node,
                                                  const vector<LogicalType> &input_types, bool render_diagnostics);

const char *SljitNativeRegionOpKindName(SljitNativeRegionOpKind kind);
const char *SljitHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode);
const char *SljitHashJoinComparisonToString(ExecutionRegionComparisonType comparison_type);
const char *SljitNestedLoopJoinValueKindToString(SljitNativeNestedLoopJoinValueKind kind);
string DescribeSljitHashJoinProbeKeys(const vector<SljitNativeHashJoinProbeKeyPlan> &keys, const char *separator);
void AppendSljitHashJoinProbeMarkOffsets(string &result, const char *name, const SljitNativeHashJoinProbePlan &probe);

} // namespace duckdb
