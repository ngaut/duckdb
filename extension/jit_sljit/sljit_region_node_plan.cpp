//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_node_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan) {
	return !node_plan.native_ops.empty();
}

SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(SljitRegionNodePlan &node_plan) {
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops.back();
}

SljitRegionNodePlan SljitNativeNode(SljitNativeRegionOpPlan &&native_op, string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_ops.push_back(std::move(native_op));
	return result;
}

SljitRegionNodePlan SljitNativeNode(vector<SljitNativeRegionOpPlan> native_ops, string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_ops = std::move(native_ops);
	return result;
}

SljitRegionNodePlan SljitRegionBoundaryNode(string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::BOUNDARY;
	result.reason = std::move(reason);
	return result;
}

SljitRegionNodePlan SljitNodeBlockerBoundary(const ExecutionRegionNode &node, const char *fallback) {
	return SljitRegionBoundaryNode(node.blocker_reason.empty() ? string(fallback) : node.blocker_reason);
}

string SljitBlockerOrReason(const string &blocker, const char *reason) {
	return blocker.empty() ? string(reason) : blocker;
}

void AppendSljitReasonPart(string &reason, const string &part, bool render_diagnostics) {
	if (render_diagnostics && !part.empty()) {
		reason += ";";
		reason += part;
	}
}

SljitNativeRegionExpressionPlan SljitNativeReferenceExpression(idx_t source_index, const LogicalType &type, string ir,
                                                               bool references_region_input) {
	SljitNativeRegionExpressionPlan result;
	result.kind = SljitNativeRegionExpressionKind::REFERENCE;
	result.references_region_input = references_region_input;
	result.return_type = type;
	result.source_index = source_index;
	result.ir = std::move(ir);
	return result;
}

SljitRegionNodePlan SljitBlockedContractBoundary(const string &blocker, const char *reason) {
	return SljitRegionBoundaryNode(SljitBlockerOrReason(blocker, reason));
}

} // namespace duckdb
