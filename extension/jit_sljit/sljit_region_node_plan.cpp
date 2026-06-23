//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_node_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static constexpr const char *SLJIT_NATIVE_CONTRACT_UNSUPPORTED =
    "region IR node is unsupported by SLJIT native contract lowering";

bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan) {
	return !node_plan.native_ops.empty();
}

const SljitNativeRegionOpPlan &SljitRegionNodeFirstNativeOp(const SljitRegionNodePlan &node_plan) {
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops[0];
}

SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(SljitRegionNodePlan &node_plan) {
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops.back();
}

bool SljitRegionNodeHasSingleNativeOp(const SljitRegionNodePlan &node_plan) {
	return node_plan.native_ops.size() == 1;
}

void AppendSljitRegionNodeNativeOps(SljitNativeRegionPlan &region, SljitRegionNodePlan &node_plan) {
	for (auto &op : node_plan.native_ops) {
		region.ops.push_back(std::move(op));
	}
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

SljitRegionNodePlan SljitBlockedContractBoundary(const string &blocker, const char *reason) {
	return SljitRegionBoundaryNode(SljitBlockerOrReason(blocker, reason));
}

SljitRegionNodePlan SljitUnsupportedExpressionBoundaryNode(const string &error) {
	string reason = SLJIT_NATIVE_CONTRACT_UNSUPPORTED;
	if (!error.empty()) {
		reason += ";";
		reason += error;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

} // namespace duckdb
