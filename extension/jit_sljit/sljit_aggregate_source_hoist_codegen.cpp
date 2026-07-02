#include "sljit_aggregate_source_hoist_codegen.hpp"

#include <algorithm>

namespace duckdb {

static void AddSljitAggregateSourceUse(vector<pair<idx_t, idx_t>> &source_uses, idx_t source_index) {
	for (auto &entry : source_uses) {
		if (entry.first == source_index) {
			entry.second++;
			return;
		}
	}
	source_uses.emplace_back(source_index, 1);
}

static void CountSljitAggregateExpressionSourceUses(const ExecutionExpressionIR &node,
                                                    vector<pair<idx_t, idx_t>> &source_uses) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitAggregateSourceUse(source_uses, node.ref_index);
		return;
	}
	if (node.left) {
		CountSljitAggregateExpressionSourceUses(*node.left, source_uses);
	}
	if (node.right) {
		CountSljitAggregateExpressionSourceUses(*node.right, source_uses);
	}
	if (node.else_node) {
		CountSljitAggregateExpressionSourceUses(*node.else_node, source_uses);
	}
	for (auto &child : node.children) {
		if (child) {
			CountSljitAggregateExpressionSourceUses(*child, source_uses);
		}
	}
}

static vector<pair<idx_t, idx_t>>
BuildSljitAggregateSourceUseCounts(const vector<SljitNativeRegionExpressionPlan> &payloads) {
	vector<pair<idx_t, idx_t>> source_uses;
	for (auto &payload : payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			AddSljitAggregateSourceUse(source_uses, payload.source_index);
		} else if (payload.expression_tree) {
			CountSljitAggregateExpressionSourceUses(*payload.expression_tree, source_uses);
		}
	}
	std::sort(source_uses.begin(), source_uses.end(),
	          [](const pair<idx_t, idx_t> &left, const pair<idx_t, idx_t> &right) {
		          if (left.second != right.second) {
			          return left.second > right.second;
		          }
		          return left.first < right.first;
	          });
	return source_uses;
}

vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitAggregateSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                           const vector<sljit_s32> &data_regs, idx_t min_use_count) {
	vector<SljitTypedExpressionTreeDataPointerHoist> result;
	if (data_regs.empty()) {
		return result;
	}
	auto source_uses = BuildSljitAggregateSourceUseCounts(payloads);
	for (auto &entry : source_uses) {
		if (entry.second < min_use_count || result.size() >= data_regs.size()) {
			break;
		}
		SljitTypedExpressionTreeDataPointerHoist hoist;
		hoist.source_index = entry.first;
		hoist.data_reg = data_regs[result.size()];
		result.push_back(hoist);
	}
	return result;
}

} // namespace duckdb
