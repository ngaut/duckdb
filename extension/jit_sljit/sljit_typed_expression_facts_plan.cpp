//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_facts_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

idx_t CountSljitTypedExpressionTreeNodes(const ExecutionExpressionIR &node) {
	idx_t result = 1;
	if (node.left) {
		result += CountSljitTypedExpressionTreeNodes(*node.left);
	}
	if (node.right) {
		result += CountSljitTypedExpressionTreeNodes(*node.right);
	}
	if (node.else_node) {
		result += CountSljitTypedExpressionTreeNodes(*node.else_node);
	}
	for (auto &child : node.children) {
		result += CountSljitTypedExpressionTreeNodes(*child);
	}
	return result;
}

bool SljitTypedExpressionTreeSourceKnownValid(const vector<idx_t> *known_valid_sources, idx_t source_index) {
	if (!known_valid_sources) {
		return false;
	}
	for (auto known_source : *known_valid_sources) {
		if (known_source == source_index) {
			return true;
		}
	}
	return false;
}

void AddSljitTypedKnownValidSource(vector<idx_t> &known_valid_sources, idx_t source_index) {
	for (auto known_source : known_valid_sources) {
		if (known_source == source_index) {
			return;
		}
	}
	known_valid_sources.push_back(source_index);
}

void CollectSljitTypedExpressionTreeReferences(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.ref_index);
		return;
	}
	if (node.left) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
	}
	if (node.right) {
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
	}
	if (node.else_node) {
		CollectSljitTypedExpressionTreeReferences(*node.else_node, known_valid_sources);
	}
	for (auto &child : node.children) {
		CollectSljitTypedExpressionTreeReferences(*child, known_valid_sources);
	}
}

SljitTypedExpressionTreeFastPathPlan BuildSljitTypedExpressionTreeFastPathPlan(const ExecutionExpressionIR &root,
                                                                               bool emit_flat_nullable_fast_path) {
	SljitTypedExpressionTreeFastPathPlan result;
	result.fast_path_supported = SljitTypedExpressionTreeFastPathSupported(root);
	const auto nullable_fast_path_candidate = emit_flat_nullable_fast_path && result.fast_path_supported;
	if (nullable_fast_path_candidate) {
		CollectSljitTypedExpressionTreeReferences(root, result.source_refs);
	}
	result.precheck_nulls_supported =
	    nullable_fast_path_candidate && SljitTypedExpressionTreeCanPrecheckNulls(root) && !result.source_refs.empty();
	result.hybrid_nulls_supported =
	    nullable_fast_path_candidate && !result.precheck_nulls_supported && !result.source_refs.empty();
	return result;
}

void CollectSljitTypedExpressionTreeTrueFacts(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NOT_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && node.left && node.right &&
	    SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeTrueFacts(*child, known_valid_sources);
		}
	}
}

void CollectSljitTypedExpressionTreeNotTrueFacts(const ExecutionExpressionIR &node,
                                                 vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeNotTrueFacts(*child, known_valid_sources);
		}
	}
}

} // namespace duckdb
