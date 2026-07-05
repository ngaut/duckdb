#include "sljit_aggregate_typed_payload_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static bool BuildSljitFusedTypedAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &payload,
                                                     const ExecutionRegionAggregateInput &aggregate,
                                                     SljitTypedExpressionTreePlan &payload_plan) {
	if (!aggregate.primitive_update_ready) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	    aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return false;
	}
	if (aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		payload_plan.supported = true;
		payload_plan.result_kind = payload.integer_kind;
		payload_plan.result_is_int64 = true;
		payload_plan.fast_path.fast_path_supported = true;
		return true;
	}
	if (payload.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !payload.expression_tree) {
		return false;
	}
	payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
	return SljitAggregateTypedPayloadPlanSupported(payload_plan, aggregate);
}

static bool SljitExpressionIRStructurallyEqual(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return DescribeExecutionExpressionIR(left) == DescribeExecutionExpressionIR(right);
}

static bool SljitExpressionIRIsNonNullZero(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && !node.constant.IsNull() && node.constant == int64_t(0);
}

static bool TryBuildSljitConditionalSharedAggregatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                        const vector<ExecutionRegionAggregateInput> &aggregates,
                                                        SljitFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (payloads.size() != 2 || aggregates.size() != 2) {
		return false;
	}
	if (aggregates[0].primitive_update_kind != aggregates[1].primitive_update_kind ||
	    (aggregates[0].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     aggregates[0].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		return false;
	}
	if (!payloads[0].expression_tree || !payloads[1].expression_tree ||
	    payloads[0].return_type != payloads[1].return_type) {
		return false;
	}
	for (idx_t conditional_lane = 0; conditional_lane < 2; conditional_lane++) {
		auto shared_lane = 1 - conditional_lane;
		auto &conditional = *payloads[conditional_lane].expression_tree;
		auto &shared = *payloads[shared_lane].expression_tree;
		if (conditional.kind != ExecutionExpressionIRKind::CASE || conditional.children.size() != 2 ||
		    !conditional.children[0] || !conditional.children[1] || !conditional.else_node ||
		    !SljitExpressionIRIsNonNullZero(*conditional.else_node) ||
		    !SljitExpressionIRStructurallyEqual(*conditional.children[1], shared)) {
			continue;
		}
		auto predicate_plan = BuildSljitTypedExpressionTreePlan(*conditional.children[0], false);
		auto value_plan = BuildSljitTypedExpressionTreePlan(shared, false);
		if (!predicate_plan.supported || !predicate_plan.result_is_bool || !value_plan.supported ||
		    !value_plan.result_is_int64) {
			return false;
		}
		codegen_plan.conditional_shared_payload = true;
		codegen_plan.conditional_lane = conditional_lane;
		codegen_plan.shared_lane = shared_lane;
		codegen_plan.conditional_predicate = conditional.children[0].get();
		codegen_plan.shared_value = &shared;
		codegen_plan.tree_node_count = predicate_plan.node_count + value_plan.node_count;
		codegen_plan.fast_path_supported =
		    predicate_plan.fast_path.fast_path_supported && value_plan.fast_path.fast_path_supported;
		return true;
	}
	return false;
}

static bool TryBuildSljitBinarySharedAggregatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                   const vector<ExecutionRegionAggregateInput> &aggregates,
                                                   SljitFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		return false;
	}
	for (idx_t base_lane = 0; base_lane < payloads.size(); base_lane++) {
		if (aggregates[base_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
		    payloads[base_lane].kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !payloads[base_lane].expression_tree) {
			continue;
		}
		auto &base = *payloads[base_lane].expression_tree;
		for (idx_t dependent_lane = base_lane + 1; dependent_lane < payloads.size(); dependent_lane++) {
			if (aggregates[dependent_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
			    payloads[dependent_lane].kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
			    !payloads[dependent_lane].expression_tree) {
				continue;
			}
			auto &dependent = *payloads[dependent_lane].expression_tree;
			if (dependent.kind != ExecutionExpressionIRKind::BINARY || dependent.arithmetic_overflow_check ||
			    !dependent.left || !dependent.right ||
			    SljitTypedExpressionTreeComparisonSupported(dependent.binary_op)) {
				continue;
			}
			SljitNativeIntegerBinaryOp native_op;
			if (!TryGetSljitExpressionTreeBinaryOp(dependent.binary_op, native_op)) {
				continue;
			}
			bool base_on_left = true;
			const ExecutionExpressionIR *other_value = nullptr;
			if (SljitExpressionIRStructurallyEqual(base, *dependent.left)) {
				other_value = dependent.right.get();
			} else if (SljitExpressionIRStructurallyEqual(base, *dependent.right)) {
				base_on_left = false;
				other_value = dependent.left.get();
			} else {
				continue;
			}
			auto other_plan = BuildSljitTypedExpressionTreePlan(*other_value, false);
			if (!other_plan.supported || !other_plan.result_is_int64 || !other_plan.fast_path.fast_path_supported) {
				continue;
			}
			codegen_plan.binary_shared_payload = true;
			codegen_plan.binary_base_lane = base_lane;
			codegen_plan.binary_dependent_lane = dependent_lane;
			codegen_plan.binary_base_on_left = base_on_left;
			codegen_plan.binary_root = &dependent;
			codegen_plan.binary_other_value = other_value;
			return true;
		}
	}
	return false;
}

bool BuildSljitFusedTypedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                              const vector<ExecutionRegionAggregateInput> &aggregates,
                                              SljitFusedTypedAggregateCodegenPlan &codegen_plan,
                                              bool force_typed_path) {
	if (payloads.size() != aggregates.size() || payloads.empty()) {
		return false;
	}
	codegen_plan = SljitFusedTypedAggregateCodegenPlan();
	codegen_plan.payloads.resize(payloads.size());
	codegen_plan.fast_path_supported = true;
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!BuildSljitFusedTypedAggregatePayloadPlan(payloads[payload_idx], aggregates[payload_idx],
		                                              codegen_plan.payloads[payload_idx])) {
			return false;
		}
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		has_typed_payload =
		    has_typed_payload || payloads[payload_idx].kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
		codegen_plan.tree_node_count += codegen_plan.payloads[payload_idx].node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	if (has_typed_payload) {
		TryBuildSljitConditionalSharedAggregatePlan(payloads, aggregates, codegen_plan);
		if (!codegen_plan.conditional_shared_payload) {
			TryBuildSljitBinarySharedAggregatePlan(payloads, aggregates, codegen_plan);
		}
	}
	return has_typed_payload || force_typed_path;
}

void EmitSljitBinarySharedPayloadValueReg(struct sljit_compiler *compiler,
                                          const SljitFusedTypedAggregateCodegenPlan &codegen_plan,
                                          sljit_sw shared_value_offset, bool fast_path, bool no_source_selection,
                                          vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(codegen_plan.binary_shared_payload);
	D_ASSERT(codegen_plan.binary_root);
	D_ASSERT(codegen_plan.binary_other_value);
	idx_t spill_index = 0;
	if (fast_path) {
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index, overflows,
		                                         data_hoists);
	} else if (no_source_selection) {
		EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index,
		                                                overflows, data_hoists);
	} else {
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index,
		                                                 overflows, data_hoists);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), shared_value_offset);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(codegen_plan.binary_root->binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT shared aggregate binary operator");
	}
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(*codegen_plan.binary_root);
	auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
	if (codegen_plan.binary_base_on_left) {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
	}
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

} // namespace duckdb
