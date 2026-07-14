//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_typed_payload_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

struct SljitFusedTypedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	idx_t tree_node_count = 0;
	bool fast_path_supported = false;
	bool conditional_shared_payload = false;
	idx_t conditional_lane = 0;
	idx_t shared_lane = 0;
	const ExecutionExpressionIR *conditional_predicate = nullptr;
	const ExecutionExpressionIR *shared_value = nullptr;
	bool binary_shared_payload = false;
	idx_t binary_base_lane = 0;
	idx_t binary_dependent_lane = 0;
	bool binary_base_on_left = true;
	const ExecutionExpressionIR *binary_root = nullptr;
	const ExecutionExpressionIR *binary_other_value = nullptr;
};

bool BuildSljitFusedTypedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                              const vector<ExecutionRegionAggregateInput> &aggregates,
                                              SljitFusedTypedAggregateCodegenPlan &codegen_plan,
                                              bool force_typed_path = false);
void EmitSljitStoreBinarySharedPayloadValue(struct sljit_compiler *compiler, sljit_s32 value_reg,
                                            sljit_s32 shared_value_reg, sljit_sw shared_value_offset);
void EmitSljitBinarySharedPayloadValueReg(
    struct sljit_compiler *compiler, const SljitFusedTypedAggregateCodegenPlan &codegen_plan,
    sljit_s32 shared_value_reg, sljit_sw shared_value_offset, bool fast_path, bool no_source_selection,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr);

} // namespace duckdb
