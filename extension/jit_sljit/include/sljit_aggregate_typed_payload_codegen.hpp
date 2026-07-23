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

enum class SljitAggregateExpressionIndexMode : uint8_t { FLAT, LOGICAL, SELECTED };

struct SljitSharedBinaryPayloadLane {
	bool matched = false;
	bool use_base_directly = false;
	bool base_on_left = true;
	const ExecutionExpressionIR *root = nullptr;
	const ExecutionExpressionIR *other_value = nullptr;
};

struct SljitSharedBinaryPayloadPlan {
	const ExecutionExpressionIR *base = nullptr;
	vector<SljitSharedBinaryPayloadLane> lanes;
	idx_t saved_node_count = 0;

	bool Enabled() const {
		return base != nullptr;
	}
};

struct SljitFusedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	idx_t tree_node_count = 0;
	bool fast_path_supported = false;
	bool conditional_shared_payload = false;
	idx_t conditional_lane = 0;
	idx_t shared_lane = 0;
	const ExecutionExpressionIR *conditional_predicate = nullptr;
	const ExecutionExpressionIR *shared_value = nullptr;
	SljitSharedBinaryPayloadPlan shared_binary;
};

bool SljitExpressionIRStructurallyEqual(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right);
void TryBuildSljitSharedBinaryPayloadPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                          const vector<SljitAggregatePayloadDescriptor> &descriptors,
                                          SljitSharedBinaryPayloadPlan &result);
bool BuildSljitFusedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                         const vector<ExecutionRegionAggregateInput> &aggregates,
                                         SljitFusedAggregateCodegenPlan &codegen_plan);
void EmitSljitSharedBinaryPayloadBase(struct sljit_compiler *compiler, const SljitSharedBinaryPayloadPlan &plan,
                                      sljit_s32 shared_value_reg, sljit_sw shared_value_offset,
                                      SljitAggregateExpressionIndexMode index_mode,
                                      vector<SljitExpressionTreeOverflowJumps> &overflows,
                                      const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr,
                                      bool restore_selected_source_array = false);
void EmitSljitSharedBinaryPayloadLane(struct sljit_compiler *compiler, const SljitSharedBinaryPayloadLane &lane,
                                      sljit_s32 shared_value_reg, sljit_sw shared_value_offset,
                                      SljitAggregateExpressionIndexMode index_mode,
                                      vector<SljitExpressionTreeOverflowJumps> &overflows,
                                      const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr,
                                      bool restore_selected_source_array = false);

} // namespace duckdb
