//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_binding.hpp"

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_region_plan_internal.hpp"

#include <utility>

namespace duckdb {

SljitFullPipelineRecipeBinding::SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
                                                               const vector<LogicalType> &source_output_types_p,
                                                               const vector<Value> &source_min_values_p,
                                                               const vector<Value> &source_max_values_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p),
      projection_aggregate_recipes(ops_p, source_output_types_p, source_min_values_p, source_max_values_p) {
}

bool SljitFullPipelineRecipeBinding::TryMakeOperatorHandOverRecipe(SljitFullPipelineRecipe &recipe) const {
	return TryMakeNativeTailRecipe(MakeSourceSequence(), 0, recipe);
}

SljitFullPipelineRecipePlan
SljitFullPipelineRecipeBinding::MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
	return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
}

bool SljitFullPipelineRecipeBinding::TryMakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts,
                                                                      SljitFullPipelineRecipe &recipe) const {
	SljitFullPipelinePrimitiveStep probe_step;
	SljitGeneratedFilterPrimitive generated_filter;
	if (!TryMakeHashJoinProbeSelectionStep(facts.hash_join_idx, probe_step) ||
	    !SljitTryBindGeneratedFilterPrimitive(ops, facts.filter_idx, generated_filter)) {
		return false;
	}
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	if (!SljitTryBindPostJoinProjectionPrimitive(ops, facts.hash_join_idx, facts.first_post_join_projection_idx,
	                                             facts.final_post_join_projection_idx,
	                                             post_join_aggregate.post_join_projection)) {
		return false;
	}
	post_join_aggregate.aggregate_idx = facts.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate)) {
		return false;
	}

	auto sequence = MakeSourceSequence();
	if (!TryAddSourcePrefixSteps(sequence, facts.hash_join_idx)) {
		return false;
	}
	sequence.Add(std::move(probe_step));
	const auto probe_step_idx = sequence.Count() - 1;
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	auto direct_consumer = SljitMakeHashJoinDirectAggregateConsumerContract(
	    probe_step_idx, sequence.Count() - 1, facts.hash_join_idx, facts.aggregate_idx, facts.filter_idx);
	recipe = MakePrimitiveSequence(std::move(sequence), direct_consumer);
	return true;
}

bool SljitFullPipelineRecipeBinding::TryAddSourcePrefixSteps(SljitFullPipelinePrimitiveSequence &sequence,
                                                             idx_t prefix_end) const {
	idx_t op_idx = 0;
	while (op_idx < prefix_end) {
		if (ops[op_idx].Kind() == SljitNativeRegionOpKind::FILTER) {
			SljitGeneratedFilterPrimitive generated_filter;
			if (!SljitTryBindGeneratedFilterPrimitive(ops, op_idx, generated_filter)) {
				return false;
			}
			sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
			op_idx++;
			continue;
		}
		// Consecutive projections run as one chain.
		auto final_projection_idx = op_idx;
		while (final_projection_idx + 1 < prefix_end &&
		       ops[final_projection_idx + 1].Kind() == SljitNativeRegionOpKind::PROJECTION) {
			final_projection_idx++;
		}
		SljitProjectionChainPrimitive projection;
		if (!SljitTryBindProjectionChainPrimitive(ops, op_idx, final_projection_idx, projection)) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		op_idx = final_projection_idx + 1;
	}
	return true;
}

} // namespace duckdb
