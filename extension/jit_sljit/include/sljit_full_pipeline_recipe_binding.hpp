//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_full_pipeline_recipe_sequence_builder.hpp"

namespace duckdb {

class SljitProjectionAggregateRecipeBinding;

class SljitFullPipelineRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_extended_source_fetch_budget_p);

	SljitFullPipelineRecipePlan MakeNativeOnlyPlan() const;

	SljitFullPipelineRecipePlan MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const;

	bool CanMakeNativeTailRecipe(idx_t tail_start_idx) const;

	SljitFullPipelineRecipe
	MakeProjectionFilterProjectionNativeTailRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts) const;

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const;

	SljitFullPipelineRecipe
	MakeSourceProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const;

	SljitFullPipelineRecipe MakeSourceUngroupedAggregateRecipe(const SljitSourceUngroupedAggregateFacts &facts) const;

	bool CanMakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts) const;

	SljitFullPipelineRecipe MakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts) const;

	bool CanMakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts) const;

	SljitFullPipelineRecipe MakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts) const;

	SljitFullPipelineRecipe
	MakeGeneratedFilterProjectionNativeTailRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts) const;

	SljitFullPipelineRecipe MakeHashJoinDelimJoinSinkRecipe(idx_t first_hash_join_idx, idx_t final_hash_join_idx,
	                                                        idx_t sink_idx) const;

	bool CanMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts) const;

	SljitFullPipelineRecipe MakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts) const;

	SljitFullPipelineRecipe
	MakeJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe MakeMarkFilterNativeTailRecipe(const SljitProjectionAggregatePrefixFacts &facts) const;

	bool ProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape,
	                                            bool allow_direct_projected_primitive_payload_update = false) const;

	bool CanMakeProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const;

private:
	SljitProjectionAggregateRecipeBinding ProjectionAggregateBinding() const;
};

} // namespace duckdb
