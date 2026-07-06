//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_sequence_builder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitFullPipelineRecipeSequenceBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                                       const vector<LogicalType> &source_output_types_p,
	                                       const vector<Value> &source_min_values_p,
	                                       const vector<Value> &source_max_values_p,
	                                       bool uses_extended_source_fetch_budget_p)
	    : ops(ops_p), source_output_types(source_output_types_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p),
	      uses_extended_source_fetch_budget(uses_extended_source_fetch_budget_p) {
	}

protected:
	bool CanMakeNativeTailRecipe(idx_t tail_start_idx) const {
		return SljitNativeTailHandoffCanConsumeTail(ops, tail_start_idx);
	}

	SljitFullPipelineRecipe MakePrimitiveSequence(SljitFullPipelinePrimitiveSequence sequence) const {
		return SljitMakeFullPipelinePrimitiveRecipe(uses_extended_source_fetch_budget, std::move(sequence));
	}

	SljitFullPipelinePrimitiveSequence MakeSourceSequence() const {
		SljitFullPipelinePrimitiveSequence sequence;
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceFetch());
		return sequence;
	}

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t projection_idx) const {
		auto projection = SljitBindProjectionChainPrimitive(ops, projection_idx);
		return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
	}

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t first_projection_idx,
	                                                       idx_t final_projection_idx) const {
		auto projection = SljitBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx);
		return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
	}

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t projection_idx) const {
		sequence.Add(MakeProjectionChainStep(projection_idx));
	}

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t first_projection_idx,
	                            idx_t final_projection_idx) const {
		sequence.Add(MakeProjectionChainStep(first_projection_idx, final_projection_idx));
	}

	SljitFullPipelineRecipe MakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                             idx_t tail_start_idx) const {
		if (tail_start_idx >= ops.size()) {
			throw InternalException("SLJIT native-tail recipe has an invalid tail start operator");
		}
		if (!CanMakeNativeTailRecipe(tail_start_idx)) {
			throw InternalException("SLJIT native-tail recipe cannot consume generated aggregate payload tail");
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::NativeTailHandoff(tail_start_idx));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const {
		const auto source_key0_int64_to_int32_unchecked =
		    hash_join_idx == 0 &&
		    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
		auto primitive =
		    SljitBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
		return SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(primitive);
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const {
		const auto source_key0_int64_to_int32_unchecked =
		    hash_join_idx == 0 &&
		    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
		auto primitive =
		    SljitBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
		return SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(primitive);
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const {
		if (SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
			return MakeHashJoinProbeSelectionStep(hash_join_idx);
		}
		return MakeHashJoinProbeMaterializeStep(hash_join_idx);
	}

	bool SourceBatchBoundaryCanCoalesce(idx_t consumer_op_idx) const {
		if (source_output_types.empty() || consumer_op_idx >= ops.size()) {
			return false;
		}
		for (auto &type : source_output_types) {
			if (!TypeIsConstantSize(type.InternalType())) {
				return false;
			}
		}
		return true;
	}

	void AddSourceBatchBoundaryIfUseful(SljitFullPipelinePrimitiveSequence &sequence, idx_t consumer_op_idx) const {
		if (SourceBatchBoundaryCanCoalesce(consumer_op_idx)) {
			sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(consumer_op_idx));
		}
	}

protected:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<LogicalType> &source_output_types;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_extended_source_fetch_budget;
};

} // namespace duckdb
