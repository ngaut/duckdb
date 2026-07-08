//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_delim_join_sink_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_hash_join_delim_join_sink_recipe.hpp"

namespace duckdb {

class SljitHashJoinDelimJoinSinkRecipeBuilder {
public:
	SljitHashJoinDelimJoinSinkRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                                        const SljitFullPipelineRecipeBinding &binding_p)
	    : ops(ops_p), binding(binding_p) {
	}

	bool Build(SljitFullPipelineRecipe &recipe, const SljitHashJoinDelimJoinSinkFacts &facts) const {
		if (!CanFitPrimitiveSequence(facts) || !CanBindDelimSink(facts) || !CanBindHashJoinInputs(facts)) {
			return false;
		}
		recipe = binding.MakeHashJoinDelimJoinSinkRecipe(facts.first_hash_join_idx, facts.final_hash_join_idx,
		                                                 facts.sink_idx);
		return true;
	}

private:
	bool CanFitPrimitiveSequence(const SljitHashJoinDelimJoinSinkFacts &facts) const {
		return facts.sink_idx + 3 <= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES;
	}

	bool CanBindDelimSink(const SljitHashJoinDelimJoinSinkFacts &facts) const {
		return SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx);
	}

	bool CanBindHashJoinInputs(const SljitHashJoinDelimJoinSinkFacts &facts) const {
		for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx <= facts.final_hash_join_idx;
		     hash_join_idx++) {
			if (hash_join_idx == facts.final_hash_join_idx) {
				if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
					return false;
				}
				continue;
			}
			if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
				return false;
			}
		}
		return true;
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipeBinding &binding;
};

bool SljitTryBuildHashJoinDelimJoinSinkRecipe(const vector<SljitExecutableRegionOp> &ops,
                                              const SljitFullPipelineRecipeBinding &binding,
                                              SljitFullPipelineRecipe &recipe,
                                              const SljitHashJoinDelimJoinSinkFacts &facts) {
	return SljitHashJoinDelimJoinSinkRecipeBuilder(ops, binding).Build(recipe, facts);
}

} // namespace duckdb
