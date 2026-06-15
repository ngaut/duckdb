//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_backend.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_backend.hpp"
#include "sljit_region.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_platform.hpp"

#include "duckdb/execution/jit/registration.hpp"

namespace duckdb {

struct SljitAutoAdmissionRuleSpec {
	const char *admission_key;
	idx_t min_cardinality;
	const char *proof;
};

static void SetSljitAutoAdmissionRule(const SljitAutoAdmissionRuleSpec &entry, JitAutoAdmissionRule &rule) {
	rule.target = JitCompileTarget::REGION;
	rule.shape_key = entry.admission_key;
	rule.min_cardinality = entry.min_cardinality;
	rule.proof = entry.proof;
}

static bool IsSljitNativeSourceFilterProjectionCandidate(const JitRegionCandidate &candidate) {
	if (!JitRegionABIIsSourcePipeline(candidate.contract.abi)) {
		return false;
	}
	if (!candidate.traits.has_table_scan_source ||
	    candidate.traits.source_execution != JitRegionSourceExecutionKind::NATIVE_SOURCE) {
		return false;
	}
	if (candidate.traits.source_filter_count == 0 || candidate.traits.source_filter_fallback_count > 0) {
		return false;
	}
	if (candidate.traits.filter_count > 0 || candidate.traits.projection_count > 0 ||
	    candidate.traits.operator_count > 0) {
		return false;
	}
	return candidate.traits.expression_traits_known && candidate.traits.expression_fallback_count == 0 &&
	       candidate.traits.operator_fallback_count == 0;
}

static bool IsSljitSourcePrefixFilterProjectionCandidate(const JitRegionCandidate &candidate) {
	if (!JitRegionABIIsSourcePipeline(candidate.contract.abi)) {
		return false;
	}
	if (!candidate.traits.has_table_scan_source) {
		return false;
	}
	if (candidate.traits.operator_count > 0 || candidate.traits.operator_fallback_count > 0 ||
	    candidate.traits.expression_fallback_count > 0) {
		return false;
	}
	if (candidate.traits.filter_count != 1 || candidate.traits.projection_count == 0) {
		return false;
	}
	if (!candidate.traits.expression_traits_known) {
		return true;
	}
	return candidate.traits.integer_comparison_filter_count > 0 &&
	       candidate.traits.non_integer_comparison_filter_count == 0 &&
	       candidate.traits.integer_arithmetic_projection_count > 0 &&
	       candidate.traits.non_integer_arithmetic_projection_count == 0;
}

static bool IsSljitSourcePrefixProjectionChainCandidate(const JitRegionCandidate &candidate) {
	if (!JitRegionABIIsSourcePipeline(candidate.contract.abi)) {
		return false;
	}
	if (!candidate.traits.has_table_scan_source) {
		return false;
	}
	if (candidate.traits.operator_count > 0 || candidate.traits.operator_fallback_count > 0 ||
	    candidate.traits.expression_fallback_count > 0) {
		return false;
	}
	if (candidate.traits.filter_count != 0 || candidate.traits.projection_count < 2) {
		return false;
	}
	return !candidate.traits.expression_traits_known || (candidate.traits.integer_arithmetic_projection_count > 0 &&
	                                                     candidate.traits.non_integer_arithmetic_projection_count == 0);
}

static bool IsSljitFilterProjectionInventory(const JitRegionPipelineInventory &inventory) {
	return inventory.has_table_scan_source && inventory.filter_operator_count == 1 &&
	       inventory.projection_operator_count > 0 &&
	       inventory.operator_count == inventory.filter_operator_count + inventory.projection_operator_count &&
	       !inventory.has_hash_join_operator && !inventory.has_hash_join_sink &&
	       !inventory.has_hash_aggregate_sink &&
	       !inventory.has_perfect_hash_aggregate_sink;
}

static bool IsSljitProjectionChainInventory(const JitRegionPipelineInventory &inventory) {
	return inventory.has_table_scan_source && inventory.filter_operator_count == 0 &&
	       inventory.projection_operator_count >= 2 &&
	       inventory.operator_count == inventory.projection_operator_count && !inventory.has_hash_join_operator &&
	       !inventory.has_hash_join_sink && !inventory.has_hash_aggregate_sink &&
	       !inventory.has_perfect_hash_aggregate_sink;
}

struct SljitAutoAdmissionFamily {
	SljitAutoAdmissionRuleSpec rule;
	bool (*candidate_matches)(const JitRegionCandidate &candidate);
	bool (*inventory_may_match)(const JitRegionPipelineInventory &inventory);
};

static constexpr SljitAutoAdmissionFamily SLJIT_AUTO_ADMISSION_FAMILIES[] = {
    {{SLJIT_SOURCE_PREFIX_FILTER_PROJECTION_SHAPE, 1000000, "benchmark/micro/jit/native_filter_projection"},
     IsSljitSourcePrefixFilterProjectionCandidate,
     IsSljitFilterProjectionInventory},
    {{SLJIT_SOURCE_PREFIX_PROJECTION_CHAIN_SHAPE, 1000000, "benchmark/micro/jit/native_projection_chain"},
     IsSljitSourcePrefixProjectionChainCandidate,
     IsSljitProjectionChainInventory}};

static bool TryGetSljitCandidateAutoAdmissionRule(const JitRegionCandidate &candidate, JitAutoAdmissionRule &rule) {
	for (auto &family : SLJIT_AUTO_ADMISSION_FAMILIES) {
		if (!family.candidate_matches(candidate)) {
			continue;
		}
		SetSljitAutoAdmissionRule(family.rule, rule);
		return true;
	}
	return false;
}

static void SetSljitAdmissionInfo(JitAdmissionInfo &info, const JitRegionCandidate &candidate,
                                  const JitAutoAdmissionRule &rule) {
	info.has_admission = true;
	info.shape_key = rule.shape_key;
	info.rule_present = true;
	info.min_cardinality = rule.min_cardinality;
	info.proof = rule.proof;
	info.has_score = true;
	info.score = static_cast<int64_t>(candidate.estimated_cardinality) - static_cast<int64_t>(rule.min_cardinality);
}

static bool TryRejectSljitAutoAdmissionCandidate(const JitRegionCandidate &candidate, JitAdmissionInfo &info,
                                                 string &reason) {
	info.has_admission = true;
	info.shape_key = BuildSljitRegionCandidateShapeKey(candidate);
	info.rule_present = false;
	if (IsSljitNativeSourceFilterProjectionCandidate(candidate)) {
		info.shape_key =
		    BuildSljitRegionCandidateContextShapeKey(candidate, SLJIT_SOURCE_PREFIX_FILTER_PROJECTION_SHAPE);
		reason =
		    "jit_policy=auto skips region before backend analysis: SLJIT native source-prefix filter/projection has "
		    "no admitted production performance proof;admission_rule=missing;shape=" +
		    info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) +
		    ";source_filter_count=" + std::to_string(candidate.traits.source_filter_count) +
		    ";source_projected_column_count=" + std::to_string(candidate.traits.source_projected_column_count) + ";" +
		    candidate.traits.ir;
		return true;
	}
	if (JitRegionABIIsSourcePipeline(candidate.contract.abi) && candidate.traits.source_filter_count > 0) {
		reason = "jit_policy=auto skips region before backend analysis: SLJIT scan-source work requires native source "
		         "fusion and measured source admission proof;admission_rule=missing;shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) +
		         ";source_filter_count=" + std::to_string(candidate.traits.source_filter_count) +
		         ";source_projected_column_count=" + std::to_string(candidate.traits.source_projected_column_count) + ";" +
		         candidate.traits.ir;
		return true;
	}
	if (JitRegionABIIsFullPipeline(candidate.contract.abi)) {
		reason = "jit_policy=auto skips region before backend analysis: SLJIT full-pipeline region "
		         "requires measured operator-aware admission proof;admission_rule=missing;shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) + ";" +
		         candidate.traits.ir;
		return true;
	}
	if (JitRegionABIIsSinkPipeline(candidate.contract.abi)) {
		reason = "jit_policy=auto skips region before backend analysis: SLJIT sink pipeline region "
		         "requires measured operator-aware admission proof;admission_rule=missing;shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) + ";" +
		         candidate.traits.ir;
		return true;
	}
	if (JitRegionABIIsChunkTransform(candidate.contract.abi)) {
		reason = "jit_policy=auto skips region before backend analysis: SLJIT post-source operator intervals have no "
		         "admitted performance proof;admission_rule=missing;shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) + ";" +
		         candidate.traits.ir;
		return true;
	}
	return false;
}

class SljitJitBackend : public JitBackend {
public:
	string Name() const override {
		return "sljit";
	}

	string Description() const override {
		return "SLJIT backend for DuckDB JIT framework";
	}

	bool IsAvailable() const override {
		return SljitPlatformAvailable();
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		return AnalyzeSljitRegion(input);
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &input) override {
		return CompileSljitRegion(Name(), input);
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionPipelineInventory &inventory, bool explain,
	                              JitAdmissionInfo &info, string &reason) const override {
		if (target != JitCompileTarget::REGION) {
			return false;
		}
		for (auto &family : SLJIT_AUTO_ADMISSION_FAMILIES) {
			if (family.inventory_may_match(inventory)) {
				return true;
			}
		}
		info.has_admission = true;
		info.rule_present = false;
		info.shape_key = "sljit:pipeline-inventory";
		if (explain && !inventory.feature_shape.empty()) {
			info.shape_key += ":" + inventory.feature_shape;
		}
		if (!explain) {
			return false;
		}
		reason = "jit_policy=auto skips pipeline before typed IR lowering: no SLJIT auto admission family can match "
		         "pipeline inventory;admission_rule=missing;shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(inventory.estimated_cardinality) +
		         ";source_filter_count=" + std::to_string(inventory.source_filter_count) +
		         ";source_projected_column_count=" + std::to_string(inventory.source_projected_column_count) +
		         ";features=" + inventory.feature_shape;
		return false;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate, JitAdmissionInfo &info,
	                              string &reason) const override {
		if (target != JitCompileTarget::REGION) {
			return true;
		}
		JitAutoAdmissionRule rule;
		if (TryGetSljitCandidateAutoAdmissionRule(candidate, rule)) {
			SetSljitAdmissionInfo(info, candidate, rule);
			if (candidate.estimated_cardinality >= rule.min_cardinality) {
				return true;
			}
			reason = "jit_policy=auto skips region before backend analysis: estimated cardinality below admitted SLJIT "
			         "auto threshold: shape=" +
			         rule.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) +
			         ";min_cardinality=" + std::to_string(rule.min_cardinality) + ";proof=" + rule.proof;
			return false;
		}
		if (TryRejectSljitAutoAdmissionCandidate(candidate, info, reason)) {
			return false;
		}

		info.has_admission = true;
		info.shape_key = BuildSljitRegionCandidateShapeKey(candidate);
		info.rule_present = false;
		reason = "jit_policy=auto skips region before backend analysis: candidate cannot map to an admitted SLJIT "
		         "auto shape: shape=" +
		         info.shape_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) +
		         ";admission_rule=missing;" + candidate.traits.ir;
		return false;
	}

	bool GetAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate,
	                          const JitRegionLoweringPlan &, JitAutoAdmissionRule &rule) const override {
		if (target != JitCompileTarget::REGION) {
			return false;
		}
		return TryGetSljitCandidateAutoAdmissionRule(candidate, rule);
	}
};

void RegisterSljitJitBackend(ExtensionLoader &loader) {
	RegisterJitBackend(loader.GetDatabaseInstance(), make_uniq<SljitJitBackend>());
}

} // namespace duckdb
