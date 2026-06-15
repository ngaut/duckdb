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

static bool TryRejectSljitAutoAdmissionCandidate(const JitRegionCandidate &candidate, JitAdmissionInfo &info,
                                                 string &reason) {
	info.has_admission = true;
	info.admission_key = BuildSljitRegionCandidateShapeKey(candidate);
	info.rule_present = false;
	if (JitRegionABIIsFullPipeline(candidate.contract.abi)) {
		reason = "jit_policy=auto skips region before backend analysis: SLJIT full-pipeline region "
		         "requires measured operator-aware admission proof;admission_rule=missing;shape=" +
		         info.admission_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) + ";" +
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
		info.has_admission = true;
		info.rule_present = false;
		info.admission_key = "sljit:pipeline-inventory";
		if (explain && !inventory.feature_shape.empty()) {
			info.admission_key += ":" + inventory.feature_shape;
		}
		if (!explain) {
			return false;
		}
		reason = "jit_policy=auto skips pipeline before typed IR lowering: no SLJIT auto admission family can match "
		         "pipeline inventory;admission_rule=missing;shape=" +
		         info.admission_key + ";estimated_cardinality=" + std::to_string(inventory.estimated_cardinality) +
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
		if (TryRejectSljitAutoAdmissionCandidate(candidate, info, reason)) {
			return false;
		}

		info.has_admission = true;
		info.admission_key = BuildSljitRegionCandidateShapeKey(candidate);
		info.rule_present = false;
		reason = "jit_policy=auto skips region before backend analysis: candidate cannot map to an admitted SLJIT "
		         "auto shape: shape=" +
		         info.admission_key + ";estimated_cardinality=" + std::to_string(candidate.estimated_cardinality) +
		         ";admission_rule=missing;" + candidate.traits.ir;
		return false;
	}

	bool GetAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate,
	                          const JitRegionLoweringPlan &, JitAutoAdmissionRule &rule) const override {
		(void)candidate;
		(void)rule;
		if (target != JitCompileTarget::REGION) {
			return false;
		}
		return false;
	}
};

void RegisterSljitJitBackend(ExtensionLoader &loader) {
	RegisterJitBackend(loader.GetDatabaseInstance(), make_uniq<SljitJitBackend>());
}

} // namespace duckdb
