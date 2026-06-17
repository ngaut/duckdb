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

#include "duckdb/execution/execution_region_admission.hpp"
#include "duckdb/execution/execution_region_graph.hpp"
#include "duckdb/execution/execution_region_registration.hpp"

namespace duckdb {

static idx_t SljitAutoAdmissionWorkUnits(const ExecutionRegionCandidate &candidate) {
	idx_t work_units = candidate.contract.generated_operator_count;
	work_units += candidate.traits.source_filter_expression_count;
	work_units += candidate.traits.filter_count;
	work_units += candidate.traits.projection_count;
	work_units += candidate.traits.operator_count;
	if (candidate.contract.OwnsSource()) {
		work_units++;
	}
	if (candidate.contract.OwnsTransform()) {
		work_units++;
	}
	if (candidate.contract.OwnsSink()) {
		work_units++;
	}
	if (candidate.contract.OwnsStateScan()) {
		work_units++;
	}
	return work_units;
}

static idx_t SljitAutoAdmissionWorkUnits(const ExecutionRegionPipelineInventory &inventory) {
	idx_t work_units = inventory.source_filter_count;
	work_units += inventory.filter_operator_count;
	work_units += inventory.projection_operator_count;
	work_units += inventory.operator_count;
	if (inventory.HasSource()) {
		work_units++;
	}
	if (inventory.HasSink()) {
		work_units++;
	}
	return work_units;
}

static bool SljitSourceIsStateful(ExecutionRegionSourceKind source_kind) {
	return source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR;
}

static bool SljitSinkHasNativeOperatorProtocol(ExecutionRegionSinkKind sink_kind) {
	switch (sink_kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
	case ExecutionRegionSinkKind::SORT:
	case ExecutionRegionSinkKind::MATERIALIZATION:
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::OPERATOR:
		return true;
	default:
		return false;
	}
}

static bool SljitCandidateHasNativeOperatorProtocol(const ExecutionRegionCandidate &candidate) {
	return candidate.traits.operator_count > 0 || SljitSinkHasNativeOperatorProtocol(candidate.traits.sink_kind);
}

static bool SljitInventoryHasNativeOperatorProtocol(const ExecutionRegionPipelineInventory &inventory) {
	return inventory.operator_count > 0 || SljitSinkHasNativeOperatorProtocol(inventory.sink_kind);
}

static bool SljitInventoryHasCoreExecutionBoundary(const ExecutionRegionPipelineInventory &inventory) {
	if (inventory.source_execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
		return true;
	}
	for (auto boundary : inventory.operator_boundaries) {
		switch (boundary) {
		case ExecutionRegionBoundaryKind::NONE:
		case ExecutionRegionBoundaryKind::OPERATOR_NATIVE:
			continue;
		default:
			return true;
		}
	}
	return false;
}

static bool SljitInventoryCanUseMeasuredAutoAdmission(const ExecutionRegionPipelineInventory &inventory,
                                                      string &reason) {
	if (!inventory.workload_relevant) {
		reason = inventory.workload_relevance_reason.empty() ? "pipeline has no compiled-region workload"
		                                                     : inventory.workload_relevance_reason;
		return false;
	}
	if (!inventory.HasSource() || !inventory.HasSink()) {
		reason = "pipeline does not own both source and sink protocols";
		return false;
	}
	if (SljitInventoryHasCoreExecutionBoundary(inventory)) {
		reason = "pipeline has a core execution boundary or missing contract";
		return false;
	}
	if (!SljitInventoryHasNativeOperatorProtocol(inventory)) {
		reason = "pipeline has no native operator protocol for auto admission";
		return false;
	}
	if (SljitAutoAdmissionWorkUnits(inventory) == 0) {
		reason = "pipeline has no native work units";
		return false;
	}
	return true;
}

static bool SljitCandidateCanUseMeasuredAutoAdmission(const ExecutionRegionCandidate &candidate, string &reason) {
	if (!ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
		reason = "candidate is not a full-pipeline region";
		return false;
	}
	if (candidate.contract.missing_contract_count != 0 || candidate.contract.source_boundary_count != 0) {
		reason = "candidate has a core execution boundary or missing contract";
		return false;
	}
	if (!candidate.contract.OwnsSource() || !candidate.contract.OwnsSink()) {
		reason = "candidate does not own both source and sink protocols";
		return false;
	}
	if (candidate.traits.expression_missing_count != 0 || candidate.traits.operator_missing_count != 0) {
		reason = "candidate has unsupported expression or operator traits";
		return false;
	}
	if (!candidate.stage_plan.HasStages()) {
		reason = "candidate has no core stage plan";
		return false;
	}
	if (!SljitCandidateHasNativeOperatorProtocol(candidate)) {
		reason = "candidate has no native operator protocol for auto admission";
		return false;
	}
	if (SljitAutoAdmissionWorkUnits(candidate) == 0) {
		reason = "candidate has no native work units";
		return false;
	}
	return true;
}

struct SljitAutoAdmissionMeasuredRule {
	const char *admission_key;
	idx_t min_cardinality;
	const char *proof;
};

static const SljitAutoAdmissionMeasuredRule *SljitAutoAdmissionMeasuredRules(idx_t &count) {
	count = 0;
	return nullptr;
}

static bool SljitHasMeasuredAutoAdmissionRules() {
	idx_t rule_count;
	SljitAutoAdmissionMeasuredRules(rule_count);
	return rule_count > 0;
}

static bool SljitLookupMeasuredAutoAdmissionRule(const string &admission_key, ExecutionRegionAdmissionRule &rule) {
	idx_t rule_count;
	auto rules = SljitAutoAdmissionMeasuredRules(rule_count);
	for (idx_t rule_idx = 0; rule_idx < rule_count; rule_idx++) {
		auto &entry = rules[rule_idx];
		if (admission_key != entry.admission_key) {
			continue;
		}
		rule.target = ExecutionRegionCompileTarget::REGION;
		rule.admission_key = admission_key;
		rule.min_cardinality = entry.min_cardinality;
		rule.proof = entry.proof;
		return true;
	}
	return false;
}

static bool SljitPopulateMeasuredAutoAdmissionRule(const ExecutionRegionCandidate &candidate, string admission_key,
                                                   ExecutionRegionAdmissionRule &rule,
                                                   const ExecutionRegionLoweringPlan *lowering_plan = nullptr) {
	string blocker;
	if (!SljitCandidateCanUseMeasuredAutoAdmission(candidate, blocker)) {
		return false;
	}
	if (lowering_plan && !ExecutionRegionLoweredRegionCanUseMeasuredAutoAdmission(candidate, *lowering_plan, blocker)) {
		return false;
	}
	return SljitLookupMeasuredAutoAdmissionRule(admission_key, rule);
}

static bool SljitPopulateMeasuredAutoAdmissionRule(const ExecutionRegionPipelineInventory &inventory,
                                                   string admission_key, ExecutionRegionAdmissionRule &rule) {
	string blocker;
	if (!SljitInventoryCanUseMeasuredAutoAdmission(inventory, blocker)) {
		return false;
	}
	return SljitLookupMeasuredAutoAdmissionRule(admission_key, rule);
}

class SljitExecutionRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "sljit";
	}

	string Description() const override {
		return "SLJIT execution region backend";
	}

	bool IsAvailable() const override {
		return SljitPlatformAvailable();
	}

	bool SupportsRegions() const override {
		return true;
	}

	bool HasAutoAdmissionRules(ExecutionRegionCompileTarget target) const override {
		return target == ExecutionRegionCompileTarget::REGION && SljitHasMeasuredAutoAdmissionRules();
	}

	bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionPipelineInventory &inventory,
	                          ExecutionRegionAdmissionRule &rule) const override {
		if (target != ExecutionRegionCompileTarget::REGION) {
			return false;
		}
		auto admission_key = BuildExecutionRegionAdmissionShapeKey(Name(), inventory);
		admission_key = BuildExecutionRegionAdmissionContextShapeKey(inventory, admission_key);
		return SljitPopulateMeasuredAutoAdmissionRule(inventory, std::move(admission_key), rule);
	}

	bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionCandidate &candidate,
	                          ExecutionRegionAdmissionRule &rule) const override {
		if (target != ExecutionRegionCompileTarget::REGION) {
			return false;
		}
		auto admission_key = BuildExecutionRegionAdmissionShapeKey(Name(), candidate.signature);
		admission_key = BuildExecutionRegionAdmissionContextShapeKey(candidate.signature, admission_key);
		return SljitPopulateMeasuredAutoAdmissionRule(candidate, std::move(admission_key), rule);
	}

	bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionCandidate &candidate,
	                          const ExecutionRegionLoweringPlan &lowering_plan,
	                          ExecutionRegionAdmissionRule &rule) const override {
		if (target != ExecutionRegionCompileTarget::REGION) {
			return false;
		}
		return SljitPopulateMeasuredAutoAdmissionRule(candidate, lowering_plan.shape_key, rule, &lowering_plan);
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		return AnalyzeSljitRegion(input);
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input) override {
		return CompileSljitRegion(Name(), input);
	}
};

void RegisterSljitExecutionRegionBackend(ExtensionLoader &loader) {
	RegisterExecutionRegionBackend(loader.GetDatabaseInstance(), make_uniq<SljitExecutionRegionBackend>());
}

} // namespace duckdb
