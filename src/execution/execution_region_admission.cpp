#include "duckdb/execution/execution_region_admission.hpp"

namespace duckdb {

static void AppendExecutionRegionAdmissionKeyPart(string &key, const string &label, const string &value) {
	if (value.empty()) {
		return;
	}
	key += ":";
	key += label;
	key += ":";
	key += value;
}

string BuildExecutionRegionAdmissionShapeKey(const string &backend_name, const ExecutionRegionSignature &signature) {
	string result = backend_name + ":" + signature.context + ":" + signature.shape;
	if (!signature.feature_shape.empty()) {
		result += ":";
		result += signature.feature_shape;
	}
	return result;
}

string BuildExecutionRegionAdmissionContextShapeKey(const ExecutionRegionSignature &signature,
                                                    const string &shape_key) {
	auto result = shape_key;
	AppendExecutionRegionAdmissionKeyPart(result, "context", signature.context_feature_shape);
	AppendExecutionRegionAdmissionKeyPart(result, "contract", signature.contract_shape);
	return result;
}

string BuildExecutionRegionAdmissionShapeKey(const string &backend_name,
                                             const ExecutionRegionPipelineInventory &inventory) {
	string result = backend_name + ":full-pipeline:" + inventory.candidate_shape;
	if (!inventory.feature_shape.empty()) {
		result += ":";
		result += inventory.feature_shape;
	}
	return result;
}

string BuildExecutionRegionAdmissionContextShapeKey(const ExecutionRegionPipelineInventory &inventory,
                                                    const string &shape_key) {
	auto result = shape_key;
	AppendExecutionRegionAdmissionKeyPart(result, "context", inventory.feature_shape);
	AppendExecutionRegionAdmissionKeyPart(result, "contract", inventory.contract_shape);
	return result;
}

bool ExecutionRegionLoweredRegionCanUseMeasuredAutoAdmission(const ExecutionRegionCandidate &candidate,
                                                             const ExecutionRegionLoweringPlan &lowering_plan,
                                                             string &reason) {
	if (!ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
		reason = "lowered candidate is not a full-pipeline region";
		return false;
	}
	if (!ExecutionRegionExecutionModeIsCompiled(lowering_plan.ExpectedCompiledExecutionMode())) {
		reason = "lowered candidate is not executable";
		return false;
	}
	if (lowering_plan.ExpectedRegionExecutionForm() != ExecutionRegionForm::FUSED) {
		reason = "lowered candidate is not a fused operator region";
		return false;
	}
	if (lowering_plan.NativeCount() == 0) {
		reason = "lowered candidate has no native stages";
		return false;
	}
	return true;
}

} // namespace duckdb
