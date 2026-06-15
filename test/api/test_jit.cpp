#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/execution/jit/region.hpp"
#include "duckdb/execution/jit/runtime.hpp"
#include "duckdb/main/settings.hpp"

#include <algorithm>
#include <atomic>
#include <thread>
#include <type_traits>

using namespace duckdb;

static_assert(std::is_constructible<JitRegionCompilationInput, ClientContext &, const JitRegionIR &,
                                    const JitRegionCandidate &>::value,
              "JIT region backends must compile from core-owned region IR candidates");
static_assert(!std::is_constructible<JitRegionCompilationInput, ClientContext &, const JitRegionIR &>::value,
              "JIT region backend input must include an explicit selected candidate");
static_assert(!std::is_constructible<JitRegionCompilationInput, ClientContext &, Pipeline &>::value,
              "JIT region backend input must not expose DuckDB executor internals");
static_assert(std::is_same<decltype(&JitRegionKernel::TryExecute),
                           bool (JitRegionKernel::*)(DataChunk &, DataChunk &, idx_t, OperatorResultType &)>::value,
              "JIT region kernels must execute through the JIT runtime chunk ABI, not DuckDB executor internals");
static_assert(std::is_same<decltype(&JitRegionKernel::TrySink),
                           bool (JitRegionKernel::*)(ExecutionContext &, DataChunk &, OperatorSinkInput &,
                                                     SinkResultType &)>::value,
              "JIT sink kernels must execute through the JIT sink runtime ABI, not DuckDB executor internals");
static_assert(
    std::is_same<decltype(&JitRegionKernel::CanExecuteSourcePipeline), bool (JitRegionKernel::*)() const>::value,
    "JIT source pipeline kernels must advertise the source-prefix executable ABI explicitly");
static_assert(
    std::is_same<decltype(&JitRegionKernel::CanExecuteSinkPipeline), bool (JitRegionKernel::*)() const>::value,
    "JIT sink pipeline kernels must advertise the sink executable ABI explicitly");
static_assert(
    std::is_same<decltype(&JitRegionKernel::CanExecuteFullPipeline), bool (JitRegionKernel::*)() const>::value,
    "JIT full pipeline kernels must advertise the full-pipeline executable ABI explicitly");
static_assert(
    std::is_same<decltype(&JitRegionKernel::TryExecuteFullPipeline),
                 bool (JitRegionKernel::*)(JitFullPipelineRuntime &, JitFullPipelineResult &)>::value,
    "JIT full pipeline kernels must execute through the JIT full-pipeline runtime ABI, not DuckDB executor internals");

namespace {

static constexpr const char *JIT_HASH_JOIN_PROBE_READY_CONTRACT =
    "native_hash_join_probe_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_BUILD_READY_CONTRACT =
    "native_hash_join_build_contract_status=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_READY_BLOCKER = "native_hash_join_probe_blocker=none";
static constexpr const char *JIT_HASH_JOIN_BUILD_READY_BLOCKER = "native_hash_join_build_blocker=none";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_READY =
    "native-hash-join-probe-executable=ready";
static constexpr const char *JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON =
    "generated native hash join probe";
static constexpr const char *JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON =
    "generated native hash join build append protocol";

class UnitTestJitBackend : public JitBackend {
public:
	string Name() const override {
		return "unit_test_jit_backend";
	}

	string Description() const override {
		return "unit test JIT backend";
	}
};

class ContractTestRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteSourcePipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return true;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		throw InternalException("contract test region kernel should not execute");
	}

private:
	string backend_name = "contract_test_region_jit_backend";
};

static bool IsKnownJitCandidateScope(const string &scope) {
	return scope == "post_source_operator_interval" || scope == "source_pipeline" || scope == "sink_pipeline" ||
	       scope == "full_pipeline";
}

static bool IsCompiledRegionExecutionMode(const string &execution_mode) {
	return execution_mode == "native";
}

static void RequireCompiledRegionScopeHonesty(const JitEvent &event) {
	if (event.candidate_scope == "source_pipeline") {
		REQUIRE(IsCompiledRegionExecutionMode(event.execution_mode));
		REQUIRE(event.candidate_contract.abi == JitRegionABI::SOURCE_PREFIX);
		return;
	}
	if (event.candidate_scope == "sink_pipeline") {
		REQUIRE(IsCompiledRegionExecutionMode(event.execution_mode));
		REQUIRE(event.candidate_contract.abi == JitRegionABI::SINK_SUFFIX);
		return;
	}
	if (event.candidate_scope == "full_pipeline") {
		REQUIRE(IsCompiledRegionExecutionMode(event.execution_mode));
		REQUIRE(event.candidate_contract.abi == JitRegionABI::FULL_PIPELINE);
		return;
	}
	REQUIRE(event.candidate_scope == "post_source_operator_interval");
	REQUIRE(event.execution_mode == "native");
	REQUIRE(event.candidate_contract.abi == JitRegionABI::CHUNK_TRANSFORM);
}

static void RequireStatefulSourceMissingProtocolABI(const JitEvent &event, bool &found_state_scan_abi) {
	const auto &contract = event.candidate_contract;
	JitRegionABI expected_abi;
	if (contract.owns_sink) {
		expected_abi = JitRegionABI::FULL_PIPELINE;
	} else if (contract.owns_transform) {
		expected_abi = JitRegionABI::SOURCE_PREFIX;
	} else {
		expected_abi = JitRegionABI::STATE_SCAN;
		found_state_scan_abi = true;
	}
	REQUIRE(contract.abi == expected_abi);
	REQUIRE(StringUtil::Contains(contract.ir, "contract<abi=" + string(JitRegionABIToString(expected_abi))));
}

static void RequireStatefulSourceNativeProtocolABI(const JitEvent &event, bool &found_state_scan_abi) {
	const auto &contract = event.candidate_contract;
	JitRegionABI expected_abi;
	if (contract.owns_sink) {
		expected_abi = JitRegionABI::FULL_PIPELINE;
	} else if (contract.owns_transform) {
		expected_abi = JitRegionABI::SOURCE_PREFIX;
	} else {
		expected_abi = JitRegionABI::STATE_SCAN;
		found_state_scan_abi = true;
	}
	REQUIRE(contract.abi == expected_abi);
	REQUIRE(contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
	REQUIRE(contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
	REQUIRE(StringUtil::Contains(contract.ir, "contract<abi=" + string(JitRegionABIToString(expected_abi))));
}

static void RequireAutoInventorySkipEvent(const JitEvent &event, const string &shape_key, const string &feature_set) {
	REQUIRE_FALSE(event.has_candidate);
	REQUIRE(event.has_pipeline);
	REQUIRE(event.status == "skipped");
	REQUIRE(event.execution_mode == "executor_fallback");
	REQUIRE(event.region_execution_form == "none");
	REQUIRE(event.policy_decision == "auto");
	REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NONE);
	REQUIRE(event.has_admission);
	REQUIRE(event.admission_shape_key == shape_key);
	REQUIRE_FALSE(event.admission_rule_present);
	REQUIRE(event.admission_min_cardinality == 0);
	REQUIRE_FALSE(event.has_admission_score);
	REQUIRE(event.admission_proof.empty());
	REQUIRE(event.backend_analysis_time_us == 0);
	REQUIRE(event.compile_time_us == 0);
	REQUIRE(event.codegen_time_us == 0);
	REQUIRE(event.code_size == 0);
	REQUIRE(StringUtil::Contains(event.reason, "before typed IR lowering"));
	REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
	REQUIRE(StringUtil::Contains(event.reason, "shape=" + shape_key));
	REQUIRE(StringUtil::Contains(event.reason, "features=" + feature_set));
	REQUIRE(StringUtil::Contains(event.ir, "duckdb.region admission-inventory"));
	REQUIRE(StringUtil::Contains(event.ir, "features=" + feature_set));
}

static bool IsMaximalTransformCandidate(const JitRegionCompilationInput &input) {
	const auto scope = input.candidate.scope;
	return (scope == JitRegionCandidateScope::SOURCE_PIPELINE ||
	        scope == JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL) &&
	       input.candidate.end_operator_index > input.candidate.start_operator_index;
}

static JitRegionLoweringPlan UnsupportedContractBoundaryPlan() {
	JitRegionLoweringPlan plan;
	plan.SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED);
	plan.AddNode("boundary", "CONTRACT_BOUNDARY", JitLoweringKind::FALLBACK,
	             "contract backend only compiles maximal transform candidates");
	return plan;
}

class ZeroCodeRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	bool CanExecuteSourcePipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return true;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		throw InternalException("zero-code region kernel should not execute");
	}

private:
	string backend_name = "contract_test_zero_code_region_jit_backend";
};

class ZeroCodeRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_zero_code_region_jit_backend";
	}

	string Description() const override {
		return "contract test zero-code region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<ZeroCodeRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-zero-code-region");
	}
};

class NonCompiledKernelResultBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_non_compiled_kernel_result_jit_backend";
	}

	string Description() const override {
		return "contract test non-compiled kernel result JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		JitRegionCompileResult result;
		result.status = JitCompileStatus::UNSUPPORTED;
		result.execution_mode = JitExecutionMode::UNSUPPORTED;
		result.reason = "contract-test-non-compiled-kernel-result";
		result.kernel = make_uniq<ContractTestRegionKernel>();
		return result;
	}
};

class AutoRejectedCountingBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_reject_jit_backend";
	}

	string Description() const override {
		return "contract test auto-rejected JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-region-compiled");
	}

	atomic<idx_t> region_compile_count {0};
};

class AutoNonFusedAdmissionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_non_fused_jit_backend";
	}

	string Description() const override {
		return "contract test auto non-fused JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate, JitAdmissionInfo &info,
	                              string &) const override {
		REQUIRE(target == JitCompileTarget::REGION);
		info.has_admission = true;
		info.admission_key = "contract:auto-non-fused";
		info.rule_present = true;
		info.min_cardinality = 0;
		info.proof = "contract:auto-non-fused-proof";
		info.has_score = true;
		info.score = static_cast<int64_t>(candidate.estimated_cardinality);
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		region_analyze_count++;
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
			}
			JitRegionLoweringPlan plan;
			plan.shape_key = "contract:auto-non-fused";
			plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
			plan.SetRegionExecutionForm(JitRegionExecutionForm::NONE);
			plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
			plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native non-fused node");
			plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-auto-non-fused-compiled");
	}

	bool GetAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &,
	                          const JitRegionLoweringPlan &lowering_plan, JitAutoAdmissionRule &rule) const override {
		if (target != JitCompileTarget::REGION || lowering_plan.shape_key != "contract:auto-non-fused") {
			return false;
		}
		rule.target = target;
		rule.admission_key = lowering_plan.shape_key;
		rule.min_cardinality = 0;
		rule.proof = "contract:auto-non-fused-proof";
		return true;
	}

	atomic<idx_t> region_analyze_count {0};
	atomic<idx_t> region_compile_count {0};
};

class AutoPrecheckCountingBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_precheck_jit_backend";
	}

	string Description() const override {
		return "contract test auto precheck JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate, JitAdmissionInfo &info,
	                              string &reason) const override {
		precheck_count++;
		REQUIRE(target == JitCompileTarget::REGION);
		info.has_admission = true;
		info.admission_key = "contract:auto-precheck:" + candidate.shape;
		info.rule_present = false;
		reason = "jit_policy=auto skips region before backend analysis: contract precheck rejects candidate;shape=" +
		         info.admission_key + ";admission_rule=missing";
		return false;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &) override {
		region_analyze_count++;
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.shape_key = "contract:auto-precheck:unexpected-analysis";
		plan.AddNode("op", "CONTRACT_OPERATOR", JitLoweringKind::NATIVE, "contract native node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-auto-precheck-compiled");
	}

	mutable atomic<idx_t> precheck_count {0};
	atomic<idx_t> region_analyze_count {0};
	atomic<idx_t> region_compile_count {0};
};

class AutoInventoryGateCountingBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_inventory_gate_jit_backend";
	}

	string Description() const override {
		return "contract test auto inventory gate JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionPipelineInventory &inventory, bool explain,
	                              JitAdmissionInfo &info, string &reason) const override {
		inventory_precheck_count++;
		REQUIRE(target == JitCompileTarget::REGION);
		REQUIRE((inventory.has_source || inventory.operator_count > 0 || inventory.has_sink));
		if (explain) {
			diagnostic_inventory_count++;
			REQUIRE_FALSE(inventory.feature_shape.empty());
			REQUIRE_FALSE(inventory.pipeline_shape.empty());
			REQUIRE_FALSE(inventory.ir.empty());
		} else {
			admission_inventory_count++;
			REQUIRE(inventory.feature_shape.empty());
			REQUIRE(inventory.pipeline_shape.empty());
			REQUIRE(inventory.ir.empty());
		}
		info.has_admission = true;
		info.admission_key = "contract:auto-inventory:" + inventory.feature_shape;
		info.rule_present = false;
		if (explain) {
			reason =
			    "jit_policy=auto skips pipeline before typed IR lowering: contract inventory gate rejects pipeline";
		}
		return false;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget, const JitRegionCandidate &, JitAdmissionInfo &,
	                              string &) const override {
		candidate_precheck_count++;
		return false;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &) override {
		region_analyze_count++;
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("op", "CONTRACT_OPERATOR", JitLoweringKind::NATIVE, "contract native node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-auto-inventory-compiled");
	}

	mutable atomic<idx_t> inventory_precheck_count {0};
	mutable atomic<idx_t> admission_inventory_count {0};
	mutable atomic<idx_t> diagnostic_inventory_count {0};
	mutable atomic<idx_t> candidate_precheck_count {0};
	atomic<idx_t> region_analyze_count {0};
	atomic<idx_t> region_compile_count {0};
};

class AutoCandidatePrecheckRejectBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_candidate_precheck_jit_backend";
	}

	string Description() const override {
		return "contract test auto candidate precheck JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate, JitAdmissionInfo &info,
	                              string &reason) const override {
		candidate_precheck_count++;
		REQUIRE(target == JitCompileTarget::REGION);
		info.has_admission = true;
		info.admission_key = "contract:auto-candidate-precheck:" + candidate.shape;
		info.rule_present = true;
		info.min_cardinality = 1000000;
		info.proof = "contract:auto-candidate-precheck";
		info.has_score = true;
		info.score = static_cast<int64_t>(candidate.estimated_cardinality) - static_cast<int64_t>(info.min_cardinality);
		reason = "contract candidate precheck rejects candidate before backend analysis;min_cardinality=1000000;"
		         "proof=contract:auto-candidate-precheck";
		return false;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &) override {
		region_analyze_count++;
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.shape_key = "contract:auto-candidate-precheck:unexpected-analysis";
		plan.AddNode("op", "CONTRACT_OPERATOR", JitLoweringKind::NATIVE, "contract native node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-auto-candidate-precheck-compiled");
	}

	mutable atomic<idx_t> candidate_precheck_count {0};
	atomic<idx_t> region_analyze_count {0};
	atomic<idx_t> region_compile_count {0};
};

class AutoSelectionRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteSourcePipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return true;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		return false;
	}

private:
	string backend_name = "contract_test_auto_select_jit_backend";
};

class AutoSelectionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_auto_select_jit_backend";
	}

	string Description() const override {
		return "contract test auto-selection JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		JitRegionLoweringPlan plan;
		plan.shape_key = "contract:auto-selection:" + input.candidate.shape + ":candidate" +
		                 std::to_string(input.candidate.candidate_id);
		if (!IsMaximalTransformCandidate(input)) {
			plan.SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED);
			plan.AddNode("boundary", "CONTRACT_BOUNDARY", JitLoweringKind::FALLBACK,
			             "contract backend only admits maximal transform candidates");
			return plan;
		}
		if (!StringUtil::Contains(input.candidate.shape, "projection") ||
		    StringUtil::Contains(input.candidate.shape, "sink")) {
			plan.SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED);
			plan.AddNode("boundary", "CONTRACT_BOUNDARY", JitLoweringKind::FALLBACK,
			             "contract backend only admits projection interval candidates");
			return plan;
		}
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("op", "CONTRACT_OPERATOR", JitLoweringKind::NATIVE, "contract native selectable region");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &input) override {
		compiled_candidate_ids.push_back(input.candidate.candidate_id);
		compiled_candidate_shapes.push_back(input.candidate.shape);
		return JitRegionCompileResult::Compiled(make_uniq<AutoSelectionRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-auto-selected-region");
	}

	bool GetAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate,
	                          const JitRegionLoweringPlan &lowering_plan, JitAutoAdmissionRule &rule) const override {
		if (target != JitCompileTarget::REGION) {
			return false;
		}
		if (!StringUtil::Contains(candidate.shape, "projection")) {
			return false;
		}
		rule.target = target;
		rule.admission_key = lowering_plan.shape_key;
		rule.min_cardinality = StringUtil::Contains(candidate.shape, "projection-projection") ? 1 : 0;
		rule.proof = "contract:auto-selection";
		return true;
	}

	vector<idx_t> compiled_candidate_ids;
	vector<string> compiled_candidate_shapes;
};

class ImplicitModeRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_implicit_mode_region_jit_backend";
	}

	string Description() const override {
		return "contract test implicit-mode region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &) override {
		JitRegionLoweringPlan plan;
		plan.AddNode("op", "CONTRACT_OPERATOR", JitLoweringKind::NATIVE,
		             "contract native node without explicit region mode");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-implicit-mode-region");
	}

	atomic<idx_t> region_compile_count {0};
};

class ThrowingVerifiedRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteSourcePipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return true;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		throw InternalException("contract test region runtime failure");
	}

private:
	string backend_name = "contract_test_throwing_region_jit_backend";
};

class ThrowingVerifiedRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_throwing_region_jit_backend";
	}

	string Description() const override {
		return "contract test throwing region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<ThrowingVerifiedRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-throwing-region");
	}
};

class DecliningRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteSourcePipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return true;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		return false;
	}

private:
	string backend_name = "contract_test_declining_region_jit_backend";
};

class DecliningRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_declining_region_jit_backend";
	}

	string Description() const override {
		return "contract test declining region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::FALLBACK, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", JitLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::FALLBACK, "contract sink boundary");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<DecliningRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-declining-region");
	}
};

class SourceAbiRejectRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool TryExecute(DataChunk &, DataChunk &, idx_t, OperatorResultType &) override {
		throw InternalException("source-prefix ABI rejection test should not execute");
	}

private:
	string backend_name = "contract_test_source_abi_region_jit_backend";
};

class SourceAbiRejectRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_source_abi_region_jit_backend";
	}

	string Description() const override {
		return "contract test source-prefix ABI region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::SOURCE_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("source", "CONTRACT_SOURCE", JitLoweringKind::NATIVE,
		             "contract source pipeline node without source-prefix ABI");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		region_compile_count++;
		return JitRegionCompileResult::Compiled(make_uniq<SourceAbiRejectRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-source-abi-region");
	}

	atomic<idx_t> region_compile_count {0};
};

class SinkAbiRejectRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool TrySink(ExecutionContext &, DataChunk &, OperatorSinkInput &, SinkResultType &) override {
		throw InternalException("sink ABI rejection test should not execute");
	}

private:
	string backend_name = "contract_test_sink_abi_region_jit_backend";
};

class SinkAbiRejectRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_sink_abi_region_jit_backend";
	}

	string Description() const override {
		return "contract test sink ABI region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::SINK_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::NATIVE,
		             "contract sink pipeline node without sink executable ABI");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<SinkAbiRejectRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-sink-abi-region");
	}
};

class FullPipelineAbiRejectRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

private:
	string backend_name = "contract_test_full_pipeline_abi_region_jit_backend";
};

class FullPipelineAbiRejectRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_full_pipeline_abi_region_jit_backend";
	}

	string Description() const override {
		return "contract test full-pipeline ABI region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::FULL_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("full", "CONTRACT_FULL_PIPELINE", JitLoweringKind::NATIVE,
		             "contract full pipeline node without full-pipeline executable ABI");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<FullPipelineAbiRejectRegionKernel>(),
		                                        JitExecutionMode::NATIVE, "contract-test-full-pipeline-abi-region");
	}
};

class DecliningFullPipelineRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(JitFullPipelineRuntime &, JitFullPipelineResult &) override {
		return false;
	}

private:
	string backend_name = "contract_test_declining_full_pipeline_region_jit_backend";
};

class DecliningFullPipelineRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_declining_full_pipeline_region_jit_backend";
	}

	string Description() const override {
		return "contract test declining full-pipeline region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::FULL_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("full", "CONTRACT_FULL_PIPELINE", JitLoweringKind::NATIVE,
		             "contract declining full pipeline node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<DecliningFullPipelineRegionKernel>(),
		                                        JitExecutionMode::NATIVE,
		                                        "contract-test-declining-full-pipeline-region");
	}
};

class SideEffectDecliningFullPipelineRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &) override {
		runtime.RecordNativeSinkResult(0, SinkResultType::NEED_MORE_INPUT);
		return false;
	}

private:
	string backend_name = "contract_test_side_effect_declining_full_pipeline_region_jit_backend";
};

class SideEffectDecliningFullPipelineRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_side_effect_declining_full_pipeline_region_jit_backend";
	}

	string Description() const override {
		return "contract test side-effect declining full-pipeline region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::FULL_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("full", "CONTRACT_FULL_PIPELINE", JitLoweringKind::NATIVE,
		             "contract side-effect declining full pipeline node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<SideEffectDecliningFullPipelineRegionKernel>(),
		                                        JitExecutionMode::NATIVE,
		                                        "contract-test-side-effect-declining-full-pipeline-region");
	}
};

class DecliningSinkRegionKernel : public JitRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteSinkPipeline() const override {
		return true;
	}

	bool TrySink(ExecutionContext &, DataChunk &, OperatorSinkInput &, SinkResultType &) override {
		return false;
	}

private:
	string backend_name = "contract_test_declining_sink_region_jit_backend";
};

class DecliningSinkRegionBackend : public JitBackend {
public:
	string Name() const override {
		return "contract_test_declining_sink_region_jit_backend";
	}

	string Description() const override {
		return "contract test declining sink region JIT backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input) override {
		if (input.candidate.scope != JitRegionCandidateScope::SINK_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		JitRegionLoweringPlan plan;
		plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(JitRegionExecutionForm::FUSED);
		plan.AddNode("sink", "CONTRACT_SINK", JitLoweringKind::NATIVE, "contract declining sink pipeline node");
		return plan;
	}

	JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &) override {
		return JitRegionCompileResult::Compiled(make_uniq<DecliningSinkRegionKernel>(), JitExecutionMode::NATIVE,
		                                        "contract-test-declining-sink-region");
	}
};

class CountingCodeHandle : public JitCodeHandle {
public:
	explicit CountingCodeHandle(bool &destroyed_p) : destroyed(destroyed_p) {
	}

	~CountingCodeHandle() override {
		destroyed = true;
	}

	idx_t CodeSize() const override {
		return 17;
	}

private:
	bool &destroyed;
};

static idx_t TotalJitCounterCount(const vector<JitCounter> &counters) {
	idx_t result = 0;
	for (auto &counter : counters) {
		result += counter.count;
	}
	return result;
}

static idx_t TotalJitDecisionCounterCount(const vector<JitDecisionCounter> &counters) {
	idx_t result = 0;
	for (auto &counter : counters) {
		result += counter.count;
	}
	return result;
}

static bool ContainsTypedIrNode(const string &ir, const string &node_kind, const string &logical_type,
                                const string &physical_type) {
	return StringUtil::Contains(ir, node_kind + "<logical=" + logical_type + ",physical=" + physical_type);
}

static void RequireRegionBoundaryCoreIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "core=(duckdb.region typed-vector-ir"));
	REQUIRE(StringUtil::Contains(ir, "candidate0<first_node="));
	REQUIRE(StringUtil::Contains(ir, "boundary=scan"));
	REQUIRE(StringUtil::Contains(ir, "boundary=sink"));
}

static void RequireFilterCoreRegionIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "input_format=unified-vector,output_format=selection-vector"));
	REQUIRE(StringUtil::Contains(ir, "vector_source=region-input"));
	REQUIRE(StringUtil::Contains(ir, "selection_source=input-selection"));
}

static void RequireProjectionCoreRegionIr(const string &ir) {
	REQUIRE(StringUtil::Contains(ir, "input_format=unified-vector,output_format=flat-vector"));
	REQUIRE(StringUtil::Contains(ir, "vector_source=operator-output"));
	REQUIRE(StringUtil::Contains(ir, "selection_source=filter-selection"));
}

static void SetJitTestOptions(ClientContext &context, const string &backend_name) {
	Settings::Set<EnableJitSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	Settings::Set<JitBackendSetting>(context, SetScope::SESSION, Value(backend_name));
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("force"));
}

} // namespace

TEST_CASE("JIT manager registers and selects database-local backends", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<UnitTestJitBackend>());

	bool found_backend = false;
	for (auto &backend : manager.GetBackends(&context)) {
		if (backend.name != "unit_test_jit_backend") {
			continue;
		}
		found_backend = true;
		REQUIRE(backend.description == "unit test JIT backend");
		REQUIRE(backend.available);
		REQUIRE(!backend.supports_regions);
	}
	REQUIRE(found_backend);

	REQUIRE_NO_FAIL(con.Query("SET jit_backend='unit_test_jit_backend'"));

	bool selected_backend = false;
	for (auto &backend : manager.GetBackends(&context)) {
		if (backend.name == "unit_test_jit_backend") {
			selected_backend = backend.selected;
		}
	}
	REQUIRE(selected_backend);

	auto missing_backend = con.Query("SET jit_backend='missing_jit_backend'");
	REQUIRE(missing_backend->HasError());
	REQUIRE(StringUtil::Contains(missing_backend->GetError(), "JIT backend \"missing_jit_backend\" is not registered"));
}

TEST_CASE("JIT manager records compile events from the selected backend", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_unsupported_full_pipeline = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(event.target != "expression");
		if (event.backend_name == "sljit" && event.target == "region" && event.phase == "decision" &&
		    event.status == "unsupported" && event.candidate_scope == "full_pipeline" &&
		    event.candidate_shape == "filter-projection-sink") {
			found_unsupported_full_pipeline = true;
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.has_candidate);
			REQUIRE(!event.candidate_pipeline_shape.empty());
			REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "source:source:"));
			REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "op0:filter:FILTER:none"));
			REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "op1:projection:PROJECTION:none"));
			REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, ":sink:"));
			REQUIRE(StringUtil::Contains(event.candidate_context_pipeline_shape, "source:source:"));
			REQUIRE(StringUtil::Contains(event.candidate_context_pipeline_shape, "sink:sink:"));
			REQUIRE(event.candidate_node_count > 0);
			REQUIRE(event.candidate_start_operator_index == 0);
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
			REQUIRE(event.candidate_estimated_cardinality > 0);
			REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native="));
			REQUIRE(StringUtil::Contains(event.reason, "source-fusion-gap:requires-native-source"));
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:fallback"));
			REQUIRE(StringUtil::Contains(event.reason, "op0:FILTER:native:generated typed predicate filter"));
			REQUIRE(StringUtil::Contains(event.reason, "op1:PROJECTION:native:generated typed projection"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:fallback:"
			                                           "full pipeline sink requires native sink or operator update protocol"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
			REQUIRE(event.code_size == 0);
			REQUIRE(StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir"));
			RequireFilterCoreRegionIr(event.ir);
			RequireProjectionCoreRegionIr(event.ir);
		}
	}
	REQUIRE(found_unsupported_full_pipeline);
}

TEST_CASE("JIT auto policy skips kernels without admitted performance proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_policy_small AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i)"));

	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetCounters().empty());
	REQUIRE(manager.GetDecisionCounters().empty());

	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i)"));

	bool found_auto_skip_ir = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name == "sljit" && event.status == "skipped" && event.execution_mode == "executor_fallback" &&
		    event.policy_decision == "auto" && event.has_admission && !event.admission_rule_present &&
		    !event.ir.empty()) {
			found_auto_skip_ir = true;
			REQUIRE(StringUtil::Contains(event.ir, "duckdb."));
			REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
		}
	}
	REQUIRE(found_auto_skip_ir);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_auto_policy_small WHERE i > 500"));

	bool found_inventory_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name == "sljit" && event.target == "region" && event.status == "skipped" &&
		    event.policy_decision == "auto" &&
		    event.admission_shape_key == "sljit:pipeline-inventory:table-scan-source") {
			found_inventory_skip = true;
			REQUIRE(event.execution_mode == "executor_fallback");
			REQUIRE_FALSE(event.has_candidate);
			REQUIRE(event.has_pipeline);
			REQUIRE(event.has_admission);
			REQUIRE_FALSE(event.admission_rule_present);
			REQUIRE(event.backend_analysis_time_us == 0);
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
			REQUIRE(StringUtil::Contains(event.reason, "before typed IR lowering"));
			REQUIRE(StringUtil::Contains(event.reason, "features=table-scan-source"));
		}
	}
	REQUIRE(found_inventory_skip);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM range(3) tbl(i) WHERE i > 1)"));

	bool found_operator_aware_full_pipeline_key = false;
	for (auto &event : manager.GetEvents()) {
			if (event.backend_name == "sljit" && event.target == "region" && event.status == "skipped" &&
			    event.policy_decision == "auto" && event.candidate_scope == "full_pipeline" &&
			    StringUtil::Contains(event.candidate_pipeline_shape, "sink:sink:UNGROUPED_AGGREGATE:sink-native")) {
			found_operator_aware_full_pipeline_key = true;
			REQUIRE(event.has_admission);
			REQUIRE(StringUtil::StartsWith(event.admission_shape_key, "sljit:full-pipeline:"));
			REQUIRE(StringUtil::Contains(event.admission_shape_key, "ungrouped-aggregate-update"));
			REQUIRE(StringUtil::Contains(event.reason, "operator-aware admission proof"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "not implemented"));
			REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
		}
	}
	REQUIRE(found_operator_aware_full_pipeline_key);
}

TEST_CASE("JIT auto admission rejects non-integer projection chains before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_projection_chain AS "
	                          "SELECT i, CAST(i AS DECIMAL(15,2)) AS d FROM range(1000000) t(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(y) FROM ("
	                          "SELECT x + CAST(1 AS DECIMAL(15,2)) AS y FROM ("
	                          "SELECT d * CAST(2 AS DECIMAL(15,2)) AS x FROM jit_decimal_projection_chain"
	                          ") p) q"));

	bool found_decimal_projection_chain_skip = false;
	bool found_decimal_projection_chain_backend_analysis = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region" || event.candidate_scope != "full_pipeline" ||
		    !StringUtil::Contains(event.candidate_shape, "projection-projection") ||
		    !StringUtil::Contains(event.candidate_shape, "sink") || !event.candidate_traits.has_table_scan_source ||
		    event.candidate_traits.non_integer_arithmetic_projection_count == 0) {
			continue;
		}
		found_decimal_projection_chain_skip = true;
		found_decimal_projection_chain_backend_analysis = found_decimal_projection_chain_backend_analysis ||
		                                                  event.backend_analysis_time_us != 0 ||
		                                                  event.status == "unsupported";
		REQUIRE(event.status == "skipped");
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.policy_decision == "auto");
		REQUIRE(event.has_admission);
		REQUIRE_FALSE(event.admission_rule_present);
		REQUIRE(event.backend_analysis_time_us == 0);
		REQUIRE(event.compile_time_us == 0);
		REQUIRE(event.codegen_time_us == 0);
		REQUIRE(event.candidate_traits.integer_arithmetic_projection_count == 0);
		REQUIRE(event.candidate_traits.non_integer_arithmetic_projection_count > 0);
		REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
		REQUIRE(StringUtil::Contains(event.reason, "operator-aware admission proof"));
		REQUIRE(StringUtil::Contains(event.reason, "non_integer_arithmetic_projections="));
		REQUIRE(StringUtil::Contains(event.ir, "logical=DECIMAL"));
		REQUIRE(StringUtil::Contains(event.ir, event.candidate_shape));
	}
	REQUIRE(found_decimal_projection_chain_skip);
	REQUIRE_FALSE(found_decimal_projection_chain_backend_analysis);
}

TEST_CASE("JIT lowers date year intrinsic as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_date_year_native(d DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_date_year_native VALUES "
	                          "(DATE '1992-02-29'), (DATE '-0001-01-01'), "
	                          "(DATE 'infinity'), (NULL)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT year(d) AS y FROM jit_date_year_native ORDER BY y NULLS LAST");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == -1);
	REQUIRE(result->GetValue(0, 1).GetValue<int64_t>() == 1992);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(0, 3).IsNull());

	bool found_native_date_year = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "date_year")) {
			found_native_date_year = true;
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(event.candidate_shape == "projection");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "date_year"));
		}
	}
	REQUIRE(found_native_date_year);
}

TEST_CASE("JIT lowers integral compression intrinsic as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_integral_compress_native AS "
	                          "SELECT CASE WHEN range=10 THEN NULL ELSE (range + 300)::INTEGER END i "
	                          "FROM range(11)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT i FROM jit_integral_compress_native ORDER BY i NULLS LAST");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 11);
	for (idx_t row_idx = 0; row_idx < 10; row_idx++) {
		REQUIRE(result->GetValue(0, row_idx).GetValue<int32_t>() == 300 + static_cast<int32_t>(row_idx));
	}
	REQUIRE(result->GetValue(0, 10).IsNull());

	bool found_native_integral_compress = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_integral_compress_fallback =
		    StringUtil::Contains(event.reason, "__internal_compress_integral") &&
		    StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_integral_compress_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "integral_compress")) {
			found_native_integral_compress = true;
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(event.candidate_shape == "projection");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "integral_compress"));
		}
	}
	REQUIRE(found_native_integral_compress);
}

TEST_CASE("JIT lowers integral decompression intrinsic as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_integral_decompress_native AS "
	                          "SELECT CASE WHEN range=10 THEN NULL ELSE (range + 1992)::BIGINT END y "
	                          "FROM range(11)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT y FROM jit_integral_decompress_native ORDER BY y NULLS LAST");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 11);
	for (idx_t row_idx = 0; row_idx < 10; row_idx++) {
		REQUIRE(result->GetValue(0, row_idx).GetValue<int64_t>() == 1992 + static_cast<int64_t>(row_idx));
	}
	REQUIRE(result->GetValue(0, 10).IsNull());

	bool found_native_integral_decompress = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_integral_decompress_fallback =
		    StringUtil::Contains(event.reason, "__internal_decompress_integral") &&
		    StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_integral_decompress_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "integral_decompress")) {
			found_native_integral_decompress = true;
			REQUIRE(event.candidate_shape == "projection");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "integral_decompress"));
		}
	}
	REQUIRE(found_native_integral_decompress);
}

TEST_CASE("JIT canonicalizes no-op optional table filters as constant predicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_optional_table_filter AS "
	                          "SELECT range::BIGINT a, (10000 - range)::BIGINT b FROM range(10000)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT count(*) FROM jit_optional_table_filter WHERE a < 5 OR a > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 9994);

	bool found_optional_filter_constant = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_optional_filter_fallback =
		    StringUtil::Contains(event.reason, "__internal_tablefilter_optional") &&
		    StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_optional_filter_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && event.candidate_traits.source_filter_count > 0 &&
		    StringUtil::Contains(event.ir, "source-filter#") &&
		    StringUtil::Contains(event.ir, "constant<logical=BOOLEAN") && StringUtil::Contains(event.ir, ">(true)")) {
			found_optional_filter_constant = true;
			REQUIRE(event.candidate_traits.source_filter_fallback_count == 0);
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
		}
	}
	REQUIRE(found_optional_filter_constant);
}

TEST_CASE("JIT lowers constant string prefix predicates as native predicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_prefix_native(id INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_prefix_native VALUES "
	                          "(1, 'PROMOabc'), "
	                          "(2, 'plain'), "
	                          "(3, 'PROM'), "
	                          "(4, NULL), "
	                          "(5, 'PROMOTION-LONG-STRING-abcdefghijklmnopqrstuvwxyz')"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto projection = con.Query("SELECT id, prefix(s, 'PROMO') AS p FROM jit_string_prefix_native ORDER BY id");
	REQUIRE_NO_FAIL(*projection);
	REQUIRE(projection->RowCount() == 5);
	REQUIRE(projection->GetValue(1, 0).GetValue<bool>());
	REQUIRE_FALSE(projection->GetValue(1, 1).GetValue<bool>());
	REQUIRE_FALSE(projection->GetValue(1, 2).GetValue<bool>());
	REQUIRE(projection->GetValue(1, 3).IsNull());
	REQUIRE(projection->GetValue(1, 4).GetValue<bool>());

	auto count = con.Query("SELECT count(*) FROM jit_string_prefix_native WHERE prefix(s, 'PROMO')");
	REQUIRE_NO_FAIL(*count);
	REQUIRE(count->RowCount() == 1);
	REQUIRE(count->GetValue(0, 0).GetValue<int64_t>() == 2);

	bool found_native_string_prefix = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_prefix_fallback = StringUtil::Contains(event.reason, "function=prefix") &&
		                             StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_prefix_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "string_prefix")) {
			found_native_string_prefix = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
		}
	}
	REQUIRE(found_native_string_prefix);
}

TEST_CASE("JIT lowers constant string match predicates as native predicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_match_native(id INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_match_native VALUES "
	                          "(1, 'EUROPE BRASS'), "
	                          "(2, 'forest green part'), "
	                          "(3, 'ordinary special shipping requests here'), "
	                          "(4, 'special only'), "
	                          "(5, NULL)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto projection =
	    con.Query("SELECT id, suffix(s, 'BRASS') AS suf, contains(s, 'green') AS has_green, "
	              "s LIKE '%special%requests%' AS ordered_like, "
	              "s NOT LIKE '%special%requests%' AS ordered_not_like "
	              "FROM jit_string_match_native ORDER BY id");
	REQUIRE_NO_FAIL(*projection);
	REQUIRE(projection->RowCount() == 5);
	REQUIRE(projection->GetValue(1, 0).GetValue<bool>());
	REQUIRE(projection->GetValue(2, 1).GetValue<bool>());
	REQUIRE(projection->GetValue(3, 2).GetValue<bool>());
	REQUIRE_FALSE(projection->GetValue(4, 2).GetValue<bool>());
	REQUIRE(projection->GetValue(4, 3).GetValue<bool>());
	REQUIRE(projection->GetValue(1, 4).IsNull());
	REQUIRE(projection->GetValue(2, 4).IsNull());
	REQUIRE(projection->GetValue(3, 4).IsNull());
	REQUIRE(projection->GetValue(4, 4).IsNull());

	auto suffix_count = con.Query("SELECT count(*) FROM jit_string_match_native WHERE suffix(s, 'BRASS')");
	REQUIRE_NO_FAIL(*suffix_count);
	REQUIRE(suffix_count->GetValue(0, 0).GetValue<int64_t>() == 1);
	auto contains_count = con.Query("SELECT count(*) FROM jit_string_match_native WHERE contains(s, 'green')");
	REQUIRE_NO_FAIL(*contains_count);
	REQUIRE(contains_count->GetValue(0, 0).GetValue<int64_t>() == 1);
	auto like_count = con.Query("SELECT count(*) FROM jit_string_match_native WHERE s LIKE '%special%requests%'");
	REQUIRE_NO_FAIL(*like_count);
	REQUIRE(like_count->GetValue(0, 0).GetValue<int64_t>() == 1);
	auto not_like_count =
	    con.Query("SELECT count(*) FROM jit_string_match_native WHERE s NOT LIKE '%special%requests%'");
	REQUIRE_NO_FAIL(*not_like_count);
	REQUIRE(not_like_count->GetValue(0, 0).GetValue<int64_t>() == 3);

	bool found_suffix = false;
	bool found_contains = false;
	bool found_like = false;
	bool found_not_like = false;
	for (auto &event : manager.GetEvents()) {
		for (auto function_name : {"suffix", "contains", "~~", "!~~"}) {
			auto stale_fallback = StringUtil::Contains(event.reason, string("function=") + function_name) &&
			                      StringUtil::Contains(event.reason, "function_or_operator_unsupported");
			REQUIRE_FALSE(stale_fallback);
		}
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled" ||
		    event.execution_mode != "native") {
			continue;
		}
		found_suffix = found_suffix || StringUtil::Contains(event.ir, "string_suffix");
		found_contains = found_contains || StringUtil::Contains(event.ir, "string_contains");
		found_like = found_like || StringUtil::Contains(event.ir, "string_like");
		found_not_like = found_not_like ||
		                 (StringUtil::Contains(event.ir, ".not(") && StringUtil::Contains(event.ir, "string_like"));
		if (StringUtil::Contains(event.ir, "string_suffix") || StringUtil::Contains(event.ir, "string_contains") ||
		    StringUtil::Contains(event.ir, "string_like")) {
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
		}
	}
	REQUIRE(found_suffix);
	REQUIRE(found_contains);
	REQUIRE(found_like);
	REQUIRE(found_not_like);
}

TEST_CASE("JIT lowers constant string substring IN predicates as native predicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_substring_native(id INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_string_substring_native VALUES "
	                          "(1, '13123'), "
	                          "(2, '319'), "
	                          "(3, '99123'), "
	                          "(4, NULL), "
	                          "(5, '1'), "
	                          "(6, '13' || chr(233)), "
	                          "(7, '1' || chr(233))"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto projection = con.Query("SELECT id, substring(s, 1, 2) AS prefix "
	                            "FROM jit_string_substring_native ORDER BY id");
	REQUIRE_NO_FAIL(*projection);
	REQUIRE(projection->RowCount() == 7);
	REQUIRE(projection->GetValue(1, 0).ToString() == "13");
	REQUIRE(projection->GetValue(1, 1).ToString() == "31");
	REQUIRE(projection->GetValue(1, 2).ToString() == "99");
	REQUIRE(projection->GetValue(1, 3).IsNull());
	REQUIRE(projection->GetValue(1, 4).ToString() == "1");
	REQUIRE(projection->GetValue(1, 5).ToString() == "13");
	REQUIRE(projection->GetValue(1, 6).ToString() == "1" + string("\xC3\xA9"));

	auto count = con.Query("SELECT count(*) FROM jit_string_substring_native "
	                       "WHERE substring(s, 1, 2) IN ('13', '31', '23')");
	REQUIRE_NO_FAIL(*count);
	REQUIRE(count->RowCount() == 1);
	REQUIRE(count->GetValue(0, 0).GetValue<int64_t>() == 3);

	bool found_native_string_substring = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_substring_fallback = StringUtil::Contains(event.reason, "function=substring") &&
		                                StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_substring_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "string_substring")) {
			found_native_string_substring = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
		}
	}
	REQUIRE(found_native_string_substring);
}

TEST_CASE("JIT lowers signed to unsigned integer cast as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_signed_to_unsigned_cast_native(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_signed_to_unsigned_cast_native VALUES (0), (1), (65535), (NULL)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT i::USMALLINT AS u FROM jit_signed_to_unsigned_cast_native ORDER BY u NULLS LAST");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<uint16_t>() == 0);
	REQUIRE(result->GetValue(0, 1).GetValue<uint16_t>() == 1);
	REQUIRE(result->GetValue(0, 2).GetValue<uint16_t>() == 65535);
	REQUIRE(result->GetValue(0, 3).IsNull());

	bool found_native_unsigned_cast = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, "logical=USMALLINT")) {
			found_native_unsigned_cast = true;
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(event.candidate_shape == "projection");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "cast"));
		}
	}
	REQUIRE(found_native_unsigned_cast);
}

TEST_CASE("JIT lowers double division as native scalar projection", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_double_divide_native(id INTEGER, a DOUBLE, b DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_double_divide_native VALUES "
	                          "(1, 8.0, 2.0), (2, 16.0, 4.0), (3, NULL, 2.0), (4, 4.0, NULL)"));

	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query("SELECT a / b AS q, a / 2.0 AS half, 16.0 / b AS inv "
	                        "FROM jit_double_divide_native ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).GetValue<double>() == 4);
	REQUIRE(result->GetValue(1, 0).GetValue<double>() == 4);
	REQUIRE(result->GetValue(2, 0).GetValue<double>() == 8);
	REQUIRE(result->GetValue(0, 1).GetValue<double>() == 4);
	REQUIRE(result->GetValue(1, 1).GetValue<double>() == 8);
	REQUIRE(result->GetValue(2, 1).GetValue<double>() == 4);
	REQUIRE(result->GetValue(0, 2).IsNull());
	REQUIRE(result->GetValue(1, 2).IsNull());
	REQUIRE(result->GetValue(2, 2).GetValue<double>() == 8);
	REQUIRE(result->GetValue(0, 3).IsNull());
	REQUIRE(result->GetValue(1, 3).GetValue<double>() == 2);
	REQUIRE(result->GetValue(2, 3).IsNull());

	bool found_native_double_divide = false;
	for (auto &event : manager.GetEvents()) {
		auto stale_double_divide_fallback = StringUtil::Contains(event.reason, "function=/") &&
		                                    StringUtil::Contains(event.reason, "function_or_operator_unsupported");
		REQUIRE_FALSE(stale_double_divide_fallback);
		if (event.backend_name != "sljit" || event.target != "region" || event.status != "compiled") {
			continue;
		}
		if (event.execution_mode == "native" && StringUtil::Contains(event.ir, ".divide")) {
			found_native_double_divide = true;
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(StringUtil::Contains(event.candidate_shape, "projection"));
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.code_size > 0);
		}
	}
	REQUIRE(found_native_double_divide);
}

TEST_CASE("JIT auto rejects source-boundary full pipeline as proof-gap only", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_q06_like AS "
	                          "SELECT "
	                          "  (DATE '1994-01-01' + ((i % 365)::INTEGER)) AS shipdate, "
	                          "  (((i % 10) + 1)::DECIMAL(15,2)) AS discount, "
	                          "  (((i % 30) + 1)::DECIMAL(15,2)) AS quantity, "
	                          "  (((i % 100) + 1)::DECIMAL(15,2)) AS extendedprice "
	                          "FROM range(6000000) t(i)"));

	const string query = "SELECT sum(extendedprice * discount) "
	                     "FROM jit_auto_q06_like "
	                     "WHERE shipdate >= DATE '1994-01-01' "
	                     "  AND shipdate < DATE '1995-01-01' "
	                     "  AND discount >= 5.00 "
	                     "  AND discount <= 7.00 "
	                     "  AND quantity < 24.00";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto baseline = con.Query(query);
	REQUIRE_NO_FAIL(*baseline);
	auto expected = baseline->GetValue(0, 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	manager.ClearEvents();
	manager.ClearCounters();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {expected}));

	bool found_q06_inventory_skip = false;
	bool found_q06_region_candidate = false;
	bool found_q06_compile = false;
	bool found_q06_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (event.has_candidate) {
			found_q06_region_candidate = true;
		}
		if (event.status == "compiled") {
			found_q06_compile = true;
		}
		if (event.phase == "runtime") {
			found_q06_runtime = true;
		}
		if (event.policy_decision == "auto" && event.status == "skipped" && !event.has_candidate &&
		    StringUtil::Contains(event.admission_shape_key, "ungrouped-aggregate-update")) {
			found_q06_inventory_skip = true;
			REQUIRE(event.execution_mode == "executor_fallback");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NONE);
			REQUIRE(event.has_admission);
			REQUIRE_FALSE(event.admission_rule_present);
			REQUIRE(event.admission_min_cardinality == 0);
			REQUIRE_FALSE(event.has_admission_score);
			REQUIRE(event.admission_proof.empty());
			REQUIRE(event.code_size == 0);
			REQUIRE(event.backend_analysis_time_us == 0);
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.codegen_time_us == 0);
			REQUIRE(StringUtil::Contains(event.reason, "before typed IR lowering"));
			REQUIRE(StringUtil::Contains(event.reason, "no SLJIT auto admission family"));
			REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
			REQUIRE(StringUtil::Contains(event.reason, "source_filter_count="));
			REQUIRE(StringUtil::Contains(event.ir, "duckdb.region admission-inventory"));
			REQUIRE(StringUtil::Contains(event.ir, "ungrouped-aggregate-update"));
		}
	}
	REQUIRE(found_q06_inventory_skip);
	REQUIRE_FALSE(found_q06_region_candidate);
	REQUIRE_FALSE(found_q06_compile);
	REQUIRE_FALSE(found_q06_runtime);
}

TEST_CASE("JIT auto rejects native-source ungrouped aggregate without production proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_source_prefix AS "
	                          "SELECT i::BIGINT AS i, (i + 1)::BIGINT AS j FROM range(5000000) t(i)"));

	manager.ClearEvents();
	auto result = con.Query("SELECT sum(i) FROM jit_auto_source_prefix WHERE j > 2500000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(9374998750000)}));

	bool found_compiled_region = false;
	bool found_inventory_skip = false;
	bool found_region_candidate = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (event.has_candidate) {
			found_region_candidate = true;
		}
		if (event.status == "compiled") {
			found_compiled_region = true;
		}
		if (event.policy_decision != "auto" || event.status != "skipped") {
			continue;
		}
		if (!event.has_candidate && StringUtil::Contains(event.admission_shape_key, "ungrouped-aggregate-update")) {
			found_inventory_skip = true;
			REQUIRE(event.execution_mode == "executor_fallback");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.has_admission);
			REQUIRE_FALSE(event.admission_rule_present);
			REQUIRE(event.admission_proof.empty());
			REQUIRE(event.backend_analysis_time_us == 0);
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.codegen_time_us == 0);
			REQUIRE(StringUtil::Contains(event.reason, "before typed IR lowering"));
			REQUIRE(StringUtil::Contains(event.reason, "no SLJIT auto admission family"));
			REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
			REQUIRE(StringUtil::Contains(event.ir, "duckdb.region admission-inventory"));
			REQUIRE(StringUtil::Contains(event.ir, "ungrouped-aggregate-update"));
		}
	}
	REQUIRE(found_inventory_skip);
	REQUIRE_FALSE(found_region_candidate);
	REQUIRE_FALSE(found_compiled_region);
}

TEST_CASE("JIT auto skips dynamic-filter join aggregate without measured proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_context_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 100000)::BIGINT AS k, i::BIGINT AS d, "
	                          "i::BIGINT AS v FROM range(6000000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_context_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(100000) t(i)"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*) FROM ("
	                        "SELECT f.k, sum(f.v) AS total_v "
	                        "FROM jit_auto_context_fact f "
	                        "JOIN jit_auto_context_dim d ON f.k=d.k "
	                        "WHERE f.d > 3000000 "
	                        "GROUP BY f.k"
	                        ") q");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {100000}));

	bool found_hash_aggregate_inventory_skip = false;
	bool found_compiled_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (event.status == "compiled") {
			found_compiled_region = true;
		}
		if (event.status != "skipped" || !StringUtil::Contains(event.admission_shape_key, "pipeline-inventory") ||
		    !StringUtil::Contains(event.admission_shape_key, "hash-aggregate-update")) {
			continue;
		}
		found_hash_aggregate_inventory_skip = true;
		REQUIRE_FALSE(event.has_candidate);
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.region_execution_form == "none");
		REQUIRE(event.policy_decision == "auto");
		REQUIRE(event.has_admission);
		REQUIRE_FALSE(event.admission_rule_present);
		REQUIRE(event.backend_analysis_time_us == 0);
		REQUIRE(event.compile_time_us == 0);
		REQUIRE(event.codegen_time_us == 0);
		REQUIRE(StringUtil::Contains(event.reason, "no SLJIT auto admission family can match pipeline inventory"));
		REQUIRE(StringUtil::Contains(event.reason, "hash-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
	}
	REQUIRE(found_hash_aggregate_inventory_skip);
	REQUIRE_FALSE(found_compiled_region);
}

TEST_CASE("JIT auto skips conjunctive-filter join aggregate without measured proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_conjunction_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 100000)::BIGINT AS k, i::BIGINT AS d, "
	                          "1::BIGINT AS v FROM range(1000000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_conjunction_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(100000) t(i)"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*) FROM ("
	                        "SELECT f.k, sum(f.v) AS total_v "
	                        "FROM jit_auto_conjunction_fact f "
	                        "JOIN jit_auto_conjunction_dim d ON f.k=d.k "
	                        "WHERE f.d >= 300000 AND f.d < 900000 "
	                        "GROUP BY f.k"
	                        ") q");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(100000)}));

	bool found_hash_aggregate_inventory_skip = false;
	bool found_compiled_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (event.status == "compiled") {
			found_compiled_region = true;
		}
		if (event.status != "skipped" || !StringUtil::Contains(event.admission_shape_key, "pipeline-inventory") ||
		    !StringUtil::Contains(event.admission_shape_key, "hash-aggregate-update")) {
			continue;
		}
		found_hash_aggregate_inventory_skip = true;
		REQUIRE_FALSE(event.has_candidate);
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.region_execution_form == "none");
		REQUIRE(event.policy_decision == "auto");
		REQUIRE(event.has_admission);
		REQUIRE_FALSE(event.admission_rule_present);
		REQUIRE(event.backend_analysis_time_us == 0);
		REQUIRE(event.compile_time_us == 0);
		REQUIRE(event.codegen_time_us == 0);
		REQUIRE(StringUtil::Contains(event.reason, "no SLJIT auto admission family can match pipeline inventory"));
		REQUIRE(StringUtil::Contains(event.reason, "hash-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
	}
	REQUIRE(found_hash_aggregate_inventory_skip);
	REQUIRE_FALSE(found_compiled_region);
}

TEST_CASE("JIT auto policy skips before backend codegen without performance proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoRejectedCountingBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_reject_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_reject_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM jit_auto_reject_input WHERE i > 0"));

	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_region_auto_skip = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(event.target != "expression");
		if (event.backend_name == "contract_test_auto_reject_jit_backend" && event.target == "region" &&
		    StringUtil::Contains(event.reason, "contract native node")) {
			found_region_auto_skip = true;
			REQUIRE(event.has_candidate);
			REQUIRE(IsKnownJitCandidateScope(event.candidate_scope));
			REQUIRE(event.candidate_node_count > 0);
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
			REQUIRE(event.status == "skipped");
			REQUIRE(event.execution_mode == "executor_fallback");
			REQUIRE(event.policy_decision == "auto");
			REQUIRE(event.decision_time_us >= 0);
			REQUIRE(event.compile_time_us == 0);
			REQUIRE(event.code_size == 0);
		}
	}
	REQUIRE(found_region_auto_skip);
}

TEST_CASE("JIT auto admission only compiles fused region forms", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoNonFusedAdmissionBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_non_fused_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_non_fused_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_auto_non_fused_input WHERE i > 0"));

	REQUIRE(backend_ref.region_analyze_count > 0);
	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_non_fused_auto_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_auto_non_fused_jit_backend" || event.target != "region") {
			continue;
		}
		if (!event.has_admission || event.admission_shape_key != "contract:auto-non-fused") {
			continue;
		}
		if (event.status != "skipped" || event.region_execution_form != "none") {
			continue;
		}
		found_non_fused_auto_skip = true;
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.policy_decision == "auto");
		REQUIRE(event.has_admission);
		REQUIRE(event.admission_shape_key == "contract:auto-non-fused");
		REQUIRE(event.admission_rule_present);
		REQUIRE(event.admission_min_cardinality == 0);
		REQUIRE(event.admission_proof == "contract:auto-non-fused-proof");
		REQUIRE(event.has_admission_score);
		REQUIRE(event.compile_time_us == 0);
		REQUIRE(event.code_size == 0);
		REQUIRE(StringUtil::Contains(event.reason, "region execution form is not fused"));
		REQUIRE(StringUtil::Contains(event.reason, "region_execution_form=none"));
		REQUIRE(StringUtil::Contains(event.reason, "requires=fused"));
	}
	REQUIRE(found_non_fused_auto_skip);
}

TEST_CASE("JIT region lowering exposes aggregate sinks only through maximal candidates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_maximal_sink_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM jit_maximal_sink_input"));

	bool found_full_candidate = false;
	bool found_full_core_ir = false;
	bool found_aggregate_sink_protocol = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE((event.has_candidate && event.candidate_scope == "sink_pipeline"));
		if (event.target != "region" || !event.has_candidate || event.candidate_scope != "full_pipeline") {
			continue;
		}
		found_full_candidate = true;
		REQUIRE(StringUtil::Contains(event.candidate_shape, "sink"));
		REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, ":sink:"));
		auto is_aggregate_sink =
		    StringUtil::Contains(event.candidate_pipeline_shape, "sink:sink:UNGROUPED_AGGREGATE:sink-native");
		if (event.status == "compiled") {
			REQUIRE(event.code_size > 0);
		} else {
			auto is_honest_fallback_mode =
			    event.execution_mode == "unsupported" || event.execution_mode == "executor_fallback";
			REQUIRE(is_honest_fallback_mode);
		}
		if (is_aggregate_sink) {
			found_aggregate_sink_protocol = true;
			if (StringUtil::Contains(event.reason, "UNGROUPED_AGGREGATE")) {
				REQUIRE(StringUtil::Contains(event.reason, "aggregate_operator_kind=ungrouped"));
				REQUIRE(StringUtil::Contains(event.reason, "aggregate_count=1"));
			}
			if (event.status == "compiled") {
				auto has_aggregate_update_reason =
				    StringUtil::Contains(event.reason, "generated native ungrouped aggregate state update") ||
				    StringUtil::Contains(event.reason, "generated typed ungrouped aggregate payload update");
				REQUIRE(has_aggregate_update_reason);
			}
			if (event.status == "unsupported") {
				REQUIRE(event.execution_mode == "unsupported");
				REQUIRE(event.code_size == 0);
				REQUIRE(StringUtil::Contains(event.reason,
				                             "backend cannot generate executable code for this whole region"));
			}
		}
		if (!event.ir.empty()) {
			found_full_core_ir = true;
			REQUIRE(StringUtil::Contains(event.ir, "boundary=sink"));
			if (is_aggregate_sink) {
				REQUIRE(StringUtil::Contains(event.ir, "sink<kind=ungrouped-aggregate-update"));
				REQUIRE(StringUtil::Contains(event.ir, "marker=DuckDB ungrouped aggregate payload update protocol"));
			}
		}
	}
	REQUIRE(found_full_candidate);
	REQUIRE(found_full_core_ir);
	REQUIRE(found_aggregate_sink_protocol);
}

TEST_CASE("JIT region lowering exposes typed table scan source protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_scan_protocol(i BIGINT, j BIGINT, p DECIMAL(15,2), d DECIMAL(15,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_scan_protocol SELECT i, i % 3, i::DECIMAL(15,2), "
	                          "(i % 10)::DECIMAL(15,2) / 100 FROM range(1000) AS t(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(p * d) FROM jit_scan_protocol WHERE j=1 AND i>=10 AND i<990"));

	bool found_table_scan_protocol = false;
	bool found_compiled_native_filtered_source = false;
	bool found_native_filtered_source_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name == "sljit" && event.target == "region" && event.status == "compiled" &&
		    event.candidate_scope == "full_pipeline" &&
		    event.candidate_traits.source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
		    event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL) {
			found_compiled_native_filtered_source = true;
			REQUIRE(event.execution_mode == "native");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE);
			REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
			REQUIRE(event.candidate_contract.transform_ownership == JitRegionOwnershipKind::GENERATED_IR);
			REQUIRE(event.candidate_contract.sink_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
			REQUIRE(event.candidate_contract.owns_source);
			REQUIRE(event.candidate_contract.owns_transform);
			REQUIRE(event.candidate_contract.owns_sink);
			REQUIRE_FALSE(event.candidate_contract.owns_state_scan);
			REQUIRE(event.candidate_contract.executor_boundary_free);
			REQUIRE(event.candidate_contract.native_fusion_ready);
			REQUIRE(event.candidate_contract.missing_protocol_count == 0);
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.reason, "generated source-prefix table scan filters"));
			REQUIRE(StringUtil::Contains(event.reason, "source-strategy=prepared-unfiltered-native-source"));
			REQUIRE(StringUtil::Contains(event.reason, "owns-source-filters=true"));
			REQUIRE(StringUtil::Contains(event.reason, "op0:PROJECTION:native:generated typed projection"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:UNGROUPED_AGGREGATE:native"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
			REQUIRE(StringUtil::Contains(event.reason, "duckdb.operator-stage-region"));
			REQUIRE(StringUtil::Contains(event.reason, "operator-stage-region"));
			REQUIRE(StringUtil::Contains(event.reason, "kernel=generic-runtime-loop"));
			REQUIRE(StringUtil::Contains(event.reason, "source=native"));
			REQUIRE(StringUtil::Contains(event.ir, "source_execution=native-source"));
			REQUIRE(StringUtil::Contains(event.ir, "source=native-protocol"));
			REQUIRE(StringUtil::Contains(event.ir, "native_fusion_ready=true"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "source-prefix-generated-filter-projection"));
		}
		if (event.backend_name == "sljit" && event.target == "region" && event.status == "executed" &&
		    event.candidate_scope == "full_pipeline" &&
		    event.candidate_traits.source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
		    event.source_native_output_rows > 0) {
			found_native_filtered_source_runtime = true;
			REQUIRE(event.execution_mode == "native");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE);
			REQUIRE(event.input_rows == event.source_native_output_rows);
			REQUIRE(event.output_rows > 0);
			REQUIRE(event.output_rows <= event.input_rows);
		}
		if (event.target != "region" || event.ir.empty() ||
		    !StringUtil::Contains(event.ir, "table_scan_protocol<function=seq_scan")) {
			continue;
		}
		found_table_scan_protocol = true;
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=duckdb-table-scan"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=duckdb-table-scan-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "output_columns=2"));
		REQUIRE(StringUtil::Contains(event.ir, "returned_columns=4"));
		REQUIRE(StringUtil::Contains(event.ir, "column_ids=4"));
		REQUIRE(StringUtil::Contains(event.ir, "projected_columns=2"));
		REQUIRE(StringUtil::Contains(event.ir, "column_id_bindings=[1|0|2|3]"));
		REQUIRE(StringUtil::Contains(event.ir, "projection_ids=[2|3]"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_input_columns=4"));
		REQUIRE(
		    StringUtil::Contains(event.ir, "source_prefix_input_types=[BIGINT|BIGINT|DECIMAL(15,2)|DECIMAL(15,2)]"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_output_projection_map=[2|3]"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_filter_column_map=[0|1]"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_requires_unfiltered_input=true"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_filter_prune_required=true"));
		REQUIRE(StringUtil::Contains(event.ir, "source_prefix_filter_split_supported=true"));
		REQUIRE(StringUtil::Contains(event.ir, "projection_pushdown=true"));
		REQUIRE(StringUtil::Contains(event.ir, "filter_pushdown=true"));
		REQUIRE(StringUtil::Contains(event.ir, "filter_prune=true"));
		REQUIRE(StringUtil::Contains(event.ir, "filter_count=2"));
		REQUIRE_FALSE(
		    StringUtil::Contains(event.reason, "table scan source boundary requires typed table scan protocol IR"));
	}
	REQUIRE(found_table_scan_protocol);
	REQUIRE(found_compiled_native_filtered_source);
	REQUIRE(found_native_filtered_source_runtime);
}

TEST_CASE("JIT region lowering exposes stateful source protocol candidates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM (SELECT l.i FROM range(1000) AS l(i) "
	                          "JOIN range(1000) AS r(i) ON l.i=r.i) t"));

	bool found_inner_hash_join_source_pipeline_candidate = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (StringUtil::Contains(event.pipeline_shape, "source:source:HASH_JOIN:source-missing-protocol") ||
		    StringUtil::Contains(event.candidate_pipeline_shape,
		                         "source:source:HASH_JOIN:source-missing-protocol") ||
		    StringUtil::Contains(event.candidate_context_pipeline_shape,
		                         "source:source:HASH_JOIN:source-missing-protocol")) {
			found_inner_hash_join_source_pipeline_candidate = true;
		}
		const bool has_non_producing_hash_join_source =
		    event.candidate_traits.has_stateful_source && StringUtil::Contains(event.ir, "function=hash_join_probe") &&
		    StringUtil::Contains(event.ir, "source_produces_rows=false");
		REQUIRE_FALSE(has_non_producing_hash_join_source);
	}
	REQUIRE_FALSE(found_inner_hash_join_source_pipeline_candidate);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM range(1000) AS l(i) "
	                          "FULL OUTER JOIN range(1200) AS r(i) ON l.i=r.i"));

	bool found_hash_join_source_protocol = false;
	bool found_hash_join_source_reason = false;
	bool found_hash_join_state_scan_abi = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.ir.empty() || !event.candidate_traits.has_stateful_source ||
		    !StringUtil::Contains(event.ir, "function=hash_join_probe")) {
			continue;
		}
		found_hash_join_source_protocol = true;
		found_hash_join_source_reason =
		    found_hash_join_source_reason ||
		    StringUtil::Contains(event.reason, "DuckDB hash join native state scan protocol");
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=stateful-operator-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "hash-join-native-state-scan"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_protocol=v1"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_blocker=none"));
		REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.owns_source);
		REQUIRE(event.candidate_contract.owns_state_scan);
		REQUIRE(event.candidate_contract.missing_protocol_count == 0);
		REQUIRE(event.candidate_contract.required_capabilities.size() >= 1);
		REQUIRE(event.candidate_contract.required_capabilities[0] == "hash-join-native-state-scan");
		REQUIRE(event.candidate_contract.blockers.empty());
		RequireStatefulSourceNativeProtocolABI(event, found_hash_join_state_scan_abi);
		found_hash_join_state_scan_abi =
		    found_hash_join_state_scan_abi ||
		    (event.candidate_contract.owns_source && event.candidate_contract.owns_state_scan);
		REQUIRE(StringUtil::Contains(event.ir, "source=native-protocol,state_scan=native-protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_protocol<join_type=full"));
		REQUIRE(StringUtil::Contains(event.ir, "join_type=full"));
		REQUIRE(StringUtil::Contains(event.ir, "source_produces_rows=true"));
		REQUIRE(StringUtil::Contains(event.ir, "condition_count=1"));
		REQUIRE(StringUtil::Contains(event.ir, "lhs_output_column_indices="));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "typed_hash_join_probe_helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "typed_hash_join_build_helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "hash-join-typed"));
		REQUIRE(StringUtil::Contains(event.ir, "regular_hash_table_layout_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_layout_offsets=["));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_tuple_size="));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_entry_size="));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_pointer_offset="));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_native_protocol_blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_join_probe_required_capability="
		                                       "hash-join-native-probe"));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_join_build_required_capability="
		                                       "hash-join-native-build"));
		REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
		REQUIRE(StringUtil::Contains(event.ir, "hash_join_keys=[key0<input_index=0,type=BIGINT"));
		REQUIRE(StringUtil::Contains(event.ir, "supported_reference=true"));
	}
	REQUIRE(found_hash_join_source_protocol);
	REQUIRE(found_hash_join_source_reason);
	REQUIRE(found_hash_join_state_scan_abi);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM (SELECT i%3 AS k, sum(i) AS s "
	                          "FROM range(1000) AS t(i) GROUP BY k) g WHERE s >= 0"));

	bool found_hash_aggregate_source_protocol = false;
	bool found_hash_aggregate_source_reason = false;
	bool found_hash_aggregate_state_scan_abi = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.ir.empty() || !event.candidate_traits.has_stateful_source ||
		    !StringUtil::Contains(event.ir, "function=hash_aggregate_scan")) {
			continue;
		}
		found_hash_aggregate_source_protocol = true;
		found_hash_aggregate_source_reason =
		    found_hash_aggregate_source_reason ||
		    StringUtil::Contains(event.reason, "DuckDB hash aggregate native state scan protocol") ||
		    StringUtil::Contains(event.reason, "native state scan source protocol");
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=stateful-operator-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "hash-aggregate-native-state-scan"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_protocol=v1"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_blocker=none"));
		REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.owns_source);
		REQUIRE(event.candidate_contract.owns_state_scan);
		REQUIRE(event.candidate_contract.missing_protocol_count == 0);
		REQUIRE(event.candidate_contract.required_capabilities.size() == 2);
		REQUIRE(event.candidate_contract.required_capabilities[0] == "hash-aggregate-native-state-scan");
		REQUIRE(event.candidate_contract.required_capabilities[1] == "hash-aggregate-native-grouped-state");
		REQUIRE(event.candidate_contract.blockers.empty());
		RequireStatefulSourceNativeProtocolABI(event, found_hash_aggregate_state_scan_abi);
		found_hash_aggregate_state_scan_abi =
		    found_hash_aggregate_state_scan_abi ||
		    (event.candidate_contract.owns_source && event.candidate_contract.owns_state_scan);
		REQUIRE(StringUtil::Contains(event.ir, "source=native-protocol,state_scan=native-protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_required_capability="
		                                       "hash-aggregate-native-grouped-state"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_blocker=none"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "typed_hash_aggregate_lookup_helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "hash-aggregate-typed-lookup-helper"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_required_capability="
		                                       "hash-aggregate-native-lookup"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_protocol<aggregate_operator_kind=hash"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_operator_kind=hash"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregates=[aggregate0<function=sum"));
		REQUIRE(StringUtil::Contains(event.ir, "groups=[group0<input_index=0,type=BIGINT"));
		REQUIRE(StringUtil::Contains(event.ir, "supported_reference=true"));
	}
	REQUIRE(found_hash_aggregate_source_protocol);
	REQUIRE(found_hash_aggregate_source_reason);
	REQUIRE(found_hash_aggregate_state_scan_abi);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_distinct_hash_aggregate AS "
	                          "SELECT (i % 4)::BIGINT AS k, i::BIGINT AS v FROM range(1000) AS t(i)"));
	REQUIRE_NO_FAIL(con.Query("SELECT k, count(DISTINCT v) FROM jit_distinct_hash_aggregate GROUP BY k"));

	bool found_distinct_hash_aggregate_boundary = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.ir.empty() || !event.candidate_contract.owns_sink ||
		    event.candidate_traits.sink_kind != JitRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "aggregate_types=[DISTINCT]")) {
			continue;
		}
		found_distinct_hash_aggregate_boundary = true;
		REQUIRE_FALSE(event.candidate_contract.native_fusion_ready);
		REQUIRE(event.candidate_contract.sink_ownership == JitRegionOwnershipKind::MISSING_PROTOCOL);
		REQUIRE(event.candidate_contract.missing_protocol_count > 0);
		REQUIRE(StringUtil::Contains(event.candidate_pipeline_shape, "sink:sink:HASH_GROUP_BY:sink"));
		REQUIRE(StringUtil::Contains(event.ir, "distinct_aggregate_count=1"));
		REQUIRE(StringUtil::Contains(event.ir, "distinct_table_count=1"));
		REQUIRE(StringUtil::Contains(event.ir, "distinct_child_count=1"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_blocker="
		                                       "hash-aggregate-distinct-grouped-state-protocol-boundary"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=missing"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_blocker="
		                                       "hash-aggregate-distinct-lookup-protocol-boundary"));
		REQUIRE(StringUtil::Contains(event.ir, "distinct aggregate update requires DuckDB distinct sink protocol"));
	}
	REQUIRE(found_distinct_hash_aggregate_boundary);

	manager.ClearEvents();
	auto cte_result =
	    con.Query("WITH x AS MATERIALIZED (SELECT i::BIGINT AS i, i + 1 AS v FROM range(100) tbl(i)) "
	              "SELECT sum(v) FROM x WHERE i < 10");
	REQUIRE_NO_FAIL(*cte_result);
	REQUIRE(cte_result->GetValue(0, 0).ToString() == "55");

	bool found_cte_source_protocol = false;
	bool found_compiled_cte_native_source = false;
	bool found_runtime_cte_native_source = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "stateful-source-protocol-boundary"));
		if (event.ir.empty() || !StringUtil::Contains(event.ir, "operator=CTE_SCAN")) {
			if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
			    event.region_execution_form == "fused" && event.candidate_scope == "full_pipeline" &&
			    event.source_native_output_rows > 0 && event.source_native_invocation_count > 0) {
				found_runtime_cte_native_source = true;
			}
			continue;
		}
		found_cte_source_protocol = true;
		REQUIRE(StringUtil::Contains(event.ir, "DuckDB column data native source protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "function=cte_scan"));
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=stateful-operator-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NONE);
		REQUIRE_FALSE(event.candidate_contract.owns_state_scan);
		if (event.candidate_contract.owns_source) {
			REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
			REQUIRE(event.candidate_contract.missing_protocol_count == 0);
		} else {
			REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NONE);
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.region_execution_form == "fused" && event.candidate_scope == "full_pipeline" &&
		    event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE) {
			found_compiled_cte_native_source = true;
			REQUIRE(StringUtil::Contains(event.reason, "native stateful source protocol"));
			REQUIRE(StringUtil::Contains(event.reason, "source-execution:native-source"));
		}
	}
	REQUIRE(found_cte_source_protocol);
	REQUIRE(found_compiled_cte_native_source);
	REQUIRE(found_runtime_cte_native_source);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_stateful_perfect_hash AS "
	                          "SELECT (i % 4)::INTEGER AS k, i::BIGINT AS v FROM range(1000) tbl(i)"));
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM (SELECT k, sum(v) AS s FROM jit_stateful_perfect_hash GROUP BY k) g "
	                          "WHERE s >= 0"));

	bool found_perfect_hash_aggregate_source_protocol = false;
	bool found_perfect_hash_aggregate_source_reason = false;
	bool found_perfect_hash_aggregate_state_scan_abi = false;
	bool found_compiled_perfect_hash_native_state_scan_source = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.ir.empty() || !event.candidate_traits.has_stateful_source ||
		    !StringUtil::Contains(event.ir, "function=perfect_hash_aggregate_scan")) {
			continue;
		}
		found_perfect_hash_aggregate_source_protocol = true;
		found_perfect_hash_aggregate_source_reason =
		    found_perfect_hash_aggregate_source_reason ||
		    StringUtil::Contains(event.reason, "DuckDB perfect hash aggregate native state scan protocol");
		found_compiled_perfect_hash_native_state_scan_source =
		    found_compiled_perfect_hash_native_state_scan_source ||
		    (event.status == "compiled" && event.execution_mode == "native" &&
		     event.candidate_contract.abi == JitRegionABI::SOURCE_PREFIX &&
		     StringUtil::Contains(event.reason, "native state scan source protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=stateful-operator-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "perfect-hash-aggregate-native-state-scan"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_protocol=v1"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_blocker=none"));
		REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.owns_source);
		REQUIRE(event.candidate_contract.owns_state_scan);
		REQUIRE(event.candidate_contract.missing_protocol_count == 0);
		REQUIRE(event.candidate_contract.required_capabilities.size() == 2);
		REQUIRE(event.candidate_contract.required_capabilities[0] == "perfect-hash-aggregate-native-state-scan");
		REQUIRE(event.candidate_contract.required_capabilities[1] == "perfect-hash-aggregate-native-grouped-state");
		RequireStatefulSourceNativeProtocolABI(event, found_perfect_hash_aggregate_state_scan_abi);
		found_perfect_hash_aggregate_state_scan_abi =
		    found_perfect_hash_aggregate_state_scan_abi ||
		    (event.candidate_contract.owns_source && event.candidate_contract.owns_state_scan);
		REQUIRE(StringUtil::Contains(event.ir, "source=native-protocol,state_scan=native-protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_required_capability="
		                                       "perfect-hash-aggregate-native-grouped-state"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_required_capability="
		                                       "perfect-hash-aggregate-native-lookup"));
		REQUIRE(StringUtil::Contains(event.ir, "native_hash_aggregate_lookup_blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_protocol<aggregate_operator_kind=perfect-hash"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_operator_kind=perfect_hash"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregates=[aggregate0<function=sum"));
		REQUIRE(StringUtil::Contains(event.ir, "groups=[group0<input_index=0,type=TINYINT"));
		REQUIRE(StringUtil::Contains(event.ir, "supported_reference=true"));
	}
	REQUIRE(found_perfect_hash_aggregate_source_protocol);
	REQUIRE(found_perfect_hash_aggregate_source_reason);
	REQUIRE(found_perfect_hash_aggregate_state_scan_abi);
	(void)found_compiled_perfect_hash_native_state_scan_source;

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(s) FROM (SELECT count(*) AS s FROM range(1000) AS t(i)) g"));

	bool found_ungrouped_aggregate_source_protocol = false;
	bool found_ungrouped_aggregate_source_reason = false;
	bool found_ungrouped_aggregate_state_scan_abi = false;
	bool found_compiled_ungrouped_native_state_scan_source = false;
	bool found_runtime_ungrouped_native_state_scan_source = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target == "region" && event.phase == "runtime" && event.status == "executed" &&
		    event.execution_mode == "native" && event.region_execution_form == "fused" &&
		    event.candidate_scope == "full_pipeline" && event.candidate_shape == "projection-sink" &&
		    event.runtime_result == "finished" && event.source_native_output_rows > 0 &&
		    event.source_native_invocation_count > 0) {
			found_runtime_ungrouped_native_state_scan_source = true;
		}
		if (event.target != "region" || event.ir.empty() || !event.candidate_traits.has_stateful_source ||
		    !StringUtil::Contains(event.ir, "function=ungrouped_aggregate_scan")) {
			continue;
		}
		found_ungrouped_aggregate_source_protocol = true;
		found_ungrouped_aggregate_source_reason =
		    found_ungrouped_aggregate_source_reason ||
		    StringUtil::Contains(event.reason, "DuckDB ungrouped aggregate native state scan protocol") ||
		    StringUtil::Contains(event.reason, "native state scan source protocol");
		found_compiled_ungrouped_native_state_scan_source =
		    found_compiled_ungrouped_native_state_scan_source ||
		    (event.status == "compiled" && event.execution_mode == "native" &&
		     event.candidate_contract.abi == JitRegionABI::FULL_PIPELINE &&
		     StringUtil::Contains(event.reason, "native state scan source protocol"));
		REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
		REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
		                                       "required_capability=stateful-operator-native-source"));
		REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability="
		                                       "ungrouped-aggregate-native-state-scan"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_protocol=v1"));
		REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_blocker=none"));
		REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
		REQUIRE(event.candidate_contract.owns_source);
		REQUIRE(event.candidate_contract.owns_state_scan);
		REQUIRE(event.candidate_contract.missing_protocol_count == 0);
		REQUIRE(std::find(event.candidate_contract.required_capabilities.begin(),
		                  event.candidate_contract.required_capabilities.end(),
		                  "ungrouped-aggregate-native-state-scan") !=
		        event.candidate_contract.required_capabilities.end());
		RequireStatefulSourceNativeProtocolABI(event, found_ungrouped_aggregate_state_scan_abi);
		found_ungrouped_aggregate_state_scan_abi =
		    found_ungrouped_aggregate_state_scan_abi ||
		    (event.candidate_contract.owns_source && event.candidate_contract.owns_state_scan);
		REQUIRE(StringUtil::Contains(event.ir, "source=native-protocol,state_scan=native-protocol"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_protocol<aggregate_operator_kind=ungrouped"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_operator_kind=ungrouped"));
		auto has_expected_ungrouped_aggregate =
		    StringUtil::Contains(event.ir, "aggregates=[aggregate0<function=count") ||
		    StringUtil::Contains(event.ir, "aggregates=[aggregate0<function=sum");
		REQUIRE(has_expected_ungrouped_aggregate);
	}
	REQUIRE(found_ungrouped_aggregate_source_protocol);
	REQUIRE(found_ungrouped_aggregate_source_reason);
	REQUIRE(found_ungrouped_aggregate_state_scan_abi);
	REQUIRE(found_compiled_ungrouped_native_state_scan_source);
	REQUIRE(found_runtime_ungrouped_native_state_scan_source);
}

TEST_CASE("JIT region lowering exposes order and top-n native state scan source protocols", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	auto require_sort_source = [&](const string &query, const string &expected, const string &function_name,
	                               const string &capability, const string &descriptor_marker) {
		manager.ClearEvents();
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->GetValue(0, 0).ToString() == expected);

		bool found_protocol = false;
		bool found_owning_protocol = false;
		bool found_compiled_native_source = false;
		bool found_runtime_native_source = false;
		for (auto &event : manager.GetEvents()) {
			if (event.target != "region") {
				continue;
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "DuckDB sort source state-scan protocol missing"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "DuckDB sort source state-scan protocol missing"));
			if (event.ir.empty() || !StringUtil::Contains(event.ir, "function=" + function_name)) {
				if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
				    event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
				    event.source_native_output_rows > 0) {
					found_runtime_native_source = true;
				}
				continue;
			}
			found_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, descriptor_marker));
			REQUIRE(StringUtil::Contains(event.ir, "source<kind=stateful-operator"));
			REQUIRE(StringUtil::Contains(event.ir, "execution=native-source"));
			REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=ready,"
			                                       "required_capability=stateful-operator-native-source"));
			REQUIRE(StringUtil::Contains(event.ir, "blocker=none"));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_required_capability=" + capability));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_protocol=v1"));
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_blocker=none"));
			if (!event.candidate_contract.owns_source) {
				continue;
			}
			found_owning_protocol = true;
			REQUIRE(event.candidate_contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
			REQUIRE(event.candidate_contract.state_scan_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL);
			REQUIRE(event.candidate_contract.owns_source);
			REQUIRE(event.candidate_contract.owns_state_scan);
			REQUIRE(event.candidate_contract.missing_protocol_count == 0);
			REQUIRE(event.candidate_contract.blockers.empty());
			if (event.status == "compiled" && event.execution_mode == "native" &&
			    event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE) {
				found_compiled_native_source = true;
				REQUIRE(StringUtil::Contains(event.reason, "native state scan source protocol"));
			}
		}
		REQUIRE(found_protocol);
		REQUIRE(found_owning_protocol);
		REQUIRE(found_compiled_native_source);
		REQUIRE(found_runtime_native_source);
	};

	require_sort_source("SELECT sum(i + 1) FROM (SELECT i FROM range(1000) t(i) ORDER BY i DESC) s WHERE i >= 0",
	                    "500500", "order_by_scan", "order-by-native-state-scan",
	                    "DuckDB order by native state scan protocol");
	require_sort_source("SELECT sum(i + 1) FROM (SELECT i FROM range(1000) t(i) ORDER BY i LIMIT 10) s WHERE i >= 0",
	                    "55", "top_n_scan", "top-n-native-state-scan",
	                    "DuckDB top-n native state scan protocol");
}

TEST_CASE("JIT region lowering exposes generic operator stage spans", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_stage_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 10)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_stage_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(10) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(f.v) FROM jit_stage_fact f JOIN jit_stage_dim d ON f.k=d.k"));

	bool found_source_prefix_hash_join_stage = false;
	bool found_full_pipeline_hash_join_stage = false;
	bool found_non_producing_hash_join_source_candidate = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || !event.has_candidate) {
			continue;
		}
		auto has_hash_join_probe =
		    StringUtil::Contains(event.candidate_pipeline_shape, "op0:operator:HASH_JOIN:operator-native");
		if (event.candidate_scope == "source_pipeline" && has_hash_join_probe) {
			found_source_prefix_hash_join_stage = true;
		}
		if (event.candidate_scope == "full_pipeline" && has_hash_join_probe && event.status == "compiled") {
			found_full_pipeline_hash_join_stage = true;
			REQUIRE(event.candidate_contract.abi == JitRegionABI::FULL_PIPELINE);
			REQUIRE(event.candidate_contract.owns_source);
			REQUIRE(event.candidate_contract.owns_transform);
			REQUIRE(event.candidate_contract.owns_sink);
			REQUIRE(event.candidate_contract.native_fusion_ready);
			REQUIRE(event.candidate_contract.missing_protocol_count == 0);
			REQUIRE(event.execution_mode == "native");
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(StringUtil::Contains(event.reason, "duckdb.operator-stage-region"));
				REQUIRE(StringUtil::Contains(event.reason, "hash-join-probe"));
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
				REQUIRE(StringUtil::Contains(event.reason, "native_probe_shape_ready=true"));
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY));
			}
		if (StringUtil::Contains(event.candidate_pipeline_shape,
		                         "source:source:HASH_JOIN:source-missing-protocol")) {
			found_non_producing_hash_join_source_candidate = true;
		}
	}
	REQUIRE_FALSE(found_source_prefix_hash_join_stage);
	REQUIRE(found_full_pipeline_hash_join_stage);
	REQUIRE_FALSE(found_non_producing_hash_join_source_candidate);
}

TEST_CASE("JIT full pipeline updates count aggregate through native sink update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_count AS "
	                          "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i::BIGINT END AS j FROM range(10000) tbl(i)"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(j) FROM jit_native_count");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {6666}));

	bool found_compiled_native_count = false;
	bool found_runtime_native_count = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || !event.has_candidate || event.candidate_scope != "full_pipeline") {
			continue;
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_shape == "projection-sink") {
			found_compiled_native_count = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE);
			REQUIRE(StringUtil::Contains(event.reason, "sink:UNGROUPED_AGGREGATE:native"));
			REQUIRE(StringUtil::Contains(event.reason, "generated native ungrouped aggregate state update"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-update-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-update-executable=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_native_update=count"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-ungrouped-aggregate-update"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "execution:generated-helper"));
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "ungrouped_aggregate_update"));
			REQUIRE(StringUtil::Contains(event.ir, "native_update=count"));
			REQUIRE(StringUtil::Contains(event.ir, "state_type=BIGINT"));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    event.candidate_shape == "projection-sink") {
			found_runtime_native_count = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_native_count);
	REQUIRE(found_runtime_native_count);
}

TEST_CASE("JIT full pipeline updates decimal sum aggregate through native sink update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_decimal_sum AS "
	                          "SELECT DATE '1994-01-01' + ((i % 365)::INTEGER) AS shipdate, "
	                          "CASE WHEN i % 11 = 0 THEN NULL ELSE (10 + (i % 5))::DECIMAL(15,2) END AS price, "
	                          "(0.05 + ((i % 3)::DECIMAL(15,2) / 100))::DECIMAL(15,2) AS discount, "
	                          "(10 + (i % 20))::DECIMAL(15,2) AS quantity "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT sum(price * discount) FROM jit_native_decimal_sum "
	                     "WHERE shipdate >= DATE '1994-01-01' AND shipdate < DATE '1995-01-01' "
	                     "AND discount >= 0.05::DECIMAL(15,2) AND discount <= 0.07::DECIMAL(15,2) "
	                     "AND quantity < 24.00::DECIMAL(15,2)";
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compiled_filtered_source_sum = false;
	bool found_runtime_filtered_source_sum = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || !event.has_candidate || event.candidate_scope != "full_pipeline") {
			continue;
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_shape == "scan-filter-scan-project-projection-sink") {
			found_compiled_filtered_source_sum = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE);
			REQUIRE(event.candidate_traits.source_filter_count == 1);
			REQUIRE(StringUtil::Contains(event.reason, "generated source-prefix table scan filters"));
			REQUIRE(StringUtil::Contains(event.reason, "source-strategy=prepared-unfiltered-native-source"));
			REQUIRE(StringUtil::Contains(event.reason, "owns-source-filters=true"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "source-prefix-generated-filter-projection"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "source-prefix-fused-filters=1"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "source-filters-owned-by-duckdb-scan"));
			REQUIRE(StringUtil::Contains(event.reason, "source-execution:native-source"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:UNGROUPED_AGGREGATE:native"));
			REQUIRE(StringUtil::Contains(event.reason, "generated native ungrouped aggregate state update"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-update-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-update-executable=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_native_update=sum"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_state_type=BIGINT"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_state_optional=true"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
				const string expected_execution =
				    "execution:native-sljit-region-filter-projection-ungrouped-aggregate-update";
				REQUIRE(StringUtil::Contains(event.reason, expected_execution));
				REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "ungrouped_aggregate_update"));
			REQUIRE(StringUtil::Contains(event.ir, "native_update=sum"));
			REQUIRE(StringUtil::Contains(event.ir, "state_type=BIGINT"));
			REQUIRE(StringUtil::Contains(event.ir, "state_is_set_offset"));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    event.candidate_shape == "scan-filter-scan-project-projection-sink") {
			found_runtime_filtered_source_sum = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE);
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows > 0);
			REQUIRE(event.output_rows <= event.input_rows);
			REQUIRE(event.source_native_output_rows == event.input_rows);
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_filtered_source_sum);
	REQUIRE(found_runtime_filtered_source_sum);
}

TEST_CASE("JIT full pipeline executes grouped hash aggregate native lookup protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_grouped_count AS "
	                          "SELECT (i % 4)::BIGINT AS k, "
	                          "CASE WHEN i % 3 = 0 THEN NULL ELSE i::BIGINT END AS v FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT k, count(*), count(v) FROM jit_native_grouped_count GROUP BY k ORDER BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2500, 2500, 2500, 2500}));
	REQUIRE(CHECK_COLUMN(result, 2, {1666, 1667, 1667, 1666}));

	bool found_compiled_hash_aggregate_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason,
		                                   "hash aggregate native lookup contract is ready but SLJIT native lookup "
		                                   "lowering is missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed-hash-aggregate-lookup-helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-aggregate-typed-lookup-helper"));
		if (event.phase == "compile" && event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "sink:HASH_GROUP_BY:native")) {
			found_compiled_hash_aggregate_sink = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(StringUtil::Contains(event.reason, "generated native hash aggregate lookup and state update"));
			REQUIRE(StringUtil::Contains(event.reason, "operator-stage-region"));
			REQUIRE(StringUtil::Contains(event.reason, "kernel=generic-runtime-loop"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "operator-fusion-gap:native-operator-codegen-missing"));
			REQUIRE(StringUtil::Contains(event.reason, "native-hash-aggregate-lookup-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-hash-aggregate-lookup-blocker=none"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-function-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-aggregate-update-executable=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-blocker=none"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "requires-native-grouped-state-abi=true"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_native_update=count-star"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate1_native_update=count"));
		}
	}
	REQUIRE(found_compiled_hash_aggregate_sink);
}

TEST_CASE("JIT full pipeline executes grouped decimal hash aggregate native lookup protocol",
          "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_grouped_decimal_sum AS "
	                          "SELECT (i % 4)::BIGINT AS k, "
	                          "CASE WHEN i % 13 = 0 THEN NULL ELSE (100 + (i % 17))::DECIMAL(15,2) END AS price, "
	                          "(0.05 + ((i % 3)::DECIMAL(15,2) / 100))::DECIMAL(15,2) AS discount "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));

	const string query = "SELECT k, sum(price * discount) AS revenue "
	                     "FROM jit_native_grouped_decimal_sum GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t column_idx = 0; column_idx < result->ColumnCount(); column_idx++) {
			REQUIRE(result->GetValue(column_idx, row_idx).ToString() ==
			        reference->GetValue(column_idx, row_idx).ToString());
		}
		REQUIRE(result->GetValue(1, row_idx).ToString() != "0.0000");
	}

	bool found_compiled_hash_aggregate_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason,
		                                   "hash aggregate native lookup contract is ready but SLJIT native lookup "
		                                   "lowering is missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed-hash-aggregate-lookup-helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-aggregate-typed-lookup-helper"));
		if (event.phase == "compile" && event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "sink:HASH_GROUP_BY:native") &&
		    StringUtil::Contains(event.reason, "aggregate0_native_update=sum")) {
			found_compiled_hash_aggregate_sink = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(StringUtil::Contains(event.reason, "generated native hash aggregate lookup and state update"));
			REQUIRE(StringUtil::Contains(event.reason, "operator-stage-region"));
			REQUIRE(StringUtil::Contains(event.reason, "kernel=generic-runtime-loop"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "operator-fusion-gap:native-operator-codegen-missing"));
			REQUIRE(StringUtil::Contains(event.reason, "native-hash-aggregate-lookup-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-hash-aggregate-lookup-blocker=none"));
			REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-function-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-aggregate-update-executable=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_state_type=BIGINT"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate0_state_optional=true"));
		}
	}
	REQUIRE(found_compiled_hash_aggregate_sink);
}

TEST_CASE("JIT full pipeline updates perfect hash aggregate through native sink update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_agg AS "
	                          "SELECT (i % 4)::INTEGER AS k, i::BIGINT AS v FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT k, sum(v) FROM jit_perfect_hash_agg GROUP BY k ORDER BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {12495000, 12497500, 12500000, 12502500}));

	bool found_compiled_perfect_hash_aggregate_sink = false;
	bool found_runtime_perfect_hash_aggregate_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (event.status == "compiled") {
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "fallback=1"));
		}
		if (event.candidate_scope != "full_pipeline") {
			continue;
		}
			if (event.status == "compiled" && StringUtil::Contains(event.reason, "sink:PERFECT_HASH_GROUP_BY:native")) {
				found_compiled_perfect_hash_aggregate_sink = true;
				REQUIRE(event.execution_mode == "native");
				REQUIRE(StringUtil::Contains(event.reason, "operator=PERFECT_HASH_GROUP_BY"));
				REQUIRE(StringUtil::Contains(event.reason, "generated native perfect hash aggregate state update"));
				REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
				REQUIRE(StringUtil::Contains(event.reason, "native-aggregate-function-contract=ready"));
				REQUIRE(StringUtil::Contains(event.reason, "native-grouped-aggregate-update-executable=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-layout-contract=ready"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-layout-offsets=["));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-layout-payload-sizes=["));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-required-capability="
			                                           "perfect-hash-aggregate-native-grouped-state"));
			REQUIRE(StringUtil::Contains(event.reason, "native-grouped-state-blocker=none"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "requires-native-grouped-state-abi=true"));
				REQUIRE(StringUtil::Contains(event.reason, "aggregate0_native_update=sum"));
				REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-"));
				REQUIRE(StringUtil::Contains(event.reason, "perfect-hash-aggregate-update"));
				REQUIRE(StringUtil::Contains(event.ir, "perfect_hash_aggregate_update"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_required_capability="
			                                       "perfect-hash-aggregate-native-grouped-state"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_blocker=none"));
			REQUIRE(StringUtil::Contains(event.ir, "grouped_state_layout_ready=true"));
			REQUIRE(StringUtil::Contains(event.ir, "grouped_state_offsets=["));
			REQUIRE(StringUtil::Contains(event.ir, "grouped_state_payload_sizes=["));
			REQUIRE(StringUtil::Contains(event.ir, "aggregate0<function=sum"));
			REQUIRE(StringUtil::Contains(event.ir, "native_update=sum"));
			REQUIRE(StringUtil::Contains(event.ir, "group0<input_index="));
			REQUIRE(StringUtil::Contains(event.ir, "payload_index="));
		}
			if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
			    event.candidate_scope == "full_pipeline" && event.input_rows > 0 &&
			    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_runtime_perfect_hash_aggregate_sink = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_perfect_hash_aggregate_sink);
	REQUIRE(found_runtime_perfect_hash_aggregate_sink);
}

TEST_CASE("JIT auto skips generic perfect hash aggregate update without admission proof", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_perfect_hash_agg AS "
	                          "SELECT (i % 4)::INTEGER AS k, i::BIGINT AS v FROM range(1000000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT k, sum(v) FROM jit_auto_perfect_hash_agg WHERE v < 800000 GROUP BY k ORDER BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::BIGINT(79999600000), Value::BIGINT(79999800000),
	                      Value::BIGINT(80000000000), Value::BIGINT(80000200000)}));

	bool found_auto_skip = false;
	bool found_compiled_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (event.status == "compiled") {
			found_compiled_region = true;
		}
		if (event.status == "skipped" &&
		    event.admission_shape_key ==
		        "sljit:pipeline-inventory:table-scan-source+perfect-hash-aggregate-update") {
			found_auto_skip = true;
			REQUIRE(event.policy_decision == "auto");
			REQUIRE(event.execution_mode == "executor_fallback");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.selected_source_execution == JitRegionSourceExecutionKind::NONE);
			REQUIRE(event.has_admission);
			REQUIRE_FALSE(event.admission_rule_present);
			REQUIRE(StringUtil::Contains(event.reason, "no SLJIT auto admission family can match pipeline inventory"));
			REQUIRE(StringUtil::Contains(event.reason, "admission_rule=missing"));
			REQUIRE(StringUtil::Contains(event.reason, "perfect-hash-aggregate-update"));
		}
	}
	REQUIRE(found_auto_skip);
	REQUIRE_FALSE(found_compiled_region);
}

TEST_CASE("JIT perfect hash native aggregate update preserves null-only groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_agg_nulls AS "
	                          "SELECT (i % 4)::INTEGER AS k, "
	                          "CASE WHEN i % 4 = 2 OR i % 5 = 0 THEN NULL ELSE i::BIGINT END AS v "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT k, count(*), count(v), sum(v) "
	                     "FROM jit_perfect_hash_agg_nulls GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t column_idx = 0; column_idx < result->ColumnCount(); column_idx++) {
			REQUIRE(result->GetValue(column_idx, row_idx).ToString() ==
			        reference->GetValue(column_idx, row_idx).ToString());
		}
	}
	REQUIRE(result->GetValue(0, 2).ToString() == "2");
	REQUIRE(result->GetValue(1, 2).ToString() == "2500");
	REQUIRE(result->GetValue(2, 2).ToString() == "0");
	REQUIRE(result->GetValue(3, 2).IsNull());

	bool found_compiled_native_update = false;
	bool found_runtime_native_update = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.candidate_scope != "full_pipeline") {
			continue;
		}
			if (event.status == "compiled" && event.execution_mode == "native" &&
			    StringUtil::Contains(event.reason, "sink:PERFECT_HASH_GROUP_BY:native")) {
				found_compiled_native_update = true;
				REQUIRE(StringUtil::Contains(event.reason, "generated native perfect hash aggregate state update"));
				REQUIRE(StringUtil::Contains(event.reason, "aggregate0_native_update=count-star"));
				REQUIRE(StringUtil::Contains(event.reason, "aggregate1_native_update=count"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate2_native_update=sum"));
			REQUIRE(StringUtil::Contains(event.reason, "aggregate2_state_optional=true"));
			REQUIRE(StringUtil::Contains(event.ir, "perfect_hash_aggregate_update"));
			REQUIRE(StringUtil::Contains(event.ir, "aggregate0<function=count_star"));
			REQUIRE(StringUtil::Contains(event.ir, "aggregate1<function=count"));
			REQUIRE(StringUtil::Contains(event.ir, "aggregate2<function=sum"));
		}
			if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
			    event.input_rows > 0 &&
			    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
				found_runtime_native_update = true;
				REQUIRE(event.runtime_result == "finished");
			}
		}
		REQUIRE(found_compiled_native_update);
		REQUIRE(found_runtime_native_update);
	}

TEST_CASE("JIT core prunes split regions behind resumable operators", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_resume_join_l AS "
	                          "SELECT (i % 10)::BIGINT AS k, i::BIGINT AS v FROM range(2000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_resume_join_r AS "
	                          "SELECT (i % 10)::BIGINT AS k, i::BIGINT AS w FROM range(2000) tbl(i)"));

	const string query = "SELECT sum(v + w) FROM ("
	                     "SELECT l.v, r.w "
	                     "FROM jit_resume_join_l l "
	                     "JOIN jit_resume_join_r r ON l.k=r.k"
	                     ") q";
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=false"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_hash_join_build_protocol = false;
	bool found_full_pipeline_probe_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "executor has in-process operators"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "reference executor continues current resume state"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "operator-fusion-gap:upstream-operator-resume-protocol-missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sink-fusion-gap:upstream-operator-resume-protocol-missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash join build native executable primitive is not implemented"));
		if (event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT)) {
			found_hash_join_build_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
			found_full_pipeline_probe_region = true;
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
		}
	}
	REQUIRE(found_hash_join_build_protocol);
	REQUIRE(found_full_pipeline_probe_region);
}

TEST_CASE("JIT full pipeline executes native hash join build append protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_l AS SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_hash_build_r AS SELECT i::BIGINT AS j, (i + 1)::BIGINT AS x FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_l a "
	                        "JOIN (SELECT j, x FROM jit_hash_build_r WHERE j > 10) b ON i=x");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {9988}));

	bool found_hash_join_build_contract = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (event.status == "compiled") {
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "fallback=1"));
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash join build native executable primitive is not implemented"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-native-build-filter-pushdown-state"));
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON)) {
			found_hash_join_build_contract = true;
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_build_helper"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-typed-build-helper"));
			REQUIRE(StringUtil::Contains(event.reason, "regular_hash_table_layout_ready=true"));
			REQUIRE(StringUtil::Contains(event.reason, "hash_join_native_protocol_blocker=none"));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.reason, "build_append_shape_ready=true"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-protocol-stage"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_keys=["));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
		}
	}
	REQUIRE(found_hash_join_build_contract);
}

TEST_CASE("JIT hash join build append protocol supports multi-key reference builds", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_multi_l AS "
	                          "SELECT i::BIGINT AS i, (i % 17)::INTEGER AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_build_multi_r AS "
	                          "SELECT i::BIGINT AS j, (i % 17)::INTEGER AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*) FROM jit_hash_build_multi_l a "
	                        "JOIN (SELECT j, k FROM jit_hash_build_multi_r WHERE j > 10) b "
	                        "ON a.i=b.j AND a.k=b.k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {9989}));

	bool found_multi_key_build_contract = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-native-build-append-condition-count"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-native-build-append-key-type"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_build_helper"));
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON) &&
		    StringUtil::Contains(event.reason, "condition_count=2")) {
			found_multi_key_build_contract = true;
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.reason, "build_append_shape_ready=true"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-protocol-stage"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
			REQUIRE(StringUtil::Contains(event.ir, "key0<input_index="));
			REQUIRE(StringUtil::Contains(event.ir, "key1<input_index="));
		}
	}
	REQUIRE(found_multi_key_build_contract);
}

TEST_CASE("JIT hash join probe lowers native non-equality match predicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_predicate_l(i BIGINT, j BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_hash_native_predicate_l VALUES "
	                          "(1, 10), (2, 20), (3, NULL), (4, 40), (5, 50)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_predicate_r(i BIGINT, j BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_hash_native_predicate_r VALUES "
	                          "(1, 11), (2, 20), (3, 30), (4, 39), (5, NULL)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT sum(r.j) FROM jit_hash_native_predicate_l l "
	                        "JOIN jit_hash_native_predicate_r r ON l.i=r.i AND l.j<>r.j");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {50}));

	bool found_native_build_contract = false;
	bool found_native_predicate_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "sink-fusion-gap:hash-join-build-protocol-missing"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_hash_join_probe_blocker=hash-join-native-non-equality-condition"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_probe_helper"));
		if (StringUtil::Contains(event.reason, "non_equality_condition_count=1")) {
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_hash_join_build_contract_status=missing"));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.reason, "build_append_shape_ready=true"));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON) &&
		    StringUtil::Contains(event.reason, "non_equality_condition_count=1")) {
			found_native_build_contract = true;
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
			REQUIRE(StringUtil::Contains(event.ir, "key0<input_index="));
			REQUIRE(StringUtil::Contains(event.ir, "key1<input_index="));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON) &&
		    StringUtil::Contains(event.reason, "non_equality_condition_count=1")) {
			found_native_predicate_probe = true;
			REQUIRE(event.code_size > 0);
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe("));
			REQUIRE(StringUtil::Contains(event.ir, "hash_keys=1"));
			REQUIRE(StringUtil::Contains(event.ir, "predicate1<input_index="));
			REQUIRE(StringUtil::Contains(event.ir, "comparison=notequal"));
		}
	}
	REQUIRE(found_native_build_contract);
	REQUIRE(found_native_predicate_probe);
}

TEST_CASE("JIT hash join probe keeps non-equality duplicate chains honest", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_chain_l(i BIGINT, j BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_hash_native_chain_l VALUES (1, 10), (1, 12), (2, 20)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_native_chain_r(i BIGINT, j BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_hash_native_chain_r VALUES "
	                          "(1, 9), (1, 10), (1, 11), (2, 20), (2, 21)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*), sum(r.j) FROM jit_hash_native_chain_l l "
	                        "JOIN jit_hash_native_chain_r r ON l.i=r.i AND l.j<>r.j");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "6");
	REQUIRE(result->GetValue(1, 0).ToString() == "71");

	bool found_native_predicate_probe = false;
	bool found_chain_protocol_decline = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_hash_join_probe_blocker=hash-join-native-non-equality-condition"));
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON) &&
		    StringUtil::Contains(event.reason, "non_equality_condition_count=1")) {
			found_native_predicate_probe = true;
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe("));
			REQUIRE(StringUtil::Contains(event.ir, "hash_keys=1"));
			REQUIRE(StringUtil::Contains(event.ir, "predicate1<input_index="));
			REQUIRE(StringUtil::Contains(event.ir, "comparison=notequal"));
		}
		if (event.phase == "runtime" && event.status == "declined" &&
		    StringUtil::Contains(event.reason, "hash-join-native-runtime-non-equality-chain-protocol-missing")) {
			found_chain_protocol_decline = true;
			REQUIRE(event.runtime_result == "fallback");
		}
	}
	REQUIRE(found_native_predicate_probe);
	REQUIRE(found_chain_protocol_decline);
}

TEST_CASE("JIT hash join native protocols own correlated mark state", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_build_outer(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_mark_build_outer VALUES (1, 1), (5, 1), (NULL, 1), (5, 2)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_build_inner(k BIGINT, g BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_mark_build_inner VALUES (1, 1), (NULL, 1)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto true_result = con.Query("SELECT count(*) FROM ("
	                             "SELECT k IN (SELECT i.k FROM jit_mark_build_inner i WHERE i.g=o.g) AS marker "
	                             "FROM jit_mark_build_outer o) t WHERE marker");
	REQUIRE_NO_FAIL(*true_result);
	REQUIRE(true_result->GetValue(0, 0).ToString() == "1");

	auto null_result = con.Query("SELECT count(*) FROM ("
	                             "SELECT k IN (SELECT i.k FROM jit_mark_build_inner i WHERE i.g=o.g) AS marker "
	                             "FROM jit_mark_build_outer o) t WHERE marker IS NULL");
	REQUIRE_NO_FAIL(*null_result);
	REQUIRE(null_result->GetValue(0, 0).ToString() == "2");

	auto false_result = con.Query("SELECT count(*) FROM ("
	                              "SELECT k IN (SELECT i.k FROM jit_mark_build_inner i WHERE i.g=o.g) AS marker "
	                              "FROM jit_mark_build_outer o) t WHERE marker = false");
	REQUIRE_NO_FAIL(*false_result);
	REQUIRE(false_result->GetValue(0, 0).ToString() == "1");

	bool found_correlated_mark_protocol = false;
	bool found_correlated_mark_build = false;
	bool found_correlated_mark_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_build_helper"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-typed-build-helper"));
		if (!StringUtil::Contains(event.reason, "join_type=mark") ||
		    !StringUtil::Contains(event.reason, "delim_types=1")) {
			continue;
		}
		found_correlated_mark_protocol = true;
		REQUIRE(StringUtil::Contains(event.reason, "correlated_mark_counts_required=true"));
		REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
		REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
		REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
		REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
		REQUIRE(StringUtil::Contains(event.reason, "native_probe_shape_ready=true"));
		REQUIRE(StringUtil::Contains(event.reason, "native_probe_output_mode=mark_probe"));
		REQUIRE(StringUtil::Contains(event.reason, "build_append_shape_ready=true"));
		REQUIRE(StringUtil::Contains(event.reason, "build_append_shape_blocker=none"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "native_hash_join_probe_blocker=hash-join-native-correlated-mark-state"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "build_append_shape_blocker=hash-join-native-correlated-mark-state"));
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    event.candidate_scope == "full_pipeline" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON)) {
			found_correlated_mark_build = true;
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
			REQUIRE(StringUtil::Contains(event.ir, "build_append_shape_ready=true"));
			REQUIRE(StringUtil::Contains(event.ir, "build_append_shape_blocker=none"));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=") &&
		    StringUtil::Contains(event.ir, "output_mode=mark_probe")) {
			found_correlated_mark_probe = true;
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.ir, "correlated_mark_counts_required=true"));
		}
	}
	REQUIRE(found_correlated_mark_protocol);
	REQUIRE(found_correlated_mark_build);
	REQUIRE(found_correlated_mark_probe);
}

	TEST_CASE("JIT hash join probe and build protocols stay honest across join shapes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 100)::DOUBLE AS k, i::BIGINT AS v "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim AS "
	                          "SELECT i::DOUBLE AS k FROM range(100) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_fact_int AS "
	                          "SELECT i::BIGINT AS i, (i % 100)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_int AS "
	                          "SELECT i::BIGINT AS k FROM range(100) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_fact_text_key AS "
	                          "SELECT i::BIGINT AS i, CAST(i % 100 AS VARCHAR) AS k, i::BIGINT AS v "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_text_key AS "
	                          "SELECT CAST(i AS VARCHAR) AS k FROM range(100) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_fact_null_key AS "
	                          "SELECT i::BIGINT AS i, "
	                          "CASE WHEN i % 5 = 0 THEN NULL ELSE CAST(i % 10 AS VARCHAR) END AS k, "
	                          "i::BIGINT AS v FROM range(100) tbl(i)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_null_key AS "
		                          "SELECT CAST(i AS VARCHAR) AS k FROM range(10) tbl(i) "
		                          "UNION ALL SELECT NULL"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_fact_null_int_key AS "
		                          "SELECT i::BIGINT AS i, "
		                          "CASE WHEN i % 5 = 0 THEN NULL ELSE i % 10 END::BIGINT AS k, "
		                          "i::BIGINT AS v FROM range(100) tbl(i)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_null_int_key AS "
		                          "SELECT i::BIGINT AS k FROM range(10) tbl(i) "
		                          "UNION ALL SELECT NULL::BIGINT"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_half AS "
		                          "SELECT i::BIGINT AS k FROM range(50) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_dim_empty AS "
	                          "SELECT i::BIGINT AS k FROM range(0) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

		auto require_hash_join_protocol = [&](const string &query, vector<string> expected, const string &join_type_ir,
		                                      bool allow_native_probe = false,
		                                      bool expect_null_equal_native_probe = false) {
		manager.ClearEvents();
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		for (idx_t column_idx = 0; column_idx < expected.size(); column_idx++) {
			REQUIRE(result->GetValue(column_idx, 0).ToString() == expected[column_idx]);
		}

		bool found_protocol = false;
		bool found_native_or_blocked_join_lowering = false;
		bool found_native_probe = false;
		string expected_probe_output_mode = "none";
		if (join_type_ir == "join_type=inner" || join_type_ir == "join_type=right") {
			expected_probe_output_mode = "matched_probe_and_build";
		} else if (join_type_ir == "join_type=semi") {
			expected_probe_output_mode = "matched_probe_only";
		} else if (join_type_ir == "join_type=right_semi") {
			expected_probe_output_mode = "mark_build_only";
		}
		for (auto &event : manager.GetEvents()) {
			if (event.target != "region") {
				continue;
			}
				REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_probe_helper"));
				REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-typed-probe-helper"));
				REQUIRE_FALSE(StringUtil::Contains(event.reason,
				                                   "hash-join-native-probe-null-equal-condition"));
				REQUIRE_FALSE(StringUtil::Contains(event.ir, "typed_hash_join_probe_helper"));
				REQUIRE_FALSE(StringUtil::Contains(event.ir, "hash-join-typed-probe-helper"));
				if (event.status == "compiled" && event.execution_mode == "native" &&
			    StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=")) {
				REQUIRE(allow_native_probe);
				found_native_probe = true;
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
					REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY));
					REQUIRE(StringUtil::Contains(event.reason, "native_probe_shape_ready=true"));
					REQUIRE(StringUtil::Contains(event.ir, string("output_mode=") + expected_probe_output_mode));
					if (expect_null_equal_native_probe) {
						REQUIRE(StringUtil::Contains(event.ir, "null_equal=true"));
					}
				}
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash join build native executable primitive is not implemented"));
			found_native_or_blocked_join_lowering =
			    found_native_or_blocked_join_lowering ||
			    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON) ||
			    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY) ||
			    StringUtil::Contains(event.reason, "hash-join-native-probe") ||
			    StringUtil::Contains(event.reason, "hash-join-native-build-append") ||
			    StringUtil::Contains(event.ir, "native_probe_shape_ready=false") ||
			    StringUtil::Contains(event.ir, "build_append_shape_ready=false") ||
			    StringUtil::Contains(event.ir, "build_append_shape_ready=true");
			if (!StringUtil::Contains(event.ir, "hash_join_protocol<") ||
			    !StringUtil::Contains(event.ir, join_type_ir)) {
				continue;
			}
			found_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, "native_hash_join_probe_required_capability="
			                                       "hash-join-native-probe"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(
			    event.ir, string("native_probe_output_mode=") + expected_probe_output_mode));
		}
		REQUIRE(found_protocol);
		REQUIRE(found_native_or_blocked_join_lowering);
		if (allow_native_probe) {
			REQUIRE(found_native_probe);
		}
	};

	require_hash_join_protocol("SELECT sum(f.v) FROM jit_hash_probe_fact f "
	                              "JOIN jit_hash_probe_dim d ON f.k=d.k",
	                              {"49995000"}, "join_type=inner");
	require_hash_join_protocol("SELECT sum(f.v) FROM jit_hash_probe_fact f "
	                              "JOIN jit_hash_probe_dim d ON f.k=d.k "
	                              "WHERE f.i % 100 < 2",
	                              {"990100"}, "join_type=inner");
	require_hash_join_protocol("SELECT sum(f.v) FROM jit_hash_probe_fact_int f "
	                              "JOIN jit_hash_probe_dim_int d ON f.k=d.k "
	                              "WHERE f.i < 200",
	                              {"19900"}, "join_type=inner", true);
	require_hash_join_protocol("SELECT sum(f.v) FROM jit_hash_probe_fact_text_key f "
	                              "JOIN jit_hash_probe_dim_text_key d ON f.k=d.k "
	                              "WHERE f.i < 200",
	                              {"19900"}, "join_type=inner");
		require_hash_join_protocol("SELECT count(*), sum(f.v) FROM jit_hash_probe_fact_null_key f "
		                              "JOIN jit_hash_probe_dim_null_key d ON f.k IS NOT DISTINCT FROM d.k "
		                              "WHERE f.i < 20",
		                              {"20", "190"}, "join_type=inner");
		require_hash_join_protocol("SELECT count(*), sum(f.v) FROM jit_hash_probe_fact_null_int_key f "
		                           "JOIN jit_hash_probe_dim_null_int_key d ON f.k IS NOT DISTINCT FROM d.k "
		                           "WHERE f.i < 100",
		                           {"100", "4950"}, "join_type=inner", true, true);
	require_hash_join_protocol("SELECT count(*), count(d.k) FROM jit_hash_probe_fact_int f "
	                              "LEFT JOIN jit_hash_probe_dim_half d ON f.k=d.k "
	                              "WHERE f.i < 100",
	                              {"100", "50"}, "join_type=left");
	require_hash_join_protocol("SELECT count(*) FROM jit_hash_probe_fact_int f "
	                              "SEMI JOIN jit_hash_probe_dim_half d ON f.k=d.k "
	                              "WHERE f.i < 100",
	                              {"50"}, "join_type=semi", true);
	require_hash_join_protocol("SELECT count(*) FROM jit_hash_probe_fact_int f "
	                              "ANTI JOIN jit_hash_probe_dim_half d ON f.k=d.k "
	                              "WHERE f.i < 100",
	                              {"50"}, "join_type=anti");
	require_hash_join_protocol("SELECT count(*), count(d.k) FROM jit_hash_probe_fact_int f "
	                              "LEFT JOIN jit_hash_probe_dim_empty d ON f.k=d.k "
	                              "WHERE f.i < 5",
	                              {"5", "0"}, "join_type=left");
}

TEST_CASE("JIT full pipeline exposes expanded hash join protocol blockers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_expanded_fact AS "
	                          "SELECT i::BIGINT AS i, (i % 100)::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(1000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_probe_expanded_dim AS "
	                          "SELECT i::BIGINT AS k FROM range(50) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_optimizer"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	auto require_join_protocol = [&](const string &query, const string &expected, const string &join_type_ir,
	                                 bool allow_native_probe = false, bool forbid_native_build = false) {
		manager.ClearEvents();
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->GetValue(0, 0).ToString() == expected);

		bool found_protocol = false;
		bool found_native_or_blocked_join_lowering = false;
		bool found_native_probe = false;
		string expected_probe_output_mode = "none";
		if (join_type_ir == "join_type=right") {
			expected_probe_output_mode = "matched_probe_and_build";
		} else if (join_type_ir == "join_type=mark") {
			expected_probe_output_mode = "mark_probe";
		} else if (join_type_ir == "join_type=right_semi") {
			expected_probe_output_mode = "mark_build_only";
		}
		for (auto &event : manager.GetEvents()) {
			if (event.target != "region") {
				continue;
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_probe_helper"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-typed-probe-helper"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "typed_hash_join_probe_helper"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "hash-join-typed-probe-helper"));
			if (event.status == "compiled" && event.execution_mode == "native" &&
			    StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=")) {
				REQUIRE(allow_native_probe);
				found_native_probe = true;
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY));
				REQUIRE(StringUtil::Contains(event.reason, "native_probe_shape_ready=true"));
				if (expected_probe_output_mode == "mark_probe") {
					REQUIRE_FALSE(StringUtil::Contains(event.ir, "mark_build_match=true"));
				} else {
					REQUIRE(StringUtil::Contains(event.ir, "mark_build_match=true"));
				}
				REQUIRE(StringUtil::Contains(event.ir, string("output_mode=") + expected_probe_output_mode));
			}
			if (event.status == "compiled" && event.execution_mode == "native" &&
			    StringUtil::Contains(event.ir, "hash_join_build(") &&
			    StringUtil::Contains(event.reason, join_type_ir)) {
				REQUIRE_FALSE(forbid_native_build);
			}
			if (StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash join build native executable primitive is not implemented"));
			found_native_or_blocked_join_lowering =
			    found_native_or_blocked_join_lowering ||
			    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON) ||
			    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY) ||
			    StringUtil::Contains(event.reason, "hash-join-native-probe") ||
			    StringUtil::Contains(event.reason, "hash-join-native-build-append") ||
			    StringUtil::Contains(event.ir, "native_probe_shape_ready=false") ||
			    StringUtil::Contains(event.ir, "build_append_shape_ready=false") ||
			    StringUtil::Contains(event.ir, "build_append_shape_ready=true");
			if (event.ir.empty() || !StringUtil::Contains(event.ir, join_type_ir)) {
				continue;
			}
			found_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, "native_hash_join_probe_required_capability="
			                                       "hash-join-native-probe"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_native_protocol_blocker=none"));
			REQUIRE(StringUtil::Contains(
			    event.ir, string("native_probe_output_mode=") + expected_probe_output_mode));
		}
		REQUIRE(found_protocol);
		REQUIRE(found_native_or_blocked_join_lowering);
		if (allow_native_probe) {
			REQUIRE(found_native_probe);
		}
	};

	require_join_protocol("SELECT sum(f.v) FROM jit_hash_probe_expanded_fact f "
	                      "RIGHT JOIN jit_hash_probe_expanded_dim d ON f.k=d.k",
	                      "237250", "join_type=right", true);
	require_join_protocol("SELECT sum(COALESCE(f.v, 0)) FROM jit_hash_probe_expanded_fact f "
	                      "FULL OUTER JOIN jit_hash_probe_expanded_dim d ON f.k=d.k",
	                      "499500", "join_type=full");
	require_join_protocol("SELECT count(*) FROM ("
	                      "SELECT i IN (SELECT k FROM jit_hash_probe_expanded_dim) AS marker "
	                      "FROM jit_hash_probe_expanded_fact WHERE i < 100) t WHERE marker",
	                      "50", "join_type=mark", true);
}

TEST_CASE("JIT full pipeline executes native hash join probe for regular native probe table", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_hash_probe_l AS "
	                          "SELECT (i * 1000000)::BIGINT AS k, i::BIGINT AS v FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_hash_probe_r AS "
	                          "SELECT (i * 1000000)::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT sum(v) FROM jit_native_hash_probe_l l "
	                        "JOIN jit_native_hash_probe_r r USING(k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "49995000");

	bool found_compiled_probe = false;
	bool found_executed_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
				found_compiled_probe = true;
				REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
				REQUIRE(StringUtil::Contains(event.ir,
				                             "hash_join_probe(hash_keys=1,conditions=key0<input_index="));
			}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows == 10000) {
			found_executed_probe = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_probe);
	REQUIRE(found_executed_probe);
}

TEST_CASE("JIT native hash join probe preserves salt after key collision mismatch", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_hash_probe_collision_build(k BIGINT, v BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_native_hash_probe_collision_build VALUES "
	                          "(3585125, 10), (1416129, 20)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_native_hash_probe_collision_probe(k BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_native_hash_probe_collision_probe VALUES (1416129)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*), sum(v) FROM jit_native_hash_probe_collision_probe p "
	                        "JOIN jit_native_hash_probe_collision_build b USING(k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "1");
	REQUIRE(result->GetValue(1, 0).ToString() == "20");

	bool found_compiled_probe = false;
	bool found_executed_probe = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_READY)) {
			found_compiled_probe = true;
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.ir,
			                             "hash_join_probe(hash_keys=1,conditions=key0<input_index="));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_executed_probe = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_compiled_probe);
	REQUIRE(found_executed_probe);
}

TEST_CASE("JIT native hash join probe materializes non-correlated mark output", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_probe_l(k BIGINT, v BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_mark_probe_l VALUES (1, 10), (2, 20), (NULL, 30), (3, 40)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mark_probe_r(k BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_mark_probe_r VALUES (1), (NULL)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto null_result = con.Query("SELECT count(*) FROM ("
	                             "SELECT k IN (SELECT k FROM jit_mark_probe_r) AS marker "
	                             "FROM jit_mark_probe_l) t WHERE marker IS NULL");
	REQUIRE_NO_FAIL(*null_result);
	REQUIRE(null_result->RowCount() == 1);
	REQUIRE(null_result->GetValue(0, 0).GetValue<int64_t>() == 3);

	auto true_result = con.Query("SELECT count(*) FROM ("
	                             "SELECT k IN (SELECT k FROM jit_mark_probe_r) AS marker "
	                             "FROM jit_mark_probe_l) t WHERE marker");
	REQUIRE_NO_FAIL(*true_result);
	REQUIRE(true_result->RowCount() == 1);
	REQUIRE(true_result->GetValue(0, 0).GetValue<int64_t>() == 1);

	bool found_mark_probe_protocol = false;
	bool found_compiled_mark_probe = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (StringUtil::Contains(event.ir, "hash_join_protocol<") &&
		    StringUtil::Contains(event.ir, "join_type=mark")) {
			found_mark_probe_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, "native_probe_output_mode=mark_probe"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=") &&
		    StringUtil::Contains(event.ir, "output_mode=mark_probe")) {
			found_compiled_mark_probe = true;
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.reason, "hash-join-probe-filter-projection-ungrouped-aggregate-update"));
			REQUIRE(StringUtil::Contains(event.ir, "projection(native:constant<INTEGER>(42)"));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows == 4) {
			found_runtime = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_mark_probe_protocol);
	REQUIRE(found_compiled_mark_probe);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT native hash join probe marks right semi duplicate chains", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_right_semi_build(k BIGINT, v BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_right_semi_build VALUES (1, 10), (1, 20), (1, 30), (2, 40), (3, 50)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_right_semi_probe AS "
	                          "SELECT 1::BIGINT AS k FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*), sum(v) FROM jit_right_semi_build b "
	                        "SEMI JOIN jit_right_semi_probe p ON b.k=p.k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "3");
	REQUIRE(result->GetValue(1, 0).ToString() == "60");

	bool found_right_semi_protocol = false;
	bool found_mark_only_probe = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region") {
			continue;
		}
		if (StringUtil::Contains(event.ir, "hash_join_protocol<") &&
		    StringUtil::Contains(event.ir, "join_type=right_semi")) {
			found_right_semi_protocol = true;
			REQUIRE(StringUtil::Contains(event.ir, "native_probe_output_mode=mark_build_only"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_BLOCKER));
		}
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.ir, "hash_join_probe(hash_keys=") &&
		    StringUtil::Contains(event.reason, "join_type=right_semi")) {
			found_mark_only_probe = true;
			REQUIRE(StringUtil::Contains(event.reason, JIT_HASH_JOIN_PROBE_EXECUTABLE_REASON));
			REQUIRE(StringUtil::Contains(event.ir, "output_mode=mark_build_only"));
			REQUIRE(StringUtil::Contains(event.ir, "mark_build_match=true"));
			REQUIRE(StringUtil::Contains(event.ir, "pointer_offset="));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed") && event.input_rows == 10000) {
			found_runtime = true;
			REQUIRE(event.runtime_result == "finished");
		}
	}
	REQUIRE(found_right_semi_protocol);
	REQUIRE(found_mark_only_probe);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT full pipeline lowers native hash join build through backend", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_fused_hash_join_a AS "
	                          "SELECT i::BIGINT AS i, (i + 1)::BIGINT AS x FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_fused_hash_join_b AS "
	                          "SELECT i::BIGINT AS j FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT count(*) FROM jit_fused_hash_join_a a "
	                        "JOIN jit_fused_hash_join_b b ON a.x=b.j WHERE a.i > 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {9988}));

		bool found_hash_join_sink_protocol = false;
		for (auto &event : manager.GetEvents()) {
			if (event.target != "region" || !event.has_candidate || event.candidate_scope != "full_pipeline") {
				continue;
			}
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash join build native executable primitive is not implemented"));
		if (event.status == "compiled" && event.execution_mode == "native" &&
		    StringUtil::Contains(event.ir, "hash_join_protocol<") &&
		    StringUtil::Contains(event.reason, JIT_HASH_JOIN_BUILD_EXECUTABLE_REASON)) {
			found_hash_join_sink_protocol = true;
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "typed_hash_join_build_helper"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "hash-join-typed-build-helper"));
			REQUIRE(StringUtil::Contains(event.reason, "full-pipeline-native-protocol-stage"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "full-pipeline-native-sink-update"));
			REQUIRE(StringUtil::Contains(event.ir, "regular_hash_table_layout_ready=true"));
			REQUIRE(StringUtil::Contains(event.ir, "hash_join_native_protocol_blocker=none"));
			REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_PROBE_READY_CONTRACT));
				REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_CONTRACT));
				REQUIRE(StringUtil::Contains(event.ir, JIT_HASH_JOIN_BUILD_READY_BLOCKER));
				REQUIRE(StringUtil::Contains(event.ir, "build_append_shape_ready=true"));
				REQUIRE(StringUtil::Contains(event.ir, "key0<input_index="));
				REQUIRE(StringUtil::Contains(event.ir, "hash_join_build("));
			}
		}
		REQUIRE(found_hash_join_sink_protocol);
	}

TEST_CASE("JIT auto precheck skips region candidates before backend analysis", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoPrecheckCountingBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_precheck_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.precheck_count > 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetCounters().empty());
	REQUIRE(manager.GetDecisionCounters().empty());

	backend_ref.precheck_count.store(0);
	backend_ref.region_analyze_count.store(0);
	backend_ref.region_compile_count.store(0);
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_dump_ir_precheck_skip = false;
	bool found_dump_ir_filter = false;
	bool found_dump_ir_projection = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_auto_precheck_jit_backend" || event.target != "region") {
			continue;
		}
		found_dump_ir_precheck_skip = true;
		REQUIRE(event.status == "skipped");
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.backend_analysis_time_us == 0);
		REQUIRE(!event.ir.empty());
		REQUIRE(StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir"));
		found_dump_ir_filter = found_dump_ir_filter || StringUtil::Contains(event.ir, ":FILTER");
		found_dump_ir_projection = found_dump_ir_projection || StringUtil::Contains(event.ir, ":PROJECTION");
	}
	REQUIRE(found_dump_ir_precheck_skip);
	REQUIRE(found_dump_ir_filter);
	REQUIRE(found_dump_ir_projection);
}

TEST_CASE("JIT auto pipeline inventory skips typed IR lowering before candidate precheck", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoInventoryGateCountingBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_inventory_gate_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(false));
	Settings::Set<JitTraceDecisionsSetting>(context, SetScope::SESSION, Value::BOOLEAN(false));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.inventory_precheck_count > 0);
	REQUIRE(backend_ref.admission_inventory_count > 0);
	REQUIRE(backend_ref.diagnostic_inventory_count == 0);
	REQUIRE(backend_ref.candidate_precheck_count == 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetCounters().empty());
	REQUIRE(manager.GetDecisionCounters().empty());

	backend_ref.inventory_precheck_count.store(0);
	backend_ref.admission_inventory_count.store(0);
	backend_ref.diagnostic_inventory_count.store(0);
	backend_ref.candidate_precheck_count.store(0);
	backend_ref.region_analyze_count.store(0);
	backend_ref.region_compile_count.store(0);
	Settings::Set<JitTraceDecisionsSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.inventory_precheck_count > 0);
	REQUIRE(backend_ref.admission_inventory_count == 0);
	REQUIRE(backend_ref.diagnostic_inventory_count > 0);
	REQUIRE(backend_ref.candidate_precheck_count == 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);
	REQUIRE(manager.GetEvents().empty());
	auto decision_counters = manager.GetDecisionCounters();
	bool found_inventory_counter = false;
	for (auto &counter : decision_counters) {
		if (counter.backend_name != "contract_test_auto_inventory_gate_jit_backend" || counter.target != "region") {
			continue;
		}
		found_inventory_counter = true;
		REQUIRE(counter.status == "skipped");
		REQUIRE(counter.execution_mode == "executor_fallback");
		REQUIRE(counter.region_execution_form == "none");
		REQUIRE(counter.policy_decision == "auto");
		REQUIRE(counter.has_pipeline);
		REQUIRE(!counter.pipeline_shape.empty());
		REQUIRE(counter.candidate_shape.empty());
		REQUIRE(counter.candidate_scope.empty());
		REQUIRE(StringUtil::StartsWith(counter.admission_shape_key, "contract:auto-inventory:"));
		REQUIRE_FALSE(counter.admission_rule_present);
		REQUIRE_FALSE(counter.has_admission_score);
		REQUIRE(StringUtil::Contains(counter.example_reason, "before typed IR lowering"));
	}
	REQUIRE(found_inventory_counter);

	backend_ref.inventory_precheck_count.store(0);
	backend_ref.admission_inventory_count.store(0);
	backend_ref.diagnostic_inventory_count.store(0);
	backend_ref.candidate_precheck_count.store(0);
	backend_ref.region_analyze_count.store(0);
	backend_ref.region_compile_count.store(0);
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.inventory_precheck_count > 0);
	REQUIRE(backend_ref.admission_inventory_count == 0);
	REQUIRE(backend_ref.diagnostic_inventory_count > 0);
	REQUIRE(backend_ref.candidate_precheck_count == 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_dump_ir_inventory_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_auto_inventory_gate_jit_backend" || event.target != "region") {
			continue;
		}
		found_dump_ir_inventory_skip = true;
		REQUIRE_FALSE(event.has_candidate);
		REQUIRE(event.has_pipeline);
		REQUIRE(event.status == "skipped");
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(StringUtil::Contains(event.ir, "duckdb.region admission-inventory"));
		REQUIRE(StringUtil::Contains(event.reason, "before typed IR lowering"));
	}
	REQUIRE(found_dump_ir_inventory_skip);
}

TEST_CASE("JIT region lowering excludes wrapper-only pipelines", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto has_wrapper_only_pipeline_shape = [](const string &value) {
		auto wrapper_only_pipeline_shape = [](const string &source, const string &sink = string()) {
			auto result = "pipeline;source:source:" + source + ":source-missing-protocol";
			if (!sink.empty()) {
				result += ";sink:sink:" + sink + ":sink";
			}
			return result;
		};
		return value == wrapper_only_pipeline_shape("CREATE_TABLE_AS", "RESULT_COLLECTOR") ||
		       value == wrapper_only_pipeline_shape("RESULT_COLLECTOR") ||
		       value == wrapper_only_pipeline_shape("EXPLAIN_ANALYZE");
	};
	auto has_wrapper_only_region_source = [](const string &value) {
		auto wrapper_only_region_source = [](const string &source) {
			return "source:" + source + ":fallback";
		};
		return StringUtil::Contains(value, wrapper_only_region_source("CREATE_TABLE_AS")) ||
		       StringUtil::Contains(value, wrapper_only_region_source("RESULT_COLLECTOR")) ||
		       StringUtil::Contains(value, wrapper_only_region_source("EXPLAIN_ANALYZE"));
	};

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE jit_inventory_wrapper_only AS SELECT 42 AS i"));
	REQUIRE(manager.GetEvents().empty());
	for (auto &counter : manager.GetDecisionCounters()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(counter.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(counter.example_reason));
	}

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE jit_inventory_wrapper_only_force AS SELECT 43 AS i"));
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.candidate_pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(event.candidate_context_pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(event.reason));
		REQUIRE_FALSE(has_wrapper_only_region_source(event.ir));
	}
	for (auto &counter : manager.GetDecisionCounters()) {
		REQUIRE_FALSE(has_wrapper_only_pipeline_shape(counter.pipeline_shape));
		REQUIRE_FALSE(has_wrapper_only_region_source(counter.example_reason));
	}
}

TEST_CASE("JIT auto candidate precheck records counters with event retention disabled", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoCandidatePrecheckRejectBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_candidate_precheck_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	Settings::Set<JitTraceDecisionsSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));

	manager.ClearEvents();
	manager.ClearCounters();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.candidate_precheck_count > 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);
	REQUIRE(manager.GetEvents().empty());

	auto decision_counters = manager.GetDecisionCounters();
	bool found_candidate_precheck_counter = false;
	for (auto &counter : decision_counters) {
		if (counter.backend_name != "contract_test_auto_candidate_precheck_jit_backend" || counter.target != "region") {
			continue;
		}
		found_candidate_precheck_counter = true;
		REQUIRE(counter.status == "skipped");
		REQUIRE(counter.execution_mode == "executor_fallback");
		REQUIRE(counter.region_execution_form == "none");
		REQUIRE(counter.policy_decision == "auto");
		REQUIRE_FALSE(counter.has_pipeline);
		REQUIRE(counter.pipeline_shape.empty());
		REQUIRE(!counter.candidate_shape.empty());
		REQUIRE(!counter.candidate_scope.empty());
		REQUIRE(StringUtil::StartsWith(counter.admission_shape_key, "contract:auto-candidate-precheck:"));
		REQUIRE(counter.admission_rule_present);
		REQUIRE(counter.admission_min_cardinality == 1000000);
		REQUIRE(counter.has_admission_score);
		REQUIRE(counter.max_admission_score < 0);
		REQUIRE(counter.admission_proof == "contract:auto-candidate-precheck");
		REQUIRE(StringUtil::Contains(counter.example_reason, "before backend analysis"));
	}
	REQUIRE(found_candidate_precheck_counter);

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=10000"));
	Settings::Set<JitDumpIrSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	REQUIRE(backend_ref.candidate_precheck_count > 0);
	REQUIRE(backend_ref.region_analyze_count == 0);
	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_dump_ir_candidate_skip = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_auto_candidate_precheck_jit_backend" || event.target != "region") {
			continue;
		}
		found_dump_ir_candidate_skip = true;
		REQUIRE(event.status == "skipped");
		REQUIRE(event.execution_mode == "executor_fallback");
		REQUIRE(event.has_candidate);
		REQUIRE_FALSE(event.has_pipeline);
		REQUIRE(event.has_admission);
		REQUIRE(StringUtil::StartsWith(event.admission_shape_key, "contract:auto-candidate-precheck:"));
		REQUIRE(event.admission_rule_present);
		REQUIRE(!event.ir.empty());
		REQUIRE(StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir"));
	}
	REQUIRE(found_dump_ir_candidate_skip);
}

TEST_CASE("JIT auto region selection uses maximal transform candidates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<AutoSelectionBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_auto_select_jit_backend");
	Settings::Set<JitPolicySetting>(context, SetScope::SESSION, Value("auto"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_auto_selection_input AS "
	                          "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT s FROM (SELECT j AS s FROM "
	                          "(SELECT i + 1 AS j FROM jit_auto_selection_input WHERE i > 0) t1) t2"));

	REQUIRE(std::find(backend_ref.compiled_candidate_shapes.begin(), backend_ref.compiled_candidate_shapes.end(),
	                  "boundary-only") == backend_ref.compiled_candidate_shapes.end());
	REQUIRE(std::find(backend_ref.compiled_candidate_shapes.begin(), backend_ref.compiled_candidate_shapes.end(),
	                  "projection") == backend_ref.compiled_candidate_shapes.end());
	REQUIRE(!backend_ref.compiled_candidate_shapes.empty());

	bool found_maximal_region_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_auto_select_jit_backend" || event.target != "region") {
			continue;
		}
		REQUIRE_FALSE(event.candidate_shape == "projection");
		if (event.status == "compiled") {
			found_maximal_region_compile = true;
			REQUIRE(event.policy_decision == "auto");
			REQUIRE(event.execution_mode == "native");
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(StringUtil::Contains(event.candidate_shape, "projection"));
		}
	}
	REQUIRE(found_maximal_region_compile);
}

TEST_CASE("JIT region capability requires explicit compiled execution mode", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	auto backend = make_uniq<ImplicitModeRegionBackend>();
	auto &backend_ref = *backend;
	manager.RegisterBackend(std::move(backend));
	SetJitTestOptions(context, "contract_test_implicit_mode_region_jit_backend");

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i FROM range(3) tbl(i) WHERE i > 0"));
	REQUIRE(backend_ref.region_compile_count == 0);

	bool found_unsupported_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_implicit_mode_region_jit_backend" || event.target != "region") {
			continue;
		}
		if (event.status == "unsupported" &&
		    StringUtil::Contains(event.reason, "backend cannot generate executable code for this whole region")) {
			found_unsupported_region = true;
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.policy_decision == "force");
			REQUIRE(StringUtil::Contains(event.reason, "region-lowering:native=1"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
		}
	}
	REQUIRE(found_unsupported_region);
}

TEST_CASE("JIT manager rejects compiled kernels without executable code", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<ZeroCodeRegionBackend>());
	SetJitTestOptions(context, "contract_test_zero_code_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_zero_code_input AS "
	                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT i + 1 FROM jit_zero_code_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled region without executable code"));
}

TEST_CASE("JIT manager rejects non-compiled results with kernels", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<NonCompiledKernelResultBackend>());
	SetJitTestOptions(context, "contract_test_non_compiled_kernel_result_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_non_compiled_kernel_input AS "
	                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT i + 1 FROM jit_non_compiled_kernel_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "returned kernel for non-compiled region status unsupported"));
}

TEST_CASE("JIT manager rejects source pipeline kernels without source-prefix ABI", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<SourceAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_source_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_source_abi_input AS SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT i + 1 FROM jit_source_abi_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled source pipeline without source-prefix executable ABI"));
}

TEST_CASE("JIT manager rejects backend-fused regions across non-fused core stages", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<FullPipelineAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_full_pipeline_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_core_gate = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_full_pipeline_abi_region_jit_backend" || event.target != "region" ||
		    event.status != "unsupported") {
			continue;
		}
		if (!StringUtil::Contains(event.reason,
		                          "backend advertised fused region but core native-fusion contract is not ready")) {
			continue;
		}
		found_core_gate = true;
		REQUIRE(event.execution_mode == "unsupported");
		REQUIRE(event.region_execution_form == "fused");
		REQUIRE_FALSE(event.candidate_contract.native_fusion_ready);
		REQUIRE(StringUtil::Contains(event.reason, "executor_boundaries="));
		REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
	}
	REQUIRE(found_core_gate);
}

TEST_CASE("JIT maximal region planner does not emit sink-only ABI candidates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<SinkAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_sink_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM range(3) tbl(i)"));
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE((event.has_candidate && event.candidate_scope == "sink_pipeline"));
	}
}

TEST_CASE("JIT manager rejects full pipeline kernels without full-pipeline ABI", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<FullPipelineAbiRejectRegionBackend>());
	SetJitTestOptions(context, "contract_test_full_pipeline_abi_region_jit_backend");

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_full_abi_input AS SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	auto result = con.Query("SELECT sum(i) FROM jit_full_abi_input WHERE i > 0");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "compiled full pipeline without full-pipeline executable ABI"));
}

TEST_CASE("JIT full pipeline ABI dispatch falls back honestly after decline", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<DecliningFullPipelineRegionBackend>());
	SetJitTestOptions(context, "contract_test_declining_full_pipeline_region_jit_backend");
	Settings::Set<JitTraceRuntimeSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_declining_full_input AS SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM jit_declining_full_input WHERE i > 0"));

	bool found_compiled_full_pipeline = false;
	bool found_declined_full_pipeline = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "contract_test_declining_full_pipeline_region_jit_backend" ||
		    event.target != "region" || event.candidate_scope != "full_pipeline") {
			continue;
		}
		if (event.status == "compiled") {
			found_compiled_full_pipeline = true;
			REQUIRE(event.execution_mode == "native");
		}
		if (event.phase == "runtime" && event.status == "declined") {
			found_declined_full_pipeline = true;
			REQUIRE(event.execution_mode == "native");
			REQUIRE(StringUtil::Contains(event.reason, "full pipeline kernel declined"));
		}
	}
	REQUIRE(found_compiled_full_pipeline);
	REQUIRE(found_declined_full_pipeline);
}

TEST_CASE("JIT full pipeline ABI rejects decline after runtime side effects", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<SideEffectDecliningFullPipelineRegionBackend>());
	SetJitTestOptions(context, "contract_test_side_effect_declining_full_pipeline_region_jit_backend");
	Settings::Set<JitTraceRuntimeSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_side_effect_full_input AS SELECT i::BIGINT AS i FROM range(4) tbl(i)"));
	auto result = con.Query("SELECT sum(i) FROM jit_side_effect_full_input");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(),
	                             "JIT full pipeline kernel declined after using runtime side-effect APIs"));
}

TEST_CASE("JIT source-prefix runtime reports JIT-only exceptions", "[api][jit]") {
	{
		DuckDB db;
		Connection con(db);
		auto &context = *con.context;
		auto &manager = JitManager::Get(context);

		manager.RegisterBackend(make_uniq<ThrowingVerifiedRegionBackend>());
		SetJitTestOptions(context, "contract_test_throwing_region_jit_backend");
		Settings::Set<JitVerifySetting>(context, SetScope::SESSION, Value::BOOLEAN(true));

		REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_throwing_verify_input AS "
		                          "SELECT i::BIGINT AS i FROM range(3) tbl(i)"));
			auto result = con.Query("SELECT i + 1 FROM jit_throwing_verify_input WHERE i > 0");
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), "contract test region runtime failure"));
		}
	}

TEST_CASE("JIT code handles clean up through their base interface", "[api][jit]") {
	bool destroyed = false;
	{
		unique_ptr<JitCodeHandle> handle = make_uniq<CountingCodeHandle>(destroyed);
		REQUIRE(handle->CodeSize() == 17);
	}
	REQUIRE(destroyed);
}

TEST_CASE("JIT events are bounded and counters are cumulative", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=3"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_event_bound_input AS "
	                          "SELECT i::BIGINT AS i FROM range(1000) tbl(i)"));

	auto counter_count_before = TotalJitCounterCount(manager.GetCounters());
	auto decision_counter_count_before = TotalJitDecisionCounterCount(manager.GetDecisionCounters());
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));

	auto events = manager.GetEvents();
	REQUIRE(events.size() == 3);
	REQUIRE(events[0].event_id + 1 == events[1].event_id);
	REQUIRE(events[1].event_id + 1 == events[2].event_id);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) > counter_count_before);

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=2"));
	events = manager.GetEvents();
	REQUIRE(!events.empty());
	REQUIRE(events.size() <= 2);
	if (events.size() == 2) {
		REQUIRE(events[0].event_id + 1 == events[1].event_id);
	}

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=3"));

	auto last_event_id = events.back().event_id;
	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	events = manager.GetEvents();
	REQUIRE(!events.empty());
	REQUIRE(events.front().event_id > last_event_id);

	auto counter_count_before_sql_clear = TotalJitCounterCount(manager.GetCounters());
	auto decision_counter_count_before_sql_clear = TotalJitDecisionCounterCount(manager.GetDecisionCounters());
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_events()"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count_before_sql_clear);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count_before_sql_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM duckdb_jit_clear_counters()"));
	REQUIRE(manager.GetCounters().empty());
	REQUIRE(manager.GetDecisionCounters().empty());
	auto counter_count_after_explicit_clear = TotalJitCounterCount(manager.GetCounters());
	auto decision_counter_count_after_explicit_clear = TotalJitDecisionCounterCount(manager.GetDecisionCounters());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) > counter_count_after_explicit_clear);

	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 AS j FROM jit_event_bound_input WHERE i > 500"));
	REQUIRE(manager.GetEvents().empty());
	auto decision_counters = manager.GetDecisionCounters();
	REQUIRE(TotalJitDecisionCounterCount(decision_counters) > decision_counter_count_after_explicit_clear);
	bool found_inventory_shape = false;
	for (auto &counter : decision_counters) {
		if (counter.backend_name != "sljit" || counter.target != "region" || counter.status != "skipped" ||
		    counter.execution_mode != "executor_fallback") {
			continue;
		}
		if (counter.admission_shape_key != "sljit:pipeline-inventory:table-scan-source") {
			continue;
		}
		found_inventory_shape = true;
		REQUIRE(counter.region_execution_form == "none");
		REQUIRE_FALSE(counter.admission_rule_present);
		REQUIRE(counter.max_estimated_cardinality > 0);
	}
	REQUIRE(found_inventory_shape);
}

TEST_CASE("SLJIT marks full pipeline result collector unsupported without native sink protocol", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_verify=true"));
	manager.ClearEvents();
	auto result = con.Query("SELECT i + 1 AS j FROM range(10000) tbl(i) WHERE i > 5000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 4999);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 5002);
	REQUIRE(result->GetValue(0, 4998).GetValue<int64_t>() == 10000);

	bool found_unsupported_full_pipeline = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region" || event.candidate_scope != "full_pipeline") {
			continue;
		}
		REQUIRE_FALSE(event.status == "compiled");
		REQUIRE_FALSE(event.phase == "runtime");
		if (event.status == "unsupported") {
			found_unsupported_full_pipeline = true;
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE(event.code_size == 0);
			REQUIRE(event.candidate_shape == "filter-projection-sink");
			REQUIRE(StringUtil::Contains(event.reason, "source-fusion-gap:requires-native-source"));
			REQUIRE(StringUtil::Contains(event.reason, "source:TABLE_SCAN:fallback"));
			REQUIRE(StringUtil::Contains(event.reason, "sink:RESULT_COLLECTOR:fallback:"
			                                           "full pipeline sink requires native sink or operator update protocol"));
			REQUIRE(StringUtil::Contains(event.reason, "execution:unsupported"));
			REQUIRE(StringUtil::Contains(event.ir, "native_source_contract<status=blocked,"
			                                       "required_capability=table-function-native-source"));
			REQUIRE(StringUtil::Contains(event.ir, "blocker=table-function-source-boundary"));
			REQUIRE(StringUtil::Contains(event.ir, "scope=full_pipeline"));
		}
	}
	REQUIRE(found_unsupported_full_pipeline);
}

TEST_CASE("JIT kernel counters preserve runtime linkage after event eviction", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_kernel_counter_input AS "
	                          "SELECT i::BIGINT AS i FROM range(10000) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_kernel_counter_input WHERE i > 0)"));

	optional_idx fused_kernel_id;
	for (auto &counter : manager.GetKernelCounters()) {
		if (counter.backend_name == "sljit" && counter.target == "region" && counter.execution_mode == "native" &&
		    counter.region_execution_form == "fused" && counter.invocation_count > 0 &&
		    counter.last_runtime_status == "executed") {
			fused_kernel_id = counter.kernel_id;
			REQUIRE(counter.code_size > 0);
			REQUIRE(counter.has_candidate);
			REQUIRE(!counter.candidate_shape.empty());
			REQUIRE(IsKnownJitCandidateScope(counter.candidate_scope));
			REQUIRE(!counter.candidate_pipeline_shape.empty());
			REQUIRE(counter.candidate_estimated_cardinality > 0);
			REQUIRE(counter.input_rows + counter.output_rows > 0);
			REQUIRE(counter.generated_body_runtime_time_us >= 0);
			REQUIRE(counter.last_runtime_status == "executed");
			const bool expected_runtime_result =
			    counter.last_runtime_result == "need_more_input" || counter.last_runtime_result == "finished";
			REQUIRE(expected_runtime_result);
		}
	}
	REQUIRE(fused_kernel_id.IsValid());

	bool retained_compile_event = false;
	for (auto &event : manager.GetEvents()) {
		if (event.phase == "compile" && event.kernel_id == fused_kernel_id.GetIndex()) {
			retained_compile_event = true;
		}
	}
	REQUIRE(!retained_compile_event);

	auto visible = con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters() WHERE kernel_id = " +
	                         std::to_string(fused_kernel_id.GetIndex()));
	REQUIRE_NO_FAIL(*visible);
	REQUIRE(CHECK_COLUMN(visible, 0, {1}));

	auto aggregate_count_before_clear = TotalJitCounterCount(manager.GetCounters());
	manager.ClearEvents();
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == aggregate_count_before_clear);
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_kernel_counter_input WHERE i > 0)"));
	REQUIRE(!manager.GetKernelCounters().empty());

	REQUIRE_NO_FAIL(con.Query("SET jit_event_log_size=0"));
	REQUIRE(manager.GetEvents().empty());
	REQUIRE(manager.GetKernelCounters().empty());
	auto hidden = con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters()");
	REQUIRE_NO_FAIL(*hidden);
	REQUIRE(CHECK_COLUMN(hidden, 0, {0}));
}

TEST_CASE("JIT kernel counters can be recreated from runtime trace identity", "[api][jit]") {
	JitEventLog log;

	JitEvent compile_event;
	compile_event.phase = "compile";
	compile_event.backend_name = "unit";
	compile_event.target = "region";
	compile_event.status = "compiled";
	compile_event.execution_mode = "native";
	compile_event.region_execution_form = "fused";
	compile_event.policy_decision = "force";
	compile_event.reason = "compile-shape";
	compile_event.decision_time_us = 3;
	compile_event.compile_time_us = 11;
	compile_event.code_size = 17;
	auto kernel_id = log.Record(1, true, std::move(compile_event));
	REQUIRE(kernel_id > 0);
	REQUIRE(log.GetKernelCounters().size() == 1);
	auto aggregate_counters = log.GetCounters();
	REQUIRE(aggregate_counters.size() == 1);
	REQUIRE(aggregate_counters[0].decision_time_us == 3);
	REQUIRE(aggregate_counters[0].compile_time_us == 11);

	log.ClearEvents();
	REQUIRE(log.GetEvents().empty());
	REQUIRE(log.GetKernelCounters().empty());

	JitEvent runtime_event;
	runtime_event.phase = "runtime";
	runtime_event.backend_name = "unit";
	runtime_event.target = "region";
	runtime_event.status = "executed";
	runtime_event.execution_mode = "native";
	runtime_event.region_execution_form = "fused";
	runtime_event.policy_decision = "runtime";
	runtime_event.reason = "region kernel executed";
	runtime_event.kernel_id = kernel_id;
	runtime_event.kernel_compile_reason = "compile-shape";
	runtime_event.kernel_compile_time_us = 11;
	runtime_event.kernel_code_size = 17;
	runtime_event.has_candidate = true;
	runtime_event.candidate_id = 2;
	runtime_event.candidate_shape = "filter-projection";
	runtime_event.candidate_scope = "post_source_operator_interval";
	runtime_event.candidate_pipeline_shape = "pipeline;op0:filter:FILTER:none;op1:projection:PROJECTION:none";
	runtime_event.candidate_node_count = 4;
	runtime_event.candidate_start_operator_index = 0;
	runtime_event.candidate_end_operator_index = 2;
	runtime_event.candidate_estimated_cardinality = 64;
	runtime_event.input_rows = 8;
	runtime_event.output_rows = 4;
	runtime_event.invocation_count = 1;
	runtime_event.runtime_time_us = 2;
	runtime_event.runtime_result = "need_more_input";
	runtime_event.source_native_output_rows = 3;
	runtime_event.source_native_invocation_count = 1;
	runtime_event.source_native_runtime_time_us = 1;
	runtime_event.generated_body_runtime_time_us = 0;
	log.Record(1, true, std::move(runtime_event));

	auto counters = log.GetKernelCounters();
	REQUIRE(counters.size() == 1);
	REQUIRE(counters[0].kernel_id == kernel_id);
	REQUIRE(counters[0].region_execution_form == "fused");
	REQUIRE(counters[0].compile_reason == "compile-shape");
	REQUIRE(counters[0].compile_time_us == 11);
	REQUIRE(counters[0].code_size == 17);
	REQUIRE(counters[0].has_candidate);
	REQUIRE(counters[0].candidate_id == 2);
	REQUIRE(counters[0].candidate_shape == "filter-projection");
	REQUIRE(counters[0].candidate_scope == "post_source_operator_interval");
	REQUIRE(counters[0].candidate_pipeline_shape == "pipeline;op0:filter:FILTER:none;op1:projection:PROJECTION:none");
	REQUIRE(counters[0].candidate_node_count == 4);
	REQUIRE(counters[0].candidate_start_operator_index == 0);
	REQUIRE(counters[0].candidate_end_operator_index == 2);
	REQUIRE(counters[0].candidate_estimated_cardinality == 64);
	REQUIRE(counters[0].input_rows == 8);
	REQUIRE(counters[0].output_rows == 4);
	REQUIRE(counters[0].invocation_count == 1);
	REQUIRE(counters[0].runtime_time_us == 2);
	REQUIRE(counters[0].source_native_output_rows == 3);
	REQUIRE(counters[0].source_native_invocation_count == 1);
	REQUIRE(counters[0].source_native_runtime_time_us == 1);
	REQUIRE(counters[0].generated_body_runtime_time_us == 0);
	REQUIRE(counters[0].declined_invocation_count == 0);
	REQUIRE(counters[0].declined_runtime_time_us == 0);
	REQUIRE(counters[0].fallback_input_rows == 0);
	REQUIRE(counters[0].fallback_output_rows == 0);
	REQUIRE(counters[0].fallback_invocation_count == 0);
	REQUIRE(counters[0].fallback_runtime_time_us == 0);
	REQUIRE(counters[0].last_runtime_result == "need_more_input");
}

TEST_CASE("JIT runtime trace separates declined kernels from executor fallback work", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<DecliningRegionBackend>());
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='contract_test_declining_region_jit_backend'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_declining_region_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM jit_declining_region_input WHERE i > 0"));

	vector<idx_t> declined_kernel_ids;
	for (auto &event : manager.GetEvents()) {
		if (event.phase != "runtime" || event.target != "region" ||
		    event.backend_name != "contract_test_declining_region_jit_backend") {
			continue;
		}
		REQUIRE(event.kernel_id > 0);
		if (event.status == "declined") {
			declined_kernel_ids.push_back(event.kernel_id);
			REQUIRE(event.has_candidate);
			REQUIRE(event.candidate_scope == "source_pipeline");
			REQUIRE(event.candidate_end_operator_index > event.candidate_start_operator_index);
			REQUIRE(event.execution_mode == "native");
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows == event.input_rows);
		}
	}
	REQUIRE(!declined_kernel_ids.empty());

	optional_idx traced_kernel_id;
	traced_kernel_id = declined_kernel_ids[0];
	REQUIRE(traced_kernel_id.IsValid());

	bool found_split_counter = false;
	for (auto &counter : manager.GetKernelCounters()) {
		if (counter.kernel_id != traced_kernel_id.GetIndex()) {
			continue;
		}
		found_split_counter = true;
		REQUIRE(counter.execution_mode == "native");
		REQUIRE(counter.has_candidate);
		REQUIRE(counter.candidate_scope == "source_pipeline");
		REQUIRE(!counter.candidate_pipeline_shape.empty());
		REQUIRE(counter.candidate_end_operator_index > counter.candidate_start_operator_index);
		REQUIRE(counter.input_rows == 0);
		REQUIRE(counter.output_rows == 0);
		REQUIRE(counter.invocation_count == 0);
		REQUIRE(counter.runtime_time_us == 0);
		REQUIRE(counter.declined_invocation_count > 0);
		REQUIRE(counter.fallback_input_rows == 0);
		REQUIRE(counter.fallback_invocation_count == 0);
		const bool expected_last_status = counter.last_runtime_status == "declined" ||
		                                  counter.last_runtime_status == "source_native";
		REQUIRE(expected_last_status);
		REQUIRE(counter.last_runtime_result != "fallback");
	}
	REQUIRE(found_split_counter);
}

TEST_CASE("JIT maximal region planner skips sink-only decline paths", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	manager.RegisterBackend(make_uniq<DecliningSinkRegionBackend>());
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='contract_test_declining_sink_region_jit_backend'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT sum(i) FROM range(16) tbl(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {120}));

	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || event.backend_name != "contract_test_declining_sink_region_jit_backend") {
			continue;
		}
		REQUIRE_FALSE((event.has_candidate && event.candidate_scope == "sink_pipeline"));
		REQUIRE_FALSE(event.phase == "runtime");
	}
	for (auto &counter : manager.GetKernelCounters()) {
		REQUIRE_FALSE((counter.has_candidate && counter.candidate_scope == "sink_pipeline"));
	}
}

TEST_CASE("JIT dump IR and execution mode expose backend honesty", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_dump_ir=true"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM range(3) tbl(i) WHERE i > 0"));

	bool found_ir = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(event.target != "expression");
		if (event.status == "compiled") {
			REQUIRE(event.execution_mode != "executor_fallback");
		}
		if (event.backend_name == "sljit" && event.target == "region" && !event.ir.empty() &&
		    StringUtil::Contains(event.ir, "duckdb.region typed-vector-ir") && StringUtil::Contains(event.ir, ".add")) {
			found_ir = true;
			REQUIRE(ContainsTypedIrNode(event.ir, "binary", "BIGINT", "INT64"));
		}
	}
	REQUIRE(found_ir);

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT b, a FROM (VALUES (1::BIGINT, 10::BIGINT), "
	                          "(2::BIGINT, 20::BIGINT)) t(a, b)"));

	bool found_reference_projection = false;
	for (auto &event : manager.GetEvents()) {
		if (event.backend_name != "sljit" || event.target != "region") {
			continue;
		}
		if (StringUtil::Contains(event.reason, "op0:PROJECTION:native:native typed reference projection")) {
			found_reference_projection = true;
			REQUIRE(event.code_size == 0);
			REQUIRE(event.status == "unsupported");
			REQUIRE(event.execution_mode == "unsupported");
			REQUIRE(event.region_execution_form == "none");
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "pass-through"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "kernel=generic-runtime-loop"));
			REQUIRE_FALSE(StringUtil::Contains(event.reason, "execution:native-sljit-region-"));
		}
	}
	REQUIRE(found_reference_projection);
}

TEST_CASE("JIT runtime trace records kernel execution facts", "[api][jit]") {
	bool found_region_runtime = false;
	bool found_runtime_counter = false;
	{
		DuckDB db;
		Connection con(db);
		auto &context = *con.context;
		auto &manager = JitManager::Get(context);

		REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
		REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
		REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_trace_input AS "
		                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

		manager.ClearEvents();
		REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM "
		                          "(SELECT i + 1 AS j FROM jit_runtime_trace_input WHERE i > 0)"));
		for (auto &event : manager.GetEvents()) {
			REQUIRE(event.target != "expression");
			if (event.phase != "runtime" || event.status != "executed" || event.target != "region" ||
			    event.output_rows == 0) {
				continue;
			}
			found_region_runtime = true;
			REQUIRE(event.kernel_id > 0);
			REQUIRE(event.invocation_count == 1);
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows > 0);
			REQUIRE(event.runtime_time_us >= 0);
			REQUIRE(event.has_candidate);
			REQUIRE(IsKnownJitCandidateScope(event.candidate_scope));
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
			REQUIRE(event.generated_body_runtime_time_us >= 0);
			REQUIRE(event.generated_body_runtime_time_us + event.source_native_runtime_time_us <=
			        event.runtime_time_us);
			const bool expected_runtime_result = event.runtime_result == "need_more_input" ||
			                                     event.runtime_result == "have_more_output" ||
			                                     event.runtime_result == "finished";
			REQUIRE(expected_runtime_result);
			REQUIRE(StringUtil::Contains(event.reason, "kernel executed"));
		}
		for (auto &counter : manager.GetCounters()) {
			if (counter.status == "executed" && counter.policy_decision == "runtime" && counter.invocation_count > 0 &&
			    counter.input_rows > 0 && counter.execution_mode == "native") {
				found_runtime_counter = true;
			}
		}
	}
	REQUIRE(found_region_runtime);
	REQUIRE(found_runtime_counter);
}

TEST_CASE("JIT runtime trace separates compiled coverage from executed kernels", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_runtime_coverage_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT sum(j) FROM (SELECT i + 1 AS j FROM jit_runtime_coverage_input WHERE i > 0)"));

	idx_t compiled_region_kernel_id = 0;
	bool found_region_runtime = false;
	for (auto &event : manager.GetEvents()) {
		REQUIRE(event.target != "expression");
		if (event.phase == "compile" && event.backend_name == "sljit" && event.target == "region" &&
		    event.status == "compiled" && event.execution_mode == "native" && event.region_execution_form == "fused" &&
		    compiled_region_kernel_id == 0) {
			compiled_region_kernel_id = event.kernel_id;
		}
		if (event.phase == "runtime" && event.target == "region" && event.kernel_id == compiled_region_kernel_id &&
		    event.status == "executed") {
			found_region_runtime = true;
			REQUIRE(event.has_candidate);
			REQUIRE(IsKnownJitCandidateScope(event.candidate_scope));
			REQUIRE(event.candidate_end_operator_index >= event.candidate_start_operator_index);
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.output_rows >= 0);
		}
	}
	REQUIRE(compiled_region_kernel_id > 0);
	REQUIRE(found_region_runtime);
}

TEST_CASE("JIT introspection does not record JIT events", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_decisions=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_introspection_input AS "
	                          "SELECT i::BIGINT AS i FROM range(16) tbl(i)"));

	manager.ClearEvents();
	REQUIRE_NO_FAIL(con.Query("SELECT i + 1 FROM jit_introspection_input WHERE i > 0"));
	auto event_count = manager.GetEvents().size();
	auto counter_count = TotalJitCounterCount(manager.GetCounters());
	auto decision_counter_count = TotalJitDecisionCounterCount(manager.GetDecisionCounters());
	auto kernel_counter_count = manager.GetKernelCounters().size();
	REQUIRE(event_count > 0);
	REQUIRE(counter_count > 0);
	REQUIRE(decision_counter_count > 0);
	REQUIRE(kernel_counter_count > 0);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_events()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_events()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_decision_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_decision_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_kernel_counters()"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(candidate_contract_abi) FROM duckdb_jit_kernel_counters()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM duckdb_jit_backends()"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);

	auto copy_path = TestCreatePath("jit_decision_counters_copy.csv");
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT * FROM duckdb_jit_decision_counters()) TO " + SQLString(copy_path) +
	                          " (HEADER, DELIMITER ',')"));
	REQUIRE(manager.GetEvents().size() == event_count);
	REQUIRE(TotalJitCounterCount(manager.GetCounters()) == counter_count);
	REQUIRE(TotalJitDecisionCounterCount(manager.GetDecisionCounters()) == decision_counter_count);
	REQUIRE(manager.GetKernelCounters().size() == kernel_counter_count);
}

TEST_CASE("JIT introspection does not suppress later statements in one SQL batch", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &manager = JitManager::Get(context);

	REQUIRE_NO_FAIL(con.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_batch_source AS "
	                          "SELECT i::BIGINT AS i, i::BIGINT AS j FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET enable_jit=true"));
	REQUIRE_NO_FAIL(con.Query("SET jit_backend='sljit'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='force'"));
	REQUIRE_NO_FAIL(con.Query("SET jit_trace_runtime=true"));

	manager.ClearEvents();
	auto result = con.Query("SELECT * FROM duckdb_jit_clear_events();"
	                        "SELECT * FROM duckdb_jit_clear_counters();"
	                        "SELECT sum(i) FROM jit_batch_source WHERE j > 2500");
	REQUIRE_NO_FAIL(*result);

	bool found_compiled_fused_region = false;
	bool found_runtime_fused_region = false;
	for (auto &event : manager.GetEvents()) {
		if (event.target != "region" || !event.has_candidate) {
			continue;
		}
		if (event.status == "compiled" && event.candidate_scope == "full_pipeline" &&
		    event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
		    StringUtil::Contains(event.reason, "generated source-prefix table scan filters") &&
		    StringUtil::Contains(event.reason,
		                         "execution:native-sljit-region-filter-projection-ungrouped-aggregate-update")) {
			found_compiled_fused_region = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(StringUtil::Contains(event.reason, "owns-source-filters=true"));
		}
		if (event.phase == "runtime" && event.status == "executed" && event.candidate_scope == "full_pipeline" &&
		    event.selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
		    StringUtil::Contains(event.reason, "full pipeline kernel executed")) {
			found_runtime_fused_region = true;
			REQUIRE(event.region_execution_form == "fused");
			REQUIRE(event.input_rows > 0);
			REQUIRE(event.source_native_output_rows == event.input_rows);
		}
	}
	REQUIRE(found_compiled_fused_region);
	REQUIRE(found_runtime_fused_region);
}

TEST_CASE("JIT event IDs are unique under concurrent compilation", "[api][jit]") {
	DuckDB db;
	Connection setup(db);
	auto &manager = JitManager::Get(*setup.context);
	REQUIRE_NO_FAIL(setup.Query("LOAD jit_sljit"));
	REQUIRE_NO_FAIL(setup.Query("SET jit_event_log_size=10000"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE jit_concurrent_event_input AS "
	                            "SELECT i::BIGINT AS i FROM range(64) tbl(i)"));
	manager.ClearEvents();

	vector<std::thread> workers;
	std::atomic<bool> failed(false);
	for (idx_t worker_idx = 0; worker_idx < 8; worker_idx++) {
		workers.emplace_back([&db, &failed, worker_idx]() {
			Connection con(db);
			if (con.Query("SET jit_backend='sljit'")->HasError()) {
				failed = true;
				return;
			}
			if (con.Query("SET jit_policy='force'")->HasError()) {
				failed = true;
				return;
			}
			for (idx_t query_idx = 0; query_idx < 8; query_idx++) {
				if (con.Query("SELECT i + " + std::to_string(worker_idx + query_idx) +
				              " FROM jit_concurrent_event_input WHERE i > 0")
				        ->HasError()) {
					failed = true;
					return;
				}
			}
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	REQUIRE(!failed.load());

	auto events = manager.GetEvents();
	REQUIRE(!events.empty());
	vector<idx_t> event_ids;
	event_ids.reserve(events.size());
	for (auto &event : events) {
		event_ids.push_back(event.event_id);
	}
	std::sort(event_ids.begin(), event_ids.end());
	REQUIRE(std::adjacent_find(event_ids.begin(), event_ids.end()) == event_ids.end());
}
