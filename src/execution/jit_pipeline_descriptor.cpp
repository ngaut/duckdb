//===----------------------------------------------------------------------===//
//                         DuckDB
//
// jit_pipeline_descriptor.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/jit/pipeline_descriptor.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

static bool JitPipelineCompiledStageIsSourceRole(const JitCompiledStageContract &stage) {
	return stage.stage == JitRegionStageKind::SOURCE;
}

static bool JitPipelineCompiledStageIsOperatorRole(const JitCompiledStageContract &stage) {
	return stage.stage == JitRegionStageKind::HASH_JOIN_PROBE ||
	       stage.stage == JitRegionStageKind::OPERATOR_BOUNDARY;
}

static bool JitPipelineCompiledStageIsSinkRole(const JitCompiledStageContract &stage) {
	return stage.stage == JitRegionStageKind::HASH_JOIN_BUILD ||
	       stage.stage == JitRegionStageKind::HASH_AGGREGATE_UPDATE ||
	       stage.stage == JitRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	       stage.stage == JitRegionStageKind::UNGROUPED_AGGREGATE_UPDATE ||
	       stage.stage == JitRegionStageKind::SINK_BOUNDARY;
}

static bool JitPipelineCompiledStageIsBoundaryFree(const JitCompiledStageContract &stage) {
	return stage.execution == JitRegionStageExecutionKind::NATIVE_PROTOCOL ||
	       stage.execution == JitRegionStageExecutionKind::GENERATED_IR ||
	       stage.execution == JitRegionStageExecutionKind::PASS_THROUGH;
}

static bool JitPipelineKeepCompiledStageForRole(const JitCompiledStageContract &stage,
                                                JitPipelineOperatorRole role) {
	switch (role) {
	case JitPipelineOperatorRole::SOURCE:
		return JitPipelineCompiledStageIsSourceRole(stage);
	case JitPipelineOperatorRole::OPERATOR:
		return JitPipelineCompiledStageIsOperatorRole(stage);
	case JitPipelineOperatorRole::SINK:
		return JitPipelineCompiledStageIsSinkRole(stage);
	default:
		return false;
	}
}

static string DescribeJitPipelineCompiledContractSlice(const JitCompiledOperatorContract &contract,
                                                       const string &role) {
	string result = "compiled_contract<role=" + role;
	result += ",stages=" + std::to_string(contract.stages.size());
	result += ",source=" + string(contract.has_source ? "true" : "false");
	result += ",operator=" + string(contract.has_operator ? "true" : "false");
	result += ",sink=" + string(contract.has_sink ? "true" : "false");
	result += ",state_scan=" + string(contract.has_state_scan ? "true" : "false");
	result += ",resumable_output=" + string(contract.has_resumable_output ? "true" : "false");
	result += ",executor_boundary_free=" + string(contract.executor_boundary_free ? "true" : "false");
	result += ">";
	return result;
}

static string JitPipelineOperatorRoleName(JitPipelineOperatorRole role) {
	switch (role) {
	case JitPipelineOperatorRole::SOURCE:
		return "source";
	case JitPipelineOperatorRole::OPERATOR:
		return "operator";
	case JitPipelineOperatorRole::SINK:
		return "sink";
	default:
		return "none";
	}
}

static JitCompiledOperatorContract SliceJitPipelineCompiledContract(const JitCompiledOperatorContract &contract,
                                                                    JitPipelineOperatorRole role) {
	JitCompiledOperatorContract result;
	result.present = false;
	result.executor_boundary_free = true;
	for (auto &stage : contract.stages) {
		if (!JitPipelineKeepCompiledStageForRole(stage, role)) {
			continue;
		}
		result.present = true;
		result.has_source = result.has_source || JitPipelineCompiledStageIsSourceRole(stage);
		result.has_operator = result.has_operator || JitPipelineCompiledStageIsOperatorRole(stage);
		result.has_sink = result.has_sink || JitPipelineCompiledStageIsSinkRole(stage);
		result.has_state_scan =
		    result.has_state_scan || stage.protocol == JitCompiledProtocolKind::STATE_SCAN_CURSOR;
		result.has_resumable_output =
		    result.has_resumable_output || stage.drain == JitCompiledDrainKind::ZERO_OR_MANY_OUTPUT;
		result.executor_boundary_free = result.executor_boundary_free && JitPipelineCompiledStageIsBoundaryFree(stage);
		result.stages.push_back(stage);
	}
	if (!result.present) {
		result.executor_boundary_free = false;
	}
	result.ir = DescribeJitPipelineCompiledContractSlice(result, JitPipelineOperatorRoleName(role));
	return result;
}

static bool JitPipelineCompiledContractHasNativeProtocol(const JitCompiledOperatorContract &contract,
                                                         JitCompiledProtocolKind protocol) {
	for (auto &stage : contract.stages) {
		if (stage.protocol == protocol && stage.execution == JitRegionStageExecutionKind::NATIVE_PROTOCOL) {
			return true;
		}
	}
	return false;
}

static bool JitPipelineCompiledContractHasNativeSource(const JitCompiledOperatorContract &contract) {
	return JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::SCAN_CURSOR) ||
	       JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::STATE_SCAN_CURSOR);
}

static bool JitPipelineCompiledContractHasNativeOperator(const JitCompiledOperatorContract &contract) {
	return JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::HASH_JOIN_PROBE_CURSOR) ||
	       JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::AGGREGATE_LOOKUP);
}

static bool JitPipelineCompiledContractHasNativeSink(const JitCompiledOperatorContract &contract) {
	return JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::HASH_JOIN_BUILD) ||
	       JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::AGGREGATE_UPDATE) ||
	       JitPipelineCompiledContractHasNativeProtocol(contract, JitCompiledProtocolKind::SINK_CURSOR);
}

static JitPipelineOperatorEntry BuildJitPipelineOperatorEntry(const PhysicalOperator &op,
                                                              JitPipelineOperatorRole role,
                                                              idx_t operator_index = DConstants::INVALID_INDEX) {
	JitPipelineOperatorEntry entry;
	entry.role = role;
	entry.operator_index = operator_index;
	entry.physical = op;
	entry.type = op.type;
	entry.operator_name = PhysicalOperatorToString(op.type);
	entry.output_types = op.GetTypes();
	entry.estimated_cardinality = op.estimated_cardinality;
	entry.descriptor = op.GetJitOperatorDescriptor();
	entry.source_contract = SliceJitPipelineCompiledContract(entry.descriptor.compiled_contract,
	                                                         JitPipelineOperatorRole::SOURCE);
	entry.operator_contract = SliceJitPipelineCompiledContract(entry.descriptor.compiled_contract,
	                                                           JitPipelineOperatorRole::OPERATOR);
	entry.sink_contract = SliceJitPipelineCompiledContract(entry.descriptor.compiled_contract,
	                                                       JitPipelineOperatorRole::SINK);
	entry.native_source = JitPipelineCompiledContractHasNativeSource(entry.source_contract);
	entry.native_operator = JitPipelineCompiledContractHasNativeOperator(entry.operator_contract);
	entry.native_sink = JitPipelineCompiledContractHasNativeSink(entry.sink_contract);
	return entry;
}

unique_ptr<JitPipelineDescriptor> BuildJitPipelineDescriptor(Pipeline &pipeline) {
	auto result = make_uniq<JitPipelineDescriptor>();
	if (pipeline.GetSource()) {
		result->source = BuildJitPipelineOperatorEntry(*pipeline.GetSource(), JitPipelineOperatorRole::SOURCE);
	}
	auto &operators = pipeline.GetIntermediateOperators();
	for (idx_t op_idx = 0; op_idx < operators.size(); op_idx++) {
		result->operators.push_back(
		    BuildJitPipelineOperatorEntry(operators[op_idx].get(), JitPipelineOperatorRole::OPERATOR, op_idx));
	}
	if (pipeline.GetSink()) {
		result->sink = BuildJitPipelineOperatorEntry(*pipeline.GetSink(), JitPipelineOperatorRole::SINK);
	}
	if (result->Empty()) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
