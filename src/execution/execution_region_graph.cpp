//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_graph.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/execution_region_graph.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

static bool ExecutionRegionCompiledStageIsSourceSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::SOURCE;
}

static bool ExecutionRegionCompiledStageIsOperatorSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
	       stage.stage == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
	       stage.stage == ExecutionRegionStageKind::OPERATOR_BOUNDARY;
}

static bool ExecutionRegionCompiledStageIsSinkSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::HASH_JOIN_BUILD ||
	       stage.stage == ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD ||
	       stage.stage == ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::HASH_AGGREGATE_DISTINCT_SINK ||
	       stage.stage == ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::APPEND_SINK || stage.stage == ExecutionRegionStageKind::SORT_SINK ||
	       stage.stage == ExecutionRegionStageKind::DELIM_JOIN_SINK ||
	       stage.stage == ExecutionRegionStageKind::SINK_BOUNDARY;
}

static bool ExecutionRegionKeepCompiledStageForSlot(const ExecutionCompiledStageContract &stage,
                                                    ExecutionRegionOperatorSlot slot) {
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		return ExecutionRegionCompiledStageIsSourceSlot(stage);
	case ExecutionRegionOperatorSlot::OPERATOR:
		return ExecutionRegionCompiledStageIsOperatorSlot(stage);
	case ExecutionRegionOperatorSlot::SINK:
		return ExecutionRegionCompiledStageIsSinkSlot(stage);
	default:
		return false;
	}
}

static string DescribeExecutionRegionCompiledContractSlice(const ExecutionCompiledOperatorContract &contract,
                                                           const string &slot) {
	string result = "compiled_contract<slot=" + slot;
	result += ",stages=" + std::to_string(contract.stages.size());
	result += ",source=" + string(contract.HasSource() ? "true" : "false");
	result += ",operator=" + string(contract.HasOperator() ? "true" : "false");
	result += ",sink=" + string(contract.HasSink() ? "true" : "false");
	result += ",state_scan=" + string(contract.HasStateScan() ? "true" : "false");
	result += ",zero_or_many_output=" + string(contract.HasZeroOrManyOutput() ? "true" : "false");
	result += ">";
	return result;
}

static string ExecutionRegionOperatorSlotName(ExecutionRegionOperatorSlot slot) {
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		return "source";
	case ExecutionRegionOperatorSlot::OPERATOR:
		return "operator";
	case ExecutionRegionOperatorSlot::SINK:
		return "sink";
	default:
		return "none";
	}
}

static ExecutionCompiledOperatorContract
SliceExecutionRegionCompiledContract(const ExecutionCompiledOperatorContract &contract,
                                     ExecutionRegionOperatorSlot slot) {
	ExecutionCompiledOperatorContract result;
	for (auto &stage : contract.stages) {
		if (!ExecutionRegionKeepCompiledStageForSlot(stage, slot)) {
			continue;
		}
		result.stages.push_back(stage);
	}
	result.ir = DescribeExecutionRegionCompiledContractSlice(result, ExecutionRegionOperatorSlotName(slot));
	return result;
}

static void SetExecutionRegionOperatorExpressions(ExecutionRegionOperatorEntry &entry,
                                                  const ExecutionTransformContract &transform) {
	entry.filter_expression = transform.filter_expression;
	entry.projection_expressions = transform.projection_expressions;
}

static ExecutionRegionOperatorEntry
BuildExecutionRegionOperatorEntry(const PhysicalOperator &op, ExecutionRegionOperatorSlot slot,
                                  idx_t operator_index = DConstants::INVALID_INDEX) {
	ExecutionRegionOperatorEntry entry;
	entry.present = true;
	entry.slot = slot;
	entry.operator_index = operator_index;
	entry.output_types = op.GetTypes();
	entry.estimated_cardinality = op.estimated_cardinality;
	entry.operator_kind = op.GetExecutionRegionOperatorKind();
	entry.operator_name = entry.operator_kind == ExecutionRegionOperatorKind::GENERIC ||
	                              entry.operator_kind == ExecutionRegionOperatorKind::SCAN_SOURCE
	                          ? op.GetName()
	                          : ExecutionRegionOperatorKindToTraceLabel(entry.operator_kind);
	auto descriptor = op.GetExecutionContract();
	SetExecutionRegionOperatorExpressions(entry, descriptor.transform);
	entry.source_contract =
	    SliceExecutionRegionCompiledContract(descriptor.compiled_contract, ExecutionRegionOperatorSlot::SOURCE);
	entry.operator_contract =
	    SliceExecutionRegionCompiledContract(descriptor.compiled_contract, ExecutionRegionOperatorSlot::OPERATOR);
	entry.sink_contract =
	    SliceExecutionRegionCompiledContract(descriptor.compiled_contract, ExecutionRegionOperatorSlot::SINK);
	entry.source_boundary_reason = std::move(descriptor.source_boundary_reason);
	entry.source_payload = std::move(descriptor.source);
	entry.operator_payload = std::move(descriptor.operator_info);
	entry.sink_payload = std::move(descriptor.sink);
	return entry;
}

unique_ptr<ExecutionRegionGraph> BuildExecutionRegionGraph(Pipeline &pipeline) {
	auto result = make_uniq<ExecutionRegionGraph>();
	if (pipeline.GetSource()) {
		result->source = BuildExecutionRegionOperatorEntry(*pipeline.GetSource(), ExecutionRegionOperatorSlot::SOURCE);
	}
	auto &operators = pipeline.GetIntermediateOperators();
	for (idx_t op_idx = 0; op_idx < operators.size(); op_idx++) {
		result->operators.push_back(
		    BuildExecutionRegionOperatorEntry(operators[op_idx].get(), ExecutionRegionOperatorSlot::OPERATOR, op_idx));
	}
	if (pipeline.GetSink()) {
		result->sink = BuildExecutionRegionOperatorEntry(*pipeline.GetSink(), ExecutionRegionOperatorSlot::SINK);
	}
	if (result->Empty()) {
		return nullptr;
	}
	return result;
}

static string DescribeExecutionRegionGraphEntry(const char *label, const ExecutionRegionOperatorEntry &entry) {
	string result = string(label) + ":";
	result += ExecutionRegionOperatorKindToTraceLabel(entry.operator_kind);
	result += ":name=" + entry.operator_name;
	result += ":estimated_cardinality=" + std::to_string(entry.estimated_cardinality);
	switch (entry.slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		result += ":source_contract=" + string(entry.HasSourceContract() ? "present" : "missing");
		result += ":source_native=" + string(entry.UsesSourceContract() ? "true" : "false");
		break;
	case ExecutionRegionOperatorSlot::OPERATOR:
		result += ":operator_contract=" + string(entry.HasOperatorContract() ? "present" : "missing");
		result += ":operator_native=" + string(entry.HasNativeOperator() ? "true" : "false");
		break;
	case ExecutionRegionOperatorSlot::SINK:
		result += ":sink_contract=" + string(entry.HasSinkContract() ? "present" : "missing");
		result += ":sink_native=" + string(entry.HasNativeSink() ? "true" : "false");
		break;
	default:
		break;
	}
	return result;
}

string DescribeExecutionRegionGraphShape(const ExecutionRegionGraph &graph) {
	string result = "graph";
	if (graph.HasSource()) {
		result += ";";
		result += DescribeExecutionRegionGraphEntry("source", graph.source);
	}
	for (idx_t op_idx = 0; op_idx < graph.operators.size(); op_idx++) {
		result += ";op" + std::to_string(op_idx) + ":";
		result += DescribeExecutionRegionGraphEntry("operator", graph.operators[op_idx]);
	}
	if (graph.HasSink()) {
		result += ";";
		result += DescribeExecutionRegionGraphEntry("sink", graph.sink);
	}
	return result;
}

} // namespace duckdb
