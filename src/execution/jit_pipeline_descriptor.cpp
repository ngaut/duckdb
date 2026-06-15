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
