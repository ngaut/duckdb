//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/pipeline_descriptor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/jit/operator_descriptor.hpp"

namespace duckdb {

class Pipeline;
class PhysicalOperator;

enum class JitPipelineOperatorRole : uint8_t { SOURCE, OPERATOR, SINK };

struct JitPipelineOperatorEntry {
	JitPipelineOperatorRole role = JitPipelineOperatorRole::OPERATOR;
	idx_t operator_index = DConstants::INVALID_INDEX;
	optional_ptr<const PhysicalOperator> physical;
	PhysicalOperatorType type = PhysicalOperatorType::INVALID;
	string operator_name;
	vector<LogicalType> output_types;
	idx_t estimated_cardinality = 0;
	JitOperatorDescriptor descriptor;
	JitCompiledOperatorContract source_contract;
	JitCompiledOperatorContract operator_contract;
	JitCompiledOperatorContract sink_contract;
	bool native_source = false;
	bool native_operator = false;
	bool native_sink = false;

	const PhysicalOperator &Physical() const {
		D_ASSERT(physical);
		return *physical;
	}
	bool HasSourceContract() const {
		return source_contract.present;
	}
	bool HasOperatorContract() const {
		return operator_contract.present;
	}
	bool HasSinkContract() const {
		return sink_contract.present;
	}
};

struct JitPipelineDescriptor {
	JitPipelineOperatorEntry source;
	vector<JitPipelineOperatorEntry> operators;
	JitPipelineOperatorEntry sink;

	bool HasSource() const {
		return source.physical;
	}
	bool HasSink() const {
		return sink.physical;
	}
	bool Empty() const {
		return !HasSource() && operators.empty() && !HasSink();
	}
	idx_t OperatorCount() const {
		return operators.size();
	}
};

DUCKDB_API unique_ptr<JitPipelineDescriptor> BuildJitPipelineDescriptor(Pipeline &pipeline);

} // namespace duckdb
