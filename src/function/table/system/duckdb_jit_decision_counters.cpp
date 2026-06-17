#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitDecisionCountersData : public GlobalTableFunctionState {
	vector<ExecutionRegionDecisionCounter> counters;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitDecisionCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("backend_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("target");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("phase");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("region_execution_form");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_body");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("policy_decision");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("admission_shape_key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("admission_rule_present");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("admission_min_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("admission_proof");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("has_admission_score");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("min_admission_score");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("max_admission_score");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("max_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("decision_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("compile_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("code_size");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("ir_lowering_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("backend_analysis_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("admission_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("overlap_check_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("codegen_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	AddExecutionRegionCandidateTraceColumns(return_types, names);
	names.emplace_back("pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("pipeline_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("example_reason");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitDecisionCountersInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitDecisionCountersData>();
	result->counters = ExecutionRegionManager::Get(context).GetDecisionCounters();
	return std::move(result);
}

static void DuckDBJitDecisionCountersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitDecisionCountersData>();
	idx_t count = 0;
	while (data.offset < data.counters.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.counters[data.offset++];
		idx_t col = 0;
		output.data[col++].Append(Value(entry.backend_name));
		output.data[col++].Append(Value(entry.target));
		output.data[col++].Append(Value(entry.phase));
		output.data[col++].Append(Value(entry.status));
		output.data[col++].Append(Value(entry.execution_mode));
		output.data[col++].Append(Value(entry.region_execution_form));
		output.data[col++].Append(Value(entry.execution_body));
		output.data[col++].Append(Value(entry.policy_decision));
		output.data[col++].Append(Value(entry.candidate_shape));
		output.data[col++].Append(Value(entry.admission_shape_key));
		output.data[col++].Append(Value::BOOLEAN(entry.admission_rule_present));
		output.data[col++].Append(Value::UBIGINT(entry.admission_min_cardinality));
		output.data[col++].Append(Value(entry.admission_proof));
		output.data[col++].Append(Value::BOOLEAN(entry.has_admission_score));
		output.data[col++].Append(Value::BIGINT(entry.min_admission_score));
		output.data[col++].Append(Value::BIGINT(entry.max_admission_score));
		output.data[col++].Append(Value::UBIGINT(entry.count));
		output.data[col++].Append(Value::UBIGINT(entry.max_estimated_cardinality));
		output.data[col++].Append(Value::BIGINT(entry.decision_time_us));
		output.data[col++].Append(Value::BIGINT(entry.compile_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.code_size));
		output.data[col++].Append(Value::BIGINT(entry.ir_lowering_time_us));
		output.data[col++].Append(Value::BIGINT(entry.backend_analysis_time_us));
		output.data[col++].Append(Value::BIGINT(entry.admission_time_us));
		output.data[col++].Append(Value::BIGINT(entry.overlap_check_time_us));
		output.data[col++].Append(Value::BIGINT(entry.codegen_time_us));
		col = AppendExecutionRegionCandidateTraceColumns(output, col, entry.candidate_signature, entry.candidate_traits,
		                                                 entry.candidate_contract);
		if (entry.has_pipeline && !entry.pipeline_shape.empty()) {
			output.data[col++].Append(Value(entry.pipeline_shape));
		} else {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		}
		if (entry.has_pipeline) {
			output.data[col++].Append(Value::UBIGINT(entry.pipeline_estimated_cardinality));
		} else {
			output.data[col++].Append(Value(LogicalType::UBIGINT));
		}
		if (entry.example_reason.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.example_reason));
		}
		count++;
	}
}

void DuckDBJitDecisionCountersFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_decision_counters", {}, DuckDBJitDecisionCountersFunction,
	                              DuckDBJitDecisionCountersBind, DuckDBJitDecisionCountersInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
