#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitKernelCountersData : public GlobalTableFunctionState {
	vector<ExecutionRegionKernelCounter> counters;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitKernelCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("kernel_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("backend_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("target");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("region_execution_form");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_body");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_node_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_start_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_end_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("compile_reason");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("compile_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("code_size");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("last_runtime_status");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("last_runtime_result");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("source_contract_output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_contract_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_contract_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_body_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_stage_runtime_breakdown");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_context_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	AddExecutionRegionCandidateTraceColumns(return_types, names);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitKernelCountersInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitKernelCountersData>();
	result->counters = ExecutionRegionManager::Get(context).GetKernelCounters();
	return std::move(result);
}

static void DuckDBJitKernelCountersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitKernelCountersData>();
	idx_t count = 0;
	while (data.offset < data.counters.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.counters[data.offset++];
		idx_t col = 0;
		output.data[col++].Append(Value::UBIGINT(entry.kernel_id));
		output.data[col++].Append(Value(entry.backend_name));
		output.data[col++].Append(Value(entry.target));
		output.data[col++].Append(Value(entry.execution_mode));
		output.data[col++].Append(Value(entry.region_execution_form));
		output.data[col++].Append(Value(entry.execution_body));
		if (entry.has_candidate) {
			output.data[col++].Append(Value::UBIGINT(entry.candidate_id));
			output.data[col++].Append(Value(entry.candidate_shape));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_node_count));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_start_operator_index));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_end_operator_index));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_estimated_cardinality));
		} else {
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::VARCHAR));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
		}
		output.data[col++].Append(Value(entry.compile_reason));
		output.data[col++].Append(Value::BIGINT(entry.compile_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.code_size));
		if (entry.last_runtime_status.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.last_runtime_status));
		}
		if (entry.last_runtime_result.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.last_runtime_result));
		}
		output.data[col++].Append(Value::UBIGINT(entry.input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.runtime_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.source_contract_output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.source_contract_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		if (entry.generated_stage_runtime_breakdown.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.generated_stage_runtime_breakdown));
		}
		if (entry.has_candidate && !entry.candidate_pipeline_shape.empty()) {
			output.data[col++].Append(Value(entry.candidate_pipeline_shape));
		} else {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		}
		if (entry.has_candidate && !entry.candidate_context_pipeline_shape.empty()) {
			output.data[col++].Append(Value(entry.candidate_context_pipeline_shape));
		} else {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		}
		if (entry.has_candidate) {
			AppendExecutionRegionCandidateTraceColumns(output, col, entry.candidate_signature, entry.candidate_traits,
			                                           entry.candidate_contract);
		} else {
			AppendNullExecutionRegionCandidateTraceColumns(output, col);
		}
		count++;
	}
}

void DuckDBJitKernelCountersFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_kernel_counters", {}, DuckDBJitKernelCountersFunction,
	                              DuckDBJitKernelCountersBind, DuckDBJitKernelCountersInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
