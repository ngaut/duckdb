#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_jit_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitKernelCountersData : public GlobalTableFunctionState {
	vector<JitKernelCounter> counters;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitKernelCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
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
	names.emplace_back("source_helper_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_helper_output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_helper_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_helper_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("source_native_output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_native_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_native_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_body_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fused_prepare_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fused_group_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fused_state_bind_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fused_update_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fused_finish_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_body_flat_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_flat_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_shared_selection_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_shared_selection_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_selection_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_selection_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_generic_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generated_body_generic_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("declined_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("declined_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("fallback_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("fallback_output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("fallback_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("fallback_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("candidate_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_context_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_scope");
	return_types.emplace_back(LogicalType::VARCHAR);
	AddJitCandidateTraceColumns(return_types, names);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitKernelCountersInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	JitSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitKernelCountersData>();
	result->counters = JitManager::Get(context).GetKernelCounters();
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
		output.data[col++].Append(Value::UBIGINT(entry.source_helper_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.source_helper_output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.source_helper_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.source_helper_runtime_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.source_native_output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.source_native_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.source_native_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.fused_prepare_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.fused_group_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.fused_state_bind_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.fused_update_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.fused_finish_runtime_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_flat_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_flat_invocation_count));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_shared_selection_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_shared_selection_invocation_count));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_selection_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_selection_invocation_count));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_generic_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.generated_body_generic_invocation_count));
		output.data[col++].Append(Value::UBIGINT(entry.declined_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.declined_runtime_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.fallback_input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.fallback_output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.fallback_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.fallback_runtime_time_us));
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
		if (entry.has_candidate && !entry.candidate_scope.empty()) {
			output.data[col++].Append(Value(entry.candidate_scope));
		} else {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		}
		if (entry.has_candidate) {
			AppendJitCandidateTraceColumns(output, col, entry.candidate_traits, entry.candidate_contract);
		} else {
			AppendNullJitCandidateTraceColumns(output, col);
		}
		count++;
	}
}

void DuckDBJitKernelCountersFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_jit_kernel_counters", {}, DuckDBJitKernelCountersFunction,
	                              DuckDBJitKernelCountersBind, DuckDBJitKernelCountersInit));
}

} // namespace duckdb
