#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitCountersData : public GlobalTableFunctionState {
	vector<JitCounter> counters;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("backend_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("target");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("region_execution_form");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("policy_decision");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("decision_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("compile_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("code_size");
	return_types.emplace_back(LogicalType::UBIGINT);
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
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitCountersInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	JitSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitCountersData>();
	result->counters = JitManager::Get(context).GetCounters();
	return std::move(result);
}

static void DuckDBJitCountersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitCountersData>();
	idx_t count = 0;
	while (data.offset < data.counters.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.counters[data.offset++];
		idx_t col = 0;
		output.data[col++].Append(Value(entry.backend_name));
		output.data[col++].Append(Value(entry.target));
		output.data[col++].Append(Value(entry.status));
		output.data[col++].Append(Value(entry.execution_mode));
		output.data[col++].Append(Value(entry.region_execution_form));
		output.data[col++].Append(Value(entry.policy_decision));
		output.data[col++].Append(Value::UBIGINT(entry.count));
		output.data[col++].Append(Value::BIGINT(entry.decision_time_us));
		output.data[col++].Append(Value::BIGINT(entry.compile_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.code_size));
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
		output.data[col++].Append(Value::BIGINT(entry.ir_lowering_time_us));
		output.data[col++].Append(Value::BIGINT(entry.backend_analysis_time_us));
		output.data[col++].Append(Value::BIGINT(entry.admission_time_us));
		output.data[col++].Append(Value::BIGINT(entry.overlap_check_time_us));
		output.data[col++].Append(Value::BIGINT(entry.codegen_time_us));
		count++;
	}
}

void DuckDBJitCountersFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_jit_counters", {}, DuckDBJitCountersFunction, DuckDBJitCountersBind,
	                              DuckDBJitCountersInit));
}

} // namespace duckdb
