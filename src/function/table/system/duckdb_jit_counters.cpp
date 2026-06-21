#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitCountersData : public ExecutionRegionTableFunctionState<ExecutionRegionCounter> {};

static void AppendJitCounterColumn(Vector &output, idx_t column_id, const ExecutionRegionCounter &entry) {
	switch (column_id) {
	case 0:
		output.Append(Value(entry.backend_name));
		return;
	case 1:
		output.Append(Value(ExecutionRegionEventStatusToString(entry.status_kind)));
		return;
	case 2:
		output.Append(Value(ExecutionRegionExecutionModeToString(entry.execution_mode_kind)));
		return;
	case 3:
		output.Append(Value(ExecutionRunnerKindToString(entry.selected_runner_kind)));
		return;
	case 4:
		AppendExecutionRegionNullableString(output, entry.blocker);
		return;
	case 5:
		output.Append(Value::UBIGINT(entry.count));
		return;
	case 6:
		output.Append(Value::BIGINT(entry.decision_time_us));
		return;
	case 7:
		output.Append(Value::BIGINT(entry.compile_time_us));
		return;
	case 8:
		output.Append(Value::UBIGINT(entry.code_size));
		return;
	case 9:
		output.Append(Value::UBIGINT(entry.input_rows));
		return;
	case 10:
		output.Append(Value::UBIGINT(entry.output_rows));
		return;
	case 11:
		output.Append(Value::UBIGINT(entry.invocation_count));
		return;
	case 12:
		output.Append(Value::BIGINT(entry.runtime_time_us));
		return;
	case 13:
		output.Append(Value::UBIGINT(entry.source_contract_output_rows));
		return;
	case 14:
		output.Append(Value::UBIGINT(entry.source_contract_invocation_count));
		return;
	case 15:
		output.Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		return;
	case 16:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.source_stage_runtime));
		return;
	case 17:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.source_stage_runtime));
		return;
	case 18:
		output.Append(Value::UBIGINT(entry.sink_next_batch_invocation_count));
		return;
	case 19:
		output.Append(Value::BIGINT(entry.sink_next_batch_runtime_time_us));
		return;
	case 20:
		output.Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		return;
	case 21:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.generated_stage_runtime));
		return;
	case 22:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.generated_stage_runtime));
		return;
	case 23:
		output.Append(Value::BIGINT(entry.ir_lowering_time_us));
		return;
	case 24:
		output.Append(Value::BIGINT(entry.backend_analysis_time_us));
		return;
	case 25:
		output.Append(Value::BIGINT(entry.codegen_time_us));
		return;
	case 26:
		output.Append(Value::BOOLEAN(entry.has_runner_cost));
		return;
	case 27:
		output.Append(Value::BIGINT(entry.runner_cost_rows));
		return;
	case 28:
		output.Append(Value::BIGINT(entry.runner_cost_batches));
		return;
	case 29:
		output.Append(Value::BIGINT(entry.runner_cost_expression_cost));
		return;
	case 30:
		output.Append(Value::BIGINT(entry.runner_cost_generated_stage_count));
		return;
	case 31:
		output.Append(Value::BIGINT(entry.runner_cost_materialization_elision_count));
		return;
	case 32:
		output.Append(Value::BIGINT(entry.runner_cost_native_join_stage_count));
		return;
	case 33:
		output.Append(Value::BIGINT(entry.runner_cost_native_aggregate_stage_count));
		return;
	case 34:
		output.Append(Value::BIGINT(entry.runner_cost_native_sort_stage_count));
		return;
	case 35:
		output.Append(Value::BOOLEAN(entry.runner_cost_full_pipeline));
		return;
	case 36:
		output.Append(Value::BIGINT(entry.runner_cost_saved_work_per_batch));
		return;
	case 37:
		output.Append(Value::BIGINT(entry.runner_cost_accelerated_runner_benefit));
		return;
	case 38:
		output.Append(Value::BIGINT(entry.runner_cost_startup_cost));
		return;
	case 39:
		output.Append(Value::BIGINT(entry.runner_cost_required_benefit));
		return;
	case 40:
		output.Append(Value::BIGINT(entry.runner_cost_net_benefit));
		return;
	case 41:
		output.Append(Value::UBIGINT(entry.runner_cost_selected_accelerated_runner_count));
		return;
	default:
		throw InternalException("Unsupported column index for duckdb_jit_counters");
	}
}

static unique_ptr<FunctionData> DuckDBJitCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("backend_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("execution_mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("selected_runner");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("blocker");
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
	names.emplace_back("source_contract_output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_contract_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_contract_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("source_stage_runtime_breakdown");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_stage_count_breakdown");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_next_batch_invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sink_next_batch_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_body_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_stage_runtime_breakdown");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("generated_stage_count_breakdown");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ir_lowering_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("backend_analysis_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("codegen_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_profile");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("runner_cost_rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_batches");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_expression_cost");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_generated_stage_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_materialization_elision_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_native_join_stage_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_native_aggregate_stage_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_native_sort_stage_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_full_pipeline");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("runner_cost_saved_work_per_batch");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_accelerated_runner_benefit");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_startup_cost");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_required_benefit");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_net_benefit");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_selected_accelerated_runner_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitCountersInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitCountersData>();
	result->entries = ExecutionRegionManager::Get(context).GetCounters();
	result->column_ids = input.column_ids;
	return std::move(result);
}

static void DuckDBJitCountersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitCountersData>();
	EmitExecutionRegionTableFunctionRows(data, output, AppendJitCounterColumn);
}

void DuckDBJitCountersFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_counters", {}, DuckDBJitCountersFunction, DuckDBJitCountersBind,
	                              DuckDBJitCountersInit);
	function.suppress_compiled_execution = true;
	function.projection_pushdown = true;
	set.AddFunction(function);
}

} // namespace duckdb
