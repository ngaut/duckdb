#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitEventsData : public ExecutionRegionTableFunctionState<ExecutionRegionEvent> {};

static constexpr idx_t JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET = 56;
static constexpr idx_t JIT_EVENT_PIPELINE_SHAPE_COLUMN =
    JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET + EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT;
static constexpr idx_t JIT_EVENT_PIPELINE_ESTIMATED_CARDINALITY_COLUMN = JIT_EVENT_PIPELINE_SHAPE_COLUMN + 1;

static void AppendJitEventCandidateColumn(Vector &output, idx_t column_id, const ExecutionRegionEvent &entry) {
	auto candidate_column_id = column_id - JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET;
	if (entry.has_candidate) {
		AppendExecutionRegionCandidateTraceColumn(output, candidate_column_id, entry.candidate_signature,
		                                          entry.candidate_traits, entry.candidate_contract);
	} else {
		AppendNullExecutionRegionCandidateTraceColumn(output, candidate_column_id);
	}
}

static void AppendJitEventColumn(Vector &output, idx_t column_id, const ExecutionRegionEvent &entry) {
	if (column_id >= JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET && column_id < JIT_EVENT_PIPELINE_SHAPE_COLUMN) {
		AppendJitEventCandidateColumn(output, column_id, entry);
		return;
	}
	switch (column_id) {
	case 0:
		output.Append(Value::UBIGINT(entry.event_id));
		return;
	case 1:
		output.Append(Value(entry.backend_name));
		return;
	case 2:
		output.Append(Value(ExecutionRegionCompileTargetToString(entry.target_kind)));
		return;
	case 3:
		output.Append(Value(ExecutionRegionEventStatusToString(entry.status_kind)));
		return;
	case 4:
		output.Append(Value(ExecutionRegionExecutionModeToString(entry.execution_mode_kind)));
		return;
	case 5:
		output.Append(Value(ExecutionRegionFormToString(entry.region_execution_form_kind)));
		return;
	case 6:
		output.Append(Value(ExecutionRegionExecutionBodyToString(entry.execution_body_kind)));
		return;
	case 7:
		output.Append(Value(ExecutionRegionSourceExecutionKindToString(entry.selected_source_execution)));
		return;
	case 8:
		output.Append(Value::BOOLEAN(entry.selected_uses_scan_filters));
		return;
	case 9:
		output.Append(Value::BOOLEAN(entry.candidate_uses_scan_filters));
		return;
	case 10:
		output.Append(Value(ExecutionRegionEventPolicyToString(entry.requested_policy_kind)));
		return;
	case 11:
		output.Append(Value(entry.reason));
		return;
	case 12:
		AppendExecutionRegionNullableString(output, entry.ir);
		return;
	case 13:
		output.Append(Value::BIGINT(entry.decision_time_us));
		return;
	case 14:
		output.Append(Value::BIGINT(entry.compile_time_us));
		return;
	case 15:
		output.Append(Value::UBIGINT(entry.code_size));
		return;
	case 16:
		output.Append(Value(ExecutionRegionEventPhaseToString(entry.phase_kind)));
		return;
	case 17:
		output.Append(Value::UBIGINT(entry.kernel_id));
		return;
	case 18:
		output.Append(Value::UBIGINT(entry.input_rows));
		return;
	case 19:
		output.Append(Value::UBIGINT(entry.output_rows));
		return;
	case 20:
		output.Append(Value::UBIGINT(entry.invocation_count));
		return;
	case 21:
		output.Append(Value::BIGINT(entry.runtime_time_us));
		return;
	case 22:
		AppendExecutionRegionNullableString(output, entry.runtime_result);
		return;
	case 23:
		output.Append(Value::UBIGINT(entry.source_contract_output_rows));
		return;
	case 24:
		output.Append(Value::UBIGINT(entry.source_contract_invocation_count));
		return;
	case 25:
		output.Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		return;
	case 26:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.source_stage_runtime));
		return;
	case 27:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.source_stage_runtime));
		return;
	case 28:
		output.Append(Value::UBIGINT(entry.sink_next_batch_invocation_count));
		return;
	case 29:
		output.Append(Value::BIGINT(entry.sink_next_batch_runtime_time_us));
		return;
	case 30:
		output.Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		return;
	case 31:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.generated_stage_runtime));
		return;
	case 32:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.generated_stage_runtime));
		return;
	case 33:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_id) : Value(LogicalType::UBIGINT));
		return;
	case 34:
		if (entry.has_candidate) {
			output.Append(Value(entry.candidate_shape));
		} else {
			output.Append(Value(LogicalType::VARCHAR));
		}
		return;
	case 35:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_node_count) : Value(LogicalType::UBIGINT));
		return;
	case 36:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_estimated_cardinality)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case 37:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_start_operator_index)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case 38:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_end_operator_index)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case 39:
		output.Append(Value(ExecutionRunnerKindToString(entry.selected_runner)));
		return;
	case 40:
		AppendExecutionRegionNullableString(output, entry.blocker);
		return;
	case 41:
		output.Append(Value::BIGINT(entry.ir_lowering_time_us));
		return;
	case 42:
		output.Append(Value::BIGINT(entry.backend_analysis_time_us));
		return;
	case 43:
		output.Append(Value::BIGINT(entry.codegen_time_us));
		return;
	case 44:
		if (entry.has_candidate) {
			AppendExecutionRegionNullableString(output, entry.candidate_pipeline_shape);
		} else {
			output.Append(Value(LogicalType::VARCHAR));
		}
		return;
	case 45:
		output.Append(Value::BOOLEAN(entry.runner_cost.present));
		return;
	case 46:
		output.Append(Value::BIGINT(entry.runner_cost.rows));
		return;
	case 47:
		output.Append(Value::BIGINT(entry.runner_cost.batches));
		return;
	case 48:
		output.Append(Value::BIGINT(entry.runner_cost.expression_cost));
		return;
	case 49:
		output.Append(Value::BIGINT(entry.runner_cost.accelerated_stage_count));
		return;
	case 50:
		output.Append(Value::BIGINT(entry.runner_cost.saved_work_per_batch));
		return;
	case 51:
		output.Append(Value::BIGINT(entry.runner_cost.accelerated_runner_benefit));
		return;
	case 52:
		output.Append(Value::BIGINT(entry.runner_cost.startup_cost));
		return;
	case 53:
		output.Append(Value::BIGINT(entry.runner_cost.required_benefit));
		return;
	case 54:
		output.Append(Value::BIGINT(entry.runner_cost.net_benefit));
		return;
	case 55:
		output.Append(Value::BOOLEAN(entry.runner_cost.selected_accelerated_runner));
		return;
	case JIT_EVENT_PIPELINE_SHAPE_COLUMN:
		if (entry.has_pipeline) {
			AppendExecutionRegionNullableString(output, entry.pipeline_shape);
		} else {
			output.Append(Value(LogicalType::VARCHAR));
		}
		return;
	case JIT_EVENT_PIPELINE_ESTIMATED_CARDINALITY_COLUMN:
		output.Append(entry.has_pipeline ? Value::UBIGINT(entry.pipeline_estimated_cardinality)
		                                 : Value(LogicalType::UBIGINT));
		return;
	default:
		throw InternalException("Unsupported column index for duckdb_jit_events");
	}
}

static unique_ptr<FunctionData> DuckDBJitEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("event_id");
	return_types.emplace_back(LogicalType::UBIGINT);
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
	names.emplace_back("execution_body");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("selected_source_execution");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("selected_uses_scan_filters");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_uses_scan_filters");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("requested_policy");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("reason");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ir");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("decision_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("compile_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("code_size");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("phase");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("kernel_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("output_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("invocation_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runtime_result");
	return_types.emplace_back(LogicalType::VARCHAR);
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
	names.emplace_back("candidate_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_node_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_start_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_end_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("selected_runner");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("blocker");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ir_lowering_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("backend_analysis_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("codegen_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("candidate_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("runner_cost_profile");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("runner_cost_rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_batches");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_expression_cost");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("runner_cost_accelerated_stage_count");
	return_types.emplace_back(LogicalType::BIGINT);
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
	names.emplace_back("runner_cost_selected_accelerated_runner");
	return_types.emplace_back(LogicalType::BOOLEAN);
	AddExecutionRegionCandidateTraceColumns(return_types, names);
	names.emplace_back("pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("pipeline_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitEventsInit(ClientContext &context, TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitEventsData>();
	result->entries = ExecutionRegionManager::Get(context).GetEvents();
	result->column_ids = input.column_ids;
	return std::move(result);
}

static void DuckDBJitEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitEventsData>();
	EmitExecutionRegionTableFunctionRows(data, output, AppendJitEventColumn);
}

void DuckDBJitEventsFun::RegisterFunction(BuiltinFunctions &set) {
	auto function =
	    TableFunction("duckdb_jit_events", {}, DuckDBJitEventsFunction, DuckDBJitEventsBind, DuckDBJitEventsInit);
	function.suppress_compiled_execution = true;
	function.projection_pushdown = true;
	set.AddFunction(function);
}

} // namespace duckdb
