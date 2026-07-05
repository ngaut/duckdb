#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitEventsData : public ExecutionRegionTableFunctionState<ExecutionRegionEvent> {};

enum JitEventColumn : idx_t {
	JIT_EVENT_EVENT_ID,
	JIT_EVENT_BACKEND_NAME,
	JIT_EVENT_STATUS,
	JIT_EVENT_EXECUTION_MODE,
	JIT_EVENT_SELECTED_SOURCE_EXECUTION,
	JIT_EVENT_SELECTED_USES_SCAN_FILTERS,
	JIT_EVENT_CANDIDATE_USES_SCAN_FILTERS,
	JIT_EVENT_REASON,
	JIT_EVENT_IR,
	JIT_EVENT_DECISION_TIME_US,
	JIT_EVENT_COMPILE_TIME_US,
	JIT_EVENT_CODE_SIZE,
	JIT_EVENT_PHASE,
	JIT_EVENT_KERNEL_ID,
	JIT_EVENT_INPUT_ROWS,
	JIT_EVENT_OUTPUT_ROWS,
	JIT_EVENT_INVOCATION_COUNT,
	JIT_EVENT_RUNTIME_TIME_US,
	JIT_EVENT_RUNTIME_RESULT,
	JIT_EVENT_SOURCE_CONTRACT_OUTPUT_ROWS,
	JIT_EVENT_SOURCE_CONTRACT_INVOCATION_COUNT,
	JIT_EVENT_SOURCE_CONTRACT_RUNTIME_TIME_US,
	JIT_EVENT_SOURCE_STAGE_RUNTIME_BREAKDOWN,
	JIT_EVENT_SOURCE_STAGE_COUNT_BREAKDOWN,
	JIT_EVENT_SINK_NEXT_BATCH_INVOCATION_COUNT,
	JIT_EVENT_SINK_NEXT_BATCH_RUNTIME_TIME_US,
	JIT_EVENT_GENERATED_BODY_RUNTIME_TIME_US,
	JIT_EVENT_GENERATED_STAGE_RUNTIME_BREAKDOWN,
	JIT_EVENT_GENERATED_STAGE_COUNT_BREAKDOWN,
	JIT_EVENT_CANDIDATE_ID,
	JIT_EVENT_CANDIDATE_SHAPE,
	JIT_EVENT_CANDIDATE_NODE_COUNT,
	JIT_EVENT_CANDIDATE_ESTIMATED_CARDINALITY,
	JIT_EVENT_CANDIDATE_START_OPERATOR_INDEX,
	JIT_EVENT_CANDIDATE_END_OPERATOR_INDEX,
	JIT_EVENT_SELECTED_RUNNER,
	JIT_EVENT_BLOCKER,
	JIT_EVENT_IR_LOWERING_TIME_US,
	JIT_EVENT_BACKEND_ANALYSIS_TIME_US,
	JIT_EVENT_CODEGEN_TIME_US,
	JIT_EVENT_PIPELINE_CBO_TIME_US,
	JIT_EVENT_GRAPH_BUILD_TIME_US,
	JIT_EVENT_CANDIDATE_CBO_TIME_US,
	JIT_EVENT_EXECUTABLE_BUILD_TIME_US,
	JIT_EVENT_MACHINE_CODEGEN_TIME_US,
	JIT_EVENT_KERNEL_BUILD_TIME_US,
	JIT_EVENT_LAZY_CODEGEN_TIME_US,
	JIT_EVENT_LAZY_MACHINE_CODEGEN_TIME_US,
	JIT_EVENT_LAZY_CODE_SIZE,
	JIT_EVENT_HASH_JOIN_PROBE_LAYOUT,
	JIT_EVENT_JIT_RUNTIME_PATH_COUNTS,
	JIT_EVENT_JIT_MATERIALIZATION_BOUNDARY_COUNTS,
	JIT_EVENT_CANDIDATE_PIPELINE_SHAPE,
	JIT_EVENT_RUNNER_COST_PROFILE,
	JIT_EVENT_RUNNER_COST_ROWS,
	JIT_EVENT_RUNNER_COST_BATCHES,
	JIT_EVENT_RUNNER_COST_EXPRESSION_COST,
	JIT_EVENT_RUNNER_COST_GENERATED_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_GENERATED_BACKEND_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_MATERIALIZATION_ELISION_COUNT,
	JIT_EVENT_RUNNER_COST_MATERIALIZATION_SOURCE_APPEND_COUNT,
	JIT_EVENT_RUNNER_COST_UNFUSED_MARK_FILTER_AGGREGATE_COUNT,
	JIT_EVENT_RUNNER_COST_NATIVE_JOIN_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_NATIVE_HASH_JOIN_BUILD_SINK_COUNT,
	JIT_EVENT_RUNNER_COST_NATIVE_AGGREGATE_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_NATIVE_GROUPED_AGGREGATE_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_NATIVE_SORT_STAGE_COUNT,
	JIT_EVENT_RUNNER_COST_FULL_PIPELINE,
	JIT_EVENT_RUNNER_COST_INPUT_SCOPE,
	JIT_EVENT_RUNNER_COST_ADMISSION_CLASS,
	JIT_EVENT_RUNNER_COST_SELECTION_REASON,
	JIT_EVENT_RUNNER_COST_GENERATED_WORK_CLASS,
	JIT_EVENT_RUNNER_COST_NATIVE_PROTOCOL_CLASS,
	JIT_EVENT_RUNNER_COST_GENERATED_EXPRESSION_WORK,
	JIT_EVENT_RUNNER_COST_GENERATED_STAGE_WORK,
	JIT_EVENT_RUNNER_COST_GENERATED_BACKEND_STAGE_WORK,
	JIT_EVENT_RUNNER_COST_NATIVE_OPERATOR_WORK,
	JIT_EVENT_RUNNER_COST_MATERIALIZATION_ELISION_WORK,
	JIT_EVENT_RUNNER_COST_MATERIALIZATION_SOURCE_APPEND_PENALTY,
	JIT_EVENT_RUNNER_COST_UNFUSED_MARK_FILTER_AGGREGATE_PENALTY,
	JIT_EVENT_RUNNER_COST_FULL_PIPELINE_WORK,
	JIT_EVENT_RUNNER_COST_STATEFUL_PROTOCOL_PENALTY,
	JIT_EVENT_RUNNER_COST_SAVED_WORK_PER_BATCH,
	JIT_EVENT_RUNNER_COST_ACCELERATED_RUNNER_BENEFIT,
	JIT_EVENT_RUNNER_COST_STARTUP_COST,
	JIT_EVENT_RUNNER_COST_REQUIRED_BENEFIT,
	JIT_EVENT_RUNNER_COST_NET_BENEFIT,
	JIT_EVENT_RUNNER_COST_COMPILED_VECTORIZED_RUNNER_BENEFIT,
	JIT_EVENT_RUNNER_COST_COMPILED_VECTORIZED_STARTUP_COST,
	JIT_EVENT_RUNNER_COST_COMPILED_VECTORIZED_REQUIRED_BENEFIT,
	JIT_EVENT_RUNNER_COST_COMPILED_VECTORIZED_NET_BENEFIT,
	JIT_EVENT_RUNNER_COST_GPU_RUNNER_BENEFIT,
	JIT_EVENT_RUNNER_COST_GPU_TRANSFER_COST,
	JIT_EVENT_RUNNER_COST_GPU_STARTUP_COST,
	JIT_EVENT_RUNNER_COST_GPU_REQUIRED_BENEFIT,
	JIT_EVENT_RUNNER_COST_GPU_NET_BENEFIT,
	JIT_EVENT_RUNNER_COST_SELECTED_ACCELERATED_RUNNER,
	JIT_EVENT_RUNNER_COST_SELECTED_COMPILED_VECTORIZED_RUNNER,
	JIT_EVENT_RUNNER_COST_SELECTED_GPU_RUNNER,
	JIT_EVENT_COLUMN_COUNT
};

static constexpr idx_t JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET = JIT_EVENT_IR_LOWERING_TIME_US;
static_assert(JIT_EVENT_KERNEL_BUILD_TIME_US - JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT);
static constexpr idx_t JIT_EVENT_RUNNER_COST_PROFILE_COLUMN_OFFSET = JIT_EVENT_RUNNER_COST_PROFILE;
static_assert(JIT_EVENT_RUNNER_COST_SELECTION_REASON - JIT_EVENT_RUNNER_COST_PROFILE_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT);
static constexpr idx_t JIT_EVENT_RUNNER_COST_WORK_COLUMN_OFFSET = JIT_EVENT_RUNNER_COST_GENERATED_EXPRESSION_WORK;
static_assert(JIT_EVENT_RUNNER_COST_GPU_NET_BENEFIT - JIT_EVENT_RUNNER_COST_WORK_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT);
static constexpr idx_t JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET = JIT_EVENT_COLUMN_COUNT;
static constexpr idx_t JIT_EVENT_PIPELINE_SHAPE_COLUMN =
    JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET + EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT;
static constexpr idx_t JIT_EVENT_PIPELINE_ESTIMATED_CARDINALITY_COLUMN = JIT_EVENT_PIPELINE_SHAPE_COLUMN + 1;

static void AddJitEventRunnerCostColumns(vector<LogicalType> &return_types, vector<string> &names) {
	AddExecutionRegionTableFunctionColumns(return_types, names, EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMNS,
	                                       EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_generated_work_class",
	                                      LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_native_protocol_class",
	                                      LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumns(return_types, names, EXECUTION_REGION_RUNNER_COST_WORK_COLUMNS,
	                                       EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_accelerated_runner",
	                                      LogicalType::BOOLEAN);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_compiled_vectorized_runner",
	                                      LogicalType::BOOLEAN);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_gpu_runner", LogicalType::BOOLEAN);
	D_ASSERT(names.size() == JIT_EVENT_COLUMN_COUNT);
	D_ASSERT(return_types.size() == JIT_EVENT_COLUMN_COUNT);
}

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
	if (column_id >= JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET &&
	    column_id < JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET + EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT) {
		AppendExecutionRegionStageTimingColumn(output, column_id - JIT_EVENT_STAGE_TIMING_COLUMN_OFFSET,
		                                       entry.stage_timings);
		return;
	}
	if (column_id >= JIT_EVENT_RUNNER_COST_PROFILE_COLUMN_OFFSET &&
	    column_id < JIT_EVENT_RUNNER_COST_PROFILE_COLUMN_OFFSET + EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT) {
		AppendExecutionRegionRunnerCostProfileColumn(output, column_id - JIT_EVENT_RUNNER_COST_PROFILE_COLUMN_OFFSET,
		                                             entry.runner_cost);
		return;
	}
	if (column_id >= JIT_EVENT_RUNNER_COST_WORK_COLUMN_OFFSET &&
	    column_id < JIT_EVENT_RUNNER_COST_WORK_COLUMN_OFFSET + EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT) {
		AppendExecutionRegionRunnerCostWorkColumn(output, column_id - JIT_EVENT_RUNNER_COST_WORK_COLUMN_OFFSET,
		                                          entry.runner_cost);
		return;
	}
	switch (column_id) {
	case JIT_EVENT_EVENT_ID:
		output.Append(Value::UBIGINT(entry.event_id));
		return;
	case JIT_EVENT_BACKEND_NAME:
		output.Append(Value(entry.backend_name));
		return;
	case JIT_EVENT_STATUS:
		output.Append(Value(ExecutionRegionEventStatusToString(entry.status_kind)));
		return;
	case JIT_EVENT_EXECUTION_MODE:
		output.Append(Value(ExecutionRegionExecutionModeToString(entry.execution_mode_kind)));
		return;
	case JIT_EVENT_SELECTED_SOURCE_EXECUTION:
		output.Append(Value(ExecutionRegionSourceExecutionKindToString(entry.selected_source_execution)));
		return;
	case JIT_EVENT_SELECTED_USES_SCAN_FILTERS:
		output.Append(Value::BOOLEAN(entry.selected_uses_scan_filters));
		return;
	case JIT_EVENT_CANDIDATE_USES_SCAN_FILTERS:
		output.Append(Value::BOOLEAN(entry.candidate_uses_scan_filters));
		return;
	case JIT_EVENT_REASON:
		output.Append(Value(entry.reason));
		return;
	case JIT_EVENT_IR:
		AppendExecutionRegionNullableString(output, entry.ir);
		return;
	case JIT_EVENT_DECISION_TIME_US:
		output.Append(Value::BIGINT(entry.decision_time_us));
		return;
	case JIT_EVENT_COMPILE_TIME_US:
		output.Append(Value::BIGINT(entry.compile_time_us));
		return;
	case JIT_EVENT_CODE_SIZE:
		output.Append(Value::UBIGINT(entry.code_size));
		return;
	case JIT_EVENT_PHASE:
		output.Append(Value(ExecutionRegionEventPhaseToString(entry.phase_kind)));
		return;
	case JIT_EVENT_KERNEL_ID:
		output.Append(Value::UBIGINT(entry.kernel_id));
		return;
	case JIT_EVENT_INPUT_ROWS:
		output.Append(Value::UBIGINT(entry.input_rows));
		return;
	case JIT_EVENT_OUTPUT_ROWS:
		output.Append(Value::UBIGINT(entry.output_rows));
		return;
	case JIT_EVENT_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.invocation_count));
		return;
	case JIT_EVENT_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.runtime_time_us));
		return;
	case JIT_EVENT_RUNTIME_RESULT:
		AppendExecutionRegionNullableString(output, entry.runtime_result);
		return;
	case JIT_EVENT_SOURCE_CONTRACT_OUTPUT_ROWS:
		output.Append(Value::UBIGINT(entry.source_contract_output_rows));
		return;
	case JIT_EVENT_SOURCE_CONTRACT_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.source_contract_invocation_count));
		return;
	case JIT_EVENT_SOURCE_CONTRACT_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		return;
	case JIT_EVENT_SOURCE_STAGE_RUNTIME_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.source_stage_runtime));
		return;
	case JIT_EVENT_SOURCE_STAGE_COUNT_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.source_stage_runtime));
		return;
	case JIT_EVENT_SINK_NEXT_BATCH_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.sink_next_batch_invocation_count));
		return;
	case JIT_EVENT_SINK_NEXT_BATCH_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.sink_next_batch_runtime_time_us));
		return;
	case JIT_EVENT_GENERATED_BODY_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		return;
	case JIT_EVENT_GENERATED_STAGE_RUNTIME_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.generated_stage_runtime));
		return;
	case JIT_EVENT_GENERATED_STAGE_COUNT_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.generated_stage_runtime));
		return;
	case JIT_EVENT_CANDIDATE_ID:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_id) : Value(LogicalType::UBIGINT));
		return;
	case JIT_EVENT_CANDIDATE_SHAPE:
		if (entry.has_candidate) {
			output.Append(Value(entry.candidate_shape));
		} else {
			output.Append(Value(LogicalType::VARCHAR));
		}
		return;
	case JIT_EVENT_CANDIDATE_NODE_COUNT:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_node_count) : Value(LogicalType::UBIGINT));
		return;
	case JIT_EVENT_CANDIDATE_ESTIMATED_CARDINALITY:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_estimated_cardinality)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case JIT_EVENT_CANDIDATE_START_OPERATOR_INDEX:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_start_operator_index)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case JIT_EVENT_CANDIDATE_END_OPERATOR_INDEX:
		output.Append(entry.has_candidate ? Value::UBIGINT(entry.candidate_end_operator_index)
		                                  : Value(LogicalType::UBIGINT));
		return;
	case JIT_EVENT_SELECTED_RUNNER:
		output.Append(Value(ExecutionRunnerKindToString(entry.selected_runner)));
		return;
	case JIT_EVENT_BLOCKER:
		AppendExecutionRegionNullableString(output, entry.blocker);
		return;
	case JIT_EVENT_LAZY_CODEGEN_TIME_US:
		output.Append(Value::BIGINT(entry.jit_runtime.lazy_codegen.codegen_time_us));
		return;
	case JIT_EVENT_LAZY_MACHINE_CODEGEN_TIME_US:
		output.Append(Value::BIGINT(entry.jit_runtime.lazy_codegen.machine_codegen_time_us));
		return;
	case JIT_EVENT_LAZY_CODE_SIZE:
		output.Append(Value::UBIGINT(entry.jit_runtime.lazy_codegen.code_size));
		return;
	case JIT_EVENT_HASH_JOIN_PROBE_LAYOUT:
		AppendExecutionRegionNullableString(output, entry.jit_runtime.hash_join_probe_layout);
		return;
	case JIT_EVENT_JIT_RUNTIME_PATH_COUNTS:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionCounterBreakdown(entry.jit_runtime.runtime_path_counts));
		return;
	case JIT_EVENT_JIT_MATERIALIZATION_BOUNDARY_COUNTS:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionCounterBreakdown(entry.jit_runtime.materialization_boundary_counts));
		return;
	case JIT_EVENT_CANDIDATE_PIPELINE_SHAPE:
		if (entry.has_candidate) {
			AppendExecutionRegionNullableString(output, entry.candidate_pipeline_shape);
		} else {
			output.Append(Value(LogicalType::VARCHAR));
		}
		return;
	case JIT_EVENT_RUNNER_COST_GENERATED_WORK_CLASS:
		output.Append(Value(PhysicalRunnerGeneratedWorkClassToString(entry.runner_cost.generated_work_class)));
		return;
	case JIT_EVENT_RUNNER_COST_NATIVE_PROTOCOL_CLASS:
		output.Append(Value(PhysicalRunnerNativeProtocolClassToString(entry.runner_cost.native_protocol_class)));
		return;
	case JIT_EVENT_RUNNER_COST_SELECTED_ACCELERATED_RUNNER:
		output.Append(Value::BOOLEAN(entry.runner_cost.selected_accelerated_runner));
		return;
	case JIT_EVENT_RUNNER_COST_SELECTED_COMPILED_VECTORIZED_RUNNER:
		output.Append(Value::BOOLEAN(entry.runner_cost.selected_compiled_vectorized_runner));
		return;
	case JIT_EVENT_RUNNER_COST_SELECTED_GPU_RUNNER:
		output.Append(Value::BOOLEAN(entry.runner_cost.selected_gpu_runner));
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
	AddExecutionRegionTableFunctionColumn(return_types, names, "event_id", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "backend_name", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "status", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "execution_mode", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "selected_source_execution", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "selected_uses_scan_filters", LogicalType::BOOLEAN);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_uses_scan_filters", LogicalType::BOOLEAN);
	AddExecutionRegionTableFunctionColumn(return_types, names, "reason", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "ir", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "decision_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "compile_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "code_size", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "phase", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "kernel_id", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "input_rows", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "output_rows", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "invocation_count", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runtime_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runtime_result", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "source_contract_output_rows", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "source_contract_invocation_count",
	                                      LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "source_contract_runtime_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "source_stage_runtime_breakdown", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "source_stage_count_breakdown", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "sink_next_batch_invocation_count",
	                                      LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "sink_next_batch_runtime_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "generated_body_runtime_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "generated_stage_runtime_breakdown",
	                                      LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "generated_stage_count_breakdown", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_id", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_shape", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_node_count", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_estimated_cardinality", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_start_operator_index", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_end_operator_index", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "selected_runner", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "blocker", LogicalType::VARCHAR);
	AddExecutionRegionStageTimingColumns(return_types, names);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_codegen_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_machine_codegen_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_code_size", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "hash_join_probe_layout", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "jit_runtime_path_counts", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "jit_materialization_boundary_counts",
	                                      LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "candidate_pipeline_shape", LogicalType::VARCHAR);
	AddJitEventRunnerCostColumns(return_types, names);
	AddExecutionRegionCandidateTraceColumns(return_types, names);
	D_ASSERT(names.size() == JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET + EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT);
	D_ASSERT(return_types.size() ==
	         JIT_EVENT_CANDIDATE_TRACE_COLUMN_OFFSET + EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "pipeline_shape", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "pipeline_estimated_cardinality", LogicalType::UBIGINT);
	D_ASSERT(names.size() == JIT_EVENT_PIPELINE_ESTIMATED_CARDINALITY_COLUMN + 1);
	D_ASSERT(return_types.size() == JIT_EVENT_PIPELINE_ESTIMATED_CARDINALITY_COLUMN + 1);
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
