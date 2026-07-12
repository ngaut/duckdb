#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitCountersData : public ExecutionRegionTableFunctionState<ExecutionRegionCounter> {};

enum JitCounterColumn : idx_t {
	JIT_COUNTER_BACKEND_NAME,
	JIT_COUNTER_KERNEL_ID,
	JIT_COUNTER_STATUS,
	JIT_COUNTER_EXECUTION_MODE,
	JIT_COUNTER_SELECTED_RUNNER,
	JIT_COUNTER_BLOCKER,
	JIT_COUNTER_COUNT,
	JIT_COUNTER_DECISION_TIME_US,
	JIT_COUNTER_COMPILE_TIME_US,
	JIT_COUNTER_CODE_SIZE,
	JIT_COUNTER_INPUT_ROWS,
	JIT_COUNTER_OUTPUT_ROWS,
	JIT_COUNTER_INVOCATION_COUNT,
	JIT_COUNTER_RUNTIME_TIME_US,
	JIT_COUNTER_SOURCE_CONTRACT_OUTPUT_ROWS,
	JIT_COUNTER_SOURCE_CONTRACT_INVOCATION_COUNT,
	JIT_COUNTER_SOURCE_CONTRACT_RUNTIME_TIME_US,
	JIT_COUNTER_SOURCE_STAGE_RUNTIME_BREAKDOWN,
	JIT_COUNTER_SOURCE_STAGE_COUNT_BREAKDOWN,
	JIT_COUNTER_SINK_NEXT_BATCH_INVOCATION_COUNT,
	JIT_COUNTER_SINK_NEXT_BATCH_RUNTIME_TIME_US,
	JIT_COUNTER_GENERATED_BODY_RUNTIME_TIME_US,
	JIT_COUNTER_GENERATED_STAGE_RUNTIME_BREAKDOWN,
	JIT_COUNTER_GENERATED_STAGE_COUNT_BREAKDOWN,
	JIT_COUNTER_IR_LOWERING_TIME_US,
	JIT_COUNTER_BACKEND_ANALYSIS_TIME_US,
	JIT_COUNTER_CODEGEN_TIME_US,
	JIT_COUNTER_PIPELINE_CBO_TIME_US,
	JIT_COUNTER_GRAPH_BUILD_TIME_US,
	JIT_COUNTER_CANDIDATE_CBO_TIME_US,
	JIT_COUNTER_EXECUTABLE_BUILD_TIME_US,
	JIT_COUNTER_MACHINE_CODEGEN_TIME_US,
	JIT_COUNTER_KERNEL_BUILD_TIME_US,
	JIT_COUNTER_LAZY_CODEGEN_TIME_US,
	JIT_COUNTER_LAZY_MACHINE_CODEGEN_TIME_US,
	JIT_COUNTER_LAZY_CODE_SIZE,
	JIT_COUNTER_HASH_JOIN_PROBE_LAYOUT,
	JIT_COUNTER_JIT_RUNTIME_PATH_COUNTS,
	JIT_COUNTER_JIT_RUNTIME_PROOF_COUNTS,
	JIT_COUNTER_JIT_RUNTIME_DELEGATION_COUNTS,
	JIT_COUNTER_RUNNER_COST_PROFILE,
	JIT_COUNTER_RUNNER_COST_ROWS,
	JIT_COUNTER_RUNNER_COST_BATCHES,
	JIT_COUNTER_RUNNER_COST_COSTED_BATCHES,
	JIT_COUNTER_RUNNER_COST_EXPRESSION_COST,
	JIT_COUNTER_RUNNER_COST_SOURCE_CONTRACT_INPUT_ROWS,
	JIT_COUNTER_RUNNER_COST_SOURCE_CONTRACT_INPUT_BATCHES,
	JIT_COUNTER_RUNNER_COST_SOURCE_CONTRACT_OUTPUT_CARDINALITY_UNKNOWN,
	JIT_COUNTER_RUNNER_COST_GENERATED_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_GENERATED_BACKEND_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_GENERATED_GROUPED_AGGREGATE_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_GROUPED_STATE_ADDRESS_LOOKUP_COUNT,
	JIT_COUNTER_RUNNER_COST_MATERIALIZATION_ELISION_COUNT,
	JIT_COUNTER_RUNNER_COST_SELECTED_HASH_JOIN_FILTER_MATERIALIZATION_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_JOIN_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_HASH_JOIN_BUILD_SINK_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_AGGREGATE_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_GROUPED_AGGREGATE_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_NATIVE_SORT_STAGE_COUNT,
	JIT_COUNTER_RUNNER_COST_FULL_PIPELINE,
	JIT_COUNTER_RUNNER_COST_INPUT_SCOPE,
	JIT_COUNTER_RUNNER_COST_ADMISSION_CLASS,
	JIT_COUNTER_RUNNER_COST_SELECTION_REASON,
	JIT_COUNTER_RUNNER_COST_GROUPED_AGGREGATE_ESTIMATED_CARDINALITY,
	JIT_COUNTER_RUNNER_COST_REQUIRED_RUNTIME_PROOFS,
	JIT_COUNTER_RUNNER_COST_GENERATED_EXPRESSION_WORK,
	JIT_COUNTER_RUNNER_COST_GENERATED_STAGE_WORK,
	JIT_COUNTER_RUNNER_COST_GENERATED_BACKEND_STAGE_WORK,
	JIT_COUNTER_RUNNER_COST_NATIVE_OPERATOR_WORK,
	JIT_COUNTER_RUNNER_COST_MATERIALIZATION_ELISION_WORK,
	JIT_COUNTER_RUNNER_COST_SELECTED_HASH_JOIN_FILTER_MATERIALIZATION_PENALTY,
	JIT_COUNTER_RUNNER_COST_SOURCE_CONTRACT_SCAN_PENALTY,
	JIT_COUNTER_RUNNER_COST_FULL_PIPELINE_WORK,
	JIT_COUNTER_RUNNER_COST_STATEFUL_PROTOCOL_PENALTY,
	JIT_COUNTER_RUNNER_COST_SAVED_WORK_PER_BATCH,
	JIT_COUNTER_RUNNER_COST_ACCELERATED_RUNNER_BENEFIT,
	JIT_COUNTER_RUNNER_COST_STARTUP_COST,
	JIT_COUNTER_RUNNER_COST_REQUIRED_BENEFIT,
	JIT_COUNTER_RUNNER_COST_NET_BENEFIT,
	JIT_COUNTER_RUNNER_COST_COMPILED_VECTORIZED_RUNNER_BENEFIT,
	JIT_COUNTER_RUNNER_COST_COMPILED_VECTORIZED_STARTUP_COST,
	JIT_COUNTER_RUNNER_COST_COMPILED_VECTORIZED_REQUIRED_BENEFIT,
	JIT_COUNTER_RUNNER_COST_COMPILED_VECTORIZED_NET_BENEFIT,
	JIT_COUNTER_RUNNER_COST_GPU_RUNNER_BENEFIT,
	JIT_COUNTER_RUNNER_COST_GPU_TRANSFER_COST,
	JIT_COUNTER_RUNNER_COST_GPU_STARTUP_COST,
	JIT_COUNTER_RUNNER_COST_GPU_REQUIRED_BENEFIT,
	JIT_COUNTER_RUNNER_COST_GPU_NET_BENEFIT,
	JIT_COUNTER_RUNNER_COST_SELECTED_ACCELERATED_RUNNER_COUNT,
	JIT_COUNTER_RUNNER_COST_SELECTED_COMPILED_VECTORIZED_RUNNER_COUNT,
	JIT_COUNTER_RUNNER_COST_SELECTED_GPU_RUNNER_COUNT,
	JIT_COUNTER_COLUMN_COUNT
};

static constexpr idx_t JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET = JIT_COUNTER_IR_LOWERING_TIME_US;
static_assert(JIT_COUNTER_KERNEL_BUILD_TIME_US - JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT);
static constexpr idx_t JIT_COUNTER_RUNNER_COST_PROFILE_COLUMN_OFFSET = JIT_COUNTER_RUNNER_COST_PROFILE;
static_assert(JIT_COUNTER_RUNNER_COST_REQUIRED_RUNTIME_PROOFS - JIT_COUNTER_RUNNER_COST_PROFILE_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT);
static constexpr idx_t JIT_COUNTER_RUNNER_COST_WORK_COLUMN_OFFSET = JIT_COUNTER_RUNNER_COST_GENERATED_EXPRESSION_WORK;
static_assert(JIT_COUNTER_RUNNER_COST_GPU_NET_BENEFIT - JIT_COUNTER_RUNNER_COST_WORK_COLUMN_OFFSET + 1 ==
              EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT);

static void AddJitCounterRunnerCostColumns(vector<LogicalType> &return_types, vector<string> &names) {
	AddExecutionRegionTableFunctionColumns(return_types, names, EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMNS,
	                                       EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT);
	AddExecutionRegionTableFunctionColumns(return_types, names, EXECUTION_REGION_RUNNER_COST_WORK_COLUMNS,
	                                       EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_accelerated_runner_count",
	                                      LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_compiled_vectorized_runner_count",
	                                      LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runner_cost_selected_gpu_runner_count",
	                                      LogicalType::UBIGINT);
	D_ASSERT(names.size() == JIT_COUNTER_COLUMN_COUNT);
	D_ASSERT(return_types.size() == JIT_COUNTER_COLUMN_COUNT);
}

static void AppendJitCounterColumn(Vector &output, idx_t column_id, const ExecutionRegionCounter &entry) {
	if (column_id >= JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET &&
	    column_id < JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET + EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT) {
		AppendExecutionRegionStageTimingColumn(output, column_id - JIT_COUNTER_STAGE_TIMING_COLUMN_OFFSET,
		                                       entry.stage_timings);
		return;
	}
	if (column_id >= JIT_COUNTER_RUNNER_COST_PROFILE_COLUMN_OFFSET &&
	    column_id < JIT_COUNTER_RUNNER_COST_PROFILE_COLUMN_OFFSET + EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT) {
		AppendExecutionRegionRunnerCostProfileColumn(output, column_id - JIT_COUNTER_RUNNER_COST_PROFILE_COLUMN_OFFSET,
		                                             entry.runner_cost);
		return;
	}
	if (column_id >= JIT_COUNTER_RUNNER_COST_WORK_COLUMN_OFFSET &&
	    column_id < JIT_COUNTER_RUNNER_COST_WORK_COLUMN_OFFSET + EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT) {
		AppendExecutionRegionRunnerCostWorkColumn(output, column_id - JIT_COUNTER_RUNNER_COST_WORK_COLUMN_OFFSET,
		                                          entry.runner_cost);
		return;
	}
	switch (column_id) {
	case JIT_COUNTER_BACKEND_NAME:
		output.Append(Value(entry.backend_name));
		return;
	case JIT_COUNTER_KERNEL_ID:
		output.Append(Value::UBIGINT(entry.kernel_id));
		return;
	case JIT_COUNTER_STATUS:
		output.Append(Value(ExecutionRegionEventStatusToString(entry.status_kind)));
		return;
	case JIT_COUNTER_EXECUTION_MODE:
		output.Append(Value(ExecutionRegionExecutionModeToString(entry.execution_mode_kind)));
		return;
	case JIT_COUNTER_SELECTED_RUNNER:
		output.Append(Value(ExecutionRunnerKindToString(entry.selected_runner_kind)));
		return;
	case JIT_COUNTER_BLOCKER:
		AppendExecutionRegionNullableString(output, entry.blocker);
		return;
	case JIT_COUNTER_COUNT:
		output.Append(Value::UBIGINT(entry.count));
		return;
	case JIT_COUNTER_DECISION_TIME_US:
		output.Append(Value::BIGINT(entry.decision_time_us));
		return;
	case JIT_COUNTER_COMPILE_TIME_US:
		output.Append(Value::BIGINT(entry.compile_time_us));
		return;
	case JIT_COUNTER_CODE_SIZE:
		output.Append(Value::UBIGINT(entry.code_size));
		return;
	case JIT_COUNTER_INPUT_ROWS:
		output.Append(Value::UBIGINT(entry.input_rows));
		return;
	case JIT_COUNTER_OUTPUT_ROWS:
		output.Append(Value::UBIGINT(entry.output_rows));
		return;
	case JIT_COUNTER_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.invocation_count));
		return;
	case JIT_COUNTER_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.runtime_time_us));
		return;
	case JIT_COUNTER_SOURCE_CONTRACT_OUTPUT_ROWS:
		output.Append(Value::UBIGINT(entry.source_contract_output_rows));
		return;
	case JIT_COUNTER_SOURCE_CONTRACT_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.source_contract_invocation_count));
		return;
	case JIT_COUNTER_SOURCE_CONTRACT_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		return;
	case JIT_COUNTER_SOURCE_STAGE_RUNTIME_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.source_stage_runtime));
		return;
	case JIT_COUNTER_SOURCE_STAGE_COUNT_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.source_stage_runtime));
		return;
	case JIT_COUNTER_SINK_NEXT_BATCH_INVOCATION_COUNT:
		output.Append(Value::UBIGINT(entry.sink_next_batch_invocation_count));
		return;
	case JIT_COUNTER_SINK_NEXT_BATCH_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.sink_next_batch_runtime_time_us));
		return;
	case JIT_COUNTER_GENERATED_BODY_RUNTIME_TIME_US:
		output.Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		return;
	case JIT_COUNTER_GENERATED_STAGE_RUNTIME_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageRuntimeBreakdown(entry.generated_stage_runtime));
		return;
	case JIT_COUNTER_GENERATED_STAGE_COUNT_BREAKDOWN:
		AppendExecutionRegionNullableString(output,
		                                    RenderExecutionRegionStageCountBreakdown(entry.generated_stage_runtime));
		return;
	case JIT_COUNTER_LAZY_CODEGEN_TIME_US:
		output.Append(Value::BIGINT(entry.jit_runtime.lazy_codegen.codegen_time_us));
		return;
	case JIT_COUNTER_LAZY_MACHINE_CODEGEN_TIME_US:
		output.Append(Value::BIGINT(entry.jit_runtime.lazy_codegen.machine_codegen_time_us));
		return;
	case JIT_COUNTER_LAZY_CODE_SIZE:
		output.Append(Value::UBIGINT(entry.jit_runtime.lazy_codegen.code_size));
		return;
	case JIT_COUNTER_HASH_JOIN_PROBE_LAYOUT:
		AppendExecutionRegionNullableString(output, entry.jit_runtime.hash_join_probe_layout);
		return;
	case JIT_COUNTER_JIT_RUNTIME_PATH_COUNTS:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionCounterBreakdown(entry.jit_runtime.runtime_path_counts));
		return;
	case JIT_COUNTER_JIT_RUNTIME_PROOF_COUNTS:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionCounterBreakdown(entry.jit_runtime.runtime_proof_counts));
		return;
	case JIT_COUNTER_JIT_RUNTIME_DELEGATION_COUNTS:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionCounterBreakdown(entry.jit_runtime.runtime_delegation_counts));
		return;
	case JIT_COUNTER_RUNNER_COST_SELECTED_ACCELERATED_RUNNER_COUNT:
		output.Append(Value::UBIGINT(entry.runner_cost.selected_accelerated_runner_count));
		return;
	case JIT_COUNTER_RUNNER_COST_SELECTED_COMPILED_VECTORIZED_RUNNER_COUNT:
		output.Append(Value::UBIGINT(entry.runner_cost.selected_compiled_vectorized_runner_count));
		return;
	case JIT_COUNTER_RUNNER_COST_SELECTED_GPU_RUNNER_COUNT:
		output.Append(Value::UBIGINT(entry.runner_cost.selected_gpu_runner_count));
		return;
	default:
		throw InternalException("Unsupported column index for duckdb_jit_counters");
	}
}

static unique_ptr<FunctionData> DuckDBJitCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	AddExecutionRegionTableFunctionColumn(return_types, names, "backend_name", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "kernel_id", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "status", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "execution_mode", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "selected_runner", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "blocker", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "count", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "decision_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "compile_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "code_size", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "input_rows", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "output_rows", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "invocation_count", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "runtime_time_us", LogicalType::BIGINT);
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
	AddExecutionRegionStageTimingColumns(return_types, names);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_codegen_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_machine_codegen_time_us", LogicalType::BIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "lazy_code_size", LogicalType::UBIGINT);
	AddExecutionRegionTableFunctionColumn(return_types, names, "hash_join_probe_layout", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "jit_runtime_path_counts", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "jit_runtime_proof_counts", LogicalType::VARCHAR);
	AddExecutionRegionTableFunctionColumn(return_types, names, "jit_runtime_delegation_counts", LogicalType::VARCHAR);
	AddJitCounterRunnerCostColumns(return_types, names);
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
