#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution_region_table_function_utils.hpp"

namespace duckdb {

struct DuckDBJitEventsData : public GlobalTableFunctionState {
	vector<ExecutionRegionEvent> events;
	idx_t offset = 0;
};

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
	names.emplace_back("policy_decision");
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
	names.emplace_back("generated_body_runtime_time_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("generated_stage_runtime_breakdown");
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
	names.emplace_back("admission_shape_key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("admission_rule_present");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("admission_min_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("admission_score");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("admission_proof");
	return_types.emplace_back(LogicalType::VARCHAR);
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
	names.emplace_back("candidate_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_context_pipeline_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
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
	result->events = ExecutionRegionManager::Get(context).GetEvents();
	return std::move(result);
}

static void DuckDBJitEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitEventsData>();
	idx_t count = 0;
	while (data.offset < data.events.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.events[data.offset++];
		idx_t col = 0;
		output.data[col++].Append(Value::UBIGINT(entry.event_id));
		output.data[col++].Append(Value(entry.backend_name));
		output.data[col++].Append(Value(entry.target));
		output.data[col++].Append(Value(entry.status));
		output.data[col++].Append(Value(entry.execution_mode));
		output.data[col++].Append(Value(entry.region_execution_form));
		output.data[col++].Append(Value(entry.execution_body));
		output.data[col++].Append(Value(ExecutionRegionSourceExecutionKindToString(entry.selected_source_execution)));
		output.data[col++].Append(Value(entry.policy_decision));
		output.data[col++].Append(Value(entry.reason));
		if (entry.ir.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.ir));
		}
		output.data[col++].Append(Value::BIGINT(entry.decision_time_us));
		output.data[col++].Append(Value::BIGINT(entry.compile_time_us));
		output.data[col++].Append(Value::UBIGINT(entry.code_size));
		output.data[col++].Append(Value(entry.phase));
		output.data[col++].Append(Value::UBIGINT(entry.kernel_id));
		output.data[col++].Append(Value::UBIGINT(entry.input_rows));
		output.data[col++].Append(Value::UBIGINT(entry.output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.runtime_time_us));
		if (entry.runtime_result.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.runtime_result));
		}
		output.data[col++].Append(Value::UBIGINT(entry.source_contract_output_rows));
		output.data[col++].Append(Value::UBIGINT(entry.source_contract_invocation_count));
		output.data[col++].Append(Value::BIGINT(entry.source_contract_runtime_time_us));
		output.data[col++].Append(Value::BIGINT(entry.generated_body_runtime_time_us));
		if (entry.generated_stage_runtime_breakdown.empty()) {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		} else {
			output.data[col++].Append(Value(entry.generated_stage_runtime_breakdown));
		}
		if (entry.has_candidate) {
			output.data[col++].Append(Value::UBIGINT(entry.candidate_id));
			output.data[col++].Append(Value(entry.candidate_shape));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_node_count));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_estimated_cardinality));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_start_operator_index));
			output.data[col++].Append(Value::UBIGINT(entry.candidate_end_operator_index));
		} else {
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::VARCHAR));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
		}
		if (entry.has_admission) {
			if (entry.admission_shape_key.empty()) {
				output.data[col++].Append(Value(LogicalType::VARCHAR));
			} else {
				output.data[col++].Append(Value(entry.admission_shape_key));
			}
			output.data[col++].Append(Value::BOOLEAN(entry.admission_rule_present));
			if (entry.admission_rule_present) {
				output.data[col++].Append(Value::UBIGINT(entry.admission_min_cardinality));
				if (entry.has_admission_score) {
					output.data[col++].Append(Value::BIGINT(entry.admission_score));
				} else {
					output.data[col++].Append(Value(LogicalType::BIGINT));
				}
				if (entry.admission_proof.empty()) {
					output.data[col++].Append(Value(LogicalType::VARCHAR));
				} else {
					output.data[col++].Append(Value(entry.admission_proof));
				}
			} else {
				output.data[col++].Append(Value(LogicalType::UBIGINT));
				output.data[col++].Append(Value(LogicalType::BIGINT));
				output.data[col++].Append(Value(LogicalType::VARCHAR));
			}
		} else {
			output.data[col++].Append(Value(LogicalType::VARCHAR));
			output.data[col++].Append(Value(LogicalType::BOOLEAN));
			output.data[col++].Append(Value(LogicalType::UBIGINT));
			output.data[col++].Append(Value(LogicalType::BIGINT));
			output.data[col++].Append(Value(LogicalType::VARCHAR));
		}
		output.data[col++].Append(Value::BIGINT(entry.ir_lowering_time_us));
		output.data[col++].Append(Value::BIGINT(entry.backend_analysis_time_us));
		output.data[col++].Append(Value::BIGINT(entry.admission_time_us));
		output.data[col++].Append(Value::BIGINT(entry.overlap_check_time_us));
		output.data[col++].Append(Value::BIGINT(entry.codegen_time_us));
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
		col = entry.has_candidate
		          ? AppendExecutionRegionCandidateTraceColumns(output, col, entry.candidate_signature,
		                                                       entry.candidate_traits, entry.candidate_contract)
		          : AppendNullExecutionRegionCandidateTraceColumns(output, col);
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
		count++;
	}
}

void DuckDBJitEventsFun::RegisterFunction(BuiltinFunctions &set) {
	auto function =
	    TableFunction("duckdb_jit_events", {}, DuckDBJitEventsFunction, DuckDBJitEventsBind, DuckDBJitEventsInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
