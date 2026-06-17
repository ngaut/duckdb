//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_telemetry.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_ir.hpp"

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
struct ExecutionContext;
struct OperatorSinkInput;

struct ExecutionRegionEvent {
	idx_t event_id = 0;
	bool has_pipeline = false;
	string pipeline_shape;
	idx_t pipeline_estimated_cardinality = 0;
	bool has_candidate = false;
	idx_t candidate_id = 0;
	string candidate_shape;
	string candidate_pipeline_shape;
	string candidate_context_pipeline_shape;
	ExecutionRegionSignature candidate_signature;
	idx_t candidate_node_count = 0;
	idx_t candidate_start_operator_index = 0;
	idx_t candidate_end_operator_index = 0;
	idx_t candidate_estimated_cardinality = 0;
	ExecutionRegionCandidateTraits candidate_traits;
	ExecutionRegionContract candidate_contract;
	bool has_admission = false;
	string admission_shape_key;
	bool admission_rule_present = false;
	idx_t admission_min_cardinality = 0;
	string admission_proof;
	bool has_admission_score = false;
	int64_t admission_score = 0;
	string phase;
	string backend_name;
	string target;
	string status;
	string execution_mode;
	string region_execution_form;
	string execution_body;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	string policy_decision;
	string reason;
	string ir;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	idx_t kernel_id = 0;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	string generated_stage_runtime_breakdown;
	string runtime_result;
	string kernel_compile_reason;
	int64_t kernel_compile_time_us = 0;
	idx_t kernel_code_size = 0;
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
};

struct ExecutionRegionCounter {
	string backend_name;
	string target;
	string status;
	string execution_mode;
	string region_execution_form;
	string execution_body;
	string policy_decision;
	idx_t count = 0;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	string generated_stage_runtime_breakdown;
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
};

struct ExecutionRegionDecisionCounter {
	string backend_name;
	string target;
	string phase;
	string status;
	string execution_mode;
	string region_execution_form;
	string execution_body;
	string policy_decision;
	bool has_pipeline = false;
	string pipeline_shape;
	idx_t pipeline_estimated_cardinality = 0;
	string candidate_shape;
	ExecutionRegionSignature candidate_signature;
	string admission_shape_key;
	bool admission_rule_present = false;
	idx_t admission_min_cardinality = 0;
	string admission_proof;
	bool has_admission_score = false;
	int64_t min_admission_score = 0;
	int64_t max_admission_score = 0;
	ExecutionRegionCandidateTraits candidate_traits;
	ExecutionRegionContract candidate_contract;
	idx_t count = 0;
	idx_t max_estimated_cardinality = 0;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
	string example_reason;
};

struct ExecutionRegionKernelCounter {
	idx_t kernel_id = 0;
	string backend_name;
	string target;
	string execution_mode;
	string region_execution_form;
	string execution_body;
	bool has_candidate = false;
	idx_t candidate_id = 0;
	string candidate_shape;
	string candidate_pipeline_shape;
	string candidate_context_pipeline_shape;
	ExecutionRegionSignature candidate_signature;
	idx_t candidate_node_count = 0;
	idx_t candidate_start_operator_index = 0;
	idx_t candidate_end_operator_index = 0;
	idx_t candidate_estimated_cardinality = 0;
	ExecutionRegionCandidateTraits candidate_traits;
	ExecutionRegionContract candidate_contract;
	string compile_reason;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	string last_runtime_status;
	string last_runtime_result;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	string generated_stage_runtime_breakdown;
};

class ExecutionRegionSuppressionGuard {
public:
	explicit ExecutionRegionSuppressionGuard(ClientContext &context);
	~ExecutionRegionSuppressionGuard();

private:
	ClientContext &context;
};

class ExecutionRegionEventLog {
public:
	idx_t Record(idx_t event_log_size, bool record_decision_counter, ExecutionRegionEvent event);
	vector<ExecutionRegionEvent> GetEvents() const;
	vector<ExecutionRegionCounter> GetCounters() const;
	vector<ExecutionRegionDecisionCounter> GetDecisionCounters() const;
	vector<ExecutionRegionKernelCounter> GetKernelCounters() const;
	void ClearEvents();
	void ClearCounters();
	void ApplyRetentionLimit(idx_t event_log_size);

private:
	void RecordCounter(const ExecutionRegionEvent &event);
	void RecordDecisionCounter(const ExecutionRegionEvent &event);
	void RecordKernelCounter(idx_t kernel_counter_log_size, const ExecutionRegionEvent &event);
	void AccumulateKernelRuntime(ExecutionRegionKernelCounter &counter, const ExecutionRegionEvent &event);
	void TrimEvents(idx_t event_log_size);
	void TrimKernelCounters(idx_t kernel_counter_log_size);

private:
	mutable mutex lock;
	vector<ExecutionRegionEvent> events;
	vector<ExecutionRegionCounter> counters;
	vector<ExecutionRegionDecisionCounter> decision_counters;
	vector<ExecutionRegionKernelCounter> kernel_counters;
	idx_t next_event_id = 1;
};

} // namespace duckdb
