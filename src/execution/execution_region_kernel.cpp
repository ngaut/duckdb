#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionCodeHandle::~ExecutionRegionCodeHandle() {
}

idx_t ExecutionRegionCodeHandle::CodeSize() const {
	return 0;
}

ExecutionRegionKernel::~ExecutionRegionKernel() {
}

idx_t ExecutionRegionKernel::CodeSize() const {
	return 0;
}

bool ExecutionRegionKernel::HasExecutableBody() const {
	return CodeSize() > 0;
}

void ExecutionRegionKernel::SetTraceInfo(idx_t trace_id_p, ExecutionRegionExecutionMode execution_mode_p,
                                         string compile_reason, int64_t compile_time_us, idx_t code_size) {
	trace_id = trace_id_p;
	execution_mode = execution_mode_p;
	execution_body = ExecutionRegionExecutionBodyForCompileEvent(ExecutionRegionCompileStatus::COMPILED,
	                                                             execution_mode_p, code_size);
	trace_compile_reason = std::move(compile_reason);
	trace_compile_time_us = compile_time_us;
	trace_code_size = code_size;
}

void ExecutionRegionKernel::SetTraceRegionExecutionForm(ExecutionRegionForm execution_form_p) {
	region_execution_form = execution_form_p;
}

void ExecutionRegionKernel::SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void ExecutionRegionKernel::SetTraceCandidate(const ExecutionRegionCandidate &candidate) {
	has_trace_candidate = true;
	trace_candidate_id = candidate.candidate_id;
	trace_candidate_shape = candidate.shape;
	trace_candidate_pipeline_shape = candidate.pipeline_shape;
	trace_candidate_context_pipeline_shape = candidate.context_pipeline_shape;
	trace_candidate_signature = candidate.signature;
	trace_candidate_node_count = candidate.node_count;
	trace_candidate_start_operator_index = candidate.start_operator_index;
	trace_candidate_end_operator_index = candidate.end_operator_index;
	trace_candidate_estimated_cardinality = candidate.estimated_cardinality;
	trace_candidate_traits = candidate.traits;
	trace_candidate_contract = candidate.contract;
}

idx_t ExecutionRegionKernel::TraceId() const {
	return trace_id;
}

ExecutionRegionExecutionMode ExecutionRegionKernel::ExecutionMode() const {
	return execution_mode;
}

ExecutionRegionForm ExecutionRegionKernel::RegionExecutionForm() const {
	return region_execution_form;
}

ExecutionRegionSourceExecutionKind ExecutionRegionKernel::SelectedSourceExecution() const {
	return selected_source_execution;
}

ExecutionRegionExecutionBody ExecutionRegionKernel::ExecutionBody() const {
	return execution_body;
}

const string &ExecutionRegionKernel::TraceCompileReason() const {
	return trace_compile_reason;
}

int64_t ExecutionRegionKernel::TraceCompileTime() const {
	return trace_compile_time_us;
}

idx_t ExecutionRegionKernel::TraceCodeSize() const {
	return trace_code_size;
}

bool ExecutionRegionKernel::HasTraceCandidate() const {
	return has_trace_candidate;
}

idx_t ExecutionRegionKernel::TraceCandidateId() const {
	return trace_candidate_id;
}

const string &ExecutionRegionKernel::TraceCandidateShape() const {
	return trace_candidate_shape;
}

const string &ExecutionRegionKernel::TraceCandidatePipelineShape() const {
	return trace_candidate_pipeline_shape;
}

const string &ExecutionRegionKernel::TraceCandidateContextPipelineShape() const {
	return trace_candidate_context_pipeline_shape;
}

const ExecutionRegionSignature &ExecutionRegionKernel::TraceCandidateSignature() const {
	return trace_candidate_signature;
}

idx_t ExecutionRegionKernel::TraceCandidateNodeCount() const {
	return trace_candidate_node_count;
}

idx_t ExecutionRegionKernel::TraceCandidateStartOperatorIndex() const {
	return trace_candidate_start_operator_index;
}

idx_t ExecutionRegionKernel::TraceCandidateEndOperatorIndex() const {
	return trace_candidate_end_operator_index;
}

idx_t ExecutionRegionKernel::TraceCandidateEstimatedCardinality() const {
	return trace_candidate_estimated_cardinality;
}

const ExecutionRegionCandidateTraits &ExecutionRegionKernel::TraceCandidateTraits() const {
	return trace_candidate_traits;
}

const ExecutionRegionContract &ExecutionRegionKernel::TraceCandidateContract() const {
	return trace_candidate_contract;
}

bool ExecutionRegionKernel::CanExecuteFullPipeline() const {
	return false;
}

bool ExecutionRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) {
	return false;
}

} // namespace duckdb
