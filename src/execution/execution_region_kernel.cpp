#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionLocalState::~ExecutionRegionLocalState() {
}

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

unique_ptr<ExecutionRegionLocalState> ExecutionRegionKernel::CreateLocalState(Allocator &) const {
	return make_uniq<ExecutionRegionLocalState>();
}

void ExecutionRegionKernel::SetTraceInfo(idx_t trace_id_p, ExecutionRegionExecutionMode execution_mode_p,
                                         string compile_reason, int64_t compile_time_us, idx_t code_size) {
	trace_id = trace_id_p;
	execution_mode = execution_mode_p;
	trace_compile_reason = std::move(compile_reason);
	trace_compile_time_us = compile_time_us;
	trace_code_size = code_size;
	trace_lazy_code_size.store(0, std::memory_order_relaxed);
}

void ExecutionRegionKernel::AddTraceCodeSize(idx_t code_size) {
	trace_lazy_code_size.fetch_add(code_size, std::memory_order_relaxed);
}

void ExecutionRegionKernel::SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void ExecutionRegionKernel::SetTraceUsesScanFilters(bool uses_scan_filters_p) {
	uses_scan_filters = uses_scan_filters_p;
}

void ExecutionRegionKernel::SetTracePipeline(const ExecutionRegionCandidate &candidate) {
	has_trace_pipeline = true;
	trace_candidate_shape = candidate.shape;
	trace_candidate_pipeline_shape = candidate.pipeline_shape;
	trace_candidate_estimated_cardinality = candidate.estimated_cardinality;
	trace_candidate_uses_scan_filters = candidate.uses_scan_filters;
}

void ExecutionRegionKernel::SetExecutionABI(ExecutionRegionABI abi) {
	execution_abi = abi;
}

idx_t ExecutionRegionKernel::TraceId() const {
	return trace_id;
}

ExecutionRegionExecutionMode ExecutionRegionKernel::ExecutionMode() const {
	return execution_mode;
}

ExecutionRegionSourceExecutionKind ExecutionRegionKernel::SelectedSourceExecution() const {
	return selected_source_execution;
}

bool ExecutionRegionKernel::UsesScanFilters() const {
	return uses_scan_filters;
}

ExecutionRegionABI ExecutionRegionKernel::ExecutionABI() const {
	return execution_abi;
}

const string &ExecutionRegionKernel::TraceCompileReason() const {
	return trace_compile_reason;
}

int64_t ExecutionRegionKernel::TraceCompileTime() const {
	return trace_compile_time_us;
}

idx_t ExecutionRegionKernel::TraceCodeSize() const {
	return trace_code_size + trace_lazy_code_size.load(std::memory_order_relaxed);
}

bool ExecutionRegionKernel::HasTracePipeline() const {
	return has_trace_pipeline;
}

const string &ExecutionRegionKernel::TraceCandidateShape() const {
	return trace_candidate_shape;
}

const string &ExecutionRegionKernel::TraceCandidatePipelineShape() const {
	return trace_candidate_pipeline_shape;
}

idx_t ExecutionRegionKernel::TraceCandidateEstimatedCardinality() const {
	return trace_candidate_estimated_cardinality;
}

bool ExecutionRegionKernel::TraceCandidateUsesScanFilters() const {
	return trace_candidate_uses_scan_filters;
}

bool ExecutionRegionKernel::CanExecuteFullPipeline() const {
	return false;
}

bool ExecutionRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) {
	return false;
}

} // namespace duckdb
