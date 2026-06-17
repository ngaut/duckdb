//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_kernel.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

class ExecutionRegionRuntime;

class ExecutionRegionCodeHandle {
public:
	virtual ~ExecutionRegionCodeHandle();

	virtual idx_t CodeSize() const;
};

class ExecutionRegionKernel {
public:
	virtual ~ExecutionRegionKernel();

	virtual const string &BackendName() const = 0;
	virtual idx_t CodeSize() const;
	virtual bool HasExecutableBody() const;
	void SetTraceInfo(idx_t trace_id, ExecutionRegionExecutionMode execution_mode, string compile_reason,
	                  int64_t compile_time_us, idx_t code_size);
	void SetTraceRegionExecutionForm(ExecutionRegionForm execution_form);
	void SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetTraceCandidate(const ExecutionRegionCandidate &candidate);
	idx_t TraceId() const;
	ExecutionRegionExecutionMode ExecutionMode() const;
	ExecutionRegionForm RegionExecutionForm() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	ExecutionRegionExecutionBody ExecutionBody() const;
	const string &TraceCompileReason() const;
	int64_t TraceCompileTime() const;
	idx_t TraceCodeSize() const;
	bool HasTraceCandidate() const;
	idx_t TraceCandidateId() const;
	const string &TraceCandidateShape() const;
	const string &TraceCandidatePipelineShape() const;
	const string &TraceCandidateContextPipelineShape() const;
	const ExecutionRegionSignature &TraceCandidateSignature() const;
	idx_t TraceCandidateNodeCount() const;
	idx_t TraceCandidateStartOperatorIndex() const;
	idx_t TraceCandidateEndOperatorIndex() const;
	idx_t TraceCandidateEstimatedCardinality() const;
	const ExecutionRegionCandidateTraits &TraceCandidateTraits() const;
	const ExecutionRegionContract &TraceCandidateContract() const;
	virtual bool CanExecuteFullPipeline() const;
	virtual bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result);

private:
	idx_t trace_id = 0;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::NONE;
	ExecutionRegionForm region_execution_form = ExecutionRegionForm::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE;
	string trace_compile_reason;
	int64_t trace_compile_time_us = 0;
	idx_t trace_code_size = 0;
	bool has_trace_candidate = false;
	idx_t trace_candidate_id = 0;
	string trace_candidate_shape;
	string trace_candidate_pipeline_shape;
	string trace_candidate_context_pipeline_shape;
	ExecutionRegionSignature trace_candidate_signature;
	idx_t trace_candidate_node_count = 0;
	idx_t trace_candidate_start_operator_index = 0;
	idx_t trace_candidate_end_operator_index = 0;
	idx_t trace_candidate_estimated_cardinality = 0;
	ExecutionRegionCandidateTraits trace_candidate_traits;
	ExecutionRegionContract trace_candidate_contract;
};

} // namespace duckdb
