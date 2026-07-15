//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_kernel.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <atomic>

namespace duckdb {

class Allocator;
class ExecutionRegionRuntime;

class DUCKDB_API ExecutionRegionLocalState {
public:
	virtual ~ExecutionRegionLocalState();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class DUCKDB_API ExecutionRegionCodeHandle {
public:
	virtual ~ExecutionRegionCodeHandle();

	virtual idx_t CodeSize() const;
};

class DUCKDB_API ExecutionRegionKernel : public TableFilterKernelProvider {
public:
	virtual ~ExecutionRegionKernel();

	virtual const string &BackendName() const = 0;
	virtual idx_t CodeSize() const;
	virtual bool HasExecutableBody() const;
	virtual unique_ptr<ExecutionRegionLocalState> CreateLocalState(Allocator &allocator) const;
	void SetTraceInfo(idx_t trace_id, ExecutionRegionExecutionMode execution_mode, string compile_reason,
	                  int64_t compile_time_us, idx_t code_size);
	void AddTraceCodeSize(idx_t code_size);
	void SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetTraceUsesScanFilters(bool uses_scan_filters);
	void SetTracePipeline(const ExecutionRegionCandidate &candidate);
	void SetExecutionABI(ExecutionRegionABI abi);
	idx_t TraceId() const;
	ExecutionRegionExecutionMode ExecutionMode() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	bool UsesScanFilters() const;
	ExecutionRegionABI ExecutionABI() const;
	const string &TraceCompileReason() const;
	int64_t TraceCompileTime() const;
	idx_t TraceCodeSize() const;
	bool HasTracePipeline() const;
	const string &TraceCandidateShape() const;
	const string &TraceCandidatePipelineShape() const;
	idx_t TraceCandidateEstimatedCardinality() const;
	bool TraceCandidateUsesScanFilters() const;
	virtual bool CanExecuteFullPipeline() const;
	virtual bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result);

private:
	idx_t trace_id = 0;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;
	ExecutionRegionABI execution_abi = ExecutionRegionABI::NONE;
	string trace_compile_reason;
	int64_t trace_compile_time_us = 0;
	idx_t trace_code_size = 0;
	std::atomic<idx_t> trace_lazy_code_size {0};
	bool has_trace_pipeline = false;
	string trace_candidate_shape;
	string trace_candidate_pipeline_shape;
	idx_t trace_candidate_estimated_cardinality = 0;
	bool trace_candidate_uses_scan_filters = false;
};

} // namespace duckdb
