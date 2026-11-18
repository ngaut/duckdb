//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_kernel.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <atomic>

namespace duckdb {

class Allocator;
class DataChunk;
class ExecutionRegionRuntime;
struct SelectionVector;
enum class SinkResultType : uint8_t;

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

//! Columns of a source chunk the source has not read: a consumer that reads them only for the rows its filter keeps
//! asks for those rows, and every other consumer for all rows, before anything reads them
class DUCKDB_API ExecutionDeferredColumns {
public:
	virtual ~ExecutionDeferredColumns();

	//! Reads the columns for the selected rows of the chunk, or for every row without a selection, into the rows'
	//! positions. The columns of other rows stay undefined
	virtual void Read(DataChunk &chunk, optional_ptr<const SelectionVector> selection, idx_t count) = 0;
	//! The source reads every column of its next chunks itself
	virtual void StopDeferring() = 0;

	void ReadAll(DataChunk &chunk);
};

//! What the source proves about every row of a chunk it hands a chunk step
struct ExecutionSourceChunkProof {
	//! The zone maps of the row groups the rows come from prove each table filter for every row: the filters need no
	//! row-level check
	bool table_filters_hold = false;
	//! The chunk's columns the source has not read yet
	optional_ptr<ExecutionDeferredColumns> deferred_columns;

	//! The proof of chunks merged into one: their deferred columns were read before the merge
	ExecutionSourceChunkProof Merge(const ExecutionSourceChunkProof &other) const {
		return {table_filters_hold && other.table_filters_hold, nullptr};
	}
};

//! A chunk-step kernel bound to one pipeline executor: the executor's loop hands it every source chunk, and it keeps
//! between chunks the rows its operators batch
class DUCKDB_API ExecutionRegionChunkStep {
public:
	virtual ~ExecutionRegionChunkStep();

	//! Runs one source chunk through the compiled operators. Output the sink blocks on queues in the executor, so the
	//! chunk always runs to completion. Returns FINISHED once the sink needs no more input, else NEED_MORE_INPUT
	virtual SinkResultType ExecuteChunk(DataChunk &input, ExecutionSourceChunkProof proof) = 0;
	//! The next chunk moves a partitioned sink to another partition: every row the step holds sinks first, under the
	//! partition it belongs to
	virtual void FlushChunks() = 0;
	//! The source is exhausted: every row the step still holds reaches the sink
	virtual void FinishChunks() = 0;
	//! The chunk DuckDB's own operator chain takes in place of a source chunk once the step hands the pipeline to
	//! it, or null while the step runs the chunks itself. A step whose sink keeps its state apart from its own
	//! batches may alternate chunk by chunk; it hands over only after flushing every row it held
	virtual optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, ExecutionSourceChunkProof proof);
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
	void SetTraceInfo(idx_t trace_id, ExecutionRegionExecutionMode execution_mode, int64_t compile_time_us,
	                  idx_t code_size);
	void AddTraceCodeSize(idx_t code_size);
	void SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetTraceScanFilterMode(ExecutionRegionScanFilterMode scan_filter_mode);
	void SetTracePipeline(const ExecutionRegionCandidate &candidate, const string &pipeline_shape);
	idx_t TraceId() const;
	ExecutionRegionExecutionMode ExecutionMode() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	bool UsesScanFilters() const;
	int64_t TraceCompileTime() const;
	idx_t TraceCodeSize() const;
	bool HasTracePipeline() const;
	const string &TraceCandidateShape() const;
	const string &TracePipelineShape() const;
	idx_t TraceCandidateEstimatedCardinality() const;
	//! Chunk-step execution, the way every kernel over source chunks runs. The pipeline executor owns the loop: it
	//! fetches the source (through the source contract the plan opened), advances the sink's batches, retries a
	//! blocked sink and finalizes. The kernel replaces the operator chain and, where it owns one, the sink update.
	virtual bool SupportsChunkStep() const;
	//! Whether every native operator the kernel delegates to is ready; asked before the first source fetch, where
	//! the pipeline can still run vectorized instead
	virtual bool EntryReady(ExecutionRegionRuntime &runtime, string &blocker);
	//! Binds the kernel to one pipeline executor, which keeps the step (and the runtime it calls back through) until
	//! the executor resets
	virtual unique_ptr<ExecutionRegionChunkStep> CreateChunkStep(ExecutionRegionRuntime &runtime);
	//! A kernel whose source is not chunks (an aggregate-state scan) fetches from the source contract itself, up to
	//! the runtime's chunk budget per entry, and reports how the pipeline stopped
	virtual bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result);

private:
	idx_t trace_id = 0;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionScanFilterMode scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
	int64_t trace_compile_time_us = 0;
	idx_t trace_code_size = 0;
	std::atomic<idx_t> trace_lazy_code_size {0};
	bool has_trace_pipeline = false;
	string trace_candidate_shape;
	string trace_pipeline_shape;
	idx_t trace_candidate_estimated_cardinality = 0;
};

} // namespace duckdb
