#include "duckdb/execution/execution_region_runner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/parallel/execution_region_pipeline_adapter.hpp"
#include "duckdb/parallel/pipeline_execution.hpp"

#include <chrono>

namespace duckdb {

static int64_t ExecutionRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static string CompiledFullPipelineResultToString(ExecutionRegionResult result) {
	switch (result) {
	case ExecutionRegionResult::NOT_FINISHED:
		return "not_finished";
	case ExecutionRegionResult::FINISHED:
		return "finished";
	case ExecutionRegionResult::INTERRUPTED:
		return "interrupted";
	default:
		return "unknown";
	}
}

static string PipelineExecuteResultToString(PipelineExecuteResult result) {
	switch (result) {
	case PipelineExecuteResult::NOT_FINISHED:
		return "not_finished";
	case PipelineExecuteResult::FINISHED:
		return "finished";
	case PipelineExecuteResult::INTERRUPTED:
		return "interrupted";
	default:
		return "unknown";
	}
}

static PipelineExecuteResult CompiledFullPipelineResultToPipelineExecuteResult(ExecutionRegionResult result) {
	switch (result) {
	case ExecutionRegionResult::NOT_FINISHED:
		return PipelineExecuteResult::NOT_FINISHED;
	case ExecutionRegionResult::FINISHED:
		return PipelineExecuteResult::FINISHED;
	case ExecutionRegionResult::INTERRUPTED:
		return PipelineExecuteResult::INTERRUPTED;
	default:
		throw InternalException("Unknown compiled full pipeline result");
	}
}

static ExecutionRegionResult PipelineExecuteResultToCompiledFullPipelineResult(PipelineExecuteResult result) {
	switch (result) {
	case PipelineExecuteResult::NOT_FINISHED:
		return ExecutionRegionResult::NOT_FINISHED;
	case PipelineExecuteResult::FINISHED:
		return ExecutionRegionResult::FINISHED;
	case PipelineExecuteResult::INTERRUPTED:
		return ExecutionRegionResult::INTERRUPTED;
	default:
		throw InternalException("Unknown pipeline execution result");
	}
}

static string CompiledRegionExceptionMessage(const std::exception_ptr &error) {
	try {
		std::rethrow_exception(error);
	} catch (std::exception &ex) {
		return ex.what();
	} catch (...) {
		return "unknown non-standard exception";
	}
}

ExecutionRunner::~ExecutionRunner() {
}

ExecutionRunnerResult ExecutionRunnerResult::Executed(PipelineExecuteResult result) {
	ExecutionRunnerResult runner_result;
	runner_result.kind = ExecutionRunnerResultKind::EXECUTED;
	runner_result.result = result;
	return runner_result;
}

ExecutionRunnerResult ExecutionRunnerResult::ContinueVectorized() {
	ExecutionRunnerResult runner_result;
	runner_result.kind = ExecutionRunnerResultKind::CONTINUE_VECTORIZED;
	return runner_result;
}

ExecutionRunnerResult VectorizedRunner::Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) {
	auto &client = pipeline.GetClientContext();
	auto kernel = pipeline.GetExecutableFullPipelineKernel();
	if (!kernel || !ExecutionRegionSettings::TraceVectorizedBaseline(client) ||
	    !ExecutionRegionSettings::TraceRuntime(client)) {
		return ExecutionRunnerResult::Executed(pipeline.ExecuteVectorizedPipeline(max_chunks));
	}
	auto start = std::chrono::steady_clock::now();
	auto result = pipeline.ExecuteVectorizedPipeline(max_chunks);
	auto elapsed_us = ExecutionRegionElapsedMicros(start);
	ExecutionRegionManager::Get(client).RecordVectorizedBaselineRuntimeEvent(
	    client, *kernel, "vectorized baseline pipeline executed for execution-region runtime comparison", elapsed_us,
	    PipelineExecuteResultToString(result));
	return ExecutionRunnerResult::Executed(result);
}

ExecutionRunnerResult CompiledVectorizedRunner::Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) {
	PipelineExecuteResult result;
	auto status = ExecuteCompiledRegion(pipeline, max_chunks, result);
	switch (status) {
	case CompiledVectorizedRunStatus::EXECUTED:
		return ExecutionRunnerResult::Executed(result);
	case CompiledVectorizedRunStatus::VECTORIZED_SUPPRESSED:
	case CompiledVectorizedRunStatus::VECTORIZED_CONTINUATION:
	case CompiledVectorizedRunStatus::VECTORIZED_DEFERRED:
		return ExecutionRunnerResult::ContinueVectorized();
	default:
		throw InternalException("Unknown compiled vectorized runner status");
	}
}

ExecutionRunner &GetExecutionRunner(ExecutionRunnerKind kind) {
	static VectorizedRunner vectorized_runner;
	static CompiledVectorizedRunner compiled_vectorized_runner;
	switch (kind) {
	case ExecutionRunnerKind::VECTORIZED:
		return vectorized_runner;
	case ExecutionRunnerKind::COMPILED_VECTORIZED:
	case ExecutionRunnerKind::COMPILED_GPU:
		return compiled_vectorized_runner;
	default:
		throw InternalException("Unknown execution runner kind");
	}
}

PipelineExecuteResult ExecuteExecutionRunner(ExecutionRunnerKind kind, ExecutionRegionPipelineAdapter &pipeline,
                                             idx_t max_chunks) {
	auto &runner = GetExecutionRunner(kind);
	auto result = runner.Execute(pipeline, max_chunks);
	switch (result.kind) {
	case ExecutionRunnerResultKind::EXECUTED:
		return result.result;
	case ExecutionRunnerResultKind::CONTINUE_VECTORIZED:
		result = GetExecutionRunner(ExecutionRunnerKind::VECTORIZED).Execute(pipeline, max_chunks);
		if (result.kind != ExecutionRunnerResultKind::EXECUTED) {
			throw InternalException("Vectorized execution runner cannot request another runner");
		}
		return result.result;
	default:
		throw InternalException("Unknown execution runner result kind");
	}
}

class CompiledRegionRuntime : public ExecutionRegionRuntime, public ExecutionOperatorRuntime {
public:
	CompiledRegionRuntime(ExecutionRegionPipelineAdapter &pipeline_p, ExecutionRegionKernel &kernel_p,
	                      ExecutionRegionLocalState &local_state_p, idx_t max_chunks_p, bool trace_runtime_p)
	    : pipeline(pipeline_p), kernel(kernel_p), local_state(local_state_p), max_chunks(max_chunks_p),
	      trace_runtime(trace_runtime_p) {
	}

	idx_t MaxChunks() const override {
		return max_chunks;
	}

	idx_t MaxThreads() const override {
		return pipeline.MaxThreads();
	}

	Allocator &GetAllocator() override {
		return pipeline.GetAllocator();
	}

	ExecutionRegionLocalState &LocalState() override {
		return local_state;
	}

	ExecutionOperatorRuntime &ExecutionOperators() override {
		return *this;
	}

	bool TraceRuntime() const override {
		return trace_runtime;
	}

	void RecordGeneratedStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us, idx_t count) override {
		if (!trace_runtime) {
			return;
		}
		AddExecutionRegionStageRuntime(generated_stage_runtime, stage, runtime_time_us, count);
	}

	void RecordHashJoinProbeLayout(const char *layout) override {
		if (!trace_runtime || !layout || !layout[0]) {
			return;
		}
		auto &hash_join_probe_layout = jit_runtime.hash_join_probe_layout;
		if (hash_join_probe_layout.empty()) {
			hash_join_probe_layout = layout;
			return;
		}
		if (hash_join_probe_layout != layout) {
			hash_join_probe_layout = "mixed";
		}
	}

	void RecordJitRuntimePath(const char *path, idx_t count) override {
		if (!trace_runtime || !path || !path[0]) {
			return;
		}
		AddExecutionRegionRecordedCounter(jit_runtime.runtime_path_counts, ExecutionRegionStageId(path), count);
	}

	void RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof proof, idx_t count) override {
		if (!trace_runtime) {
			return;
		}
		AddExecutionRegionRecordedCounter(jit_runtime.runtime_proof_counts,
		                                  ExecutionRegionStageId(ExecutionRegionJitRuntimeProofName(proof)), count);
	}

	void RecordJitRuntimeDelegation(const char *delegation, idx_t count) override {
		if (!trace_runtime || !delegation || !delegation[0]) {
			return;
		}
		AddExecutionRegionRecordedCounter(jit_runtime.runtime_delegation_counts, ExecutionRegionStageId(delegation),
		                                  count);
	}

	void RecordLazyCodegen(const ExecutionRegionLazyCodegenMetrics &metrics) override {
		kernel.AddTraceCodeSize(metrics.code_size);
		if (!trace_runtime) {
			return;
		}
		AddExecutionRegionLazyCodegenMetrics(jit_runtime.lazy_codegen, metrics);
	}

	bool TryMarkOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index) override {
		return pipeline.TryMarkRuntimeOnce(flag, index);
	}

	void AddEntryElapsed(int64_t elapsed_us) {
		entry_elapsed_us += elapsed_us;
	}

	int64_t EntryElapsedMicros() const {
		return entry_elapsed_us;
	}

	SourceResultType FetchSourceContract(DataChunk *&result) override {
		if (sink_finished) {
			throw InternalException("compiled region runtime cannot fetch source contract data after a finished sink");
		}
		if (!trace_runtime) {
			return pipeline.FetchSourceContract(result);
		}
		ExecutionRegionSourceContractMetrics source_metrics;
		auto trace_start = std::chrono::steady_clock::now();
		auto source_result = pipeline.FetchSourceContract(result, &source_metrics);
		auto source_elapsed_us = ExecutionRegionElapsedMicros(trace_start);
		source_contract_output_rows += result ? result->size() : 0;
		source_contract_invocation_count++;
		source_contract_runtime_time_us += source_elapsed_us;
		RecordSourceMetrics(source_metrics);
		return source_result;
	}

	SourceResultType FetchPrimitiveAggregateStateSourceContract(ExecutionAggregateStateScanBatch *&result) override {
		if (sink_finished) {
			throw InternalException(
			    "compiled region runtime cannot fetch aggregate-state source data after a finished sink");
		}
		if (!trace_runtime) {
			return pipeline.FetchPrimitiveAggregateStateSourceContract(result);
		}
		ExecutionRegionSourceContractMetrics source_metrics;
		auto trace_start = std::chrono::steady_clock::now();
		auto source_result = pipeline.FetchPrimitiveAggregateStateSourceContract(result, &source_metrics);
		auto source_elapsed_us = ExecutionRegionElapsedMicros(trace_start);
		source_contract_output_rows += result ? result->Count() : 0;
		source_contract_invocation_count++;
		source_contract_runtime_time_us += source_elapsed_us;
		RecordSourceMetrics(source_metrics);
		return source_result;
	}

	ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                         const ExecutionRegionOperatorInfo &operator_info,
	                                         ExecutionOperatorBinding &binding) override {
		return pipeline.BindOperator(operator_index, input, operator_info, binding);
	}

	ExecutionOperatorReadiness GetOperatorReadiness(idx_t operator_index,
	                                                const ExecutionRegionOperatorInfo &operator_info) override {
		return pipeline.GetOperatorReadiness(operator_index, operator_info);
	}

	bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) override {
		ValidateSink();
		return pipeline.BindSink(input, sink_info, binding);
	}

	SinkResultType AppendSink(const ExecutionAppendSinkBinding &binding, DataChunk &chunk) override {
		ValidateSink();
		return pipeline.AppendSink(binding, chunk);
	}

	SinkResultType RecordSinkResult(idx_t input_rows, SinkResultType sink_result) override {
		ValidateSink();
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException(
			    "compiled full pipeline native sink update returned BLOCKED after entering sink state");
		}
		sink_input_rows += input_rows;
		if (sink_result == SinkResultType::FINISHED) {
			sink_finished = true;
			pipeline.FinishProcessing();
		}
		return sink_result;
	}

	idx_t SourceContractOutputRows() const {
		return source_contract_output_rows;
	}

	idx_t SinkInputRows() const {
		return sink_input_rows;
	}

	ExecutionRegionRuntimeMetrics Metrics(int64_t runtime_time_us) const {
		ExecutionRegionRuntimeMetrics result;
		result.source_contract_output_rows = source_contract_output_rows;
		result.source_contract_invocation_count = source_contract_invocation_count;
		result.source_contract_runtime_time_us = source_contract_runtime_time_us;
		result.source_stage_runtime = source_stage_runtime;
		result.generated_body_runtime_time_us =
		    runtime_time_us > source_contract_runtime_time_us ? runtime_time_us - source_contract_runtime_time_us : 0;
		result.generated_stage_runtime = generated_stage_runtime;
		result.jit_runtime = jit_runtime;
		return result;
	}

private:
	void ValidateSink() const {
		if (sink_finished) {
			throw InternalException("compiled region runtime cannot sink after a finished sink");
		}
		// Sink presence and local state are validated by the pipeline adapter.
	}

	void RecordSourceMetrics(const ExecutionRegionSourceContractMetrics &metrics) {
		AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.setup", metrics.setup_runtime_time_us);
		AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.start_operator",
		                               metrics.start_operator_runtime_time_us);
		if (metrics.get_data_stages.empty()) {
			AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.get_data",
			                               metrics.get_data_runtime_time_us);
		} else {
			for (auto &stage : metrics.get_data_stages) {
				AddExecutionRegionStageRuntime(source_stage_runtime, stage.stage, stage.runtime_time_us, stage.count);
			}
			auto get_data_stage_runtime_us = metrics.GetDataStageRuntimeSum();
			if (metrics.get_data_runtime_time_us > get_data_stage_runtime_us) {
				AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.get_data.unattributed",
				                               metrics.get_data_runtime_time_us - get_data_stage_runtime_us);
			}
		}
		AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.finish_source",
		                               metrics.finish_source_runtime_time_us);
		AddExecutionRegionStageRuntime(source_stage_runtime, "source_contract.end_operator",
		                               metrics.end_operator_runtime_time_us);
	}

private:
	//! A handle to the pipeline executor, held by value: a chunk step's runtime outlives the adapter of the entry
	//! that created it
	ExecutionRegionPipelineAdapter pipeline;
	ExecutionRegionKernel &kernel;
	ExecutionRegionLocalState &local_state;
	idx_t max_chunks;
	bool trace_runtime;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> source_stage_runtime;
	vector<ExecutionRegionRecordedStageRuntime> generated_stage_runtime;
	ExecutionRegionJitRuntimeMetrics jit_runtime;
	idx_t sink_input_rows = 0;
	bool sink_finished = false;
	//! The time a chunk step spent in the entries so far
	int64_t entry_elapsed_us = 0;
};

//! Whether the kernel can start: every native operator it delegates to is ready (or a debug setting forces the
//! vectorized loop). Asked before the first source fetch, where nothing is in flight yet
static bool CompiledEntryReady(ClientContext &client, ExecutionRegionKernel &kernel, ExecutionRegionRuntime &runtime,
                               string &blocker) {
	if (ExecutionRegionSettings::DebugForceEntryDefer(client)) {
		blocker = "debug forced defer at kernel entry";
		return false;
	}
	return kernel.EntryReady(runtime, blocker);
}

//! Leaves the pipeline to the vectorized loop for good: the latch keeps this executor vectorized
static CompiledVectorizedRunStatus DeferCompiledEntry(ExecutionRegionPipelineAdapter &pipeline,
                                                      ExecutionRegionKernel &kernel, const string &blocker,
                                                      bool trace_runtime,
                                                      std::chrono::steady_clock::time_point trace_start) {
	if (trace_runtime) {
		auto &client = pipeline.GetClientContext();
		ExecutionRegionManager::Get(client).RecordRuntimeEvent(client, kernel, ExecutionRegionEventStatus::SKIPPED,
		                                                       "full pipeline kernel deferred: " + blocker, 0, 0,
		                                                       ExecutionRegionElapsedMicros(trace_start), "deferred");
	}
	pipeline.LatchDeferredCompiledExecution();
	pipeline.AddProfilingAnnotation("jit_deferred", blocker, false);
	return CompiledVectorizedRunStatus::VECTORIZED_DEFERRED;
}

static void RecordCompiledRegionError(ExecutionRegionPipelineAdapter &pipeline, ExecutionRegionKernel &kernel,
                                      std::chrono::steady_clock::time_point trace_start) {
	auto &client = pipeline.GetClientContext();
	ExecutionRegionManager::Get(client).RecordRuntimeEvent(client, kernel, ExecutionRegionEventStatus::ERROR,
	                                                       "full pipeline kernel threw: " +
	                                                           CompiledRegionExceptionMessage(std::current_exception()),
	                                                       0, 0, ExecutionRegionElapsedMicros(trace_start), "error");
}

//! A chunk-step kernel runs inside the executor's own loop, which fetches, advances batches, retries a blocked sink
//! and finalizes; any executor state is a legal point to continue from. The step stays bound to the executor across
//! entries, and one trace event covers all of them
static CompiledVectorizedRunStatus ExecuteCompiledStepRegion(ExecutionRegionPipelineAdapter &pipeline,
                                                             ExecutionRegionKernel &kernel, idx_t max_chunks,
                                                             PipelineExecuteResult &result) {
	auto &client = pipeline.GetClientContext();
	auto trace_runtime = ExecutionRegionSettings::TraceRuntime(client);
	auto trace_start = trace_runtime ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	auto bound_runtime = pipeline.CompiledStepRuntime(kernel);
	if (!bound_runtime) {
		auto &local_state = pipeline.GetOrCreateLocalState(kernel);
		auto runtime = make_uniq<CompiledRegionRuntime>(pipeline, kernel, local_state, max_chunks, trace_runtime);
		string blocker;
		if (!CompiledEntryReady(client, kernel, *runtime, blocker)) {
			return DeferCompiledEntry(pipeline, kernel, blocker, trace_runtime, trace_start);
		}
		auto step = kernel.CreateChunkStep(*runtime);
		bound_runtime = runtime.get();
		pipeline.BindCompiledStep(kernel, std::move(runtime), std::move(step));
	}
	auto &runtime = bound_runtime->Cast<CompiledRegionRuntime>();
	try {
		{
			ExecutionRegionSuppressionGuard guard(client);
			result = pipeline.ExecuteVectorizedPipeline(max_chunks);
		}
		pipeline.AddProfilingAnnotation("jit", "compiled full-pipeline kernel (" + kernel.BackendName() + ")", true);
		if (trace_runtime) {
			runtime.AddEntryElapsed(ExecutionRegionElapsedMicros(trace_start));
			// The executor yields between entries (budget, a blocked source or sink); the step's one event counts them
			runtime.RecordJitRuntimePath("chunk_step.entry", 1);
			if (result == PipelineExecuteResult::INTERRUPTED) {
				runtime.RecordJitRuntimePath("chunk_step.entry.interrupted", 1);
			}
			if (result == PipelineExecuteResult::FINISHED) {
				auto elapsed_us = runtime.EntryElapsedMicros();
				auto compiled_result = PipelineExecuteResultToCompiledFullPipelineResult(result);
				ExecutionRegionManager::Get(client).RecordRuntimeEvent(
				    client, kernel, ExecutionRegionEventStatus::EXECUTED, "full pipeline kernel executed",
				    runtime.SourceContractOutputRows(), runtime.SinkInputRows(), elapsed_us,
				    CompiledFullPipelineResultToString(compiled_result), runtime.Metrics(elapsed_us));
			}
		}
		return CompiledVectorizedRunStatus::EXECUTED;
	} catch (...) {
		if (trace_runtime) {
			RecordCompiledRegionError(pipeline, kernel, trace_start);
		}
		throw;
	}
}

//! A kernel whose source is not chunks (the aggregate-state source recipe) fetches from the source contract itself
//! for one entry's chunk budget. It neither sinks chunks nor advances batches, so the executor's sink protocol never
//! runs inside it; once it finishes, the executor flushes and finalizes the sink as the vectorized loop would
static CompiledVectorizedRunStatus ExecuteCompiledLoopRegion(ExecutionRegionPipelineAdapter &pipeline,
                                                             ExecutionRegionKernel &kernel, idx_t max_chunks,
                                                             PipelineExecuteResult &result) {
	auto &client = pipeline.GetClientContext();
	auto trace_runtime = ExecutionRegionSettings::TraceRuntime(client);
	auto trace_start = trace_runtime ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	if (!pipeline.IsAtCleanSourceToSinkBoundary()) {
		// The kernel finished and the sink's finalization yielded: the vectorized loop completes it
		if (trace_runtime) {
			ExecutionRegionManager::Get(client).RecordRuntimeEvent(
			    client, kernel, ExecutionRegionEventStatus::SKIPPED,
			    "full pipeline kernel not entered because executor state is not at a clean source-to-sink boundary", 0,
			    0, 0, "boundary");
		}
		return CompiledVectorizedRunStatus::VECTORIZED_CONTINUATION;
	}
	auto &local_state = pipeline.GetOrCreateLocalState(kernel);
	CompiledRegionRuntime runtime(pipeline, kernel, local_state, max_chunks, trace_runtime);
	string blocker;
	if (pipeline.SourceCursorUntouched() && !CompiledEntryReady(client, kernel, runtime, blocker)) {
		return DeferCompiledEntry(pipeline, kernel, blocker, trace_runtime, trace_start);
	}
	try {
		auto compiled_result = ExecutionRegionResult::NOT_FINISHED;
		string runtime_reason = "full pipeline kernel executed";
		{
			ExecutionRegionSuppressionGuard guard(client);
			if (!kernel.TryExecuteFullPipeline(runtime, compiled_result)) {
				throw InternalException("compiled full pipeline kernel returned false at runtime");
			}
			result = CompiledFullPipelineResultToPipelineExecuteResult(compiled_result);
			if (compiled_result == ExecutionRegionResult::FINISHED) {
				result = pipeline.FlushAndFinalizeAfterCompiledFinish(max_chunks, runtime_reason);
				compiled_result = PipelineExecuteResultToCompiledFullPipelineResult(result);
			}
		}
		pipeline.AddProfilingAnnotation("jit", "compiled full-pipeline kernel (" + kernel.BackendName() + ")", true);
		if (trace_runtime) {
			auto elapsed_us = ExecutionRegionElapsedMicros(trace_start);
			ExecutionRegionManager::Get(client).RecordRuntimeEvent(
			    client, kernel, ExecutionRegionEventStatus::EXECUTED, std::move(runtime_reason),
			    runtime.SourceContractOutputRows(), runtime.SinkInputRows(), elapsed_us,
			    CompiledFullPipelineResultToString(compiled_result), runtime.Metrics(elapsed_us));
		}
		return CompiledVectorizedRunStatus::EXECUTED;
	} catch (...) {
		if (trace_runtime) {
			RecordCompiledRegionError(pipeline, kernel, trace_start);
		}
		throw;
	}
}

CompiledVectorizedRunStatus CompiledVectorizedRunner::ExecuteCompiledRegion(ExecutionRegionPipelineAdapter &pipeline,
                                                                            idx_t max_chunks,
                                                                            PipelineExecuteResult &result) {
	if (pipeline.IsCompiledExecutionSuppressed()) {
		return CompiledVectorizedRunStatus::VECTORIZED_SUPPRESSED;
	}
	if (!pipeline.HasSourceAndSink()) {
		throw InternalException("Compiled vectorized runner selected for a pipeline without a source and sink");
	}
	auto kernel = pipeline.GetExecutableFullPipelineKernel();
	if (!kernel) {
		throw InternalException("Compiled vectorized runner selected without an executable full-pipeline kernel");
	}
	if (kernel->SupportsChunkStep()) {
		return ExecuteCompiledStepRegion(pipeline, *kernel, max_chunks, result);
	}
	return ExecuteCompiledLoopRegion(pipeline, *kernel, max_chunks, result);
}

} // namespace duckdb
