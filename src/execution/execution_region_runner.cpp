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
	case ExecutionRegionResult::DEFERRED:
		return "deferred";
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
	case ExecutionRegionResult::DEFERRED:
		throw InternalException("Deferred compiled full pipeline result cannot be converted to a pipeline result");
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
	CompiledRegionRuntime(ExecutionRegionPipelineAdapter &pipeline_p, idx_t max_chunks_p, bool trace_runtime_p)
	    : pipeline(pipeline_p), max_chunks(max_chunks_p), trace_runtime(trace_runtime_p) {
	}

	idx_t MaxChunks() const override {
		return max_chunks;
	}

	Allocator &GetAllocator() override {
		return pipeline.GetAllocator();
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

	void RecordJitMaterializationBoundary(const char *boundary, idx_t count) override {
		if (!trace_runtime || !boundary || !boundary[0]) {
			return;
		}
		AddExecutionRegionRecordedCounter(jit_runtime.materialization_boundary_counts, ExecutionRegionStageId(boundary),
		                                  count);
	}

	void RecordLazyCodegen(const ExecutionRegionLazyCodegenMetrics &metrics) override {
		if (!trace_runtime) {
			return;
		}
		AddExecutionRegionLazyCodegenMetrics(jit_runtime.lazy_codegen, metrics);
	}

	bool TryMarkOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index) override {
		return pipeline.TryMarkRuntimeOnce(flag, index);
	}

	void Defer(string reason) override {
		deferred_reason = std::move(reason);
	}

	const string &DeferredReason() const override {
		return deferred_reason;
	}

	SourceResultType FetchSourceContract(DataChunk *&result) override {
		if (sink_blocked) {
			throw InternalException("compiled region runtime cannot fetch source contract data after a blocked sink");
		}
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

	SinkNextBatchType AdvanceSinkBatch(DataChunk &source_chunk, bool have_more_output) override {
		if (sink_blocked) {
			throw InternalException("compiled region runtime cannot advance sink batch after a blocked sink");
		}
		if (sink_finished) {
			throw InternalException("compiled region runtime cannot advance sink batch after a finished sink");
		}
		if (!trace_runtime) {
			return pipeline.AdvanceSinkBatch(source_chunk, have_more_output);
		}
		auto trace_start = std::chrono::steady_clock::now();
		auto result = pipeline.AdvanceSinkBatch(source_chunk, have_more_output);
		sink_next_batch_runtime_time_us += ExecutionRegionElapsedMicros(trace_start);
		sink_next_batch_invocation_count++;
		return result;
	}

	optional_ptr<DataChunk> PendingSourceContractBatch() override {
		return pipeline.PendingSourceContractBatch();
	}

	DataChunk &PrepareSourceContractBatch(const vector<LogicalType> &types) override {
		return pipeline.PrepareSourceContractBatch(types);
	}

	void ResetSourceContractBatch() override {
		pipeline.ResetSourceContractBatch();
	}

	ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                         const ExecutionRegionOperatorInfo &operator_info,
	                                         ExecutionOperatorBinding &binding) override {
		return pipeline.BindOperator(operator_index, input, operator_info, binding);
	}

	bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) override {
		ValidateSink();
		return pipeline.BindSink(input, sink_info, binding);
	}

	SinkResultType RecordSinkResult(DataChunk &chunk, SinkResultType sink_result) override {
		if (sink_result == SinkResultType::BLOCKED) {
			pipeline.RecordBlockedSinkChunk(chunk);
			RecordSinkAccounting(chunk.size(), sink_result);
			return sink_result;
		}
		return RecordSinkResult(chunk.size(), sink_result);
	}

	SinkResultType RecordSinkResult(idx_t input_rows, SinkResultType sink_result) override {
		ValidateSink();
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException(
			    "compiled full pipeline native sink update returned BLOCKED after entering sink state");
		}
		RecordSinkAccounting(input_rows, sink_result);
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
		result.sink_next_batch_invocation_count = sink_next_batch_invocation_count;
		result.sink_next_batch_runtime_time_us = sink_next_batch_runtime_time_us;
		auto non_generated_runtime_time_us = source_contract_runtime_time_us + sink_next_batch_runtime_time_us;
		result.generated_body_runtime_time_us =
		    runtime_time_us > non_generated_runtime_time_us ? runtime_time_us - non_generated_runtime_time_us : 0;
		result.generated_stage_runtime = generated_stage_runtime;
		result.jit_runtime = jit_runtime;
		return result;
	}

private:
	void ValidateSink() const {
		if (sink_blocked) {
			throw InternalException("compiled region runtime cannot sink after a blocked sink");
		}
		if (sink_finished) {
			throw InternalException("compiled region runtime cannot sink after a finished sink");
		}
		// Sink presence and local state are validated by the pipeline adapter.
	}

	void RecordSinkAccounting(idx_t input_rows, SinkResultType sink_result) {
		sink_input_rows += input_rows;
		if (sink_result == SinkResultType::BLOCKED) {
			sink_blocked = true;
		}
		if (sink_result == SinkResultType::FINISHED) {
			sink_finished = true;
			pipeline.FinishProcessing();
		}
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
	ExecutionRegionPipelineAdapter &pipeline;
	idx_t max_chunks;
	bool trace_runtime;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> source_stage_runtime;
	idx_t sink_next_batch_invocation_count = 0;
	int64_t sink_next_batch_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> generated_stage_runtime;
	ExecutionRegionJitRuntimeMetrics jit_runtime;
	idx_t sink_input_rows = 0;
	bool sink_blocked = false;
	bool sink_finished = false;
	string deferred_reason;
};

CompiledVectorizedRunStatus CompiledVectorizedRunner::ExecuteCompiledRegion(ExecutionRegionPipelineAdapter &pipeline,
                                                                            idx_t max_chunks,
                                                                            PipelineExecuteResult &result) {
	auto &client = pipeline.GetClientContext();
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

	auto trace_runtime = ExecutionRegionSettings::TraceRuntime(client);
	std::chrono::steady_clock::time_point trace_start;
	bool trace_started = false;
	if (!pipeline.IsAtCleanSourceToSinkBoundary()) {
		if (trace_runtime) {
			ExecutionRegionManager::Get(client).RecordRuntimeEvent(
			    client, *kernel, ExecutionRegionEventStatus::SKIPPED,
			    "full pipeline kernel not entered because executor state is not at a clean "
			    "source-to-sink boundary",
			    0, 0, 0, "boundary");
		}
		return CompiledVectorizedRunStatus::VECTORIZED_CONTINUATION;
	}

	try {
		if (trace_runtime) {
			trace_start = std::chrono::steady_clock::now();
			trace_started = true;
		}
		CompiledRegionRuntime runtime(pipeline, max_chunks, trace_runtime);
		ExecutionRegionResult compiled_result = ExecutionRegionResult::NOT_FINISHED;
		auto compiled_executed = kernel->TryExecuteFullPipeline(runtime, compiled_result);
		if (!compiled_executed) {
			throw InternalException("compiled full pipeline kernel returned false at runtime");
		}
		if (compiled_result == ExecutionRegionResult::DEFERRED) {
			auto elapsed_us = trace_runtime ? ExecutionRegionElapsedMicros(trace_start) : 0;
			if (trace_runtime) {
				auto reason = runtime.DeferredReason().empty()
				                  ? "full pipeline kernel deferred"
				                  : "full pipeline kernel deferred: " + runtime.DeferredReason();
				auto runtime_metrics = runtime.Metrics(elapsed_us);
				ExecutionRegionManager::Get(client).RecordRuntimeEvent(
				    client, *kernel, ExecutionRegionEventStatus::SKIPPED, std::move(reason),
				    runtime.SourceContractOutputRows(), runtime.SinkInputRows(), elapsed_us,
				    CompiledFullPipelineResultToString(compiled_result), runtime_metrics);
			}
			return CompiledVectorizedRunStatus::VECTORIZED_DEFERRED;
		}
		auto pipeline_result = CompiledFullPipelineResultToPipelineExecuteResult(compiled_result);
		string runtime_reason = "full pipeline kernel executed";
		if (compiled_result == ExecutionRegionResult::FINISHED) {
			pipeline_result = pipeline.FlushAndFinalizeAfterCompiledFinish(max_chunks, runtime_reason);
			compiled_result = PipelineExecuteResultToCompiledFullPipelineResult(pipeline_result);
		}
		result = pipeline_result;
		auto elapsed_us = trace_runtime ? ExecutionRegionElapsedMicros(trace_start) : 0;
		if (trace_runtime) {
			auto runtime_metrics = runtime.Metrics(elapsed_us);
			ExecutionRegionManager::Get(client).RecordRuntimeEvent(
			    client, *kernel, ExecutionRegionEventStatus::EXECUTED, std::move(runtime_reason),
			    runtime.SourceContractOutputRows(), runtime.SinkInputRows(), elapsed_us,
			    CompiledFullPipelineResultToString(compiled_result), runtime_metrics);
		}
		return CompiledVectorizedRunStatus::EXECUTED;
	} catch (...) {
		auto compiled_error = std::current_exception();
		if (trace_runtime) {
			auto elapsed_us = trace_started ? ExecutionRegionElapsedMicros(trace_start) : 0;
			ExecutionRegionManager::Get(client).RecordRuntimeEvent(client, *kernel, ExecutionRegionEventStatus::ERROR,
			                                                       "full pipeline kernel threw: " +
			                                                           CompiledRegionExceptionMessage(compiled_error),
			                                                       0, 0, elapsed_us, "error");
		}
		throw;
	}
}

} // namespace duckdb
