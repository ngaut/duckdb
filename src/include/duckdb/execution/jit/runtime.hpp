//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/region.hpp"
#include "duckdb/execution/jit/aggregate_runtime.hpp"
#include "duckdb/execution/jit/operator_runtime.hpp"

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class JitFullPipelineRuntime;
struct ExecutionContext;
struct OperatorSinkInput;

struct JitBackendInfo {
	string name;
	string description;
	bool available = false;
	bool supports_regions = false;
	bool selected = false;
};

struct JitEvent {
	idx_t event_id = 0;
	bool has_pipeline = false;
	string pipeline_shape;
	idx_t pipeline_estimated_cardinality = 0;
	bool has_candidate = false;
	idx_t candidate_id = 0;
	string candidate_shape;
	string candidate_scope;
	string candidate_pipeline_shape;
	string candidate_context_pipeline_shape;
	idx_t candidate_node_count = 0;
	idx_t candidate_start_operator_index = 0;
	idx_t candidate_end_operator_index = 0;
	idx_t candidate_estimated_cardinality = 0;
	JitRegionCandidateTraits candidate_traits;
	JitRegionContract candidate_contract;
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
	JitRegionSourceExecutionKind selected_source_execution = JitRegionSourceExecutionKind::NONE;
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
	idx_t source_native_output_rows = 0;
	idx_t source_native_invocation_count = 0;
	int64_t source_native_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	int64_t fused_prepare_runtime_time_us = 0;
	int64_t fused_group_runtime_time_us = 0;
	int64_t fused_state_bind_runtime_time_us = 0;
	int64_t fused_update_runtime_time_us = 0;
	int64_t fused_finish_runtime_time_us = 0;
	idx_t generated_body_flat_input_rows = 0;
	idx_t generated_body_flat_invocation_count = 0;
	idx_t generated_body_shared_selection_input_rows = 0;
	idx_t generated_body_shared_selection_invocation_count = 0;
	idx_t generated_body_selection_input_rows = 0;
	idx_t generated_body_selection_invocation_count = 0;
	idx_t generated_body_generic_input_rows = 0;
	idx_t generated_body_generic_invocation_count = 0;
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

struct JitCounter {
	string backend_name;
	string target;
	string status;
	string execution_mode;
	string region_execution_form;
	string policy_decision;
	idx_t count = 0;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_native_output_rows = 0;
	idx_t source_native_invocation_count = 0;
	int64_t source_native_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	int64_t fused_prepare_runtime_time_us = 0;
	int64_t fused_group_runtime_time_us = 0;
	int64_t fused_state_bind_runtime_time_us = 0;
	int64_t fused_update_runtime_time_us = 0;
	int64_t fused_finish_runtime_time_us = 0;
	idx_t generated_body_flat_input_rows = 0;
	idx_t generated_body_flat_invocation_count = 0;
	idx_t generated_body_shared_selection_input_rows = 0;
	idx_t generated_body_shared_selection_invocation_count = 0;
	idx_t generated_body_selection_input_rows = 0;
	idx_t generated_body_selection_invocation_count = 0;
	idx_t generated_body_generic_input_rows = 0;
	idx_t generated_body_generic_invocation_count = 0;
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
};

struct JitDecisionCounter {
	string backend_name;
	string target;
	string phase;
	string status;
	string execution_mode;
	string region_execution_form;
	string policy_decision;
	bool has_pipeline = false;
	string pipeline_shape;
	idx_t pipeline_estimated_cardinality = 0;
	string candidate_shape;
	string candidate_scope;
	string admission_shape_key;
	bool admission_rule_present = false;
	idx_t admission_min_cardinality = 0;
	string admission_proof;
	bool has_admission_score = false;
	int64_t min_admission_score = 0;
	int64_t max_admission_score = 0;
	JitRegionCandidateTraits candidate_traits;
	JitRegionContract candidate_contract;
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

struct JitKernelCounter {
	idx_t kernel_id = 0;
	string backend_name;
	string target;
	string execution_mode;
	string region_execution_form;
	bool has_candidate = false;
	idx_t candidate_id = 0;
	string candidate_shape;
	string candidate_scope;
	string candidate_pipeline_shape;
	string candidate_context_pipeline_shape;
	idx_t candidate_node_count = 0;
	idx_t candidate_start_operator_index = 0;
	idx_t candidate_end_operator_index = 0;
	idx_t candidate_estimated_cardinality = 0;
	JitRegionCandidateTraits candidate_traits;
	JitRegionContract candidate_contract;
	string compile_reason;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	string last_runtime_status;
	string last_runtime_result;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_native_output_rows = 0;
	idx_t source_native_invocation_count = 0;
	int64_t source_native_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	int64_t fused_prepare_runtime_time_us = 0;
	int64_t fused_group_runtime_time_us = 0;
	int64_t fused_state_bind_runtime_time_us = 0;
	int64_t fused_update_runtime_time_us = 0;
	int64_t fused_finish_runtime_time_us = 0;
	idx_t generated_body_flat_input_rows = 0;
	idx_t generated_body_flat_invocation_count = 0;
	idx_t generated_body_shared_selection_input_rows = 0;
	idx_t generated_body_shared_selection_invocation_count = 0;
	idx_t generated_body_selection_input_rows = 0;
	idx_t generated_body_selection_invocation_count = 0;
	idx_t generated_body_generic_input_rows = 0;
	idx_t generated_body_generic_invocation_count = 0;
	idx_t declined_invocation_count = 0;
	int64_t declined_runtime_time_us = 0;
	idx_t fallback_input_rows = 0;
	idx_t fallback_output_rows = 0;
	idx_t fallback_invocation_count = 0;
	int64_t fallback_runtime_time_us = 0;
};

struct JitAutoAdmissionRule {
	JitCompileTarget target = JitCompileTarget::REGION;
	string shape_key;
	idx_t min_cardinality = 0;
	string proof;
};

struct JitAdmissionInfo {
	bool has_admission = false;
	string shape_key;
	bool rule_present = false;
	idx_t min_cardinality = 0;
	string proof;
	bool has_score = false;
	int64_t score = 0;
};

struct JitAdmissionDecision {
	bool compile = false;
	string policy_decision;
	string reason;
	JitAdmissionInfo info;
};

struct JitStageTimings {
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
};

enum class JitGeneratedBodyPath : uint8_t { FLAT_ALL_VALID, SHARED_SELECTION_ALL_VALID, SELECTION_ALL_VALID, GENERIC };

enum class JitFusedRuntimeStage : uint8_t { PREPARE, GROUP, STATE_BIND, UPDATE, FINISH };

struct JitRuntimeMetrics {
	idx_t source_native_output_rows = 0;
	idx_t source_native_invocation_count = 0;
	int64_t source_native_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	int64_t fused_prepare_runtime_time_us = 0;
	int64_t fused_group_runtime_time_us = 0;
	int64_t fused_state_bind_runtime_time_us = 0;
	int64_t fused_update_runtime_time_us = 0;
	int64_t fused_finish_runtime_time_us = 0;
	idx_t generated_body_flat_input_rows = 0;
	idx_t generated_body_flat_invocation_count = 0;
	idx_t generated_body_shared_selection_input_rows = 0;
	idx_t generated_body_shared_selection_invocation_count = 0;
	idx_t generated_body_selection_input_rows = 0;
	idx_t generated_body_selection_invocation_count = 0;
	idx_t generated_body_generic_input_rows = 0;
	idx_t generated_body_generic_invocation_count = 0;
};

struct JitRegionNodeLowering {
	string role;
	string operator_name;
	JitLoweringKind kind = JitLoweringKind::FALLBACK;
	string reason;
};

class JitBackendPlan {
public:
	virtual ~JitBackendPlan();
};

struct JitRegionLoweringPlan {
	void AddNode(string role, string operator_name, JitLoweringKind kind, string reason);
	void AddFusionBlocker(string reason);
	void SetCompiledExecutionMode(JitExecutionMode execution_mode);
	void SetRegionExecutionForm(JitRegionExecutionForm execution_form);
	void SetOwnsSourceFilters(bool owns_source_filters);
	void SetSelectedSourceExecution(JitRegionSourceExecutionKind source_execution);
	void SetOperatorStageIR(string stage_ir);
	idx_t NativeCount() const;
	idx_t FallbackCount() const;
	idx_t PassThroughCount() const;
	JitExecutionMode ExpectedCompiledExecutionMode() const;
	JitRegionExecutionForm ExpectedRegionExecutionForm() const;
	bool OwnsSourceFilters() const;
	JitRegionSourceExecutionKind SelectedSourceExecution() const;
	string EventReason() const;

	vector<JitRegionNodeLowering> nodes;
	vector<string> fusion_blockers;
	string shape_key;
	shared_ptr<JitBackendPlan> backend_plan;
	JitExecutionMode compiled_execution_mode = JitExecutionMode::UNSUPPORTED;
	JitRegionExecutionForm region_execution_form = JitRegionExecutionForm::NONE;
	JitRegionSourceExecutionKind selected_source_execution = JitRegionSourceExecutionKind::NONE;
	bool owns_source_filters = false;
	string operator_stage_ir;
};

struct JitPreparedRegionCandidate {
	idx_t candidate_index = 0;
	JitRegionLoweringPlan lowering_plan;
	JitAdmissionDecision admission;
	JitStageTimings stage_timings;
	int64_t decision_time_us = 0;
};

struct JitPreparedSourceContract {
	bool present = false;
	bool selected = false;
	bool owns_filters = false;
	bool native_source = false;
	bool requires_unfiltered_input = false;
	bool filter_prune_required = false;
	bool filter_split_supported = false;
	vector<LogicalType> input_types;
	vector<idx_t> output_projection_map;
	vector<idx_t> filter_column_map;
	string reason;
	string ir;
};

struct JitPreparedPipeline {
	bool initialized = false;
	bool enabled = false;
	string backend_name;
	JitPolicyMode policy = JitPolicyMode::OFF;
	string policy_decision;
	unique_ptr<JitRegionIR> region_ir;
	vector<JitPreparedRegionCandidate> selected_regions;
	JitPreparedSourceContract source_contract;

	bool HasSelectedRegions() const {
		return !selected_regions.empty();
	}
	bool RequiresPreparedSourceInput() const {
		return source_contract.present && source_contract.selected && source_contract.owns_filters;
	}
	bool RequiresNativeSource() const {
		return source_contract.present && source_contract.selected && source_contract.native_source;
	}
	const vector<LogicalType> &SourceInputTypes(const vector<LogicalType> &fallback_types) const {
		return RequiresPreparedSourceInput() ? source_contract.input_types : fallback_types;
	}
};

class JitCodeHandle {
public:
	virtual ~JitCodeHandle();

	virtual idx_t CodeSize() const;
};

class JitSuppressionGuard {
public:
	explicit JitSuppressionGuard(ClientContext &context);
	~JitSuppressionGuard();

private:
	ClientContext &context;
};

class JitFullPipelineRuntime {
public:
	virtual ~JitFullPipelineRuntime();

	virtual idx_t MaxChunks() const = 0;
	virtual bool TraceRuntime() const = 0;
	virtual bool HasRequiredPartitionInfo() const = 0;
	virtual bool HasInProcessOperators() const = 0;
	virtual SourceResultType FetchNativeSource(DataChunk *&result, int64_t &source_fetch_time_us) = 0;
	virtual bool BindNativeOperator(idx_t operator_index, DataChunk &input, const JitRegionOperatorInfo &operator_info,
	                                JitNativeOperatorBinding &binding) = 0;
	virtual bool BindNativeSink(DataChunk &input, const JitRegionSinkInfo &sink_info,
	                            JitNativeSinkBinding &binding) = 0;
	virtual void BindNativeUngroupedAggregateStates(const vector<JitNativeUngroupedAggregateState> &requested_states,
	                                                vector<JitNativeUngroupedAggregateState> &bound_states) = 0;
	virtual void
	BindNativeHashAggregateStates(DataChunk &chunk,
	                              const vector<JitGroupedAggregateGroupBinding> &group_bindings,
	                              const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	                              JitNativeGroupedAggregateStateSet &bound_states) = 0;
	virtual SinkResultType FinishNativeHashAggregateUpdate(idx_t input_rows) = 0;
	virtual void
	BindNativePerfectHashAggregateStates(DataChunk &chunk,
	                                     const vector<JitGroupedAggregateGroupBinding> &group_bindings,
	                                     const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	                                     JitNativeGroupedAggregateStateSet &bound_states) = 0;
	virtual void
	BindNativePerfectHashAggregateStateLayout(const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	                                          JitNativeGroupedAggregateStateSet &bound_states,
	                                          JitNativePerfectHashAggregateStateLayout &state_layout) = 0;
	virtual SinkResultType FinishNativePerfectHashAggregateUpdate(idx_t input_rows) = 0;
	virtual SinkResultType RecordNativeSinkResult(DataChunk &chunk, SinkResultType sink_result) = 0;
	virtual SinkResultType RecordNativeSinkResult(idx_t input_rows, SinkResultType sink_result) = 0;
	virtual void RecordGeneratedBodyPath(JitGeneratedBodyPath path, idx_t input_rows) = 0;
	virtual void RecordFusedStageRuntime(JitFusedRuntimeStage stage, int64_t runtime_time_us) = 0;
};

class JitEventLog {
public:
	idx_t Record(idx_t event_log_size, bool record_decision_counter, JitEvent event);
	vector<JitEvent> GetEvents() const;
	vector<JitCounter> GetCounters() const;
	vector<JitDecisionCounter> GetDecisionCounters() const;
	vector<JitKernelCounter> GetKernelCounters() const;
	void ClearEvents();
	void ClearCounters();
	void ApplyRetentionLimit(idx_t event_log_size);

private:
	void RecordCounter(const JitEvent &event);
	void RecordDecisionCounter(const JitEvent &event);
	void RecordKernelCounter(idx_t kernel_counter_log_size, const JitEvent &event);
	void AccumulateKernelRuntime(JitKernelCounter &counter, const JitEvent &event);
	void TrimEvents(idx_t event_log_size);
	void TrimKernelCounters(idx_t kernel_counter_log_size);

private:
	mutable mutex lock;
	vector<JitEvent> events;
	vector<JitCounter> counters;
	vector<JitDecisionCounter> decision_counters;
	vector<JitKernelCounter> kernel_counters;
	idx_t next_event_id = 1;
};

class JitRegionKernel {
public:
	virtual ~JitRegionKernel();

	virtual const string &BackendName() const = 0;
	virtual idx_t CodeSize() const;
	virtual bool HasExecutableBody() const;
	void SetTraceInfo(idx_t trace_id, JitExecutionMode execution_mode, string compile_reason, int64_t compile_time_us,
	                  idx_t code_size);
	void SetTraceRegionExecutionForm(JitRegionExecutionForm execution_form);
	void SetTraceSelectedSourceExecution(JitRegionSourceExecutionKind source_execution);
	void SetTraceCandidate(const JitRegionCandidate &candidate);
	void SetRuntimeDeclineReason(string reason);
	string ConsumeRuntimeDeclineReason();
	idx_t TraceId() const;
	JitExecutionMode ExecutionMode() const;
	JitRegionExecutionForm RegionExecutionForm() const;
	JitRegionSourceExecutionKind SelectedSourceExecution() const;
	const string &TraceCompileReason() const;
	int64_t TraceCompileTime() const;
	idx_t TraceCodeSize() const;
	bool HasTraceCandidate() const;
	idx_t TraceCandidateId() const;
	const string &TraceCandidateShape() const;
	const string &TraceCandidateScope() const;
	const string &TraceCandidatePipelineShape() const;
	const string &TraceCandidateContextPipelineShape() const;
	idx_t TraceCandidateNodeCount() const;
	idx_t TraceCandidateStartOperatorIndex() const;
	idx_t TraceCandidateEndOperatorIndex() const;
	idx_t TraceCandidateEstimatedCardinality() const;
	const JitRegionCandidateTraits &TraceCandidateTraits() const;
	const JitRegionContract &TraceCandidateContract() const;
	const vector<LogicalType> &TraceCandidateOutputTypes() const;
	virtual bool CanExecuteSourcePipeline() const;
	virtual bool CanExecuteSinkPipeline() const;
	virtual bool CanExecuteFullPipeline() const;
	virtual bool RequiresNativeSource() const;
	virtual bool TryExecute(DataChunk &input, DataChunk &result, idx_t initial_idx, OperatorResultType &execute_result);
	virtual bool TrySink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
	                     SinkResultType &sink_result);
	virtual bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result);

private:
	idx_t trace_id = 0;
	JitExecutionMode execution_mode = JitExecutionMode::NONE;
	JitRegionExecutionForm region_execution_form = JitRegionExecutionForm::NONE;
	JitRegionSourceExecutionKind selected_source_execution = JitRegionSourceExecutionKind::NONE;
	string trace_compile_reason;
	int64_t trace_compile_time_us = 0;
	idx_t trace_code_size = 0;
	bool has_trace_candidate = false;
	idx_t trace_candidate_id = 0;
	string trace_candidate_shape;
	string trace_candidate_scope;
	string trace_candidate_pipeline_shape;
	string trace_candidate_context_pipeline_shape;
	idx_t trace_candidate_node_count = 0;
	idx_t trace_candidate_start_operator_index = 0;
	idx_t trace_candidate_end_operator_index = 0;
	idx_t trace_candidate_estimated_cardinality = 0;
	JitRegionCandidateTraits trace_candidate_traits;
	JitRegionContract trace_candidate_contract;
	vector<LogicalType> trace_candidate_output_types;
	string runtime_decline_reason;
};

struct JitRegionCompilationInput {
	JitRegionCompilationInput(ClientContext &context, const JitRegionIR &region_ir,
	                          const JitRegionCandidate &candidate);

	ClientContext &context;
	const JitRegionIR &region_ir;
	const JitRegionCandidate &candidate;
	const JitRegionLoweringPlan *lowering_plan = nullptr;
};

struct JitRegionCompileResult {
	static JitRegionCompileResult Compiled(unique_ptr<JitRegionKernel> kernel, JitExecutionMode execution_mode,
	                                       string reason = string(), string ir = string());
	static JitRegionCompileResult Unsupported(string reason);
	static JitRegionCompileResult Unavailable(string reason);
	static JitRegionCompileResult Error(string reason);

	JitCompileStatus status = JitCompileStatus::UNSUPPORTED;
	JitExecutionMode execution_mode = JitExecutionMode::UNSUPPORTED;
	string reason;
	string ir;
	unique_ptr<JitRegionKernel> kernel;
};

class JitBackend {
public:
	virtual ~JitBackend();

	virtual string Name() const = 0;
	virtual string Description() const = 0;
	virtual bool IsAvailable() const;
	virtual bool SupportsRegions() const;
	virtual JitRegionLoweringPlan AnalyzeRegion(const JitRegionCompilationInput &input);
	virtual JitRegionCompileResult CompileRegion(const JitRegionCompilationInput &input);
	virtual bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionPipelineInventory &inventory,
	                                      bool explain, JitAdmissionInfo &info, string &reason) const;
	virtual bool MayHaveAutoAdmissionRule(JitCompileTarget target, const JitRegionCandidate &candidate,
	                                      JitAdmissionInfo &info, string &reason) const;
	virtual bool GetAutoAdmissionRule(JitCompileTarget target, const string &shape_key,
	                                  JitAutoAdmissionRule &rule) const;
};

} // namespace duckdb
