#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/gathered_metrics.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "yyjson.hpp"
#include "yyjson_utils.hpp"

#include <algorithm>
#include <initializer_list>
#include <utility>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

void QueryProfileResult::AddValue(const string &k, Value val) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::VALUE;
	child->key = k;
	child->value = std::move(val);
	children.push_back(std::move(child));
}

QueryProfileResult &QueryProfileResult::AddObject(const string &k) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::OBJECT;
	child->key = k;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfileResult &QueryProfileResult::AddList(const string &k) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::LIST;
	child->key = k;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfileResult &QueryProfileResult::AppendObject() {
	D_ASSERT(kind == QueryProfileResultKind::LIST);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::OBJECT;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

void QueryProfileResult::AppendValue(Value val) {
	D_ASSERT(kind == QueryProfileResultKind::LIST);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::VALUE;
	child->value = std::move(val);
	children.push_back(std::move(child));
}

QueryProfileResult &QueryProfileResult::AppendList() {
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::LIST;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

static int64_t Micros(double seconds) {
	return seconds <= 0 ? 0 : static_cast<int64_t>((seconds * 1000000.0) + 0.5);
}

static Value Text(const string &value) {
	return Value(value);
}

static Value Text(const char *value) {
	return Value(string(value));
}

static Value NullableText(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

static Value Count(idx_t value) {
	return Value::UBIGINT(value);
}

static Value Time(int64_t value) {
	return Value::BIGINT(value);
}

static int64_t RuntimeUnattributedTime(const ExecutionRegionTraceSummary &summary) {
	auto attributed = summary.source_us + summary.sink_us + summary.generated_us;
	if (summary.runtime_us <= attributed) {
		return 0;
	}
	return summary.runtime_us - attributed;
}

static double RuntimePercent(int64_t value, int64_t runtime_us) {
	if (runtime_us <= 0) {
		return 0;
	}
	return (double(value) * 100.0) / double(runtime_us);
}

static const char *DominantRuntimeComponent(const ExecutionRegionTraceSummary &summary) {
	auto unattributed_us = RuntimeUnattributedTime(summary);
	if (summary.runtime_us <= 0 && summary.source_us <= 0 && summary.sink_us <= 0 && summary.generated_us <= 0) {
		return "none";
	}
	auto dominant = "unattributed";
	auto dominant_us = unattributed_us;
	if (summary.source_us >= dominant_us) {
		dominant = "source";
		dominant_us = summary.source_us;
	}
	if (summary.generated_us >= dominant_us) {
		dominant = "generated";
		dominant_us = summary.generated_us;
	}
	if (summary.sink_us >= dominant_us) {
		dominant = "sink";
	}
	return dominant;
}

static string ExecutionRegionProfileToken(string value, idx_t max_length = 96) {
	if (value.empty()) {
		return "none";
	}
	value = StringUtil::Replace(value, "\n", " ");
	value = StringUtil::Replace(value, "\r", " ");
	value = StringUtil::Replace(value, "\t", " ");
	if (value.size() <= max_length) {
		return value;
	}
	if (max_length <= 3) {
		return value.substr(0, max_length);
	}
	return value.substr(0, max_length - 3) + "...";
}

static string ExecutionRegionProfileShape(const ExecutionRegionEvent &event) {
	if (event.has_candidate && event.candidate_traits.sink_kind != ExecutionRegionSinkKind::NONE) {
		return ExecutionRegionSinkKindToString(event.candidate_traits.sink_kind);
	}
	if (event.has_candidate && !event.candidate_shape.empty()) {
		return event.candidate_shape;
	}
	auto pipeline_shape = ExecutionRegionEventPipelineShape(event);
	if (!pipeline_shape.empty()) {
		return pipeline_shape;
	}
	return "none";
}

static string ExecutionRegionProfileStageCosts(const PhysicalRunnerCostProfile &cost) {
	string result = "gen:" + std::to_string(cost.generated_stage_count);
	result += ",join:" + std::to_string(cost.native_join_stage_count);
	result += ",join_build_sink:" + std::to_string(cost.native_hash_join_build_sink_count);
	result += ",agg:" + std::to_string(cost.native_aggregate_stage_count);
	result += ",grouped_agg:" + std::to_string(cost.native_grouped_aggregate_stage_count);
	result += ",sort:" + std::to_string(cost.native_sort_stage_count);
	result += ",mat:" + std::to_string(cost.materialization_elision_count);
	result += ",source_append:" + std::to_string(cost.materialization_source_append_count);
	result += ",unfused_mark_filter_agg:" + std::to_string(cost.unfused_mark_filter_aggregate_count);
	result += ",full:";
	result += cost.full_pipeline ? "true" : "false";
	result += ",scope:";
	result += PhysicalRunnerCostInputScopeToString(cost.input_scope);
	result += ",expr:" + std::to_string(cost.expression_cost);
	result += ",gen_class:";
	result += PhysicalRunnerGeneratedWorkClassToString(cost.generated_work_class);
	result += ",native_protocol:";
	result += PhysicalRunnerNativeProtocolClassToString(cost.native_protocol_class);
	result += ",admission:";
	result += cost.admission_class.empty() ? "none" : cost.admission_class;
	result += ",selection_reason:";
	result += cost.selection_reason.empty() ? "none" : cost.selection_reason;
	result += ",selected_runner:";
	result += ExecutionRunnerKindToString(cost.selected_runner);
	return result;
}

static string ExecutionRegionProfileCostComponents(const PhysicalRunnerCostProfile &cost) {
	string result = "expr:" + std::to_string(cost.generated_expression_work);
	result += ",gen_stage:" + std::to_string(cost.generated_stage_work);
	result += ",native:" + std::to_string(cost.native_operator_work);
	result += ",mat:" + std::to_string(cost.materialization_elision_work);
	result += ",source_append_penalty:" + std::to_string(cost.materialization_source_append_penalty);
	result += ",unfused_mark_filter_agg_penalty:" + std::to_string(cost.unfused_mark_filter_aggregate_penalty);
	result += ",full:" + std::to_string(cost.full_pipeline_work);
	result += ",protocol_penalty:" + std::to_string(cost.stateful_protocol_penalty);
	result += ",gpu_transfer:" + std::to_string(cost.gpu_transfer_cost);
	return result;
}

static string FindExecutionRegionProfileReasonToken(const string &reason, const string &marker) {
	idx_t start = 0;
	while (start < reason.size()) {
		auto end = reason.find(';', start);
		auto length = end == string::npos ? reason.size() - start : end - start;
		auto token = reason.substr(start, length);
		if (token.find(marker) != string::npos) {
			return token;
		}
		if (end == string::npos) {
			break;
		}
		start = end + 1;
	}
	return string();
}

static string FirstExecutionRegionProfileReasonToken(const string &reason) {
	if (reason.empty()) {
		return "none";
	}
	auto separator = reason.find(';');
	if (separator == string::npos) {
		return reason;
	}
	return reason.substr(0, separator);
}

static string ExecutionRegionProfileReason(const string &reason) {
	for (const auto marker : {":boundary:", "candidate-builder-blocked:", "region-lowering-blocked:"}) {
		auto token = FindExecutionRegionProfileReasonToken(reason, marker);
		if (!token.empty()) {
			return token;
		}
	}
	return FirstExecutionRegionProfileReasonToken(reason);
}

static void AddFields(QueryProfileResult &node, std::initializer_list<std::pair<const char *, Value>> fields) {
	for (const auto &field : fields) {
		node.AddValue(field.first, field.second);
	}
}

static string SelectedExecutionRegionBackendName(ClientContext &context) {
	for (const auto &backend : ExecutionRegionManager::Get(context).GetBackends(&context)) {
		if (backend.selected) {
			return backend.name;
		}
	}
	return "none";
}

static void AddExecutionRegionSummary(QueryProfileResult &node, ClientContext &context,
                                      const ExecutionRegionTraceSummary &summary, int64_t query_runtime_us) {
	AddFields(node,
	          {{"jit_enabled", Value::BOOLEAN(ExecutionRegionSettings::Enabled(context))},
	           {"jit_policy", Text(ExecutionRegionPolicyModeToString(ExecutionRegionSettings::Policy(context)))},
	           {"jit_requested_backend", Text(ExecutionRegionSettings::RequestedBackend(context))},
	           {"jit_selected_backend", Text(SelectedExecutionRegionBackendName(context))},
	           {"runtime_regions", Count(summary.runtime_regions)},
	           {"compiled_regions", Count(summary.compiled)},
	           {"compile_errors", Count(summary.compile_errors)},
	           {"runtime_events", Count(summary.runtime_events)},
	           {"decisions", Count(summary.decisions)},
	           {"unsupported_decisions", Count(summary.unsupported)},
	           {"skipped_decisions", Count(summary.skipped)},
	           {"unavailable_decisions", Count(summary.unavailable)},
	           {"disabled_decisions", Count(summary.disabled)},
	           {"code_size", Count(summary.code_size)},
	           {"query_runtime_time_us", Time(query_runtime_us)},
	           {"runtime_time_us", Time(summary.runtime_us)},
	           {"source_runtime_time_us", Time(summary.source_us)},
	           {"sink_next_batch_runtime_time_us", Time(summary.sink_us)},
	           {"generated_runtime_time_us", Time(summary.generated_us)},
	           {"runtime_unattributed_time_us", Time(RuntimeUnattributedTime(summary))},
	           {"source_runtime_pct", Value::DOUBLE(RuntimePercent(summary.source_us, summary.runtime_us))},
	           {"sink_next_batch_runtime_pct", Value::DOUBLE(RuntimePercent(summary.sink_us, summary.runtime_us))},
	           {"generated_runtime_pct", Value::DOUBLE(RuntimePercent(summary.generated_us, summary.runtime_us))},
	           {"runtime_unattributed_pct",
	            Value::DOUBLE(RuntimePercent(RuntimeUnattributedTime(summary), summary.runtime_us))},
	           {"runtime_dominant_component", Text(DominantRuntimeComponent(summary))},
	           {"decision_time_us", Time(summary.decision_us)},
	           {"compile_time_us", Time(summary.compile_us)},
	           {"pipeline_cbo_time_us", Time(summary.pipeline_cbo_us)},
	           {"graph_build_time_us", Time(summary.graph_build_us)},
	           {"candidate_cbo_time_us", Time(summary.candidate_cbo_us)},
	           {"ir_lowering_time_us", Time(summary.ir_lowering_us)},
	           {"backend_analysis_time_us", Time(summary.backend_analysis_us)},
	           {"codegen_time_us", Time(summary.codegen_us)},
	           {"executable_build_time_us", Time(summary.executable_build_us)},
	           {"machine_codegen_time_us", Time(summary.machine_codegen_us)},
	           {"kernel_build_time_us", Time(summary.kernel_build_us)},
	           {"lazy_codegen_time_us", Time(summary.lazy_codegen.codegen_time_us)},
	           {"lazy_machine_codegen_time_us", Time(summary.lazy_codegen.machine_codegen_time_us)},
	           {"lazy_code_size", Count(summary.lazy_codegen.code_size)}});
}

static void AddExecutionRegionEvent(QueryProfileResult &row, const ExecutionRegionEvent &event, bool is_runtime) {
	AddFields(
	    row,
	    {{"entry_type", Text(is_runtime ? "runtime" : "decision")},
	     {"event_id", Count(event.event_id)},
	     {"kernel_id", Count(event.kernel_id)},
	     {"status", Text(ExecutionRegionEventStatusToString(event.status_kind))},
	     {"backend_name", Text(event.backend_name)},
	     {"execution_mode", Text(ExecutionRegionExecutionModeToString(event.execution_mode_kind))},
	     {"selected_source_execution",
	      Text(ExecutionRegionSourceExecutionKindToString(event.selected_source_execution))},
	     {"selected_uses_scan_filters", Value::BOOLEAN(event.selected_uses_scan_filters)},
	     {"candidate_uses_scan_filters", Value::BOOLEAN(event.candidate_uses_scan_filters)},
	     {"shape", Text(event.candidate_shape)},
	     {"pipeline_shape", Text(ExecutionRegionEventPipelineShape(event))},
	     {"estimated_cardinality", Count(ExecutionRegionEventEstimatedCardinality(event))},
	     {"selected_runner", Text(ExecutionRunnerKindToString(event.selected_runner))},
	     {"runner_cost_profile", Value::BOOLEAN(event.runner_cost.present)},
	     {"runner_cost_rows", Time(event.runner_cost.rows)},
		     {"runner_cost_batches", Time(event.runner_cost.batches)},
		     {"runner_cost_expression_cost", Time(event.runner_cost.expression_cost)},
		     {"runner_cost_generated_stage_count", Time(event.runner_cost.generated_stage_count)},
		     {"runner_cost_generated_backend_stage_count", Time(event.runner_cost.generated_backend_stage_count)},
		     {"runner_cost_materialization_elision_count", Time(event.runner_cost.materialization_elision_count)},
	     {"runner_cost_materialization_source_append_count",
	      Time(event.runner_cost.materialization_source_append_count)},
	     {"runner_cost_unfused_mark_filter_aggregate_count",
	      Time(event.runner_cost.unfused_mark_filter_aggregate_count)},
	     {"runner_cost_native_join_stage_count", Time(event.runner_cost.native_join_stage_count)},
		     {"runner_cost_native_hash_join_build_sink_count",
		      Time(event.runner_cost.native_hash_join_build_sink_count)},
		     {"runner_cost_native_aggregate_stage_count", Time(event.runner_cost.native_aggregate_stage_count)},
		     {"runner_cost_native_grouped_aggregate_stage_count",
		      Time(event.runner_cost.native_grouped_aggregate_stage_count)},
		     {"runner_cost_native_sort_stage_count", Time(event.runner_cost.native_sort_stage_count)},
	     {"runner_cost_full_pipeline", Value::BOOLEAN(event.runner_cost.full_pipeline)},
	     {"runner_cost_input_scope", Text(PhysicalRunnerCostInputScopeToString(event.runner_cost.input_scope))},
	     {"runner_cost_generated_work_class",
	      Text(PhysicalRunnerGeneratedWorkClassToString(event.runner_cost.generated_work_class))},
	     {"runner_cost_native_protocol_class",
	      Text(PhysicalRunnerNativeProtocolClassToString(event.runner_cost.native_protocol_class))},
	     {"runner_cost_admission_class", NullableText(event.runner_cost.admission_class)},
	     {"runner_cost_selection_reason", NullableText(event.runner_cost.selection_reason)},
		     {"runner_cost_generated_expression_work", Time(event.runner_cost.generated_expression_work)},
		     {"runner_cost_generated_stage_work", Time(event.runner_cost.generated_stage_work)},
		     {"runner_cost_generated_backend_stage_work", Time(event.runner_cost.generated_backend_stage_work)},
		     {"runner_cost_native_operator_work", Time(event.runner_cost.native_operator_work)},
	     {"runner_cost_materialization_elision_work", Time(event.runner_cost.materialization_elision_work)},
	     {"runner_cost_materialization_source_append_penalty",
	      Time(event.runner_cost.materialization_source_append_penalty)},
	     {"runner_cost_unfused_mark_filter_aggregate_penalty",
	      Time(event.runner_cost.unfused_mark_filter_aggregate_penalty)},
	     {"runner_cost_full_pipeline_work", Time(event.runner_cost.full_pipeline_work)},
	     {"runner_cost_stateful_protocol_penalty", Time(event.runner_cost.stateful_protocol_penalty)},
	     {"runner_cost_saved_work_per_batch", Time(event.runner_cost.saved_work_per_batch)},
	     {"runner_cost_compiled_vectorized_runner_benefit", Time(event.runner_cost.compiled_vectorized_runner_benefit)},
	     {"runner_cost_compiled_vectorized_startup_cost", Time(event.runner_cost.compiled_vectorized_startup_cost)},
	     {"runner_cost_compiled_vectorized_required_benefit",
	      Time(event.runner_cost.compiled_vectorized_required_benefit)},
	     {"runner_cost_compiled_vectorized_net_benefit", Time(event.runner_cost.compiled_vectorized_net_benefit)},
	     {"runner_cost_gpu_runner_benefit", Time(event.runner_cost.gpu_runner_benefit)},
	     {"runner_cost_gpu_transfer_cost", Time(event.runner_cost.gpu_transfer_cost)},
	     {"runner_cost_gpu_startup_cost", Time(event.runner_cost.gpu_startup_cost)},
	     {"runner_cost_gpu_required_benefit", Time(event.runner_cost.gpu_required_benefit)},
	     {"runner_cost_gpu_net_benefit", Time(event.runner_cost.gpu_net_benefit)},
	     {"runner_cost_accelerated_runner_benefit", Time(event.runner_cost.accelerated_runner_benefit)},
	     {"runner_cost_startup_cost", Time(event.runner_cost.startup_cost)},
	     {"runner_cost_required_benefit", Time(event.runner_cost.required_benefit)},
	     {"runner_cost_net_benefit", Time(event.runner_cost.net_benefit)},
	     {"runner_cost_selected_accelerated_runner", Value::BOOLEAN(event.runner_cost.selected_accelerated_runner)},
	     {"runner_cost_selected_compiled_vectorized_runner",
	      Value::BOOLEAN(event.runner_cost.selected_compiled_vectorized_runner)},
	     {"runner_cost_selected_gpu_runner", Value::BOOLEAN(event.runner_cost.selected_gpu_runner)},
	     {"reason", Text(event.reason)},
	     {"blocker", NullableText(event.blocker)},
	     {"runtime_result", Text(event.runtime_result)},
	     {"code_size", Count(ExecutionRegionEventProfileCodeSize(event))},
	     {"input_rows", Count(event.input_rows)},
	     {"output_rows", Count(event.output_rows)},
	     {"invocation_count", Count(event.invocation_count)},
	     {"source_contract_output_rows", Count(event.source_contract_output_rows)},
	     {"source_contract_invocation_count", Count(event.source_contract_invocation_count)},
	     {"sink_next_batch_invocation_count", Count(event.sink_next_batch_invocation_count)},
	     {"decision_time_us", Time(event.decision_time_us)},
	     {"compile_time_us", Time(ExecutionRegionEventProfileCompileTime(event))},
	     {"pipeline_cbo_time_us", Time(event.stage_timings.pipeline_cbo_time_us)},
	     {"graph_build_time_us", Time(event.stage_timings.graph_build_time_us)},
	     {"candidate_cbo_time_us", Time(event.stage_timings.candidate_cbo_time_us)},
	     {"ir_lowering_time_us", Time(event.stage_timings.ir_lowering_time_us)},
	     {"backend_analysis_time_us", Time(event.stage_timings.backend_analysis_time_us)},
	     {"codegen_time_us", Time(event.stage_timings.codegen_time_us)},
	     {"executable_build_time_us", Time(event.stage_timings.executable_build_time_us)},
	     {"machine_codegen_time_us", Time(event.stage_timings.machine_codegen_time_us)},
	     {"kernel_build_time_us", Time(event.stage_timings.kernel_build_time_us)},
	     {"lazy_codegen_time_us", Time(event.jit_runtime.lazy_codegen.codegen_time_us)},
	     {"lazy_machine_codegen_time_us", Time(event.jit_runtime.lazy_codegen.machine_codegen_time_us)},
	     {"lazy_code_size", Count(event.jit_runtime.lazy_codegen.code_size)},
	     {"hash_join_probe_layout", NullableText(event.jit_runtime.hash_join_probe_layout)},
	     {"jit_runtime_path_counts",
	      NullableText(RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_path_counts))},
	     {"jit_materialization_boundary_counts",
	      NullableText(RenderExecutionRegionCounterBreakdown(event.jit_runtime.materialization_boundary_counts))},
	     {"runtime_time_us", Time(event.runtime_time_us)},
	     {"source_runtime_time_us", Time(event.source_contract_runtime_time_us)},
	     {"sink_next_batch_runtime_time_us", Time(event.sink_next_batch_runtime_time_us)},
	     {"generated_runtime_time_us", Time(event.generated_body_runtime_time_us)},
	     {"source_stage_runtime_breakdown",
	      Text(RenderExecutionRegionStageRuntimeBreakdown(event.source_stage_runtime))},
	     {"generated_stage_runtime_breakdown",
	      Text(RenderExecutionRegionStageRuntimeBreakdown(event.generated_stage_runtime))}});
	if (!event.ir.empty()) {
		row.AddValue("ir", Text(event.ir));
	}
}

static void AddExecutionRegionEvents(QueryProfileResult &node, const QueryProfilerExecutionRegionTrace &trace) {
	auto &rows = node.AddList("events");
	for (const auto &event : trace) {
		if (!ExecutionRegionEventIsVisibleInQueryProfile(event)) {
			continue;
		}
		AddExecutionRegionEvent(rows.AppendObject(), event, ExecutionRegionEventIsRuntime(event));
	}
}

static void RenderExecutionRegionCboPipelineToStream(std::ostream &ss, const QueryProfilerExecutionRegionTrace &trace);
static void RenderExecutionRegionRuntimePipelineToStream(std::ostream &ss,
                                                         const QueryProfilerExecutionRegionTrace &trace);

static void RenderExecutionRegionsToStream(std::ostream &ss, ClientContext &context, double query_runtime_seconds,
                                           const QueryProfilerExecutionRegionTrace &trace) {
	if (trace.empty()) {
		return;
	}
	auto summary = SummarizeExecutionRegionTrace(trace);
	ss << "JIT_EXECUTION_REGIONS\n";
	ss << "  policy=" << ExecutionRegionPolicyModeToString(ExecutionRegionSettings::Policy(context))
	   << " requested_backend=" << ExecutionRegionSettings::RequestedBackend(context)
	   << " selected_backend=" << SelectedExecutionRegionBackendName(context)
	   << " enabled=" << (ExecutionRegionSettings::Enabled(context) ? "true" : "false")
	   << " compiled=" << summary.compiled << " runtime_regions=" << summary.runtime_regions
	   << " decisions=" << summary.decisions << " unsupported=" << summary.unsupported << " skipped=" << summary.skipped
	   << " query_runtime_us=" << Micros(query_runtime_seconds) << " region_runtime_us=" << summary.runtime_us
	   << " source_runtime_us=" << summary.source_us << " sink_runtime_us=" << summary.sink_us
	   << " generated_runtime_us=" << summary.generated_us
	   << " runtime_unattributed_us=" << RuntimeUnattributedTime(summary)
	   << " runtime_dominant=" << DominantRuntimeComponent(summary)
	   << " source_runtime_pct=" << RuntimePercent(summary.source_us, summary.runtime_us)
	   << " generated_runtime_pct=" << RuntimePercent(summary.generated_us, summary.runtime_us)
	   << " decision_time_us=" << summary.decision_us << " compile_time_us=" << summary.compile_us
	   << " pipeline_cbo_us=" << summary.pipeline_cbo_us << " graph_build_us=" << summary.graph_build_us
	   << " candidate_cbo_us=" << summary.candidate_cbo_us << " ir_lowering_us=" << summary.ir_lowering_us
	   << " backend_analysis_us=" << summary.backend_analysis_us << " codegen_us=" << summary.codegen_us
	   << " executable_build_us=" << summary.executable_build_us << " machine_codegen_us=" << summary.machine_codegen_us
	   << " kernel_build_us=" << summary.kernel_build_us << " lazy_codegen_us=" << summary.lazy_codegen.codegen_time_us
	   << " lazy_machine_codegen_us=" << summary.lazy_codegen.machine_codegen_time_us
	   << " lazy_code_size=" << summary.lazy_codegen.code_size << "\n";
	RenderExecutionRegionCboPipelineToStream(ss, trace);
	RenderExecutionRegionRuntimePipelineToStream(ss, trace);
}

static void RenderExecutionRegionCboPipelineToStream(std::ostream &ss, const QueryProfilerExecutionRegionTrace &trace) {
	bool wrote_header = false;
	for (const auto &event : trace) {
		if (ExecutionRegionEventIsRuntime(event) || !ExecutionRegionEventIsVisibleInQueryProfile(event)) {
			continue;
		}
		if (!wrote_header) {
			ss << "  CBO_PIPELINE\n";
			wrote_header = true;
		}
		ss << "    id=" << event.event_id << " phase=" << ExecutionRegionEventPhaseToString(event.phase_kind)
		   << " status=" << ExecutionRegionEventStatusToString(event.status_kind)
		   << " mode=" << ExecutionRegionExecutionModeToString(event.execution_mode_kind)
		   << " runner=" << ExecutionRunnerKindToString(event.selected_runner)
		   << " shape=" << ExecutionRegionProfileToken(ExecutionRegionProfileShape(event), 64)
		   << " rows=" << ExecutionRegionEventEstimatedCardinality(event);
		if (event.runner_cost.present) {
			ss << " batches=" << event.runner_cost.batches
			   << " stages=" << ExecutionRegionProfileStageCosts(event.runner_cost)
			   << " work=" << ExecutionRegionProfileCostComponents(event.runner_cost)
			   << " saved=" << event.runner_cost.saved_work_per_batch
			   << " benefit=" << event.runner_cost.accelerated_runner_benefit
			   << " required=" << event.runner_cost.required_benefit << " net=" << event.runner_cost.net_benefit
			   << " selected=" << (event.runner_cost.selected_accelerated_runner ? "true" : "false");
		} else {
			ss << " cost=none";
		}
		ss << " decision_us=" << event.decision_time_us
		   << " pipeline_cbo_us=" << event.stage_timings.pipeline_cbo_time_us
		   << " graph_build_us=" << event.stage_timings.graph_build_time_us
		   << " candidate_cbo_us=" << event.stage_timings.candidate_cbo_time_us
		   << " ir_lowering_us=" << event.stage_timings.ir_lowering_time_us
		   << " backend_analysis_us=" << event.stage_timings.backend_analysis_time_us
		   << " compile_us=" << ExecutionRegionEventProfileCompileTime(event)
		   << " codegen_us=" << event.stage_timings.codegen_time_us
		   << " executable_build_us=" << event.stage_timings.executable_build_time_us
		   << " machine_codegen_us=" << event.stage_timings.machine_codegen_time_us
		   << " kernel_build_us=" << event.stage_timings.kernel_build_time_us
		   << " code_size=" << ExecutionRegionEventProfileCodeSize(event)
		   << " blocker=" << ExecutionRegionProfileToken(event.blocker)
		   << " why=" << ExecutionRegionProfileToken(ExecutionRegionProfileReason(event.reason), 128) << "\n";
	}
}

static void RenderExecutionRegionRuntimePipelineToStream(std::ostream &ss,
                                                         const QueryProfilerExecutionRegionTrace &trace) {
	bool wrote_header = false;
	for (const auto &event : trace) {
		if (!ExecutionRegionEventIsRuntime(event) || !ExecutionRegionEventIsVisibleInQueryProfile(event)) {
			continue;
		}
		if (!wrote_header) {
			ss << "  RUNTIME_PIPELINE\n";
			wrote_header = true;
		}
		ExecutionRegionTraceSummary event_summary;
		event_summary.runtime_us = event.runtime_time_us;
		event_summary.source_us = event.source_contract_runtime_time_us;
		event_summary.sink_us = event.sink_next_batch_runtime_time_us;
		event_summary.generated_us = event.generated_body_runtime_time_us;
		ss << "    id=" << event.event_id << " kernel=" << event.kernel_id
		   << " status=" << ExecutionRegionEventStatusToString(event.status_kind)
		   << " result=" << ExecutionRegionProfileToken(event.runtime_result, 32)
		   << " mode=" << ExecutionRegionExecutionModeToString(event.execution_mode_kind)
		   << " shape=" << ExecutionRegionProfileToken(ExecutionRegionProfileShape(event), 64)
		   << " rows=" << event.input_rows << "->" << event.output_rows << " calls=" << event.invocation_count
		   << " runtime_us=" << event.runtime_time_us << " source_us=" << event.source_contract_runtime_time_us
		   << " generated_us=" << event.generated_body_runtime_time_us
		   << " sink_us=" << event.sink_next_batch_runtime_time_us
		   << " hash_join_probe_layout=" << ExecutionRegionProfileToken(event.jit_runtime.hash_join_probe_layout)
		   << " jit_runtime_path_counts="
		   << ExecutionRegionProfileToken(RenderExecutionRegionCounterBreakdown(event.jit_runtime.runtime_path_counts),
		                                  128)
		   << " jit_materialization_boundary_counts="
		   << ExecutionRegionProfileToken(
		          RenderExecutionRegionCounterBreakdown(event.jit_runtime.materialization_boundary_counts), 128)
		   << " lazy_codegen_us=" << event.jit_runtime.lazy_codegen.codegen_time_us
		   << " lazy_machine_codegen_us=" << event.jit_runtime.lazy_codegen.machine_codegen_time_us
		   << " lazy_code_size=" << event.jit_runtime.lazy_codegen.code_size
		   << " dominant=" << DominantRuntimeComponent(event_summary) << "\n";
	}
}

static void AppendExecutionRegionsToResult(QueryProfileResult &result, ClientContext &context,
                                           double query_runtime_seconds,
                                           const QueryProfilerExecutionRegionTrace &trace) {
	if (trace.empty()) {
		return;
	}
	auto &node = result.AddObject("execution_regions");
	AddExecutionRegionSummary(node, context, SummarizeExecutionRegionTrace(trace), Micros(query_runtime_seconds));
	AddExecutionRegionEvents(node, trace);
}

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false),
      execution_region_trace(make_uniq<QueryProfilerExecutionRegionTrace>()), metrics_finalized(false) {
}

QueryProfiler::~QueryProfiler() = default;

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze || ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return !is_explain_analyze && ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat(ExplainFormat format) const {
	auto print_format = ClientConfig::GetConfig(context).profiler_print_format;
	switch (format) {
	case ExplainFormat::DEFAULT:
		if (print_format != ProfilerPrintFormat::NO_OUTPUT) {
			return print_format;
		}
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case ExplainFormat::TEXT:
		return ProfilerPrintFormat::QUERY_TREE;
	case ExplainFormat::JSON:
		return ProfilerPrintFormat::JSON;
	case ExplainFormat::HTML:
		return ProfilerPrintFormat::HTML;
	case ExplainFormat::GRAPHVIZ:
		return ProfilerPrintFormat::GRAPHVIZ;
	case ExplainFormat::MERMAID:
		return ProfilerPrintFormat::MERMAID;
	default:
		throw NotImplementedException("No mapping from ExplainFormat::%s to ProfilerPrintFormat",
		                              EnumUtil::ToString(format));
	}
}

ExplainFormat QueryProfiler::GetExplainFormat(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return ExplainFormat::TEXT;
	case ProfilerPrintFormat::JSON:
		return ExplainFormat::JSON;
	case ProfilerPrintFormat::HTML:
		return ExplainFormat::HTML;
	case ProfilerPrintFormat::GRAPHVIZ:
		return ExplainFormat::GRAPHVIZ;
	case ProfilerPrintFormat::MERMAID:
		return ExplainFormat::MERMAID;
	case ProfilerPrintFormat::NO_OUTPUT:
		throw InternalException("Should not attempt to get ExplainFormat for ProfilerPrintFormat::NO_OUTPUT");
	default:
		throw NotImplementedException("No mapping from ProfilePrintFormat::%s to ExplainFormat",
		                              EnumUtil::ToString(format));
	}
}

bool QueryProfiler::PrintOptimizerOutput() const {
	if (GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled()) {
		return true;
	}
	if (metrics) {
		return metrics->MetricIsTracked("optimizer.join_order");
	}
	// Fall back to checking tracked_metrics patterns directly
	auto &config = ClientConfig::GetConfig(context);
	for (const auto &pattern : config.tracked_metrics) {
		if (pattern == "*" || StringUtil::StartsWith(pattern, "optimizer")) {
			return true;
		}
	}
	return false;
}

string QueryProfiler::GetSaveLocation() const {
	return is_explain_analyze ? string() : ClientConfig::GetConfig(context).profiler_save_location;
}

QueryProfiler &QueryProfiler::Get(ClientContext &context) {
	return *ClientData::Get(context).profiler;
}

void QueryProfiler::Start(const string &query) {
	Reset();
	running = true;
	query_metrics.query_sql = query;
	query_metrics.latency_timer = make_uniq<MetricsTimer>(StartTimer<MetricQueryTotalTime>());
}

void QueryProfiler::Reset() {
	tree_map.clear();
	root = nullptr;
	metrics.reset();
	running = false;
	query_metrics.Reset();
	execution_region_trace->clear();
	result_tree.reset();
	metrics_finalized = false;
}

void QueryProfiler::StartQuery(const string &query, bool is_explain_analyze_p, bool start_at_optimizer) {
	lock_guard<std::mutex> guard(lock);
	// Always reset byte counters at the start of each query so the progress bar shows per-query values
	query_metrics.bytes_read = 0;
	query_metrics.bytes_written = 0;
	if (is_explain_analyze_p) {
		StartExplainAnalyze();
	}
	if (!IsEnabled()) {
		return;
	}
	if (start_at_optimizer && !PrintOptimizerOutput()) {
		// This is the StartQuery call before the optimizer, but we don't have to print optimizer output
		return;
	}
	if (running) {
		// Called while already running: this should only happen when we print optimizer output
		// D_ASSERT(PrintOptimizerOutput());
		return;
	}
	Start(query);
}

bool QueryProfiler::OperatorRequiresProfiling(const PhysicalOperatorType op_type) {
	const auto &config = ClientConfig::GetConfig(context);
	if (config.profiling_coverage == ProfilingCoverage::ALL) {
		return true;
	}

	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::RECURSIVE_KEY_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
	case PhysicalOperatorType::EXTENSION:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

bool QueryProfiler::IsExplainAnalyze() const {
	return is_explain_analyze;
}

void QueryProfiler::EndQuery() {
	unique_lock<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	FinalizeMetricsInternal();
	running = false;
	bool emit_output = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze && ClientConfig::GetConfig(context).emit_profiler_output) {
		emit_output = true;
	}

	is_explain_analyze = false;

	// To log is inexpensive, whether to log or not depends on whether logging is active
	ToLogInternal();

	guard.unlock();

	if (emit_output) {
		string tree = ToString();
		auto save_location = GetSaveLocation();

		if (save_location.empty()) {
			Printer::Print(tree);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), tree);
		}
	}
}

void QueryProfiler::FinalizeMetrics() {
	lock_guard<std::mutex> guard(lock);
	FinalizeMetricsInternal();
}

void QueryProfiler::TrackBytesRead(const idx_t amount) {
	query_metrics.UpdateBytesRead(amount);
}

void QueryProfiler::TrackBytesWritten(const idx_t amount) {
	query_metrics.UpdateBytesWritten(amount);
}

void QueryProfiler::TrackTotalMemoryAllocated(const idx_t amount) {
	query_metrics.UpdateTotalMemoryAllocated(amount);
}

void QueryProfiler::AddToMetricCounter(const string &key, const idx_t amount) {
	if (IsEnabled()) {
		query_metrics.UpdateMetricCounter(key, amount);
	}
}

void QueryProfiler::SetMetric(const string &key, Value new_value) {
	if (!IsEnabled()) {
		return;
	}
	metrics->SetMetric(key, std::move(new_value));
}

bool QueryProfiler::MetricIsTracked(const string &key) const {
	if (!IsEnabled()) {
		return false;
	}
	return metrics->MetricIsTracked(key);
}

idx_t QueryProfiler::GetBytesRead() const {
	return query_metrics.GetBytesRead();
}

idx_t QueryProfiler::GetBytesWritten() const {
	return query_metrics.GetBytesWritten();
}

MetricsTimer QueryProfiler::StartTimerInternal(const string &key) {
	return MetricsTimer(query_metrics, key, IsEnabled());
}

string QueryProfiler::ToString(ExplainFormat explain_format) const {
	return ToString(GetPrintFormat(explain_format));
}

string QueryProfiler::ToString(ProfilerPrintFormat format) const {
	if (!IsEnabled()) {
		return RenderDisabledMessage(format);
	}
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::HTML:
	case ProfilerPrintFormat::GRAPHVIZ:
	case ProfilerPrintFormat::MERMAID: {
		lock_guard<std::mutex> guard(lock);
		// checking the tree to ensure the query is really empty
		// the query string is empty when a logical plan is deserialized
		if (query_metrics.query_sql.empty() || !root) {
			return "";
		}
		auto renderer = TreeRenderer::CreateRenderer(GetExplainFormat(format));
		stringstream str;
		renderer->Render(*root, str);
		return str.str();
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}
	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}
	active_operator = phys_op;

	if (!OperatorMetricsIsInitialized(*active_operator)) {
		// first time calling into this operator - fetch the info
		auto &info = GetOperatorMetrics(*active_operator);
		info.SetExtraInfo(active_operator->ParamsToString());
	}

	// Start the timing of the current operator.
	op.Start();
}

void OperatorMetrics::GatherMetrics(ClientContext &context, double elapsed_time, optional_ptr<DataChunk> chunk) {
	time += elapsed_time;
	if (chunk) {
		elements_returned += chunk->size();
		intermediate_size_bytes += LossyNumericCast<idx_t>(chunk->GetDataSize());
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto used_memory = buffer_manager.GetBufferPool().GetUsedMemory(false);
	if (used_memory > system_peak_buffer_manager_memory) {
		system_peak_buffer_manager_memory = used_memory;
	}
	auto used_swap = buffer_manager.GetUsedSwap();
	if (used_swap > system_peak_temp_directory_size) {
		system_peak_temp_directory_size = used_swap;
	}
}

void OperatorMetrics::MergeInternal(const OperatorMetrics &other) {
	time += other.time;
	elements_returned += other.elements_returned;
	intermediate_size_bytes += other.intermediate_size_bytes;
	rows_scanned += other.rows_scanned;
	row_groups_scanned += other.row_groups_scanned;
	if (other.system_peak_buffer_manager_memory > system_peak_buffer_manager_memory) {
		system_peak_buffer_manager_memory = other.system_peak_buffer_manager_memory;
	}
	if (other.system_peak_temp_directory_size > system_peak_temp_directory_size) {
		system_peak_temp_directory_size = other.system_peak_temp_directory_size;
	}
}

void OperatorMetrics::Accumulate(const OperatorMetrics &other) {
	MergeInternal(other);
	total_row_groups_to_scan += other.total_row_groups_to_scan;
}

void OperatorMetrics::Merge(const OperatorMetrics &other) {
	MergeInternal(other);
	total_row_groups_to_scan = MaxValue<idx_t>(total_row_groups_to_scan, other.total_row_groups_to_scan);
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while no operator is active");
	}

	auto &info = GetOperatorMetrics(*active_operator);
	op.End();
	info.GatherMetrics(context, op.Elapsed(), chunk);
	active_operator = nullptr;
}

void OperatorProfiler::FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call FinishSource while no operator is active");
	}
	FinishSource(*active_operator, gstate, lstate);
}

void OperatorProfiler::FinishSource(const PhysicalOperator &phys_op, GlobalSourceState &gstate,
                                    LocalSourceState &lstate) {
	if (phys_op.type == PhysicalOperatorType::TABLE_SCAN) {
		const auto &table_scan = phys_op.Cast<PhysicalTableScan>();
		auto &scan_metrics = GetOperatorMetrics(phys_op);
		table_scan.GetMetrics(context, gstate, lstate, scan_metrics);
	}
}

bool OperatorProfiler::OperatorMetricsIsInitialized(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	return entry != operator_metrics.end();
}

OperatorMetrics &OperatorProfiler::GetOperatorMetrics(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	if (entry != operator_metrics.end()) {
		return entry->second;
	}

	// Add a new entry.
	operator_metrics[phys_op] = OperatorMetrics();
	return operator_metrics[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	if (entry == operator_metrics.end()) {
		return;
	}

	auto &info = entry->second;
	if (info.name.empty()) {
		info.name = EnumUtil::ToString(phys_op.type);
	}
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.operator_metrics) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetOperatorMetrics();
		info.Merge(node.second);
		// Update extra_info from the per-thread metrics: these are set during execution (StartOperator),
		// so they capture runtime values like dynamic filters that aren't known at plan-creation time.
		if (!node.second.GetExtraInfo().empty()) {
			info.SetExtraInfo(node.second.GetExtraInfo());
		}

		if (node.second.system_peak_buffer_manager_memory > query_metrics.system_peak_buffer_memory) {
			query_metrics.system_peak_buffer_memory = node.second.system_peak_buffer_manager_memory;
		}
		if (node.second.system_peak_temp_directory_size > query_metrics.system_peak_temp_dir_size) {
			query_metrics.system_peak_temp_dir_size = node.second.system_peak_temp_directory_size;
		}
		node.second.ResetMetrics();
	}
}

void QueryProfiler::SetBlockedTime(const double &blocked_thread_time) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	query_metrics.blocked_thread_time = blocked_thread_time;
}

bool QueryProfiler::AcceptsExecutionRegionEvents() const {
	lock_guard<std::mutex> guard(lock);
	return IsEnabled() && running;
}

void QueryProfiler::RecordExecutionRegionEvent(const ExecutionRegionEvent &event) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	execution_region_trace->push_back(event);
	result_tree.reset();
}

string QueryProfiler::DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		auto half_spaces = width / 2;
		auto extra_left_space = NumericCast<idx_t>(width % 2 != 0 ? 1 : 0);
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = NumericCast<char>(toupper(str[0]));
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = NumericCast<char>(toupper(str[i + 1]));
			}
		}
	}
	return str;
}

static string RenderTiming(double timing) {
	string timing_s;
	if (timing >= 1) {
		timing_s = StringUtil::Format("%.2f", timing);
	} else if (timing >= 0.1) {
		timing_s = StringUtil::Format("%.3f", timing);
	} else {
		timing_s = StringUtil::Format("%.4f", timing);
	}
	return timing_s + "s";
}

string QueryProfiler::QueryTreeToString() const {
	duckdb::stringstream str;
	QueryTreeToStream(str);
	return str.str();
}

void RenderPhaseTimings(std::ostream &ss, const pair<string, double> &head, map<string, double> &timings, idx_t width) {
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│" + QueryProfiler::DrawPadded(RenderTitleCase(head.first) + ": " + RenderTiming(head.second), width - 2) +
	          "│\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";

	for (const auto &entry : timings) {
		ss << "││" +
		          QueryProfiler::DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
		                                    width - 4) +
		          "││\n";
	}
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
}

void PrintPhaseTimingsToStream(std::ostream &ss, const GatheredMetrics &info, idx_t width) {
	map<string, double> optimizer_timings;
	map<string, double> planner_timings;
	map<string, double> parser_timings;
	map<string, double> physical_planner_timings;

	pair<string, double> optimizer_head;
	pair<string, double> planner_head;
	pair<string, double> parser_head;
	pair<string, double> physical_planner_head;

	for (const auto &entry : info.GetMetrics()) {
		const auto &metric = entry.first;
		// Check specific total_time metrics BEFORE group checks — MetricInGroup would otherwise match these first.
		if (MetricsUtils::IsMetric<MetricOptimizerTotalTime>(metric)) {
			optimizer_head = {"Optimizer", entry.second.GetValue<double>()};
		} else if (MetricsUtils::IsMetric<MetricPhysicalPlannerTotalTime>(metric)) {
			physical_planner_head = {"Physical Planner", entry.second.GetValue<double>()};
		} else if (MetricsUtils::IsMetric<MetricPlannerTotalTime>(metric)) {
			planner_head = {"Planner", entry.second.GetValue<double>()};
		} else if (MetricsUtils::IsMetric<MetricParserTotalTime>(metric)) {
			parser_head = {"Parser", entry.second.GetValue<double>()};
		} else if (MetricsUtils::MetricInGroup(metric, "optimizer")) {
			// "optimizer.expression_rewriter" -> display as "expression_rewriter"
			optimizer_timings[metric.substr(10)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::MetricInGroup(metric, "physical_planner")) {
			// "physical_planner.column_binding" -> display as "column_binding"
			physical_planner_timings[metric.substr(17)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::IsMetric<MetricPlannerBindingTime>(metric)) {
			planner_timings["binding_time"] = entry.second.GetValue<double>();
		}
	}

	if (!optimizer_head.first.empty()) {
		RenderPhaseTimings(ss, optimizer_head, optimizer_timings, width);
	}
	if (!physical_planner_head.first.empty()) {
		RenderPhaseTimings(ss, physical_planner_head, physical_planner_timings, width);
	}
	if (!planner_head.first.empty()) {
		RenderPhaseTimings(ss, planner_head, planner_timings, width);
	}
	if (!parser_head.first.empty()) {
		RenderPhaseTimings(ss, parser_head, parser_timings, width);
	}
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	lock_guard<std::mutex> guard(lock);

	bool show_query_name = false;
	if (root) {
		auto &info = *metrics;
		show_query_name = info.MetricIsTracked<MetricQuerySQL>();
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << (show_query_name ? StringUtil::Replace(query_metrics.query_sql, "\n", " ") : "") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_metrics.query_sql.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(query_metrics.GetStringMetricInSeconds("query.total_time"));
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// render the main operator tree
	if (root) {
		// print phase timings
		if (PrintOptimizerOutput()) {
			PrintPhaseTimingsToStream(ss, *metrics, TOTAL_BOX_WIDTH);
		}
		RenderExecutionRegionsToStream(ss, context, query_metrics.GetStringMetricInSeconds("query.total_time"),
		                               *execution_region_trace);
		Render(*root, ss);
	}
}

Value QueryProfiler::JSONSanitize(const Value &input) {
	D_ASSERT(input.type().id() == LogicalTypeId::MAP);

	InsertionOrderPreservingMap<string> result;
	auto children = MapValue::GetChildren(input);
	for (auto &child : children) {
		auto struct_children = StructValue::GetChildren(child);
		auto key = struct_children[0].GetValue<string>();
		auto value = struct_children[1].GetValue<string>();

		if (StringUtil::StartsWith(key, "__")) {
			key = StringUtil::Replace(key, "__", "");
			key = StringUtil::Replace(key, "_", " ");
			key = StringUtil::Title(key);
		}
		result[key] = value;
	}
	return Value::MAP(result);
}

string QueryProfiler::JSONSanitize(const std::string &text) {
	string result;
	result.reserve(text.size());
	for (char i : text) {
		switch (i) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += i;
			break;
		}
	}
	return result;
}

profiler_metrics_t OperatorMetrics::GetMetrics(const GatheredMetrics &info) const {
	profiler_metrics_t result;
	if (info.MetricIsTracked<MetricOperatorType>()) {
		result["type"] = Value(EnumUtil::ToString(operator_type));
	}
	if (info.MetricIsTracked<MetricOperatorTiming>()) {
		result["timing"] = Value::DOUBLE(time);
	}
	if (info.MetricIsTracked<MetricOperatorIntermediateRows>()) {
		result["intermediate_rows"] = Value::UBIGINT(elements_returned);
	}
	if (info.MetricIsTracked<MetricOperatorIntermediateSizeBytes>()) {
		result["intermediate_size_bytes"] = Value::UBIGINT(intermediate_size_bytes);
	}
	if (info.MetricIsTracked<MetricOperatorRowsScanned>() && operator_type == PhysicalOperatorType::TABLE_SCAN) {
		result["rows_scanned"] = Value::UBIGINT(rows_scanned);
	}
	if (info.MetricIsTracked<MetricOperatorRowGroupsScanned>() && operator_type == PhysicalOperatorType::TABLE_SCAN) {
		result["row_groups_scanned"] = Value::UBIGINT(row_groups_scanned);
	}
	if (info.MetricIsTracked<MetricOperatorTotalRowGroupsToScan>() &&
	    operator_type == PhysicalOperatorType::TABLE_SCAN) {
		result["total_row_groups_to_scan"] = Value::UBIGINT(total_row_groups_to_scan);
	}
	if (info.MetricIsTracked<MetricOperatorExtraInfo>()) {
		result["extra_info"] = QueryProfiler::JSONSanitize(Value::MAP(extra_info));
	}
	return result;
}

static yyjson_mut_val *ValueToJSON(yyjson_mut_doc *doc, const Value &val) {
	if (val.IsNull()) {
		return yyjson_mut_null(doc);
	}
	auto &type = val.type();
	if (type.id() == LogicalTypeId::MAP) {
		// MAP values (e.g. extra_info) become JSON objects; multiline string values become arrays
		auto obj = yyjson_mut_obj(doc);
		for (auto &child : MapValue::GetChildren(val)) {
			auto kv = StructValue::GetChildren(child);
			auto k = kv[0].GetValue<string>();
			auto v = kv[1].GetValue<string>();
			auto key_ptr = yyjson_mut_get_str(yyjson_mut_strcpy(doc, k.c_str()));
			auto splits = StringUtil::Split(v, "\n");
			if (splits.size() > 1) {
				auto arr = yyjson_mut_arr(doc);
				for (auto &s : splits) {
					yyjson_mut_arr_add_strcpy(doc, arr, s.c_str());
				}
				yyjson_mut_obj_add_val(doc, obj, key_ptr, arr);
			} else {
				yyjson_mut_obj_add_strcpy(doc, obj, key_ptr, v.c_str());
			}
		}
		return obj;
	}
	if (type.id() == LogicalTypeId::BOOLEAN) {
		return yyjson_mut_bool(doc, val.GetValue<bool>());
	}
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return yyjson_mut_sint(doc, val.GetValue<int8_t>());
	case LogicalTypeId::SMALLINT:
		return yyjson_mut_sint(doc, val.GetValue<int16_t>());
	case LogicalTypeId::INTEGER:
		return yyjson_mut_sint(doc, val.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return yyjson_mut_sint(doc, val.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
		return yyjson_mut_uint(doc, val.GetValue<uint8_t>());
	case LogicalTypeId::USMALLINT:
		return yyjson_mut_uint(doc, val.GetValue<uint16_t>());
	case LogicalTypeId::UINTEGER:
		return yyjson_mut_uint(doc, val.GetValue<uint32_t>());
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, val.GetValue<uint64_t>());
	default:
		break;
	}
	if (type.IsIntegral()) {
		return yyjson_mut_uint(doc, val.GetValue<uint64_t>());
	}
	if (type.IsNumeric()) {
		return yyjson_mut_real(doc, val.GetValue<double>());
	}
	auto str = val.GetValue<string>();
	return yyjson_mut_strncpy(doc, str.c_str(), str.size());
}

static yyjson_mut_val *QueryProfileResultToJSON(yyjson_mut_doc *doc, const QueryProfileResult &node) {
	switch (node.kind) {
	case QueryProfileResultKind::VALUE:
		return ValueToJSON(doc, node.value);
	case QueryProfileResultKind::LIST: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &child : node.children) {
			yyjson_mut_arr_add_val(arr, QueryProfileResultToJSON(doc, *child));
		}
		return arr;
	}
	case QueryProfileResultKind::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		// Sort children alphabetically by key for deterministic output
		vector<reference<const QueryProfileResult>> sorted_children;
		sorted_children.reserve(node.children.size());
		for (auto &child : node.children) {
			sorted_children.push_back(*child);
		}
		std::sort(sorted_children.begin(), sorted_children.end(),
		          [](const QueryProfileResult &a, const QueryProfileResult &b) {
			          if (a.IsNested() != b.IsNested()) {
				          return !a.IsNested();
			          }
			          return a.key < b.key;
		          });
		for (const QueryProfileResult &child : sorted_children) {
			D_ASSERT(!child.key.empty());
			auto key_ptr = yyjson_mut_get_str(yyjson_mut_strcpy(doc, child.key.c_str()));
			yyjson_mut_obj_add_val(doc, obj, key_ptr, QueryProfileResultToJSON(doc, child));
		}
		return obj;
	}
	default:
		throw InternalException("Unknown QueryProfileResultKind");
	}
}

static string StringifyAndFree(ConvertedJSONHolder &json_holder, yyjson_mut_val *object) {
	json_holder.stringified_json = yyjson_mut_val_write_opts(
	    object, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr, nullptr, nullptr);
	if (!json_holder.stringified_json) {
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	auto result = string(json_holder.stringified_json);
	return result;
}

void QueryProfiler::ToLogInternal() const {
	if (!root) {
		return;
	}
	metrics->WriteMetricsToLog(context);
}

void QueryProfiler::ToLog() const {
	lock_guard<std::mutex> guard(lock);
	ToLogInternal();
}

static void OperatorToResultTree(const GatheredMetrics &settings, ProfilingNode &node, QueryProfileResult &result) {
	auto operator_metrics = node.GetOperatorMetrics().GetMetrics(settings);
	for (auto &entry : operator_metrics) {
		result.AddValue(entry.first, std::move(entry.second));
	}
	if (node.GetChildCount() > 0) {
		auto &children_list = result.AddList("children");
		for (idx_t i = 0; i < node.GetChildCount(); i++) {
			auto &child_result = children_list.AppendObject();
			OperatorToResultTree(settings, *node.GetChild(i), child_result);
		}
	}
}

struct LegacyCumulative {
	double timing = 0;
	uint64_t cardinality = 0;
	uint64_t rows_scanned = 0;
};

static LegacyCumulative LegacyOperatorToResultTree(const GatheredMetrics &info, ProfilingNode &node,
                                                   QueryProfileResult &result) {
	auto operator_metrics = node.GetOperatorMetrics().GetMetrics(info);

	auto emit_as = [&](const string &old_key, const string &new_key) {
		auto it = operator_metrics.find(old_key);
		if (it != operator_metrics.end()) {
			result.AddValue(new_key, it->second);
		}
	};

	emit_as("type", "operator_type");
	emit_as("timing", "operator_timing");
	emit_as("rows_scanned", "operator_rows_scanned");
	emit_as("intermediate_rows", "operator_cardinality");
	emit_as("intermediate_size_bytes", "result_set_size");

	auto it_extra = operator_metrics.find("extra_info");
	if (it_extra != operator_metrics.end()) {
		result.AddValue("extra_info", it_extra->second);
	}
	result.AddValue("system_peak_buffer_memory", Value::UBIGINT(0));
	result.AddValue("system_peak_temp_dir_size", Value::UBIGINT(0));

	LegacyCumulative cumulative;
	auto timing_it = operator_metrics.find("timing");
	if (timing_it != operator_metrics.end()) {
		cumulative.timing = timing_it->second.GetValue<double>();
	}
	auto card_it = operator_metrics.find("intermediate_rows");
	if (card_it != operator_metrics.end()) {
		cumulative.cardinality = card_it->second.GetValue<uint64_t>();
	}
	auto rows_it = operator_metrics.find("rows_scanned");
	if (rows_it != operator_metrics.end()) {
		cumulative.rows_scanned = rows_it->second.GetValue<uint64_t>();
	}

	if (node.GetChildCount() > 0) {
		auto &children_list = result.AddList("children");
		for (idx_t i = 0; i < node.GetChildCount(); i++) {
			auto &child_result = children_list.AppendObject();
			auto child_cum = LegacyOperatorToResultTree(info, *node.GetChild(i), child_result);
			cumulative.timing += child_cum.timing;
			cumulative.cardinality += child_cum.cardinality;
			cumulative.rows_scanned += child_cum.rows_scanned;
		}
	}

	result.AddValue("cpu_time", Value::DOUBLE(cumulative.timing));
	result.AddValue("cumulative_cardinality", Value::UBIGINT(cumulative.cardinality));
	result.AddValue("cumulative_rows_scanned", Value::UBIGINT(cumulative.rows_scanned));
	return cumulative;
}

unique_ptr<QueryProfileResult> QueryProfiler::ToLegacyResultTree() const {
	auto result = make_uniq<QueryProfileResult>();
	if (!root) {
		result->AddValue("result", Value(query_metrics.query_sql.empty() ? "empty" : "error"));
		return result;
	}

	const auto &gathered = metrics->GetMetrics();

	auto emit = [&](const string &new_key, const string &old_key) {
		auto it = gathered.find(old_key);
		if (it != gathered.end()) {
			result->AddValue(new_key, it->second);
		}
	};

	emit("total_memory_allocated", "system.total_memory_allocated");
	emit("total_bytes_written", "io.total_bytes_written");
	emit("total_bytes_read", "io.total_bytes_read");
	emit("system_peak_temp_dir_size", "system.peak_temp_dir_size");
	emit("system_peak_buffer_memory", "system.peak_buffer_memory");

	// rows_returned = root operator's elements_returned (rows sent to client)
	{
		auto root_op_metrics = root->GetOperatorMetrics().GetMetrics(*metrics);
		auto it = root_op_metrics.find("intermediate_rows");
		if (it != root_op_metrics.end()) {
			result->AddValue("rows_returned", it->second);
		}
	}

	emit("result_set_size", "query.total_intermediate_size_bytes");
	emit("latency", "query.total_time");
	emit("wal_replay_entry_count", "storage.wal_replay_entry_count");
	result->AddValue("extra_info", Value::MAP(InsertionOrderPreservingMap<string>()));
	emit("commit_local_storage_latency", "storage.commit_local_storage_latency");
	emit("attach_load_storage_latency", "storage.attach_load_storage_latency");
	emit("query_name", "query.sql");
	emit("cpu_time", "query.cpu_time");
	emit("checkpoint_latency", "storage.checkpoint_latency");
	emit("cumulative_cardinality", "query.total_intermediate_rows");
	emit("waiting_to_attach_latency", "storage.waiting_to_attach_latency");
	emit("write_to_wal_latency", "storage.write_to_wal_latency");
	emit("attach_replay_wal_latency", "storage.attach_replay_wal_latency");
	emit("blocked_thread_time", "system.blocked_thread_time");
	emit("cumulative_rows_scanned", "query.total_rows_scanned");
	emit("total_vacuum_time", "storage.total_vacuum_time");

	auto &children_list = result->AddList("children");
	auto &root_node = children_list.AppendObject();
	LegacyOperatorToResultTree(*metrics, *root, root_node);
	AppendExecutionRegionsToResult(*result, context, query_metrics.GetStringMetricInSeconds("query.total_time"),
	                               *execution_region_trace);
	return result;
}

unique_ptr<QueryProfileResult> QueryProfiler::ToResultTree() const {
	if (Settings::Get<LegacyMetricsFormatSetting>(context)) {
		return ToLegacyResultTree();
	}
	auto result = make_uniq<QueryProfileResult>();
	if (!root) {
		result->AddValue("result", Value(query_metrics.query_sql.empty() ? "empty" : "error"));
		return result;
	}
	metrics->MetricsToProfileResult(*result);
	if (metrics->AnyOperatorMetricTracked()) {
		auto &op_list = result->AddList("operator");
		auto &op_node = op_list.AppendObject();
		OperatorToResultTree(*metrics, *root, op_node);
	}
	AppendExecutionRegionsToResult(*result, context, query_metrics.GetStringMetricInSeconds("query.total_time"),
	                               *execution_region_trace);
	return result;
}

QueryProfileResult &QueryProfiler::GetResult() {
	lock_guard<std::mutex> guard(lock);
	if (!result_tree) {
		result_tree = ToResultTree();
	}
	return *result_tree;
}

bool QueryProfiler::HasRoot() const {
	return root != nullptr;
}

string QueryProfiler::ToJSON() const {
	lock_guard<std::mutex> guard(lock);
	ConvertedJSONHolder json_holder;
	json_holder.doc = yyjson_mut_doc_new(nullptr);
	auto result = ToResultTree();
	auto root_val = QueryProfileResultToJSON(json_holder.doc, *result);
	yyjson_mut_doc_set_root(json_holder.doc, root_val);
	return StringifyAndFree(json_holder, root_val);
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
	auto file = fs.OpenFile(path, flags);
	file->Write((void *)info.c_str(), info.size());
	file->Close();
}

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, const idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	auto node = make_uniq<ProfilingNode>();
	auto &info = node->GetOperatorMetrics();
	node->depth = depth;

	info.name = EnumUtil::ToString(root_p.type);
	info.operator_type = root_p.type;
	auto params = root_p.ParamsToString();
	info.SetExtraInfo(std::move(params));

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), depth + 1);
		node->AddChild(std::move(child_node));
	}
	return node;
}

string QueryProfiler::RenderDisabledMessage(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
	case ProfilerPrintFormat::HTML:
		return R"(
				<!DOCTYPE html>
                <html lang="en"><head/><body>
                  Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!
                </body></html>
			)";
	case ProfilerPrintFormat::GRAPHVIZ:
		return R"(
				digraph G {
				    node [shape=box, style=rounded, fontname="Courier New", fontsize=10];
				    node_0_0 [label="Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!"];
				}
			)";
	case ProfilerPrintFormat::MERMAID:
		return R"(flowchart TD
    node_0_0["`**DISABLED**
Query profiling is disabled.
Use 'PRAGMA enable_profiling;' to enable profiling!`"]
)";
	case ProfilerPrintFormat::JSON: {
		ConvertedJSONHolder json_holder;
		json_holder.doc = yyjson_mut_doc_new(nullptr);
		auto result_obj = yyjson_mut_obj(json_holder.doc);
		yyjson_mut_doc_set_root(json_holder.doc, result_obj);

		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "disabled");
		return StringifyAndFree(json_holder, result_obj);
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	query_requires_profiling = false;
	root = CreateTree(root_op, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		running = false;
		tree_map.clear();
		root = nullptr;
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		metrics = make_uniq<GatheredMetrics>(client_config.tracked_metrics);
	}
}

void QueryProfiler::Render(const ProfilingNode &node, std::ostream &ss) const {
	TextTreeRenderer renderer;
	if (IsDetailedEnabled()) {
		renderer.EnableDetailed();
	} else {
		renderer.EnableStandard();
	}
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(QueryTreeToString());
}

static void MergeOperatorMeasurements(ProfilingNode &root, OperatorMetrics &result) {
	// merge in this layer
	result.Accumulate(root.GetOperatorMetrics());
	// recurse into children
	for (idx_t i = 0; i < root.GetChildCount(); i++) {
		auto child = root.GetChild(i);
		MergeOperatorMeasurements(*child, result);
	}
}

void QueryProfiler::FinalizeMetricsInternal() {
	if (metrics_finalized || !IsEnabled() || !metrics) {
		return;
	}
	if (query_metrics.latency_timer) {
		query_metrics.latency_timer->EndTimer();
	}
	if (root) {
		OperatorMetrics cumulative_metrics;
		MergeOperatorMeasurements(*root, cumulative_metrics);
		metrics->SetMetric<MetricQueryCPUTime>(cumulative_metrics.time);
		metrics->SetMetric<MetricQueryTotalIntermediateRows>(cumulative_metrics.elements_returned);
		metrics->SetMetric<MetricQueryTotalRowsScanned>(cumulative_metrics.rows_scanned);
		metrics->SetMetric<MetricQueryTotalIntermediateSizeBytes>(cumulative_metrics.intermediate_size_bytes);
		metrics->SetMetric<MetricQueryTotalRowGroupsScanned>(cumulative_metrics.row_groups_scanned);
		metrics->SetMetric<MetricQueryTotalRowGroupsToScan>(cumulative_metrics.total_row_groups_to_scan);
	}
	query_metrics.FinalizeMetrics(*metrics);
	metrics_finalized = true;
}

} // namespace duckdb
