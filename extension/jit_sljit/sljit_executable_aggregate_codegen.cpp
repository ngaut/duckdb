//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_aggregate_codegen.hpp"

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_executable_aggregate_payload_sources.hpp"
#include "sljit_native_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>

namespace duckdb {

struct SljitFusedTypedAggregatePayloads {
	vector<SljitExecutableRegionExpression> executable_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_group_expressions;
	vector<idx_t> combined_sources;
	vector<bool> combined_source_not_null;
	vector<Value> combined_source_min_values;
	vector<Value> combined_source_max_values;
};

static bool TryBuildFusedTypedAggregatePayloads(
    const SljitNativeAggregateUpdatePlan &op, SljitFusedTypedAggregatePayloads &payloads,
    const vector<bool> *input_not_null = nullptr, const vector<Value> *input_min_values = nullptr,
    const vector<Value> *input_max_values = nullptr,
    const vector<SljitNativeRegionExpressionPlan> *group_expressions = nullptr) {
	payloads = SljitFusedTypedAggregatePayloads();
	payloads.executable_payloads.reserve(op.payloads.size());
	payloads.codegen_payloads.reserve(op.payloads.size());
	auto combined_source_not_null = input_not_null ? &payloads.combined_source_not_null : nullptr;
	auto combined_source_min_values = input_min_values ? &payloads.combined_source_min_values : nullptr;
	auto combined_source_max_values = input_max_values ? &payloads.combined_source_max_values : nullptr;

	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &aggregate = op.sink_info.aggregates[payload_idx];
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = op.payloads[payload_idx].Copy(true, false);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			payloads.executable_payloads.push_back(std::move(executable_payload));
			continue;
		}
		if (executable_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			executable_payload.plan.source_index =
			    AddSljitCombinedInputSource(executable_payload.plan.source_index, payloads.combined_sources,
			                                combined_source_not_null, input_not_null, combined_source_min_values,
			                                combined_source_max_values, input_min_values, input_max_values);
			payloads.executable_payloads.push_back(std::move(executable_payload));
			continue;
		}
		if (executable_payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !executable_payload.plan.expression_tree) {
			return false;
		}
		has_typed_payload = true;
		vector<idx_t> local_sources = executable_payload.plan.expression_tree_source_indices;
		if (local_sources.empty()) {
			return false;
		}
		RemapSljitExpressionTreeToCombinedInputs(*executable_payload.plan.expression_tree, local_sources,
		                                         payloads.combined_sources, combined_source_not_null, input_not_null,
		                                         combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
		payloads.executable_payloads.push_back(std::move(executable_payload));
	}
	if (group_expressions) {
		payloads.codegen_group_expressions.reserve(group_expressions->size());
		for (auto &group_expression : *group_expressions) {
			auto codegen_group = group_expression.Copy(true, false);
			if (codegen_group.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				if (!codegen_group.expression_tree || codegen_group.expression_tree_source_indices.empty()) {
					return false;
				}
				has_typed_payload = true;
				vector<idx_t> local_sources = codegen_group.expression_tree_source_indices;
				RemapSljitExpressionTreeToCombinedInputs(
				    *codegen_group.expression_tree, local_sources, payloads.combined_sources, combined_source_not_null,
				    input_not_null, combined_source_min_values, combined_source_max_values, input_min_values,
				    input_max_values);
			}
			payloads.codegen_group_expressions.push_back(std::move(codegen_group));
		}
	}
	if (!has_typed_payload) {
		return false;
	}

	for (auto &payload : payloads.executable_payloads) {
		payload.input_source_indices = payloads.combined_sources;
		payload.input_source_not_null = payloads.combined_source_not_null;
		payload.plan.expression_tree_source_indices = payloads.combined_sources;
		payloads.codegen_payloads.push_back(payload.plan.Copy(true, false));
	}
	for (auto &group_expression : payloads.codegen_group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			group_expression.expression_tree_source_indices = payloads.combined_sources;
		}
	}
	return true;
}

static bool TryBuildUngroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                                 SljitExecutableAggregateUpdate &executable,
                                                                 string &error, const vector<bool> &input_not_null) {
	if (!op.UsesPrimitivePayloads() || op.use_grouped_state_addresses ||
	    op.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeUngroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, fused_function, fused_error);
	auto compiled =
	    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
	if (compiled.IsExecutable()) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update = std::move(compiled);
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const SljitNativeAggregateUpdatePlan &op, SljitExecutableAggregateUpdate &executable, string &error,
    const vector<bool> &input_not_null, const vector<Value> &input_min_values, const vector<Value> &input_max_values) {
	if (!op.UsesPrimitivePayloads() || !op.use_perfect_hash_group_lookup || op.payloads.empty() ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null, &input_min_values, &input_max_values,
	                                         &op.group_expressions)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, op.sink_info.groups, payloads.codegen_group_expressions,
	    op.sink_info.aggregate_contract, payloads.combined_source_not_null, payloads.combined_source_min_values,
	    payloads.combined_source_max_values, fused_function, fused_error);
	auto compiled =
	    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
	if (compiled.IsExecutable()) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update = std::move(compiled);
		executable.fused_payload_update_owns_group_lookup = true;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildGroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                               SljitExecutableAggregateUpdate &executable,
                                                               string &error, const vector<bool> &input_not_null) {
	if (!op.UsesPrimitivePayloads() || !op.use_grouped_state_addresses || op.use_perfect_hash_group_lookup ||
	    op.payloads.empty() || op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeGroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function,
	    fused_error);
	auto compiled =
	    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
	if (compiled.IsExecutable()) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update = std::move(compiled);
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static void PopulateSljitExecutableAggregatePayloadSourceFacts(SljitExecutableRegionExpression &payload,
                                                               const vector<bool> &input_not_null) {
	auto &plan = payload.plan;
	payload.input_source_indices.clear();
	payload.input_source_not_null.clear();
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		payload.input_source_indices.push_back(plan.source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.source_index));
		return;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		payload.input_source_indices.push_back(plan.source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.source_index));
		payload.input_source_indices.push_back(plan.right_source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.right_source_index));
		return;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		payload.input_source_indices = plan.expression_tree_source_indices;
		payload.input_source_not_null.reserve(payload.input_source_indices.size());
		for (auto source_idx : payload.input_source_indices) {
			payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, source_idx));
		}
		return;
	default:
		return;
	}
}

static void PopulateSljitExecutableAggregateGroupSourceFacts(const SljitNativeAggregateUpdatePlan &op,
                                                             SljitExecutableAggregateUpdate &executable,
                                                             const vector<bool> &input_not_null) {
	auto &groups = op.sink_info.groups;
	if (!op.group_expressions.empty() && op.group_expressions.size() != groups.size()) {
		throw InternalException("SLJIT aggregate group expression count mismatch");
	}
	executable.group_source_not_null.clear();
	executable.group_source_not_null.reserve(groups.size());
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		const auto source_idx =
		    op.group_expressions.empty() ? groups[group_idx].input_index : op.group_expressions[group_idx].source_index;
		executable.group_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, source_idx));
	}
}

void SljitBuildExecutableAggregateUpdateMetadata(const SljitNativeAggregateUpdatePlan &op,
                                                 SljitExecutableAggregateUpdate &executable,
                                                 const vector<bool> &input_not_null) {
	executable.plan.sink_info = op.sink_info;
	executable.plan.input_types = op.input_types;
	executable.plan.estimated_input_count = op.estimated_input_count;
	executable.plan.distinct_key_cardinality_upper_bound = op.distinct_key_cardinality_upper_bound;
	executable.plan.group_reserve = op.group_reserve;
	executable.plan.payload_binding_state = op.payload_binding_state;
	executable.plan.use_grouped_state_addresses = op.use_grouped_state_addresses;
	executable.plan.use_perfect_hash_group_lookup = op.use_perfect_hash_group_lookup;
	executable.plan.group_expressions.reserve(op.group_expressions.size());
	for (auto &group_expression : op.group_expressions) {
		executable.plan.group_expressions.push_back(group_expression.Copy(true, false));
	}
	PopulateSljitExecutableAggregateGroupSourceFacts(op, executable, input_not_null);
	if (op.UsesPrimitivePayloads()) {
		if (op.payloads.size() != op.sink_info.aggregates.size()) {
			throw InternalException("SLJIT aggregate payload descriptor count mismatch");
		}
		executable.payload_descriptors.resize(op.payloads.size());
		for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
			if (!SljitTryBindAggregatePayloadDescriptor(op.payloads[payload_idx], op.sink_info.aggregates[payload_idx],
			                                            executable.payload_descriptors[payload_idx])) {
				throw InternalException("SLJIT aggregate payload descriptor binding failed");
			}
		}
	}
	executable.payloads.reserve(op.payloads.size());
	for (auto &payload : op.payloads) {
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = payload.Copy(true, false);
		PopulateSljitExecutableAggregatePayloadSourceFacts(executable_payload, input_not_null);
		executable.payloads.push_back(std::move(executable_payload));
	}
}

static bool SljitAggregateUpdateRequiresTypedGroupedBackend(const SljitNativeAggregateUpdatePlan &op) {
	if (!op.UsesPrimitivePayloads() || !op.use_grouped_state_addresses) {
		return false;
	}
	for (auto &payload : op.payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	for (auto &group_expression : op.group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	return false;
}

bool SljitBuildExecutableAggregateUpdatePayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                    SljitExecutableAggregateUpdate &executable, string &error,
                                                    const vector<bool> &input_not_null,
                                                    const vector<Value> &input_min_values,
                                                    const vector<Value> &input_max_values) {
	if (op.UsesPrimitivePayloads() && !op.use_grouped_state_addresses && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(op.payloads, op.sink_info.aggregates,
		                                                                         fused_function, fused_error);
		auto compiled =
		    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
		if (compiled.IsExecutable()) {
			executable.fused_payload_update = std::move(compiled);
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	if (!TryBuildUngroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null)) {
		return false;
	}
	if (executable.fused_payload_update.Function()) {
		return true;
	}
	if (!TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null,
	                                                                   input_min_values, input_max_values)) {
		return false;
	}
	if (executable.fused_payload_update.Function()) {
		return true;
	}
	if (!TryBuildGroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null)) {
		return false;
	}
	if (executable.fused_payload_update.Function()) {
		return true;
	}
	if (SljitAggregateUpdateRequiresTypedGroupedBackend(op)) {
		error = "SLJIT grouped typed aggregate update has no generated typed payload backend";
		return false;
	}
	if (op.UsesPrimitivePayloads() && op.use_perfect_hash_group_lookup && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.groups, op.group_expressions,
		    op.sink_info.aggregate_contract, fused_function, fused_error);
		auto compiled =
		    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
		if (compiled.IsExecutable()) {
			executable.fused_payload_update = std::move(compiled);
			executable.fused_payload_update_owns_group_lookup = true;
			return true;
		}
		if (!fused_error.empty()) {
			error = fused_error;
			return false;
		}
	}
	if (op.UsesPrimitivePayloads() && op.use_grouped_state_addresses && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function, fused_error);
		auto compiled =
		    SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(fused_code), fused_function);
		if (compiled.IsExecutable()) {
			executable.fused_payload_update = std::move(compiled);
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	return SljitBuildExecutableAggregateUpdateFallbackPayloadCode(op, executable, error);
}

struct SljitPrimitiveRunGroupCodegenSpecialization {
	PhysicalType source_type;
	ExecutionRowPointerGroupKeyCastKind cast_kind;
};

// Multi-lane kernels are deliberately unrolled. Bound them to one small instruction-cache budget; wider aggregate
// lists keep using the generic preaggregation loop until they have a looped generated lowering.
static constexpr idx_t SLJIT_PRIMITIVE_RUN_MAX_UNROLLED_LANES = 8;

static vector<SljitPrimitiveRunGroupCodegenSpecialization>
SljitPrimitiveRunGroupSpecializations(PhysicalType target_type) {
	vector<SljitPrimitiveRunGroupCodegenSpecialization> result {
	    {target_type, ExecutionRowPointerGroupKeyCastKind::NONE}};
	switch (target_type) {
	case PhysicalType::INT8:
		result.push_back({PhysicalType::INT32, ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8});
		break;
	case PhysicalType::INT16:
		result.push_back({PhysicalType::INT64, ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16});
		break;
	case PhysicalType::INT32:
		result.push_back({PhysicalType::INT64, ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32});
		break;
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		for (auto source_type : {PhysicalType::INT8, PhysicalType::INT16, PhysicalType::INT32, PhysicalType::INT64}) {
			if (SljitPrimitiveRunGroupCastSupported(source_type, target_type,
			                                        ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS)) {
				result.push_back({source_type, ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS});
			}
		}
		break;
	default:
		break;
	}
	return result;
}

void SljitPlanExecutablePrimitiveRunUpdate(const SljitNativeAggregateUpdatePlan &op,
                                           SljitExecutableAggregateUpdate &executable) {
	auto &sink = op.sink_info;
	if (sink.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || !op.UsesPrimitivePayloads() ||
	    !op.use_grouped_state_addresses || op.use_perfect_hash_group_lookup || sink.groups.size() != 1 ||
	    sink.aggregates.empty() || sink.aggregates.size() != executable.payload_descriptors.size() ||
	    sink.aggregates.size() > SLJIT_PRIMITIVE_RUN_MAX_UNROLLED_LANES || !SljitPrimitiveRunCodegenSupported()) {
		return;
	}
	auto group_type = sink.groups[0].type.InternalType();
	vector<PhysicalType> payload_types;
	vector<AggregatePrimitiveUpdateKind> primitive_kinds;
	payload_types.reserve(executable.payload_descriptors.size());
	primitive_kinds.reserve(executable.payload_descriptors.size());
	for (auto &descriptor : executable.payload_descriptors) {
		auto primitive_kind = descriptor.primitive_kind;
		PhysicalType payload_type = PhysicalType::INVALID;
		switch (primitive_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			payload_type = descriptor.input_type;
			break;
		default:
			return;
		}
		if (!SljitPrimitiveRunPayloadSupported(payload_type, primitive_kind, false) ||
		    (executable.payload_descriptors.size() == 1 && primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
		     !SljitPrimitiveRunPayloadSupported(payload_type, primitive_kind, true))) {
			return;
		}
		payload_types.push_back(payload_type);
		primitive_kinds.push_back(primitive_kind);
	}
	for (auto group_specialization : SljitPrimitiveRunGroupSpecializations(group_type)) {
		if (!SljitPrimitiveRunGroupCastSupported(group_specialization.source_type, group_type,
		                                         group_specialization.cast_kind)) {
			continue;
		}
		SljitExecutablePrimitiveRunSpecialization specialization;
		specialization.group_source_type = group_specialization.source_type;
		specialization.group_cast_kind = group_specialization.cast_kind;
		executable.primitive_run_update.flat_specializations.push_back(std::move(specialization));
	}
	if (executable.primitive_run_update.flat_specializations.empty()) {
		return;
	}
	executable.primitive_run_update.group_type = group_type;
	executable.primitive_run_update.payload_types = std::move(payload_types);
	executable.primitive_run_update.primitive_kinds = std::move(primitive_kinds);
}

SljitNativePrimitiveRunFunction
SljitEnsureExecutablePrimitiveRunUpdate(ExecutionRegionRuntime &runtime, SljitExecutablePrimitiveRunUpdate &run_update,
                                        PhysicalType group_source_type,
                                        ExecutionRowPointerGroupKeyCastKind group_cast_kind, bool payload_nullable) {
	auto specialization = run_update.Specialization(group_source_type, group_cast_kind);
	if (!specialization || !run_update.HasDeferredCodegen()) {
		return nullptr;
	}
	auto &artifact = run_update.primitive_kinds.size() > 1
	                     ? specialization->multi_lane_compiled
	                     : (payload_nullable ? specialization->nullable_compiled : specialization->compiled);
	return artifact.Ensure([&]() {
		SljitNativePrimitiveRunFunction function = nullptr;
		string error;
		unique_ptr<ExecutionRegionCodeHandle> code;
		ExecutionRegionCompileTimings timings;
		auto codegen_start = std::chrono::steady_clock::now();
		{
			SljitCodegenTimingScope codegen_timing_scope(&timings);
			if (run_update.primitive_kinds.size() == 1) {
				code = BuildSljitNativePrimitiveRunUpdate(group_source_type, run_update.group_type, group_cast_kind,
				                                          run_update.payload_types[0], run_update.primitive_kinds[0],
				                                          payload_nullable, function, error);
			} else {
				code = BuildSljitNativePrimitiveRunMultiUpdate(group_source_type, run_update.group_type,
				                                               group_cast_kind, run_update.payload_types,
				                                               run_update.primitive_kinds, function, error);
			}
		}
		if (!code || !function) {
			throw InternalException("SLJIT primitive run update lazy code generation failed: %s",
			                        error.empty() ? "unknown error" : error);
		}
		ExecutionRegionLazyCodegenMetrics metrics;
		metrics.codegen_time_us =
		    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - codegen_start)
		        .count();
		metrics.machine_codegen_time_us = timings.machine_codegen_time_us;
		metrics.code_size = code->CodeSize();
		runtime.RecordLazyCodegen(metrics);
		return SljitCompiledFunction<SljitNativePrimitiveRunFunction>(std::move(code), function);
	});
}

void SljitSelectExecutableAggregateDirectUpdatePlan(SljitExecutableAggregateUpdate &executable) {
	auto &direct_update = executable.grouped_direct_update;
	direct_update.Clear();
	auto &plan = executable.plan;
	if (plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || !plan.UsesPrimitivePayloads() ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    executable.fused_payload_update_owns_group_lookup) {
		return;
	}
	if (executable.fused_payload_update.Function() &&
	    SljitFusedAggregatePayloadsUseTypedExpressionTrees(executable.payloads, executable.payload_descriptors)) {
		direct_update.kind = SljitGroupedAggregateDirectUpdatePlanKind::DIRECT_STATE_ADDRESS_PAYLOAD_ONLY;
		return;
	}
	direct_update.kind = SljitGroupedAggregateDirectUpdatePlanKind::ADAPTIVE_GROUPED_STATE_ADDRESS;
}

} // namespace duckdb
