#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

const char *JitCompileTargetToString(JitCompileTarget target) {
	switch (target) {
	case JitCompileTarget::REGION:
		return "region";
	default:
		return "unknown";
	}
}

const char *JitCompileStatusToString(JitCompileStatus status) {
	switch (status) {
	case JitCompileStatus::COMPILED:
		return "compiled";
	case JitCompileStatus::SKIPPED:
		return "skipped";
	case JitCompileStatus::UNSUPPORTED:
		return "unsupported";
	case JitCompileStatus::UNAVAILABLE:
		return "unavailable";
	case JitCompileStatus::DISABLED:
		return "disabled";
	case JitCompileStatus::ERROR:
		return "error";
	default:
		return "unknown";
	}
}

const char *JitExecutionModeToString(JitExecutionMode mode) {
	switch (mode) {
	case JitExecutionMode::NONE:
		return "none";
	case JitExecutionMode::NATIVE:
		return "native";
	case JitExecutionMode::EXECUTOR_FALLBACK:
		return "executor_fallback";
	case JitExecutionMode::UNSUPPORTED:
		return "unsupported";
	default:
		return "unknown";
	}
}

const char *JitRegionExecutionFormToString(JitRegionExecutionForm form) {
	switch (form) {
	case JitRegionExecutionForm::NONE:
		return "none";
	case JitRegionExecutionForm::FUSED:
		return "fused";
	default:
		return "unknown";
	}
}

const char *JitLoweringKindToString(JitLoweringKind kind) {
	switch (kind) {
	case JitLoweringKind::NATIVE:
		return "native";
	case JitLoweringKind::HELPER_CALL:
		return "helper-call";
	case JitLoweringKind::FALLBACK:
		return "fallback";
	case JitLoweringKind::PASS_THROUGH:
		return "pass-through";
	default:
		return "unknown";
	}
}

const char *JitRegionIRNodeKindToString(JitRegionIRNodeKind kind) {
	switch (kind) {
	case JitRegionIRNodeKind::SOURCE:
		return "source";
	case JitRegionIRNodeKind::FILTER:
		return "filter";
	case JitRegionIRNodeKind::PROJECTION:
		return "projection";
	case JitRegionIRNodeKind::OPERATOR:
		return "operator";
	case JitRegionIRNodeKind::SINK:
		return "sink";
	default:
		return "unknown";
	}
}

const char *JitRegionABIToString(JitRegionABI abi) {
	switch (abi) {
	case JitRegionABI::NONE:
		return "none";
	case JitRegionABI::CHUNK_TRANSFORM:
		return "chunk_transform";
	case JitRegionABI::SOURCE_PREFIX:
		return "source_prefix";
	case JitRegionABI::SINK_SUFFIX:
		return "sink_suffix";
	case JitRegionABI::FULL_PIPELINE:
		return "full_pipeline";
	case JitRegionABI::STATE_SCAN:
		return "state_scan";
	default:
		return "unknown";
	}
}

bool JitRegionABIOwnsSource(JitRegionABI abi) {
	return abi == JitRegionABI::SOURCE_PREFIX || abi == JitRegionABI::FULL_PIPELINE ||
	       abi == JitRegionABI::STATE_SCAN;
}

bool JitRegionABIOwnsSink(JitRegionABI abi) {
	return abi == JitRegionABI::SINK_SUFFIX || abi == JitRegionABI::FULL_PIPELINE;
}

bool JitRegionABIIsChunkTransform(JitRegionABI abi) {
	return abi == JitRegionABI::CHUNK_TRANSFORM;
}

bool JitRegionABIIsSourcePipeline(JitRegionABI abi) {
	return abi == JitRegionABI::SOURCE_PREFIX || abi == JitRegionABI::STATE_SCAN;
}

bool JitRegionABIIsSinkPipeline(JitRegionABI abi) {
	return abi == JitRegionABI::SINK_SUFFIX;
}

bool JitRegionABIIsFullPipeline(JitRegionABI abi) {
	return abi == JitRegionABI::FULL_PIPELINE;
}

const char *JitRegionCandidateScopeToString(JitRegionCandidateScope scope) {
	switch (scope) {
	case JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL:
		return "post_source_operator_interval";
	case JitRegionCandidateScope::SOURCE_PIPELINE:
		return "source_pipeline";
	case JitRegionCandidateScope::SINK_PIPELINE:
		return "sink_pipeline";
	case JitRegionCandidateScope::FULL_PIPELINE:
		return "full_pipeline";
	default:
		return "unknown";
	}
}

const char *JitRegionSourceKindToString(JitRegionSourceKind kind) {
	switch (kind) {
	case JitRegionSourceKind::NONE:
		return "none";
	case JitRegionSourceKind::DUCKDB_TABLE_SCAN:
		return "duckdb-table-scan";
	case JitRegionSourceKind::TABLE_FUNCTION_SCAN:
		return "table-function-scan";
	case JitRegionSourceKind::GENERIC_SCAN:
		return "generic-scan";
	case JitRegionSourceKind::STATEFUL_OPERATOR:
		return "stateful-operator";
	default:
		return "unknown";
	}
}

const char *JitRegionSourceExecutionKindToString(JitRegionSourceExecutionKind kind) {
	switch (kind) {
	case JitRegionSourceExecutionKind::NONE:
		return "none";
	case JitRegionSourceExecutionKind::DUCKDB_GETDATA_HELPER:
		return "duckdb-getdata-helper";
	case JitRegionSourceExecutionKind::EXECUTOR_FALLBACK:
		return "executor-fallback";
	case JitRegionSourceExecutionKind::NATIVE_SOURCE:
		return "native-source";
	default:
		return "unknown";
	}
}

const char *JitRegionNativeSourceStatusToString(JitRegionNativeSourceStatus status) {
	switch (status) {
	case JitRegionNativeSourceStatus::NONE:
		return "none";
	case JitRegionNativeSourceStatus::READY:
		return "ready";
	case JitRegionNativeSourceStatus::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

const char *JitRegionStateContractStatusToString(JitRegionStateContractStatus status) {
	switch (status) {
	case JitRegionStateContractStatus::NONE:
		return "none";
	case JitRegionStateContractStatus::READY:
		return "ready";
	case JitRegionStateContractStatus::MISSING:
		return "missing";
	case JitRegionStateContractStatus::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

const char *JitRegionOperatorKindToString(JitRegionOperatorKind kind) {
	switch (kind) {
	case JitRegionOperatorKind::NONE:
		return "none";
	case JitRegionOperatorKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case JitRegionOperatorKind::OPERATOR:
		return "operator";
	default:
		return "unknown";
	}
}

const char *JitRegionSinkKindToString(JitRegionSinkKind kind) {
	switch (kind) {
	case JitRegionSinkKind::NONE:
		return "none";
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case JitRegionSinkKind::SORT:
		return "sort";
	case JitRegionSinkKind::MATERIALIZATION:
		return "materialization";
	case JitRegionSinkKind::OPERATOR:
		return "operator";
	default:
		return "unknown";
	}
}

const char *JitRegionAggregateOperatorKindToString(JitRegionAggregateOperatorKind kind) {
	switch (kind) {
	case JitRegionAggregateOperatorKind::NONE:
		return "none";
	case JitRegionAggregateOperatorKind::HASH:
		return "hash";
	case JitRegionAggregateOperatorKind::PERFECT_HASH:
		return "perfect-hash";
	case JitRegionAggregateOperatorKind::UNGROUPED:
		return "ungrouped";
	default:
		return "unknown";
	}
}

const char *JitAggregateUpdateKindToString(JitAggregateUpdateKind kind) {
	switch (kind) {
	case JitAggregateUpdateKind::NONE:
		return "none";
	case JitAggregateUpdateKind::COUNT_STAR:
		return "count-star";
	case JitAggregateUpdateKind::COUNT:
		return "count";
	case JitAggregateUpdateKind::SUM:
		return "sum";
	default:
		return "unknown";
	}
}

const char *JitRegionVectorFormatKindToString(JitRegionVectorFormatKind kind) {
	switch (kind) {
	case JitRegionVectorFormatKind::NONE:
		return "none";
	case JitRegionVectorFormatKind::DATA_CHUNK:
		return "data-chunk";
	case JitRegionVectorFormatKind::UNIFIED_VECTOR:
		return "unified-vector";
	case JitRegionVectorFormatKind::FLAT_VECTOR:
		return "flat-vector";
	case JitRegionVectorFormatKind::SELECTION_VECTOR:
		return "selection-vector";
	case JitRegionVectorFormatKind::EXECUTOR_BOUNDARY:
		return "executor-boundary";
	default:
		return "unknown";
	}
}

const char *JitRegionVectorSourceKindToString(JitRegionVectorSourceKind kind) {
	switch (kind) {
	case JitRegionVectorSourceKind::NONE:
		return "none";
	case JitRegionVectorSourceKind::REGION_INPUT:
		return "region-input";
	case JitRegionVectorSourceKind::OPERATOR_OUTPUT:
		return "operator-output";
	case JitRegionVectorSourceKind::EXECUTOR_BOUNDARY:
		return "executor-boundary";
	default:
		return "unknown";
	}
}

const char *JitRegionSelectionSourceKindToString(JitRegionSelectionSourceKind kind) {
	switch (kind) {
	case JitRegionSelectionSourceKind::NONE:
		return "none";
	case JitRegionSelectionSourceKind::INPUT_SELECTION:
		return "input-selection";
	case JitRegionSelectionSourceKind::FILTER_SELECTION:
		return "filter-selection";
	case JitRegionSelectionSourceKind::EXECUTOR_BOUNDARY:
		return "executor-boundary";
	default:
		return "unknown";
	}
}

const char *JitRegionBoundaryKindToString(JitRegionBoundaryKind kind) {
	switch (kind) {
	case JitRegionBoundaryKind::NONE:
		return "none";
	case JitRegionBoundaryKind::SCAN:
		return "scan";
	case JitRegionBoundaryKind::SOURCE_NATIVE:
		return "source-native";
	case JitRegionBoundaryKind::SINK:
		return "sink";
	case JitRegionBoundaryKind::SINK_NATIVE:
		return "sink-native";
	case JitRegionBoundaryKind::OPERATOR_NATIVE:
		return "operator-native";
	case JitRegionBoundaryKind::OPERATOR_HELPER:
		return "operator-helper";
	case JitRegionBoundaryKind::OPERATOR_FALLBACK:
		return "operator-fallback";
	case JitRegionBoundaryKind::EXPRESSION_FALLBACK:
		return "expression-fallback";
	default:
		return "unknown";
	}
}

const char *JitRegionOwnershipKindToString(JitRegionOwnershipKind kind) {
	switch (kind) {
	case JitRegionOwnershipKind::NONE:
		return "none";
	case JitRegionOwnershipKind::GENERATED_IR:
		return "generated-ir";
	case JitRegionOwnershipKind::NATIVE_PROTOCOL:
		return "native-protocol";
	case JitRegionOwnershipKind::TYPED_HELPER:
		return "typed-helper";
	case JitRegionOwnershipKind::EXECUTOR_BOUNDARY:
		return "executor-boundary";
	case JitRegionOwnershipKind::MISSING_PROTOCOL:
		return "missing-protocol";
	default:
		return "unknown";
	}
}

const char *JitRegionStageKindToString(JitRegionStageKind kind) {
	switch (kind) {
	case JitRegionStageKind::SOURCE:
		return "source";
	case JitRegionStageKind::SOURCE_FILTER:
		return "source-filter";
	case JitRegionStageKind::FILTER:
		return "filter";
	case JitRegionStageKind::PROJECTION:
		return "projection";
	case JitRegionStageKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case JitRegionStageKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case JitRegionStageKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case JitRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case JitRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case JitRegionStageKind::SINK_BOUNDARY:
		return "sink-boundary";
	case JitRegionStageKind::OPERATOR_BOUNDARY:
		return "operator-boundary";
	default:
		return "unknown";
	}
}

const char *JitRegionStageExecutionKindToString(JitRegionStageExecutionKind kind) {
	switch (kind) {
	case JitRegionStageExecutionKind::NONE:
		return "none";
	case JitRegionStageExecutionKind::GENERATED_IR:
		return "generated-ir";
	case JitRegionStageExecutionKind::NATIVE_PROTOCOL:
		return "native-protocol";
	case JitRegionStageExecutionKind::TYPED_HELPER:
		return "typed-helper";
	case JitRegionStageExecutionKind::EXECUTOR_FALLBACK:
		return "executor-fallback";
	case JitRegionStageExecutionKind::MISSING_PROTOCOL:
		return "missing-protocol";
	case JitRegionStageExecutionKind::PASS_THROUGH:
		return "pass-through";
	default:
		return "unknown";
	}
}

const char *JitCompiledProtocolKindToString(JitCompiledProtocolKind kind) {
	switch (kind) {
	case JitCompiledProtocolKind::SCAN_CURSOR:
		return "scan_cursor";
	case JitCompiledProtocolKind::FILTER_STAGE:
		return "filter_stage";
	case JitCompiledProtocolKind::PROJECTION_STAGE:
		return "projection_stage";
	case JitCompiledProtocolKind::HASH_JOIN_BUILD:
		return "hash_join_build";
	case JitCompiledProtocolKind::HASH_JOIN_PROBE_CURSOR:
		return "hash_join_probe_cursor";
	case JitCompiledProtocolKind::AGGREGATE_LOOKUP:
		return "aggregate_lookup";
	case JitCompiledProtocolKind::AGGREGATE_UPDATE:
		return "aggregate_update";
	case JitCompiledProtocolKind::SINK_CURSOR:
		return "sink_cursor";
	case JitCompiledProtocolKind::STATE_SCAN_CURSOR:
		return "state_scan_cursor";
	default:
		return "none";
	}
}

const char *JitCompiledDrainKindToString(JitCompiledDrainKind kind) {
	switch (kind) {
	case JitCompiledDrainKind::ONE_INPUT_ONE_OUTPUT:
		return "one_input_one_output";
	case JitCompiledDrainKind::ZERO_OR_ONE_OUTPUT:
		return "zero_or_one_output";
	case JitCompiledDrainKind::ZERO_OR_MANY_OUTPUT:
		return "zero_or_many_output";
	case JitCompiledDrainKind::STATE_DRAIN:
		return "state_drain";
	default:
		return "none";
	}
}

const char *JitPolicyModeToString(JitPolicyMode mode) {
	switch (mode) {
	case JitPolicyMode::AUTO:
		return "auto";
	case JitPolicyMode::FORCE:
		return "force";
	case JitPolicyMode::OFF:
		return "off";
	default:
		return "unknown";
	}
}

const char *JitExpressionValidityKindToString(JitExpressionValidityKind kind) {
	switch (kind) {
	case JitExpressionValidityKind::CONSTANT_NULL:
		return "constant-null";
	case JitExpressionValidityKind::CONSTANT_VALID:
		return "constant-valid";
	case JitExpressionValidityKind::SOURCE:
		return "source";
	case JitExpressionValidityKind::NOT_NULL:
		return "not-null";
	case JitExpressionValidityKind::CHILD:
		return "child";
	case JitExpressionValidityKind::CHILDREN_NULL_PROPAGATING:
		return "children-null-propagating";
	case JitExpressionValidityKind::CHILD_OR_CAST_FAILURE:
		return "child-or-cast-failure";
	case JitExpressionValidityKind::THREE_VALUED_BOOLEAN:
		return "three-valued-boolean";
	case JitExpressionValidityKind::FIRST_VALID_CHILD:
		return "first-valid-child";
	case JitExpressionValidityKind::CONSTANT_PLUS_NULL_GUARDS:
		return "constant-plus-null-guards";
	case JitExpressionValidityKind::SQL_IN_LIST:
		return "sql-in-list";
	case JitExpressionValidityKind::SQL_BETWEEN:
		return "sql-between";
	case JitExpressionValidityKind::SELECTED_BRANCH:
		return "selected-branch";
	case JitExpressionValidityKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *JitExpressionSourceKindToString(JitExpressionSourceKind kind) {
	switch (kind) {
	case JitExpressionSourceKind::CONSTANT:
		return "constant";
	case JitExpressionSourceKind::VECTOR:
		return "vector";
	case JitExpressionSourceKind::DERIVED:
		return "derived";
	case JitExpressionSourceKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *JitExpressionExceptionKindToString(JitExpressionExceptionKind kind) {
	switch (kind) {
	case JitExpressionExceptionKind::NONE:
		return "none";
	case JitExpressionExceptionKind::CAST:
		return "cast";
	case JitExpressionExceptionKind::NULL_ON_CAST_ERROR:
		return "null-on-cast-error";
	case JitExpressionExceptionKind::ARITHMETIC:
		return "arithmetic";
	case JitExpressionExceptionKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *JitExpressionIRKindToString(JitExpressionIRKind kind) {
	switch (kind) {
	case JitExpressionIRKind::CONSTANT:
		return "constant";
	case JitExpressionIRKind::REFERENCE:
		return "reference";
	case JitExpressionIRKind::UNARY:
		return "unary";
	case JitExpressionIRKind::BINARY:
		return "binary";
	case JitExpressionIRKind::CAST:
		return "cast";
	case JitExpressionIRKind::CONJUNCTION:
		return "conjunction";
	case JitExpressionIRKind::COALESCE:
		return "coalesce";
	case JitExpressionIRKind::CONSTANT_OR_NULL:
		return "constant_or_null";
	case JitExpressionIRKind::IN_LIST:
		return "in_list";
	case JitExpressionIRKind::BETWEEN:
		return "between";
	case JitExpressionIRKind::CASE:
		return "case";
	case JitExpressionIRKind::INTRINSIC:
		return "intrinsic";
	default:
		return "unknown";
	}
}

const char *JitExpressionIntrinsicKindToString(JitExpressionIntrinsicKind kind) {
	switch (kind) {
	case JitExpressionIntrinsicKind::NONE:
		return "none";
	case JitExpressionIntrinsicKind::STRING_COMPRESS:
		return "string_compress";
	case JitExpressionIntrinsicKind::STRING_PREFIX:
		return "string_prefix";
	case JitExpressionIntrinsicKind::STRING_SUFFIX:
		return "string_suffix";
	case JitExpressionIntrinsicKind::STRING_CONTAINS:
		return "string_contains";
	case JitExpressionIntrinsicKind::STRING_LIKE:
		return "string_like";
	case JitExpressionIntrinsicKind::STRING_SUBSTRING:
		return "string_substring";
	case JitExpressionIntrinsicKind::INTEGRAL_COMPRESS:
		return "integral_compress";
	case JitExpressionIntrinsicKind::INTEGRAL_DECOMPRESS:
		return "integral_decompress";
	case JitExpressionIntrinsicKind::DATE_YEAR:
		return "date_year";
	default:
		return "unknown";
	}
}

void JitRegionLoweringPlan::AddNode(string role, string operator_name, JitLoweringKind kind, string reason) {
	JitRegionNodeLowering node;
	node.role = std::move(role);
	node.operator_name = std::move(operator_name);
	node.kind = kind;
	node.reason = std::move(reason);
	nodes.push_back(std::move(node));
}

void JitRegionLoweringPlan::AddFusionBlocker(string reason) {
	fusion_blockers.push_back(std::move(reason));
}

void JitRegionLoweringPlan::SetCompiledExecutionMode(JitExecutionMode execution_mode) {
	compiled_execution_mode = execution_mode;
}

void JitRegionLoweringPlan::SetRegionExecutionForm(JitRegionExecutionForm execution_form) {
	region_execution_form = execution_form;
}

void JitRegionLoweringPlan::SetOwnsSourceFilters(bool owns_source_filters_p) {
	owns_source_filters = owns_source_filters_p;
}

void JitRegionLoweringPlan::SetSelectedSourceExecution(JitRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void JitRegionLoweringPlan::SetOperatorStageIR(string stage_ir) {
	operator_stage_ir = std::move(stage_ir);
}

idx_t JitRegionLoweringPlan::NativeCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == JitLoweringKind::NATIVE) {
			result++;
		}
	}
	return result;
}

idx_t JitRegionLoweringPlan::HelperCallCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == JitLoweringKind::HELPER_CALL) {
			result++;
		}
	}
	return result;
}

idx_t JitRegionLoweringPlan::FallbackCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == JitLoweringKind::FALLBACK) {
			result++;
		}
	}
	return result;
}

idx_t JitRegionLoweringPlan::PassThroughCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == JitLoweringKind::PASS_THROUGH) {
			result++;
		}
	}
	return result;
}

JitExecutionMode JitRegionLoweringPlan::ExpectedCompiledExecutionMode() const {
	return compiled_execution_mode;
}

JitRegionExecutionForm JitRegionLoweringPlan::ExpectedRegionExecutionForm() const {
	return region_execution_form;
}

bool JitRegionLoweringPlan::OwnsSourceFilters() const {
	return owns_source_filters;
}

JitRegionSourceExecutionKind JitRegionLoweringPlan::SelectedSourceExecution() const {
	return selected_source_execution;
}

string JitRegionLoweringPlan::EventReason() const {
	string result = "region-lowering:native=" + std::to_string(NativeCount()) +
	                ",helper=" + std::to_string(HelperCallCount()) + ",fallback=" + std::to_string(FallbackCount()) +
	                ",pass-through=" + std::to_string(PassThroughCount()) +
	                ",execution-form=" + JitRegionExecutionFormToString(region_execution_form);
	if (selected_source_execution != JitRegionSourceExecutionKind::NONE) {
		result += ";selected-source-execution=";
		result += JitRegionSourceExecutionKindToString(selected_source_execution);
	}
	if (owns_source_filters) {
		result += ";owns-source-filters=true";
	}
	if (!operator_stage_ir.empty()) {
		result += ";";
		result += operator_stage_ir;
	}
	for (auto &blocker : fusion_blockers) {
		result += ";fusion-blocker:";
		result += blocker;
	}
	for (auto &node : nodes) {
		result += ";";
		result += node.role;
		result += ":";
		result += node.operator_name;
		result += ":";
		result += JitLoweringKindToString(node.kind);
		if (!node.reason.empty()) {
			result += ":";
			result += node.reason;
		}
	}
	return result;
}

unique_ptr<JitExpressionIR> JitExpressionIR::Copy() const {
	auto result = make_uniq<JitExpressionIR>();
	result->kind = kind;
	result->return_type = return_type;
	result->physical_type = physical_type;
	result->validity = validity;
	result->source = source;
	result->exception_behavior = exception_behavior;
	result->constant = constant;
	result->ref_index = ref_index;
	result->unary_op = unary_op;
	result->binary_op = binary_op;
	result->conjunction_op = conjunction_op;
	result->intrinsic = intrinsic;
	result->try_cast = try_cast;
	result->not_in = not_in;
	result->not_between = not_between;
	result->lower_inclusive = lower_inclusive;
	result->upper_inclusive = upper_inclusive;
	if (left) {
		result->left = left->Copy();
	}
	if (right) {
		result->right = right->Copy();
	}
	if (else_node) {
		result->else_node = else_node->Copy();
	}
	result->children.reserve(children.size());
	for (auto &child : children) {
		result->children.push_back(child->Copy());
	}
	return result;
}

} // namespace duckdb
