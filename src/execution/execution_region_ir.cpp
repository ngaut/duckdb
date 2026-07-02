#include "duckdb/execution/execution_region_lowering.hpp"

#include "execution_region_contract.hpp"
#include "execution_region_description.hpp"
#include "execution_region_signature.hpp"
#include "execution_region_stage_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_contract.hpp"

#include <algorithm>

namespace duckdb {

static bool ExecutionRegionExpressionCanUseGeneratedSourceStage(const ExecutionExpressionFragment &expression) {
	if (!expression.root) {
		return false;
	}
	return !expression.traits.has_arithmetic_binary;
}

static bool ExecutionRegionTypeCanUseGeneratedSourceStage(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

static bool ExecutionRegionSourceFilterCanUseGeneratedSourceStage(const ExecutionSourceContract &descriptor,
                                                                  const ExecutionRegionSourceFilter &filter) {
	if (!filter.expression || !ExecutionRegionExpressionCanUseGeneratedSourceStage(*filter.expression)) {
		return false;
	}
	auto &input_types = descriptor.table_scan_contract.source_contract_input_types;
	if (filter.scan_column_index >= input_types.size()) {
		return false;
	}
	return ExecutionRegionTypeCanUseGeneratedSourceStage(input_types[filter.scan_column_index]);
}

static void AddExecutionRegionSourceFilters(const ExecutionSourceContract &descriptor,
                                            ExecutionRegionSourceInfo &source,
                                            ExecutionExpressionIRMode expression_mode,
                                            ExecutionExpressionAnalysisCache *expression_cache) {
	for (auto &entry : descriptor.filters) {
		ExecutionRegionSourceFilter filter;
		filter.filter_index = entry.filter_index;
		filter.scan_column_index = entry.scan_column_index;
		filter.table_column_index = entry.table_column_index;
		if (!entry.expression) {
			filter.reason = entry.reason.empty() ? "source filter has no expression descriptor" : entry.reason;
			source.filters.push_back(std::move(filter));
			continue;
		}
		filter.expression = expression_cache
		                        ? expression_cache->Copy(*entry.expression, filter.filter_index, expression_mode)
		                        : TryLowerExecutionExpression(*entry.expression, filter.filter_index, expression_mode);
		if (!filter.expression) {
			filter.reason = DescribeExecutionExpressionLoweringFailure(*entry.expression);
		} else {
			filter.generated_source_stage_candidate =
			    ExecutionRegionSourceFilterCanUseGeneratedSourceStage(descriptor, filter);
		}
		source.filters.push_back(std::move(filter));
	}
}

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionSourceInfo(const ExecutionSourceContract &descriptor, ExecutionExpressionIRMode expression_mode,
                               ExecutionRegionIRMode mode, ExecutionExpressionAnalysisCache *expression_cache) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = descriptor.kind;
	result->execution = descriptor.execution;
	result->function_name = descriptor.function_name;
	result->estimated_source_cardinality = descriptor.estimated_source_cardinality;
	result->estimated_source_cardinality_exact = descriptor.estimated_source_cardinality_exact;
	result->output_column_count = descriptor.output_column_count;
	result->returned_column_count = descriptor.returned_column_count;
	result->column_ids = descriptor.column_ids;
	result->projection_ids = descriptor.projection_ids;
	result->projection_pushdown = descriptor.projection_pushdown;
	result->filter_pushdown = descriptor.filter_pushdown;
	result->filter_prune = descriptor.filter_prune;
	result->dynamic_filters = descriptor.dynamic_filters;
	result->in_out_function = descriptor.in_out_function;
	result->table_scan_contract = descriptor.table_scan_contract;
	result->hash_join_contract = descriptor.hash_join_contract;
	result->nested_loop_join_contract = descriptor.nested_loop_join_contract;
	result->aggregate_contract = descriptor.aggregate_contract;
	result->order_contract = descriptor.order_contract;
	result->aggregates = descriptor.aggregates;
	result->hash_join_keys = descriptor.hash_join_keys;
	result->groups = descriptor.groups;
	result->source_contract = descriptor.source_contract;
	result->native_state_scan_contract = descriptor.native_state_scan_contract;
	result->reason = descriptor.reason;
	AddExecutionRegionSourceFilters(descriptor, *result, expression_mode, expression_cache);
	result->fields = descriptor.fields;
	FinalizeExecutionRegionSourceInfo(*result, mode);
	return result;
}

static unique_ptr<ExecutionRegionOperatorInfo>
BuildExecutionRegionOperatorInfo(const ExecutionRegionOperatorInfo &descriptor, ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionOperatorInfo>(descriptor);
	FinalizeExecutionRegionOperatorInfo(*result, mode);
	return result;
}

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionGenericScanSourceInfo(const ExecutionRegionOperatorEntry &entry, const string &reason,
                                          ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = ExecutionRegionSourceKind::GENERIC_SCAN;
	result->execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result->function_name = StringUtil::Lower(entry.operator_name);
	result->output_column_count = entry.output_types.size();
	result->returned_column_count = entry.output_types.size();
	result->source_contract.status = ExecutionRegionSourceContractStatus::BLOCKED;
	result->source_contract.required_capability = "generic-scan-source-contract";
	result->source_contract.contract_version = "v1";
	result->source_contract.blocker = "generic-scan-source-boundary";
	result->reason = reason;
	FinalizeExecutionRegionSourceInfo(*result, mode);
	return result;
}

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionStatefulSourceInfo(const ExecutionRegionOperatorEntry &entry, const string &reason,
                                       ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	result->execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result->function_name = StringUtil::Lower(entry.operator_name);
	result->output_column_count = entry.output_types.size();
	result->returned_column_count = entry.output_types.size();
	result->source_contract.status = ExecutionRegionSourceContractStatus::BLOCKED;
	result->source_contract.required_capability = "stateful-operator-source-contract";
	result->source_contract.contract_version = "v1";
	result->source_contract.blocker = "stateful-source-contract-boundary";
	result->reason = reason;
	FinalizeExecutionRegionSourceInfo(*result, mode);
	return result;
}

static string BuildExecutionRegionGenericSinkContractReason(const ExecutionRegionOperatorEntry &entry) {
	if (entry.IsSortSink()) {
		return "DuckDB ordered sink contract missing;operator=" + entry.operator_name;
	}
	if (entry.IsMaterializationSink()) {
		return "DuckDB materialization sink contract missing;operator=" + entry.operator_name;
	}
	return "DuckDB sink operator missing;operator=" + entry.operator_name;
}

static unique_ptr<ExecutionRegionSinkInfo> BuildExecutionRegionSinkInfo(const ExecutionRegionOperatorEntry &entry,
                                                                        const ExecutionRegionSinkInfo &sink_payload,
                                                                        bool has_sink_contract,
                                                                        ExecutionRegionIRMode mode) {
	auto result =
	    has_sink_contract ? make_uniq<ExecutionRegionSinkInfo>(sink_payload) : make_uniq<ExecutionRegionSinkInfo>();
	if (!has_sink_contract) {
		result->reason = BuildExecutionRegionGenericSinkContractReason(entry);
	}
	FinalizeExecutionRegionSinkInfo(*result, mode);
	return result;
}

static string BuildExecutionRegionSourceBoundaryReason(const ExecutionRegionOperatorEntry &entry) {
	if (!entry.source_boundary_reason.empty()) {
		return entry.source_boundary_reason;
	}
	if (entry.IsScanSource()) {
		return "DuckDB scan source boundary;operator=" + entry.operator_name;
	}
	return "DuckDB stateful source boundary;operator=" + entry.operator_name;
}

static string BuildExecutionRegionSinkBoundaryReason(const ExecutionRegionOperatorEntry &entry) {
	if (entry.HasSinkContract() && !entry.sink_payload.reason.empty()) {
		return entry.sink_payload.reason;
	}
	return BuildExecutionRegionGenericSinkContractReason(entry);
}

static ExecutionRegionNode BuildExecutionRegionBoundaryNode(string label, const ExecutionRegionOperatorEntry &entry,
                                                            ExecutionRegionNodeKind kind, string blocker_reason) {
	ExecutionRegionNode node;
	node.label = std::move(label);
	node.operator_name = entry.operator_name;
	node.operator_kind = entry.operator_kind;
	node.operator_index = entry.operator_index;
	node.kind = kind;
	node.output_types = entry.output_types;
	node.estimated_cardinality = entry.estimated_cardinality;
	node.estimated_cardinality_exact =
	    entry.slot == ExecutionRegionOperatorSlot::SOURCE && entry.source_payload.estimated_source_cardinality_exact;
	node.input_format = ExecutionRegionVectorFormatKind::BOUNDARY;
	node.output_format = ExecutionRegionVectorFormatKind::BOUNDARY;
	node.vector_source = ExecutionRegionVectorSourceKind::BOUNDARY;
	node.selection_source = ExecutionRegionSelectionSourceKind::BOUNDARY;
	switch (kind) {
	case ExecutionRegionNodeKind::SOURCE:
		node.boundary =
		    entry.IsScanSource() ? ExecutionRegionBoundaryKind::SCAN : ExecutionRegionBoundaryKind::OPERATOR_MISSING;
		break;
	case ExecutionRegionNodeKind::SINK:
		node.boundary = ExecutionRegionBoundaryKind::SINK;
		break;
	default:
		node.boundary = ExecutionRegionBoundaryKind::OPERATOR_MISSING;
		break;
	}
	node.blocker_reason = std::move(blocker_reason);
	return node;
}

struct ExecutionRegionDataflowState {
	ExecutionRegionVectorSourceKind vector_source = ExecutionRegionVectorSourceKind::REGION_INPUT;
	ExecutionRegionSelectionSourceKind selection_source = ExecutionRegionSelectionSourceKind::INPUT_SELECTION;
};

static void SetExecutionRegionInputDataflow(ExecutionRegionNode &node, const ExecutionRegionDataflowState &state) {
	node.vector_source = state.vector_source;
	node.selection_source = state.selection_source;
}

static void SetExecutionRegionBoundaryDataflow(ExecutionRegionDataflowState &state) {
	state.vector_source = ExecutionRegionVectorSourceKind::BOUNDARY;
	state.selection_source = ExecutionRegionSelectionSourceKind::BOUNDARY;
}

static ExecutionRegionNode BuildExecutionRegionOperatorNode(string label, const ExecutionRegionOperatorEntry &entry,
                                                            ExecutionRegionDataflowState &state,
                                                            ExecutionExpressionIRMode expression_mode,
                                                            ExecutionRegionIRMode mode,
                                                            ExecutionExpressionAnalysisCache *expression_cache) {
	if (entry.IsFilter()) {
		if (!entry.filter_expression) {
			throw InternalException("Execution region filter descriptor missing expression");
		}
		auto &filter_expression = *entry.filter_expression;
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.kind = ExecutionRegionNodeKind::FILTER;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = ExecutionRegionVectorFormatKind::SELECTION_VECTOR;
		SetExecutionRegionInputDataflow(node, state);
		node.filter = expression_cache ? expression_cache->Copy(filter_expression, 0, expression_mode)
		                               : TryLowerExecutionExpression(filter_expression, 0, expression_mode);
		if (!node.filter) {
			node.boundary = ExecutionRegionBoundaryKind::EXPRESSION_MISSING;
			node.blocker_reason = "core filter expression lowering unsupported;" +
			                      DescribeExecutionExpressionLoweringFailure(filter_expression);
			SetExecutionRegionBoundaryDataflow(state);
			return node;
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::FILTER_SELECTION;
		return node;
	}
	if (entry.IsProjection()) {
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.kind = ExecutionRegionNodeKind::PROJECTION;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = ExecutionRegionVectorFormatKind::FLAT_VECTOR;
		SetExecutionRegionInputDataflow(node, state);
		for (idx_t expr_idx = 0; expr_idx < entry.projection_expressions.size(); expr_idx++) {
			auto &projection_expression = *entry.projection_expressions[expr_idx];
			auto fragment = expression_cache
			                    ? expression_cache->Copy(projection_expression, expr_idx, expression_mode)
			                    : TryLowerExecutionExpression(projection_expression, expr_idx, expression_mode);
			if (!fragment) {
				node.projections.clear();
				node.boundary = ExecutionRegionBoundaryKind::EXPRESSION_MISSING;
				node.blocker_reason =
				    "core projection expression lowering unsupported;expression_index=" + std::to_string(expr_idx) +
				    ";" + DescribeExecutionExpressionLoweringFailure(projection_expression);
				SetExecutionRegionBoundaryDataflow(state);
				return node;
			}
			node.projections.push_back(std::move(fragment));
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::NONE;
		return node;
	}
	if (entry.HasOperatorContract()) {
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.compiled_contract = entry.operator_contract;
		node.kind = ExecutionRegionNodeKind::OPERATOR;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::DATA_CHUNK;
		node.output_format = ExecutionRegionVectorFormatKind::DATA_CHUNK;
		SetExecutionRegionInputDataflow(node, state);
		node.operator_info = BuildExecutionRegionOperatorInfo(entry.operator_payload, mode);
		if (entry.HasNativeOperator()) {
			node.boundary = ExecutionRegionBoundaryKind::OPERATOR_NATIVE;
		} else {
			node.boundary = ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY;
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::NONE;
		return node;
	}
	auto node = BuildExecutionRegionBoundaryNode(std::move(label), entry, ExecutionRegionNodeKind::OPERATOR,
	                                             "DuckDB physical operator outside generated execution region");
	SetExecutionRegionBoundaryDataflow(state);
	return node;
}

static bool ExecutionRegionOrderDependencyCoveredByPrimitiveUpdate(const ExecutionRegionAggregateInput &aggregate) {
	return aggregate.primitive_update_ready &&
	       aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE;
}

string ExecutionRegionAggregateNativeStateUpdateBlocker(const ExecutionRegionAggregateContract &contract,
                                                        const vector<ExecutionRegionAggregateInput> &aggregates,
                                                        const vector<ExecutionRegionGroupInput> &groups) {
	if (!contract.present) {
		return "aggregate-state-update-contract-missing";
	}
	const bool has_distinct_state = contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	                                contract.distinct_child_count != 0 || contract.distinct_filter_count != 0;
	if (has_distinct_state && !contract.distinct_count_pointer_keys) {
		return "aggregate-state-update-distinct-state";
	}
	if (contract.distinct_count_pointer_keys) {
		if (contract.kind != ExecutionRegionAggregateOperatorKind::HASH) {
			return "aggregate-state-update-distinct-pointer-kind";
		}
		if (contract.distinct_aggregate_count != 1 || contract.distinct_table_count != 1 ||
		    contract.distinct_child_count != 1 || contract.distinct_filter_count != 1 ||
		    contract.aggregate_count != 1) {
			return "aggregate-state-update-distinct-pointer-shape";
		}
	}
	if (contract.aggregate_filter_count != 0) {
		return "aggregate-state-update-filtered-aggregate";
	}
	if (contract.aggregate_order_count != 0) {
		return "aggregate-state-update-ordered-aggregate";
	}

	switch (contract.kind) {
	case ExecutionRegionAggregateOperatorKind::HASH:
	case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
		if (groups.size() != contract.group_count) {
			return "aggregate-state-update-group-binding-count";
		}
		if (!contract.distinct_count_pointer_keys &&
		    contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY) {
			return contract.native_grouped_state_contract.blocker.empty()
			           ? "aggregate-state-update-grouped-state-contract"
			           : contract.native_grouped_state_contract.blocker;
		}
		for (auto &group : groups) {
			if (!group.supported_reference) {
				return group.reason.empty() ? "aggregate-state-update-group-reference" : group.reason;
			}
		}
		break;
	case ExecutionRegionAggregateOperatorKind::UNGROUPED:
		if (!groups.empty()) {
			return "ungrouped-aggregate-state-update-unexpected-groups";
		}
		break;
	default:
		return "aggregate-state-update-operator-kind";
	}

	for (auto &aggregate : aggregates) {
		if (!aggregate.reason.empty()) {
			return aggregate.reason;
		}
		if (aggregate.distinct && !contract.distinct_count_pointer_keys) {
			return "aggregate-state-update-distinct-aggregate";
		}
		if (contract.distinct_count_pointer_keys &&
		    (!aggregate.distinct || aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::COUNT ||
		     aggregate.child_count != 1)) {
			return "aggregate-state-update-distinct-pointer-aggregate";
		}
		if (aggregate.has_filter) {
			return "aggregate-state-update-filtered-aggregate";
		}
		if (aggregate.has_order_bys ||
		    (aggregate.order_dependent && !ExecutionRegionOrderDependencyCoveredByPrimitiveUpdate(aggregate))) {
			return "aggregate-state-update-ordered-aggregate";
		}
		if (!aggregate.has_state_update) {
			return "aggregate-state-update-callback-missing";
		}
		if (!aggregate.payload_expressions_ready) {
			return aggregate.payload_expression_blocker.empty() ? "aggregate-state-update-payload-expression"
			                                                    : aggregate.payload_expression_blocker;
		}
		if (!aggregate.supported_payload_references) {
			return "aggregate-state-update-payload-reference";
		}
	}
	return string();
}

static ExecutionRegionSourceKind InferExecutionRegionSourceKind(const ExecutionRegionNode &node) {
	if (node.source) {
		return node.source->kind;
	}
	if (node.operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN) {
		return ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN;
	}
	if (node.boundary == ExecutionRegionBoundaryKind::SCAN ||
	    node.boundary == ExecutionRegionBoundaryKind::SOURCE_CONTRACT) {
		return ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN;
	}
	if (node.kind == ExecutionRegionNodeKind::SOURCE) {
		return ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	}
	return ExecutionRegionSourceKind::NONE;
}

static void AccumulateExecutionRegionExpressionTraits(const ExecutionExpressionTraits &expression_traits,
                                                      ExecutionRegionCandidateTraits &traits) {
	traits.predicate_expression_count += expression_traits.predicate_expression_count;
	traits.control_expression_count += expression_traits.control_expression_count;
	traits.expression_cost += expression_traits.expression_cost;
}

static void AccumulateExecutionRegionSourceTraits(const ExecutionRegionNode &node,
                                                  ExecutionRegionSourceExecutionKind execution,
                                                  ExecutionRegionCandidateTraits &traits) {
	traits.source_kind = InferExecutionRegionSourceKind(node);
	traits.source_execution = execution;
	if (!node.source) {
		return;
	}

	traits.source_filter_count = node.source->filters.size();
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root) {
			continue;
		}
		traits.source_filter_expression_count++;
		if (filter.expression->traits.has_conjunction) {
			traits.source_conjunction_filter_count++;
		}
	}
}

static ExecutionRegionCandidateTraits BuildExecutionRegionCandidateTraits(const ExecutionRegionIR &region_ir,
                                                                          const ExecutionRegionCandidate &candidate,
                                                                          const ExecutionRegionStagePlan &stage_plan,
                                                                          ExecutionRegionIRMode mode) {
	ExecutionRegionCandidateTraits traits;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			AccumulateExecutionRegionSourceTraits(node, GetExecutionRegionCandidateSourceExecution(candidate, node),
			                                      traits);
			break;
		case ExecutionRegionNodeKind::SINK:
			traits.sink_present = true;
			traits.sink_kind = node.sink ? node.sink->kind : ExecutionRegionSinkKind::NONE;
			if (node.sink) {
				if (node.sink->aggregate_contract.present) {
					traits.aggregate_count += node.sink->aggregate_contract.aggregate_count;
				}
			}
			break;
		case ExecutionRegionNodeKind::FILTER:
			traits.filter_count++;
			if (node.filter && node.filter->root) {
				AccumulateExecutionRegionExpressionTraits(node.filter->traits, traits);
			}
			break;
		case ExecutionRegionNodeKind::PROJECTION:
			traits.projection_count++;
			if (node.projections.empty()) {
				break;
			}
			for (auto &projection : node.projections) {
				if (!projection || !projection->root) {
					continue;
				}
				AccumulateExecutionRegionExpressionTraits(projection->traits, traits);
				if (projection->traits.root_is_reference) {
					traits.reference_projection_count++;
					if (projection->return_type.id() == LogicalTypeId::VARCHAR) {
						traits.reference_varchar_projection_count++;
					}
				}
				if (projection->traits.has_arithmetic_binary) {
					traits.arithmetic_projection_count++;
				}
				if (projection->traits.expression_cost >= HIGH_COST_GENERATED_PROJECTION_EXPRESSION_COST) {
					traits.high_cost_projection_count++;
				}
			}
			break;
		case ExecutionRegionNodeKind::OPERATOR:
			traits.operator_count++;
			if (node.operator_info && node.operator_info->hash_join_contract.present) {
				traits.hash_join_operator_count++;
			}
			break;
		default:
			traits.operator_count++;
			break;
		}
	}
	FinalizeExecutionRegionCandidateTraits(traits, mode);
	return traits;
}

static idx_t EstimateExecutionRegionCandidateCardinality(const ExecutionRegionIR &region_ir,
                                                         const ExecutionRegionCandidate &candidate) {
	if (candidate.first_node < region_ir.nodes.size()) {
		auto &source = region_ir.nodes[candidate.first_node];
		if (source.kind == ExecutionRegionNodeKind::SOURCE && source.estimated_cardinality_exact) {
			bool row_preserving_or_reducing = true;
			for (idx_t node_idx = candidate.first_node + 1; node_idx < candidate.EndNode(); node_idx++) {
				auto &node = region_ir.nodes[node_idx];
				if (node.kind != ExecutionRegionNodeKind::FILTER && node.kind != ExecutionRegionNodeKind::PROJECTION &&
				    node.kind != ExecutionRegionNodeKind::SINK) {
					row_preserving_or_reducing = false;
					break;
				}
			}
			if (row_preserving_or_reducing) {
				return source.estimated_cardinality;
			}
		}
	}
	idx_t result = 0;
	if (candidate.first_node > 0 && candidate.first_node - 1 < region_ir.nodes.size()) {
		result = MaxValue(result, region_ir.nodes[candidate.first_node - 1].estimated_cardinality);
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		result = MaxValue(result, region_ir.nodes[node_idx].estimated_cardinality);
	}
	return result;
}

static vector<LogicalType> GetExecutionRegionCandidateInputTypes(const ExecutionRegionIR &region_ir,
                                                                 const ExecutionRegionCandidate &candidate) {
	if (candidate.first_node < region_ir.nodes.size() &&
	    region_ir.nodes[candidate.first_node].kind == ExecutionRegionNodeKind::SOURCE) {
		return region_ir.nodes[candidate.first_node].output_types;
	}
	if (candidate.first_node > 0) {
		return region_ir.nodes[candidate.first_node - 1].output_types;
	}
	if (!region_ir.nodes.empty()) {
		return region_ir.nodes[candidate.first_node].output_types;
	}
	return vector<LogicalType>();
}

static vector<LogicalType> GetExecutionRegionCandidateOutputTypes(const ExecutionRegionIR &region_ir,
                                                                  const ExecutionRegionCandidate &candidate) {
	for (idx_t node_offset = candidate.node_count; node_offset > 0; node_offset--) {
		auto &node = region_ir.nodes[candidate.first_node + node_offset - 1];
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			continue;
		}
		return node.output_types;
	}
	return vector<LogicalType>();
}

static bool HasExecutionRegionCandidate(const ExecutionRegionIR &region_ir, idx_t first_node, idx_t node_count,
                                        idx_t start_operator_index, idx_t end_operator_index,
                                        ExecutionRegionSourceExecutionKind source_execution) {
	for (auto &candidate : region_ir.candidates) {
		if (candidate.first_node == first_node && candidate.node_count == node_count &&
		    candidate.start_operator_index == start_operator_index &&
		    candidate.end_operator_index == end_operator_index && candidate.source_execution == source_execution) {
			return true;
		}
	}
	return false;
}

static void AddExecutionRegionCandidateBlocker(ExecutionRegionIR &region_ir, string reason) {
	if (std::find(region_ir.candidate_blockers.begin(), region_ir.candidate_blockers.end(), reason) ==
	    region_ir.candidate_blockers.end()) {
		region_ir.candidate_blockers.push_back(std::move(reason));
	}
}

static string AppendExecutionRegionCandidateDiagnostic(string reason, const string &diagnostic) {
	if (diagnostic.empty()) {
		return reason;
	}
	return std::move(reason) + ";" + diagnostic;
}

struct ExecutionRegionCandidateSummary {
	ExecutionRegionCandidate candidate;
	string blocker;

	bool Accepted() const {
		return blocker.empty();
	}
};

static bool ExecutionRegionCandidateCoversFullPipeline(const ExecutionRegionIR &region_ir,
                                                       const ExecutionRegionCandidate &candidate) {
	return candidate.first_node == 0 && candidate.node_count == region_ir.nodes.size();
}

static bool ExecutionRegionCandidateContextHasMissingOperatorContract(const ExecutionRegionIR &region_ir,
                                                                      const ExecutionRegionCandidate &candidate) {
	if (ExecutionRegionCandidateCoversFullPipeline(region_ir, candidate)) {
		return CountExecutionRegionMissingOperatorContracts(candidate.stage_plan) > 0;
	}
	ExecutionRegionCandidate context_candidate;
	context_candidate.first_node = 0;
	context_candidate.node_count = region_ir.nodes.size();
	context_candidate.contract =
	    BuildExecutionRegionContract(region_ir, context_candidate, ExecutionRegionIRMode::COMPACT);
	context_candidate.stage_plan =
	    BuildExecutionRegionStagePlan(region_ir, context_candidate, ExecutionRegionIRMode::COMPACT);
	return CountExecutionRegionMissingOperatorContracts(context_candidate.stage_plan) > 0;
}

static ExecutionRegionCandidateSummary
BuildExecutionRegionCandidateSummary(const ExecutionRegionIR &region_ir, idx_t candidate_id, idx_t first_node,
                                     idx_t node_count, idx_t start_operator_index, idx_t end_operator_index,
                                     ExecutionRegionSourceExecutionKind source_execution, ExecutionRegionIRMode mode) {
	ExecutionRegionCandidateSummary summary;
	auto &candidate = summary.candidate;
	candidate.candidate_id = candidate_id;
	candidate.first_node = first_node;
	candidate.node_count = node_count;
	candidate.start_operator_index = start_operator_index;
	candidate.end_operator_index = end_operator_index;
	candidate.source_execution = source_execution;
	candidate.estimated_cardinality = EstimateExecutionRegionCandidateCardinality(region_ir, candidate);
	candidate.input_types = GetExecutionRegionCandidateInputTypes(region_ir, candidate);
	candidate.output_types = GetExecutionRegionCandidateOutputTypes(region_ir, candidate);
	candidate.shape = DescribeExecutionRegionCandidateShape(region_ir, candidate);
	candidate.pipeline_shape = DescribeExecutionRegionPipelineShape(region_ir, first_node, node_count);
	if (candidate.first_node < region_ir.nodes.size()) {
		candidate.uses_scan_filters = ExecutionRegionCandidateUsesScanFilters(candidate, region_ir.nodes[first_node]);
	}
	candidate.contract = BuildExecutionRegionContract(region_ir, candidate, mode);

	auto describe_span = [&]() {
		return DescribeExecutionRegionCandidateSpan(first_node, node_count, start_operator_index, end_operator_index,
		                                            source_execution);
	};
	if (!candidate.contract.OwnsSource()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:source-ownership-missing;" + describe_span(), candidate.contract.ir);
		return summary;
	}
	if (!candidate.contract.OwnsSink()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:sink-ownership-missing;" + describe_span(), candidate.contract.ir);
		return summary;
	}
	if (!ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:requires-full-pipeline-abi;" + describe_span(), candidate.contract.ir);
		return summary;
	}

	candidate.stage_plan = BuildExecutionRegionStagePlan(region_ir, candidate, mode);
	if (!candidate.stage_plan.HasExecutableWork()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:no-executable-work;" + describe_span(), candidate.stage_plan.ir);
		return summary;
	}
	candidate.traits = BuildExecutionRegionCandidateTraits(region_ir, candidate, candidate.stage_plan, mode);
	candidate.signature = BuildExecutionRegionSignature(region_ir, candidate);
	candidate.context_has_missing_operator_contract =
	    ExecutionRegionCandidateContextHasMissingOperatorContract(region_ir, candidate);
	FinalizeExecutionRegionCandidate(candidate, mode);
	return summary;
}

static bool AddExecutionRegionCandidate(
    ExecutionRegionIR &region_ir, idx_t candidate_id, idx_t first_node, idx_t node_count, idx_t start_operator_index,
    idx_t end_operator_index,
    ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE,
    ExecutionRegionIRMode mode = ExecutionRegionIRMode::COMPACT) {
	D_ASSERT(node_count > 0);
	if (HasExecutionRegionCandidate(region_ir, first_node, node_count, start_operator_index, end_operator_index,
	                                source_execution)) {
		return false;
	}
	if (first_node >= region_ir.nodes.size() || region_ir.nodes[first_node].kind != ExecutionRegionNodeKind::SOURCE) {
		AddExecutionRegionCandidateBlocker(
		    region_ir, "candidate-builder-blocked:span-does-not-start-at-source;" +
		                   DescribeExecutionRegionCandidateSpan(first_node, node_count, start_operator_index,
		                                                        end_operator_index, source_execution));
		return false;
	}
	auto summary =
	    BuildExecutionRegionCandidateSummary(region_ir, candidate_id, first_node, node_count, start_operator_index,
	                                         end_operator_index, source_execution, mode);
	if (!summary.Accepted()) {
		AddExecutionRegionCandidateBlocker(region_ir, std::move(summary.blocker));
		return false;
	}
	region_ir.candidates.push_back(std::move(summary.candidate));
	return true;
}

static bool AddExecutionRegionCandidateAndIncrement(ExecutionRegionIR &region_ir, idx_t &candidate_id, idx_t first_node,
                                                    idx_t node_count, idx_t start_operator_index,
                                                    idx_t end_operator_index,
                                                    ExecutionRegionIRMode mode = ExecutionRegionIRMode::COMPACT) {
	if (AddExecutionRegionCandidate(region_ir, candidate_id, first_node, node_count, start_operator_index,
	                                end_operator_index, ExecutionRegionSourceExecutionKind::NONE, mode)) {
		candidate_id++;
		return true;
	}
	return false;
}

static void BuildExecutionRegionCandidates(ExecutionRegionIR &region_ir, idx_t operator_count,
                                           ExecutionRegionIRMode mode) {
	if (region_ir.nodes.empty()) {
		return;
	}
	idx_t candidate_id = 0;
	AddExecutionRegionCandidateAndIncrement(region_ir, candidate_id, 0, region_ir.nodes.size(), 0, operator_count,
	                                        mode);
}

static ExecutionExpressionIRMode ExecutionExpressionModeFromRegionMode(ExecutionRegionIRMode mode) {
	return mode == ExecutionRegionIRMode::TRACE ? ExecutionExpressionIRMode::TRACE : ExecutionExpressionIRMode::COMPACT;
}

static unique_ptr<ExecutionRegionIR> TryBuildExecutionRegion(const ExecutionRegionGraph &descriptor,
                                                             ExecutionRegionIRMode mode,
                                                             ExecutionExpressionAnalysisCache *expression_cache) {
	auto expression_mode = ExecutionExpressionModeFromRegionMode(mode);
	auto result = make_uniq<ExecutionRegionIR>();
	if (descriptor.HasSource()) {
		auto &source = descriptor.source;
		auto source_node = BuildExecutionRegionBoundaryNode("source", source, ExecutionRegionNodeKind::SOURCE,
		                                                    BuildExecutionRegionSourceBoundaryReason(source));
		source_node.compiled_contract = source.source_contract;
		if (source.HasSourceContract()) {
			source_node.source =
			    BuildExecutionRegionSourceInfo(source.source_payload, expression_mode, mode, expression_cache);
			if (source.UsesSourceContract()) {
				source_node.boundary = ExecutionRegionBoundaryKind::SOURCE_CONTRACT;
			}
		} else if (source.IsScanSource()) {
			source_node.source = BuildExecutionRegionGenericScanSourceInfo(source, source_node.blocker_reason, mode);
		} else {
			source_node.source = BuildExecutionRegionStatefulSourceInfo(source, source_node.blocker_reason, mode);
		}
		result->nodes.push_back(std::move(source_node));
	}
	ExecutionRegionDataflowState state;
	for (idx_t op_idx = 0; op_idx < descriptor.operators.size(); op_idx++) {
		result->nodes.push_back(BuildExecutionRegionOperatorNode("op" + std::to_string(op_idx),
		                                                         descriptor.operators[op_idx], state, expression_mode,
		                                                         mode, expression_cache));
	}
	if (descriptor.HasSink()) {
		auto &sink = descriptor.sink;
		auto sink_reason = BuildExecutionRegionSinkBoundaryReason(sink);
		auto sink_node =
		    BuildExecutionRegionBoundaryNode("sink", sink, ExecutionRegionNodeKind::SINK, std::move(sink_reason));
		sink_node.compiled_contract = sink.sink_contract;
		sink_node.sink = BuildExecutionRegionSinkInfo(sink, sink.sink_payload, sink.HasSinkContract(), mode);
		if (sink_node.sink) {
			sink_node.blocker_reason = sink_node.sink->reason;
			if (sink.HasNativeSink()) {
				sink_node.boundary = ExecutionRegionBoundaryKind::SINK_NATIVE;
			}
		}
		result->nodes.push_back(std::move(sink_node));
	}
	if (result->nodes.empty()) {
		return nullptr;
	}
	result->pipeline_shape = DescribeExecutionRegionPipelineShape(*result);
	BuildExecutionRegionCandidates(*result, descriptor.OperatorCount(), mode);
	FinalizeExecutionRegionIR(*result, mode);
	return result;
}

unique_ptr<ExecutionRegionIR> TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor,
                                                      ExecutionRegionIRMode mode,
                                                      ExecutionExpressionAnalysisCache *expression_cache) {
	return TryBuildExecutionRegion(descriptor, mode, expression_cache);
}

} // namespace duckdb
