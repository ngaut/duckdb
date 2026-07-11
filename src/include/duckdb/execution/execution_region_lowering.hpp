//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_lowering.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_graph.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

class Expression;
class ExecutionRegionBackendPlan;

enum class ExecutionRegionIRMode : uint8_t { COMPACT, TRACE };

class ExecutionExpressionAnalysisCache {
public:
	const ExecutionExpressionFragment *Get(const Expression &expression,
	                                       ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::COMPACT);
	unique_ptr<ExecutionExpressionFragment> Copy(const Expression &expression, idx_t expression_index,
	                                             ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::COMPACT);

private:
	struct Entry {
		bool attempted = false;
		unique_ptr<ExecutionExpressionFragment> fragment;
	};

private:
	unordered_map<const Expression *, Entry> entries;
};

struct ExecutionRegionNodeLowering {
	string label;
	string operator_name;
	ExecutionRegionOperatorKind operator_kind = ExecutionRegionOperatorKind::GENERIC;
	ExecutionRegionLoweringKind kind = ExecutionRegionLoweringKind::BOUNDARY;
	string reason;
};

struct ExecutionRegionLoweringCapabilityFacts {
	ExecutionRegionOperatorKindCounts native_operator_kind_counts = {};
	ExecutionRegionOperatorKindCounts boundary_operator_kind_counts = {};
	ExecutionRegionVectorFormatKindCounts native_input_format_counts = {};
	ExecutionRegionVectorFormatKindCounts native_output_format_counts = {};
	ExecutionRegionVectorSourceKindCounts native_vector_source_counts = {};
	ExecutionRegionSelectionSourceKindCounts native_selection_source_counts = {};
	ExecutionRegionCapabilityValidityKindCounts backend_source_validity_counts = {};
	ExecutionRegionCapabilityTypeKindCounts backend_join_key_type_counts = {};
	ExecutionRegionCapabilityTypeKindCounts backend_group_key_type_counts = {};
	ExecutionRegionCapabilityTypeKindCounts backend_payload_type_counts = {};
	idx_t backend_hash_join_probe_count = 0;
	idx_t backend_regular_hash_join_probe_count = 0;
	idx_t backend_perfect_hash_join_probe_count = 0;
	idx_t backend_residual_hash_join_probe_count = 0;
	idx_t backend_hash_join_equality_key_count = 0;
	idx_t backend_hash_join_non_equality_key_count = 0;
	idx_t backend_hash_join_build_count = 0;
	idx_t backend_nested_loop_join_probe_count = 0;
	idx_t backend_nested_loop_join_build_count = 0;
	idx_t backend_hash_aggregate_update_count = 0;
	idx_t backend_perfect_hash_aggregate_update_count = 0;
	idx_t backend_ungrouped_aggregate_update_count = 0;
	idx_t backend_primitive_aggregate_payload_update_count = 0;
	idx_t backend_distinct_key_fast_insert_count = 0;
	idx_t backend_grouped_state_address_lookup_count = 0;
	idx_t backend_generated_perfect_hash_lookup_count = 0;
	idx_t backend_native_state_address_lookup_count = 0;
	idx_t backend_weak_accelerated_work_count = 0;
};

struct ExecutionRegionLoweringPlan {
	void AddNode(string label, string operator_name, ExecutionRegionLoweringKind kind, string reason);
	void AddNode(string label, string operator_name, ExecutionRegionOperatorKind operator_kind,
	             ExecutionRegionLoweringKind kind, string reason);
	void AddCompactNode(ExecutionRegionOperatorKind operator_kind, ExecutionRegionLoweringKind kind,
	                    const string &reason);
	void AddBackendDataShapeCapability(ExecutionRegionVectorFormatKind input_format,
	                                   ExecutionRegionVectorFormatKind output_format,
	                                   ExecutionRegionVectorSourceKind vector_source,
	                                   ExecutionRegionSelectionSourceKind selection_source);
	void AddBackendSourceValidityCapability(bool not_null);
	void AddBackendJoinKeyTypeCapability(const LogicalType &type);
	void AddBackendGroupKeyTypeCapability(const LogicalType &type);
	void AddBackendPayloadTypeCapability(const LogicalType &type);
	void AddBackendHashJoinProbeCapability(bool perfect_hash_probe, bool residual_predicate, idx_t equality_key_count,
	                                       idx_t key_count);
	void AddBackendHashJoinBuildCapability();
	void AddBackendNestedLoopJoinProbeCapability();
	void AddBackendNestedLoopJoinBuildCapability();
	void AddBackendAggregateUpdateCapability(ExecutionRegionAggregateOperatorKind kind, bool primitive_payloads,
	                                         bool grouped_state_addresses, bool perfect_hash_group_lookup);
	void AddBackendDistinctKeyFastInsertCapability();
	void AddBackendWeakAcceleratedWorkCapability();
	void AddFusionBlocker(string reason);
	void SetRecordDetailedNodes(bool record_detailed_nodes);
	void SetCompiledExecutionMode(ExecutionRegionExecutionMode execution_mode);
	void SetFullyFused(bool fully_fused);
	void SetUsesScanFilters(bool uses_scan_filters);
	void SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetSourceContractInputTypes(vector<LogicalType> input_types);
	void SetOperatorStageIR(string stage_ir);
	idx_t NativeCount() const;
	idx_t BoundaryCount() const;
	bool HasNodes() const;
	bool RecordDetailedNodes() const;
	ExecutionRegionExecutionMode ExpectedCompiledExecutionMode() const;
	bool IsFullyFused() const;
	bool UsesScanFilters() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	const vector<LogicalType> &SourceContractInputTypes() const;
	string CompactEventReason() const;
	string EventReason() const;

	vector<ExecutionRegionNodeLowering> nodes;
	vector<string> fusion_blockers;
	shared_ptr<ExecutionRegionBackendPlan> backend_plan;
	ExecutionRegionExecutionMode compiled_execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	idx_t node_count = 0;
	idx_t native_count = 0;
	idx_t boundary_count = 0;
	ExecutionRegionLoweringCapabilityFacts capability_facts;
	bool record_detailed_nodes = true;
	bool fully_fused = false;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;
	vector<LogicalType> source_contract_input_types;
	string operator_stage_ir;
	string first_boundary_reason;
	string first_fusion_blocker;
};

DUCKDB_API unique_ptr<ExecutionExpressionFragment>
TryLowerExecutionExpression(const Expression &expression, idx_t expression_index = 0,
                            ExecutionExpressionIRMode mode = ExecutionExpressionIRMode::TRACE);
DUCKDB_API string DescribeExecutionExpressionLoweringFailure(const Expression &expression);
DUCKDB_API unique_ptr<ExecutionRegionIR>
TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor, ExecutionRegionIRMode mode,
                        ExecutionExpressionAnalysisCache *expression_cache = nullptr);

} // namespace duckdb
