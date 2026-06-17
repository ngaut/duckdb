//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_admission.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

struct ExecutionRegionAdmissionRule {
	ExecutionRegionCompileTarget target = ExecutionRegionCompileTarget::REGION;
	string admission_key;
	idx_t min_cardinality = 0;
	string proof;
};

struct ExecutionRegionAdmissionProfileRule {
	string backend_name;
	ExecutionRegionAdmissionRule rule;
};

string BuildExecutionRegionAdmissionShapeKey(const string &backend_name, const ExecutionRegionSignature &signature);
string BuildExecutionRegionAdmissionContextShapeKey(const ExecutionRegionSignature &signature, const string &shape_key);
string BuildExecutionRegionAdmissionShapeKey(const string &backend_name,
                                             const ExecutionRegionPipelineInventory &inventory);
string BuildExecutionRegionAdmissionContextShapeKey(const ExecutionRegionPipelineInventory &inventory,
                                                    const string &shape_key);
bool ExecutionRegionLoweredRegionCanUseMeasuredAutoAdmission(const ExecutionRegionCandidate &candidate,
                                                             const ExecutionRegionLoweringPlan &lowering_plan,
                                                             string &reason);

struct ExecutionRegionAdmissionInfo {
	bool has_admission = false;
	string admission_key;
	bool rule_present = false;
	idx_t min_cardinality = 0;
	string proof;
	bool has_score = false;
	int64_t score = 0;
};

struct ExecutionRegionAdmissionDecision {
	bool compile = false;
	string policy_decision;
	string reason;
	ExecutionRegionAdmissionInfo info;
};

struct ExecutionRegionStageTimings {
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t admission_time_us = 0;
	int64_t overlap_check_time_us = 0;
	int64_t codegen_time_us = 0;
};

} // namespace duckdb
