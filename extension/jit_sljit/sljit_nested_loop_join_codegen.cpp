//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_nested_loop_join_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_join_probe_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include "sljitLir.h"

namespace duckdb {

static inline bool SljitNestedLoopJoinValueIsValid(const validity_t *validity, idx_t row_idx) {
	if (!validity) {
		return true;
	}
	const auto entry_idx = row_idx / ValidityMask::BITS_PER_VALUE;
	const auto idx_in_entry = row_idx % ValidityMask::BITS_PER_VALUE;
	return ValidityMask::RowIsValid(validity[entry_idx], idx_in_entry);
}

template <class T>
static inline bool SljitNestedLoopJoinCompareValue(T left, T right, ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return left == right;
	case ExecutionRegionComparisonType::NOT_EQUAL:
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return left != right;
	case ExecutionRegionComparisonType::LESS_THAN:
		return left < right;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return left > right;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return left <= right;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return left >= right;
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return left == right;
	default:
		throw InternalException("Unsupported SLJIT nested loop join comparison");
	}
}

template <class T, ExecutionRegionComparisonType COMPARISON>
static void SljitNestedLoopJoinProbeTypedCompare(SljitNativeNestedLoopJoinProbeInput *input) {
	auto left_data = reinterpret_cast<const T *>(input->left_data);
	auto right_data = reinterpret_cast<const T *>(input->right_data);
	auto left_idx = input->left_offset;
	auto right_idx = input->right_offset;
	idx_t out_idx = 0;

	while (left_idx < input->left_count) {
		auto left_source_idx = input->left_sel ? input->left_sel[left_idx] : left_idx;
		if (!SljitNestedLoopJoinValueIsValid(input->left_validity, left_source_idx)) {
			left_idx++;
			right_idx = 0;
			continue;
		}
		auto left_value = left_data[left_source_idx];
		while (right_idx < input->right_count) {
			auto right_source_idx = input->right_sel ? input->right_sel[right_idx] : right_idx;
			if (SljitNestedLoopJoinValueIsValid(input->right_validity, right_source_idx) &&
			    SljitNestedLoopJoinCompareValue<T>(left_value, right_data[right_source_idx], COMPARISON)) {
				input->left_match_sel[out_idx] = UnsafeNumericCast<sel_t>(left_idx);
				input->right_match_sel[out_idx] = UnsafeNumericCast<sel_t>(right_idx);
				out_idx++;
				right_idx++;
				if (out_idx == input->output_capacity) {
					input->left_offset = left_idx;
					input->right_offset = right_idx;
					input->selected_count = out_idx;
					input->right_chunk_finished = false;
					return;
				}
				continue;
			}
			right_idx++;
		}
		left_idx++;
		right_idx = 0;
	}

	input->left_offset = left_idx;
	input->right_offset = 0;
	input->selected_count = out_idx;
	input->right_chunk_finished = true;
}

using SljitNestedLoopJoinProbeHelper = void (*)(SljitNativeNestedLoopJoinProbeInput *);

template <class T>
static SljitNestedLoopJoinProbeHelper
SelectSljitNestedLoopJoinProbeComparisonHelper(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::EQUAL>;
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::NOT_EQUAL>;
	case ExecutionRegionComparisonType::LESS_THAN:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::LESS_THAN>;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::GREATER_THAN>;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL>;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL>;
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::NOT_DISTINCT_FROM>;
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::DISTINCT_FROM>;
	default:
		return nullptr;
	}
}

static SljitNestedLoopJoinProbeHelper
SelectSljitNestedLoopJoinProbeHelper(const SljitNativeNestedLoopJoinProbePlan &plan, string &error) {
	if (plan.conditions.size() != 1) {
		error = "SLJIT native nested loop join probe requires one comparison condition";
		return nullptr;
	}
	auto &condition = plan.conditions[0];
	switch (condition.value_kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<int32_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<int64_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<hugeint_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<double>(condition.comparison_type);
	default:
		error = "SLJIT native nested loop join probe has unsupported value kind";
		return nullptr;
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNestedLoopJoinProbe(const SljitNativeNestedLoopJoinProbePlan &plan,
                                                                    SljitNativeNestedLoopJoinProbeFunction &function,
                                                                    string &error) {
	auto helper = SelectSljitNestedLoopJoinProbeHelper(plan, error);
	if (!helper) {
		if (error.empty()) {
			error = "SLJIT native nested loop join probe has no typed comparison helper";
		}
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 1, 1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(helper));
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
