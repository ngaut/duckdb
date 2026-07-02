//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_strategy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_common.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <array>
#include <cstring>
#include <utility>

namespace duckdb {

struct SljitStringSetCaseGroupedPayloadProjection {
	static constexpr idx_t CONSTANT_COUNT = 2;

	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	idx_t compressed_group_source_idx = DConstants::INVALID_INDEX;
	std::array<string, CONSTANT_COUNT> constants;
};

static bool SljitIsIntegerConstantValue(const ExecutionExpressionIR &node, int32_t expected) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && node.return_type.id() == LogicalTypeId::INTEGER &&
	       !node.constant.IsNull() && node.constant.GetValue<int32_t>() == expected;
}

static bool SljitTryReadStringReferenceCompareConstant(const ExecutionExpressionIR &node, idx_t reference_index,
                                                       ExecutionExpressionBinaryOp expected_op, string &constant) {
	if (node.kind != ExecutionExpressionIRKind::BINARY || node.binary_op != expected_op || !node.left || !node.right) {
		return false;
	}
	auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
		if (reference.kind != ExecutionExpressionIRKind::REFERENCE || reference.ref_index != reference_index ||
		    reference.return_type.id() != LogicalTypeId::VARCHAR ||
		    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
		    constant_node.return_type.id() != LogicalTypeId::VARCHAR || constant_node.constant.IsNull()) {
			return false;
		}
		constant = StringValue::Get(constant_node.constant);
		return true;
	};
	return try_read(*node.left, *node.right) || try_read(*node.right, *node.left);
}

static bool SljitSameStringConstantSet(const std::array<string, 2> &left, const std::array<string, 2> &right) {
	return (left[0] == right[0] && left[1] == right[1]) || (left[0] == right[1] && left[1] == right[0]);
}

static bool SljitTryReadStringSetCaseExpression(const SljitExecutableRegionExpression &expr, idx_t reference_index,
                                                bool match_values, std::array<string, 2> &constants) {
	auto &plan = expr.plan;
	if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
	    plan.return_type.id() != LogicalTypeId::INTEGER) {
		return false;
	}
	auto &root = *plan.expression_tree;
	if (root.kind != ExecutionExpressionIRKind::CASE || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || !root.else_node || !SljitIsIntegerConstantValue(*root.children[1], 1) ||
	    !SljitIsIntegerConstantValue(*root.else_node, 0)) {
		return false;
	}
	auto &predicate = *root.children[0];
	const auto expected_conjunction =
	    match_values ? ExecutionExpressionConjunctionOp::OR : ExecutionExpressionConjunctionOp::AND;
	const auto expected_compare =
	    match_values ? ExecutionExpressionBinaryOp::COMPARE_EQUAL : ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL;
	if (predicate.kind != ExecutionExpressionIRKind::CONJUNCTION || predicate.conjunction_op != expected_conjunction ||
	    predicate.children.size() != 2 || !predicate.children[0] || !predicate.children[1]) {
		return false;
	}
	return SljitTryReadStringReferenceCompareConstant(*predicate.children[0], reference_index, expected_compare,
	                                                  constants[0]) &&
	       SljitTryReadStringReferenceCompareConstant(*predicate.children[1], reference_index, expected_compare,
	                                                  constants[1]);
}

static bool SljitStringEqualsConstant(const string_t &value, const string &constant) {
	return value.GetSize() == constant.size() && memcmp(value.GetData(), constant.data(), constant.size()) == 0;
}

static bool SljitTryFastProjectStringSetCaseGroupedPayload(const SljitStringSetCaseGroupedPayloadProjection &descriptor,
                                                           DataChunk &join_output, DataChunk &projected) {
	if (projected.ColumnCount() != 3 || descriptor.predicate_source_idx >= join_output.ColumnCount() ||
	    descriptor.compressed_group_source_idx >= join_output.ColumnCount() ||
	    join_output.data[descriptor.predicate_source_idx].GetType().id() != LogicalTypeId::VARCHAR ||
	    join_output.data[descriptor.compressed_group_source_idx].GetType() != projected.data[0].GetType() ||
	    !DirectAppendSupportsFixedSizeType(projected.data[0].GetType()) ||
	    projected.data[1].GetType().id() != LogicalTypeId::INTEGER ||
	    projected.data[2].GetType().id() != LogicalTypeId::INTEGER) {
		return false;
	}
	const auto count = join_output.size();
	projected.Reset();
	projected.data[0].Reference(join_output.data[descriptor.compressed_group_source_idx]);
	projected.data[1].SetVectorType(VectorType::FLAT_VECTOR);
	projected.data[2].SetVectorType(VectorType::FLAT_VECTOR);
	auto high_data = FlatVector::GetDataMutable<int32_t>(projected.data[1]);
	auto low_data = FlatVector::GetDataMutable<int32_t>(projected.data[2]);
	FlatVector::ValidityMutable(projected.data[1]).SetAllValid(count);
	FlatVector::ValidityMutable(projected.data[2]).SetAllValid(count);

	UnifiedVectorFormat predicate_format;
	join_output.data[descriptor.predicate_source_idx].ToUnifiedFormat(predicate_format);
	auto predicate_data = UnifiedVectorFormat::GetData<string_t>(predicate_format);
	auto predicate_sel = predicate_format.sel;
	auto &predicate_validity = predicate_format.validity;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = predicate_sel->get_index(row_idx);
		bool high = false;
		bool low = false;
		if (predicate_validity.RowIsValid(source_idx)) {
			auto predicate = predicate_data[source_idx];
			high = SljitStringEqualsConstant(predicate, descriptor.constants[0]) ||
			       SljitStringEqualsConstant(predicate, descriptor.constants[1]);
			low = !high;
		}
		high_data[row_idx] = high ? 1 : 0;
		low_data[row_idx] = low ? 1 : 0;
	}
	projected.SetChildCardinality(count);
	return true;
}

enum class SljitPostJoinProjectionFastPath : uint8_t { NONE, STRING_SET_CASE_GROUPED_PAYLOAD };

struct SljitPostJoinProjectionDescriptor {
	bool Built() const {
		return build_state.Built();
	}

	bool Ready() const {
		return build_state.Ready();
	}

	const string &Blocker() const {
		return build_state.blocker;
	}

	void ClearBuiltState() {
		projection_idx = DConstants::INVALID_INDEX;
		projection_ref = nullptr;
		composed_projection = SljitExecutableRegionOp();
		output_to_projection.clear();
	}

	bool Block(const char *blocker_p) {
		ClearBuiltState();
		return build_state.Block(blocker_p);
	}

	void MarkReady() {
		build_state.MarkReady();
	}

	void BorrowProjection(idx_t projection_idx_p, SljitExecutableRegionOp &projection_p) {
		projection_idx = projection_idx_p;
		projection_ref = &projection_p;
	}

	void OwnProjection(idx_t projection_idx_p, SljitExecutableRegionOp &&projection_p) {
		projection_idx = projection_idx_p;
		composed_projection = std::move(projection_p);
		projection_ref = optional_ptr<SljitExecutableRegionOp>(&composed_projection);
	}

	SljitExecutableRegionOp &Projection() {
		return *projection_ref;
	}

	optional_ptr<const vector<idx_t>> OutputMap() const {
		if (output_to_projection.empty()) {
			return nullptr;
		}
		return optional_ptr<const vector<idx_t>>(&output_to_projection);
	}

	SljitDeferredBuildState build_state;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	optional_ptr<SljitExecutableRegionOp> projection_ref;
	SljitExecutableRegionOp composed_projection;
	vector<idx_t> output_to_projection;
};

struct SljitPostJoinProjectionStrategy {
	void Initialize(idx_t hash_join_idx_p, idx_t first_projection_idx_p, idx_t final_projection_idx_p) {
		hash_join_idx = hash_join_idx_p;
		first_projection_idx = first_projection_idx_p;
		final_projection_idx = final_projection_idx_p;
		trace_projection_idx = first_projection_idx_p;
	}

	void EnableStringSetCaseGroupedPayload(const SljitStringSetCaseGroupedPayloadProjection &descriptor) {
		fast_path = SljitPostJoinProjectionFastPath::STRING_SET_CASE_GROUPED_PAYLOAD;
		string_set_case_projection = descriptor;
		direct_projection_disabled_reason = "string_set_case_fast_path";
	}

	bool HasFastPath() const {
		return fast_path != SljitPostJoinProjectionFastPath::NONE;
	}

	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t trace_projection_idx = DConstants::INVALID_INDEX;
	const char *direct_projection_disabled_reason = nullptr;
	SljitPostJoinProjectionFastPath fast_path = SljitPostJoinProjectionFastPath::NONE;
	SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
	SljitPostJoinProjectionDescriptor descriptor;
};

} // namespace duckdb
