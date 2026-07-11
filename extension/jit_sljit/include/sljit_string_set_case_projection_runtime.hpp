//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_string_set_case_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_fast_path.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <cstring>

namespace duckdb {

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

struct SljitStringConstantSignature {
	uint64_t header = 0;
	uint64_t inline_tail = 0;
	bool inlined = false;
};

static SljitStringConstantSignature SljitPrepareStringConstantSignature(const string &constant) {
	SljitStringConstantSignature result;
	string_t value(constant);
	result.header = Load<uint64_t>(const_data_ptr_cast(&value));
	result.inlined = value.IsInlined();
	if (result.inlined) {
		result.inline_tail = Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t));
	}
	return result;
}

static bool SljitStringEqualsConstant(const string_t &value, const string &constant,
                                      const SljitStringConstantSignature &signature) {
	if (Load<uint64_t>(const_data_ptr_cast(&value)) != signature.header) {
		return false;
	}
	if (signature.inlined) {
		return Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t)) == signature.inline_tail;
	}
	return memcmp(value.GetData(), constant.data(), constant.size()) == 0;
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
	std::array<SljitStringConstantSignature, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> signatures;
	for (idx_t constant_idx = 0; constant_idx < signatures.size(); constant_idx++) {
		signatures[constant_idx] = SljitPrepareStringConstantSignature(descriptor.constants[constant_idx]);
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = predicate_sel->get_index(row_idx);
		bool high = false;
		bool low = false;
		if (predicate_validity.RowIsValid(source_idx)) {
			auto predicate = predicate_data[source_idx];
			high = SljitStringEqualsConstant(predicate, descriptor.constants[0], signatures[0]) ||
			       SljitStringEqualsConstant(predicate, descriptor.constants[1], signatures[1]);
			low = !high;
		}
		high_data[row_idx] = high ? 1 : 0;
		low_data[row_idx] = low ? 1 : 0;
	}
	projected.SetChildCardinality(count);
	return true;
}

static bool SljitTryBuildStringSetCaseGroupedPayloadProjection(
    const vector<SljitExecutableRegionOp> &ops, const SljitPreJoinProjectionViewDescriptor &pre_join_view,
    idx_t first_projection_idx, idx_t final_projection_idx, SljitStringSetCaseGroupedPayloadProjection &descriptor) {
	descriptor = SljitStringSetCaseGroupedPayloadProjection();
	if (pre_join_view.columns.size() != 2 ||
	    pre_join_view.columns[0].kind != SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST ||
	    pre_join_view.columns[0].source_idx != 0 ||
	    pre_join_view.columns[1].kind != SljitPreJoinProjectionViewColumnKind::REFERENCE ||
	    pre_join_view.columns[1].source_idx != 1 || pre_join_view.hash_join_idx >= ops.size() ||
	    first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx) {
		return false;
	}
	auto &join = ops[pre_join_view.hash_join_idx].hash_join_probe.plan;
	if (join.keys.size() != 1 || join.keys[0].key_input_index != 0 ||
	    join.keys[0].key_kind != SljitNativeHashJoinKeyKind::INT32) {
		return false;
	}
	auto &first_projection = ops[first_projection_idx];
	if (first_projection.projections.size() != 2) {
		return false;
	}
	idx_t predicate_projection_idx = DConstants::INVALID_INDEX;
	idx_t group_projection_idx = DConstants::INVALID_INDEX;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	idx_t compressed_group_source_idx = DConstants::INVALID_INDEX;
	for (idx_t projection_idx = 0; projection_idx < first_projection.projections.size(); projection_idx++) {
		auto &plan = first_projection.projections[projection_idx].plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE &&
		    plan.return_type.id() == LogicalTypeId::VARCHAR) {
			if (predicate_projection_idx != DConstants::INVALID_INDEX) {
				return false;
			}
			predicate_projection_idx = projection_idx;
			predicate_source_idx = plan.source_index;
			continue;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
		    plan.return_type.id() == LogicalTypeId::VARCHAR) {
			if (group_projection_idx != DConstants::INVALID_INDEX) {
				return false;
			}
			group_projection_idx = projection_idx;
			compressed_group_source_idx = plan.source_index;
			continue;
		}
		return false;
	}
	if (predicate_projection_idx == DConstants::INVALID_INDEX || group_projection_idx == DConstants::INVALID_INDEX) {
		return false;
	}

	auto &final_projection = ops[final_projection_idx];
	if (final_projection.projections.size() != 3 || final_projection.output_types.size() != 3 ||
	    !DirectAppendSupportsFixedSizeType(final_projection.output_types[0]) ||
	    final_projection.output_types[1].id() != LogicalTypeId::INTEGER ||
	    final_projection.output_types[2].id() != LogicalTypeId::INTEGER) {
		return false;
	}
	auto &group_compress = final_projection.projections[0].plan;
	if (group_compress.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	    group_compress.source_index != group_projection_idx ||
	    group_compress.return_type != final_projection.output_types[0]) {
		return false;
	}
	std::array<string, 2> matching_constants;
	std::array<string, 2> non_matching_constants;
	if (!SljitTryReadStringSetCaseExpression(final_projection.projections[1], predicate_projection_idx, true,
	                                         matching_constants) ||
	    !SljitTryReadStringSetCaseExpression(final_projection.projections[2], predicate_projection_idx, false,
	                                         non_matching_constants) ||
	    !SljitSameStringConstantSet(matching_constants, non_matching_constants)) {
		return false;
	}
	descriptor.predicate_source_idx = predicate_source_idx;
	descriptor.compressed_group_source_idx = compressed_group_source_idx;
	descriptor.constants = matching_constants;
	return true;
}

} // namespace duckdb
