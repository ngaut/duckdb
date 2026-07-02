//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_decimal64_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_hash_join_rhs_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

static bool SljitRuntimeExpressionIsDecimal64(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitRuntimeDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right || !SljitRuntimeExpressionIsDecimal64(node) ||
	    !SljitRuntimeExpressionIsDecimal64(*node.left) || !SljitRuntimeExpressionIsDecimal64(*node.right)) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

struct SljitRuntimeDecimal64PayloadSources {
	bool Prepare(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
	             const SelectionVector &match_selection, Vector &row_pointers, idx_t count,
	             const vector<idx_t> &input_source_indices) {
		sources.assign(input_source_indices.size(), SljitRuntimeDecimal64Source());
		lhs_formats.resize(input_source_indices.size());
		lhs_format_count = 0;
		for (idx_t source_idx = 0; source_idx < input_source_indices.size(); source_idx++) {
			if (!PrepareSource(binding, join_input, match_selection, row_pointers, count,
			                   input_source_indices[source_idx], sources[source_idx])) {
				return false;
			}
		}
		return true;
	}

	bool Contains(idx_t source_idx) const {
		return source_idx < sources.size();
	}

	int64_t Load(idx_t source_idx, idx_t row_idx) const {
		auto &source = sources[source_idx];
		if (source.row_pointer_source) {
			auto row_location = source.row_pointers[row_idx];
			return duckdb::Load<int64_t>(row_location + source.layout_offset);
		}
		const auto match_idx = source.match_selection->get_index(row_idx);
		const auto selected_idx = source.source_sel ? source.source_sel[match_idx] : match_idx;
		return source.data[selected_idx];
	}

private:
	bool PrepareSource(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
	                   const SelectionVector &match_selection, Vector &row_pointers, idx_t count, idx_t source_idx,
	                   SljitRuntimeDecimal64Source &source) {
		if (source_idx >= binding.output_types.size() ||
		    binding.output_types[source_idx].InternalType() != PhysicalType::INT64 ||
		    binding.output_types[source_idx].id() != LogicalTypeId::DECIMAL) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (source_idx < lhs_column_count) {
			return PrepareLHS(binding, join_input, match_selection, count, source_idx, source);
		}
		return PrepareRHS(binding, row_pointers, count, source_idx - lhs_column_count, source);
	}

	bool PrepareLHS(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
	                const SelectionVector &match_selection, idx_t count, idx_t source_idx,
	                SljitRuntimeDecimal64Source &source) {
		const auto input_col = binding.lhs_output_column_indices[source_idx];
		if (input_col >= join_input.ColumnCount() ||
		    join_input.data[input_col].GetType() != binding.output_types[source_idx] ||
		    lhs_format_count >= lhs_formats.size()) {
			return false;
		}
		auto &format = lhs_formats[lhs_format_count++];
		join_input.data[input_col].ToUnifiedFormat(format);
		auto source_sel = SljitNormalizedSourceSelectionData(format);
		if (!format.validity.CannotHaveNull()) {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				const auto match_idx = match_selection.get_index(row_idx);
				const auto selected_idx = source_sel ? source_sel[match_idx] : match_idx;
				if (!format.validity.RowIsValid(selected_idx)) {
					return false;
				}
			}
		}
		source.data = UnifiedVectorFormat::GetData<int64_t>(format);
		source.source_sel = source_sel;
		source.match_selection = &match_selection;
		source.row_pointer_source = false;
		return true;
	}

	bool PrepareRHS(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers, idx_t count, idx_t rhs_col_idx,
	                SljitRuntimeDecimal64Source &source) {
		if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		ExecutionHashJoinRHSFixedColumnSource rhs_source;
		if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
		    rhs_source.type != binding.output_types[rhs_col_idx + binding.lhs_output_column_indices.size()] ||
		    rhs_source.physical_type != PhysicalType::INT64 || rhs_source.type.id() != LogicalTypeId::DECIMAL ||
		    rhs_source.layout_offset == DConstants::INVALID_INDEX) {
			return false;
		}
		source.row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_location = source.row_pointers[row_idx];
			if (!row_location ||
			    (!rhs_source.all_valid && !SljitHashJoinRHSFixedColumnSourceIsValid(row_location, rhs_source))) {
				return false;
			}
		}
		source.layout_offset = rhs_source.layout_offset;
		source.row_pointer_source = true;
		return true;
	}

	vector<UnifiedVectorFormat> lhs_formats;
	vector<SljitRuntimeDecimal64Source> sources;
	idx_t lhs_format_count = 0;
};

static bool SljitTryReadRuntimeDecimal64Reference(const ExecutionExpressionIR &node, idx_t source_count,
                                                  idx_t &source_idx) {
	if (node.kind != ExecutionExpressionIRKind::REFERENCE || !SljitRuntimeExpressionIsDecimal64(node) ||
	    node.ref_index >= source_count) {
		return false;
	}
	source_idx = node.ref_index;
	return true;
}

static bool SljitTryReadRuntimeDecimal64Constant(const ExecutionExpressionIR &node, int64_t &constant) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT || node.constant.IsNull() ||
	    node.constant.type().id() != LogicalTypeId::DECIMAL ||
	    node.constant.type().InternalType() != PhysicalType::INT64 || !SljitRuntimeExpressionIsDecimal64(node)) {
		return false;
	}
	constant = node.constant.GetValueUnsafe<int64_t>();
	return true;
}

static bool SljitRuntimeDecimal64BinaryCanRunUnchecked(const ExecutionExpressionIR &node,
                                                       ExecutionExpressionBinaryOp op) {
	return node.kind == ExecutionExpressionIRKind::BINARY && node.binary_op == op && !node.arithmetic_overflow_check &&
	       SljitRuntimeDecimal64BinaryHasRawSemantics(node);
}

static bool
SljitTryBuildRuntimeDecimal64DiscountedAmountProgram(const ExecutionExpressionIR &node, idx_t source_count,
                                                     SljitRuntimeDecimal64DiscountedAmountProgram &program) {
	program = SljitRuntimeDecimal64DiscountedAmountProgram();
	if (!SljitRuntimeDecimal64BinaryCanRunUnchecked(node, ExecutionExpressionBinaryOp::SUBTRACT) || !node.left ||
	    !node.right || !SljitRuntimeDecimal64BinaryCanRunUnchecked(*node.left, ExecutionExpressionBinaryOp::MULTIPLY) ||
	    !SljitRuntimeDecimal64BinaryCanRunUnchecked(*node.right, ExecutionExpressionBinaryOp::MULTIPLY) ||
	    !node.left->left || !node.left->right || !node.right->left || !node.right->right) {
		return false;
	}
	if (!SljitTryReadRuntimeDecimal64Reference(*node.left->left, source_count, program.gross_source_idx) ||
	    !SljitTryReadRuntimeDecimal64Reference(*node.right->left, source_count, program.cost_source_idx) ||
	    !SljitTryReadRuntimeDecimal64Reference(*node.right->right, source_count, program.quantity_source_idx)) {
		return false;
	}
	auto &discount_expr = *node.left->right;
	if (!SljitRuntimeDecimal64BinaryCanRunUnchecked(discount_expr, ExecutionExpressionBinaryOp::SUBTRACT) ||
	    !discount_expr.left || !discount_expr.right ||
	    !SljitTryReadRuntimeDecimal64Constant(*discount_expr.left, program.discount_base) ||
	    !SljitTryReadRuntimeDecimal64Reference(*discount_expr.right, source_count, program.discount_source_idx)) {
		return false;
	}
	program.ready = true;
	return true;
}

static bool SljitTryMaterializeHashJoinAllValidDecimal64ExpressionToBatch(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionExpression &source_expr, DataChunk &join_input,
    const SelectionVector &match_selection, Vector &row_pointers, DataChunk &batch, idx_t output_idx,
    idx_t current_size, idx_t count, const SljitRuntimeDecimal64DiscountedAmountProgram &program,
    string *reason = nullptr) {
	auto set_reason = [&](const char *value) {
		if (reason && reason->empty()) {
			*reason = value;
		}
		return false;
	};
	auto &target = batch.data[output_idx];
	if (source_expr.input_source_indices.empty() || !program.ready) {
		return set_reason("program");
	}
	if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
	    target.GetType().id() != LogicalTypeId::DECIMAL || target.GetType().InternalType() != PhysicalType::INT64 ||
	    FlatVector::GetCapacity(target) < current_size + count) {
		return set_reason("target");
	}

	SljitRuntimeDecimal64PayloadSources sources;
	if (!sources.Prepare(binding, join_input, match_selection, row_pointers, count, source_expr.input_source_indices)) {
		return set_reason("source");
	}
	if (!sources.Contains(program.gross_source_idx) || !sources.Contains(program.discount_source_idx) ||
	    !sources.Contains(program.cost_source_idx) || !sources.Contains(program.quantity_source_idx)) {
		return set_reason("program_source");
	}

	auto result_data = FlatVector::GetDataMutable<int64_t>(target);
	auto &result_validity = FlatVector::ValidityMutable(target);
	result_validity.Reset(current_size + count);
	result_validity.SetAllValid(current_size + count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto gross = sources.Load(program.gross_source_idx, row_idx);
		const auto discount = sources.Load(program.discount_source_idx, row_idx);
		const auto cost = sources.Load(program.cost_source_idx, row_idx);
		const auto quantity = sources.Load(program.quantity_source_idx, row_idx);
		result_data[current_size + row_idx] = gross * (program.discount_base - discount) - cost * quantity;
	}
	return true;
}

} // namespace duckdb
