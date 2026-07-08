//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_count_star_projection_input.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_executable.hpp"

namespace duckdb {

static bool SljitCountStarFixedWidthProjectionInputSupported(const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.size() != 1 ||
	    projection_op.output_types.size() != 1) {
		return false;
	}
	idx_t source_index;
	auto &projection = projection_op.projections[0].plan;
	return TryReadProjectionSourceReferenceIndex(projection, source_index) &&
	       source_index < projection_op.input_types.size() && projection.return_type == projection_op.output_types[0] &&
	       projection_op.input_types[source_index] == projection_op.output_types[0] &&
	       TypeIsConstantSize(projection_op.output_types[0].InternalType());
}

struct SljitStringCompressedCountStarProjectionInput {
	idx_t source_index = DConstants::INVALID_INDEX;
	PhysicalType compressed_type = PhysicalType::INVALID;
};

static bool SljitStringCompressedCountStarProjectionTypeSupported(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
		return true;
	default:
		return false;
	}
}

static bool SljitTryReadStringCompressedCountStarProjectionInput(
    const SljitExecutableRegionOp &projection_op, SljitStringCompressedCountStarProjectionInput &input) {
	input = SljitStringCompressedCountStarProjectionInput();
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.size() != 1 ||
	    projection_op.output_types.size() != 1) {
		return false;
	}
	auto &projection = projection_op.projections[0].plan;
	if (projection.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	    projection.return_type != projection_op.output_types[0] ||
	    projection.source_index >= projection_op.input_types.size() ||
	    projection_op.input_types[projection.source_index].id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	const auto compressed_type = projection_op.output_types[0].InternalType();
	if (!SljitStringCompressedCountStarProjectionTypeSupported(compressed_type) ||
	    projection.string_compress_target_size != GetTypeIdSize(compressed_type)) {
		return false;
	}
	input.source_index = projection.source_index;
	input.compressed_type = compressed_type;
	return true;
}

static bool SljitStringCompressedCountStarProjectionInputMatchesChunk(
    const SljitStringCompressedCountStarProjectionInput &projection_input, DataChunk &input) {
	return projection_input.source_index < input.ColumnCount() &&
	       input.data[projection_input.source_index].GetType().id() == LogicalTypeId::VARCHAR;
}

static bool SljitCountStarProjectionInputSupported(const SljitExecutableRegionOp &projection_op) {
	SljitStringCompressedCountStarProjectionInput string_compressed_input;
	return SljitCountStarFixedWidthProjectionInputSupported(projection_op) ||
	       SljitTryReadStringCompressedCountStarProjectionInput(projection_op, string_compressed_input);
}

} // namespace duckdb
