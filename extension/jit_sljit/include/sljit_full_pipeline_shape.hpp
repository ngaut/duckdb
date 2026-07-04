//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_shape.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

struct SljitFullPipelineProjectionAggregateShape {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	idx_t ProjectionCount() const {
		if (first_projection_idx == DConstants::INVALID_INDEX || final_projection_idx == DConstants::INVALID_INDEX ||
		    final_projection_idx < first_projection_idx) {
			return 0;
		}
		return final_projection_idx - first_projection_idx + 1;
	}
};

} // namespace duckdb
