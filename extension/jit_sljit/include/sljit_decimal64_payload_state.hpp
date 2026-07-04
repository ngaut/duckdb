//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_decimal64_payload_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

struct SljitRuntimeDecimal64Source {
	const int64_t *data = nullptr;
	const sel_t *source_sel = nullptr;
	const SelectionVector *match_selection = nullptr;
	const data_ptr_t *row_pointers = nullptr;
	idx_t layout_offset = DConstants::INVALID_INDEX;
	bool row_pointer_source = false;
};

struct SljitRuntimeDecimal64DiscountedAmountProgram {
	bool ready = false;
	idx_t gross_source_idx = DConstants::INVALID_INDEX;
	idx_t discount_source_idx = DConstants::INVALID_INDEX;
	idx_t cost_source_idx = DConstants::INVALID_INDEX;
	idx_t quantity_source_idx = DConstants::INVALID_INDEX;
	int64_t discount_base = 0;
};

} // namespace duckdb
