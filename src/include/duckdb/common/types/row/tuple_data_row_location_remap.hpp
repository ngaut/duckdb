//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_row_location_remap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class TupleDataRowLocationRemap {
public:
	virtual ~TupleDataRowLocationRemap() = default;

	virtual void Remap(Vector &source_addresses, const SelectionVector &source_sel, Vector &target_addresses,
	                   idx_t count) = 0;

	virtual void Flush() {
	}
};

} // namespace duckdb
