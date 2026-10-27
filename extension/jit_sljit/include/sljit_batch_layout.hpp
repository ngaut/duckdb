//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_batch_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! A logical output column and the physical input column carrying its value.
struct SljitBatchLayoutColumn {
	idx_t source_index;
	LogicalType source_type;
	LogicalType type;

	//! Computed output columns have no physical input slot.
	bool HasSource() const {
		return source_index != DConstants::INVALID_INDEX;
	}
};

//! Immutable layout of a producer's output over its input. Type changes require producer evaluation, not
//! reinterpretation.
class SljitBatchLayout {
public:
	SljitBatchLayout(idx_t producer_idx_p, const vector<LogicalType> &input_types,
	                 vector<SljitBatchLayoutColumn> columns_p)
	    : producer_idx(producer_idx_p), columns(std::move(columns_p)) {
		if (producer_idx == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT batch layout requires a producer");
		}
		for (const auto &column : columns) {
			if (column.type.id() == LogicalTypeId::INVALID ||
			    (column.HasSource() ? column.source_index >= input_types.size() ||
			                              input_types[column.source_index] != column.source_type
			                        : column.source_type.id() != LogicalTypeId::INVALID)) {
				throw InternalException("SLJIT batch layout column does not match its input");
			}
		}
	}

	idx_t ProducerIndex() const {
		return producer_idx;
	}

	idx_t ColumnCount() const {
		return columns.size();
	}

	const SljitBatchLayoutColumn &Column(idx_t column_idx) const {
		return columns.at(column_idx);
	}

	bool PreservesSourceColumnOrdinals() const {
		for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
			if (columns[col_idx].source_index != col_idx) {
				return false;
			}
		}
		return true;
	}

	vector<LogicalType> Types() const {
		vector<LogicalType> result;
		result.reserve(columns.size());
		for (const auto &column : columns) {
			result.push_back(column.type);
		}
		return result;
	}

	vector<idx_t> SourceColumns() const {
		vector<idx_t> result;
		result.reserve(columns.size());
		for (const auto &column : columns) {
			if (!column.HasSource()) {
				throw InternalException("SLJIT computed output has no input column mapping");
			}
			result.push_back(column.source_index);
		}
		return result;
	}

private:
	idx_t producer_idx;
	vector<SljitBatchLayoutColumn> columns;
};

} // namespace duckdb
