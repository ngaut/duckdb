//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_double_source_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

namespace duckdb {

static inline bool IsDirectNativeFloatingSource(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::FLOAT || kind == SljitNativeDoubleSourceKind::DOUBLE;
}

} // namespace duckdb
