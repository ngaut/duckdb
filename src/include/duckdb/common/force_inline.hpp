//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/force_inline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

//! Inlines a small function into hot loops even where the compiler's heuristics would call it
#if defined(_MSC_VER)
#define DUCKDB_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define DUCKDB_FORCE_INLINE inline __attribute__((always_inline))
#else
#define DUCKDB_FORCE_INLINE inline
#endif
