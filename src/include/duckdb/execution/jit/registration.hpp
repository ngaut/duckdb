//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/registration.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

class DatabaseInstance;

DUCKDB_API void RegisterJitBackend(DatabaseInstance &db, unique_ptr<JitBackend> backend);

} // namespace duckdb
