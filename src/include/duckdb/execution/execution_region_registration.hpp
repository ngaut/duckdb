//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_registration.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_backend.hpp"

namespace duckdb {

class DatabaseInstance;

DUCKDB_API void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend);

} // namespace duckdb
