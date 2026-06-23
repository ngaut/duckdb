//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metal_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterMetalExecutionRegionBackend(ExtensionLoader &loader);

} // namespace duckdb
