//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_backend.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterSljitExecutionRegionBackend(ExtensionLoader &loader);

} // namespace duckdb
