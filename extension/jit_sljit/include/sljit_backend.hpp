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

void RegisterSljitJitBackend(ExtensionLoader &loader);

} // namespace duckdb
