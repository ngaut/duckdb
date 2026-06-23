//===----------------------------------------------------------------------===//
//                         DuckDB
//
// jit_metal_extension.cpp
//
//===----------------------------------------------------------------------===//

#include "jit_metal_extension.hpp"
#include "metal_backend.hpp"

namespace duckdb {

void JitMetalExtension::Load(ExtensionLoader &loader) {
	RegisterMetalExecutionRegionBackend(loader);
}

std::string JitMetalExtension::Name() {
	return "jit_metal";
}

std::string JitMetalExtension::Version() const {
#ifdef EXT_VERSION_JIT_METAL
	return EXT_VERSION_JIT_METAL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(jit_metal, loader) {
	duckdb::RegisterMetalExecutionRegionBackend(loader);
}
}
