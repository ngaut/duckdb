//===----------------------------------------------------------------------===//
//                         DuckDB
//
// jit_sljit_extension.cpp
//
//
//===----------------------------------------------------------------------===//

#include "jit_sljit_extension.hpp"
#include "sljit_backend.hpp"

namespace duckdb {

void JitSljitExtension::Load(ExtensionLoader &loader) {
	RegisterSljitExecutionRegionBackend(loader);
}

std::string JitSljitExtension::Name() {
	return "jit_sljit";
}

std::string JitSljitExtension::Version() const {
#ifdef EXT_VERSION_JIT_SLJIT
	return EXT_VERSION_JIT_SLJIT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(jit_sljit, loader) {
	duckdb::RegisterSljitExecutionRegionBackend(loader);
}
}
