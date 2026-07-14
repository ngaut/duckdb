//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_codegen_capabilities.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

// These queries intentionally have no dependency on SLJIT's private headers so
// architecture and portability tests can inspect backend capabilities without
// importing the machine-code ABI.
bool SljitPrimitiveRunMachineWordSupported();
bool SljitPrimitiveRunCodegenSupported();

} // namespace duckdb
