//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_function_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <exception>

namespace duckdb {

template <class FUNCTION, class INPUT>
static void SljitExecuteNativeFunction(FUNCTION function, INPUT &native_input) {
	function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

} // namespace duckdb
