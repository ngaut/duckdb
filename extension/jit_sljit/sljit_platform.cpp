#include "sljit_platform.hpp"

#include "sljitLir.h"

namespace duckdb {

bool SljitPlatformAvailable() {
	return sljit_get_platform_name() != nullptr;
}

} // namespace duckdb
