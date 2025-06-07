#pragma once

#include "duckdb/common/common.hpp" // For DUCKDB_API, string, etc.
#include <string>

// Forward declaration for lua_State
struct lua_State;

namespace duckdb {

class LuaJITStateWrapper {
public:
	DUCKDB_API LuaJITStateWrapper();
	DUCKDB_API ~LuaJITStateWrapper();

	// Executes a Lua script string.
	// Returns true on success, false on error.
	// Errors are printed to stderr.
	DUCKDB_API bool ExecuteString(const std::string &lua_script);

	// Get the raw lua_State* if needed for more advanced operations
	DUCKDB_API lua_State *GetState() const;

private:
	lua_State *L;

	// Prevent copying and assignment
	LuaJITStateWrapper(const LuaJITStateWrapper &) = delete;
	LuaJITStateWrapper &operator=(const LuaJITStateWrapper &) = delete;
};

} // namespace duckdb
