#pragma once

#include "duckdb/common/common.hpp" // For DUCKDB_API, string, etc.
#include <string>
#include <vector> // Added for std::vector
#include "duckdb/common/luajit_ffi_structs.hpp" // Added for ffi::FFIVector

// Forward declaration for lua_State
struct lua_State;

namespace duckdb {

class LuaJITStateWrapper {
public:
	DUCKDB_API LuaJITStateWrapper();
	DUCKDB_API ~LuaJITStateWrapper();

	// Executes a Lua script string. Errors are printed to stderr.
	DUCKDB_API bool ExecuteString(const std::string &lua_script);
    // Overload that captures error messages
	DUCKDB_API bool ExecuteString(const std::string &lua_script, std::string& out_error_message);


	// Get the raw lua_State* if needed for more advanced operations
	DUCKDB_API lua_State *GetState() const;

	// Compiles a Lua script string that defines a function and sets it as a global variable.
	// Errors are captured in out_error_message.
	DUCKDB_API bool CompileStringAndSetGlobal(const std::string &full_lua_function_definition_script,
	                                          const std::string &global_function_name,
	                                          std::string& out_error_message);

    // Calls a globally defined Lua function with FFIVector arguments.
    // Assumes function signature: func_name(output_ffi_vec*, input1_ffi_vec*, ..., inputN_ffi_vec*, count)
    // Errors are captured in out_error_message.
    DUCKDB_API bool PCallGlobal(const std::string& global_function_name,
                                const std::vector<ffi::FFIVector*>& ffi_input_args,
                                ffi::FFIVector* ffi_output_arg,
                                idx_t count,
                                std::string& out_error_message);

private:
	lua_State *L;

	// Prevent copying and assignment
	LuaJITStateWrapper(const LuaJITStateWrapper &) = delete;
	LuaJITStateWrapper &operator=(const LuaJITStateWrapper &) = delete;
};

} // namespace duckdb
