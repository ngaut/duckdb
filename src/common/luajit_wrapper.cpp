#include "duckdb/common/luajit_wrapper.hpp"
#include <iostream> // For stderr

// Correct way to include LuaJIT headers
// They are typically C headers, so wrap in extern "C" if included from C++
extern "C" {
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
// #include "luajit.h" // Specific LuaJIT API extensions, if needed
}

namespace duckdb {

LuaJITStateWrapper::LuaJITStateWrapper() : L(nullptr) {
    L = luaL_newstate();
    if (!L) {
        // Consider throwing an exception here for critical failure
        std::cerr << "Error: Failed to create LuaJIT state (luaL_newstate returned NULL)." << std::endl;
        return;
    }
    luaL_openlibs(L); // Load standard libraries (base, table, string, math, io, os, package, debug, ffi)

    // Register custom C functions for FFI
    lua_pushcfunction(L, duckdb_ffi_add_string_to_output_vector);
    lua_setglobal(L, "duckdb_ffi_add_string_to_output_vector");

    lua_pushcfunction(L, duckdb_ffi_set_string_output_null);
    lua_setglobal(L, "duckdb_ffi_set_string_output_null");

    lua_pushcfunction(L, duckdb_ffi_extract_from_date);
    lua_setglobal(L, "duckdb_ffi_extract_from_date");

    lua_pushcfunction(L, duckdb_ffi_extract_from_timestamp);
    lua_setglobal(L, "duckdb_ffi_extract_from_timestamp");

    lua_pushcfunction(L, duckdb_ffi_extract_year_from_date);
    lua_setglobal(L, "duckdb_ffi_extract_year_from_date");

    lua_pushcfunction(L, duckdb_ffi_add_lua_string_table_to_output_vector);
    lua_setglobal(L, "duckdb_ffi_add_lua_string_table_to_output_vector");

    lua_pushcfunction(L, duckdb_ffi_starts_with);
    lua_setglobal(L, "duckdb_ffi_starts_with");

    lua_pushcfunction(L, duckdb_ffi_contains);
    lua_setglobal(L, "duckdb_ffi_contains");

    lua_pushcfunction(L, duckdb_ffi_date_trunc);
    lua_setglobal(L, "duckdb_ffi_date_trunc");
}

LuaJITStateWrapper::~LuaJITStateWrapper() {
    if (L) {
        lua_close(L);
        L = nullptr;
    }
}

bool LuaJITStateWrapper::ExecuteString(const std::string &lua_script) {
    if (!L) {
        std::cerr << "Error: LuaJIT state is not initialized." << std::endl;
        return false;
    }

    // Load the string containing the script
    if (luaL_loadstring(L, lua_script.c_str()) != LUA_OK) {
        // Error loading the script (e.g., syntax error)
        // The error message is on top of the stack
        const char *error_msg = lua_tostring(L, -1);
        if (error_msg) {
            std::cerr << "LuaJIT Error (luaL_loadstring): " << error_msg << std::endl;
        } else {
            std::cerr << "LuaJIT Error (luaL_loadstring): Unknown error." << std::endl;
        }
        lua_pop(L, 1); // Pop the error message from the stack
        return false;
    }

    // Execute the loaded script
    // 0 arguments, 0 results, 0 is for the default error handler (prints to stderr)
    // For more control, a custom error handler function can be pushed onto the stack before pcall.
    if (lua_pcall(L, 0, LUA_MULTRET, 0) != LUA_OK) {
        // Error executing the script (e.g., runtime error)
        // The error message is on top of the stack
        const char *error_msg = lua_tostring(L, -1);
        if (error_msg) {
            std::cerr << "LuaJIT Error (lua_pcall): " << error_msg << std::endl;
        } else {
            std::cerr << "LuaJIT Error (lua_pcall): Unknown error." << std::endl;
        }
        lua_pop(L, 1); // Pop the error message
        return false;
    }

    // If execution reached here, it was successful.
    // Any return values from the script are now on the stack.
    // For this simple ExecuteString, we don't process them here but clear them.
    lua_settop(L, 0); // Clear the stack

    return true;
}

// Overload that captures error messages
bool LuaJITStateWrapper::ExecuteString(const std::string &lua_script, std::string& out_error_message) {
    out_error_message.clear();
    if (!L) {
        out_error_message = "LuaJIT state is not initialized.";
        return false;
    }

    if (luaL_loadstring(L, lua_script.c_str()) != LUA_OK) {
        const char *error_msg_c = lua_tostring(L, -1);
        out_error_message = error_msg_c ? error_msg_c : "Unknown error during luaL_loadstring.";
        lua_pop(L, 1); // Pop error message
        return false;
    }

    if (lua_pcall(L, 0, LUA_MULTRET, 0) != LUA_OK) {
        const char *error_msg_c = lua_tostring(L, -1);
        out_error_message = error_msg_c ? error_msg_c : "Unknown error during lua_pcall.";
        lua_pop(L, 1); // Pop error message
        return false;
    }
    lua_settop(L, 0); // Clear the stack of any return values
    return true;
}


lua_State *LuaJITStateWrapper::GetState() const {
    return L;
}

bool LuaJITStateWrapper::CompileStringAndSetGlobal(const std::string &full_lua_function_definition_script,
                                               const std::string &global_function_name,
                                               std::string& out_error_message) {
    out_error_message.clear();
    if (!L) {
        out_error_message = "LuaJIT state is not initialized for CompileStringAndSetGlobal.";
        return false;
    }

    // The full_lua_function_definition_script should already be in the form:
    // `global_function_name = function(args...) ... end`
    // or `function global_function_name(args...) ... end`
    // So we just need to execute it.
    return ExecuteString(full_lua_function_definition_script, out_error_message);
}

bool LuaJITStateWrapper::PCallGlobal(const std::string& global_function_name,
                                   const std::vector<ffi::FFIVector*>& ffi_input_args,
                                   ffi::FFIVector* ffi_output_arg,
                                   idx_t count,
                                   std::string& out_error_message) {
    out_error_message.clear();
    if (!L) {
        out_error_message = "LuaJIT state is not initialized for PCallGlobal.";
        return false;
    }

    lua_getglobal(L, global_function_name.c_str());
    if (!lua_isfunction(L, -1)) {
        out_error_message = "Lua function '" + global_function_name + "' not found or not a function.";
        lua_pop(L, 1); // Pop non-function
        return false;
    }

    // Push arguments: output_ffi_vec, input1_ffi_vec, ..., inputN_ffi_vec, count
    int num_args = 0;
    if (ffi_output_arg) { // Output arg is optional; some functions might only read.
        lua_pushlightuserdata(L, ffi_output_arg);
        num_args++;
    } else { // Push a nil placeholder if no output FFIVector is provided (e.g. for scalar returns)
        lua_pushnil(L);
        num_args++;
    }

    for (const auto& input_arg : ffi_input_args) {
        lua_pushlightuserdata(L, input_arg);
        num_args++;
    }
    lua_pushinteger(L, count);
    num_args++;

    if (lua_pcall(L, num_args, 0, 0) != LUA_OK) { // 0 results expected on Lua stack from the function itself
        const char *error_msg_c = lua_tostring(L, -1);
        out_error_message = error_msg_c ? error_msg_c : "Unknown error during lua_pcall for " + global_function_name;
        lua_pop(L, 1); // Pop error message
        return false;
    }
    // Function results, if any, are not handled by this wrapper on C++ stack, Lua function should write to FFIVector
    return true;
}

} // namespace duckdb
