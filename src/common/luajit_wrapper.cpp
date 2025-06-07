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

lua_State *LuaJITStateWrapper::GetState() const {
    return L;
}

} // namespace duckdb
