#include "catch.hpp"
#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp" // Our new FFI struct header
#include <vector>
#include <string>
#include <iostream> // For INFO messages

// This test file demonstrates using LuaJIT FFI to interact with C++ data
// structures (specifically, our FFIVector that mimics simplified DuckDB vectors).

TEST_CASE("LuaJIT FFI with FFIVector Tests", "[luajit][ffi]") {
    duckdb::LuaJITStateWrapper lua_wrapper;
    REQUIRE(lua_wrapper.GetState() != nullptr);

    // --- Common Lua FFI CDEF for FFIVector ---
    // This CDEF string will be prepended to Lua scripts that use FFIVector.
    // It must be kept in sync with the C++ FFIVector struct definition.
    const std::string ffi_cdef_luastring = R"(
        local ffi = require("ffi")
        ffi.cdef[[
            // Must match duckdb::ffi::FFIVector in luajit_ffi_structs.hpp
            typedef struct FFIVector {
                void* data;
                bool* nullmask; // True means NULL
                unsigned long long count; // Using unsigned long long for idx_t
            } FFIVector;

            // Must match duckdb::ffi::FFIString in luajit_ffi_structs.hpp
            typedef struct FFIString {
                char* ptr;
                unsigned int len; // uint32_t
            } FFIString;
            // typedef FFIString ffi_string_array[]; // Example if needed directly

            // Example for specific typed data access if type_id was passed
            // typedef int int_array[]; // Not used directly in this simplified test
        ]]
        return ffi -- Return ffi object for convenience if script needs it
    )";
    REQUIRE(lua_wrapper.ExecuteString(ffi_cdef_luastring)); // Load FFI definitions into the Lua state

    SECTION("Test Case 1: Read Data from C++ FFIVector in Lua") {
        const int data_size = 5;
        std::vector<int> cpp_data = {10, 20, 0, 40, 50}; // Element at index 2 will be NULL
        std::vector<bool> cpp_nulls = {false, false, true, false, false}; // True means NULL

        duckdb::ffi::FFIVector ffi_vec;
        ffi_vec.data = cpp_data.data();
        ffi_vec.nullmask = cpp_nulls.data();
        ffi_vec.count = data_size;

        // Pointer to our C++ FFIVector struct
        void* p_ffi_vec = &ffi_vec;

        std::string lua_script = R"(
            -- ffi object is already loaded by previous ExecuteString
            -- local ffi = require("ffi") -- Not needed if ffi_cdef_luastring returned it and it's global

            function process_and_sum(ffi_vector_ptr)
                local vec_struct = ffi.cast("FFIVector*", ffi_vector_ptr)
                local data_ptr = ffi.cast("int*", vec_struct.data)
                local null_ptr = ffi.cast("bool*", vec_struct.nullmask)
                local count = vec_struct.count

                local sum = 0
                local non_null_count = 0
                for i = 0, count - 1 do
                    if not null_ptr[i] then
                        -- print("Lua: Reading data[" .. i .. "] = " .. data_ptr[i]) -- For debugging
                        sum = sum + data_ptr[i]
                        non_null_count = non_null_count + 1
                    else
                        -- print("Lua: Data at " .. i .. " is NULL") -- For debugging
                    end
                end
                -- Store sum and non_null_count globally for C++ to retrieve (simplistic retrieval for PoC)
                -- A better way would be to return them from the function and get them from Lua stack.
                GLOBAL_SUM = sum
                GLOBAL_NON_NULL_COUNT = non_null_count
                return sum -- Also return directly
            end
        )";
        REQUIRE(lua_wrapper.ExecuteString(lua_script)); // Define the Lua function

        // Now call the Lua function `process_and_sum`
        lua_State* L = lua_wrapper.GetState();
        lua_getglobal(L, "process_and_sum"); // Get the Lua function
        REQUIRE(lua_isfunction(L, -1));

        lua_pushlightuserdata(L, p_ffi_vec); // Pass pointer to FFIVector as argument

        // Call the function: 1 argument, 1 result (the sum)
        if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
            const char* error_msg = lua_tostring(L, -1);
            FAIL("Lua pcall failed: " << (error_msg ? error_msg : "unknown error"));
            lua_pop(L, 1); // Pop error
        } else {
            REQUIRE(lua_isnumber(L, -1)); // Check if the result is a number
            int sum_from_lua = lua_tointeger(L, -1);
            lua_pop(L, 1); // Pop the result

            INFO("Sum calculated by Lua: " << sum_from_lua);
            int expected_sum = 10 + 20 + 40 + 50; // 0 at index 2 is NULL
            REQUIRE(sum_from_lua == expected_sum);

            // Also check globals (alternative way to get results, for PoC)
            lua_getglobal(L, "GLOBAL_SUM");
            REQUIRE(lua_isnumber(L, -1));
            REQUIRE(lua_tointeger(L, -1) == expected_sum);
            lua_pop(L, 1);

            lua_getglobal(L, "GLOBAL_NON_NULL_COUNT");
            REQUIRE(lua_isnumber(L, -1));
            REQUIRE(lua_tointeger(L, -1) == 4); // 4 non-null values
            lua_pop(L, 1);
        }
        lua_settop(L, 0); // Clear stack
    }

    SECTION("Test Case 2: Write Data from Lua to C++ FFIVector") {
        const int data_size = 4;
        std::vector<int> cpp_data = {1, 2, 3, 4};
        std::vector<bool> cpp_nulls = {false, false, false, false}; // Initially all non-NULL

        duckdb::ffi::FFIVector ffi_vec;
        ffi_vec.data = cpp_data.data();
        ffi_vec.nullmask = cpp_nulls.data();
        ffi_vec.count = data_size;
        void* p_ffi_vec = &ffi_vec;

        std::string lua_script = R"(
            -- ffi object assumed to be available globally or loaded

            function modify_vector_data(ffi_vector_ptr)
                local vec = ffi.cast("FFIVector*", ffi_vector_ptr)
                local data_ptr = ffi.cast("int*", vec.data)
                local null_ptr = ffi.cast("bool*", vec.nullmask)
                local count = vec.count

                for i = 0, count - 1 do
                    if i % 2 == 0 then -- Modify even indices
                        data_ptr[i] = data_ptr[i] * 10
                        null_ptr[i] = false -- Ensure not null
                    else -- Set odd indices to NULL
                        data_ptr[i] = -1 -- Set to some sentinel, though it's NULL
                        null_ptr[i] = true -- Set to NULL
                    end
                end
            end
        )";
        REQUIRE(lua_wrapper.ExecuteString(lua_script)); // Define the Lua function

        // Call the Lua function `modify_vector_data`
        lua_State* L = lua_wrapper.GetState();
        lua_getglobal(L, "modify_vector_data");
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, p_ffi_vec);

        if (lua_pcall(L, 1, 0, 0) != LUA_OK) { // 1 arg, 0 results
            const char* error_msg = lua_tostring(L, -1);
            FAIL("Lua pcall for modify_vector_data failed: " << (error_msg ? error_msg : "unknown error"));
            lua_pop(L,1);
        }

        // Verify modifications in C++
        INFO("Data after Lua modification:");
        REQUIRE(cpp_data[0] == 1 * 10);
        REQUIRE(cpp_nulls[0] == false);

        // cpp_data[1] could be -1 or its original value depending on Lua; null flag is key
        REQUIRE(cpp_nulls[1] == true);

        REQUIRE(cpp_data[2] == 3 * 10);
        REQUIRE(cpp_nulls[2] == false);

        REQUIRE(cpp_nulls[3] == true);
        lua_settop(L, 0); // Clear stack
    }

    SECTION("Test Case 3: Different Data Types (Double)") {
        const int data_size = 3;
        std::vector<double> cpp_data_double = {1.5, 2.5, 3.5};
        std::vector<bool> cpp_nulls_double = {false, true, false}; // Middle one is NULL

        duckdb::ffi::FFIVector ffi_vec_double;
        ffi_vec_double.data = cpp_data_double.data();
        ffi_vec_double.nullmask = cpp_nulls_double.data();
        ffi_vec_double.count = data_size;
        void* p_ffi_vec_double = &ffi_vec_double;

        std::string lua_script = R"(
            function process_double_vector(ffi_vector_ptr)
                local vec = ffi.cast("FFIVector*", ffi_vector_ptr)
                -- CRITICAL: Cast to the correct C data type in Lua
                local data_ptr = ffi.cast("double*", vec.data)
                local null_ptr = ffi.cast("bool*", vec.nullmask)
                local count = vec.count
                local sum = 0.0

                for i = 0, count - 1 do
                    if not null_ptr[i] then
                        sum = sum + data_ptr[i]
                        if i == (count - 1) then -- Last non-null element
                           data_ptr[i] = data_ptr[i] + 0.1 -- Modify one value
                        end
                    end
                end
                GLOBAL_DOUBLE_SUM = sum
            end
        )";
        REQUIRE(lua_wrapper.ExecuteString(lua_script));

        lua_State* L = lua_wrapper.GetState();
        lua_getglobal(L, "process_double_vector");
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, p_ffi_vec_double);

        if (lua_pcall(L, 1, 0, 0) != LUA_OK) {
            const char* error_msg = lua_tostring(L, -1);
            FAIL("Lua pcall for process_double_vector failed: " << (error_msg ? error_msg : "unknown error"));
            lua_pop(L,1);
        }

        lua_getglobal(L, "GLOBAL_DOUBLE_SUM");
        REQUIRE(lua_isnumber(L, -1));
        double sum_from_lua = lua_tonumber(L, -1);
        lua_pop(L, 1);

        double expected_sum = 1.5 + 3.5; // 2.5 is NULL
        REQUIRE(sum_from_lua == Approx(expected_sum));
        // Check modification
        REQUIRE(cpp_data_double[2] == Approx(3.5 + 0.1));
        lua_settop(L, 0); // Clear stack
    }

    SECTION("Test Case 4: Read String Data via FFIString") {
        const int data_size = 3;
        // Store actual string data. std::string manages memory.
        std::vector<std::string> string_values = {"hello", "world", "duckdb"};
        std::vector<duckdb::ffi::FFIString> cpp_ffi_strings(data_size);
        std::vector<bool> cpp_nulls = {false, true, false}; // "world" will be NULL

        for(int i=0; i < data_size; ++i) {
            // Point FFIString to the data managed by std::string
            // This is safe as long as string_values outlives cpp_ffi_strings and its use in Lua.
            cpp_ffi_strings[i].ptr = const_cast<char*>(string_values[i].c_str());
            cpp_ffi_strings[i].len = static_cast<uint32_t>(string_values[i].length());
        }

        duckdb::ffi::FFIVector ffi_vec_str;
        // For FFIVector of FFIString, `data` points to an array of FFIString structs
        ffi_vec_str.data = cpp_ffi_strings.data();
        ffi_vec_str.nullmask = cpp_nulls.data();
        ffi_vec_str.count = data_size;
        void* p_ffi_vec_str = &ffi_vec_str;

        std::string lua_script = R"(
            function process_string_vector(ffi_vector_ptr)
                local vec = ffi.cast("FFIVector*", ffi_vector_ptr)
                -- Cast FFIVector.data to an array of FFIString structs
                local string_data_array = ffi.cast("FFIString*", vec.data)
                local null_ptr = ffi.cast("bool*", vec.nullmask)
                local count = vec.count

                GLOBAL_STRING_CONCAT = ""
                GLOBAL_STRING_LENGTH_SUM = 0

                for i = 0, count - 1 do
                    if not null_ptr[i] then
                        -- Access FFIString struct members
                        local ffi_str_item = string_data_array[i]
                        -- Convert to Lua string for manipulation
                        local lua_s = ffi.string(ffi_str_item.ptr, ffi_str_item.len)
                        -- print("Lua: String[" .. i .. "] = '" .. lua_s .. "', len=" .. ffi_str_item.len)
                        GLOBAL_STRING_CONCAT = GLOBAL_STRING_CONCAT .. lua_s
                        GLOBAL_STRING_LENGTH_SUM = GLOBAL_STRING_LENGTH_SUM + ffi_str_item.len
                        if lua_s == "duckdb" then -- Example: modify a C++ FFIString based on content
                           -- This is dangerous if the original C buffer isn't writable or large enough
                           -- For this test, string_values[i].c_str() is const.
                           -- To test writing, ptr should point to a mutable buffer.
                           -- For now, we just read.
                           -- ffi_str_item.ptr[0] = 'D' -- Example write (needs mutable C buffer)
                        end
                    else
                        -- print("Lua: String[" .. i .. "] is NULL")
                    end
                end
            end
        )";
        REQUIRE(lua_wrapper.ExecuteString(lua_script));

        lua_State* L = lua_wrapper.GetState();
        lua_getglobal(L, "process_string_vector");
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, p_ffi_vec_str);

        if (lua_pcall(L, 1, 0, 0) != LUA_OK) {
            const char* error_msg = lua_tostring(L, -1);
            FAIL("Lua pcall for process_string_vector failed: " << (error_msg ? error_msg : "unknown error"));
            lua_pop(L,1);
        }

        lua_getglobal(L, "GLOBAL_STRING_CONCAT");
        REQUIRE(lua_isstring(L, -1));
        std::string concat_from_lua = lua_tostring(L, -1);
        lua_pop(L, 1);

        lua_getglobal(L, "GLOBAL_STRING_LENGTH_SUM");
        REQUIRE(lua_isnumber(L, -1));
        int len_sum_from_lua = lua_tointeger(L, -1);
        lua_pop(L, 1);

        std::string expected_concat = "helloduckdb"; // "world" is NULL
        int expected_len_sum = string_values[0].length() + string_values[2].length();

        REQUIRE(concat_from_lua == expected_concat);
        REQUIRE(len_sum_from_lua == expected_len_sum);

        // Example of checking if C++ data was modified (if Lua script wrote back)
        // For this read-only test, string_values should be unchanged.
        // REQUIRE(string_values[2][0] == 'D'); // If we had enabled write test

        lua_settop(L, 0); // Clear stack
    }
}
```
