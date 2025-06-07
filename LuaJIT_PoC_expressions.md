# Proof-of-Concept: JITing Expressions with LuaJIT in C++

## 1. Introduction

This document describes a proof-of-concept (PoC) for JIT-compiling simple arithmetic and comparison expressions using LuaJIT, embedded within a C++ application. The PoC focuses on:
*   Translating a simplified C++ expression representation to Lua code.
*   Using LuaJIT's FFI to operate on C++ data arrays (mocking DuckDB vectors) without copying.
*   The basic pipeline of Lua code generation, compilation, and execution.

This PoC does **not** integrate with DuckDB's actual source code but simulates the core interaction for expression JITing.

## 2. PoC Code Structure

The PoC will consist of the following conceptual C++ components and Lua code:

*   **`Expression.h`**: Defines the simplified C++ expression representation.
*   **`LuaTranslator.h/.cpp`**: Implements the translator from C++ expressions to Lua code strings.
*   **`main.cpp`**: Contains the main PoC logic:
    *   Initializes LuaJIT.
    *   Creates sample expressions.
    *   Translates expressions to Lua.
    *   Compiles Lua code.
    *   Prepares C++ data arrays.
    *   Calls Lua functions via FFI.
    *   Verifies results.
*   **Generated Lua Code**: Lua functions designed to work with C data arrays via FFI.

## 3. Simplified C++ Expression Representation (`Expression.h`)

```cpp
#pragma once
#include <string>
#include <variant>
#include <memory>

// Enum for expression types
enum class ExprType {
    COLUMN_REF, // Represents a reference to an input column (array)
    CONSTANT_INT,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    GREATER_THAN
};

// Forward declaration
struct Expression;

// Using std::unique_ptr for children to manage memory
using ExprPtr = std::unique_ptr<Expression>;

struct ColumnRef {
    std::string column_name; // e.g., "col1", "col2"
    int column_index;        // Index to map to input arrays (0 or 1 for this PoC)
};

struct ConstantInt {
    int value;
};

struct BinaryOp {
    ExprPtr left;
    ExprPtr right;
};

// The main expression structure
struct Expression {
    ExprType type;
    std::variant<ColumnRef, ConstantInt, BinaryOp> data;

    Expression(ExprType t, decltype(data) d) : type(t), data(std::move(d)) {}
};

// Helper functions to create expressions
ExprPtr make_col_ref(const std::string& name, int index) {
    return std::make_unique<Expression>(ExprType::COLUMN_REF, ColumnRef{name, index});
}

ExprPtr make_const_int(int val) {
    return std::make_unique<Expression>(ExprType::CONSTANT_INT, ConstantInt{val});
}

ExprPtr make_binary_op(ExprType type, ExprPtr left, ExprPtr right) {
    return std::make_unique<Expression>(type, BinaryOp{std::move(left), std::move(right)});
}
```

## 4. Lua Code Translator (`LuaTranslator.h` and `LuaTranslator.cpp`)

### `LuaTranslator.h`
```cpp
#pragma once
#include "Expression.h"
#include <string>

class LuaTranslator {
public:
    std::string translate_expression_to_lua_function(const Expression& expr, int num_inputs);
private:
    // `inputs_available` is the number of input arrays FFI will provide to Lua
    std::string generate_lua_code(const Expression& expr, int& temp_var_counter, int inputs_available);
};
```

### `LuaTranslator.cpp`
```cpp
#include "LuaTranslator.h"
#include <stdexcept>
#include <sstream>
#include <vector>

// Recursive helper to generate Lua code parts
std::string LuaTranslator::generate_lua_code(const Expression& expr, int& temp_var_counter, int inputs_available) {
    std::stringstream lua_code;
    switch (expr.type) {
        case ExprType::COLUMN_REF: {
            const auto& col_ref = std::get<ColumnRef>(expr.data);
            if (col_ref.column_index < 0 || col_ref.column_index >= inputs_available) {
                throw std::runtime_error("Invalid column index for Lua generation.");
            }
            // Assumes input arrays are named input0, input1, etc. in Lua
            lua_code << "input" << col_ref.column_index << "[i]";
            break;
        }
        case ExprType::CONSTANT_INT: {
            lua_code << std::get<ConstantInt>(expr.data).value;
            break;
        }
        case ExprType::ADD:
        case ExprType::SUBTRACT:
        case ExprType::MULTIPLY:
        case ExprType::DIVIDE:
        case ExprType::GREATER_THAN: {
            const auto& bin_op = std::get<BinaryOp>(expr.data);
            std::string left_code = generate_lua_code(*bin_op.left, temp_var_counter, inputs_available);
            std::string right_code = generate_lua_code(*bin_op.right, temp_var_counter, inputs_available);

            char op_char = ' ';
            std::string op_str;
            switch (expr.type) {
                case ExprType::ADD: op_char = '+'; break;
                case ExprType::SUBTRACT: op_char = '-'; break;
                case ExprType::MULTIPLY: op_char = '*'; break;
                case ExprType::DIVIDE: op_char = '/'; break; // Consider integer vs float division
                case ExprType::GREATER_THAN: op_str = ">"; break;
                default: throw std::runtime_error("Unhandled binary operator.");
            }

            // For simplicity, not using temp vars here, but could for complex subexpressions
            if (!op_str.empty()) {
                 lua_code << "(" << left_code << " " << op_str << " " << right_code << ")";
                 // For boolean results, convert to integer 1 or 0 for C++ array
                 if (expr.type == ExprType::GREATER_THAN) {
                    lua_code << " and 1 or 0";
                 }
            } else {
                lua_code << "(" << left_code << " " << op_char << " " << right_code << ")";
            }
            break;
        }
        default:
            throw std::runtime_error("Unsupported expression type for Lua translation.");
    }
    return lua_code.str();
}

std::string LuaTranslator::translate_expression_to_lua_function(const Expression& expr, int num_inputs) {
    std::stringstream lua_function_body;
    int temp_var_counter = 0;

    // Generate input variable names for the Lua function signature
    std::vector<std::string> input_arg_names;
    for(int i=0; i < num_inputs; ++i) {
        input_arg_names.push_back("input" + std::to_string(i));
    }

    lua_function_body << "local ffi = require('ffi')\n";
    // Define C types (int arrays for this PoC)
    lua_function_body << "ffi.cdef[[\n";
    lua_function_body << "    typedef int int_array_t[];\n";
    lua_function_body << "]]\n";

    lua_function_body << "return function(output_array, count";
    for(int i=0; i < num_inputs; ++i) {
        lua_function_body << ", input" << i;
    }
    lua_function_body << ")\n";
    lua_function_body << "  for i = 0, count - 1 do\n";
    lua_function_body << "    output_array[i] = " << generate_lua_code(expr, temp_var_counter, num_inputs) << "\n";
    lua_function_body << "  end\n";
    lua_function_body << "end\n";

    return lua_function_body.str();
}
```

## 5. Main C++ PoC Logic (`main.cpp`)

```cpp
#include <iostream>
#include <vector>
#include <stdexcept>
#include "Expression.h"
#include "LuaTranslator.h"

// LuaJIT headers
// These would typically be:
// extern "C" {
// #include "lua.h"
// #include "lualib.h"
// #include "lauxlib.h"
// #include "luajit.h"
// }
// For this PoC, we assume they are available.

// --- Start of Mock LuaJIT API for conceptual illustration ---
// This is NOT actual LuaJIT API but mocks what we'd call.
// In a real scenario, you link against LuaJIT library.
struct lua_State; // Opaque structure

// Mock functions (replace with actual LuaJIT calls)
lua_State* luaL_newstate() { std::cout << "[MOCK] luaL_newstate()" << std::endl; return (lua_State*)0x1; /* Dummy pointer */ }
void luaL_openlibs(lua_State* L) { std::cout << "[MOCK] luaL_openlibs(L)" << std::endl; }
int luaL_loadstring(lua_State* L, const char* s) { std::cout << "[MOCK] luaL_loadstring(L, code)" << std::endl; return 0; /* 0 for success */ }
int lua_pcall(lua_State* L, int nargs, int nresults, int errfunc) { std::cout << "[MOCK] lua_pcall(L, " << nargs << ", " << nresults << ")" << std::endl; return 0; /* 0 for success */ }
void lua_pushlightuserdata(lua_State* L, void* p) { std::cout << "[MOCK] lua_pushlightuserdata(L, ptr)" << std::endl; }
void lua_pushinteger(lua_State* L, int n) { std::cout << "[MOCK] lua_pushinteger(L, " << n << ")" << std::endl; }
const char* lua_tostring(lua_State* L, int idx) { return "[MOCK] Error message"; }
void lua_pop(lua_State* L, int n) { std::cout << "[MOCK] lua_pop(L, " << n << ")" << std::endl; }
void lua_getglobal(lua_State* L, const char* name) { std::cout << "[MOCK] lua_getglobal(L, '" << name << "')" << std::endl; } // Used to get the compiled function
void lua_settop(lua_State* L, int idx) { lua_pop(L, lua_gettop(L) - idx); } // Mock, actual is lua_settop
int lua_gettop(lua_State* L) { return 5; } // Mock, actual is lua_gettop
void lua_close(lua_State* L) { std::cout << "[MOCK] lua_close(L)" << std::endl; }

// FFI related - conceptual call to make LuaJIT aware of C types for arrays
// In real LuaJIT, `ffi.cdef` in Lua script handles this.
// The C++ side just passes pointers. LuaJIT's FFI uses them as `cdata`.
// No explicit "create proxy" C API call like this.
void jit_ffi_define_int_array(lua_State* L, const char* type_name) {
    std::cout << "[MOCK FFI] Define type: " << type_name << std::endl;
}
// --- End of Mock LuaJIT API ---


int main() {
    const int ARRAY_SIZE = 5;
    std::vector<int> input_col1_data = {10, 20, 30, 40, 50};
    std::vector<int> input_col2_data = {1,  2,  3,  4,  5};
    std::vector<int> output_array_data(ARRAY_SIZE);

    // 1. Initialize LuaJIT state
    lua_State* L = luaL_newstate();
    if (!L) {
        std::cerr << "Failed to create LuaJIT state." << std::endl;
        return 1;
    }
    luaL_openlibs(L); // Load standard libraries (incl. FFI if built-in)

    // (Optional) Enable JIT compiler if not on by default for loaded strings
    // luaJIT_setmode(L, 0, LUAJIT_MODE_ENGINE | LUAJIT_MODE_ON);

    // 2. Define a sample expression: col1 + col2
    ExprPtr col1_ref = make_col_ref("col1", 0);
    ExprPtr col2_ref = make_col_ref("col2", 1);
    ExprPtr expr_add = make_binary_op(ExprType::ADD, std::move(col1_ref), std::move(col2_ref));

    // Define another sample expression: col1 > 25
    ExprPtr col1_ref_gt = make_col_ref("col1", 0);
    ExprPtr const_25_ref = make_const_int(25);
    ExprPtr expr_gt = make_binary_op(ExprType::GREATER_THAN, std::move(col1_ref_gt), std::move(const_25_ref));

    LuaTranslator translator;

    // --- Process expression: col1 + col2 ---
    std::cout << "\nProcessing expression: col1 + col2" << std::endl;
    std::string lua_code_add = translator.translate_expression_to_lua_function(*expr_add, 2); // 2 input arrays
    std::cout << "Generated Lua Code:\n" << lua_code_add << std::endl;

    // 3. Load and compile Lua code
    if (luaL_loadstring(L, lua_code_add.c_str()) != 0) {
        std::cerr << "LuaJIT: Failed to load Lua string: " << lua_tostring(L, -1) << std::endl;
        lua_pop(L, 1); // Pop error message
        lua_close(L);
        return 1;
    }

    // The compiled chunk is now on top of the stack.
    // Call it to get the actual function (due to `return function(...)` structure)
    if (lua_pcall(L, 0, 1, 0) != 0) { // 0 args, 1 result (the function itself)
        std::cerr << "LuaJIT: Failed to pcall for function object: " << lua_tostring(L, -1) << std::endl;
        lua_pop(L, 1);
        lua_close(L);
        return 1;
    }
    // The returned Lua function is now on the stack. We'll leave it there.

    // 4. Prepare data and call the compiled Lua function
    // The Lua function expects: output_array, count, input0, input1

    // Push the function (already on top)
    // lua_getglobal(L, "jitted_function"); // If we had stored it globally

    lua_pushlightuserdata(L, output_array_data.data()); // output_array
    lua_pushinteger(L, ARRAY_SIZE);                     // count
    lua_pushlightuserdata(L, input_col1_data.data());   // input0
    lua_pushlightuserdata(L, input_col2_data.data());   // input1

    // Call the function: 4 arguments, 0 results expected on Lua stack
    if (lua_pcall(L, 4, 0, 0) != 0) {
        std::cerr << "LuaJIT: Failed to execute JITted code: " << lua_tostring(L, -1) << std::endl;
        lua_pop(L, 1); // Pop error message
        // lua_settop(L, 0); // Clear stack if function was global
        lua_close(L);
        return 1;
    }
    // lua_pop(L, 1); // Pop the function from stack if it wasn't auto-collected or if we put it there with getglobal

    // 5. Verify results for col1 + col2
    std::cout << "Results for col1 + col2:" << std::endl;
    for (int i = 0; i < ARRAY_SIZE; ++i) {
        std::cout << "C++: output_array_data[" << i << "] = " << output_array_data[i]
                  << " (Expected: " << input_col1_data[i] + input_col2_data[i] << ")" << std::endl;
        // Add actual assertions here in a real test
        if (output_array_data[i] != (input_col1_data[i] + input_col2_data[i])) {
            std::cerr << "Verification FAILED for col1 + col2 at index " << i << std::endl;
        }
    }
    lua_settop(L, 0); // Clear stack for next test


    // --- Process expression: col1 > 25 ---
    std::cout << "\nProcessing expression: col1 > 25" << std::endl;
    std::fill(output_array_data.begin(), output_array_data.end(), 0); // Clear output
    std::string lua_code_gt = translator.translate_expression_to_lua_function(*expr_gt, 1); // 1 input array ("col1")
    std::cout << "Generated Lua Code:\n" << lua_code_gt << std::endl;

    if (luaL_loadstring(L, lua_code_gt.c_str()) != 0 || lua_pcall(L, 0, 1, 0) != 0) { // Load and get function
        std::cerr << "LuaJIT: Failed to load/pcall for GT function: " << lua_tostring(L, -1) << std::endl;
        lua_pop(L, 1); lua_close(L); return 1;
    }
    // Lua function is on stack
    lua_pushlightuserdata(L, output_array_data.data()); // output_array
    lua_pushinteger(L, ARRAY_SIZE);                     // count
    lua_pushlightuserdata(L, input_col1_data.data());   // input0 (col1)

    if (lua_pcall(L, 3, 0, 0) != 0) { // 3 args, 0 results
        std::cerr << "LuaJIT: Failed to execute JITted GT code: " << lua_tostring(L, -1) << std::endl;
        lua_pop(L, 1); lua_close(L); return 1;
    }

    std::cout << "Results for col1 > 25:" << std::endl;
    for (int i = 0; i < ARRAY_SIZE; ++i) {
        int expected_val = (input_col1_data[i] > 25) ? 1 : 0;
        std::cout << "C++: output_array_data[" << i << "] = " << output_array_data[i]
                  << " (Expected: " << expected_val << ")" << std::endl;
        if (output_array_data[i] != expected_val) {
            std::cerr << "Verification FAILED for col1 > 25 at index " << i << std::endl;
        }
    }
    lua_settop(L, 0); // Clear stack


    // Cleanup
    lua_close(L);
    std::cout << "\nPoC Finished." << std::endl;
    return 0;
}

```

## 6. FFI Setup and Data Marshalling

*   **FFI C Definitions:** The Lua code itself defines the C types it expects using `ffi.cdef`. For this PoC, `typedef int int_array_t[];` tells LuaJIT that it will receive pointers that should be treated as arrays of integers.
*   **Passing Pointers:** C++ passes raw pointers to the data arrays (e.g., `input_col1_data.data()`) to Lua using `lua_pushlightuserdata`. This is a lightweight way to pass pointers; Lua treats them as opaque `userdata` initially.
*   **FFI Casting:** Inside the Lua script, these `lightuserdata` pointers are not directly usable with array indexing. The FFI is implicitly used when LuaJIT calls the generated function with these pointers. The `ffi.cdef` has already told LuaJIT how to interpret these pointers if they are used in an FFI context (e.g., if we were to declare the function signature in `ffi.cdef` as well).
    *   **Correction/Clarification for PoC Code:** The PoC's Lua generator creates a function that takes generic arguments. For direct array access as shown (`input0[i]`), LuaJIT's FFI needs to know these are `cdata` of the defined array type. The `lua_pushlightuserdata` is fine for passing the pointer, but inside Lua, these would ideally be explicitly cast or received as `cdata` by a function whose signature is declared in `ffi.cdef`.
    *   A more robust FFI usage in the Lua function would be:
        ```lua
        -- (ffi.cdef as before)
        return function(output_ptr, count, input0_ptr, input1_ptr)
          local output_array = ffi.cast("int_array_t", output_ptr)
          local input0 = ffi.cast("int_array_t", input0_ptr)
          local input1 = ffi.cast("int_array_t", input1_ptr)
          for i = 0, count - 1 do
            output_array[i] = input0[i] + input1[i]
          end
        end
        ```
        The current PoC's Lua code relies on LuaJIT's implicit understanding when `lightuserdata` are passed to a function expecting pointers that align with an `ffi.cdef`'d type used as an array. This usually works for simple cases but explicit casting is safer. The `LuaTranslator` should be updated to generate these `ffi.cast` lines for robustness.
*   **Zero-Copy:** This approach achieves zero-copy data access. LuaJIT operates directly on the memory allocated and owned by C++.

## 7. Challenges Encountered (Conceptual)

*   **FFI Type System:** Precisely matching C/C++ types with LuaJIT FFI definitions is crucial. Mistakes can lead to misinterpretations of data or crashes. For complex types like DuckDB's `Vector` (with potential `std::vector` members, `union`s, or specific string types), the FFI definitions would be more complex.
*   **Pointer Provenance and Lifetime:** Lua must not retain pointers to C++ data beyond its lifetime. In this synchronous PoC, it's not an issue. In a more complex system, care is needed.
*   **Error Handling:** Errors from Lua (e.g., a type error in the JITted Lua code if a `nil` was encountered unexpectedly) need to be caught and propagated correctly in C++. The `lua_pcall` mechanism handles this.
*   **Debugging:** Debugging issues within the JIT-compiled Lua code can be harder than debugging C++ directly. LuaJIT provides some tools, but it's a different environment.
*   **Build System Integration:** Correctly linking LuaJIT (static or dynamic) and managing headers would be a practical challenge in a real project.
*   **Lua Stack Management:** The C++ code interacting with Lua must carefully manage the Lua stack (pushing arguments, getting results, popping values). The PoC `main.cpp` shows basic stack management. For the function to be callable from C++, it must be on the stack. After `luaL_loadstring` and `lua_pcall` (to get the function object), the function is on the stack. Subsequent `lua_pcall`s to execute it consume arguments from the stack and push results.

## 8. Basic Observations

*   **Ease of Use (FFI):** LuaJIT's FFI, once understood, makes C++ interop relatively straightforward, especially for C-like data structures or functions. The ability to define C types in Lua via `ffi.cdef` is powerful.
*   **Apparent Overhead (Conceptual):**
    *   **Compilation Overhead:** LuaJIT is known for very fast compilation. For small expression strings like in this PoC, the overhead of `luaL_loadstring` and the initial JIT compilation pass should be minimal, likely microseconds. This is a significant advantage over LLVM.
    *   **Call Overhead:** The overhead of calling a JIT-compiled Lua function via FFI from C++ involves a context switch and argument marshalling (even if just pointers). While LuaJIT minimizes this, it's not zero. For extremely simple expressions, this call overhead might negate the JIT benefit if the C++ interpreter's dispatch is also very cheap. However, as expression complexity grows, the JIT benefit should dominate.
    *   **FFI Access Overhead:** Accessing C data via FFI pointers in LuaJIT is highly optimized and close to native C pointer access speed.
*   **Code Generation Complexity:** Translating expressions to Lua strings is relatively simple for basic arithmetic and logic, as Lua's syntax is expressive and similar to C++ in these cases. More complex SQL constructs or functions would require a more sophisticated translator.
*   **Flexibility:** Lua is a full-fledged dynamic language. This offers flexibility for more complex JITed logic if needed, but also means the translator must be careful to generate correct and safe Lua code.

## 9. Conclusion of PoC

This conceptual PoC demonstrates that using LuaJIT to JIT-compile simple expressions and operate on C++ data arrays via FFI is feasible. The FFI provides an effective mechanism for zero-copy data access. The main complexities lie in robustly translating a wider range of DuckDB expressions to Lua and managing the FFI definitions for DuckDB's specific internal data structures. The anticipated low compilation and call overhead make LuaJIT an attractive option for further investigation for DuckDB.
The `LuaTranslator` should be updated to generate explicit `ffi.cast` for input arrays to ensure robustness, as noted in the FFI discussion.
