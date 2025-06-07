#pragma once

#include "duckdb/common/common.hpp" // For DUCKDB_API, idx_t, std::string, etc.
#include "duckdb/common/types.hpp"   // For Value, LogicalType, etc. (though Value might be too complex for this PoC)
#include <string>
#include <memory> // For std::unique_ptr
#include <variant> // For Constant value
#include <vector>  // For children in n-ary ops if needed later

namespace duckdb {

class LuaTranslatorContext; // Forward declaration

// Enum for expression types (can be expanded)
// This is distinct from DuckDB's internal ExpressionType enum for now
enum class LuaJITExpressionType {
    CONSTANT,
    COLUMN_REFERENCE,
    BINARY_OPERATOR,
    UNARY_OPERATOR, // Added for NOT
    CASE_EXPRESSION  // Added for CASE
    // Future: FUNCTION_CALL, etc.
};

// Operator types for BinaryOperatorExpression
enum class LuaJITBinaryOperatorType {
    // Arithmetic
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    // Comparison
    EQUALS,
    NOT_EQUALS, // Added
    GREATER_THAN,
    LESS_THAN,    // Added
    GREATER_THAN_OR_EQUALS, // Added
    LESS_THAN_OR_EQUALS,    // Added
    // Logical (binary)
    AND,          // Added
    OR,           // Added
    // String
    CONCAT,       // Added
    LIKE          // Added
};

// Operator types for UnaryOperatorExpression
enum class LuaJITUnaryOperatorType {
    NOT,          // Added
    IS_NULL,      // Future
    IS_NOT_NULL   // Future
};

// --- Base Expression Class ---
class BaseExpression {
public:
    DUCKDB_API BaseExpression(LuaJITExpressionType type) : type(type) {}
    DUCKDB_API virtual ~BaseExpression() = default;

    // Main method for Lua translation - implemented by derived classes
    // This was originally planned, but the current task asks for LuaTranslator
    // to have static Translate methods. So, this virtual method might not be used
    // directly by LuaTranslator as initially designed in this comment block,
    // but it's good practice for an expression tree.
    // For now, LuaTranslator will use type-based dispatch.
    // virtual std::string ToLuaString(LuaTranslatorContext& ctx) const = 0;

    LuaJITExpressionType type;
};

// --- Constant Expression ---
// For this PoC, we'll handle int and double. Strings are more complex due to quoting.
// Using std::variant for the value.
using ConstantValue = std::variant<int, double, std::string>; // Add more types as needed

class ConstantExpression : public BaseExpression {
public:
    DUCKDB_API ConstantExpression(ConstantValue val)
        : BaseExpression(LuaJITExpressionType::CONSTANT), value(std::move(val)) {}

    ConstantValue value;
};

// --- Column Reference Expression ---
class ColumnReferenceExpression : public BaseExpression {
public:
    DUCKDB_API ColumnReferenceExpression(idx_t col_idx)
        : BaseExpression(LuaJITExpressionType::COLUMN_REFERENCE), column_index(col_idx) {}

    idx_t column_index; // Index into the input FFIVector array
};

// --- Binary Operator Expression ---
class BinaryOperatorExpression : public BaseExpression {
public:
    DUCKDB_API BinaryOperatorExpression(LuaJITBinaryOperatorType op_type,
                                     std::unique_ptr<BaseExpression> left,
                                     std::unique_ptr<BaseExpression> right)
        : BaseExpression(LuaJITExpressionType::BINARY_OPERATOR),
          operator_type(op_type), left_child(std::move(left)), right_child(std::move(right)) {}

    LuaJITBinaryOperatorType operator_type;
    std::unique_ptr<BaseExpression> left_child;
    std::unique_ptr<BaseExpression> right_child;
};

// --- Unary Operator Expression ---
class UnaryOperatorExpression : public BaseExpression {
public:
    DUCKDB_API UnaryOperatorExpression(LuaJITUnaryOperatorType op_type,
                                    std::unique_ptr<BaseExpression> child)
        : BaseExpression(LuaJITExpressionType::UNARY_OPERATOR),
          operator_type(op_type), child_expression(std::move(child)) {}

    LuaJITUnaryOperatorType operator_type;
    std::unique_ptr<BaseExpression> child_expression;
};

// --- CASE Expression ---
// Represents: CASE WHEN condition THEN result_true ELSE result_false END
// For simplicity, this PoC only supports one WHEN clause and an ELSE.
// A full CASE expression is more complex (multiple WHENs, optional ELSE).
struct CaseBranch {
    std::unique_ptr<BaseExpression> condition;
    std::unique_ptr<BaseExpression> result_if_true;
};

class CaseExpression : public BaseExpression {
public:
    DUCKDB_API CaseExpression(std::vector<CaseBranch> branches, // For now, expect only one branch
                           std::unique_ptr<BaseExpression> else_result)
        : BaseExpression(LuaJITExpressionType::CASE_EXPRESSION),
          case_branches(std::move(branches)), result_if_else(std::move(else_result)) {}

    std::vector<CaseBranch> case_branches;
    std::unique_ptr<BaseExpression> result_if_else;
};


// Helper functions to create expressions (optional, but can be convenient)
inline std::unique_ptr<ConstantExpression> MakeLuaConstant(ConstantValue val) {
    return std::make_unique<ConstantExpression>(std::move(val));
}

inline std::unique_ptr<ColumnReferenceExpression> MakeLuaColumnRef(idx_t col_idx) {
    return std::make_unique<ColumnReferenceExpression>(col_idx);
}

inline std::unique_ptr<BinaryOperatorExpression> MakeLuaBinaryOp(
    LuaJITBinaryOperatorType op_type,
    std::unique_ptr<BaseExpression> left,
    std::unique_ptr<BaseExpression> right) {
    return std::make_unique<BinaryOperatorExpression>(op_type, std::move(left), std::move(right));
}

inline std::unique_ptr<UnaryOperatorExpression> MakeLuaUnaryOp(
    LuaJITUnaryOperatorType op_type,
    std::unique_ptr<BaseExpression> child) {
    return std::make_unique<UnaryOperatorExpression>(op_type, std::move(child));
}

inline std::unique_ptr<CaseExpression> MakeLuaCaseExpression(
    std::vector<CaseBranch> branches,
    std::unique_ptr<BaseExpression> else_result) {
    return std::make_unique<CaseExpression>(std::move(branches), std::move(else_result));
}


} // namespace duckdb
