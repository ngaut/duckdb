//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metal_backend.mm
//
//===----------------------------------------------------------------------===//

#include "metal_backend.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

#include <chrono>
#include <cmath>
#include <limits>

namespace duckdb {

namespace {

static constexpr idx_t METAL_MAX_BATCH_CHUNKS = 64;

static int64_t MetalElapsedMicros(std::chrono::steady_clock::time_point start) {
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static string MetalNSErrorToString(NSError *error) {
    if (!error) {
        return "unknown Metal error";
    }
    auto description = [error localizedDescription];
    if (!description) {
        return "unknown Metal error";
    }
    return string([description UTF8String]);
}

static bool MetalDeviceAvailable() {
    id<MTLDevice> device = MTLCreateSystemDefaultDevice();
    if (!device) {
        return false;
    }
    [device release];
    return true;
}

enum class MetalScalarKind : uint8_t { INT64, FLOAT, DECIMAL64 };

struct MetalScalarType {
    MetalScalarKind kind = MetalScalarKind::INT64;
    uint8_t decimal_width = 0;
    uint8_t decimal_scale = 0;
};

static bool MetalScalarTypesEquivalent(const MetalScalarType &left, const MetalScalarType &right) {
    if (left.kind != right.kind) {
        return false;
    }
    if (left.kind != MetalScalarKind::DECIMAL64) {
        return true;
    }
    return left.decimal_width == right.decimal_width && left.decimal_scale == right.decimal_scale;
}

static bool MetalDecimalScalesMatch(const MetalScalarType &left, const MetalScalarType &right) {
    return left.kind == MetalScalarKind::DECIMAL64 && right.kind == MetalScalarKind::DECIMAL64 &&
           left.decimal_scale == right.decimal_scale;
}

static string MetalSupportedTypesError() {
    return "Metal projection supports BIGINT, FLOAT, and INT64-backed DECIMAL columns";
}

static bool TryGetMetalScalarType(const LogicalType &type, MetalScalarType &result, string &error) {
    switch (type.id()) {
    case LogicalTypeId::BIGINT:
        result.kind = MetalScalarKind::INT64;
        result.decimal_width = 0;
        result.decimal_scale = 0;
        return true;
    case LogicalTypeId::FLOAT:
        result.kind = MetalScalarKind::FLOAT;
        result.decimal_width = 0;
        result.decimal_scale = 0;
        return true;
    case LogicalTypeId::DECIMAL:
        if (type.InternalType() != PhysicalType::INT64) {
            error = "Metal projection only supports DECIMAL values backed by INT64 storage";
            return false;
        }
        result.kind = MetalScalarKind::DECIMAL64;
        result.decimal_width = DecimalType::GetWidth(type);
        result.decimal_scale = DecimalType::GetScale(type);
        if (result.decimal_width >= NumericHelper::CACHED_POWERS_OF_TEN) {
            error = "Metal projection DECIMAL width is outside INT64 decimal bounds";
            return false;
        }
        return true;
    default:
        error = MetalSupportedTypesError();
        return false;
    }
}

static string MetalScalarTypeName(const MetalScalarType &type) {
    switch (type.kind) {
    case MetalScalarKind::INT64:
    case MetalScalarKind::DECIMAL64:
        return "long";
    case MetalScalarKind::FLOAT:
        return "float";
    default:
        throw InternalException("Unknown Metal scalar kind");
    }
}

static idx_t MetalScalarTypeSize(const MetalScalarType &type) {
    switch (type.kind) {
    case MetalScalarKind::INT64:
    case MetalScalarKind::DECIMAL64:
        return sizeof(int64_t);
    case MetalScalarKind::FLOAT:
        return sizeof(float);
    default:
        throw InternalException("Unknown Metal scalar kind");
    }
}

static int64_t MetalDecimalMaxRawValue(const MetalScalarType &type) {
    D_ASSERT(type.kind == MetalScalarKind::DECIMAL64);
    return NumericHelper::POWERS_OF_TEN[type.decimal_width] - 1;
}

static int64_t MetalDecimalMinRawValue(const MetalScalarType &type) {
    return -MetalDecimalMaxRawValue(type);
}

static string MetalFormatFloat(float value) {
    if (!std::isfinite(value)) {
        throw InvalidInputException("Metal projection floating constants must be finite");
    }
    auto result = StringUtil::Format("%.9g", value);
    if (result.find_first_of(".eE") == string::npos) {
        result += ".0";
    }
    return result + "f";
}

static bool TryCastMetalConstant(const Value &value, const LogicalType &target_type, Value &result, string &error) {
    if (value.IsNull()) {
        error = "Metal projection constants cannot be NULL";
        return false;
    }
    string cast_error;
    if (!value.DefaultTryCastAs(target_type, result, &cast_error) || result.IsNull()) {
        error = cast_error.empty() ? "Metal projection constant cannot be cast to the target type" : cast_error;
        return false;
    }
    return true;
}

static bool TryGetMetalInt64Constant(const Value &value, int64_t &result, string &error) {
    if (value.IsNull()) {
        error = "Metal projection constants cannot be NULL";
        return false;
    }
    switch (value.type().InternalType()) {
    case PhysicalType::INT8:
        result = value.GetValueUnsafe<int8_t>();
        return true;
    case PhysicalType::INT16:
        result = value.GetValueUnsafe<int16_t>();
        return true;
    case PhysicalType::INT32:
        result = value.GetValueUnsafe<int32_t>();
        return true;
    case PhysicalType::INT64:
        result = value.GetValueUnsafe<int64_t>();
        return true;
    case PhysicalType::UINT8:
        result = value.GetValueUnsafe<uint8_t>();
        return true;
    case PhysicalType::UINT16:
        result = value.GetValueUnsafe<uint16_t>();
        return true;
    case PhysicalType::UINT32:
        result = value.GetValueUnsafe<uint32_t>();
        return true;
    default:
        error = "Metal projection only supports integral constants that fit in BIGINT";
        return false;
    }
}

static bool TryGetMetalConstantSource(const Value &value, const LogicalType &target_type,
                                      const MetalScalarType &metal_type, string &result, string &error) {
    Value cast_value;
    if (!TryCastMetalConstant(value, target_type, cast_value, error)) {
        return false;
    }
    switch (metal_type.kind) {
    case MetalScalarKind::INT64: {
        int64_t constant = 0;
        if (!TryGetMetalInt64Constant(cast_value, constant, error)) {
            return false;
        }
        result = "((long)" + std::to_string(constant) + ")";
        return true;
    }
    case MetalScalarKind::DECIMAL64: {
        if (cast_value.type().InternalType() != PhysicalType::INT64) {
            error = "Metal projection DECIMAL constants must use INT64 storage";
            return false;
        }
        auto constant = cast_value.GetValueUnsafe<int64_t>();
        result = "((long)" + std::to_string(constant) + ")";
        return true;
    }
    case MetalScalarKind::FLOAT: {
        if (cast_value.type().InternalType() != PhysicalType::FLOAT) {
            error = "Metal projection FLOAT constants must cast to FLOAT";
            return false;
        }
        result = "((float)" + MetalFormatFloat(cast_value.GetValueUnsafe<float>()) + ")";
        return true;
    }
    default:
        error = MetalSupportedTypesError();
        return false;
    }
}

struct MetalExpressionBuilder {
    explicit MetalExpressionBuilder(idx_t expression_index_p) : expression_index(expression_index_p) {
    }

    string AddTemporary(const MetalScalarType &type, const string &source) {
        auto name = "duckdb_e" + std::to_string(expression_index) + "_tmp" + std::to_string(temp_index++);
        statements += "\t" + MetalScalarTypeName(type) + " " + name + " = " + source + ";\n";
        return name;
    }

    idx_t expression_index;
    idx_t temp_index = 0;
    string statements;
};

static bool BuildMetalExpressionSource(const ExecutionExpressionIR &expression, const vector<LogicalType> &input_types,
                                       const vector<MetalScalarType> &input_metal_types,
                                       MetalExpressionBuilder &builder, string &result, MetalScalarType &result_type,
                                       string &error) {
    if (!TryGetMetalScalarType(expression.return_type, result_type, error)) {
        error = "Metal projection expression result is unsupported: " + error;
        return false;
    }
    switch (expression.kind) {
    case ExecutionExpressionIRKind::REFERENCE:
        if (expression.ref_index >= input_types.size()) {
            error = "Metal projection reference index is outside the input chunk";
            return false;
        }
        if (!MetalScalarTypesEquivalent(input_metal_types[expression.ref_index], result_type)) {
            error = "Metal projection reference type does not match its input column";
            return false;
        }
        result = "in" + std::to_string(expression.ref_index) + "[row]";
        return true;
    case ExecutionExpressionIRKind::CONSTANT: {
        return TryGetMetalConstantSource(expression.constant, expression.return_type, result_type, result, error);
    }
    case ExecutionExpressionIRKind::BINARY: {
        if (!expression.left || !expression.right) {
            error = "Metal projection binary expression is missing a child";
            return false;
        }
        string left;
        string right;
        MetalScalarType left_type;
        MetalScalarType right_type;
        if (!BuildMetalExpressionSource(*expression.left, input_types, input_metal_types, builder, left, left_type,
                                        error) ||
            !BuildMetalExpressionSource(*expression.right, input_types, input_metal_types, builder, right, right_type,
                                        error)) {
            return false;
        }
        const char *op = nullptr;
        switch (expression.binary_op) {
        case ExecutionExpressionBinaryOp::ADD:
            op = "+";
            break;
        case ExecutionExpressionBinaryOp::SUBTRACT:
            op = "-";
            break;
        case ExecutionExpressionBinaryOp::MULTIPLY:
            op = "*";
            break;
        case ExecutionExpressionBinaryOp::DIVIDE:
            if (result_type.kind == MetalScalarKind::FLOAT) {
                op = "/";
                break;
            }
            error = "Metal projection only supports FLOAT division expressions";
            return false;
        default:
            error = "Metal projection only supports add/subtract/multiply expressions";
            return false;
        }
        if (result_type.kind == MetalScalarKind::DECIMAL64) {
            if (expression.binary_op != ExecutionExpressionBinaryOp::ADD &&
                expression.binary_op != ExecutionExpressionBinaryOp::SUBTRACT) {
                error = "Metal projection only supports DECIMAL add/subtract expressions";
                return false;
            }
            if (!MetalDecimalScalesMatch(left_type, result_type) || !MetalDecimalScalesMatch(right_type, result_type)) {
                error = "Metal projection only supports same-scale DECIMAL add/subtract expressions";
                return false;
            }
            const auto min_value = MetalDecimalMinRawValue(result_type);
            const auto max_value = MetalDecimalMaxRawValue(result_type);
            auto helper =
                expression.binary_op == ExecutionExpressionBinaryOp::ADD ? "duckdb_decimal_add" : "duckdb_decimal_sub";
            auto source = string(helper) + "((" + left + "), (" + right + "), ((long)" + std::to_string(min_value) +
                          "), ((long)" + std::to_string(max_value) + "), duckdb_error)";
            result = builder.AddTemporary(result_type, source);
            return true;
        }
        if (result_type.kind == MetalScalarKind::INT64) {
            if (left_type.kind != MetalScalarKind::INT64 || right_type.kind != MetalScalarKind::INT64) {
                error = "Metal projection BIGINT arithmetic requires BIGINT operands";
                return false;
            }
        }
        if (result_type.kind == MetalScalarKind::FLOAT) {
            if (left_type.kind != MetalScalarKind::FLOAT || right_type.kind != MetalScalarKind::FLOAT) {
                error = "Metal projection FLOAT arithmetic requires FLOAT operands";
                return false;
            }
        }
        const auto metal_type_name = MetalScalarTypeName(result_type);
        auto source = "((" + metal_type_name + ")(" + left + ") " + op + " (" + metal_type_name + ")(" + right + "))";
        result = builder.AddTemporary(result_type, source);
        return true;
    }
    case ExecutionExpressionIRKind::CAST: {
        if (!expression.left || expression.try_cast) {
            error = "Metal projection only supports non-try casts";
            return false;
        }
        MetalScalarType child_type;
        if (!BuildMetalExpressionSource(*expression.left, input_types, input_metal_types, builder, result, child_type,
                                        error)) {
            return false;
        }
        if (child_type.kind == MetalScalarKind::DECIMAL64 || result_type.kind == MetalScalarKind::DECIMAL64) {
            if (!MetalScalarTypesEquivalent(child_type, result_type)) {
                error = "Metal projection only supports type-preserving DECIMAL casts";
                return false;
            }
        } else if (result_type.kind == MetalScalarKind::INT64 && child_type.kind == MetalScalarKind::FLOAT) {
            error = "Metal projection does not support FLOAT to BIGINT casts";
            return false;
        }
        result = builder.AddTemporary(result_type, "((" + MetalScalarTypeName(result_type) + ")(" + result + "))");
        return true;
    }
    default:
        error = "Metal projection expression kind is unsupported";
        return false;
    }
}

struct MetalProjectionStage {
    vector<string> expressions;
    vector<LogicalType> input_types;
    vector<LogicalType> output_types;
    vector<MetalScalarType> input_metal_types;
    vector<MetalScalarType> output_metal_types;
};

struct MetalRegionBackendPlan : public ExecutionRegionBackendPlan {
    MetalProjectionStage projection;
    ExecutionRegionSinkInfo sink_info;
    string ir;
};

static string BuildMetalProjectionSource(const MetalProjectionStage &projection) {
    string source;
    source += "#include <metal_stdlib>\n";
    source += "using namespace metal;\n";
    source += "inline long duckdb_decimal_add(long left, long right, long min_value, long max_value, ";
    source += "device atomic_uint *duckdb_error) {\n";
    source += "\tif ((right > 0 && left > max_value - right) || (right < 0 && left < min_value - right)) {\n";
    source += "\t\tatomic_store_explicit(duckdb_error, 1u, memory_order_relaxed);\n";
    source += "\t\treturn 0;\n";
    source += "\t}\n";
    source += "\treturn left + right;\n";
    source += "}\n";
    source += "inline long duckdb_decimal_sub(long left, long right, long min_value, long max_value, ";
    source += "device atomic_uint *duckdb_error) {\n";
    source += "\tif ((right < 0 && left > max_value + right) || (right > 0 && left < min_value + right)) {\n";
    source += "\t\tatomic_store_explicit(duckdb_error, 1u, memory_order_relaxed);\n";
    source += "\t\treturn 0;\n";
    source += "\t}\n";
    source += "\treturn left - right;\n";
    source += "}\n";
    source += "kernel void duckdb_jit_metal_projection(";
    for (idx_t col_idx = 0; col_idx < projection.input_types.size(); col_idx++) {
        if (col_idx > 0) {
            source += ", ";
        }
        source += "device const " + MetalScalarTypeName(projection.input_metal_types[col_idx]) + " *in" +
                  std::to_string(col_idx) + " [[buffer(" + std::to_string(col_idx) + ")]]";
    }
    for (idx_t col_idx = 0; col_idx < projection.output_types.size(); col_idx++) {
        if (!projection.input_types.empty() || col_idx > 0) {
            source += ", ";
        }
        auto buffer_idx = projection.input_types.size() + col_idx;
        source += "device " + MetalScalarTypeName(projection.output_metal_types[col_idx]) + " *out" +
                  std::to_string(col_idx) + " [[buffer(" + std::to_string(buffer_idx) + ")]]";
    }
    auto count_buffer_idx = projection.input_types.size() + projection.output_types.size();
    source += ", constant uint &count [[buffer(" + std::to_string(count_buffer_idx) + ")]], ";
    source += "device atomic_uint *duckdb_error [[buffer(" + std::to_string(count_buffer_idx + 1) + ")]], ";
    source += "uint row [[thread_position_in_grid]]) {\n";
    source += "\tif (row >= count) {\n";
    source += "\t\treturn;\n";
    source += "\t}\n";
    for (idx_t col_idx = 0; col_idx < projection.expressions.size(); col_idx++) {
        source += projection.expressions[col_idx];
    }
    source += "}\n";
    return source;
}

static bool IsMetalAppendSink(const ExecutionRegionSinkInfo &sink) {
    return (sink.kind == ExecutionRegionSinkKind::MATERIALIZATION ||
            sink.kind == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK) &&
           sink.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
}

static bool BuildMetalProjectionStage(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                      MetalProjectionStage &projection, string &error) {
    if (node.kind != ExecutionRegionNodeKind::PROJECTION) {
        error = "Metal projection stage requires a projection node";
        return false;
    }
    if (node.projections.empty()) {
        error = "Metal projection stage has no projection expressions";
        return false;
    }
    projection.input_types = input_types;
    projection.output_types = node.output_types;
    for (auto &type : projection.input_types) {
        MetalScalarType metal_type;
        if (!TryGetMetalScalarType(type, metal_type, error)) {
            return false;
        }
        projection.input_metal_types.push_back(metal_type);
    }
    for (auto &type : projection.output_types) {
        MetalScalarType metal_type;
        if (!TryGetMetalScalarType(type, metal_type, error)) {
            return false;
        }
        projection.output_metal_types.push_back(metal_type);
    }
    for (auto &projection_expression : node.projections) {
        if (!projection_expression || !projection_expression->root) {
            error = "Metal projection expression is missing lowered expression IR";
            return false;
        }
        string expression_source;
        MetalScalarType expression_type;
        MetalExpressionBuilder builder(projection.expressions.size());
        if (!BuildMetalExpressionSource(*projection_expression->root, projection.input_types,
                                        projection.input_metal_types, builder, expression_source, expression_type,
                                        error)) {
            return false;
        }
        if (!MetalScalarTypesEquivalent(expression_type,
                                        projection.output_metal_types[projection.expressions.size()])) {
            error = "Metal projection expression result type does not match projection output type";
            return false;
        }
        builder.statements +=
            "\tout" + std::to_string(projection.expressions.size()) + "[row] = " + expression_source + ";\n";
        projection.expressions.push_back(std::move(builder.statements));
    }
    return true;
}

static string MetalRegionIR(const MetalRegionBackendPlan &plan) {
    string result = "metal_region<projection_expressions=" + std::to_string(plan.projection.expressions.size());
    result += ",input_columns=" + std::to_string(plan.projection.input_types.size());
    result += ",output_columns=" + std::to_string(plan.projection.output_types.size());
    result += ",sink=" + string(ExecutionRegionSinkKindToString(plan.sink_info.kind));
    result += ">";
    return result;
}

static shared_ptr<MetalRegionBackendPlan> BuildMetalRegionPlan(const ExecutionRegionIR &region_ir,
                                                               const ExecutionRegionCandidate &candidate,
                                                               ExecutionRegionLoweringPlan &lowering_plan) {
    lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
    if (candidate.stage_plan.HasStages()) {
        lowering_plan.SetOperatorStageIR(candidate.stage_plan.ir);
    }
    if (!ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
        lowering_plan.AddFusionBlocker("metal-backend-blocker:requires-full-pipeline-abi");
        return nullptr;
    }
    if (candidate.EndNode() > region_ir.nodes.size()) {
        lowering_plan.AddFusionBlocker("metal-backend-blocker:candidate-outside-region-ir");
        return nullptr;
    }

    auto plan = make_shared_ptr<MetalRegionBackendPlan>();
    vector<LogicalType> current_types = candidate.input_types;
    bool saw_source_contract = false;
    bool saw_projection = false;
    bool saw_sink = false;

    for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
        auto &node = region_ir.nodes[node_idx];
        switch (node.kind) {
        case ExecutionRegionNodeKind::SOURCE:
            if (!node.source || node.source->source_contract.status != ExecutionRegionSourceContractStatus::READY ||
                node.source->execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
                auto reason = "Metal backend requires a ready source contract";
                lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                      ExecutionRegionLoweringKind::BOUNDARY, reason);
                lowering_plan.AddFusionBlocker("metal-backend-blocker:source-contract-missing");
                return nullptr;
            }
            current_types = node.output_types;
            saw_source_contract = true;
            lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                  ExecutionRegionLoweringKind::NATIVE, "Metal source contract");
            break;
        case ExecutionRegionNodeKind::PROJECTION: {
            if (saw_projection) {
                auto reason = "Metal backend currently supports one projection stage";
                lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                      ExecutionRegionLoweringKind::BOUNDARY, reason);
                lowering_plan.AddFusionBlocker("metal-backend-blocker:multiple-projection-stages");
                return nullptr;
            }
            string error;
            if (!BuildMetalProjectionStage(node, current_types, plan->projection, error)) {
                lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                      ExecutionRegionLoweringKind::BOUNDARY, error);
                lowering_plan.AddFusionBlocker("metal-backend-blocker:" + error);
                return nullptr;
            }
            current_types = plan->projection.output_types;
            saw_projection = true;
            lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                  ExecutionRegionLoweringKind::NATIVE, "Metal scalar projection");
            break;
        }
        case ExecutionRegionNodeKind::SINK:
            if (!node.sink || !IsMetalAppendSink(*node.sink)) {
                auto reason = "Metal backend currently requires a native append/materialization sink";
                lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                      ExecutionRegionLoweringKind::BOUNDARY, reason);
                lowering_plan.AddFusionBlocker("metal-backend-blocker:append-sink-contract-missing");
                return nullptr;
            }
            plan->sink_info = *node.sink;
            saw_sink = true;
            lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                  ExecutionRegionLoweringKind::NATIVE, "Metal append sink");
            break;
        default: {
            auto reason = "Metal backend currently supports source-projection-append pipelines";
            lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind,
                                  ExecutionRegionLoweringKind::BOUNDARY, reason);
            lowering_plan.AddFusionBlocker("metal-backend-blocker:unsupported-node-kind");
            return nullptr;
        }
        }
    }

    if (!saw_source_contract) {
        lowering_plan.AddFusionBlocker("metal-backend-blocker:source-contract-missing");
        return nullptr;
    }
    if (!saw_projection) {
        lowering_plan.AddFusionBlocker("metal-backend-blocker:projection-missing");
        return nullptr;
    }
    if (!saw_sink) {
        lowering_plan.AddFusionBlocker("metal-backend-blocker:append-sink-missing");
        return nullptr;
    }

    plan->ir = MetalRegionIR(*plan);
    lowering_plan.SetFullyFused(true);
    lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::GPU);
    lowering_plan.SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
    lowering_plan.SetScanFilterMode(ExecutionRegionScanFilterMode::NONE);
    lowering_plan.backend_plan = plan;
    return plan;
}

class ScopedMetalObject {
  public:
    explicit ScopedMetalObject(id object_p = nil) : object(object_p) {
    }

    ~ScopedMetalObject() {
        if (object) {
            [object release];
        }
    }

    ScopedMetalObject(const ScopedMetalObject &) = delete;
    ScopedMetalObject &operator=(const ScopedMetalObject &) = delete;

    ScopedMetalObject(ScopedMetalObject &&other) noexcept : object(other.object) {
        other.object = nil;
    }

    ScopedMetalObject &operator=(ScopedMetalObject &&other) noexcept {
        if (this == &other) {
            return *this;
        }
        Reset(other.object);
        other.object = nil;
        return *this;
    }

    id Get() const {
        return object;
    }

    void Reset(id object_p = nil) {
        if (object) {
            [object release];
        }
        object = object_p;
    }

  private:
    id object;
};

class MetalProjectionKernel : public ExecutionRegionKernel {
  public:
    MetalProjectionKernel(string backend_name_p, id<MTLDevice> device_p, id<MTLCommandQueue> queue_p,
                          id<MTLComputePipelineState> pipeline_p, MetalProjectionStage projection_p,
                          ExecutionRegionSinkInfo sink_info_p, string metal_source_p)
        : backend_name(std::move(backend_name_p)), device(device_p), queue(queue_p), pipeline(pipeline_p),
          projection(std::move(projection_p)), sink_info(std::move(sink_info_p)),
          metal_source(std::move(metal_source_p)) {
    }

    ~MetalProjectionKernel() override {
        if (pipeline) {
            [pipeline release];
        }
        if (queue) {
            [queue release];
        }
        if (device) {
            [device release];
        }
    }

    const string &BackendName() const override {
        return backend_name;
    }

    idx_t CodeSize() const override {
        return metal_source.size();
    }

    bool HasExecutableBody() const override {
        return true;
    }

    bool CanExecuteFullPipeline() const override {
        return true;
    }

    bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) override {
        MetalProjectionScratch scratch(runtime.GetAllocator(), projection.output_types);
        idx_t processed_chunks = 0;
        while (true) {
            if (processed_chunks >= runtime.MaxChunks()) {
                result = ExecutionRegionResult::NOT_FINISHED;
                return true;
            }

            const auto chunk_budget = runtime.MaxChunks() - processed_chunks;
            const auto batch_chunk_budget =
                ShouldBatchSourceChunks() ? MinValue<idx_t>(chunk_budget, METAL_MAX_BATCH_CHUNKS)
                                          : MinValue<idx_t>(chunk_budget, 1);
            bool source_finished = false;
            auto fetch_result = FetchProjectionBatch(runtime, scratch, batch_chunk_budget, source_finished);
            processed_chunks += scratch.batch_chunks;
            if (fetch_result == MetalProjectionBatchFetchResult::BLOCKED) {
                result = ExecutionRegionResult::INTERRUPTED;
                return true;
            }
            if (scratch.batch_rows > 0) {
                ExecuteMetalProjectionBatch(runtime, scratch);
                auto sink_result = FlushProjectionBatch(runtime, scratch);
                if (sink_result == SinkResultType::BLOCKED) {
                    result = ExecutionRegionResult::INTERRUPTED;
                    return true;
                }
                if (sink_result == SinkResultType::FINISHED) {
                    result = ExecutionRegionResult::FINISHED;
                    return true;
                }
            }
            if (source_finished) {
                result = ExecutionRegionResult::FINISHED;
                return true;
            }
        }
    }

  private:
    enum class MetalProjectionBatchFetchResult : uint8_t { READY, BLOCKED };

    struct MetalProjectionScratch {
        MetalProjectionScratch(Allocator &allocator, const vector<LogicalType> &output_types) {
            output.Initialize(allocator, output_types);
        }

        void ResetBatch() {
            batch_rows = 0;
            batch_chunks = 0;
            chunk_sizes.clear();
        }

        DataChunk output;
        ExecutionSinkBinding sink_binding;
        vector<ScopedMetalObject> input_buffers;
        vector<ScopedMetalObject> output_buffers;
        ScopedMetalObject count_buffer;
        ScopedMetalObject error_buffer;
        vector<idx_t> chunk_sizes;
        idx_t buffer_capacity = 0;
        idx_t batch_rows = 0;
        idx_t batch_chunks = 0;
        bool sink_bound = false;
    };

    bool ShouldBatchSourceChunks() const {
        return sink_info.kind == ExecutionRegionSinkKind::MATERIALIZATION;
    }

    void ValidateInputChunk(DataChunk &input) const {
        if (input.ColumnCount() != projection.input_types.size()) {
            throw InternalException("Metal projection input column count mismatch");
        }
        input.Flatten();
        for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
            MetalScalarType runtime_type;
            string error;
            if (!TryGetMetalScalarType(input.data[col_idx].GetType(), runtime_type, error) ||
                !MetalScalarTypesEquivalent(runtime_type, projection.input_metal_types[col_idx])) {
                throw InternalException("Metal projection runtime input column type mismatch: %s", error);
            }
            if (!FlatVector::Validity(input.data[col_idx]).CheckAllValid(input.size())) {
                throw InvalidInputException("Metal JIT backend does not yet support NULL input values");
            }
        }
    }

    void CopyInputColumnToMetalBuffer(MetalProjectionScratch &scratch, DataChunk &input, idx_t col_idx,
                                      idx_t count) const {
        auto target_buffer = (id<MTLBuffer>)scratch.input_buffers[col_idx].Get();
        auto &type = projection.input_metal_types[col_idx];
        switch (type.kind) {
        case MetalScalarKind::INT64:
        case MetalScalarKind::DECIMAL64: {
            auto source = FlatVector::GetData<int64_t>(input.data[col_idx]);
            auto target = reinterpret_cast<int64_t *>([target_buffer contents]);
            memcpy(target + scratch.batch_rows, source, count * sizeof(int64_t));
            return;
        }
        case MetalScalarKind::FLOAT: {
            auto source = FlatVector::GetData<float>(input.data[col_idx]);
            auto target = reinterpret_cast<float *>([target_buffer contents]);
            memcpy(target + scratch.batch_rows, source, count * sizeof(float));
            return;
        }
        default:
            throw InternalException("Unknown Metal scalar kind");
        }
    }

    void CopyMetalBufferToOutputColumn(MetalProjectionScratch &scratch, idx_t col_idx, idx_t row_offset,
                                       idx_t count) const {
        auto source_buffer = (id<MTLBuffer>)scratch.output_buffers[col_idx].Get();
        auto &type = projection.output_metal_types[col_idx];
        switch (type.kind) {
        case MetalScalarKind::INT64:
        case MetalScalarKind::DECIMAL64: {
            auto target = FlatVector::GetDataMutable<int64_t>(scratch.output.data[col_idx]);
            auto source = reinterpret_cast<const int64_t *>([source_buffer contents]);
            memcpy(target, source + row_offset, count * sizeof(int64_t));
            break;
        }
        case MetalScalarKind::FLOAT: {
            auto target = FlatVector::GetDataMutable<float>(scratch.output.data[col_idx]);
            auto source = reinterpret_cast<const float *>([source_buffer contents]);
            memcpy(target, source + row_offset, count * sizeof(float));
            break;
        }
        default:
            throw InternalException("Unknown Metal scalar kind");
        }
        FlatVector::ValidityMutable(scratch.output.data[col_idx]).SetAllValid(count);
    }

    void EnsureMetalBuffers(MetalProjectionScratch &scratch, idx_t capacity) const {
        if (capacity == 0) {
            capacity = STANDARD_VECTOR_SIZE;
        }
        if (capacity > NumericLimits<uint32_t>::Maximum()) {
            throw InternalException("Metal projection batch is larger than uint32_t row indexing supports");
        }
        if (scratch.buffer_capacity >= capacity && scratch.input_buffers.size() == projection.input_types.size() &&
            scratch.output_buffers.size() == projection.output_types.size() && scratch.count_buffer.Get() &&
            scratch.error_buffer.Get()) {
            return;
        }

        @autoreleasepool {
            scratch.input_buffers.clear();
            scratch.output_buffers.clear();
            scratch.input_buffers.reserve(projection.input_types.size());
            scratch.output_buffers.reserve(projection.output_types.size());
            for (idx_t col_idx = 0; col_idx < projection.input_types.size(); col_idx++) {
                const auto byte_size = capacity * MetalScalarTypeSize(projection.input_metal_types[col_idx]);
                id<MTLBuffer> buffer = [device newBufferWithLength:byte_size options:MTLResourceStorageModeShared];
                if (!buffer) {
                    throw OutOfMemoryException("Metal input buffer allocation failed");
                }
                scratch.input_buffers.emplace_back(buffer);
            }
            for (idx_t col_idx = 0; col_idx < projection.output_types.size(); col_idx++) {
                const auto byte_size = capacity * MetalScalarTypeSize(projection.output_metal_types[col_idx]);
                id<MTLBuffer> buffer = [device newBufferWithLength:byte_size options:MTLResourceStorageModeShared];
                if (!buffer) {
                    throw OutOfMemoryException("Metal output buffer allocation failed");
                }
                scratch.output_buffers.emplace_back(buffer);
            }
            scratch.count_buffer.Reset([device newBufferWithLength:sizeof(uint32_t)
                                                           options:MTLResourceStorageModeShared]);
            if (!scratch.count_buffer.Get()) {
                throw OutOfMemoryException("Metal count buffer allocation failed");
            }
            scratch.error_buffer.Reset([device newBufferWithLength:sizeof(uint32_t)
                                                           options:MTLResourceStorageModeShared]);
            if (!scratch.error_buffer.Get()) {
                throw OutOfMemoryException("Metal error buffer allocation failed");
            }
            scratch.buffer_capacity = capacity;
        }
    }

    void AppendChunkToProjectionBatch(MetalProjectionScratch &scratch, DataChunk &input) const {
        ValidateInputChunk(input);
        auto count = input.size();
        if (scratch.batch_rows + count > scratch.buffer_capacity) {
            throw InternalException("Metal projection batch exceeded its allocated row capacity");
        }
        for (idx_t col_idx = 0; col_idx < projection.input_types.size(); col_idx++) {
            CopyInputColumnToMetalBuffer(scratch, input, col_idx, count);
        }
        scratch.chunk_sizes.push_back(count);
        scratch.batch_rows += count;
        scratch.batch_chunks++;
    }

    MetalProjectionBatchFetchResult FetchProjectionBatch(ExecutionRegionRuntime &runtime,
                                                         MetalProjectionScratch &scratch, idx_t chunk_budget,
                                                         bool &source_finished) const {
        scratch.ResetBatch();
        const auto row_budget = chunk_budget * STANDARD_VECTOR_SIZE;
        EnsureMetalBuffers(scratch, row_budget);
        for (idx_t chunk_idx = 0; chunk_idx < chunk_budget; chunk_idx++) {
            DataChunk *source_chunk = nullptr;
            auto source_result = runtime.FetchSourceContract(source_chunk);
            if (source_result == SourceResultType::BLOCKED) {
                return MetalProjectionBatchFetchResult::BLOCKED;
            }
            if (source_chunk) {
                auto next_batch_result =
                    runtime.AdvanceSinkBatch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
                if (next_batch_result == SinkNextBatchType::BLOCKED) {
                    return MetalProjectionBatchFetchResult::BLOCKED;
                }
            }
            if (source_chunk && source_chunk->size() > 0) {
                AppendChunkToProjectionBatch(scratch, *source_chunk);
            }
            if (source_result == SourceResultType::FINISHED) {
                source_finished = true;
                return MetalProjectionBatchFetchResult::READY;
            }
            if (!ShouldBatchSourceChunks() && scratch.batch_chunks > 0) {
                return MetalProjectionBatchFetchResult::READY;
            }
        }
        return MetalProjectionBatchFetchResult::READY;
    }

    void BindSinkIfNeeded(ExecutionRegionRuntime &runtime, MetalProjectionScratch &scratch) const {
        auto &native_runtime = runtime.ExecutionOperators();
        if (!scratch.sink_bound) {
            if (!native_runtime.BindSink(scratch.output, sink_info, scratch.sink_binding)) {
                auto blocker = scratch.sink_binding.blocker.empty() ? "metal-append-sink-binding-failed"
                                                                    : scratch.sink_binding.blocker;
                throw InternalException("Metal append sink binding failed: %s", blocker);
            }
            if (!scratch.sink_binding.ready || !scratch.sink_binding.append_sink.ready) {
                throw InternalException("Metal append sink binding did not return a ready append state");
            }
            scratch.sink_bound = true;
        }
    }

    SinkResultType FlushProjectionBatch(ExecutionRegionRuntime &runtime, MetalProjectionScratch &scratch) const {
        BindSinkIfNeeded(runtime, scratch);
        idx_t row_offset = 0;
        auto &native_runtime = runtime.ExecutionOperators();
        for (idx_t chunk_idx = 0; chunk_idx < scratch.chunk_sizes.size(); chunk_idx++) {
            const auto count = scratch.chunk_sizes[chunk_idx];
            scratch.output.Reset();
            for (idx_t col_idx = 0; col_idx < projection.output_types.size(); col_idx++) {
                CopyMetalBufferToOutputColumn(scratch, col_idx, row_offset, count);
            }
            scratch.output.SetChildCardinality(count);
            auto sink_result = ExecutionSinkAppend(scratch.sink_binding.append_sink, scratch.output);
            if (sink_result != SinkResultType::NEED_MORE_INPUT && chunk_idx + 1 < scratch.chunk_sizes.size()) {
                throw InternalException("Metal batched materialization sink stopped before the batch was flushed");
            }
            auto recorded_result = native_runtime.RecordSinkResult(scratch.output, sink_result);
            if (recorded_result != SinkResultType::NEED_MORE_INPUT) {
                return recorded_result;
            }
            row_offset += count;
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    void ExecuteMetalProjectionBatch(ExecutionRegionRuntime &runtime, MetalProjectionScratch &scratch) const {
        auto count = scratch.batch_rows;
        if (count == 0) {
            return;
        }
        @autoreleasepool {
            if (count > NumericLimits<uint32_t>::Maximum()) {
                throw InternalException("Metal projection batch is larger than uint32_t row indexing supports");
            }
            uint32_t metal_count = static_cast<uint32_t>(count);
            auto count_target = reinterpret_cast<uint32_t *>([(id<MTLBuffer>)scratch.count_buffer.Get() contents]);
            *count_target = metal_count;
            auto error_target = reinterpret_cast<uint32_t *>([(id<MTLBuffer>)scratch.error_buffer.Get() contents]);
            *error_target = 0;

            id<MTLCommandBuffer> command_buffer = [queue commandBuffer];
            if (!command_buffer) {
                throw InternalException("Metal command buffer allocation failed");
            }
            id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
            if (!encoder) {
                throw InternalException("Metal compute encoder allocation failed");
            }
            [encoder setComputePipelineState:pipeline];
            for (idx_t col_idx = 0; col_idx < scratch.input_buffers.size(); col_idx++) {
                [encoder setBuffer:(id<MTLBuffer>)scratch.input_buffers[col_idx].Get() offset:0 atIndex:col_idx];
            }
            for (idx_t col_idx = 0; col_idx < scratch.output_buffers.size(); col_idx++) {
                [encoder setBuffer:(id<MTLBuffer>)scratch.output_buffers[col_idx].Get()
                            offset:0
                           atIndex:projection.input_types.size() + col_idx];
            }
            [encoder setBuffer:(id<MTLBuffer>)scratch.count_buffer.Get()
                        offset:0
                       atIndex:projection.input_types.size() + projection.output_types.size()];
            [encoder setBuffer:(id<MTLBuffer>)scratch.error_buffer.Get()
                        offset:0
                       atIndex:projection.input_types.size() + projection.output_types.size() + 1];

            auto stage_start = std::chrono::steady_clock::now();
            auto max_threads = [pipeline maxTotalThreadsPerThreadgroup];
            NSUInteger threads_per_group = max_threads < 256 ? max_threads : 256;
            MTLSize grid_size = MTLSizeMake(count, 1, 1);
            MTLSize threadgroup_size = MTLSizeMake(threads_per_group, 1, 1);
            [encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
            [encoder endEncoding];
            [command_buffer commit];
            [command_buffer waitUntilCompleted];
            if ([command_buffer status] == MTLCommandBufferStatusError) {
                throw InternalException("Metal projection command failed: %s",
                                        MetalNSErrorToString([command_buffer error]));
            }
            if (*error_target != 0) {
                throw OutOfRangeException("Overflow in Metal DECIMAL projection");
            }
            runtime.RecordGeneratedStageRuntime("metal.projection", MetalElapsedMicros(stage_start));
        }
    }

  private:
    string backend_name;
    id<MTLDevice> device;
    id<MTLCommandQueue> queue;
    id<MTLComputePipelineState> pipeline;
    MetalProjectionStage projection;
    ExecutionRegionSinkInfo sink_info;
    string metal_source;
};

class MetalExecutionRegionBackend : public ExecutionRegionBackend {
  public:
    string Name() const override {
        return "jit_metal";
    }

    string Description() const override {
        return "Apple Metal execution region backend";
    }

    ExecutionRunnerKind RunnerKind() const override {
        return ExecutionRunnerKind::COMPILED_GPU;
    }

    bool IsAvailable() const override {
        return MetalDeviceAvailable();
    }

    bool SupportsRegions() const override {
        return true;
    }

    ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
        ExecutionRegionLoweringPlan lowering_plan;
        BuildMetalRegionPlan(input.region_ir, input.candidate, lowering_plan);
        return lowering_plan;
    }

    ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input) override {
        if (!input.lowering_plan || !input.lowering_plan->backend_plan) {
            return ExecutionRegionCompileResult::Unsupported("Metal backend plan is missing");
        }
        auto backend_plan = dynamic_cast<MetalRegionBackendPlan *>(input.lowering_plan->backend_plan.get());
        if (!backend_plan) {
            return ExecutionRegionCompileResult::Unsupported("Metal backend plan has unexpected type");
        }
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            return ExecutionRegionCompileResult::Unavailable("Apple Metal device is unavailable");
        }
        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) {
            [device release];
            return ExecutionRegionCompileResult::Unavailable("Apple Metal command queue is unavailable");
        }

        auto metal_source = BuildMetalProjectionSource(backend_plan->projection);
        NSError *error = nil;
        NSString *source = [[NSString alloc] initWithUTF8String:metal_source.c_str()];
        auto compile_start = std::chrono::steady_clock::now();
        id<MTLLibrary> library = [device newLibraryWithSource:source options:nil error:&error];
        [source release];
        if (!library) {
            [queue release];
            [device release];
            return ExecutionRegionCompileResult::Error("Apple Metal library compilation failed: " +
                                                       MetalNSErrorToString(error));
        }
        id<MTLFunction> function = [library newFunctionWithName:@"duckdb_jit_metal_projection"];
        [library release];
        if (!function) {
            [queue release];
            [device release];
            return ExecutionRegionCompileResult::Error("Apple Metal projection function was not found");
        }
        id<MTLComputePipelineState> pipeline = [device newComputePipelineStateWithFunction:function error:&error];
        [function release];
        if (!pipeline) {
            [queue release];
            [device release];
            return ExecutionRegionCompileResult::Error("Apple Metal compute pipeline creation failed: " +
                                                       MetalNSErrorToString(error));
        }

        auto kernel = make_uniq<MetalProjectionKernel>(Name(), device, queue, pipeline, backend_plan->projection,
                                                       backend_plan->sink_info, metal_source);
        auto result =
            ExecutionRegionCompileResult::Compiled(std::move(kernel), ExecutionRegionExecutionMode::GPU,
                                                   "execution:metal-projection-append-sink", backend_plan->ir);
        result.timings.kernel_build_time_us = MetalElapsedMicros(compile_start);
        return result;
    }
};

} // namespace

void RegisterMetalExecutionRegionBackend(ExtensionLoader &loader) {
    RegisterExecutionRegionBackend(loader.GetDatabaseInstance(), make_uniq<MetalExecutionRegionBackend>(),
                                   EXECUTION_REGION_BACKEND_ABI_VERSION);
}

} // namespace duckdb
