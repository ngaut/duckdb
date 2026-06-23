cmake_minimum_required(VERSION 2.8.12...3.29)

project(JitSljitExtension)

include_directories(include)

add_third_party(sljit)

set(JIT_SLJIT_EXTENSION_FILES
    jit_sljit_extension.cpp
    sljit_aggregate_codegen.cpp
    sljit_backend.cpp
    sljit_codegen_util.cpp
    sljit_expression_tree_codegen.cpp
    sljit_native_codegen.cpp
    sljit_native_plan.cpp
    sljit_native_runtime.cpp
    sljit_native_util.cpp
    sljit_platform.cpp
    sljit_predicate_codegen.cpp
    sljit_region_description.cpp
    sljit_region_codegen.cpp
    sljit_region_executable.cpp
    sljit_region_expression_plan.cpp
    sljit_region_join_plan.cpp
    sljit_region_node_plan.cpp
    sljit_region_plan_copy.cpp
    sljit_region_plan.cpp
    sljit_region_runtime.cpp
    sljit_region_sink_plan.cpp
    sljit_region_source_plan.cpp
    sljit_typed_expression_codegen.cpp
    sljit_typed_expression_plan.cpp
    sljit_region.cpp)

build_static_extension(jit_sljit ${JIT_SLJIT_EXTENSION_FILES})
target_link_libraries(jit_sljit_extension duckdb_sljit)

install(
  TARGETS jit_sljit_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
