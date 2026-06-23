cmake_minimum_required(VERSION 2.8.12...3.29)

project(JitMetalExtension)

if(APPLE)
  enable_language(OBJCXX)

  include_directories(include)

  set(JIT_METAL_EXTENSION_FILES jit_metal_extension.cpp metal_backend.mm)

  build_static_extension(jit_metal ${JIT_METAL_EXTENSION_FILES})
  target_link_libraries(jit_metal_extension "-framework Metal" "-framework Foundation")
  set_property(
    TARGET jit_metal_extension
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES "-framework Metal" "-framework Foundation")

  install(
    TARGETS jit_metal_extension
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
endif()
