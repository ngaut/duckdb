add_library(duckdb_sljit STATIC sljit_src/sljitLir.c)
target_include_directories(duckdb_sljit PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sljit_src>)
set_target_properties(duckdb_sljit PROPERTIES EXPORT_NAME duckdb_sljit POSITION_INDEPENDENT_CODE ON)

# Executable-memory policy is a platform contract, not a backend runtime choice.
# Apple requires SLJIT's MAP_JIT allocator (including pthread_jit_write_protect_np
# on arm64). Linux can retain pooled allocation while exposing distinct RW and RX
# mappings. Other supported platforms use the per-allocation W^X implementation.
if(APPLE)
  # Keep SLJIT's native Apple executable allocator.
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_compile_definitions(duckdb_sljit PRIVATE SLJIT_PROT_EXECUTABLE_ALLOCATOR=1)
elseif(WIN32 OR UNIX)
  target_compile_definitions(duckdb_sljit PRIVATE SLJIT_WX_EXECUTABLE_ALLOCATOR=1)
else()
  message(FATAL_ERROR "jit_sljit has no W^X executable-memory policy for ${CMAKE_SYSTEM_NAME}")
endif()

install(
  TARGETS duckdb_sljit
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_sljit)
