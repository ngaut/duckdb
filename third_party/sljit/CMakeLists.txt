add_library(duckdb_sljit STATIC sljit_src/sljitLir.c)
target_include_directories(duckdb_sljit PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sljit_src>)
set_target_properties(duckdb_sljit PROPERTIES EXPORT_NAME duckdb_sljit POSITION_INDEPENDENT_CODE ON)

install(
  TARGETS duckdb_sljit
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_sljit)
