//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/operator_descriptor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/compiled_contract.hpp"
#include "duckdb/execution/jit/region.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class Expression;

struct JitOperatorSourceFilterDescriptor {
	idx_t filter_index = 0;
	idx_t scan_column_index = 0;
	idx_t table_column_index = 0;
	optional_ptr<const Expression> expression;
	string reason;
};

struct JitOperatorSourceDescriptor {
	JitRegionSourceKind kind = JitRegionSourceKind::NONE;
	JitRegionSourceExecutionKind execution = JitRegionSourceExecutionKind::NONE;
	string function_name;
	vector<JitRegionProtocolField> fields;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	vector<JitOperatorSourceFilterDescriptor> filters;
	JitRegionTableScanProtocol table_scan_protocol;
	JitRegionHashJoinProtocol hash_join_protocol;
	JitRegionAggregateProtocol aggregate_protocol;
	vector<JitRegionAggregateInput> aggregates;
	vector<JitRegionHashJoinKeyInput> hash_join_keys;
	vector<JitRegionGroupInput> groups;
	JitRegionNativeSourceContract native_source_contract;
	JitRegionNativeStateScanContract native_state_scan_contract;
	string reason;
};

struct JitOperatorDescriptor {
	JitCompiledOperatorContract compiled_contract;

	string source_boundary_reason;
	bool has_source = false;
	JitOperatorSourceDescriptor source;

	bool has_operator = false;
	JitRegionOperatorInfo operator_info;

	bool has_sink = false;
	JitRegionSinkInfo sink;
};

DUCKDB_API JitOperatorDescriptor FinalizeJitOperatorDescriptor(JitOperatorDescriptor descriptor);
DUCKDB_API vector<JitRegionProtocolField> BuildJitDescriptorProtocolFields(const string &reason);

} // namespace duckdb
