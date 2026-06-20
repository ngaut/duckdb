#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/physical_table_scan_enum.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <utility>

namespace duckdb {

struct TableScanExecutionSourceConfig {
	bool use_source_contract = false;
	vector<idx_t> projection_ids;
	optional_ptr<TableFilterSet> filters;
};

struct TableScanExecutionSourceContractGlobalState {
	DataTable *storage = nullptr;
	DuckTransaction *transaction = nullptr;
	ParallelTableScanState parallel_state;
	vector<StorageIndex> storage_ids;
	vector<LogicalType> scanned_types;
	vector<idx_t> projection_ids;
	bool can_remove_filter_columns = false;
	bool is_create_index = false;
};

static bool IsExecutionTableScanSourceContractSupported(const PhysicalTableScan &op) {
	if (StringUtil::Lower(op.function.name.GetIdentifierName()) != "seq_scan") {
		return false;
	}
	if (!op.function.function || op.function.in_out_function) {
		return false;
	}
	if (!op.bind_data) {
		return false;
	}
	auto &bind_data = op.bind_data->Cast<TableScanBindData>();
	return !bind_data.is_index_scan;
}

static TableScanExecutionSourceConfig
BuildTableScanExecutionSourceConfig(const PhysicalTableScan &op, optional_ptr<TableFilterSet> filters,
                                    const ExecutionRegionOpenRequest &open_request) {
	TableScanExecutionSourceConfig result;
	result.projection_ids = op.projection_ids;
	result.filters = filters;
	result.use_source_contract = open_request.UsesSourceContract() && IsExecutionTableScanSourceContractSupported(op);
	return result;
}

static LogicalType GetExecutionSourceContractTableScanColumnType(const PhysicalTableScan &scan,
                                                                 const ColumnIndex &column_index) {
	if (column_index.IsRowIdColumn() || column_index.IsRowNumberColumn()) {
		return LogicalType::ROW_TYPE;
	}
	if (column_index.HasType()) {
		return column_index.GetScanType();
	}
	auto column_id = column_index.GetPrimaryIndex();
	if (IsVirtualColumn(column_id)) {
		auto entry = scan.virtual_columns.find(column_id);
		if (entry == scan.virtual_columns.end()) {
			throw InternalException(
			    "Virtual column not found while initializing execution region table scan source contract");
		}
		return entry->second.type;
	}
	if (column_id >= scan.returned_types.size()) {
		throw InternalException(
		    "Column index %llu is outside returned type count %llu while initializing execution table "
		    "scan source contract",
		    static_cast<unsigned long long>(column_id), static_cast<unsigned long long>(scan.returned_types.size()));
	}
	return scan.returned_types[column_id];
}

static void
InitializeExecutionSourceContractTableScanGlobalState(ClientContext &context, const PhysicalTableScan &op,
                                                      TableScanExecutionSourceConfig &execution_source_config,
                                                      TableScanExecutionSourceContractGlobalState &contract_state) {
	if (!execution_source_config.use_source_contract) {
		return;
	}
	auto &bind_data = op.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	contract_state.storage = &storage;
	contract_state.transaction = &DuckTransaction::Get(context, duck_table.catalog);
	contract_state.is_create_index = bind_data.is_create_index;

	for (auto &column_index : op.column_ids) {
		contract_state.storage_ids.push_back(bind_data.table.GetStorageIndex(column_index));
		contract_state.scanned_types.push_back(GetExecutionSourceContractTableScanColumnType(op, column_index));
	}
	contract_state.projection_ids = execution_source_config.projection_ids;
	contract_state.can_remove_filter_columns =
	    !contract_state.projection_ids.empty() && contract_state.projection_ids.size() != op.column_ids.size();

	if (bind_data.order_options) {
		auto transaction = TransactionData(*contract_state.transaction);
		contract_state.parallel_state.scan_state.reorderer =
		    make_uniq<RowGroupReorderer>(*bind_data.order_options, transaction);
		contract_state.parallel_state.local_state.reorderer =
		    make_uniq<RowGroupReorderer>(*bind_data.order_options, transaction);
	}
	if (bind_data.partitions_to_scan) {
		contract_state.parallel_state.scan_state.partitions_to_scan = bind_data.partitions_to_scan.get();
	}
	for (idx_t column_idx = 0; column_idx < op.column_ids.size(); column_idx++) {
		if (op.column_ids[column_idx].GetPrimaryIndex() == COLUMN_IDENTIFIER_ROW_NUMBER) {
			contract_state.parallel_state.scan_state.row_number_base = 0;
			break;
		}
	}
	storage.InitializeParallelScan(context, contract_state.parallel_state, op.column_ids);
}

PhysicalTableScan::PhysicalTableScan(PhysicalPlan &physical_plan, vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<ColumnIndex> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info,
                                     vector<Value> parameters_p, virtual_column_map_t virtual_columns_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),

      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)), extra_info(std::move(extra_info)), parameters(std::move(parameters_p)),
      virtual_columns(std::move(virtual_columns_p)) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op,
	                           const ExecutionRegionOpenRequest &open_request) {
		physical_table_scan_execution_strategy = Settings::Get<DebugPhysicalTableScanExecutionStrategySetting>(context);

		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}

		if (op.function.init_global) {
			auto filters = table_filters ? optional_ptr<TableFilterSet>(*table_filters) : GetTableFilters(op);
			execution_source_config = BuildTableScanExecutionSourceConfig(op, filters, open_request);
			InitializeExecutionSourceContractTableScanGlobalState(context, op, execution_source_config,
			                                                      execution_source_contract);
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, filters,
			                             op.extra_info.sample_options, &op);

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(BufferAllocator::Get(context), input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].Reference(op.parameters[c], count_t(1));
			}
		}
	}

	idx_t max_threads = 0;
	PhysicalTableScanExecutionStrategy physical_table_scan_execution_strategy;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;
	TableScanExecutionSourceConfig execution_source_config;
	TableScanExecutionSourceContractGlobalState execution_source_contract;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			auto filters = gstate.GetTableFilters(op);
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, filters,
			                             op.extra_info.sample_options, &op);
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
		if (gstate.execution_source_contract.storage) {
			InitializeExecutionSourceContract(context, gstate, op);
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
	TableScanState execution_source_contract_scan_state;
	DataChunk execution_source_contract_all_columns;

private:
	void InitializeExecutionSourceContract(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                                       const PhysicalTableScan &op) {
		auto filters = gstate.GetTableFilters(op);
		execution_source_contract_scan_state.Initialize(gstate.execution_source_contract.storage_ids, context.client,
		                                                filters, op.extra_info.sample_options);
		gstate.execution_source_contract.storage->NextParallelScan(
		    context.client, gstate.execution_source_contract.parallel_state, execution_source_contract_scan_state);
		if (gstate.execution_source_contract.can_remove_filter_columns) {
			execution_source_contract_all_columns.Initialize(context.client,
			                                                 gstate.execution_source_contract.scanned_types);
		}
		execution_source_contract_scan_state.options.force_fetch_row =
		    Settings::Get<DebugForceFetchRowSetting>(context.client);
	}
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	ExecutionRegionOpenRequest open_request;
	return make_uniq<TableScanGlobalSourceState>(context, *this, open_request);
}

unique_ptr<GlobalSourceState>
PhysicalTableScan::GetGlobalSourceState(ClientContext &context, const ExecutionRegionOpenRequest &open_request) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this, open_request);
}

void PhysicalTableScan::GetExecutionRegionPreGraphSourceInfo(idx_t &estimated_source_cardinality,
                                                             idx_t &source_filter_count) const {
	estimated_source_cardinality = estimated_cardinality;
	if (bind_data && StringUtil::Lower(function.name.GetIdentifierName()) == "seq_scan") {
		auto &table_scan_bind = bind_data->Cast<TableScanBindData>();
		estimated_source_cardinality = table_scan_bind.table.GetStorage().GetTotalRows();
	}
	source_filter_count = table_filters ? table_filters->FilterCount() : 0;
}

bool PhysicalTableScan::SupportsExecutionSourceContract(const ExecutionRegionOpenRequest &open_request) const {
	return open_request.UsesSourceContract() && IsExecutionTableScanSourceContractSupported(*this);
}

SourceResultType PhysicalTableScan::GetExecutionSourceContractDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                           OperatorSourceInput &input) const {
	auto &g_state = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &l_state = input.local_state.Cast<TableScanLocalSourceState>();
	if (!g_state.execution_source_contract.storage) {
		throw InternalException("execution region table scan source contract was not initialized");
	}
	auto &contract_state = g_state.execution_source_contract;
	auto &scan_state = l_state.execution_source_contract_scan_state;
	scan_state.options.force_fetch_row = Settings::Get<DebugForceFetchRowSetting>(context.client);

	do {
		if (contract_state.is_create_index) {
			ExecutionOperatorStageTimer timer(input.stage_recorder, "source_contract.table_scan.create_index_scan");
			contract_state.storage->CreateIndexScan(scan_state, chunk);
		} else if (contract_state.can_remove_filter_columns) {
			l_state.execution_source_contract_all_columns.Reset();
			{
				ExecutionOperatorStageTimer timer(input.stage_recorder,
				                                  "source_contract.table_scan.storage_scan_all_columns");
				contract_state.storage->Scan(*contract_state.transaction, l_state.execution_source_contract_all_columns,
				                             scan_state);
			}
			{
				ExecutionOperatorStageTimer timer(input.stage_recorder,
				                                  "source_contract.table_scan.reference_projection");
				chunk.ReferenceColumns(l_state.execution_source_contract_all_columns, contract_state.projection_ids);
			}
		} else {
			ExecutionOperatorStageTimer timer(input.stage_recorder,
			                                  "source_contract.table_scan.storage_scan_projected");
			contract_state.storage->Scan(*contract_state.transaction, chunk, scan_state);
		}

		if (chunk.size() > 0) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		}

		idx_t rows_in_current_row_group;
		{
			ExecutionOperatorStageTimer timer(input.stage_recorder, "source_contract.table_scan.next_parallel_scan");
			rows_in_current_row_group =
			    contract_state.storage->NextParallelScan(context.client, contract_state.parallel_state, scan_state);
		}
		if (rows_in_current_row_group == 0) {
			return SourceResultType::FINISHED;
		}
		{
			ExecutionOperatorStageTimer timer(input.stage_recorder, "source_contract.table_scan.interrupt_check");
			context.client.InterruptCheck();
		}
	} while (true);
}

static void ValidateAsyncStrategyResult(const PhysicalTableScanExecutionStrategy &strategy,
                                        const AsyncResultsExecutionMode &execution_mode_pre,
                                        const AsyncResultsExecutionMode &execution_mode_post,
                                        const AsyncResultType &result_pre, const AsyncResultType &result_post,
                                        const idx_t output_chunk_size) {
	auto execution_mode_pre_computed = AsyncResult::ConvertToAsyncResultExecutionMode(strategy);
	if (execution_mode_pre_computed != execution_mode_pre) {
		throw InternalException("ValidateAsyncStrategyResult: invalid conversion PhysicalTableScanExecutionStrategy to "
		                        "AsyncResultsExecutionMode, from '%s', to '%s'",
		                        EnumUtil::ToChars(strategy), EnumUtil::ToChars(execution_mode_pre));
	}

	if (execution_mode_pre != execution_mode_post) {
		throw InternalException("ValidateAsyncStrategyResult: results_execution_mode changed within table API's "
		                        "`function` call, before '%s', after '%s'",
		                        EnumUtil::ToChars(execution_mode_pre), EnumUtil::ToChars(execution_mode_post));
	}
	if (result_pre != AsyncResultType::IMPLICIT) {
		throw InternalException("ValidateAsyncStrategyResult: async_result is supposed to be IMPLICIT, was '%s', "
		                        "before table API's `function` call",
		                        EnumUtil::ToChars(result_pre));
	}
	switch (strategy) {
	case PhysicalTableScanExecutionStrategy::TASK_EXECUTOR_BUT_FORCE_SYNC_CHECKS:
		// This is a funny one, expected to throw on non-trivial workflows in this function
	case PhysicalTableScanExecutionStrategy::SYNCHRONOUS:
		switch (result_post) {
		case AsyncResultType::INVALID:
			throw InternalException("ValidateAsyncStrategyResult: found INVALID");
		case AsyncResultType::BLOCKED:
			throw InternalException("ValidateAsyncStrategyResult: found BLOCKED");
		case AsyncResultType::FINISHED:
			if (output_chunk_size > 0) {
				throw InternalException("ValidateAsyncStrategyResult: found FINISHED with non-empty chunk");
			}
			break;
		case AsyncResultType::HAVE_MORE_OUTPUT:
			if (output_chunk_size == 0) {
				throw InternalException("ValidateAsyncStrategyResult: found HAVE_MORE_OUTPUT with empty chunk");
			}
			break;
		case AsyncResultType::IMPLICIT:
			break;
		}
		break;
	default:
		if (result_post == AsyncResultType::BLOCKED) {
			if (output_chunk_size > 0) {
				throw InternalException("ValidateAsyncStrategyResult: found BLOCKED with non-empty chunk");
			}
		}
		break;
	}
}

SourceResultType PhysicalTableScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &g_state = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &l_state = input.local_state.Cast<TableScanLocalSourceState>();

	TableFunctionInput data(bind_data.get(), l_state.local_state.get(), g_state.global_state.get());

	if (function.function) {
		data.async_result = AsyncResultType::IMPLICIT;

		const auto initial_async_result = data.async_result.GetResultType();
		const auto execution_strategy = g_state.physical_table_scan_execution_strategy;
		const auto input_execution_mode = AsyncResult::ConvertToAsyncResultExecutionMode(execution_strategy);
		data.results_execution_mode = input_execution_mode;

		// Actually call the function
		function.function(context.client, data, chunk);

		const auto output_async_result = data.async_result.GetResultType();

		// Compare and check whether state before and after function.function call is compatible, will throw in case of
		// inconsistencies
		ValidateAsyncStrategyResult(execution_strategy, input_execution_mode, data.results_execution_mode,
		                            initial_async_result, output_async_result, chunk.size());

		// Handle results
		switch (output_async_result) {
		case AsyncResultType::BLOCKED: {
			D_ASSERT(data.async_result.HasTasks());
			{
				annotated_lock_guard<annotated_mutex> guard(g_state.lock);
				if (g_state.CanBlock()) {
					data.async_result.ScheduleTasks(input.interrupt_state, context.pipeline->executor);
					return SourceResultType::BLOCKED;
				}
			}
			data.async_result.ExecuteTasksSynchronously();
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		case AsyncResultType::IMPLICIT:
			if (chunk.size() > 0) {
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			return SourceResultType::FINISHED;
		case AsyncResultType::FINISHED:
			return SourceResultType::FINISHED;
		case AsyncResultType::HAVE_MORE_OUTPUT:
			return SourceResultType::HAVE_MORE_OUTPUT;
		default:
			throw InternalException(
			    "PhysicalTableScan::GetData call of function.function returned unexpected return '%'",
			    EnumUtil::ToChars(data.async_result.GetResultType()));
		}
		throw InternalException("PhysicalTableScan::GetData hasn't handled a function.function return");
	}

	if (g_state.in_out_final) {
		function.in_out_function_final(context, data, chunk);
	}
	switch (function.in_out_function(context, data, g_state.input_chunk, chunk)) {
	case OperatorResultType::BLOCKED: {
		annotated_lock_guard<annotated_mutex> guard(g_state.lock);
		return g_state.BlockSource(input.interrupt_state);
	}
	default:
		// FIXME: Handling for other cases (such as NEED_MORE_INPUT) breaks current functionality and extensions that
		// might be relying on current behaviour. Needs a rework that is not in scope
		break;
	}

	if (chunk.size() == 0 && function.in_out_function_final) {
		function.in_out_function_final(context, data, chunk);
		g_state.in_out_final = true;
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	ProgressData res;
	if (function.table_scan_progress) {
		double table_progress = function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
		if (table_progress < 0.0) {
			res.SetInvalid();
		} else {
			res.done = table_progress;
			res.total = 100.0;
			// Assume cardinality is always 1e3
			res.Normalize(1e3);
		}
	} else {
		// if table_scan_progress is not implemented we don't support this function yet in the progress bar
		res.SetInvalid();
	}
	return res;
}

bool PhysicalTableScan::SupportsPartitioning(const OperatorPartitionInfo &partition_info) const {
	if (!function.get_partition_data) {
		return false;
	}
	// FIXME: actually check if partition info is supported
	return true;
}

OperatorPartitionData PhysicalTableScan::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalSourceState &gstate_p, LocalSourceState &lstate,
                                                          const OperatorPartitionInfo &partition_info) const {
	D_ASSERT(SupportsPartitioning(partition_info));
	D_ASSERT(function.get_partition_data);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionGetPartitionInput input(bind_data.get(), state.local_state.get(), gstate.global_state.get(),
	                                     partition_info);
	return function.get_partition_data(context.client, input);
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + (function.extra_info.empty() ? "" : " " + function.extra_info));
}

void AddProjectionNames(const ColumnIndex &index, const string &name, const LogicalType &type, string &result) {
	if (!index.HasChildren()) {
		// base case - no children projected out
		if (!result.empty()) {
			result += "\n";
		}
		result += name;
		return;
	}

	if (type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		for (auto &child_index : index.GetChildIndexes()) {
			if (child_index.HasPrimaryIndex()) {
				auto &ele = child_types[child_index.GetPrimaryIndex()];
				AddProjectionNames(child_index, name + "." + ele.first, ele.second, result);
			} else {
				auto field_type = child_index.HasType() ? child_index.GetType() : LogicalType::VARIANT();
				AddProjectionNames(child_index, name + "." + child_index.GetFieldName(), field_type, result);
			}
		}
	} else if (type.id() == LogicalTypeId::VARIANT) {
		for (auto &child_index : index.GetChildIndexes()) {
			D_ASSERT(!child_index.HasPrimaryIndex());
			auto field_type = child_index.HasType() ? child_index.GetType() : LogicalType::VARIANT();
			AddProjectionNames(child_index, name + "." + child_index.GetFieldName(), field_type, result);
		}
	} else {
		throw InternalException("Unexpected type (%s) in AddProjectionNames", type.ToString());
	}
}

string PhysicalTableScan::GetFilterInfo(const TableFilterSet &filter_set) const {
	string filters_info;
	bool first_item = true;
	for (auto &f : filter_set) {
		auto filter_idx = f.GetIndex();
		auto &filter = f.Filter().Cast<ExpressionFilter>();
		if (filter_idx < names.size()) {
			if (!first_item) {
				filters_info += "\n";
			}
			first_item = false;

			auto &column_id = column_ids[filter_idx];
			const auto col_id = column_id.GetPrimaryIndex();
			if (IsVirtualColumn(col_id)) {
				auto entry = virtual_columns.find(col_id);
				if (entry == virtual_columns.end()) {
					throw InternalException("Virtual column not found");
				}
				filters_info += filter.ToString(entry->second.name.GetIdentifierName());
			} else {
				auto column_name = column_id.GetName(names[col_id]);
				filters_info += filter.ToString(column_name);
			}
		}
	}
	return filters_info;
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (function.to_string) {
		TableFunctionToStringInput input(function, bind_data.get());
		auto to_string_result = function.to_string(input);
		for (const auto &it : to_string_result) {
			result[it.first] = it.second;
		}
	} else {
		result["Function"] = StringUtil::Upper(function.name.GetIdentifierName());
	}
	if (function.projection_pushdown) {
		string projections;
		idx_t projected_column_count = function.filter_prune ? projection_ids.size() : column_ids.size();
		for (idx_t i = 0; i < projected_column_count; i++) {
			auto base_index = function.filter_prune ? projection_ids[i] : i;
			auto &column_index = column_ids[base_index];
			auto column_id = column_index.GetPrimaryIndex();
			if (column_id >= names.size()) {
				continue;
			}
			AddProjectionNames(column_index, names[column_id], returned_types[column_id], projections);
		}
		result["Projections"] = projections;
	}
	if (function.filter_pushdown && table_filters) {
		result["Filters"] = GetFilterInfo(*table_filters);
	}

	if (function.filter_pushdown && dynamic_filters && dynamic_filters->HasFilters()) {
		result["Dynamic Filters"] = GetFilterInfo(*dynamic_filters->GetFinalTableFilters(*this, nullptr));
	}

	if (extra_info.sample_options) {
		result["Sample Method"] = "System: " + extra_info.sample_options->sample_size.ToString() + "%";
	}
	if (!extra_info.file_filters.empty()) {
		result["File Filters"] = extra_info.file_filters;
		if (extra_info.filtered_files.IsValid() && extra_info.total_files.IsValid()) {
			result["Scanning Files"] = StringUtil::Format("%llu/%llu", extra_info.filtered_files.GetIndex(),
			                                              extra_info.total_files.GetIndex());
		}
	}

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function != other.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

bool PhysicalTableScan::ParallelSource() const {
	if (!function.function) {
		// table in-out functions cannot be executed in parallel as part of a PhysicalTableScan
		// since they have only a single input row
		return false;
	}
	return true;
}

TableFunctionParallelism PhysicalTableScan::SourceParallelism() const {
	return function.parallelism;
}

void PhysicalTableScan::GetMetrics(ClientContext &context, GlobalSourceState &gstate_p, LocalSourceState &lstate,
                                   OperatorMetrics &operator_metrics) const {
	if (!function.get_metrics) {
		return;
	}
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionGetMetricsInput input(context, bind_data.get(), state.local_state.get(), gstate.global_state.get(),
	                                   operator_metrics);
	function.get_metrics(input);
}

} // namespace duckdb
