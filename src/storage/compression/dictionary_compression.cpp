#include "duckdb/storage/compression/dictionary/analyze.hpp"
#include "duckdb/storage/compression/dictionary/compression.hpp"
#include "duckdb/storage/compression/dictionary/decompression.hpp"

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

/*
Data layout per segment:
+------------------------------------------------------+
|                  Header                              |
|   +----------------------------------------------+   |
|   |   dictionary_compression_header_t  header    |   |
|   +----------------------------------------------+   |
|                                                      |
+------------------------------------------------------+
|             Selection Buffer               |
|   +------------------------------------+   |
|   |   uint16_t index_buffer_idx[]      |   |
|   +------------------------------------+   |
|      tuple index -> index buffer idx       |
|                                            |
+--------------------------------------------+
|               Index Buffer                 |
|   +------------------------------------+   |
|   |   uint16_t  dictionary_offset[]    |   |
|   +------------------------------------+   |
|  string_index -> offset in the dictionary  |
|                                            |
+--------------------------------------------+
|                Dictionary                  |
|   +------------------------------------+   |
|   |   uint8_t *raw_string_data         |   |
|   +------------------------------------+   |
|      the string data without lengths       |
|                                            |
+--------------------------------------------+
*/

namespace duckdb {

struct DictionaryCompressionStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, const Vector &input);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, const Vector &scan_vector);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(const QueryContext &context, ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
	                         SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
	                         TableFilterState &filter_state);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &storage_manager = col_data.GetStorageManager();
	if (StorageManager::TargetAtLeastVersion(StorageVersion::V1_3_0, storage_manager.GetStorageVersion())) {
		// dict_fsst introduced - disable dictionary
		return nullptr;
	}

	return make_uniq<DictionaryAnalyzeState>(col_data.GetBlockManager());
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<DictionaryAnalyzeState>();
	return DictionaryCompression::UpdateState(state, input);
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<DictionaryAnalyzeState>();

	if (state.current_tuple_count != 0) {
		state.UpdateMaxUniqueCount();
	}

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space = DictionaryCompression::RequiredSpace(state.current_tuple_count, state.current_unique_count,
	                                                      state.current_dict_size, width);

	const auto total_space = state.segment_count * state.info.GetBlockSize() + req_space;
	return LossyNumericCast<idx_t>(DictionaryCompression::MINIMUM_COMPRESSION_RATIO * float(total_space));
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                                           unique_ptr<AnalyzeState> state_p) {
	const auto &state = state_p->Cast<DictionaryAnalyzeState>();
	return make_uniq<DictionaryCompressionCompressState>(checkpoint_data, state.max_unique_count_across_segments);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	DictionaryCompression::UpdateState(state, scan_vector);
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(const QueryContext &context,
                                                                          ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto state = make_uniq<CompressedStringScanState>(buffer_manager.Pin(segment.GetBlockHandle()));
	state->Initialize(segment, true);
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();

	auto start = state.GetPositionInSegment();
	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE) {
		scan_state.ScanToFlatVector(result, result_offset, start, scan_count);
	} else {
		scan_state.ScanToDictionaryVector(segment, result, result_offset, start, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

static bool TryAddDictionaryStringEqualityConstant(const Expression &expr, vector<string> &constants) {
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	optional_ptr<const BoundConstantExpression> constant;
	if (left.GetExpressionType() == ExpressionType::BOUND_REF && right.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		constant = &right.Cast<BoundConstantExpression>();
	} else if (right.GetExpressionType() == ExpressionType::BOUND_REF &&
	           left.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		constant = &left.Cast<BoundConstantExpression>();
	} else {
		return false;
	}
	auto &value = constant->GetValue();
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	constants.push_back(value.GetValue<string>());
	return true;
}

static optional_ptr<const Expression> TryGetDictionaryExactOptionalStringFilterChild(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != OptionalFilterScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	auto &data = function_expr.BindInfo()->Cast<OptionalFilterFunctionData>();
	return data.child_filter_expr.get();
}

static bool TryCollectDictionaryStringEqualityConstants(const Expression &expr, vector<string> &constants) {
	if (auto child = TryGetDictionaryExactOptionalStringFilterChild(expr)) {
		return TryCollectDictionaryStringEqualityConstants(*child, constants);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!TryCollectDictionaryStringEqualityConstants(*child, constants)) {
				return false;
			}
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &in_expr = expr.Cast<BoundOperatorExpression>();
		auto &children = in_expr.GetChildren();
		if (children.size() < 2 || children[0]->GetExpressionType() != ExpressionType::BOUND_REF) {
			return false;
		}
		for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
			if (children[child_idx]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
				return false;
			}
			auto &value = children[child_idx]->Cast<BoundConstantExpression>().GetValue();
			if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
				return false;
			}
			constants.push_back(value.GetValue<string>());
		}
		return true;
	}
	return TryAddDictionaryStringEqualityConstant(expr, constants);
}

static bool GetDictionaryStringEqualityConstants(const TableFilter &filter, TableFilterState &filter_state,
                                                 const vector<string> *&constants) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &state = filter_state.Cast<ExpressionFilterState>();
	if (!state.fast_string_equality_filter_initialized) {
		state.fast_string_equality_filter_initialized = true;
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		state.fast_string_equality_filter_supported =
		    TryCollectDictionaryStringEqualityConstants(*expression_filter.expr, state.fast_string_equality_constants) &&
		    !state.fast_string_equality_constants.empty();
		if (!state.fast_string_equality_filter_supported) {
			state.fast_string_equality_constants.clear();
		}
	}
	if (!state.fast_string_equality_filter_supported) {
		return false;
	}
	constants = &state.fast_string_equality_constants;
	return true;
}

static void BuildDictionaryStringEqualityMatches(Vector &dictionary, idx_t dictionary_size,
                                                 const vector<string> &constants, unsafe_unique_array<bool> &matches) {
	for (idx_t dict_idx = 0; dict_idx < dictionary_size; dict_idx++) {
		matches[dict_idx] = false;
	}
	auto dict_data = FlatVector::GetData<string_t>(dictionary);
	auto &dict_validity = FlatVector::Validity(dictionary);
	for (idx_t dict_idx = 0; dict_idx < dictionary_size; dict_idx++) {
		if (!dict_validity.RowIsValid(dict_idx)) {
			continue;
		}
		auto &dict_value = dict_data[dict_idx];
		for (auto &constant : constants) {
			string_t constant_value(constant);
			if (string_t::StringComparisonOperators::Equals(dict_value, constant_value)) {
				matches[dict_idx] = true;
				break;
			}
		}
	}
}

void DictionaryCompressionStorage::StringFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                                Vector &result, SelectionVector &sel, idx_t &sel_count,
                                                const TableFilter &filter, TableFilterState &filter_state) {
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	if (vector_count != STANDARD_VECTOR_SIZE || !scan_state.dictionary) {
		StringScan(segment, state, vector_count, result);
		UnifiedVectorFormat vdata;
		result.ToUnifiedFormat(vdata);
		ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, vector_count, sel_count);
		return;
	}

	if (!scan_state.filter_result) {
		scan_state.filter_result = make_unsafe_uniq_array<bool>(scan_state.dictionary_size);
		const vector<string> *constants;
		if (GetDictionaryStringEqualityConstants(filter, filter_state, constants)) {
			BuildDictionaryStringEqualityMatches(scan_state.dictionary->data, scan_state.dictionary_size, *constants,
			                                     scan_state.filter_result);
		} else {
			for (idx_t i = 0; i < scan_state.dictionary_size; i++) {
				scan_state.filter_result[i] = false;
			}
			auto &dict_data = scan_state.dictionary->data;
			UnifiedVectorFormat vdata;
			dict_data.ToUnifiedFormat(vdata);
			SelectionVector dict_sel;
			idx_t filter_count = scan_state.dictionary_size;
			ColumnSegment::FilterSelection(dict_sel, dict_data, vdata, filter, filter_state, scan_state.dictionary_size,
			                               filter_count);
			for (idx_t i = 0; i < filter_count; i++) {
				auto idx = dict_sel.get_index(i);
				scan_state.filter_result[idx] = true;
			}
		}
	}

	auto start = state.GetPositionInSegment();
	auto &dict_sel = scan_state.GetSelVec(start, vector_count);
	if (scan_state.filter_sel.Capacity() < sel_count) {
		scan_state.filter_sel.Initialize(MaxValue<idx_t>(sel_count, STANDARD_VECTOR_SIZE));
	}
	auto &new_sel = scan_state.filter_sel;
	idx_t approved_tuple_count = 0;
	for (idx_t idx = 0; idx < sel_count; idx++) {
		auto row_idx = sel.get_index(idx);
		auto dict_offset = dict_sel.get_index(row_idx);
		if (scan_state.filter_result[dict_offset]) {
			new_sel.set_index(approved_tuple_count++, row_idx);
		}
	}
	if (approved_tuple_count < vector_count) {
		sel.Initialize(new_sel);
	}
	sel_count = approved_tuple_count;
	result.Dictionary(scan_state.dictionary, dict_sel, vector_count);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	CompressedStringScanState scan_state(state.GetOrInsertHandle(segment));
	scan_state.Initialize(segment, false);
	scan_state.ScanToFlatVector(result, result_idx, NumericCast<idx_t>(row_id), 1);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	auto function = CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, DictionaryCompressionStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial<false>,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip,
	    UncompressedStringStorage::StringInitSegment);
	function.filter = DictionaryCompressionStorage::StringFilter;
	return function;
}

bool DictionaryCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
