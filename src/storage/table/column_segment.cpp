#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Create
//===--------------------------------------------------------------------===//

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
                                                                 DataPointer &data_pointer) {
	auto &block_id = data_pointer.block_pointer.block_id;
	auto &offset = data_pointer.block_pointer.offset;
	auto &count = data_pointer.tuple_count;
	auto &compression_type = data_pointer.compression_type;
	auto &statistics = data_pointer.statistics;
	auto &segment_state = data_pointer.segment_state;
	auto &byte_size = data_pointer.byte_size;

	auto &config = DBConfig::GetConfig(db);
	shared_ptr<BlockHandle> block;

	auto &type = statistics.GetType();
	auto function = config.GetCompressionFunction(compression_type, type.InternalType());
	if (block_id == INVALID_BLOCK && !function.get().supports_null_block) {
		throw DataCorruptionException("Persistent %s segment is missing its data block",
		                              CompressionTypeToString(compression_type));
	}
	if (block_id != INVALID_BLOCK) {
		block = block_manager.RegisterBlock(block_id);
	}

	auto segment_size = block_manager.GetBlockSize();
	return make_uniq<ColumnSegment>(db, std::move(block), ColumnSegmentType::PERSISTENT, count, function,
	                                std::move(statistics), block_id, offset, segment_size, std::move(segment_state),
	                                byte_size);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db,
                                                                const CompressionFunction &function,
                                                                const LogicalType &type, const idx_t segment_size,
                                                                BlockManager &block_manager, bool zero_initialize) {
	// Allocate a buffer for the uncompressed segment.
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	D_ASSERT(&buffer_manager == &block_manager.buffer_manager);
	auto block = buffer_manager.RegisterTransientMemory(segment_size, block_manager);
	// we want to zero initialize segments that end up on disk
	if (zero_initialize) {
		auto handle = buffer_manager.Pin(block);
		memset(handle.GetDataMutable(), 0, segment_size);
	}

	return make_uniq<ColumnSegment>(db, std::move(block), ColumnSegmentType::TRANSIENT, 0U, function,
	                                BaseStatistics::CreateEmpty(type), INVALID_BLOCK, 0U, segment_size);
}

unique_ptr<ColumnSegment> SuballocationBlock::CreateTransientSegment(DatabaseInstance &db,
                                                                     const CompressionFunction &function,
                                                                     const LogicalType &type, const idx_t segment_size,
                                                                     BlockManager &block_manager) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	D_ASSERT(&buffer_manager == &block_manager.buffer_manager);

	//	Do we have enough room in the block?
	if (!block || block->GetBlockSize() < allocated + segment_size) {
		auto &temp_block_manger = buffer_manager.GetTemporaryBlockManager();
		block = buffer_manager.RegisterTransientMemory(temp_block_manger.GetBlockSize(), temp_block_manger);
		allocated = 0;
		block_id = block->BlockId();
	}

	const auto offset = allocated;
	allocated += segment_size;
	return make_uniq<ColumnSegment>(db, block, ColumnSegmentType::TRANSIENT, 0U, function,
	                                BaseStatistics::CreateEmpty(type), INVALID_BLOCK, offset, segment_size);
}

//===--------------------------------------------------------------------===//
// Construct/Destruct
//===--------------------------------------------------------------------===//
ColumnSegment::ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block_p,
                             const ColumnSegmentType segment_type, const idx_t count,
                             const CompressionFunction &function_p, BaseStatistics statistics,
                             const block_id_t block_id_p, const idx_t offset, const idx_t segment_size_p,
                             const unique_ptr<ColumnSegmentState> segment_state_p,
                             optional<uint32_t> data_size_in_bytes_p)

    : SegmentBase<ColumnSegment>(count), db(db), segment_type(segment_type), block(std::move(block_p)),
      function(function_p), block_id(block_id_p), offset(offset), segment_size(segment_size_p),
      data_size_in_bytes(data_size_in_bytes_p), stats(std::move(statistics)) {
	if (function.get().init_segment) {
		segment_state = function.get().init_segment(*this, block_id, segment_state_p.get());
	}

	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockSize());
}

ColumnSegment::ColumnSegment(ColumnSegment &other)
    : SegmentBase<ColumnSegment>(other.count.load()), db(other.db), segment_type(other.segment_type),
      block(std::move(other.block)), function(other.function), block_id(other.block_id), offset(other.offset),
      segment_size(other.segment_size), data_size_in_bytes(other.data_size_in_bytes),
      segment_state(std::move(other.segment_state)), stats(std::move(other.stats)) {
	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockSize());
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &) {
	if (!block || block->BlockId() >= MAXIMUM_BLOCK) {
		// not an on-disk block
		return;
	}
	if (function.get().init_prefetch) {
		function.get().init_prefetch(*this, prefetch_state);
	} else {
		prefetch_state.AddBlock(block);
	}
}

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function.get().init_scan(state.context, *this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         ScanVectorType scan_type) {
	if (scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Select(ColumnScanState &state, idx_t scan_count, Vector &result, const SelectionVector &sel,
                           idx_t sel_count, idx_t result_offset, ScanVectorType scan_type) {
	if (!function.get().select) {
		throw InternalException("ColumnSegment::Select not implemented for this compression method");
	}
	if (scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		D_ASSERT(result_offset == 0);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
	function.get().select(*this, state, scan_count, result, sel, sel_count, result_offset, scan_type);
}

void ColumnSegment::Filter(ColumnScanState &state, idx_t scan_count, Vector &result, SelectionVector &sel,
                           idx_t &sel_count, const TableFilter &filter, TableFilterState &filter_state) {
	if (!function.get().filter) {
		throw InternalException("ColumnSegment::Filter not implemented for this compression method");
	}
	function.get().filter(*this, state, scan_count, result, sel, sel_count, filter, filter_state);
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function.get().skip(*this, state, state.offset_in_column - state.internal_index);
	state.internal_index = state.offset_in_column;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	function.get().scan_vector(*this, state, scan_count, result);
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	function.get().scan_partial(*this, state, scan_count, result, result_offset);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::SegmentSize() const {
	return segment_size;
}

void ColumnSegment::Resize(idx_t new_size) {
	D_ASSERT(new_size > segment_size);
	D_ASSERT(offset == 0);
	D_ASSERT(block && new_size <= GetBlockSize());

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto old_handle = buffer_manager.Pin(block);
	// The resized block must keep the transient-segment lifecycle: registered
	// with the block manager and spillable to temporary storage. A plain
	// allocation is destroy-on-eviction, and a live table segment on such a
	// block loses its rows under memory pressure.
	auto new_block = buffer_manager.RegisterTransientMemory(new_size, block->GetBlockManager());
	auto new_handle = buffer_manager.Pin(new_block);
	memcpy(new_handle.GetDataMutable(), old_handle.Ptr(), segment_size);

	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	this->block_id = INVALID_BLOCK;
	this->block = std::move(new_block);
	this->segment_size = new_size;
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().init_append) {
		throw InternalException("Attempting to init append to a segment without init_append method");
	}
	state.append_state = function.get().init_append(*this);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, UnifiedVectorFormat &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().append) {
		throw InternalException("Attempting to append to a segment without append method");
	}
	return function.get().append(*state.append_state, *this, *state.append_stats, append_data, offset, count);
}

idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	auto result_count = function.get().finalize_append(*this, *state.append_stats);
	state.append_state.reset();
	return result_count;
}

void ColumnSegment::RevertAppend(idx_t new_count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function.get().revert_append) {
		function.get().revert_append(*this, new_count);
	}
	this->count = new_count;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistentConstant() {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	D_ASSERT(stats.statistics.IsConstant());

	segment_type = ColumnSegmentType::PERSISTENT;
	block_id = INVALID_BLOCK;
	offset = 0;

	auto &config = DBConfig::GetConfig(db);
	function = config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, GetType().InternalType());
	block.reset();
}

void ColumnSegment::MarkAsPersistent(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	block_id = block_p->BlockId();
	SetBlock(std::move(block_p), offset_p);
}

void ColumnSegment::UsePackedCopy(const shared_ptr<BlockHandle> &block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	offset = offset_p;
	block = block_p;
}

void ColumnSegment::SetBlock(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	segment_type = ColumnSegmentType::PERSISTENT;
	offset = offset_p;
	block = std::move(block_p);
}

DataPointer ColumnSegment::GetDataPointer(idx_t row_start) {
	if (segment_type != ColumnSegmentType::PERSISTENT) {
		throw InternalException("Attempting to call ColumnSegment::GetDataPointer on a transient segment");
	}
	// set up the data pointer directly using the data from the persistent segment
	DataPointer pointer(stats.statistics.Copy());
	pointer.block_pointer.block_id = GetBlockId();
	pointer.block_pointer.offset = NumericCast<uint32_t>(GetBlockOffset());
	pointer.row_start = row_start;
	pointer.tuple_count = count;
	pointer.compression_type = function.get().type;
	pointer.byte_size = data_size_in_bytes;
	if (function.get().serialize_state) {
		pointer.segment_state = function.get().serialize_state(*this);
	}
	return pointer;
}

//===--------------------------------------------------------------------===//
// Drop Segment
//===--------------------------------------------------------------------===//
void ColumnSegment::VisitBlockIds(BlockIdVisitor &visitor) const {
	if (block_id != INVALID_BLOCK) {
		visitor.Visit(block_id);
	}
	if (function.get().visit_block_ids) {
		function.get().visit_block_ids(*this, visitor);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
static SelectionVector &EnsureSelectionCapacity(SelectionVector &selection, idx_t count) {
	if (selection.Capacity() < count) {
		selection.Initialize(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
	}
	return selection;
}

static SelectionVector &GetFastFilterSelection(ExpressionFilterState &state, idx_t count) {
	return EnsureSelectionCapacity(state.fast_filter_sel, count);
}

template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(const UnifiedVectorFormat &vdata, T predicate, const SelectionVector &sel,
                                      const idx_t approved_tuple_count, SelectionVector &result_sel) {
	auto &mask = vdata.validity;
	const auto vec = UnifiedVectorFormat::GetData<const T>(vdata);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		const auto idx = sel.get_index(i);
		auto vector_idx = vdata.sel->get_index(idx);
		bool comparison_result =
		    (!HAS_NULL || mask.RowIsValidUnsafe(vector_idx)) && OP::Operation(vec[vector_idx], predicate);
		result_sel.set_index(result_count, idx);
		result_count += comparison_result;
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type) {
	SelectionVector new_sel(approved_tuple_count);
	auto &mask = vdata.validity;
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(vdata, predicate, sel,
			                                                                          approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThanEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.CannotHaveNull()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(vdata, predicate, sel,
			                                                                             approved_tuple_count, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(vdata, predicate, sel,
			                                                                            approved_tuple_count, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(UnifiedVectorFormat &vdata, SelectionVector &sel, idx_t &approved_tuple_count) {
	auto &mask = vdata.validity;
	if (mask.CannotHaveNull()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto vector_idx = vdata.sel->get_index(idx);
			if (mask.RowIsValid(vector_idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

static idx_t ExecuteExpressionFilterSelection(SelectionVector &sel, Vector &vector, ExpressionExecutor &executor,
                                              idx_t scan_count, idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0) {
		return 0;
	}
	SelectionVector result_sel(approved_tuple_count);
	if (scan_count > STANDARD_VECTOR_SIZE) {
		// scan count is > vector size - split up the vector into multiple chunks
		idx_t offset = 0;
		idx_t result_offset = 0;
		idx_t current_sel_offset = 0;
		SelectionVector current_sel(approved_tuple_count);
		while (offset < scan_count) {
			idx_t chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, scan_count - offset);
			idx_t chunk_end = offset + chunk_count;
			DataChunk chunk;
			chunk.data.emplace_back(vector, offset, chunk_end);

			// construct the relevant selection vector for the current chunk (offset ... offset + chunk_count)
			idx_t current_count = 0;
			for (; current_sel_offset < approved_tuple_count; current_sel_offset++) {
				auto sel_index = sel.get_index(current_sel_offset);
				if (sel_index >= chunk_end) {
					// exhausted the chunk
					break;
				}
				if (sel_index < offset) {
					throw InternalException("sel_index < offset in expression filter");
				}
				current_sel.set_index(current_count++, sel_index - offset);
			}
			if (current_count == 0) {
				// no matching tuples in this chunk
				offset += chunk_count;
				continue;
			}
			auto current_result_data = result_sel.data() + result_offset;
			SelectionVector current_result_sel(current_result_data, result_sel.Capacity() - result_offset);
			idx_t new_matches = executor.SelectExpression(chunk, current_result_sel, current_sel, current_count);
			// increment all matches by the offset
			for (idx_t i = 0; i < new_matches; i++) {
				current_result_data[i] += offset;
			}
			result_offset += new_matches;
			offset += chunk_count;
		}
		approved_tuple_count = result_offset;
	} else {
		// standard case: we can handle everything at once - run the expression once
		DataChunk chunk;
		chunk.data.emplace_back(Vector::Ref(vector));
		chunk.SetChildCardinality(scan_count);
		SelectionVector identity_sel;
		optional_ptr<SelectionVector> current_sel = sel.IsSet() ? &sel : nullptr;
		if (!sel.IsSet() && (vector.GetType().IsNested() || approved_tuple_count != scan_count)) {
			identity_sel = SelectionVector::Incremental(approved_tuple_count);
			current_sel = &identity_sel;
		}
		approved_tuple_count = executor.SelectExpression(chunk, result_sel, current_sel, approved_tuple_count);
	}
	sel.Initialize(result_sel);
	return approved_tuple_count;
}

template <class T, bool HAS_NULL>
static idx_t TemplatedSignedNumericRangeSelection(const UnifiedVectorFormat &vdata, const SelectionVector &sel,
                                                  const idx_t approved_tuple_count, SelectionVector &result_sel,
                                                  const bool has_lower, const int64_t lower, const bool has_upper,
                                                  const int64_t upper) {
	auto &mask = vdata.validity;
	const auto vec = UnifiedVectorFormat::GetData<const T>(vdata);
	const auto lower_value = static_cast<T>(lower);
	const auto upper_value = static_cast<T>(upper);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		const auto idx = sel.get_index(i);
		const auto vector_idx = vdata.sel->get_index(idx);
		const auto value = vec[vector_idx];
		const bool comparison_result = (!HAS_NULL || mask.RowIsValidUnsafe(vector_idx)) &&
		                               (!has_lower || value >= lower_value) && (!has_upper || value <= upper_value);
		if (comparison_result) {
			result_sel.set_index(result_count++, idx);
		}
	}
	return result_count;
}

template <class T>
static void SignedNumericRangeSelectionSwitch(ExpressionFilterState &state, const UnifiedVectorFormat &vdata,
                                              SelectionVector &sel, idx_t &approved_tuple_count, const bool has_lower,
                                              const int64_t lower, const bool has_upper, const int64_t upper) {
	auto &result_sel = GetFastFilterSelection(state, approved_tuple_count);
	auto &mask = vdata.validity;
	if (mask.CannotHaveNull()) {
		approved_tuple_count = TemplatedSignedNumericRangeSelection<T, false>(
		    vdata, sel, approved_tuple_count, result_sel, has_lower, lower, has_upper, upper);
	} else {
		approved_tuple_count = TemplatedSignedNumericRangeSelection<T, true>(
		    vdata, sel, approved_tuple_count, result_sel, has_lower, lower, has_upper, upper);
	}
	sel.Initialize(result_sel);
}

static bool TryFastSignedNumericRangeFilter(SelectionVector &sel, Vector &input_vector, ExpressionFilterState &state,
                                            idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0) {
		return false;
	}
	SignedNumericRangeFilterData range;
	if (!TryGetSignedNumericRange(state, input_vector.GetType(), range)) {
		return false;
	}
	if (range.empty) {
		approved_tuple_count = 0;
		return true;
	}

	UnifiedVectorFormat vdata;
	input_vector.ToUnifiedFormat(vdata);
	switch (input_vector.GetType().InternalType()) {
	case PhysicalType::INT8:
		SignedNumericRangeSelectionSwitch<int8_t>(state, vdata, sel, approved_tuple_count, range.has_lower, range.lower,
		                                          range.has_upper, range.upper);
		return true;
	case PhysicalType::INT16:
		SignedNumericRangeSelectionSwitch<int16_t>(state, vdata, sel, approved_tuple_count, range.has_lower,
		                                           range.lower, range.has_upper, range.upper);
		return true;
	case PhysicalType::INT32:
		SignedNumericRangeSelectionSwitch<int32_t>(state, vdata, sel, approved_tuple_count, range.has_lower,
		                                           range.lower, range.has_upper, range.upper);
		return true;
	case PhysicalType::INT64:
		SignedNumericRangeSelectionSwitch<int64_t>(state, vdata, sel, approved_tuple_count, range.has_lower,
		                                           range.lower, range.has_upper, range.upper);
		return true;
	default:
		return false;
	}
}

static optional_ptr<const SelectivityOptionalFilterFunctionData> TryGetSelectivityOptionalData(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != SelectivityOptionalFilterScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	return &function_expr.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
}

//! Whether the internal filter reads the scanned column directly in the filter's key representation. Filters over a
//! cast of the column (runtime filters across join key types) must stay on the expression-executor path.
static bool InternalFilterInputMatchesTarget(const BoundFunctionExpression &function_expr,
                                             const LogicalType &filter_key_type, const LogicalType &target_type) {
	if (function_expr.GetChildren().size() != 1) {
		return false;
	}
	auto &input = *function_expr.GetChildren()[0];
	return input.GetExpressionClass() == ExpressionClass::BOUND_REF && filter_key_type == target_type &&
	       input.GetReturnType() == target_type;
}

static optional_ptr<const PrefixRangeFunctionData> TryGetPrefixRangeFunctionData(const Expression &expr,
                                                                                 const LogicalType &target_type) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != PrefixRangeScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	auto &data = function_expr.BindInfo()->Cast<PrefixRangeFunctionData>();
	// The native lookup drops NULL keys, so a filter that lets NULLs pass stays on the expression-executor path.
	if (!data.filters_null_values) {
		return nullptr;
	}
	return InternalFilterInputMatchesTarget(function_expr, data.key_type, target_type) ? &data : nullptr;
}

static optional_ptr<const BloomFilterFunctionData> TryGetBloomFilterFunctionData(const Expression &expr,
                                                                                 const LogicalType &target_type) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != BloomFilterScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	auto &data = function_expr.BindInfo()->Cast<BloomFilterFunctionData>();
	if (!data.filter || !data.filter->IsInitialized()) {
		return nullptr;
	}
	// Bloom filters hash the post-cast key representation. Hashing the source representation directly is only
	// equivalent when both physical types match, so cast inputs must remain on the expression-executor path.
	return InternalFilterInputMatchesTarget(function_expr, data.key_type, target_type) ? &data : nullptr;
}

static void ApplyFastSignedNumericRangeOperation(ExpressionFilterState &state, SelectionVector &sel,
                                                 UnifiedVectorFormat &vdata,
                                                 const FastInternalFilterOperation &operation,
                                                 const LogicalType &target_type, idx_t &approved_tuple_count) {
	if (operation.range_empty) {
		approved_tuple_count = 0;
		return;
	}

	switch (target_type.InternalType()) {
	case PhysicalType::INT8:
		SignedNumericRangeSelectionSwitch<int8_t>(state, vdata, sel, approved_tuple_count, operation.range_has_lower,
		                                          operation.range_lower, operation.range_has_upper,
		                                          operation.range_upper);
		return;
	case PhysicalType::INT16:
		SignedNumericRangeSelectionSwitch<int16_t>(state, vdata, sel, approved_tuple_count, operation.range_has_lower,
		                                           operation.range_lower, operation.range_has_upper,
		                                           operation.range_upper);
		return;
	case PhysicalType::INT32:
		SignedNumericRangeSelectionSwitch<int32_t>(state, vdata, sel, approved_tuple_count, operation.range_has_lower,
		                                           operation.range_lower, operation.range_has_upper,
		                                           operation.range_upper);
		return;
	case PhysicalType::INT64:
		SignedNumericRangeSelectionSwitch<int64_t>(state, vdata, sel, approved_tuple_count, operation.range_has_lower,
		                                           operation.range_lower, operation.range_has_upper,
		                                           operation.range_upper);
		return;
	default:
		D_ASSERT(false);
	}
}

static void ApplyFastPrefixRangeFilter(ExpressionFilterState &state, SelectionVector &sel, Vector &input_vector,
                                       const PrefixRangeFunctionData &data, idx_t &approved_tuple_count) {
	if (!data.filter || !data.filter->IsInitialized()) {
		// not published yet: the filter passes every row
		return;
	}
	D_ASSERT(data.filters_null_values);
	if (!sel.IsSet()) {
		// without an input selection the lookup positions are the row ids
		auto &result_sel = GetFastFilterSelection(state, approved_tuple_count);
		const auto result_count = data.filter->LookupKeys(input_vector, result_sel, approved_tuple_count);
		if (result_count != approved_tuple_count) {
			sel.Initialize(result_sel);
			approved_tuple_count = result_count;
		}
		return;
	}
	// the lookup yields positions local to sel; they are strictly increasing and at least their own index, so the
	// translation back to row ids can compact into a buffer that aliases sel
	auto &local_sel = EnsureSelectionCapacity(state.fast_filter_local_sel, approved_tuple_count);
	const auto result_count = data.filter->LookupKeys(input_vector, sel, local_sel, approved_tuple_count);
	if (result_count == approved_tuple_count) {
		return;
	}
	auto &result_sel = GetFastFilterSelection(state, approved_tuple_count);
	for (idx_t i = 0; i < result_count; i++) {
		result_sel.set_index(i, sel.get_index(local_sel.get_index(i)));
	}
	sel.Initialize(result_sel);
	approved_tuple_count = result_count;
}

template <class T>
static void ApplyFastBloomFilterTyped(ExpressionFilterState &state, SelectionVector &sel,
                                      const UnifiedVectorFormat &vdata, const BloomFilterFunctionData &data,
                                      idx_t &approved_tuple_count) {
	auto input_data = UnifiedVectorFormat::GetData<T>(vdata);
	auto &result_sel = GetFastFilterSelection(state, approved_tuple_count);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto row_idx = sel.get_index(i);
		auto input_idx = vdata.sel->get_index(row_idx);
		if (!vdata.validity.RowIsValid(input_idx)) {
			if (!data.filters_null_values) {
				result_sel.set_index(result_count++, row_idx);
			}
			continue;
		}
		if (data.filter->LookupOne(Hash<T>(input_data[input_idx]))) {
			result_sel.set_index(result_count++, row_idx);
		}
	}
	sel.Initialize(result_sel);
	approved_tuple_count = result_count;
}

static void ApplyFastBloomFilter(ExpressionFilterState &state, SelectionVector &sel, const UnifiedVectorFormat &vdata,
                                 const BloomFilterFunctionData &data, const LogicalType &target_type,
                                 idx_t &approved_tuple_count) {
	switch (target_type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ApplyFastBloomFilterTyped<int8_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::INT16:
		ApplyFastBloomFilterTyped<int16_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::INT32:
		ApplyFastBloomFilterTyped<int32_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::INT64:
		ApplyFastBloomFilterTyped<int64_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::UINT8:
		ApplyFastBloomFilterTyped<uint8_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::UINT16:
		ApplyFastBloomFilterTyped<uint16_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::UINT32:
		ApplyFastBloomFilterTyped<uint32_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::UINT64:
		ApplyFastBloomFilterTyped<uint64_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::INT128:
		ApplyFastBloomFilterTyped<hugeint_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::UINT128:
		ApplyFastBloomFilterTyped<uhugeint_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::FLOAT:
		ApplyFastBloomFilterTyped<float>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::DOUBLE:
		ApplyFastBloomFilterTyped<double>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::INTERVAL:
		ApplyFastBloomFilterTyped<interval_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	case PhysicalType::VARCHAR:
		ApplyFastBloomFilterTyped<string_t>(state, sel, vdata, data, approved_tuple_count);
		break;
	default:
		D_ASSERT(false);
		approved_tuple_count = 0;
		break;
	}
}

static bool TryBuildFastInternalFilterOperation(const Expression &expr, const LogicalType &target_type,
                                                FastInternalFilterOperation &operation, bool &exact) {
	if (auto optional_data = TryGetSelectivityOptionalData(expr)) {
		if (!optional_data->child_filter_expr) {
			return false;
		}
		// the wrapper owns the selectivity policy of the single operation it wraps
		FastInternalFilterOperation child_operation {FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE};
		if (!TryBuildFastInternalFilterOperation(*optional_data->child_filter_expr, target_type, child_operation,
		                                         exact) ||
		    child_operation.selectivity) {
			return false;
		}
		child_operation.selectivity =
		    make_uniq<FilterSelectivityState>(optional_data->n_vectors_to_check, optional_data->selectivity_threshold);
		operation = std::move(child_operation);
		return true;
	}

	SignedNumericRangeFilterData range;
	if (TryGetSignedNumericRange(expr, target_type, range)) {
		operation = FastInternalFilterOperation {FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE};
		operation.range_empty = range.empty;
		operation.range_has_lower = range.has_lower;
		operation.range_has_upper = range.has_upper;
		operation.range_lower = range.lower;
		operation.range_upper = range.upper;
		exact = false;
		return true;
	}
	if (auto prefix_data = TryGetPrefixRangeFunctionData(expr, target_type)) {
		operation = FastInternalFilterOperation {FastInternalFilterOperationType::PREFIX_RANGE};
		operation.prefix_range_data = prefix_data;
		exact = false;
		return true;
	}
	if (auto bloom_data = TryGetBloomFilterFunctionData(expr, target_type)) {
		operation = FastInternalFilterOperation {FastInternalFilterOperationType::BLOOM_FILTER};
		operation.bloom_filter_data = bloom_data;
		exact = true;
		return true;
	}
	return false;
}

static void ExtractFastInternalFilterOperations(const Expression &expr, const LogicalType &target_type,
                                                vector<FastInternalFilterOperation> &exact_operations,
                                                vector<FastInternalFilterOperation> &residual_operations,
                                                vector<unique_ptr<Expression>> &residual_expressions) {
	FastInternalFilterOperation operation {FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE};
	bool exact = false;
	if (TryBuildFastInternalFilterOperation(expr, target_type, operation, exact)) {
		(exact ? exact_operations : residual_operations).push_back(std::move(operation));
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			ExtractFastInternalFilterOperations(*child, target_type, exact_operations, residual_operations,
			                                    residual_expressions);
		}
		return;
	}
	residual_expressions.push_back(expr.Copy());
}

static unique_ptr<Expression> BuildFastInternalFilterResidual(vector<unique_ptr<Expression>> expressions) {
	if (expressions.empty()) {
		return nullptr;
	}
	if (expressions.size() == 1) {
		return std::move(expressions[0]);
	}
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &expression : expressions) {
		conjunction->GetChildrenMutable().push_back(std::move(expression));
	}
	return std::move(conjunction);
}

static void BuildFastInternalFilterScanPlan(ExpressionFilterState &state) {
	auto &operations = state.fast_internal_filter_operations;
	if (operations.empty()) {
		return;
	}

	auto &plan = state.fast_internal_filter_scan_plan;
	plan.primary_operation_count = 1;
	if (!operations[0].selectivity) {
		while (plan.primary_operation_count < operations.size() &&
		       operations[plan.primary_operation_count].type == FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE) {
			plan.primary_operation_count++;
		}
	}
}

bool ColumnSegment::PrepareInternalFilterPlan(ExpressionFilterState &state, const Expression &expr,
                                              const LogicalType &target_type) {
	if (state.fast_internal_filter_initialized) {
		return state.fast_internal_filter_supported && state.fast_internal_filter_type == target_type.InternalType();
	}
	state.fast_internal_filter_initialized = true;
	state.fast_internal_filter_type = target_type.InternalType();
	vector<FastInternalFilterOperation> exact_operations;
	vector<FastInternalFilterOperation> residual_operations;
	vector<unique_ptr<Expression>> residual_expressions;
	ExtractFastInternalFilterOperations(expr, target_type, exact_operations, residual_operations, residual_expressions);
	state.fast_internal_filter_supported = !exact_operations.empty() || !residual_operations.empty();
	if (!state.fast_internal_filter_supported) {
		return false;
	}
	state.fast_internal_filter_operations.reserve(exact_operations.size() + residual_operations.size());
	for (auto &operation : exact_operations) {
		state.fast_internal_filter_operations.push_back(std::move(operation));
	}
	for (auto &operation : residual_operations) {
		state.fast_internal_filter_operations.push_back(std::move(operation));
	}
	BuildFastInternalFilterScanPlan(state);
	state.fast_internal_filter_residual_expression = BuildFastInternalFilterResidual(std::move(residual_expressions));
	if (state.fast_internal_filter_residual_expression) {
		state.fast_internal_filter_residual_executor = make_uniq<ExpressionExecutor>(state.GetContext());
		state.fast_internal_filter_residual_executor->AddExpression(*state.fast_internal_filter_residual_expression);
	}
	return true;
}

void ColumnSegment::ApplyInternalFilterPlan(ExpressionFilterState &state, SelectionVector &sel, Vector &input_vector,
                                            UnifiedVectorFormat &vdata, idx_t &approved_tuple_count,
                                            idx_t first_operation, idx_t skipped_operation) {
	D_ASSERT(first_operation <= state.fast_internal_filter_operations.size());
	for (idx_t operation_idx = first_operation; operation_idx < state.fast_internal_filter_operations.size();
	     operation_idx++) {
		if (operation_idx == skipped_operation) {
			continue;
		}
		auto &operation = state.fast_internal_filter_operations[operation_idx];
		if (approved_tuple_count == 0) {
			return;
		}
		if (operation.selectivity && !operation.selectivity->IsActive()) {
			operation.selectivity->Update(0, 0);
			continue;
		}
		const auto input_count = approved_tuple_count;
		switch (operation.type) {
		case FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE:
			ApplyFastSignedNumericRangeOperation(state, sel, vdata, operation, input_vector.GetType(),
			                                     approved_tuple_count);
			break;
		case FastInternalFilterOperationType::PREFIX_RANGE:
			D_ASSERT(operation.prefix_range_data);
			ApplyFastPrefixRangeFilter(state, sel, input_vector, *operation.prefix_range_data, approved_tuple_count);
			break;
		case FastInternalFilterOperationType::BLOOM_FILTER:
			D_ASSERT(operation.bloom_filter_data);
			ApplyFastBloomFilter(state, sel, vdata, *operation.bloom_filter_data, input_vector.GetType(),
			                     approved_tuple_count);
			break;
		default:
			D_ASSERT(false);
		}
		if (operation.selectivity) {
			operation.selectivity->Update(approved_tuple_count, input_count);
		}
	}
}

idx_t ColumnSegment::ApplyInternalFilterResidual(ExpressionFilterState &state, SelectionVector &sel,
                                                 Vector &input_vector, idx_t scan_count, idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0 || !state.fast_internal_filter_residual_expression) {
		return approved_tuple_count;
	}
	if (state.kernel) {
		UnifiedVectorFormat vdata;
		input_vector.ToUnifiedFormat(vdata);
		if (state.kernel->TrySelect(input_vector, vdata, sel, scan_count, approved_tuple_count)) {
			return approved_tuple_count;
		}
	}
	D_ASSERT(state.fast_internal_filter_residual_executor);
	return ExecuteExpressionFilterSelection(sel, input_vector, *state.fast_internal_filter_residual_executor,
	                                        scan_count, approved_tuple_count);
}

static bool TryFastInternalFilterExpression(SelectionVector &sel, Vector &input_vector, ExpressionFilterState &state,
                                            idx_t scan_count, idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0) {
		return false;
	}
	if (!ColumnSegment::PrepareInternalFilterPlan(state, state.expression, input_vector.GetType())) {
		return false;
	}
	UnifiedVectorFormat vdata;
	input_vector.ToUnifiedFormat(vdata);
	ColumnSegment::ApplyInternalFilterPlan(state, sel, input_vector, vdata, approved_tuple_count);
	ColumnSegment::ApplyInternalFilterResidual(state, sel, input_vector, scan_count, approved_tuple_count);
	return true;
}

static bool TryAddStringEqualityConstant(const Expression &expr, vector<string> &constants) {
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	optional_ptr<const BoundConstantExpression> constant;
	if (left.GetExpressionType() == ExpressionType::BOUND_REF &&
	    right.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
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

static optional_ptr<const Expression> TryGetExactOptionalStringFilterChild(const Expression &expr) {
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

static bool TryCollectStringEqualityConstants(const Expression &expr, vector<string> &constants) {
	if (auto child = TryGetExactOptionalStringFilterChild(expr)) {
		return TryCollectStringEqualityConstants(*child, constants);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!TryCollectStringEqualityConstants(*child, constants)) {
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
	return TryAddStringEqualityConstant(expr, constants);
}

static bool GetFastStringEqualityConstants(ExpressionFilterState &state, const vector<string> *&constants) {
	if (!state.fast_string_equality_filter_initialized) {
		state.fast_string_equality_filter_initialized = true;
		state.fast_string_equality_filter_supported =
		    TryCollectStringEqualityConstants(state.expression, state.fast_string_equality_constants) &&
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

static bool TryFastDictionaryStringEqualityFilter(SelectionVector &sel, Vector &input_vector,
                                                  ExpressionFilterState &state, idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0 || input_vector.GetVectorType() != VectorType::DICTIONARY_VECTOR ||
	    input_vector.GetType().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	auto dictionary_size = DictionaryVector::DictionarySize(input_vector);
	if (!dictionary_size.IsValid()) {
		return false;
	}
	auto &dictionary = DictionaryVector::Child(input_vector);
	if (dictionary.GetVectorType() != VectorType::FLAT_VECTOR || dictionary.GetType().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	auto &dict_buffer = input_vector.Buffer().Cast<DictionaryBuffer>();
	auto &dictionary_id = dict_buffer.GetDictionaryId();
	const bool can_cache_matches = !dictionary_id.empty();
	const vector<string> *constants;
	if (!GetFastStringEqualityConstants(state, constants)) {
		return false;
	}

	const auto dict_count = dictionary_size.GetIndex();
	auto &dictionary_matches = state.fast_dictionary_matches;
	const bool cache_hit = can_cache_matches && state.fast_dictionary_matches_count == dict_count &&
	                       state.fast_dictionary_matches_dictionary_id == dictionary_id;
	if (!cache_hit) {
		auto dict_data = FlatVector::GetData<string_t>(dictionary);
		auto &dict_validity = FlatVector::Validity(dictionary);
		if (dictionary_matches.size() < dict_count) {
			dictionary_matches.resize(dict_count);
		}
		std::fill(dictionary_matches.begin(), dictionary_matches.begin() + dict_count, 0);
		for (idx_t dict_idx = 0; dict_idx < dict_count; dict_idx++) {
			if (!dict_validity.RowIsValid(dict_idx)) {
				continue;
			}
			auto &dict_value = dict_data[dict_idx];
			for (auto &constant : *constants) {
				string_t constant_value(constant);
				if (string_t::StringComparisonOperators::Equals(dict_value, constant_value)) {
					dictionary_matches[dict_idx] = 1;
					break;
				}
			}
		}
		state.fast_dictionary_matches_dictionary_id = can_cache_matches ? dictionary_id : string();
		state.fast_dictionary_matches_count = can_cache_matches ? dict_count : 0;
	}

	auto &dict_sel = DictionaryVector::SelVector(input_vector);
	auto &result_sel = GetFastFilterSelection(state, approved_tuple_count);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		const auto row_idx = sel.get_index(i);
		const auto dict_idx = dict_sel.get_index(row_idx);
		if (dict_idx < dict_count && dictionary_matches[dict_idx]) {
			result_sel.set_index(result_count++, row_idx);
		}
	}
	sel.Initialize(result_sel);
	approved_tuple_count = result_count;
	return true;
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                     const TableFilter &filter, TableFilterState &filter_state, idx_t scan_count,
                                     idx_t &approved_tuple_count) {
	(void)vdata;
	return FilterSelection(sel, vector, filter_state, scan_count, approved_tuple_count);
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &vector, TableFilterState &filter_state,
                                     idx_t scan_count, idx_t &approved_tuple_count) {
	auto &state = filter_state.Cast<ExpressionFilterState>();
	// native storage fast paths first, then a backend-compiled kernel, then the generic fast executor and finally the
	// canonical expression executor
	if (TryFastSignedNumericRangeFilter(sel, vector, state, approved_tuple_count)) {
		return approved_tuple_count;
	}
	if (TryFastInternalFilterExpression(sel, vector, state, scan_count, approved_tuple_count)) {
		return approved_tuple_count;
	}
	if (TryFastDictionaryStringEqualityFilter(sel, vector, state, approved_tuple_count)) {
		return approved_tuple_count;
	}
	if (state.kernel) {
		UnifiedVectorFormat vdata;
		vector.ToUnifiedFormat(vdata);
		if (state.kernel->TrySelect(vector, vdata, sel, scan_count, approved_tuple_count)) {
			return approved_tuple_count;
		}
	}
	if (state.fast_executor && scan_count <= STANDARD_VECTOR_SIZE) {
		return state.fast_executor->FilterSelection(sel, vector, scan_count, approved_tuple_count);
	}
	D_ASSERT(state.executor);
	return ExecuteExpressionFilterSelection(sel, vector, *state.executor, scan_count, approved_tuple_count);
}

const CompressionFunction &ColumnSegment::GetCompressionFunction() {
	return function.get();
}

} // namespace duckdb
