//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_continuation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_runtime_batch_runtime.hpp"

#include <array>
#include <chrono>

namespace duckdb {

//! Measures whether the region or DuckDB's operators run a class of a pipeline's chunks faster: a leg of chunks each,
//! timed from one source chunk to the next so both legs pay the scan alike, without the first chunk of a leg, which
//! warms it. DuckDB's operators keep the class's remaining chunks only when they were faster beyond the measurement's
//! noise
class SljitNativeLegMeasurement {
public:
	static constexpr idx_t LEG_CHUNKS = 16;

	//! Whether DuckDB's operators take the class's next chunk
	bool TakesNative() {
		const auto chunk = chunk_count++;
		if (chunk < LEG_CHUNKS) {
			return false;
		}
		if (chunk < 2 * LEG_CHUNKS) {
			return true;
		}
		if (!decided) {
			decided = true;
			native = generated_rows != 0 && native_rows != 0 &&
			         native_nanos * generated_rows * NOISE_PERCENT_DENOMINATOR <
			             generated_nanos * native_rows * (NOISE_PERCENT_DENOMINATOR - NOISE_PERCENT);
		}
		return native;
	}

	//! The time and rows of the class's latest chunk
	void Attribute(std::chrono::steady_clock::duration duration, idx_t rows) {
		const auto chunk = chunk_count - 1;
		if (chunk == 0 || chunk == LEG_CHUNKS || chunk >= 2 * LEG_CHUNKS) {
			return;
		}
		const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
		if (chunk < LEG_CHUNKS) {
			generated_nanos += nanos;
			generated_rows += NumericCast<int64_t>(rows);
		} else {
			native_nanos += nanos;
			native_rows += NumericCast<int64_t>(rows);
		}
	}

	bool Decided() const {
		return decided;
	}

private:
	static constexpr int64_t NOISE_PERCENT = 3;
	static constexpr int64_t NOISE_PERCENT_DENOMINATOR = 100;

	idx_t chunk_count = 0;
	int64_t generated_nanos = 0;
	int64_t generated_rows = 0;
	int64_t native_nanos = 0;
	int64_t native_rows = 0;
	bool decided = false;
	bool native = false;
};

//! Turns the chunks a region fetches through its source contract into the chunks DuckDB's scan would have produced,
//! with the source's own filters and layout repair, once the region handed its pipeline to DuckDB's operators
class SljitNativeContinuation {
public:
	explicit SljitNativeContinuation(SljitExecutableNativeContinuation &plan_p)
	    : plan(plan_p), ops(plan_p.source_ops), adapters(ops.size()) {
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			adapters[op_idx].resize(op.Kind() == SljitNativeRegionOpKind::PROJECTION ? op.Projections().size() : 1);
		}
	}

	//! A chunk whose zone maps prove the source's filters for every row passes them unfiltered
	DataChunk &Prepare(ExecutionRegionRuntime &runtime, DataChunk &input, bool source_filters_hold) {
		const auto source_rows = input.size();
		reference<DataChunk> current(input);
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.Kind() == SljitNativeRegionOpKind::FILTER) {
				if (source_filters_hold) {
					continue;
				}
				const auto count = current.get().size();
				const auto selected =
				    SljitSelectFilter(op, current.get(), selection, adapters[op_idx][0], nullptr, count, &runtime);
				if (selected == count) {
					continue;
				}
				// Gathering costs every kept row while a slice costs the same for any count: a filter that keeps at
				// most half of the rows hands them over gathered
				if (selected * 2 > count) {
					current.get().Slice(selection, selected);
				} else {
					auto &output = Output(runtime, op_idx);
					Compact(current.get(), selected, output);
					current = output;
				}
				continue;
			}
			D_ASSERT(op.Kind() == SljitNativeRegionOpKind::PROJECTION);
			auto &output = Output(runtime, op_idx);
			output.Reset();
			SljitExecuteProjection(runtime, *this, op_idx, op, current.get(), output);
			current = output;
		}
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimePath("source.native_continuation", current.get().size());
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::NATIVE_CONTINUATION, source_rows);
		}
		return current.get();
	}

	const SljitExecutableNativeContinuation &Plan() const {
		return plan;
	}

	SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t col_idx) {
		return adapters[op_idx][col_idx];
	}

private:
	//! The rows the filter selected, as DuckDB's scan hands a region source the rows its filters keep: a flat
	//! fixed-width column holds just those rows, any other column keeps its entries under the selection
	void Compact(DataChunk &input, idx_t selected, DataChunk &output) {
		output.Reset();
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto &source = input.data[col_idx];
			auto &target = output.data[col_idx];
			const auto physical_type = source.GetType().InternalType();
			if (source.GetVectorType() != VectorType::FLAT_VECTOR || !TypeIsConstantSize(physical_type)) {
				target.Slice(source, selection, selected);
			} else if (FlatVector::Validity(source).AllValid()) {
				UnifiedVectorFormat source_format;
				source.ToUnifiedFormat(source_format);
				source_format.sel = &selection;
				SljitCopyFixedWidthUnifiedAllValid(source_format, FlatVector::GetDataMutable(target), selected,
				                                   GetTypeIdSize(physical_type));
			} else {
				VectorOperations::Copy(source, target, selection, selected, 0, 0);
			}
		}
		output.SetChildCardinality(selected);
	}

	DataChunk &Output(ExecutionRegionRuntime &runtime, idx_t op_idx) {
		if (outputs.size() <= op_idx) {
			outputs.resize(op_idx + 1);
		}
		if (!outputs[op_idx]) {
			outputs[op_idx] = make_uniq<DataChunk>();
			outputs[op_idx]->Initialize(runtime.GetAllocator(), ops[op_idx].output_types);
		}
		return *outputs[op_idx];
	}

private:
	SljitExecutableNativeContinuation &plan;
	vector<SljitExecutableRegionOp> &ops;
	vector<vector<SljitExpressionAdapterScratch>> adapters;
	vector<unique_ptr<DataChunk>> outputs;
	SelectionVector selection {STANDARD_VECTOR_SIZE};
};

//! Which chunks of a step DuckDB's operators take: a region that may hand over decides once on its first chunk, and a
//! measured one measures the chunks whose zone maps prove the source's filters apart from the rest, since the region
//! runs them through different steps. FLUSH sinks every row the step still holds before DuckDB's operators take a
//! chunk after the step ran one
class SljitNativeContinuationDecision {
public:
	SljitNativeContinuationDecision(ExecutionRegionRuntime &runtime_p,
	                                optional_ptr<SljitExecutableNativeContinuation> plan)
	    : runtime(runtime_p) {
		if (!plan) {
			return;
		}
		continuation = make_uniq<SljitNativeContinuation>(*plan);
		measured = plan->measured;
		if (measured && !runtime.MeasureNativeLegs()) {
			continuation.reset();
		}
	}

	//! HANDS_OVER decides an unmeasured step on its first chunk. READ_INPUT reads every column of a chunk that
	//! DuckDB's operators take, or that HANDS_OVER looks at
	template <class HANDS_OVER, class FLUSH, class READ_INPUT>
	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, bool source_filters_hold, HANDS_OVER &&hands_over,
	                                          FLUSH &&flush, READ_INPUT &&read_input) {
		if (!continuation) {
			return nullptr;
		}
		if (!measured) {
			if (!decided) {
				decided = true;
				read_input();
				if (!hands_over(continuation->Plan())) {
					continuation.reset();
					return nullptr;
				}
			}
			read_input();
			return &continuation->Prepare(runtime, input, source_filters_hold);
		}
		const bool native = TakesNativeMeasured(input.size(), source_filters_hold);
		if (native && !previous_native) {
			flush();
		}
		previous_native = native;
		if (!native) {
			return nullptr;
		}
		read_input();
		return &continuation->Prepare(runtime, input, source_filters_hold);
	}

private:
	bool TakesNativeMeasured(idx_t rows, bool source_filters_hold) {
		auto &measurement = measurements[source_filters_hold];
		if (!measurements[0].Decided() || !measurements[1].Decided()) {
			const auto now = std::chrono::steady_clock::now();
			if (has_previous) {
				measurements[previous_filters_hold].Attribute(now - previous_start, previous_rows);
			}
			has_previous = true;
			previous_start = now;
			previous_rows = rows;
			previous_filters_hold = source_filters_hold;
		}
		const bool was_decided = measurement.Decided();
		const bool native = measurement.TakesNative();
		if (!was_decided && measurement.Decided() && runtime.TraceRuntime()) {
			runtime.RecordJitRuntimePath(native ? "native_leg_measurement.native" : "native_leg_measurement.generated");
		}
		return native;
	}

	ExecutionRegionRuntime &runtime;
	unique_ptr<SljitNativeContinuation> continuation;
	bool measured = false;
	bool decided = false;
	//! Per chunk class: the chunks whose zone maps prove the source's filters, and the rest
	std::array<SljitNativeLegMeasurement, 2> measurements;
	bool has_previous = false;
	std::chrono::steady_clock::time_point previous_start;
	idx_t previous_rows = 0;
	bool previous_filters_hold = false;
	bool previous_native = false;
};

} // namespace duckdb
