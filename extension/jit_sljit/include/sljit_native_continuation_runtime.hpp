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

#include <algorithm>
#include <array>

namespace duckdb {

//! Measures whether the region or DuckDB's operators run a class of a pipeline's chunks faster. The legs alternate
//! chunk by chunk, so both see the pipeline's state warm up alike (the first chunks of an aggregate touch most of its
//! groups for the first time). Each chunk is timed from one source chunk to the next, without the source's fetch: the
//! fetch is the same for either leg and holds the waits for the scan's lock, which would bury the legs' difference.
//! The first chunk of each leg, which warms its code, is left out. Each of DuckDB's chunks is compared with the
//! generated chunk before it, which ran on the same kind of core unless the thread just moved, and the median of those
//! ratios decides, so a chunk whose thread was preempted or moved to a slower core sways only its own pair. DuckDB's
//! operators keep the class's remaining chunks only when they were faster beyond the measurement's noise
class SljitNativeLegMeasurement {
public:
	static constexpr idx_t LEG_CHUNKS = 16;

	//! Whether DuckDB's operators take the class's next chunk
	bool TakesNative() {
		const auto chunk = chunk_count++;
		if (chunk < 2 * LEG_CHUNKS) {
			return IsNativeChunk(chunk);
		}
		if (!decided) {
			decided = true;
			if (pair_count != 0) {
				std::nth_element(native_ratios.begin(), native_ratios.begin() + pair_count / 2,
				                 native_ratios.begin() + pair_count);
				native = native_ratios[pair_count / 2] * NOISE_PERCENT_DENOMINATOR <
				         RATIO_SCALE * (NOISE_PERCENT_DENOMINATOR - NOISE_PERCENT);
			}
		}
		return native;
	}

	//! The time and rows of the class's latest chunk
	void Attribute(int64_t nanos, idx_t rows) {
		const auto chunk = chunk_count - 1;
		if (chunk < 2 || chunk >= 2 * LEG_CHUNKS || rows == 0) {
			return;
		}
		const auto nanos_per_row = std::max<int64_t>(nanos, 1) * RATIO_SCALE / NumericCast<int64_t>(rows);
		if (!IsNativeChunk(chunk)) {
			generated_chunk = chunk;
			generated_nanos_per_row = nanos_per_row;
			return;
		}
		if (generated_chunk + 1 == chunk) {
			native_ratios[pair_count++] = nanos_per_row * RATIO_SCALE / std::max<int64_t>(generated_nanos_per_row, 1);
		}
	}

	bool Decided() const {
		return decided;
	}

private:
	static bool IsNativeChunk(idx_t chunk) {
		return chunk % 2 == 1;
	}

private:
	static constexpr int64_t NOISE_PERCENT = 3;
	static constexpr int64_t NOISE_PERCENT_DENOMINATOR = 100;
	//! fixed point for the times per row and their ratios
	static constexpr int64_t RATIO_SCALE = 1024;

	idx_t chunk_count = 0;
	idx_t generated_chunk = 0;
	int64_t generated_nanos_per_row = 0;
	std::array<int64_t, LEG_CHUNKS> native_ratios;
	idx_t pair_count = 0;
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

//! Which chunks of a step DuckDB's operators take: a region that may hand over decides on its first chunk, or on the
//! first chunk after its own batches decided when the first chunk left the decision pending, and a measured one
//! measures the chunks whose zone maps prove the source's filters apart from the rest, since the region runs them
//! through different steps. FLUSH sinks every row the step still holds before DuckDB's operators take a chunk after the
//! step ran one
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

	//! Whether the next chunk is still measured: the source then times its fetch
	bool TimesSourceChunks() const {
		if (!continuation || !measured) {
			return false;
		}
		// a class no chunk reached yet keeps no measurement open
		return (!seen_class[0] && !seen_class[1]) || (seen_class[0] && !measurements[0].Decided()) ||
		       (seen_class[1] && !measurements[1].Decided());
	}

	//! HANDS_OVER decides an unmeasured step on its first chunk; when it leaves the decision PENDING, PENDING_HANDOVER
	//! reports the step's own decision before each later chunk. READ_INPUT reads every column of a chunk that
	//! DuckDB's operators take, or that HANDS_OVER looks at
	template <class HANDS_OVER, class PENDING_HANDOVER, class FLUSH, class READ_INPUT>
	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, bool source_filters_hold, int64_t source_nanos,
	                                          HANDS_OVER &&hands_over, PENDING_HANDOVER &&pending_handover,
	                                          FLUSH &&flush, READ_INPUT &&read_input) {
		if (!continuation) {
			return nullptr;
		}
		if (!measured) {
			if (handover == SljitNativeHandover::PENDING) {
				if (asked) {
					handover = pending_handover();
					if (handover == SljitNativeHandover::NATIVE) {
						flush();
					}
				} else {
					asked = true;
					read_input();
					handover = hands_over(continuation->Plan());
				}
			}
			switch (handover) {
			case SljitNativeHandover::PENDING:
				return nullptr;
			case SljitNativeHandover::GENERATED:
				continuation.reset();
				return nullptr;
			case SljitNativeHandover::NATIVE:
				break;
			}
			read_input();
			return &continuation->Prepare(runtime, input, source_filters_hold);
		}
		const bool native = TakesNativeMeasured(input.size(), source_filters_hold, source_nanos);
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
	bool TakesNativeMeasured(idx_t rows, bool source_filters_hold, int64_t source_nanos) {
		auto &measurement = measurements[source_filters_hold];
		if (has_previous || TimesSourceChunks()) {
			const auto now = ExecutionClockNanos();
			if (has_previous) {
				// the window ran the previous chunk's leg and fetched this chunk, whose fetch is left out
				measurements[previous_filters_hold].Attribute(now - previous_start - source_nanos, previous_rows);
			}
			has_previous = !measurement.Decided();
			previous_start = now;
			previous_rows = rows;
			previous_filters_hold = source_filters_hold;
		}
		seen_class[source_filters_hold] = true;
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
	bool asked = false;
	SljitNativeHandover handover = SljitNativeHandover::PENDING;
	//! Per chunk class: the chunks whose zone maps prove the source's filters, and the rest
	std::array<SljitNativeLegMeasurement, 2> measurements;
	std::array<bool, 2> seen_class {{false, false}};
	bool has_previous = false;
	int64_t previous_start = 0;
	idx_t previous_rows = 0;
	bool previous_filters_hold = false;
	bool previous_native = false;
};

} // namespace duckdb
