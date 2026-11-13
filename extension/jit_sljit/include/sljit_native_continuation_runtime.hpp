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

namespace duckdb {

//! Measures whether the region or DuckDB's operators run a pipeline's chunks faster: a leg of chunks each, timed from
//! one source chunk to the next so both legs pay the scan alike, without the first chunk of a leg, which warms it.
//! DuckDB's operators keep the remaining chunks only when they were faster beyond the measurement's noise
class SljitNativeLegMeasurement {
public:
	static constexpr idx_t LEG_CHUNKS = 16;

	//! Whether DuckDB's operators take the next chunk, of the given rows
	bool TakesNative(idx_t rows) {
		const auto now = std::chrono::steady_clock::now();
		if (chunk_count != 0) {
			Attribute(chunk_count - 1, now - previous_start, previous_rows);
		}
		previous_start = now;
		previous_rows = rows;
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

	bool Decided() const {
		return decided;
	}

	bool InNativeLeg() const {
		return !decided && chunk_count > LEG_CHUNKS;
	}

private:
	static constexpr int64_t NOISE_PERCENT = 3;
	static constexpr int64_t NOISE_PERCENT_DENOMINATOR = 100;

	void Attribute(idx_t chunk, std::chrono::steady_clock::duration duration, idx_t rows) {
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

	idx_t chunk_count = 0;
	std::chrono::steady_clock::time_point previous_start;
	idx_t previous_rows = 0;
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

	DataChunk &Prepare(ExecutionRegionRuntime &runtime, DataChunk &input) {
		const auto source_rows = input.size();
		reference<DataChunk> current(input);
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.Kind() == SljitNativeRegionOpKind::FILTER) {
				const auto count = current.get().size();
				const auto selected =
				    SljitSelectFilter(op, current.get(), selection, adapters[op_idx][0], nullptr, count, &runtime);
				if (selected != count) {
					current.get().Slice(selection, selected);
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
//! measured one alternates a leg of chunks each before keeping the faster. FLUSH sinks every row the step still holds
//! before DuckDB's operators take a chunk
class SljitNativeContinuationDecision {
public:
	SljitNativeContinuationDecision(ExecutionRegionRuntime &runtime_p,
	                                optional_ptr<SljitExecutableNativeContinuation> plan)
	    : runtime(runtime_p) {
		if (!plan) {
			return;
		}
		continuation = make_uniq<SljitNativeContinuation>(*plan);
		if (plan->measured) {
			// a measured step decides by its measurement, never on its first chunk
			decided = true;
			if (runtime.MeasureNativeLegs()) {
				measurement = make_uniq<SljitNativeLegMeasurement>();
			} else {
				continuation.reset();
			}
		}
	}

	//! HANDS_OVER decides an unmeasured step on its first chunk
	template <class HANDS_OVER, class FLUSH>
	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, HANDS_OVER &&hands_over, FLUSH &&flush) {
		if (!continuation) {
			return nullptr;
		}
		if (!measurement && !decided) {
			decided = true;
			if (!hands_over(continuation->Plan())) {
				continuation.reset();
				return nullptr;
			}
		}
		if (measurement && !TakesNativeMeasured(input, flush)) {
			return nullptr;
		}
		return &continuation->Prepare(runtime, input);
	}

private:
	template <class FLUSH>
	bool TakesNativeMeasured(DataChunk &input, FLUSH &flush) {
		const bool was_native = measurement->InNativeLeg();
		const bool native = measurement->TakesNative(input.size());
		if (native && !was_native) {
			flush();
		}
		if (measurement->Decided()) {
			if (runtime.TraceRuntime()) {
				runtime.RecordJitRuntimePath(native ? "native_leg_measurement.native"
				                                    : "native_leg_measurement.generated");
			}
			measurement.reset();
			if (!native) {
				continuation.reset();
			}
		}
		return native;
	}

	ExecutionRegionRuntime &runtime;
	unique_ptr<SljitNativeContinuation> continuation;
	unique_ptr<SljitNativeLegMeasurement> measurement;
	bool decided = false;
};

} // namespace duckdb
