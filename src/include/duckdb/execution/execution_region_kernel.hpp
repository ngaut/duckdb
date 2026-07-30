//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_kernel.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <atomic>

namespace duckdb {

class Allocator;
class ExecutionRegionRuntime;

class DUCKDB_API ExecutionRegionLocalState {
public:
	virtual ~ExecutionRegionLocalState();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class DUCKDB_API ExecutionRegionCodeHandle {
public:
	virtual ~ExecutionRegionCodeHandle();

	virtual idx_t CodeSize() const;
};

//! The lifecycle of one pipeline's measured runner decision. One thread at a time
//! owns a measurement leg; the verdict, once published, is final for the pipeline.
enum class ExecutionRegionAdaptiveAbPhase : uint8_t {
	UNDECIDED,
	MEASURING_NATIVE,
	MEASURING_COMPILED,
	MEASURING_COMPILED_RUNNING,
	COMMIT_COMPILED,
	FALLBACK_NATIVE
};
// MEASURING_NATIVE doubles as the running state: the executor that holds the
// vectorized claim budget owns the leg and resumes it across scheduler yields;
// every other executor runs natively as a bystander.

enum class ExecutionRegionAdaptiveAbVerdict : uint8_t { COMMIT_COMPILED, FALLBACK_NATIVE };

//! The measured-runner state machine. This struct is the single transition
//! authority: every phase store lives in a method here, next to the atomics it
//! guards, so verdict policy cannot be split across the layers that feed it.
struct ExecutionRegionAdaptiveAbState {
	std::atomic<ExecutionRegionAdaptiveAbPhase> phase {ExecutionRegionAdaptiveAbPhase::UNDECIDED};
	std::atomic<int64_t> compiled_leg_us {0};
	std::atomic<int64_t> native_leg_us {0};
	//! Rows each leg processed, so verdict analysis can normalize for short tail
	//! row groups and quantify cold-first-leg bias from the recorded events.
	std::atomic<idx_t> compiled_leg_rows {0};
	std::atomic<idx_t> native_leg_rows {0};
	std::atomic<const void *> compiled_leg_owner {nullptr};

	bool TryBeginPhase(ExecutionRegionAdaptiveAbPhase expected, ExecutionRegionAdaptiveAbPhase next) {
		return phase.compare_exchange_strong(expected, next);
	}

	bool TryBeginCompiledLeg(const void *owner) {
		D_ASSERT(owner);
		const void *expected_owner = nullptr;
		if (!compiled_leg_owner.compare_exchange_strong(expected_owner, owner, std::memory_order_relaxed)) {
			return false;
		}
		auto expected = ExecutionRegionAdaptiveAbPhase::MEASURING_COMPILED;
		if (!phase.compare_exchange_strong(expected, ExecutionRegionAdaptiveAbPhase::MEASURING_COMPILED_RUNNING,
		                                   std::memory_order_release, std::memory_order_relaxed)) {
			compiled_leg_owner.store(nullptr, std::memory_order_relaxed);
			return false;
		}
		return true;
	}

	bool CompiledLegOwnedBy(const void *owner) const {
		if (phase.load(std::memory_order_acquire) != ExecutionRegionAdaptiveAbPhase::MEASURING_COMPILED_RUNNING) {
			return false;
		}
		return compiled_leg_owner.load(std::memory_order_relaxed) == owner;
	}

	//! The native leg accumulates across scheduler yields; rows are the leg total.
	void RecordNativeLeg(int64_t leg_us, idx_t leg_rows) {
		native_leg_us.fetch_add(leg_us);
		native_leg_rows.store(leg_rows);
	}

	void RecordCompiledLeg(int64_t leg_us, idx_t leg_rows) {
		compiled_leg_us.fetch_add(leg_us);
		compiled_leg_rows.fetch_add(leg_rows);
	}

	//! The native leg ended at its declined boundary: the compiled leg may begin.
	void AdvanceToCompiledLeg() {
		phase.store(ExecutionRegionAdaptiveAbPhase::MEASURING_COMPILED);
	}

	//! The native leg finished or blocked before reaching a boundary: there is
	//! nothing to compare against, stay native.
	void ResolveFallbackFromNativeLeg() {
		phase.store(ExecutionRegionAdaptiveAbPhase::FALLBACK_NATIVE);
	}

	//! The compiled leg reached its first row-group boundary: decide the verdict.
	ExecutionRegionAdaptiveAbVerdict ResolveVerdictAtBoundary(int64_t compiled_us, idx_t compiled_rows,
	                                                          idx_t margin_basis_points) {
		RecordCompiledLeg(compiled_us, compiled_rows);
		compiled_us = compiled_leg_us.load();
		compiled_rows = compiled_leg_rows.load();
		const auto native_us = native_leg_us.load();
		const auto native_rows = native_leg_rows.load();
		// Empty-output row groups do not provide a rate sample. Keep the static CBO
		// selection instead of manufacturing an infinite or zero per-row cost.
		bool commit_compiled = compiled_rows == 0 || native_rows == 0;
		if (!commit_compiled) {
			// The complete UBIGINT margin domain needs up to 193 bits after
			// cross-multiplying two 64-bit rate terms. Keep the comparison exact
			// with four little-endian limbs instead of narrowing the setting or
			// silently wrapping a 128-bit intermediate.
			struct WideCost {
				uint64_t words[4] {};

				explicit WideCost(uhugeint_t value) {
					words[0] = value.lower;
					words[1] = value.upper;
				}

				void Multiply(uint64_t factor) {
					uint64_t carry = 0;
					for (idx_t word_idx = 0; word_idx < 4; word_idx++) {
						auto product = uhugeint_t(words[word_idx]) * uhugeint_t(factor) + uhugeint_t(carry);
						words[word_idx] = product.lower;
						carry = product.upper;
					}
					D_ASSERT(carry == 0);
				}

				bool operator<=(const WideCost &other) const {
					for (idx_t word_idx = 4; word_idx > 0; word_idx--) {
						if (words[word_idx - 1] != other.words[word_idx - 1]) {
							return words[word_idx - 1] < other.words[word_idx - 1];
						}
					}
					return true;
				}
			};

			D_ASSERT(compiled_us >= 0);
			D_ASSERT(native_us >= 0);
			WideCost compiled_cost(uhugeint_t(margin_basis_points) + uhugeint_t(10000));
			compiled_cost.Multiply(static_cast<uint64_t>(compiled_us));
			compiled_cost.Multiply(native_rows);
			WideCost native_cost(uhugeint_t(10000));
			native_cost.Multiply(static_cast<uint64_t>(native_us));
			native_cost.Multiply(compiled_rows);
			commit_compiled = compiled_cost <= native_cost;
		}
		phase.store(commit_compiled ? ExecutionRegionAdaptiveAbPhase::COMMIT_COMPILED
		                            : ExecutionRegionAdaptiveAbPhase::FALLBACK_NATIVE);
		return commit_compiled ? ExecutionRegionAdaptiveAbVerdict::COMMIT_COMPILED
		                       : ExecutionRegionAdaptiveAbVerdict::FALLBACK_NATIVE;
	}

	//! The compiled leg finished or could not enter before reaching a
	//! boundary: keep the CBO's compiled selection.
	void ResolveCommitWithoutBoundary() {
		auto expected = ExecutionRegionAdaptiveAbPhase::MEASURING_COMPILED_RUNNING;
		phase.compare_exchange_strong(expected, ExecutionRegionAdaptiveAbPhase::COMMIT_COMPILED);
	}
};

class DUCKDB_API ExecutionRegionKernel : public TableFilterKernelProvider {
public:
	virtual ~ExecutionRegionKernel();

	ExecutionRegionAdaptiveAbState &AdaptiveAb() {
		return adaptive_ab;
	}

	//! Whether the planner judged this pipeline's static selection thin enough to be
	//! worth a measured verdict; confident selections skip the measurement tax.
	void SetAdaptiveMeasurementCandidate(bool candidate) {
		adaptive_measurement_candidate = candidate;
	}
	bool AdaptiveMeasurementCandidate() const {
		return adaptive_measurement_candidate;
	}

	virtual const string &BackendName() const = 0;
	virtual idx_t CodeSize() const;
	virtual bool HasExecutableBody() const;
	virtual unique_ptr<ExecutionRegionLocalState> CreateLocalState(Allocator &allocator) const;
	void SetTraceInfo(idx_t trace_id, ExecutionRegionExecutionMode execution_mode, string compile_reason,
	                  int64_t compile_time_us, idx_t code_size);
	void AddTraceCodeSize(idx_t code_size);
	void SetTraceSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution);
	void SetTraceScanFilterMode(ExecutionRegionScanFilterMode scan_filter_mode);
	void SetTracePipeline(const ExecutionRegionCandidate &candidate, const string &pipeline_shape);
	idx_t TraceId() const;
	ExecutionRegionExecutionMode ExecutionMode() const;
	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const;
	bool UsesScanFilters() const;
	const string &TraceCompileReason() const;
	int64_t TraceCompileTime() const;
	idx_t TraceCodeSize() const;
	bool HasTracePipeline() const;
	const string &TraceCandidateShape() const;
	const string &TracePipelineShape() const;
	idx_t TraceCandidateEstimatedCardinality() const;
	//! Whether this kernel may hand the pipeline to the vectorized continuation
	//! mid-query. Recipes that claim exclusive ownership of sink finalization
	//! (for example inline distinct-key counting) must refuse: a handoff strands
	//! the rows the other runner sinks under a claim it cannot see.
	virtual bool SupportsRunnerHandoff() const;
	virtual bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) = 0;

private:
	idx_t trace_id = 0;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionScanFilterMode scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
	string trace_compile_reason;
	int64_t trace_compile_time_us = 0;
	idx_t trace_code_size = 0;
	std::atomic<idx_t> trace_lazy_code_size {0};
	bool has_trace_pipeline = false;
	string trace_candidate_shape;
	string trace_pipeline_shape;
	idx_t trace_candidate_estimated_cardinality = 0;
	ExecutionRegionAdaptiveAbState adaptive_ab;
	bool adaptive_measurement_candidate = true;
};

} // namespace duckdb
