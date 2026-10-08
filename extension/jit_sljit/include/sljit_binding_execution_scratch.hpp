//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_binding_execution_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_scratch_access.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitBindingExecutionScratch {
	void Resize(idx_t count) {
		operator_bindings.resize(count);
		operator_binding_ready.resize(count);
		sink_bindings.resize(count);
		sink_binding_ready.resize(count);
		output_operator_bindings.resize(count);
		output_operator_binding_ready.resize(count);
	}

	bool HasSink(idx_t op_idx) const {
		return op_idx < sink_binding_ready.size() && sink_binding_ready[op_idx];
	}

	ExecutionSinkBinding &Sink(idx_t op_idx) {
		return SljitCheckedScratchSlot(sink_bindings, op_idx, "SLJIT full pipeline sink has no binding scratch");
	}

	void MarkSinkReady(idx_t op_idx) {
		if (op_idx >= sink_binding_ready.size()) {
			throw InternalException("SLJIT full pipeline sink has no binding-ready scratch");
		}
		sink_binding_ready[op_idx] = true;
	}

	bool HasOperator(idx_t op_idx) const {
		return op_idx < operator_binding_ready.size() && operator_binding_ready[op_idx];
	}

	ExecutionOperatorBinding &Operator(idx_t op_idx) {
		return SljitCheckedScratchSlot(operator_bindings, op_idx,
		                               "SLJIT full pipeline operator has no binding scratch");
	}

	void MarkOperatorReady(idx_t op_idx) {
		if (op_idx >= operator_binding_ready.size()) {
			throw InternalException("SLJIT full pipeline operator has no binding-ready scratch");
		}
		operator_binding_ready[op_idx] = true;
	}

	//! A second binding of an operator whose output consumers index a different input layout than the one the
	//! operator itself executes on (a hash join probing the source of an elided pre-join projection)
	bool HasOutputOperator(idx_t op_idx) const {
		return op_idx < output_operator_binding_ready.size() && output_operator_binding_ready[op_idx];
	}

	ExecutionOperatorBinding &OutputOperator(idx_t op_idx) {
		return SljitCheckedScratchSlot(output_operator_bindings, op_idx,
		                               "SLJIT full pipeline operator has no output binding scratch");
	}

	void MarkOutputOperatorReady(idx_t op_idx) {
		if (op_idx >= output_operator_binding_ready.size()) {
			throw InternalException("SLJIT full pipeline operator has no output binding-ready scratch");
		}
		output_operator_binding_ready[op_idx] = true;
	}

private:
	vector<ExecutionOperatorBinding> operator_bindings;
	vector<bool> operator_binding_ready;
	vector<ExecutionOperatorBinding> output_operator_bindings;
	vector<bool> output_operator_binding_ready;
	vector<ExecutionSinkBinding> sink_bindings;
	vector<bool> sink_binding_ready;
};

} // namespace duckdb
