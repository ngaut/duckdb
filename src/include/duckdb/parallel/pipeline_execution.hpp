//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_execution.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! The result of executing a pipeline runner.
enum class PipelineExecuteResult {
	//! The pipeline is fully executed: the source is completely exhausted and finalization completed.
	FINISHED,
	//! The pipeline is not fully executed and can be called again immediately.
	NOT_FINISHED,
	//! Core continuation work completed and execution must re-enter the selected runner.
	RUNNER_HANDOFF,
	//! The pipeline was interrupted and should not be called again until the interrupt is handled.
	INTERRUPTED
};

class ExecutionBudget {
public:
	explicit ExecutionBudget(idx_t maximum) : processed(0), maximum_to_process(maximum) {
	}

public:
	bool Next() {
		if (IsDepleted()) {
			return false;
		}
		processed++;
		return true;
	}
	bool IsDepleted() const {
		return processed >= maximum_to_process;
	}

private:
	idx_t processed;
	idx_t maximum_to_process;
};

} // namespace duckdb
