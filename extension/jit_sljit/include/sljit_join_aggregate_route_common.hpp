//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_aggregate_route_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct SljitDataChunkBatch {
	idx_t Count() const {
		return initialized ? chunk.size() : 0;
	}

	bool Empty() const {
		return Count() == 0;
	}

	bool Initialized() const {
		return initialized;
	}

	idx_t ColumnCount() const {
		return initialized ? chunk.ColumnCount() : 0;
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &types) {
		if (initialized) {
			return;
		}
		chunk.Initialize(allocator, types);
		initialized = true;
	}

	void Reset() {
		if (initialized) {
			chunk.Reset();
		}
	}

	DataChunk chunk;
	bool initialized = false;
};

struct SljitDeferredBuildState {
	enum class Status : uint8_t { UNBUILT, READY, BLOCKED };

	bool Built() const {
		return status != Status::UNBUILT;
	}

	bool Ready() const {
		return status == Status::READY;
	}

	void MarkReady() {
		status = Status::READY;
		blocker.clear();
	}

	bool Block(const char *blocker_p) {
		status = Status::BLOCKED;
		blocker = blocker_p;
		return false;
	}

	Status status = Status::UNBUILT;
	string blocker;
};

} // namespace duckdb
