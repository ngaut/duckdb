//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_artifact_cache.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/execution_region_common.hpp"

#include <condition_variable>

namespace duckdb {

class ExecutionRegionArtifact;

//! Validated metadata associated with one reusable semantic artifact.
struct ExecutionRegionCachedArtifact {
	shared_ptr<const ExecutionRegionArtifact> artifact;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	string compile_reason;
	string ir;
};

//! A reservation returned by the artifact cache. Exactly one caller builds a
//! missing key; other callers wait and then receive the validated publication.
class ExecutionRegionArtifactCacheReservation;

//! The database-local execution-region manager owns this bounded cache. The
//! in-flight map is separate from the ready LRU: active kernels retain shared
//! artifact ownership after an entry is evicted.
class ExecutionRegionArtifactCache {
private:
	struct BuildState;
	friend class ExecutionRegionArtifactCacheReservation;

public:
	static constexpr idx_t DEFAULT_CAPACITY = 8;

	explicit ExecutionRegionArtifactCache(idx_t capacity = DEFAULT_CAPACITY);
	~ExecutionRegionArtifactCache();

	ExecutionRegionArtifactCacheReservation LookupOrReserve(const string &key);
	void Publish(ExecutionRegionArtifactCacheReservation &reservation,
	             shared_ptr<ExecutionRegionCachedArtifact> artifact);
	void Abort(ExecutionRegionArtifactCacheReservation &reservation);

	idx_t ReadyCount() const;
	idx_t WaitingCount(const string &key) const;

private:
	struct BuildState {
		bool building = false;
		idx_t waiters = 0;
		//! Keeps one publication alive for callers already sleeping on this
		//! build, even if the bounded ready LRU evicts the key before they wake.
		shared_ptr<const ExecutionRegionCachedArtifact> published;
		std::condition_variable ready;
	};

	mutable mutex lock;
	SharedLruCache<string, ExecutionRegionCachedArtifact> ready;
	unordered_map<string, shared_ptr<BuildState>> building;
};

class ExecutionRegionArtifactCacheReservation {
public:
	ExecutionRegionArtifactCacheReservation();
	~ExecutionRegionArtifactCacheReservation();
	ExecutionRegionArtifactCacheReservation(ExecutionRegionArtifactCacheReservation &&other) noexcept;
	ExecutionRegionArtifactCacheReservation &operator=(ExecutionRegionArtifactCacheReservation &&other) noexcept;

	ExecutionRegionArtifactCacheReservation(const ExecutionRegionArtifactCacheReservation &) = delete;
	ExecutionRegionArtifactCacheReservation &operator=(const ExecutionRegionArtifactCacheReservation &) = delete;

	bool IsBuilder() const {
		return builder;
	}
	bool IsHit() const {
		return cached != nullptr;
	}
	const shared_ptr<const ExecutionRegionCachedArtifact> &Cached() const {
		return cached;
	}

private:
	friend class ExecutionRegionArtifactCache;
	string key;
	bool builder = false;
	bool completed = false;
	ExecutionRegionArtifactCache *owner = nullptr;
	shared_ptr<const ExecutionRegionCachedArtifact> cached;
	shared_ptr<ExecutionRegionArtifactCache::BuildState> state;
};

} // namespace duckdb
