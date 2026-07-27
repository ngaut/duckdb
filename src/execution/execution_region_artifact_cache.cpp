#include "duckdb/execution/execution_region_artifact_cache.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

ExecutionRegionArtifactCacheReservation::ExecutionRegionArtifactCacheReservation() {
}

ExecutionRegionArtifactCacheReservation::~ExecutionRegionArtifactCacheReservation() {
	if (owner && builder && !completed) {
		owner->Abort(*this);
	}
}

ExecutionRegionArtifactCacheReservation::ExecutionRegionArtifactCacheReservation(
    ExecutionRegionArtifactCacheReservation &&other) noexcept
    : key(std::move(other.key)), builder(other.builder), completed(other.completed), owner(other.owner),
      cached(std::move(other.cached)), state(std::move(other.state)) {
	other.builder = false;
	other.completed = true;
	other.owner = nullptr;
}

ExecutionRegionArtifactCacheReservation &
ExecutionRegionArtifactCacheReservation::operator=(ExecutionRegionArtifactCacheReservation &&other) noexcept {
	if (this != &other) {
		if (owner && builder && !completed) {
			owner->Abort(*this);
		}
		key = std::move(other.key);
		builder = other.builder;
		completed = other.completed;
		owner = other.owner;
		cached = std::move(other.cached);
		state = std::move(other.state);
		other.builder = false;
		other.completed = true;
		other.owner = nullptr;
	}
	return *this;
}

ExecutionRegionArtifactCache::ExecutionRegionArtifactCache(idx_t capacity) : ready(capacity) {
}

ExecutionRegionArtifactCache::~ExecutionRegionArtifactCache() {
}

ExecutionRegionArtifactCacheReservation ExecutionRegionArtifactCache::LookupOrReserve(const string &key) {
	unique_lock<mutex> guard(lock);
	while (true) {
		auto cached = ready.Get(key);
		if (cached) {
			ExecutionRegionArtifactCacheReservation reservation;
			reservation.key = key;
			reservation.owner = this;
			reservation.cached = std::move(cached);
			reservation.completed = true;
			return reservation;
		}
		auto found = building.find(key);
		if (found == building.end()) {
			auto state = make_shared_ptr<BuildState>();
			state->building = true;
			building.emplace(key, state);
			ExecutionRegionArtifactCacheReservation reservation;
			reservation.key = key;
			reservation.builder = true;
			reservation.owner = this;
			reservation.state = std::move(state);
			return reservation;
		}
		auto state = found->second;
		state->waiters++;
		state->ready.wait(guard, [&] { return !state->building; });
		state->waiters--;
		if (state->published) {
			ExecutionRegionArtifactCacheReservation reservation;
			reservation.key = key;
			reservation.owner = this;
			reservation.cached = state->published;
			reservation.completed = true;
			return reservation;
		}
	}
}

void ExecutionRegionArtifactCache::Publish(ExecutionRegionArtifactCacheReservation &reservation,
                                           shared_ptr<ExecutionRegionCachedArtifact> artifact) {
	if (!reservation.builder || reservation.completed || !reservation.state || !artifact || !artifact->artifact) {
		throw InternalException("invalid execution-region artifact cache publication");
	}
	auto state = reservation.state;
	{
		lock_guard<mutex> guard(lock);
		auto found = building.find(reservation.key);
		if (found == building.end() || found->second != state) {
			throw InternalException("execution-region artifact cache lost its build reservation");
		}
		state->published = artifact;
		ready.Put(reservation.key, std::move(artifact));
		building.erase(found);
		state->building = false;
		reservation.completed = true;
	}
	state->ready.notify_all();
}

void ExecutionRegionArtifactCache::Abort(ExecutionRegionArtifactCacheReservation &reservation) {
	if (!reservation.builder || reservation.completed || !reservation.state) {
		return;
	}
	auto state = reservation.state;
	{
		lock_guard<mutex> guard(lock);
		auto found = building.find(reservation.key);
		if (found != building.end() && found->second == state) {
			building.erase(found);
		}
		state->building = false;
		reservation.completed = true;
	}
	state->ready.notify_all();
}

idx_t ExecutionRegionArtifactCache::ReadyCount() const {
	lock_guard<mutex> guard(lock);
	return NumericCast<idx_t>(ready.Size());
}

idx_t ExecutionRegionArtifactCache::WaitingCount(const string &key) const {
	lock_guard<mutex> guard(lock);
	auto found = building.find(key);
	return found == building.end() ? 0 : found->second->waiters;
}

} // namespace duckdb
