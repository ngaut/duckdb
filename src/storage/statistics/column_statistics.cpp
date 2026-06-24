#include "duckdb/storage/statistics/column_statistics.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

ColumnStatistics::ColumnStatistics(BaseStatistics stats_p) : stats(std::move(stats_p)) {
	if (DistinctStatistics::TypeIsSupported(stats.GetType())) {
		distinct_stats = make_uniq<DistinctStatistics>();
	}
}
ColumnStatistics::ColumnStatistics(BaseStatistics stats_p, unique_ptr<DistinctStatistics> distinct_stats_p)
    : stats(std::move(stats_p)), distinct_stats(std::move(distinct_stats_p)) {
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	return make_shared_ptr<ColumnStatistics>(BaseStatistics::CreateEmpty(type));
}

void ColumnStatistics::Merge(ColumnStatistics &other) {
	const bool this_had_values = stats.CanHaveNoNull();
	const bool other_had_values = other.stats.CanHaveNoNull();
	const auto this_distinct_count = GetDistinctCount();
	const auto other_distinct_count = other.GetDistinctCount();
	const bool this_has_hll = distinct_stats.get();
	const bool other_has_hll = other.distinct_stats.get();
	stats.Merge(other.stats);
	if (this_has_hll && other_has_hll) {
		distinct_stats->Merge(*other.distinct_stats);
		stats.SetDistinctCount(0);
		return;
	}
	if (!this_had_values) {
		if (other_distinct_count > 0) {
			SetDistinctCount(other_distinct_count);
		}
		return;
	}
	if (!other_had_values) {
		if (this_distinct_count > 0 && !this_has_hll) {
			SetDistinctCount(this_distinct_count);
		}
		return;
	}
	if (this_distinct_count > 0 || other_distinct_count > 0) {
		ClearDistinctCount();
	}
}

BaseStatistics &ColumnStatistics::Statistics() {
	return stats;
}

bool ColumnStatistics::HasDistinctStats() {
	return distinct_stats.get();
}

DistinctStatistics &ColumnStatistics::DistinctStats() {
	if (!distinct_stats) {
		throw InternalException("DistinctStats called without distinct_stats");
	}
	return *distinct_stats;
}

idx_t ColumnStatistics::GetDistinctCount() {
	if (distinct_stats) {
		return distinct_stats->GetCount();
	}
	return stats.GetDistinctCount();
}

void ColumnStatistics::SetDistinctCount(idx_t count) {
	stats.SetDistinctCount(count);
	distinct_stats.reset();
}

void ColumnStatistics::ClearDistinctCount() {
	stats.SetDistinctCount(0);
	distinct_stats.reset();
}

void ColumnStatistics::SetDistinct(unique_ptr<DistinctStatistics> distinct) {
	this->distinct_stats = std::move(distinct);
	if (this->distinct_stats) {
		stats.SetDistinctCount(0);
	}
}

void ColumnStatistics::UpdateDistinctStatistics(const Vector &v, idx_t count, Vector &hashes) {
	if (!distinct_stats) {
		return;
	}
	// we use a sample to update the distinct statistics for performance reasons
	distinct_stats->UpdateSample(v, count, hashes);
}

shared_ptr<ColumnStatistics> ColumnStatistics::Copy() const {
	return make_shared_ptr<ColumnStatistics>(stats.Copy(), distinct_stats ? distinct_stats->Copy() : nullptr);
}

void ColumnStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "statistics", stats);
	serializer.WritePropertyWithDefault(101, "distinct", distinct_stats, unique_ptr<DistinctStatistics>());
}

shared_ptr<ColumnStatistics> ColumnStatistics::Deserialize(Deserializer &deserializer) {
	auto stats = deserializer.ReadProperty<BaseStatistics>(100, "statistics");
	auto distinct_stats = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<DistinctStatistics>>(
	    101, "distinct", unique_ptr<DistinctStatistics>());
	return make_shared_ptr<ColumnStatistics>(std::move(stats), std::move(distinct_stats));
}

} // namespace duckdb
