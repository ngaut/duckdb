#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

static void IgnoreStateAddresses(const uintptr_t *, const sel_t *, idx_t,
                                 ExecutionGroupedAggregateStateAddressUpdateMode, void *) {
}

static void WriteKeys(DataChunk &groups, int64_t first, idx_t count) {
	groups.Reset();
	auto keys = FlatVector::Writer<int64_t>(groups.data[0], count);
	for (idx_t i = 0; i < count; i++) {
		keys.WriteValue(first + NumericCast<int64_t>(i));
	}
	groups.CheckCardinality(count);
}

TEST_CASE("Appending new groups refuses a key a lookup created since the append last ran", "[aggregate_hll]") {
	DuckDB db(nullptr);
	Connection con(db);
	vector<LogicalType> types {LogicalType::BIGINT};
	GroupedAggregateHashTable ht(*con.context, Allocator::DefaultAllocator(), types,
	                             TupleDataValidityType::CANNOT_HAVE_NULL_VALUES);
	ht.Resize(16384);
	DataChunk groups;
	groups.Initialize(Allocator::DefaultAllocator(), types);

	// keys 0-9 are appended as new groups
	WriteKeys(groups, 0, 10);
	REQUIRE(ht.TryAppendNewGroupsWithStateAddressesFast(groups, IgnoreStateAddresses, nullptr));
	REQUIRE(ht.Count() == 10);

	// key 10 is created by a lookup
	WriteKeys(groups, 10, 1);
	Vector hashes(LogicalType::HASH);
	Vector addresses(LogicalType::POINTER);
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	groups.Hash(hashes);
	REQUIRE(ht.FindOrCreateGroups(groups, hashes, addresses, new_groups) == 1);
	REQUIRE(ht.Count() == 11);

	// appending key 10 as new would duplicate it
	WriteKeys(groups, 10, 1);
	REQUIRE_FALSE(ht.TryAppendNewGroupsWithStateAddressesFast(groups, IgnoreStateAddresses, nullptr));
	REQUIRE(ht.Count() == 11);

	// keys 11-12 are still new
	WriteKeys(groups, 11, 2);
	REQUIRE(ht.TryAppendNewGroupsWithStateAddressesFast(groups, IgnoreStateAddresses, nullptr));
	REQUIRE(ht.Count() == 13);
}

} // namespace duckdb
