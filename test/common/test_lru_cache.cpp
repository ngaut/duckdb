#include "catch.hpp"
#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {

// Test value type
struct TestValue {
	int value = 0;
	idx_t size = 0;

	TestValue(int val, idx_t sz = 100) : value(val), size(sz) {
	}
};

struct ThrowingPayload {
	explicit ThrowingPayload(bool should_throw = false) {
		if (should_throw) {
			throw std::runtime_error("payload construction failed");
		}
	}

	idx_t GetWeight() const {
		return 1;
	}
};

struct ThrowingCopyKey {
	explicit ThrowingCopyKey(int value_p) : value(value_p) {
	}

	ThrowingCopyKey(const ThrowingCopyKey &other) : value(other.value) {
		if (throw_on_copy) {
			throw std::runtime_error("key copy failed");
		}
	}

	ThrowingCopyKey(ThrowingCopyKey &&) noexcept = default;
	ThrowingCopyKey &operator=(const ThrowingCopyKey &) = default;
	ThrowingCopyKey &operator=(ThrowingCopyKey &&) noexcept = default;

	bool operator==(const ThrowingCopyKey &other) const {
		return value == other.value;
	}

	int value;
	static bool throw_on_copy;
};

bool ThrowingCopyKey::throw_on_copy = false;

struct ThrowingCopyKeyHash {
	size_t operator()(const ThrowingCopyKey &key) const noexcept {
		return std::hash<int> {}(key.value);
	}
};

} // namespace

TEST_CASE("LRU cache insertion is transactional", "[lru_cache]") {
	SECTION("payload construction failure preserves existing entries") {
		SharedLruCache<string, TestValue, ThrowingPayload> cache(1);
		auto original = make_shared_ptr<TestValue>(1);
		cache.Put("original", original, false);

		REQUIRE_THROWS_AS(cache.Put("original", make_shared_ptr<TestValue>(2), true), std::runtime_error);
		REQUIRE(cache.Size() == 1);
		REQUIRE(cache.CurrentTotalWeight() == 1);
		REQUIRE(cache.Get("original") == original);
	}

	SECTION("map insertion failure rolls back the unpublished LRU node") {
		SharedLruCache<ThrowingCopyKey, TestValue, DefaultPayload, ThrowingCopyKeyHash> cache(1);
		auto original = make_shared_ptr<TestValue>(1);
		cache.Put(ThrowingCopyKey(1), original);

		ThrowingCopyKey::throw_on_copy = true;
		REQUIRE_THROWS_AS(cache.Put(ThrowingCopyKey(2), make_shared_ptr<TestValue>(2)), std::runtime_error);
		ThrowingCopyKey::throw_on_copy = false;

		REQUIRE(cache.Size() == 1);
		REQUIRE(cache.CurrentTotalWeight() == 1);
		REQUIRE(cache.Get(ThrowingCopyKey(1)) == original);
		REQUIRE(cache.Get(ThrowingCopyKey(2)) == nullptr);
	}
}

TEST_CASE("LRU Cache Basic Operations", "[lru_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	SharedLruCache<string, TestValue, BufferPoolPayload> cache(1000);

	SECTION("Put and Get") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		auto reservation = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key1", val1, std::move(reservation));

		auto result = cache.Get("key1");
		REQUIRE(result != nullptr);
		REQUIRE(result->value == 42);
		REQUIRE(cache.Size() == 1);
		REQUIRE(cache.CurrentTotalWeight() == 100);
	}

	SECTION("Get non-existent key") {
		auto result = cache.Get("nonexistent");
		REQUIRE(result == nullptr);
	}

	SECTION("Replace existing key") {
		auto val1 = make_shared_ptr<TestValue>(1, 100);
		auto val2 = make_shared_ptr<TestValue>(2, 150);

		auto reservation1 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key1", val1, std::move(reservation1));

		auto reservation2 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/150);
		cache.Put("key1", val2, std::move(reservation2));

		auto result = cache.Get("key1");
		REQUIRE(result != nullptr);
		REQUIRE(result->value == 2);
		REQUIRE(cache.CurrentTotalWeight() == 150);
	}

	SECTION("Delete") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		auto reservation = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key1", val1, std::move(reservation));

		bool deleted = cache.Delete("key1");
		REQUIRE(deleted == true);
		REQUIRE(cache.Get("key1") == nullptr);
		REQUIRE(cache.Size() == 0);
		REQUIRE(cache.CurrentTotalWeight() == 0);

		const bool deleted_again = cache.Delete("key1");
		REQUIRE(!deleted_again);
	}
}

TEST_CASE("LRU Cache Eviction", "[lru_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	SECTION("Evict LRU when exceeding max weight") {
		SharedLruCache<string, TestValue, BufferPoolPayload> cache(500);

		auto val1 = make_shared_ptr<TestValue>(1, 200);
		auto val2 = make_shared_ptr<TestValue>(2, 200);
		auto val3 = make_shared_ptr<TestValue>(3, 200);

		auto reservation1 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/200);
		cache.Put("key1", val1, std::move(reservation1));

		auto reservation2 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/200);
		cache.Put("key2", val2, std::move(reservation2));

		auto reservation3 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/200);
		cache.Put("key3", val3, std::move(reservation3));

		REQUIRE(cache.Get("key1") == nullptr);
		REQUIRE(cache.Get("key2") != nullptr);
		REQUIRE(cache.Get("key3") != nullptr);
		REQUIRE(cache.Size() == 2);
		REQUIRE(cache.CurrentTotalWeight() <= 500);
	}

	SECTION("LRU ordering") {
		SharedLruCache<string, TestValue, BufferPoolPayload> cache(300);

		auto val1 = make_shared_ptr<TestValue>(1, 100);
		auto val2 = make_shared_ptr<TestValue>(2, 100);
		auto val3 = make_shared_ptr<TestValue>(3, 100);
		auto val4 = make_shared_ptr<TestValue>(4, 100);

		auto reservation1 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key1", val1, std::move(reservation1));

		auto reservation2 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key2", val2, std::move(reservation2));

		auto reservation3 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key3", val3, std::move(reservation3));

		cache.Get("key1");

		auto reservation4 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key4", val4, std::move(reservation4));

		REQUIRE(cache.Get("key1") != nullptr);
		REQUIRE(cache.Get("key2") == nullptr);
		REQUIRE(cache.Get("key3") != nullptr);
		REQUIRE(cache.Get("key4") != nullptr);
	}
}

TEST_CASE("LRU Cache Unlimited Memory", "[lru_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	SharedLruCache<string, TestValue, BufferPoolPayload> cache(0);

	for (int idx = 0; idx < 100; ++idx) {
		auto val = make_shared_ptr<TestValue>(idx, 100);
		auto reservation = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
		cache.Put("key" + std::to_string(idx), val, std::move(reservation));
	}

	REQUIRE(cache.Size() == 100);
}

TEST_CASE("LRU Cache Clear", "[lru_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	SharedLruCache<string, TestValue, BufferPoolPayload> cache(1000);

	auto val1 = make_shared_ptr<TestValue>(1, 100);
	auto val2 = make_shared_ptr<TestValue>(2, 100);

	auto reservation1 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
	cache.Put("key1", val1, std::move(reservation1));

	auto reservation2 = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, /*size=*/100);
	cache.Put("key2", val2, std::move(reservation2));

	cache.Clear();

	REQUIRE(cache.Size() == 0);
	REQUIRE(cache.CurrentTotalWeight() == 0);
	REQUIRE(cache.Get("key1") == nullptr);
	REQUIRE(cache.Get("key2") == nullptr);
}

TEST_CASE("LRU Cache Evict To Reduce Memory", "[lru_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	// Set max memory to a large value, which exceeds all entries emplaced.
	SharedLruCache<string, TestValue, BufferPoolPayload> cache(20000);

	// Put a few entries, and check memory consumption.
	constexpr idx_t obj_size = 1000;
	for (int idx = 0; idx < 10; ++idx) {
		auto val = make_shared_ptr<TestValue>(idx, obj_size);
		auto reservation = make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, obj_size);
		cache.Put(StringUtil::Format("key%d", idx), val, std::move(reservation));
	}
	REQUIRE(cache.Size() == 10);
	REQUIRE(cache.CurrentTotalWeight() == 10 * obj_size);

	// Perform cache entries eviction, and check memory consumption.
	// Evict 4 * objects, leaving 6 objects in cache
	const idx_t bytes_to_free = 4 * obj_size;
	const idx_t freed = cache.EvictToReduceAtLeast(bytes_to_free);
	REQUIRE(freed >= bytes_to_free); // Should free at least the requested amount
	REQUIRE(cache.CurrentTotalWeight() == 6 * obj_size);
	REQUIRE(cache.Size() == 6);

	// The first 4 items should be evicted.
	for (int idx = 0; idx < 4; ++idx) {
		REQUIRE(cache.Get(StringUtil::Format("key%d", idx)) == nullptr);
	}
	// The later 6 items should be kept in cache.
	for (int idx = 6; idx < 10; ++idx) {
		REQUIRE(cache.Get(StringUtil::Format("key%d", idx)) != nullptr);
	}
}
