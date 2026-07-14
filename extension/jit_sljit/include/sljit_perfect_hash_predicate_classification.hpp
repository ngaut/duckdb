//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_perfect_hash_predicate_classification.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

#include <atomic>

namespace duckdb {

enum class SljitPerfectHashPredicateClassification : uint8_t { NULL_VALUE, NON_MATCHING, MATCHING };

struct SljitPerfectHashPredicateClassificationArtifact {
	SljitPerfectHashPredicateClassificationArtifact(buffer_ptr<DictionaryEntry> dictionary_p, bool all_valid_p,
	                                                vector<uint8_t> classifications_p)
	    : dictionary(std::move(dictionary_p)), all_valid(all_valid_p), classifications(std::move(classifications_p)) {
	}

	buffer_ptr<DictionaryEntry> dictionary;
	bool all_valid;
	vector<uint8_t> classifications;
};

struct SljitPerfectHashPredicateClassificationBuildResult {
	bool all_valid = false;
	vector<uint8_t> classifications;
};

struct SljitPerfectHashPredicateClassificationObservation {
	shared_ptr<const SljitPerfectHashPredicateClassificationArtifact> artifact;
	bool started_dictionary_epoch = false;
	bool activation_pending = false;
};

//! A hash-probe executable is shared by all local pipeline states. Once their
//! combined direct volume covers one RHS dictionary, one task builds an
//! immutable classifier and atomically publishes the dictionary owner with its
//! callable bytes. The predicate descriptor is fixed by that executable's
//! direct terminal; a different dictionary starts a new observation epoch.
class SljitSharedPerfectHashPredicateClassificationCache {
private:
	struct State {
		mutex lock;
		buffer_ptr<DictionaryEntry> observed_dictionary;
		idx_t observed_probe_rows = 0;
		shared_ptr<const SljitPerfectHashPredicateClassificationArtifact> published;
	};

public:
	SljitSharedPerfectHashPredicateClassificationCache() : state(make_uniq<State>()) {
	}

	template <class BUILD>
	SljitPerfectHashPredicateClassificationObservation Observe(const buffer_ptr<DictionaryEntry> &dictionary,
	                                                           idx_t count, BUILD build) {
		if (!dictionary) {
			return {};
		}
		auto published = state->published.atomic_load(std::memory_order_acquire);
		if (published && published->dictionary == dictionary) {
			return {std::move(published), false, false};
		}

		lock_guard<mutex> guard(state->lock);
		published = state->published.atomic_load(std::memory_order_relaxed);
		if (published && published->dictionary == dictionary) {
			return {std::move(published), false, false};
		}
		bool started_dictionary_epoch = false;
		if (state->observed_dictionary != dictionary) {
			state->observed_dictionary = dictionary;
			state->observed_probe_rows = 0;
			started_dictionary_epoch = true;
		}
		const auto activation_probe_rows = MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 64, dictionary->data.size());
		if (state->observed_probe_rows < activation_probe_rows) {
			const auto remaining_probe_rows = activation_probe_rows - state->observed_probe_rows;
			if (count < remaining_probe_rows) {
				state->observed_probe_rows += count;
				return {nullptr, started_dictionary_epoch, true};
			}
			state->observed_probe_rows = activation_probe_rows;
		}

		auto result = build();
		if (result.classifications.size() != dictionary->data.size()) {
			throw InternalException("SLJIT shared perfect-hash predicate classifier has an invalid dictionary size");
		}
		published = make_shared_ptr<SljitPerfectHashPredicateClassificationArtifact>(dictionary, result.all_valid,
		                                                                             std::move(result.classifications));
		state->published.atomic_store(published);
		return {std::move(published), started_dictionary_epoch, false};
	}

private:
	unique_ptr<State> state;
};

} // namespace duckdb
