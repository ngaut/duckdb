//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_compiled_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

template <class FUNCTION>
class SljitCompiledFunction {
public:
	SljitCompiledFunction() = default;

	SljitCompiledFunction(unique_ptr<ExecutionRegionCodeHandle> code_p, FUNCTION function_p)
	    : code(std::move(code_p)), function(function_p) {
		D_ASSERT((code != nullptr) == (function != nullptr));
	}

	static SljitCompiledFunction TryCreate(unique_ptr<ExecutionRegionCodeHandle> code_p, FUNCTION function_p) {
		if (!code_p || !function_p) {
			return {};
		}
		return SljitCompiledFunction(std::move(code_p), function_p);
	}

	FUNCTION Function() const {
		return function;
	}

	bool IsExecutable() const {
		return code && function;
	}

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}

private:
	shared_ptr<ExecutionRegionCodeHandle> code;
	FUNCTION function = nullptr;
};

template <class FUNCTION>
class SljitLazyCompiledFunction {
private:
	struct State {
		std::once_flag once;
		std::atomic<bool> ready {false};
		SljitCompiledFunction<FUNCTION> compiled;
	};

public:
	SljitLazyCompiledFunction() : state(make_uniq<State>()) {
	}

	template <class BUILD>
	FUNCTION Ensure(BUILD build) {
		if (state->ready.load(std::memory_order_acquire)) {
			return state->compiled.Function();
		}
		std::call_once(state->once, [&]() {
			state->compiled = build();
			D_ASSERT(state->compiled.IsExecutable());
			state->ready.store(true, std::memory_order_release);
		});
		return state->compiled.Function();
	}

	bool IsReady() const {
		return state->ready.load(std::memory_order_acquire);
	}

	idx_t CodeSize() const {
		return IsReady() ? state->compiled.CodeSize() : 0;
	}

private:
	unique_ptr<State> state;
};

} // namespace duckdb
