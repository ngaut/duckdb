#include "test_jit_helpers.hpp"

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_codegen_capabilities.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_native_codegen.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"

using namespace duckdb;

static void RequirePrimitiveRunGenericFallback(ExecutionRegionManager &manager) {
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths,
		                                "aggregate_update.generated_pending_primitive_group_runs_miss.code=") &&
		           StringUtil::Contains(runtime_paths, "aggregate_update.pending_preaggregated_grouped_update_flush=");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("Primitive aggregate fresh-state initialization has an endian-independent flag layout", "[api][jit]") {
	ExecutionPrimitiveAggregateUpdateLane lane;
	lane.kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	lane.state_size = sizeof(int64_t) + sizeof(uint64_t);
	lane.state_value_offset = 0;
	lane.state_is_set_offset = sizeof(int64_t);

	alignas(uint64_t) std::array<uint8_t, sizeof(int64_t) + sizeof(uint64_t)> state;
	state.fill(0xa5);
	ExecutionInitializeFreshPrimitiveAggregateState<int64_t>(state.data(), lane, -42, true);
	REQUIRE(Load<int64_t>(state.data()) == -42);
	REQUIRE(Load<bool>(state.data() + lane.state_is_set_offset));
	for (idx_t padding_idx = 1; padding_idx < sizeof(uint64_t); padding_idx++) {
		REQUIRE(state[lane.state_is_set_offset + padding_idx] == 0);
	}

	state.fill(0xa5);
	ExecutionInitializeFreshPrimitiveAggregateState<int64_t>(state.data(), lane, 99, false);
	REQUIRE(Load<int64_t>(state.data()) == 0);
	REQUIRE_FALSE(Load<bool>(state.data() + lane.state_is_set_offset));
	for (idx_t tail_idx = 1; tail_idx < sizeof(uint64_t); tail_idx++) {
		REQUIRE(state[lane.state_is_set_offset + tail_idx] == 0);
	}
}

TEST_CASE("JIT generated preaggregated state writer preserves canonical primitive semantics", "[api][jit]") {
	if (!SljitPrimitiveRunCodegenSupported()) {
		return;
	}

	vector<ExecutionPrimitiveAggregateUpdateLane> lanes(3);
	lanes[0].ready = true;
	lanes[0].kind = AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	lanes[0].state_offset = 0;
	lanes[0].state_size = sizeof(hugeint_t) + sizeof(uint64_t);
	lanes[0].state_value_offset = 0;
	lanes[0].state_is_set_offset = sizeof(hugeint_t);
	lanes[1].ready = true;
	lanes[1].kind = AggregatePrimitiveUpdateKind::COUNT;
	lanes[1].state_offset = lanes[0].state_size;
	lanes[1].state_size = sizeof(int64_t);
	lanes[1].state_value_offset = 0;
	lanes[2].ready = true;
	lanes[2].kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	lanes[2].state_offset = lanes[1].state_offset + lanes[1].state_size;
	lanes[2].state_size = sizeof(int64_t) + sizeof(uint64_t);
	lanes[2].state_value_offset = 0;
	lanes[2].state_is_set_offset = sizeof(int64_t);
	const idx_t row_size = lanes[2].state_offset + lanes[2].state_size;

	std::array<std::array<uint8_t, 48>, 3> states;
	REQUIRE(row_size == states[0].size());
	for (auto &state : states) {
		state.fill(0xa5);
	}
	const uintptr_t addresses[] = {reinterpret_cast<uintptr_t>(states[0].data()),
	                               reinterpret_cast<uintptr_t>(states[1].data()),
	                               reinterpret_cast<uintptr_t>(states[2].data())};
	vector<hugeint_t> huge_values {hugeint_t(1, NumericLimits<uint64_t>::Maximum()), hugeint_t(3, 7), hugeint_t(-2, 9)};
	vector<int64_t> count_values {2, 3, 4};
	vector<int64_t> int64_values {5, -6, 7};
	vector<uint8_t> huge_is_set {1, 0, 1};
	vector<uint8_t> int64_is_set {1, 1, 0};
	vector<SljitNativePreaggregatedPrimitiveLaneInput> lane_inputs(3);
	lane_inputs[0].hugeint_values = huge_values.data();
	lane_inputs[0].value_is_set = huge_is_set.data();
	lane_inputs[0].state_offset = lanes[0].state_offset;
	lane_inputs[1].int64_values = count_values.data();
	lane_inputs[1].state_offset = lanes[1].state_offset;
	lane_inputs[2].int64_values = int64_values.data();
	lane_inputs[2].value_is_set = int64_is_set.data();
	lane_inputs[2].state_offset = lanes[2].state_offset;
	vector<AggregatePrimitiveUpdateKind> primitive_kinds;
	vector<idx_t> state_offsets;
	for (auto &lane : lanes) {
		primitive_kinds.push_back(lane.kind);
		state_offsets.push_back(lane.state_offset);
	}

	SljitNativePreaggregatedPrimitiveUpdateFunction initialize = nullptr;
	string error;
	auto initialize_code =
	    BuildSljitNativePreaggregatedPrimitiveUpdate(primitive_kinds, state_offsets, true, initialize, error);
	REQUIRE(initialize_code);
	REQUIRE(initialize);
	SljitNativePreaggregatedPrimitiveUpdateInput input;
	input.addresses = addresses;
	input.lane_inputs = lane_inputs.data();
	input.count = states.size();
	initialize(&input);

	REQUIRE(Load<hugeint_t>(states[0].data()) == huge_values[0]);
	REQUIRE(Load<bool>(states[0].data() + lanes[0].state_is_set_offset));
	REQUIRE(Load<hugeint_t>(states[1].data()) == hugeint_t(0));
	REQUIRE_FALSE(Load<bool>(states[1].data() + lanes[0].state_is_set_offset));
	REQUIRE(Load<int64_t>(states[1].data() + lanes[1].state_offset) == 3);
	REQUIRE(Load<int64_t>(states[1].data() + lanes[2].state_offset) == -6);
	REQUIRE(Load<bool>(states[1].data() + lanes[2].state_offset + lanes[2].state_is_set_offset));
	REQUIRE(Load<int64_t>(states[2].data() + lanes[2].state_offset) == 0);
	REQUIRE_FALSE(Load<bool>(states[2].data() + lanes[2].state_offset + lanes[2].state_is_set_offset));
	for (idx_t padding_idx = 1; padding_idx < sizeof(uint64_t); padding_idx++) {
		REQUIRE(states[1][lanes[0].state_is_set_offset + padding_idx] == 0);
		REQUIRE(states[2][lanes[2].state_offset + lanes[2].state_is_set_offset + padding_idx] == 0);
	}

	huge_values = {hugeint_t(0, 1), hugeint_t(-1, NumericLimits<uint64_t>::Maximum()), hugeint_t(4, 5)};
	count_values = {10, 20, 30};
	int64_values = {40, 50, 60};
	huge_is_set = {1, 1, 0};
	int64_is_set = {1, 0, 1};
	lane_inputs[0].hugeint_values = huge_values.data();
	lane_inputs[0].value_is_set = huge_is_set.data();
	lane_inputs[1].int64_values = count_values.data();
	lane_inputs[2].int64_values = int64_values.data();
	lane_inputs[2].value_is_set = int64_is_set.data();
	SljitNativePreaggregatedPrimitiveUpdateFunction update = nullptr;
	auto update_code =
	    BuildSljitNativePreaggregatedPrimitiveUpdate(primitive_kinds, state_offsets, false, update, error);
	REQUIRE(update_code);
	REQUIRE(update);
	update(&input);

	REQUIRE(Load<hugeint_t>(states[0].data()) == hugeint_t(2, 0));
	REQUIRE(Load<hugeint_t>(states[1].data()) == hugeint_t(-1, NumericLimits<uint64_t>::Maximum()));
	REQUIRE(Load<bool>(states[1].data() + lanes[0].state_is_set_offset));
	REQUIRE(Load<int64_t>(states[0].data() + lanes[1].state_offset) == 12);
	REQUIRE(Load<int64_t>(states[1].data() + lanes[1].state_offset) == 23);
	REQUIRE(Load<int64_t>(states[2].data() + lanes[1].state_offset) == 34);
	REQUIRE(Load<int64_t>(states[0].data() + lanes[2].state_offset) == 45);
	REQUIRE(Load<int64_t>(states[1].data() + lanes[2].state_offset) == -6);
	REQUIRE(Load<int64_t>(states[2].data() + lanes[2].state_offset) == 60);
	REQUIRE(Load<bool>(states[2].data() + lanes[2].state_offset + lanes[2].state_is_set_offset));
}

TEST_CASE("JIT generated preaggregated state writer follows row and address selections", "[api][jit]") {
	if (!SljitPrimitiveRunCodegenSupported()) {
		return;
	}

	const vector<AggregatePrimitiveUpdateKind> primitive_kinds {AggregatePrimitiveUpdateKind::SUM_INT64};
	const vector<idx_t> state_offsets {0};
	string error;
	SljitNativePreaggregatedPrimitiveUpdateFunction row_selected_update = nullptr;
	auto row_selected_update_code = BuildSljitNativePreaggregatedPrimitiveUpdate(
	    primitive_kinds, state_offsets, false, row_selected_update, error, false, true);
	REQUIRE(row_selected_update_code);
	REQUIRE(row_selected_update);

	const vector<int64_t> values {10, 20, 30};
	const vector<uint8_t> value_is_set {1, 1, 1};
	SljitNativePreaggregatedPrimitiveLaneInput lane;
	lane.int64_values = values.data();
	lane.value_is_set = value_is_set.data();

	constexpr idx_t state_size = sizeof(int64_t) + sizeof(uint64_t);
	std::array<std::array<uint8_t, state_size>, 3> states;
	std::array<uintptr_t, 3> addresses;
	for (idx_t state_idx = 0; state_idx < states.size(); state_idx++) {
		states[state_idx].fill(0);
		Store<int64_t>(NumericCast<int64_t>(100 * (state_idx + 1)), states[state_idx].data());
		Store<uint64_t>(1, states[state_idx].data() + sizeof(int64_t));
		addresses[state_idx] = reinterpret_cast<uintptr_t>(states[state_idx].data());
	}

	const sel_t execute_sel[] = {2, 0};
	SljitNativePreaggregatedPrimitiveUpdateInput input;
	input.addresses = addresses.data();
	input.execute_sel = execute_sel;
	input.lane_inputs = &lane;
	input.count = 2;
	row_selected_update(&input);
	REQUIRE(Load<int64_t>(states[0].data()) == 130);
	REQUIRE(Load<int64_t>(states[1].data()) == 210);
	REQUIRE(Load<int64_t>(states[2].data()) == 300);

	const sel_t address_sel[] = {1, 2, 0};
	input.address_sel = address_sel;
	SljitNativePreaggregatedPrimitiveUpdateFunction fully_selected_update = nullptr;
	auto fully_selected_update_code = BuildSljitNativePreaggregatedPrimitiveUpdate(
	    primitive_kinds, state_offsets, false, fully_selected_update, error, true, true);
	REQUIRE(fully_selected_update_code);
	REQUIRE(fully_selected_update);
	fully_selected_update(&input);
	REQUIRE(Load<int64_t>(states[0].data()) == 160);
	REQUIRE(Load<int64_t>(states[1].data()) == 220);
	REQUIRE(Load<int64_t>(states[2].data()) == 300);

	input.execute_sel = nullptr;
	SljitNativePreaggregatedPrimitiveUpdateFunction address_selected_update = nullptr;
	auto address_selected_update_code = BuildSljitNativePreaggregatedPrimitiveUpdate(
	    primitive_kinds, state_offsets, false, address_selected_update, error, true, false);
	REQUIRE(address_selected_update_code);
	REQUIRE(address_selected_update);
	address_selected_update(&input);
	REQUIRE(Load<int64_t>(states[0].data()) == 160);
	REQUIRE(Load<int64_t>(states[1].data()) == 230);
	REQUIRE(Load<int64_t>(states[2].data()) == 320);
}

TEST_CASE("JIT generated preaggregated state writer loops wide homogeneous lanes", "[api][jit]") {
	if (!SljitPrimitiveRunCodegenSupported()) {
		return;
	}
	constexpr idx_t lane_count = 9;
	constexpr idx_t comparison_lane_count = 16;
	constexpr idx_t row_count = 2;
	const std::array<AggregatePrimitiveUpdateKind, 3> kinds {AggregatePrimitiveUpdateKind::COUNT,
	                                                         AggregatePrimitiveUpdateKind::SUM_INT64,
	                                                         AggregatePrimitiveUpdateKind::SUM_HUGEINT};

	auto make_lanes = [](AggregatePrimitiveUpdateKind kind, idx_t count) {
		vector<ExecutionPrimitiveAggregateUpdateLane> lanes(count);
		const auto value_size = AggregatePrimitiveUpdateStateValueSize(kind);
		const auto has_state_is_set = AggregatePrimitiveUpdateHasStateIsSet(kind);
		const auto state_size = value_size + (has_state_is_set ? sizeof(uint64_t) : 0);
		for (idx_t lane_idx = 0; lane_idx < count; lane_idx++) {
			auto &lane = lanes[lane_idx];
			lane.ready = true;
			lane.kind = kind;
			lane.state_offset = lane_idx * state_size;
			lane.state_size = state_size;
			lane.state_value_offset = 0;
			lane.state_is_set_offset = has_state_is_set ? value_size : 0;
		}
		return lanes;
	};

	for (auto kind : kinds) {
		auto lanes = make_lanes(kind, lane_count);
		const vector<AggregatePrimitiveUpdateKind> primitive_kinds(lane_count, kind);
		vector<idx_t> state_offsets;
		for (auto &lane : lanes) {
			state_offsets.push_back(lane.state_offset);
		}
		const auto row_state_size = lane_count * lanes[0].state_size;
		vector<uint8_t> states(row_count * row_state_size, 0xa5);
		vector<uintptr_t> addresses(row_count);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			addresses[row_idx] = reinterpret_cast<uintptr_t>(states.data() + row_idx * row_state_size);
		}

		vector<vector<int64_t>> int64_values(lane_count, vector<int64_t>(row_count));
		vector<vector<hugeint_t>> hugeint_values(lane_count, vector<hugeint_t>(row_count));
		vector<vector<uint8_t>> value_is_set(lane_count, vector<uint8_t>(row_count));
		vector<SljitNativePreaggregatedPrimitiveLaneInput> lane_inputs(lane_count);
		for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
			int64_values[lane_idx] = {NumericCast<int64_t>(lane_idx + 1), NumericCast<int64_t>(lane_idx + 11)};
			hugeint_values[lane_idx] = {hugeint_t(int64_values[lane_idx][0]), hugeint_t(int64_values[lane_idx][1])};
			value_is_set[lane_idx] = {1, static_cast<uint8_t>(lane_idx % 2 == 0)};
			auto &input = lane_inputs[lane_idx];
			input.int64_values = int64_values[lane_idx].data();
			input.hugeint_values = hugeint_values[lane_idx].data();
			input.value_is_set = value_is_set[lane_idx].data();
			input.state_offset = lanes[lane_idx].state_offset;
		}

		string error;
		SljitNativePreaggregatedPrimitiveUpdateFunction initialize = nullptr;
		auto initialize_code =
		    BuildSljitNativePreaggregatedPrimitiveUpdate(primitive_kinds, state_offsets, true, initialize, error);
		REQUIRE(initialize_code);
		REQUIRE(initialize);
		SljitNativePreaggregatedPrimitiveUpdateInput input;
		input.addresses = addresses.data();
		input.lane_inputs = lane_inputs.data();
		input.count = row_count;
		initialize(&input);

		for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
			int64_values[lane_idx] = {NumericCast<int64_t>(lane_idx + 101), NumericCast<int64_t>(lane_idx + 201)};
			hugeint_values[lane_idx] = {hugeint_t(int64_values[lane_idx][0]), hugeint_t(int64_values[lane_idx][1])};
			value_is_set[lane_idx] = {1, static_cast<uint8_t>(lane_idx % 2 != 0)};
			lane_inputs[lane_idx].int64_values = int64_values[lane_idx].data();
			lane_inputs[lane_idx].hugeint_values = hugeint_values[lane_idx].data();
			lane_inputs[lane_idx].value_is_set = value_is_set[lane_idx].data();
		}
		SljitNativePreaggregatedPrimitiveUpdateFunction update = nullptr;
		auto update_code =
		    BuildSljitNativePreaggregatedPrimitiveUpdate(primitive_kinds, state_offsets, false, update, error);
		REQUIRE(update_code);
		REQUIRE(update);
		update(&input);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
				auto state = states.data() + row_idx * row_state_size + lanes[lane_idx].state_offset;
				if (kind == AggregatePrimitiveUpdateKind::COUNT) {
					const auto expected = NumericCast<int64_t>(2 * lane_idx + (row_idx == 0 ? 102 : 212));
					REQUIRE(Load<int64_t>(state) == expected);
					continue;
				}
				const bool initialized = row_idx == 0 || lane_idx % 2 == 0;
				const bool updated = row_idx == 0 || lane_idx % 2 != 0;
				const auto initial_value = NumericCast<int64_t>(lane_idx + (row_idx == 0 ? 1 : 11));
				const auto update_value = NumericCast<int64_t>(lane_idx + (row_idx == 0 ? 101 : 201));
				const auto expected = (initialized ? initial_value : 0) + (updated ? update_value : 0);
				if (kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					REQUIRE(Load<int64_t>(state) == expected);
				} else {
					REQUIRE(Load<hugeint_t>(state) == hugeint_t(expected));
				}
				REQUIRE(Load<bool>(state + lanes[lane_idx].state_is_set_offset));
			}
		}

		const vector<AggregatePrimitiveUpdateKind> comparison_primitive_kinds(comparison_lane_count, kind);
		vector<idx_t> comparison_state_offsets(comparison_lane_count);
		for (idx_t lane_idx = 0; lane_idx < comparison_lane_count; lane_idx++) {
			comparison_state_offsets[lane_idx] = lane_idx * lanes[0].state_size;
		}
		SljitNativePreaggregatedPrimitiveUpdateFunction comparison_initialize = nullptr;
		auto comparison_initialize_code = BuildSljitNativePreaggregatedPrimitiveUpdate(
		    comparison_primitive_kinds, comparison_state_offsets, true, comparison_initialize, error);
		REQUIRE(comparison_initialize_code);
		REQUIRE(comparison_initialize);
		REQUIRE(comparison_initialize_code->CodeSize() == initialize_code->CodeSize());
		SljitNativePreaggregatedPrimitiveUpdateFunction comparison_update = nullptr;
		auto comparison_update_code = BuildSljitNativePreaggregatedPrimitiveUpdate(
		    comparison_primitive_kinds, comparison_state_offsets, false, comparison_update, error);
		REQUIRE(comparison_update_code);
		REQUIRE(comparison_update);
		REQUIRE(comparison_update_code->CodeSize() == update_code->CodeSize());
	}
}

TEST_CASE("JIT executable group ranges prove signed narrowing casts once", "[api][jit]") {
	SljitExecutableIntegralGroupKeyRange range;
	range.ready = true;
	range.source_physical_type = PhysicalType::INT64;
	range.min_value = -500000;
	range.max_value = 4500000;

	ExecutionRowPointerGroupKeySource source;
	source.source_physical_type = PhysicalType::INT64;
	source.target_physical_type = PhysicalType::INT32;
	source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32;
	vector<SljitExecutableIntegralGroupKeyRange> ranges {range};
	vector<ExecutionRowPointerGroupKeySource> sources {source};
	SljitApplyExecutableIntegralGroupKeyRangeProofs(ranges, sources);
	REQUIRE(sources[0].unchecked_integral_cast);

	ranges[0].min_value = int64_t(NumericLimits<int32_t>::Minimum()) - 1;
	sources[0].unchecked_integral_cast = false;
	SljitApplyExecutableIntegralGroupKeyRangeProofs(ranges, sources);
	REQUIRE_FALSE(sources[0].unchecked_integral_cast);
}

TEST_CASE("JIT canonical single-lane sums initialize fresh states directly", "[api][jit]") {
	ExecutionPrimitiveAggregateUpdateLane lane;
	lane.ready = true;
	lane.kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	lane.state_size = sizeof(int64_t) + sizeof(uint64_t);
	lane.state_value_offset = 0;
	lane.state_is_set_offset = sizeof(int64_t);
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes {&lane};
	vector<SljitPreaggregatedPrimitivePayloadDeltas> payloads(1);
	payloads[0].kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	payloads[0].int64_values = {11, 22};
	payloads[0].value_is_set = {1, 0};
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(lanes, payloads, 1);

	std::array<uint8_t, sizeof(int64_t) + sizeof(uint64_t)> first_state;
	std::array<uint8_t, sizeof(int64_t) + sizeof(uint64_t)> second_state;
	first_state.fill(0xa5);
	second_state.fill(0xa5);
	const uintptr_t addresses[] = {reinterpret_cast<uintptr_t>(first_state.data()),
	                               reinterpret_cast<uintptr_t>(second_state.data())};
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(
	    addresses, nullptr, 2, ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE, &update_state);
	REQUIRE(Load<int64_t>(first_state.data()) == 11);
	REQUIRE(Load<bool>(first_state.data() + lane.state_is_set_offset));
	REQUIRE(Load<int64_t>(second_state.data()) == 0);
	REQUIRE_FALSE(Load<bool>(second_state.data() + lane.state_is_set_offset));
	REQUIRE(update_state.captured_address == addresses[1]);
}

TEST_CASE("Grouped aggregate append callbacks expose identity address order directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);
	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CANNOT_HAVE_NULL_VALUES);

	DataChunk groups;
	groups.Initialize(allocator, {LogicalType::INTEGER});
	const idx_t group_count = 64;
	auto group_data = FlatVector::GetDataMutable<int32_t>(groups.data[0]);
	for (idx_t row_idx = 0; row_idx < group_count; row_idx++) {
		group_data[row_idx] = UnsafeNumericCast<int32_t>(row_idx);
	}
	FlatVector::SetSize(groups.data[0], group_count);
	groups.SetChildCardinality(group_count);

	struct AppendUpdateState {
		bool called = false;
		bool identity_order = false;
		idx_t count = 0;
	};
	AppendUpdateState update_state;
	auto update = [](const uintptr_t *addresses, const sel_t *address_sel, idx_t count,
	                 ExecutionGroupedAggregateStateAddressUpdateMode mode, void *state_p) {
		auto &state = *reinterpret_cast<AppendUpdateState *>(state_p);
		state.called = true;
		state.identity_order = address_sel == nullptr;
		state.count = count;
		REQUIRE(addresses);
		REQUIRE(mode == ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			REQUIRE(addresses[row_idx] != 0);
		}
	};

	ht.SkipLookups(false);
	REQUIRE(ht.TryAppendNewGroupsWithStateAddressesFast(groups, update, &update_state));
	REQUIRE(update_state.called);
	REQUIRE(update_state.identity_order);
	REQUIRE(update_state.count == group_count);
	REQUIRE(ht.Count() == group_count);
}

TEST_CASE("JIT shared affine grouped deltas do not overflow before cancellation", "[api][jit]") {
	ExecutionPrimitiveAggregateUpdateLane left_lane;
	left_lane.ready = true;
	left_lane.kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	left_lane.state_size = sizeof(int64_t) + sizeof(uint64_t);
	left_lane.state_value_offset = 0;
	left_lane.state_is_set_offset = sizeof(int64_t);

	auto right_lane = left_lane;
	right_lane.state_offset = left_lane.state_size;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes {&left_lane, &right_lane};
	SljitPreaggregatedPrimitiveAggregateScratch scratch;
	scratch.PrepareSharedAffine(lanes, 1);
	for (auto &payload : scratch.payloads) {
		REQUIRE(payload.int64_values.capacity() == 0);
		REQUIRE(payload.hugeint_values.capacity() == 0);
		REQUIRE(payload.value_is_set.capacity() == 0);
	}
	const idx_t valid_count = 4294967299ULL;
	const auto source_value = NumericLimits<int32_t>::Maximum();
	SljitAppendSharedAffineValue(scratch, hugeint_t(NumericCast<int64_t>(valid_count)) * hugeint_t(source_value));
	scratch.shared_valid_counts.push_back(valid_count);

	SljitExecutableFusedAffineRunUpdate affine;
	affine.source_position = 0;
	affine.source_type = PhysicalType::INT32;
	affine.primitive_kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	affine.lanes.push_back({1, -source_value});
	affine.lanes.push_back({-1, source_value});
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(lanes, scratch, &affine);

	std::array<uint8_t, 2 * (sizeof(int64_t) + sizeof(uint64_t))> state;
	state.fill(0xa5);
	const uintptr_t addresses[] = {reinterpret_cast<uintptr_t>(state.data())};
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(
	    addresses, nullptr, 1, ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE, &update_state);
	REQUIRE(Load<int64_t>(state.data()) == 0);
	REQUIRE(Load<bool>(state.data() + left_lane.state_is_set_offset));
	REQUIRE(Load<int64_t>(state.data() + right_lane.state_offset) == 0);
	REQUIRE(Load<bool>(state.data() + right_lane.state_offset + right_lane.state_is_set_offset));
}

TEST_CASE("JIT shared affine scratch slices hugeint lanes without per-lane storage", "[api][jit]") {
	ExecutionPrimitiveAggregateUpdateLane lane;
	lane.kind = AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes {&lane};

	SljitPreaggregatedPrimitiveAggregateScratch source;
	source.PrepareSharedAffine(lanes, 2);
	source.group_row_counts.push_back(3);
	source.group_row_counts.push_back(5);
	SljitAppendSharedAffineValue(source, hugeint_t(11));
	const auto wide_value = hugeint_t(NumericLimits<int64_t>::Maximum()) + hugeint_t(17);
	SljitAppendSharedAffineValue(source, wide_value);
	source.shared_valid_counts.push_back(2);
	source.shared_valid_counts.push_back(4);

	SljitPreaggregatedPrimitiveAggregateScratch target;
	REQUIRE(SlicePreaggregatedPrimitiveScratch(source, lanes, 1, 1, target));
	REQUIRE(target.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE);
	REQUIRE(target.payloads.size() == 1);
	REQUIRE(target.payloads[0].hugeint_values.capacity() == 0);
	REQUIRE(target.payloads[0].value_is_set.capacity() == 0);
	REQUIRE(target.group_row_counts == vector<idx_t> {5});
	REQUIRE(target.shared_int64_values == vector<int64_t> {0});
	REQUIRE(target.shared_hugeint_values == vector<hugeint_t> {wide_value});
	REQUIRE(target.shared_value_is_wide == vector<uint8_t> {1});
	REQUIRE(target.shared_valid_counts == vector<idx_t> {4});
}

TEST_CASE("JIT shared affine progressions bind canonical aggregate states once", "[api][jit]") {
	std::array<ExecutionPrimitiveAggregateUpdateLane, 3> lane_storage;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes;
	for (idx_t lane_idx = 0; lane_idx < lane_storage.size(); lane_idx++) {
		auto &lane = lane_storage[lane_idx];
		lane.ready = true;
		lane.kind = AggregatePrimitiveUpdateKind::SUM_INT64;
		lane.state_size = sizeof(int64_t) + sizeof(uint64_t);
		lane.state_offset = lane_idx * lane.state_size;
		lane.state_value_offset = 0;
		lane.state_is_set_offset = sizeof(int64_t);
		lanes.push_back(&lane);
	}

	SljitPreaggregatedPrimitiveAggregateScratch scratch;
	scratch.PrepareSharedAffine(lanes, 1);
	scratch.group_row_counts.push_back(3);
	SljitAppendSharedAffineValue(scratch, hugeint_t(6));
	scratch.shared_valid_counts.push_back(0);
	scratch.shared_valid_counts_are_row_counts = true;

	SljitExecutableFusedAffineRunUpdate affine;
	affine.source_position = 0;
	affine.source_type = PhysicalType::INT32;
	affine.primitive_kind = AggregatePrimitiveUpdateKind::SUM_INT64;
	affine.lanes = {{1, 0}, {1, 1}, {1, 2}};
	affine.lanes_form_arithmetic_progression = true;
	affine.lane_step = {0, 1};
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(lanes, scratch, &affine);
	REQUIRE(update_state.shared_affine_canonical_states);
	REQUIRE(update_state.shared_affine_state_stride == sizeof(int64_t) + sizeof(uint64_t));

	std::array<uint8_t, 3 * (sizeof(int64_t) + sizeof(uint64_t))> states;
	states.fill(0xa5);
	const uintptr_t addresses[] = {reinterpret_cast<uintptr_t>(states.data())};
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(
	    addresses, nullptr, 1, ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE, &update_state);
	for (idx_t lane_idx = 0; lane_idx < lane_storage.size(); lane_idx++) {
		auto state = states.data() + lane_storage[lane_idx].state_offset;
		REQUIRE(Load<int64_t>(state) == int64_t(6 + 3 * lane_idx));
		REQUIRE(Load<bool>(state + sizeof(int64_t)));
		for (idx_t tail_idx = 1; tail_idx < sizeof(uint64_t); tail_idx++) {
			REQUIRE(state[sizeof(int64_t) + tail_idx] == 0);
		}
	}
	REQUIRE(SljitMaterializeSharedAffineValidCounts(scratch));
	REQUIRE_FALSE(scratch.shared_valid_counts_are_row_counts);
	REQUIRE(scratch.shared_valid_counts == vector<idx_t> {3});
}

TEST_CASE("JIT shared affine hugeint progressions initialize canonical aggregate states once", "[api][jit]") {
	std::array<ExecutionPrimitiveAggregateUpdateLane, 3> lane_storage;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes;
	for (idx_t lane_idx = 0; lane_idx < lane_storage.size(); lane_idx++) {
		auto &lane = lane_storage[lane_idx];
		lane.ready = true;
		lane.kind = AggregatePrimitiveUpdateKind::SUM_HUGEINT;
		lane.state_size = sizeof(hugeint_t) + sizeof(uint64_t);
		lane.state_offset = lane_idx * lane.state_size;
		lane.state_value_offset = 0;
		lane.state_is_set_offset = sizeof(hugeint_t);
		lanes.push_back(&lane);
	}

	SljitPreaggregatedPrimitiveAggregateScratch scratch;
	scratch.PrepareSharedAffine(lanes, 3);
	const vector<idx_t> valid_counts {3, 2, 3};
	const vector<hugeint_t> shared_values {hugeint_t(6), hugeint_t(-6),
	                                       hugeint_t(NumericLimits<int64_t>::Maximum()) + hugeint_t(17)};
	for (idx_t row_idx = 0; row_idx < shared_values.size(); row_idx++) {
		scratch.group_row_counts.push_back(valid_counts[row_idx]);
		SljitAppendSharedAffineValue(scratch, shared_values[row_idx]);
		scratch.shared_valid_counts.push_back(0);
	}
	scratch.shared_valid_counts_are_row_counts = true;

	SljitExecutableFusedAffineRunUpdate affine;
	affine.source_position = 0;
	affine.source_type = PhysicalType::INT64;
	affine.primitive_kind = AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	affine.lanes = {{1, 0}, {1, 1}, {1, 2}};
	affine.lanes_form_arithmetic_progression = true;
	affine.lane_step = {0, 1};
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(lanes, scratch, &affine);
	REQUIRE(update_state.shared_affine_canonical_states);
	REQUIRE(update_state.shared_affine_state_stride == sizeof(hugeint_t) + sizeof(uint64_t));

	const auto row_state_size = lane_storage.size() * (sizeof(hugeint_t) + sizeof(uint64_t));
	std::array<uint8_t, 3 * 3 * (sizeof(hugeint_t) + sizeof(uint64_t))> states;
	states.fill(0xa5);
	const uintptr_t addresses[] = {reinterpret_cast<uintptr_t>(states.data()),
	                               reinterpret_cast<uintptr_t>(states.data() + row_state_size),
	                               reinterpret_cast<uintptr_t>(states.data() + 2 * row_state_size)};
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(
	    addresses, nullptr, shared_values.size(),
	    ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE, &update_state);
	for (idx_t row_idx = 0; row_idx < shared_values.size(); row_idx++) {
		for (idx_t lane_idx = 0; lane_idx < lane_storage.size(); lane_idx++) {
			auto state = states.data() + row_idx * row_state_size + lane_storage[lane_idx].state_offset;
			const auto lane_delta =
			    hugeint_t(NumericCast<int64_t>(valid_counts[row_idx])) * hugeint_t(NumericCast<int64_t>(lane_idx));
			REQUIRE(Load<hugeint_t>(state) == shared_values[row_idx] + lane_delta);
			REQUIRE(Load<bool>(state + sizeof(hugeint_t)));
			for (idx_t tail_idx = 1; tail_idx < sizeof(uint64_t); tail_idx++) {
				REQUIRE(state[sizeof(hugeint_t) + tail_idx] == 0);
			}
		}
	}
}

TEST_CASE("JIT ungrouped aggregate sinks use native state-update contracts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_boundary AS "
	                          "SELECT i::BIGINT AS i, CASE WHEN i % 5 = 0 THEN NULL ELSE i::BIGINT END AS v "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager);
	auto result = con.Query("SELECT sum(v) FROM jit_aggregate_boundary WHERE i > 100");
	REQUIRE_NO_FAIL(*result);

	bool found_aggregate_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		found_aggregate_update = true;
		RequireGeneratedMachineCodeRegion(event);
		RequireGeneratedSourceFilterContract(event);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.reason, "ungrouped-aggregate-native-state-update"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_aggregate_update);
}

TEST_CASE("JIT fuses projection payloads into primitive ungrouped aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_payload AS "
	                          "SELECT (i % 1000)::BIGINT AS a, (i % 10)::BIGINT AS b "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_aggregate_payload");
	REQUIRE_NO_FAIL(*result);

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:")) {
			continue;
		}
		found_compile = true;
		INFO(event.ir);
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
	}
	REQUIRE(found_compile);

	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(event.invocation_count > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "aggregate_update"));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                   "aggregate_update.primitive_payload_update="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT primitive decimal aggregate payloads elide stats-proven checks", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_primitive_safe_payload AS "
	                          "SELECT CAST(i % 1000 AS DECIMAL(15,2)) AS extended_price, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT sum(extended_price * discount) FROM jit_decimal_primitive_safe_payload";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:decimal64-multiply-references")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir,
		                             "primitive_payloads=native:decimal64-multiply-references:no-overflow:in-range"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT widening decimal products feed exact hugeint aggregate states", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_wide_decimal_product AS "
	                          "SELECT (i % 10000)::INTEGER AS g, "
	                          "CASE WHEN i % 17 = 0 THEN NULL "
	                          "     ELSE CAST((i % 2001) - 1000 AS DECIMAL(15,2)) END AS d, "
	                          "CASE WHEN i % 19 = 0 THEN NULL "
	                          "     ELSE ((i % 100000) - 50000)::BIGINT END AS q "
	                          "FROM range(200000) tbl(i)"));

	const string grouped_query = "SELECT g, sum(d * q) AS v FROM jit_wide_decimal_product GROUP BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_wide_decimal_reference AS " + grouped_query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_wide_decimal_result AS " + grouped_query));
	auto difference =
	    con.Query("SELECT count(*) FROM ((SELECT * FROM jit_wide_decimal_reference EXCEPT ALL "
	              "SELECT * FROM jit_wide_decimal_result) UNION ALL (SELECT * FROM jit_wide_decimal_result EXCEPT ALL "
	              "SELECT * FROM jit_wide_decimal_reference))");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).GetValue<int64_t>() == 0);

	bool found_compile = false;
	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		    StringUtil::Contains(event.ir, "native:decimal128-widening-multiply")) {
			found_compile = true;
			RequireGeneratedMachineCodeRegion(event);
		}
		if (EventPhase(event) == "runtime" && event.backend_name == "sljit" && event.invocation_count > 0 &&
		    StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "projection")) {
			found_runtime = true;
		}
	}
	REQUIRE(found_compile);
	REQUIRE(found_runtime);
}

TEST_CASE("JIT fuses generated filters into primitive ungrouped aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_aggregate_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b "
	                          "FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(a * b) FROM jit_filtered_aggregate_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b) FROM jit_filtered_aggregate_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_filtered_aggregate = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_aggregate = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_filtered_aggregate);
}

TEST_CASE("JIT filtered aggregate remaps expression-tree sources after generated source-filter projection",
          "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_aggregate_source_remap AS "
	                          "SELECT CASE i % 10 "
	                          "       WHEN 0 THEN 0::BIGINT "
	                          "       WHEN 1 THEN 1::BIGINT "
	                          "       WHEN 2 THEN 2::BIGINT "
	                          "       WHEN 3 THEN 3::BIGINT "
	                          "       WHEN 4 THEN 4::BIGINT "
	                          "       WHEN 5 THEN 5::BIGINT "
	                          "       WHEN 6 THEN 6::BIGINT "
	                          "       WHEN 7 THEN 7::BIGINT "
	                          "       WHEN 8 THEN 8::BIGINT "
	                          "       ELSE 9::BIGINT END AS discount, "
	                          "       CAST(1 + (i % 50) AS BIGINT) AS quantity, "
	                          "       CAST(100 + (i % 1000) AS BIGINT) AS extendedprice "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT sum(extendedprice * discount) "
	                     "FROM jit_filtered_aggregate_source_remap "
	                     "WHERE discount BETWEEN 5 AND 7 "
	                     "  AND quantity < 24";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_scan_filtered_aggregate = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || event.candidate_traits.source_filter_count <= 1 ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			continue;
		}
		found_scan_filtered_aggregate = true;
		RequireGeneratedSourceFilterContract(event);
	}
	REQUIRE(found_scan_filtered_aggregate);
}

TEST_CASE("JIT fuses generated filters into primitive count-star aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_count_star_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b "
	                          "FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT count(*) FROM jit_filtered_count_star_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*) FROM jit_filtered_count_star_payload WHERE (a + b) > 1000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_filtered_count = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_count = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_filtered_count);
}

TEST_CASE("JIT fuses generated filters into multiple primitive aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_multi_aggregate_payload AS "
	                          "SELECT CASE WHEN i % 9973 = 0 THEN NULL ELSE i::BIGINT END AS a, "
	                          "CASE WHEN i % 7919 = 0 THEN NULL ELSE (i + 3)::BIGINT END AS b, "
	                          "CASE WHEN i % 3571 = 0 THEN NULL ELSE (i * 10)::BIGINT END AS d "
	                          "FROM range(100000) tbl(i)"));

	const string query = "SELECT count(*), sum(a * b), sum(d) "
	                     "FROM jit_filtered_multi_aggregate_payload WHERE (a + b) > 1000";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).ToString() == reference->GetValue(2, 0).ToString());

	bool found_filtered_multi = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		auto stage_counts = EventGeneratedStageCountBreakdown(event);
		if (!StringUtil::Contains(stage_counts, "aggregate_update.filtered_primitive_update=")) {
			continue;
		}
		found_filtered_multi = true;
		REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter.selection="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "filter="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update="));
		REQUIRE_FALSE(StringUtil::Contains(stage_counts, "aggregate_update.primitive_payload_update_fused="));
	}
	REQUIRE(found_filtered_multi);
}

TEST_CASE("JIT auto CBO selects high-work filtered primitive aggregate fusion", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, true, 10000);
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=4096"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_filtered_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i + 3)::BIGINT AS b, (i % 97)::BIGINT AS c "
	                          "FROM range(500000) tbl(i)"));

	const string payload = "((((a * b) + (a * 7)) - (b * 13)) + ((a + c) * (b - c)))";
	const string predicate = "((a + b) > 1000) AND (((a * 3) + (b * 5)) > 10000)";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference =
	    con.Query("SELECT sum(" + payload + ") FROM jit_auto_filtered_aggregate_payload WHERE " + predicate);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(" + payload + ") FROM jit_auto_filtered_aggregate_payload WHERE " + predicate);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED);
		    REQUIRE(event.runner_cost.present);
		    REQUIRE(event.runner_cost.SelectedAcceleratedRunner());
		    REQUIRE(event.runner_cost.materialization_elision_count == 1);
		    REQUIRE(event.runner_cost.accelerated_runner_benefit > event.runner_cost.required_benefit);
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		    RequireGeneratedMachineCodeRegion(event);
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.filtered_primitive_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "filter.selection="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                       "aggregate_update.primitive_payload_update="));
	    });
}

TEST_CASE("JIT fuses multiple primitive ungrouped aggregate payload lanes into one reducer", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i + 1)::BIGINT AS b "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum(a * b), count(*) FROM jit_multi_aggregate_payload");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "333333330000");
	REQUIRE(result->GetValue(1, 0).ToString() == "10000");

	bool found_fused_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_fused_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.primitive_payload_update="));
	}
	REQUIRE(found_fused_runtime);
}

TEST_CASE("JIT fuses exact-width ungrouped hugeint reference lanes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_hugeint_payload AS "
	                          "SELECT CASE WHEN i % 7 = 0 THEN NULL "
	                          "            ELSE i::HUGEINT * 1000000000000::HUGEINT END AS a, "
	                          "       CASE WHEN i % 11 = 0 THEN NULL "
	                          "            ELSE -i::HUGEINT * 1000000000000000::HUGEINT END AS b "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT sum(a), sum(b), count(*) FROM jit_multi_hugeint_payload";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.fused_payload_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.primitive_payload_update="));
	    });
}

TEST_CASE("JIT fuses decimal CASE aggregate payload lanes", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, true, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_payload_probe AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       100.00::DECIMAL(15,2) + (i % 13)::DECIMAL(15,2) AS base_amount, "
	                          "       0.10::DECIMAL(15,2) AS rebate_rate "
	                          "FROM range(0, 20000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_case_payload_dim AS "
	                          "SELECT i::BIGINT AS item_key, "
	                          "       CASE WHEN i % 7 IN (0, 3) THEN 'PRIORITY BRUSHED STEEL' "
	                          "            ELSE 'STANDARD ANODIZED COPPER' END AS category_name "
	                          "FROM range(0, 20000) tbl(i)"));

	const string query = "SELECT sum(CASE WHEN category_name LIKE 'PRIORITY%' "
	                     "                THEN base_amount * (1.00 - rebate_rate) "
	                     "                ELSE 0.0000 END) AS matched_sum, "
	                     "       sum(base_amount * (1.00 - rebate_rate)) AS total_sum "
	                     "FROM jit_decimal_case_payload_probe, jit_decimal_case_payload_dim "
	                     "WHERE jit_decimal_case_payload_probe.item_key = jit_decimal_case_payload_dim.item_key";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "hash_join_probe") &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "case<logical=DECIMAL"));
		    REQUIRE(StringUtil::Contains(event.ir, "string_prefix"));
	    });

	bool found_fused_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsGeneratedAggregateUpdateRuntime(event) || !HasGeneratedAggregateUpdateStage(event)) {
			continue;
		}
		found_fused_runtime = true;
		RequireGeneratedAggregateUpdateRuntimeOwnership(event);
	}
	REQUIRE(found_fused_runtime);
}

TEST_CASE("JIT typed aggregate descriptors preserve mixed BIGINT and DECIMAL64 lowering", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_mixed_typed_payload AS "
	                          "SELECT i::BIGINT AS i, (100.00 + (i % 17))::DECIMAL(15,2) AS amount, "
	                          "       0.10::DECIMAL(15,2) AS rate "
	                          "FROM range(0, 20000) tbl(i)"));
	const string query = "SELECT sum(CASE WHEN i % 3 = 0 THEN i * 2 ELSE i + 7 END), "
	                     "       sum(CASE WHEN i % 5 = 0 THEN amount * (1.00 - rate) ELSE 0.0000 END) "
	                     "FROM jit_mixed_typed_payload";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "case<logical=BIGINT"));
		    REQUIRE(StringUtil::Contains(event.ir, "case<logical=DECIMAL"));
	    });
	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT perfect hash aggregate composes date-year group projection into fused payload update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_date_year_group AS "
	                          "SELECT DATE '1995-01-01' + (i % 700)::INTEGER AS d, "
	                          "       (i % 100)::DECIMAL(15,2) AS ep, "
	                          "       0.05::DECIMAL(15,2) AS disc, "
	                          "       CASE WHEN i % 2 = 0 THEN 'BRAZIL' ELSE 'PERU' END AS n "
	                          "FROM range(0, 10000) tbl(i)"));

	const string query = "SELECT year(d) AS y, "
	                     "       sum(CASE WHEN n='BRAZIL' THEN ep * (1 - disc) ELSE 0 END) AS a, "
	                     "       sum(ep * (1 - disc)) AS b "
	                     "FROM jit_perfect_hash_date_year_group GROUP BY 1 ORDER BY 1";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < reference->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < reference->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "group_expressions=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "execution:native-sljit-region-aggregate-update"));
		    REQUIRE(StringUtil::Contains(event.ir, "integral_compress"));
		    REQUIRE(StringUtil::Contains(event.ir, "date_year"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "op0=projection("));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
	    });
}

TEST_CASE("JIT generic grouped primitive aggregate payload lanes use native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_multi_aggregate_payload AS "
	                          "SELECT i::BIGINT AS k, i::BIGINT AS v "
	                          "FROM range(200000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT k, sum(v), count(*) FROM jit_grouped_multi_aggregate_payload GROUP BY k");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 200000);

	bool found_hash_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			found_hash_state_address_update = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		}
	}
	REQUIRE(found_hash_state_address_update);

	bool found_direct_runtime = false;
	idx_t grouped_sink_bind_count = 0;
	for (auto &event : manager.GetEvents()) {
		if (!IsGeneratedAggregateUpdateRuntime(event)) {
			continue;
		}
		found_direct_runtime = true;
		idx_t event_grouped_sink_bind_count = 0;
		for (auto &stage : event.generated_stage_runtime) {
			if (StringUtil::Contains(stage.stage.name, "aggregate_update.bind_sink_contract")) {
				event_grouped_sink_bind_count += stage.count;
			}
		}
		grouped_sink_bind_count += event_grouped_sink_bind_count;
		REQUIRE(event_grouped_sink_bind_count <= 1);
		RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		REQUIRE_FALSE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                   "aggregate_update.fused_payload_update_with_grouped_state_addresses="));
	}
	REQUIRE(found_direct_runtime);
	REQUIRE(grouped_sink_bind_count > 0);
}

TEST_CASE("Grouped aggregate dense lookup supports nullable all-valid group layouts", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	const idx_t group_count = STANDARD_VECTOR_SIZE;
	DataChunk groups;
	groups.Initialize(allocator, {LogicalType::INTEGER});
	auto group_data = FlatVector::GetDataMutable<int32_t>(groups.data[0]);
	for (idx_t row_idx = 0; row_idx < group_count; row_idx++) {
		group_data[row_idx] = UnsafeNumericCast<int32_t>(row_idx);
	}
	groups.SetChildCardinality(group_count);

	ExecutionDenseGroupDomain dense_domain;
	dense_domain.ready = true;
	dense_domain.physical_type = PhysicalType::INT32;
	dense_domain.min_key = 0;
	dense_domain.max_key = group_count - 1;
	dense_domain.distinct_count = group_count;

	struct DenseLookupUpdateState {
		DataChunk *groups = nullptr;
		vector<uintptr_t> address_by_key;
		bool expect_existing = false;
		idx_t update_count = 0;
	};
	auto record_addresses = [](const uintptr_t *addresses, const sel_t *address_sel, const sel_t *execute_sel,
	                           idx_t count, void *state_p) {
		auto &state = *reinterpret_cast<DenseLookupUpdateState *>(state_p);
		REQUIRE(addresses);
		REQUIRE(state.groups);
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			const auto address_idx = address_sel ? address_sel[row_idx] : idx;
			const auto address = addresses[address_idx];
			REQUIRE(address != 0);
			const auto key = state.groups->data[0].GetValue(row_idx).GetValue<int32_t>();
			REQUIRE(key >= 0);
			REQUIRE(UnsafeNumericCast<idx_t>(key) < state.address_by_key.size());
			auto &stored_address = state.address_by_key[UnsafeNumericCast<idx_t>(key)];
			if (state.expect_existing) {
				REQUIRE(stored_address == address);
			} else {
				REQUIRE(stored_address == 0);
				stored_address = address;
			}
			state.update_count++;
		}
	};

	DenseLookupUpdateState state;
	state.groups = &groups;
	state.address_by_key.resize(group_count, 0);
	REQUIRE(ht.TryFindOrCreateGroupsSelectedStateUpdateFast(groups, record_addresses, &state, nullptr, nullptr,
	                                                        &dense_domain));
	REQUIRE(state.update_count == group_count);
	REQUIRE(ht.Count() == group_count);

	const idx_t selected_count = 512;
	vector<sel_t> selected_rows;
	selected_rows.reserve(selected_count);
	for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
		selected_rows.push_back(UnsafeNumericCast<sel_t>(selected_count - row_idx - 1));
	}
	SelectionVector selected_row_selection(selected_rows.data(), selected_count);
	DataChunk selected_groups;
	selected_groups.Initialize(allocator, {LogicalType::INTEGER});
	selected_groups.data[0].Slice(groups.data[0], selected_row_selection, selected_count);
	selected_groups.SetChildCardinality(selected_count);

	state.groups = &selected_groups;
	state.expect_existing = true;
	state.update_count = 0;
	REQUIRE(ht.TryFindOrCreateGroupsSelectedStateUpdateFast(selected_groups, record_addresses, &state, nullptr, nullptr,
	                                                        &dense_domain));
	REQUIRE(state.update_count == selected_count);
	REQUIRE(ht.Count() == group_count);
}

static ExecutionRowPointerGroupKeySource BuildBigintToIntegerInputVectorGroupSource() {
	ExecutionRowPointerGroupKeySource source;
	source.ready = true;
	source.source_kind = ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR;
	source.source_type = LogicalType::BIGINT;
	source.target_type = LogicalType::INTEGER;
	source.source_physical_type = PhysicalType::INT64;
	source.target_physical_type = PhysicalType::INT32;
	source.input_vector_index = 0;
	source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32;
	source.unchecked_integral_cast = true;
	source.all_valid = true;
	return source;
}

TEST_CASE("Grouped aggregate input-vector group targets use descriptor lookup directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	const idx_t group_count = STANDARD_VECTOR_SIZE;
	DataChunk payload_input;
	payload_input.Initialize(allocator, {LogicalType::BIGINT});
	auto input_data = FlatVector::GetDataMutable<int64_t>(payload_input.data[0]);
	for (idx_t row_idx = 0; row_idx < group_count; row_idx++) {
		input_data[row_idx] = UnsafeNumericCast<int64_t>(row_idx);
	}
	FlatVector::SetSize(payload_input.data[0], group_count);
	payload_input.SetChildCardinality(group_count);

	vector<ExecutionRowPointerGroupKeySource> group_sources;
	group_sources.push_back(BuildBigintToIntegerInputVectorGroupSource());

	auto verify_targets = [&](DataChunk &input, ExecutionGroupedAggregateStateTargetBatch &targets,
	                          vector<uintptr_t> &address_by_key, bool expect_existing) {
		auto &span = targets.InputOrder();
		REQUIRE(span.addresses);
		REQUIRE(span.count == input.size());
		for (idx_t target_idx = 0; target_idx < span.count; target_idx++) {
			const auto row_idx = span.row_sel ? span.row_sel[target_idx] : target_idx;
			const auto address_idx = span.address_sel ? span.address_sel[target_idx] : target_idx;
			const auto address = span.addresses[address_idx];
			REQUIRE(address != 0);
			const auto key = input.data[0].GetValue(row_idx).GetValue<int64_t>();
			REQUIRE(key >= 0);
			REQUIRE(UnsafeNumericCast<idx_t>(key) < address_by_key.size());
			auto &stored_address = address_by_key[UnsafeNumericCast<idx_t>(key)];
			if (expect_existing) {
				REQUIRE(stored_address == address);
			} else {
				REQUIRE(stored_address == 0);
				stored_address = address;
			}
		}
	};

	ExecutionGroupedAggregateStateTargetBatch targets;
	vector<uintptr_t> address_by_key(group_count, 0);
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(payload_input, payload_input.size(), group_sources,
	                                                           targets));
	verify_targets(payload_input, targets, address_by_key, false);
	REQUIRE(ht.Count() == group_count);

	const idx_t selected_count = 512;
	vector<sel_t> selected_rows;
	selected_rows.reserve(selected_count);
	for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
		selected_rows.push_back(UnsafeNumericCast<sel_t>(selected_count - row_idx - 1));
	}
	SelectionVector selected_row_selection(selected_rows.data(), selected_count);
	DataChunk selected_payload_input;
	selected_payload_input.Initialize(allocator, {LogicalType::BIGINT});
	selected_payload_input.data[0].Slice(payload_input.data[0], selected_row_selection, selected_count);
	selected_payload_input.SetChildCardinality(selected_count);

	targets.Reset();
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(selected_payload_input, selected_payload_input.size(),
	                                                           group_sources, targets));
	verify_targets(selected_payload_input, targets, address_by_key, true);
	REQUIRE(ht.Count() == group_count);
}

TEST_CASE("Grouped aggregate input-vector targets rebuild dense cache from existing groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &allocator = Allocator::Get(*con.context);

	GroupedAggregateHashTable ht(*con.context, allocator, {LogicalType::INTEGER},
	                             TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	DataChunk existing_groups;
	existing_groups.Initialize(allocator, {LogicalType::INTEGER});
	auto existing_group_data = FlatVector::GetDataMutable<int32_t>(existing_groups.data[0]);
	existing_group_data[0] = 0;
	FlatVector::SetSize(existing_groups.data[0], 1);
	existing_groups.SetChildCardinality(1);

	Vector existing_addresses(LogicalType::POINTER);
	ht.FindOrCreateGroups(existing_groups, existing_addresses);
	existing_addresses.Flatten();
	const auto existing_group_address =
	    reinterpret_cast<uintptr_t>(FlatVector::GetData<data_ptr_t>(existing_addresses)[0]);
	REQUIRE(existing_group_address != 0);
	REQUIRE(ht.Count() == 1);

	DataChunk payload_input;
	payload_input.Initialize(allocator, {LogicalType::BIGINT});
	auto input_data = FlatVector::GetDataMutable<int64_t>(payload_input.data[0]);
	input_data[0] = 0;
	input_data[1] = 1;
	input_data[2] = 2;
	FlatVector::SetSize(payload_input.data[0], 3);
	payload_input.SetChildCardinality(3);

	ExecutionDenseGroupDomain dense_domain;
	dense_domain.ready = true;
	dense_domain.physical_type = PhysicalType::INT32;
	dense_domain.min_key = 0;
	dense_domain.max_key = 2;
	dense_domain.distinct_count = 3;

	vector<ExecutionRowPointerGroupKeySource> group_sources;
	group_sources.push_back(BuildBigintToIntegerInputVectorGroupSource());

	ExecutionGroupedAggregateStateTargetBatch targets;
	REQUIRE(ht.TryFindOrCreateInputVectorGroupStateTargetsFast(payload_input, payload_input.size(), group_sources,
	                                                           targets, nullptr, &dense_domain));
	auto &span = targets.InputOrder();
	REQUIRE(span.addresses);
	REQUIRE(span.count == 3);
	REQUIRE(span.addresses[0] == existing_group_address);
	REQUIRE(span.addresses[1] != 0);
	REQUIRE(span.addresses[2] != 0);
	REQUIRE(span.addresses[1] != existing_group_address);
	REQUIRE(span.addresses[2] != existing_group_address);
	REQUIRE(ht.Count() == 3);
}

TEST_CASE("JIT regular hash aggregate updates mixed preaggregated groups directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_existing_grouped_update AS "
	                          "SELECT CASE WHEN i % 4 = 0 THEN 'A' "
	                          "            WHEN i % 4 = 1 THEN 'B' "
	                          "            WHEN i % 4 = 2 THEN 'C' ELSE 'D' END AS k, "
	                          "       (i % 100)::BIGINT AS v "
	                          "FROM range(200000) tbl(i)"));

	const string query = "SELECT k, sum(v), count(*) FROM jit_existing_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 4);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsGeneratedAggregateUpdateRuntime(event) && HasGeneratedAggregateUpdateStage(event);
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT projected grouped primitive updates existing groups directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preaggregated_existing_grouped_update AS "
	                          "SELECT ((i // 2) % 2)::INTEGER AS k, "
	                          "       (i % 2 = 0)::INTEGER AS high_payload, "
	                          "       (i % 2 <> 0)::INTEGER AS low_payload "
	                          "FROM range(200000) tbl(i)"));

	const string query = "SELECT k, "
	                     "       sum(high_payload * 3 + 1), "
	                     "       sum(low_payload * 5 + 2) "
	                     "FROM jit_preaggregated_existing_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}
	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT preaggregated grouped aggregate avoids source-row reserve", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_preaggregated_group_reserve AS "
	                          "SELECT ((i // 2) % 100000)::INTEGER AS k, "
	                          "       (i % 2 = 0)::INTEGER AS high_payload, "
	                          "       (i % 2 <> 0)::INTEGER AS low_payload "
	                          "FROM range(200000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_preaggregated_group_reserve"));

	const string query = "SELECT k, "
	                     "       sum(high_payload), "
	                     "       sum(low_payload) "
	                     "FROM jit_preaggregated_group_reserve GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_preaggregated_group_reserve_reference AS " + query));
	auto reference_count = con.Query("SELECT count(*) FROM jit_preaggregated_group_reserve_reference");
	REQUIRE_NO_FAIL(*reference_count);
	REQUIRE(reference_count->GetValue(0, 0).GetValue<int64_t>() == 100000);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_preaggregated_group_reserve_output AS " + query));
	auto diff = con.Query("SELECT count(*) FROM ("
	                      "  (SELECT * FROM jit_preaggregated_group_reserve_output "
	                      "   EXCEPT ALL SELECT * FROM jit_preaggregated_group_reserve_reference) "
	                      "  UNION ALL "
	                      "  (SELECT * FROM jit_preaggregated_group_reserve_reference "
	                      "   EXCEPT ALL SELECT * FROM jit_preaggregated_group_reserve_output)"
	                      ") diff");
	REQUIRE_NO_FAIL(*diff);
	REQUIRE(diff->GetValue(0, 0).GetValue<int64_t>() == 0);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    return StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.grouped_aggregate_reserve_target=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    const auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve_target="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve_target=200000"));
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve=1"));
		    REQUIRE_FALSE(StringUtil::Contains(stage_counts, "grouped_aggregate_reserve.reserve_groups.resize="));
	    });
}

TEST_CASE("JIT join-expanded unique group keys reserve input-vector aggregate groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_group_reserve_lhs AS "
	                          "SELECT i::INTEGER AS k FROM range(100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_group_reserve_rhs AS "
	                          "SELECT (i % 100000)::INTEGER AS k FROM range(400000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_join_group_reserve_lhs"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_join_group_reserve_rhs"));

	const string query = "SELECT lhs.k, count(rhs.k) "
	                     "FROM jit_join_group_reserve_lhs lhs "
	                     "LEFT JOIN jit_join_group_reserve_rhs rhs ON lhs.k = rhs.k "
	                     "GROUP BY lhs.k ORDER BY lhs.k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 100000);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve_target=") &&
		           HasJitAggregateUpdatePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    REQUIRE(StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve=1"));
		    REQUIRE_FALSE(
		        StringUtil::Contains(runtime_paths, "aggregate_update.grouped_aggregate_reserve_target=400000"));
	    });
}

TEST_CASE("JIT count-star distribution aggregate uses dense preaggregated groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_distribution_ids AS "
	                          "SELECT i::BIGINT AS id FROM range(4200) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_distribution_orders AS "
	                          "SELECT id, seq "
	                          "FROM jit_count_star_distribution_ids, range(42) reps(seq) "
	                          "WHERE seq < id % 42"));

	const string query = "SELECT c_count, count(*) AS customer_count "
	                     "FROM ("
	                     "    SELECT ids.id, count(orders.seq) AS c_count "
	                     "    FROM jit_count_star_distribution_ids ids "
	                     "    LEFT JOIN jit_count_star_distribution_orders orders ON ids.id = orders.id "
	                     "    GROUP BY ids.id"
	                     ") grouped_counts "
	                     "GROUP BY c_count ORDER BY c_count";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 42);
	for (idx_t row_idx = 0; row_idx < reference->RowCount(); row_idx++) {
		REQUIRE(reference->GetValue(0, row_idx).ToString() == to_string(row_idx));
		REQUIRE(reference->GetValue(1, row_idx).ToString() == "100");
	}

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}
	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT direct join aggregate accumulates dense count-one groups across batches", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_dense_pending_count_lhs AS "
	                          "SELECT i::BIGINT AS id FROM range(1, 150001) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_dense_pending_count_rhs AS "
	                          "SELECT ((i * 7919) % 150000 + 1)::BIGINT AS id, i::BIGINT AS seq "
	                          "FROM range(1500000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_dense_pending_count_lhs"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_dense_pending_count_rhs"));

	const string query = "SELECT lhs.id, count(rhs.seq) AS c "
	                     "FROM jit_dense_pending_count_lhs lhs "
	                     "LEFT JOIN jit_dense_pending_count_rhs rhs ON lhs.id = rhs.id "
	                     "GROUP BY lhs.id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dense_pending_count_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_dense_pending_count_output AS " + query));
	auto result_shape = con.Query("SELECT count(*), min(c), max(c) FROM jit_dense_pending_count_output");
	REQUIRE_NO_FAIL(*result_shape);
	REQUIRE(result_shape->GetValue(0, 0).ToString() == "150000");
	REQUIRE(result_shape->GetValue(1, 0).ToString() == "10");
	REQUIRE(result_shape->GetValue(2, 0).ToString() == "10");
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_dense_pending_count_output EXCEPT ALL "
	                            "   SELECT * FROM jit_dense_pending_count_reference) UNION ALL "
	                            "  (SELECT * FROM jit_dense_pending_count_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_dense_pending_count_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO(observed_runtime_paths);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_dense_single_lane_grouped_update=1500000") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_dense_single_lane_grouped_update_flush=1500000");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT sorted grouped sums preserve compact runs across scheduler yields", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_sorted_grouped_sum AS "
	                          "SELECT (i // 3)::BIGINT AS group_id, "
	                          "       CASE WHEN i >= 131070 THEN NULL ELSE (i % 97)::INTEGER END AS value "
	                          "FROM range(131072) tbl(i)"));

	const string query = "SELECT group_id, sum(value) AS value_sum "
	                     "FROM jit_sorted_grouped_sum GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_sorted_grouped_sum_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_sorted_grouped_sum_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_sorted_grouped_sum_output EXCEPT ALL "
	                            "   SELECT * FROM jit_sorted_grouped_sum_reference) UNION ALL "
	                            "  (SELECT * FROM jit_sorted_grouped_sum_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_sorted_grouped_sum_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	auto null_tail = con.Query("SELECT count(*) FROM jit_sorted_grouped_sum_output "
	                           "WHERE group_id=43690 AND value_sum IS NULL");
	REQUIRE_NO_FAIL(*null_tail);
	REQUIRE(null_tail->GetValue(0, 0).ToString() == "1");
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO(observed_runtime_paths);
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	idx_t generated_rows = 0;
	idx_t flushed_rows = 0;
	idx_t generated_state_initializations = 0;
	idx_t generated_runtime_count = 0;
	idx_t lazy_codegen_code_size = 0;
	bool proven_unique_append_enabled = false;
	bool producer_order_proof = false;
	bool final_combine_required = false;
	idx_t grouped_aggregate_reserve_target = 0;
	bool grouped_aggregate_reserve_resized = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit" ||
		    !StringUtil::Contains(EventJitRuntimePathCounts(event),
		                          "aggregate_update.generated_pending_primitive_group_runs=")) {
			continue;
		}
		generated_runtime_count++;
		lazy_codegen_code_size += event.jit_runtime.lazy_codegen.code_size;
		RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		grouped_aggregate_reserve_resized =
		    grouped_aggregate_reserve_resized ||
		    StageNameContains(event.generated_stage_runtime, "grouped_aggregate_reserve.reserve_groups.resize");
		for (auto &counter : event.jit_runtime.runtime_path_counts) {
			if (counter.counter.name == "aggregate_update.generated_pending_primitive_group_runs") {
				generated_rows += counter.count;
			} else if (counter.counter.name == "aggregate_update.pending_preaggregated_grouped_update_flush") {
				flushed_rows += counter.count;
			} else if (counter.counter.name == "aggregate_update.generated_preaggregated_state_initialize") {
				generated_state_initializations += counter.count;
			} else if (counter.counter.name == "aggregate_update.proven_unique_append.enabled") {
				proven_unique_append_enabled = true;
			} else if (counter.counter.name == "aggregate_update.proven_unique_append.producer_order_proof") {
				producer_order_proof = true;
			} else if (counter.counter.name == "aggregate_update.proven_unique_append.final_combine_required") {
				final_combine_required = true;
			} else if (counter.counter.name == "aggregate_update.grouped_aggregate_reserve_target") {
				grouped_aggregate_reserve_target += counter.count;
			}
		}
	}
	REQUIRE(generated_runtime_count > 1);
	REQUIRE(lazy_codegen_code_size > 0);
	REQUIRE(generated_rows == 131072);
	REQUIRE(flushed_rows == 131072);
	REQUIRE(generated_state_initializations > 0);
	REQUIRE(proven_unique_append_enabled);
	REQUIRE(producer_order_proof);
	REQUIRE_FALSE(final_combine_required);
	REQUIRE(grouped_aggregate_reserve_target <= STANDARD_VECTOR_SIZE);
	REQUIRE_FALSE(grouped_aggregate_reserve_resized);
}

TEST_CASE("JIT generated grouped runs publish existing aggregate states", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_repeated_group_runs AS "
	                          "SELECT ((i % 2048) // 4)::BIGINT AS group_id, "
	                          "       CASE WHEN i % 101 = 0 THEN NULL ELSE (i % 97)::INTEGER END AS value "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT group_id, sum(value) AS value_sum, count(value) AS value_count "
	                     "FROM jit_repeated_group_runs GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_repeated_group_runs_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_repeated_group_runs_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_repeated_group_runs_output EXCEPT ALL "
	                            "   SELECT * FROM jit_repeated_group_runs_reference) UNION ALL "
	                            "  (SELECT * FROM jit_repeated_group_runs_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_repeated_group_runs_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	idx_t generated_state_updates = 0;
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		for (auto &counter : event.jit_runtime.runtime_path_counts) {
			if (counter.counter.name == "aggregate_update.generated_preaggregated_state_update") {
				generated_state_updates += counter.count;
			}
		}
	}
	INFO(observed_runtime_paths);
	REQUIRE(generated_state_updates > 0);
}

TEST_CASE("JIT unordered grouped input does not generate unused run kernels", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_unordered_grouped_sum AS "
	                          "SELECT ((i * 7919) % 4096)::BIGINT AS group_id, "
	                          "       (i % 97)::INTEGER AS value "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT group_id, sum(value) AS value_sum "
	                     "FROM jit_unordered_grouped_sum GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_unordered_grouped_sum_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_unordered_grouped_sum_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_unordered_grouped_sum_output EXCEPT ALL "
	                            "   SELECT * FROM jit_unordered_grouped_sum_reference) UNION ALL "
	                            "  (SELECT * FROM jit_unordered_grouped_sum_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_unordered_grouped_sum_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	bool rejected_by_runtime_economics = false;
	idx_t lazy_codegen_code_size = 0;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		const auto runtime_paths = EventJitRuntimePathCounts(event);
		rejected_by_runtime_economics =
		    rejected_by_runtime_economics ||
		    StringUtil::Contains(runtime_paths, "aggregate_update.pending_primitive_group_runs_miss.economics=");
		lazy_codegen_code_size += event.jit_runtime.lazy_codegen.code_size;
	}
	REQUIRE(rejected_by_runtime_economics);
	REQUIRE(lazy_codegen_code_size == 0);
}

TEST_CASE("JIT grouped run codegen bounds wide homogeneous lane code size", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	string columns = "(i // 4)::BIGINT AS group_id";
	for (idx_t lane_idx = 0; lane_idx < 16; lane_idx++) {
		columns += ", ((i + " + to_string(lane_idx) + ") % 97)::INTEGER AS value_" + to_string(lane_idx);
	}
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE jit_grouped_run_lane_budget AS SELECT " + columns + " FROM range(32768) tbl(i)"));

	auto verify_lane_count = [&](idx_t lane_count, bool heterogeneous, bool expect_generated) {
		string aggregates;
		for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
			if (lane_idx > 0) {
				aggregates += ", ";
			}
			if (heterogeneous && lane_idx == 0) {
				aggregates += "count(*) AS lane_0";
			} else {
				aggregates += "sum(value_" + to_string(lane_idx) + ") AS lane_" + to_string(lane_idx);
			}
		}
		const auto query = "SELECT group_id, " + aggregates + " FROM jit_grouped_run_lane_budget GROUP BY group_id";
		const auto prefix = "jit_grouped_run_" + to_string(lane_count) + (heterogeneous ? "_mixed_lane" : "_lane");
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE " + prefix + "_reference AS " + query));

		ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
		REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
		ClearJitTrace(manager, true);
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE " + prefix + "_output AS " + query));
		auto difference = con.Query("SELECT count(*) FROM ("
		                            "  (SELECT * FROM " +
		                            prefix + "_output EXCEPT ALL SELECT * FROM " + prefix +
		                            "_reference) UNION ALL "
		                            "  (SELECT * FROM " +
		                            prefix + "_reference EXCEPT ALL SELECT * FROM " + prefix +
		                            "_output)"
		                            ") differences");
		REQUIRE_NO_FAIL(*difference);
		REQUIRE(difference->GetValue(0, 0).ToString() == "0");

		bool generated = false;
		bool code_miss = false;
		idx_t lazy_codegen_code_size = 0;
		string observed_runtime_paths;
		for (auto &event : manager.GetEvents()) {
			if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
				continue;
			}
			const auto runtime_paths = EventJitRuntimePathCounts(event);
			observed_runtime_paths += runtime_paths + "\n";
			generated = generated ||
			            StringUtil::Contains(runtime_paths, "aggregate_update.generated_pending_primitive_group_runs=");
			code_miss =
			    code_miss || StringUtil::Contains(runtime_paths,
			                                      "aggregate_update.generated_pending_primitive_group_runs_miss.code=");
			lazy_codegen_code_size += event.jit_runtime.lazy_codegen.code_size;
		}
		INFO("lane count " + to_string(lane_count) + " runtime paths:\n" + observed_runtime_paths);
		if (expect_generated && SljitPrimitiveRunCodegenSupported()) {
			REQUIRE(generated);
			REQUIRE_FALSE(code_miss);
			REQUIRE(lazy_codegen_code_size > 0);
		} else {
			REQUIRE_FALSE(generated);
			REQUIRE(code_miss);
			REQUIRE(lazy_codegen_code_size == 0);
		}
		return lazy_codegen_code_size;
	};

	const auto unrolled_code_size = verify_lane_count(8, false, true);
	const auto looped_nine_lane_code_size = verify_lane_count(9, false, true);
	const auto looped_sixteen_lane_code_size = verify_lane_count(16, false, true);
	verify_lane_count(9, true, false);
	if (SljitPrimitiveRunCodegenSupported()) {
		REQUIRE(looped_nine_lane_code_size == looped_sixteen_lane_code_size);
		REQUIRE(looped_sixteen_lane_code_size < unrolled_code_size);
	}
}

TEST_CASE("JIT grouped typed payloads preserve wide shared-source expressions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_shared_source_payload AS "
	                          "SELECT (i // 3)::BIGINT AS group_id, "
	                          "CASE WHEN (i // 3) % 13 = 0 OR i % 11 = 0 THEN NULL "
	                          "ELSE (i % 97)::INTEGER END AS value "
	                          "FROM range(32771) tbl(i)"));

	string aggregates;
	for (idx_t lane_idx = 0; lane_idx < 16; lane_idx++) {
		if (lane_idx > 0) {
			aggregates += ", ";
		}
		string expression;
		switch (lane_idx % 5) {
		case 0:
			expression = "value";
			break;
		case 1:
			expression = "value + " + to_string(lane_idx);
			break;
		case 2:
			expression = "value - " + to_string(lane_idx);
			break;
		case 3:
			expression = to_string(lane_idx) + " - value";
			break;
		default:
			expression = "value * " + to_string((lane_idx % 3) + 2);
			break;
		}
		aggregates += "sum(" + expression + ") AS lane_" + to_string(lane_idx);
	}
	const auto query = "SELECT group_id, " + aggregates + " FROM jit_grouped_shared_source_payload GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_grouped_shared_source_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_grouped_shared_source_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_grouped_shared_source_output EXCEPT ALL "
	                            "   SELECT * FROM jit_grouped_shared_source_reference) UNION ALL "
	                            "  (SELECT * FROM jit_grouped_shared_source_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_grouped_shared_source_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });
	bool generated_affine_runs = false;
	bool generated_affine_miss = false;
	idx_t lazy_codegen_code_size = 0;
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		const auto runtime_paths = EventJitRuntimePathCounts(event);
		observed_runtime_paths += runtime_paths + "\n";
		generated_affine_runs =
		    generated_affine_runs ||
		    StringUtil::Contains(runtime_paths,
		                         "aggregate_update.generated_pending_fused_affine_primitive_group_runs=");
		generated_affine_miss =
		    generated_affine_miss ||
		    StringUtil::Contains(runtime_paths,
		                         "aggregate_update.generated_pending_fused_affine_primitive_group_runs_miss.");
		lazy_codegen_code_size += event.jit_runtime.lazy_codegen.code_size;
	}
	INFO("wide shared-source runtime paths:\n" + observed_runtime_paths);
	REQUIRE(generated_affine_runs);
	REQUIRE_FALSE(generated_affine_miss);
	REQUIRE(lazy_codegen_code_size > 0);
}

TEST_CASE("JIT parallel sorted grouped sums reconcile only overlapping dense runs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 20000, 4);
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_parallel_sorted_grouped_sum AS "
	                          "SELECT (i // 3)::BIGINT AS disjoint_group_id, "
	                          "       ((i // 3) * 3)::BIGINT AS sparse_disjoint_group_id, "
	                          "       (i // 7)::BIGINT AS boundary_group_id, 1::BIGINT AS value "
	                          "FROM range(4000000) tbl(i)"));

	auto verify_query = [&](const string &group_column, const string &expected_count, bool require_local_proof) {
		const auto query = "SELECT count(*), sum(group_sum) FROM ("
		                   "SELECT " +
		                   group_column +
		                   ", sum(value) AS group_sum "
		                   "FROM jit_parallel_sorted_grouped_sum GROUP BY " +
		                   group_column + ") grouped";
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		auto reference = con.Query(query);
		REQUIRE_NO_FAIL(*reference);

		ConfigureSljitForCoverageSettings(con, false, true, true, 20000, 4);
		REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
		ClearJitTrace(manager, true);
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->ColumnCount() == reference->ColumnCount());
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
		}
		REQUIRE(result->GetValue(0, 0).ToString() == expected_count);
		REQUIRE(result->GetValue(1, 0).ToString() == "4000000");
		if (!SljitPrimitiveRunCodegenSupported()) {
			RequirePrimitiveRunGenericFallback(manager);
			return;
		}

		idx_t generated_runtime_count = 0;
		bool local_proof_enabled = false;
		bool local_proof_failed = false;
		bool disjoint_finalize_receipt = false;
		string observed_source_stages;
		for (auto &event : manager.GetEvents()) {
			if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
				continue;
			}
			for (auto &stage : event.source_stage_runtime) {
				observed_source_stages += stage.stage.name + "=" + to_string(stage.count) + ";";
				disjoint_finalize_receipt =
				    disjoint_finalize_receipt ||
				    stage.stage.name == "source_contract.hash_aggregate_state_scan.finalize.disjoint_proven_ranges";
			}
			const auto runtime_paths = EventJitRuntimePathCounts(event);
			if (!StringUtil::Contains(runtime_paths, "aggregate_update.generated_pending_primitive_group_runs=")) {
				continue;
			}
			generated_runtime_count++;
			local_proof_enabled =
			    local_proof_enabled || StringUtil::Contains(runtime_paths, "proven_unique_append.enabled=1");
			local_proof_failed = local_proof_failed ||
			                     StringUtil::Contains(runtime_paths, "proven_unique_append.final_combine_required");
			RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		}
		REQUIRE(generated_runtime_count > 1);
		if (require_local_proof) {
			INFO("hash aggregate source stages: " + observed_source_stages);
			REQUIRE(local_proof_enabled);
			REQUIRE_FALSE(local_proof_failed);
			REQUIRE(disjoint_finalize_receipt);
		}
	};

	// Every physical row-group boundary is also a logical group boundary for runs of three.
	verify_query("disjoint_group_id", "1333334", true);
	// Producer-proven sparse batches retain their scheduler gaps without per-key rescans.
	verify_query("sparse_disjoint_group_id", "1333334", true);
	// Runs of seven cross physical row-group ownership boundaries, so global reconciliation must remain exact.
	verify_query("boundary_group_id", "571429", false);
}

TEST_CASE("JIT generated grouped runs hand a one-row final chunk to the generic path", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_generated_run_tail AS "
	                          "SELECT (i // 5)::BIGINT AS group_id, (i % 17)::INTEGER AS value "
	                          "FROM range(6145) tbl(i)"));
	const string query = "SELECT group_id, sum(value) AS value_sum "
	                     "FROM jit_generated_run_tail GROUP BY group_id";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_generated_run_tail_reference AS " + query));
	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_generated_run_tail_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_generated_run_tail_output EXCEPT ALL "
	                            "   SELECT * FROM jit_generated_run_tail_reference) UNION ALL "
	                            "  (SELECT * FROM jit_generated_run_tail_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_generated_run_tail_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	idx_t generated_rows = 0;
	idx_t shape_miss_rows = 0;
	bool final_combine_required = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		for (auto &counter : event.jit_runtime.runtime_path_counts) {
			if (counter.counter.name == "aggregate_update.generated_pending_primitive_group_runs") {
				generated_rows += counter.count;
			} else if (counter.counter.name == "aggregate_update.pending_primitive_group_runs_miss.shape") {
				shape_miss_rows += counter.count;
			} else if (counter.counter.name == "aggregate_update.proven_unique_append.final_combine_required") {
				final_combine_required = true;
			}
		}
	}
	REQUIRE(generated_rows == 6144);
	REQUIRE(shape_miss_rows == 1);
	REQUIRE(final_combine_required);
}

TEST_CASE("JIT generated sparse grouped runs preserve local uniqueness across the interval budget", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_signed_grouped_runs AS "
	                          "SELECT (((i // 4) * 3) - 500000)::BIGINT AS group_id, "
	                          "       CASE WHEN (i // 4) % 13 = 0 OR i % 11 = 0 THEN NULL "
	                          "            ELSE ((i % 17)::INTEGER - 8) END AS value, "
	                          "       CASE WHEN (i // 4) % 13 = 0 OR i % 11 = 0 THEN NULL "
	                          "            WHEN i % 4 = 0 THEN 9223372036854775800::BIGINT "
	                          "            WHEN i % 4 = 1 THEN 100::BIGINT "
	                          "            WHEN i % 4 = 2 THEN -9223372036854775700::BIGINT "
	                          "            ELSE -200::BIGINT END AS big_value "
	                          "FROM range(32768) tbl(i)"));

	auto require_generated_runs = [&](const string &name, const string &aggregate,
	                                  bool require_shared_payload_validity = false,
	                                  bool require_generated_state_initialize = false) {
		const auto query = "SELECT group_id, " + aggregate +
		                   " AS aggregate_value "
		                   "FROM jit_signed_grouped_runs GROUP BY group_id";
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE " + name + "_reference AS " + query));

		ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
		REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
		ClearJitTrace(manager, true);
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMP TABLE " + name + "_output AS " + query));
		auto difference = con.Query("SELECT count(*) FROM ("
		                            "  (SELECT * FROM " +
		                            name + "_output EXCEPT ALL SELECT * FROM " + name +
		                            "_reference) UNION ALL "
		                            "  (SELECT * FROM " +
		                            name + "_reference EXCEPT ALL SELECT * FROM " + name +
		                            "_output)"
		                            ") differences");
		REQUIRE_NO_FAIL(*difference);
		REQUIRE(difference->GetValue(0, 0).ToString() == "0");
		string observed_runtime_paths;
		for (auto &event : manager.GetEvents()) {
			if (EventPhase(event) == "runtime") {
				observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
			}
		}
		INFO(name + " runtime paths:\n" + observed_runtime_paths);
		if (!SljitPrimitiveRunCodegenSupported()) {
			RequirePrimitiveRunGenericFallback(manager);
			return;
		}

		RequireJitEvent(
		    manager,
		    [&](const ExecutionRegionEvent &event) {
			    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
				    return false;
			    }
			    const auto runtime_paths = EventJitRuntimePathCounts(event);
			    const bool base_paths =
			        StringUtil::Contains(runtime_paths,
			                             "aggregate_update.generated_pending_primitive_group_runs=32768") &&
			        StringUtil::Contains(runtime_paths,
			                             "aggregate_update.generated_primitive_group_cast.integral_compress=32768") &&
			        StringUtil::Contains(runtime_paths,
			                             "aggregate_update.proven_unique_append.producer_order_proof=8192") &&
			        StringUtil::Contains(runtime_paths, "aggregate_update.proven_unique_append.enabled=1") &&
			        !StringUtil::Contains(runtime_paths,
			                              "aggregate_update.proven_unique_append.final_combine_required") &&
			        !StringUtil::Contains(runtime_paths, "pending_primitive_group_runs_miss.group_replay");
			    return base_paths &&
			           (!require_shared_payload_validity ||
			            StringUtil::Contains(
			                runtime_paths,
			                "aggregate_update.generated_primitive_group_runs.shared_payload_validity=32768")) &&
			           (!require_generated_state_initialize ||
			            StringUtil::Contains(runtime_paths,
			                                 "aggregate_update.generated_preaggregated_state_initialize=8192"));
		    },
		    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
	};

	require_generated_runs("jit_signed_grouped_sum", "sum(value)");
	require_generated_runs("jit_signed_grouped_count", "count(value)");
	require_generated_runs("jit_signed_grouped_multi", "sum(value), count(value), count(*)", true, true);
	require_generated_runs("jit_signed_grouped_hugeint_sum", "sum(big_value)");
	require_generated_runs("jit_signed_grouped_hugeint_multi", "sum(big_value), count(big_value), sum(value)");
}

TEST_CASE("JIT generated grouped runs elide invariant affine group projections", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_affine_grouped_runs AS "
	                          "SELECT ((i // 4) * 3)::INTEGER AS group_id, "
	                          "       0::INTEGER AS group_offset, "
	                          "       ((i % 17)::INTEGER - 8) AS value "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT group_id + group_offset AS grouped_value, sum(value) AS value_sum "
	                     "FROM jit_affine_grouped_runs GROUP BY group_id + group_offset";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_affine_grouped_runs_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_affine_grouped_runs_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_affine_grouped_runs_output EXCEPT ALL "
	                            "   SELECT * FROM jit_affine_grouped_runs_reference) UNION ALL "
	                            "  (SELECT * FROM jit_affine_grouped_runs_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_affine_grouped_runs_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO("affine projected group runtime paths:\n" + observed_runtime_paths);

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths, "aggregate_update.grouped_direct_projected_input=32768") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.generated_pending_primitive_group_runs=32768") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_group_output_transform.add_constant=8192") &&
		           !StringUtil::Contains(runtime_paths, "aggregate_update.grouped_direct_materialized_input") &&
		           !StringUtil::Contains(runtime_paths, "generated_pending_primitive_group_runs_miss");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT affine grouped projections preserve nullable invariant semantics", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nullable_affine_groups AS "
	                          "SELECT ((i // 4) * 3)::INTEGER AS group_id, "
	                          "       CASE WHEN i % 97 = 0 THEN NULL ELSE 7::INTEGER END AS group_offset, "
	                          "       ((i % 17)::INTEGER - 8) AS value "
	                          "FROM range(32768) tbl(i)"));

	const string query = "SELECT group_id + group_offset AS grouped_value, sum(value) AS value_sum "
	                     "FROM jit_nullable_affine_groups GROUP BY group_id + group_offset";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_nullable_affine_groups_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_nullable_affine_groups_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_nullable_affine_groups_output EXCEPT ALL "
	                            "   SELECT * FROM jit_nullable_affine_groups_reference) UNION ALL "
	                            "  (SELECT * FROM jit_nullable_affine_groups_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_nullable_affine_groups_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO("nullable affine group runtime paths:\n" + observed_runtime_paths);

	RequireJitEvent(manager, [](const ExecutionRegionEvent &event) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			return false;
		}
		const auto runtime_paths = EventJitRuntimePathCounts(event);
		return StringUtil::Contains(runtime_paths, "aggregate_update.grouped_direct_projected_input=32768") &&
		       StringUtil::Contains(runtime_paths,
		                            "aggregate_update.pending_primitive_group_runs_miss.group_prepare=32768") &&
		       !StringUtil::Contains(runtime_paths, "pending_group_output_transform.add_constant") &&
		       !StringUtil::Contains(runtime_paths, "aggregate_update.grouped_direct_materialized_input");
	});
}

TEST_CASE("JIT large projected grouped sums admit cross-batch run preaggregation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_large_projected_grouped_sum AS "
	                          "SELECT (i // 4)::BIGINT AS group_id, (i % 17)::INTEGER AS value "
	                          "FROM range(8400000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("VACUUM jit_large_projected_grouped_sum"));

	const string grouped = "SELECT group_id, sum(value)::HUGEINT AS value_sum "
	                       "FROM jit_large_projected_grouped_sum GROUP BY group_id";
	const string query =
	    "SELECT count(*), sum(value_sum)::HUGEINT, min(value_sum), max(value_sum) FROM (" + grouped + ") grouped";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "2100000");

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO(observed_runtime_paths);
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(runtime_paths, "aggregate_update.grouped_direct_projected_input=") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.direct_input_vector_pending_preaggregated_grouped_update=") &&
		           StringUtil::Contains(runtime_paths, "aggregate_update.generated_pending_primitive_group_runs=") &&
		           StringUtil::Contains(runtime_paths, "aggregate_update.proven_unique_append.enabled=1") &&
		           StringUtil::Contains(runtime_paths, "aggregate_update.pending_preaggregated_grouped_update_flush=");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT sorted grouped sums keep one run strategy across changing batch density", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_changing_run_density AS "
	                          "SELECT CASE WHEN i < 2048 THEN i // 8 ELSE i END::BIGINT AS group_id, "
	                          "       (i % 97)::INTEGER AS value "
	                          "FROM range(8192) tbl(i)"));

	const string query = "SELECT group_id, sum(value) AS value_sum "
	                     "FROM jit_changing_run_density GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_changing_run_density_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_changing_run_density_output AS " + query));
	auto shape = con.Query("SELECT count(*), sum(value_sum) FROM jit_changing_run_density_output");
	REQUIRE_NO_FAIL(*shape);
	REQUIRE(shape->GetValue(0, 0).ToString() == "6400");
	REQUIRE(shape->GetValue(1, 0).ToString() == "392050");
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_changing_run_density_output EXCEPT ALL "
	                            "   SELECT * FROM jit_changing_run_density_reference) UNION ALL "
	                            "  (SELECT * FROM jit_changing_run_density_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_changing_run_density_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO(observed_runtime_paths);
	if (!SljitPrimitiveRunCodegenSupported()) {
		RequirePrimitiveRunGenericFallback(manager);
		return;
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    const auto runtime_paths = EventJitRuntimePathCounts(event);
		    return StringUtil::Contains(
		               runtime_paths,
		               "aggregate_update.direct_input_vector_pending_preaggregated_grouped_update=8192") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.generated_pending_primitive_group_runs=8192") &&
		           StringUtil::Contains(runtime_paths,
		                                "aggregate_update.pending_preaggregated_grouped_update_flush=8192") &&
		           StringUtil::Contains(runtime_paths, "aggregate_update.proven_unique_append.enabled=1") &&
		           !StringUtil::Contains(runtime_paths, "proven_unique_append.final_combine_required");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT high-uniqueness grouped append reconciles late duplicates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_late_duplicate_groups AS "
	                          "SELECT (i // 2)::BIGINT AS group_id, 1::BIGINT AS value "
	                          "FROM range(300000) tbl(i) "
	                          "UNION ALL SELECT 42::BIGINT, 5::BIGINT"));

	const string query = "SELECT group_id, sum(value) AS total "
	                     "FROM jit_late_duplicate_groups GROUP BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_late_duplicate_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_late_duplicate_output AS " + query));
	auto shape = con.Query("SELECT count(*), min(total), max(total), "
	                       "sum(CASE WHEN group_id=42 THEN total ELSE 0 END) "
	                       "FROM jit_late_duplicate_output");
	REQUIRE_NO_FAIL(*shape);
	REQUIRE(shape->GetValue(0, 0).ToString() == "150000");
	REQUIRE(shape->GetValue(1, 0).ToString() == "2");
	REQUIRE(shape->GetValue(2, 0).ToString() == "7");
	REQUIRE(shape->GetValue(3, 0).ToString() == "7");
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_late_duplicate_output EXCEPT ALL "
	                            "   SELECT * FROM jit_late_duplicate_reference) UNION ALL "
	                            "  (SELECT * FROM jit_late_duplicate_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_late_duplicate_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	string observed_runtime_paths;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime") {
			observed_runtime_paths += EventJitRuntimePathCounts(event) + "\n";
		}
	}
	INFO(observed_runtime_paths);
	REQUIRE(StringUtil::Contains(observed_runtime_paths, "proven_unique_append.enabled"));
	REQUIRE(StringUtil::Contains(observed_runtime_paths, "proven_unique_append.final_combine_required"));
}

TEST_CASE("JIT count-star grouped aggregate uses row-delta backend for high-cardinality batches", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_count_star_row_delta AS "
	                          "SELECT i::INTEGER AS k FROM range(2048) tbl(i)"));

	const string query = "SELECT k + 0 AS g, count(*) AS c "
	                     "FROM jit_count_star_row_delta GROUP BY g ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2048);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		REQUIRE(result->GetValue(0, row_idx).ToString() == reference->GetValue(0, row_idx).ToString());
		REQUIRE(result->GetValue(1, row_idx).ToString() == "1");
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			    return false;
		    }
		    return StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.primitive_grouped_count_star_row_update=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.primitive_grouped_count_star_row_update=2048"));
	    });
}

TEST_CASE("JIT regular hash aggregate appends new groups directly", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_new_grouped_update AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v "
	                          "FROM range(64) tbl(i)"));

	const string query = "SELECT k, sum(v), count(*) FROM jit_new_grouped_update GROUP BY k ORDER BY k";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 64);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsGeneratedAggregateUpdateRuntime(event) &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "append_new_groups=");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT regular hash aggregate fuses typed expression payloads with native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_typed_payload AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT count(*), sum(s)::HUGEINT, sum(s2)::HUGEINT, sum(s3)::HUGEINT, sum(s4)::HUGEINT "
	                     "FROM (SELECT k, "
	                     "             sum(p * (1.00::DECIMAL(15,2) - d)) AS s, "
	                     "             sum((p + 3.00::DECIMAL(15,2)) * (d + 1.00::DECIMAL(15,2))) AS s2, "
	                     "             sum((p - d) * (p + d)) AS s3, "
	                     "             sum(((p * 2.00::DECIMAL(15,2)) + (d * 7.00::DECIMAL(15,2))) - "
	                     "                 11.00::DECIMAL(15,2)) AS s4 "
	                     "      FROM jit_regular_hash_typed_payload GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "10000");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.fused_payload_update=5");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.primitive_payload_update="));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
		    REQUIRE(StringUtil::Contains(event.ir, "columns=3"));
		    REQUIRE(StringUtil::Contains(event.ir, "aggregates=4"));
		    REQUIRE(StringUtil::Contains(event.ir, "DECIMAL(18,4)"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT regular hash aggregate fuses typed expression payloads for existing groups", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_existing_typed_payload AS "
	                          "SELECT (i % 100)::BIGINT AS k, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS p, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT count(*), sum(s)::HUGEINT, sum(s2)::HUGEINT, sum(s3)::HUGEINT, sum(s4)::HUGEINT "
	                     "FROM (SELECT k, "
	                     "             sum(p * (1.00::DECIMAL(15,2) - d)) AS s, "
	                     "             sum((p + 3.00::DECIMAL(15,2)) * (d + 1.00::DECIMAL(15,2))) AS s2, "
	                     "             sum((p - d) * (p + d)) AS s3, "
	                     "             sum(((p * 2.00::DECIMAL(15,2)) + (d * 7.00::DECIMAL(15,2))) - "
	                     "                 11.00::DECIMAL(15,2)) AS s4 "
	                     "      FROM jit_regular_hash_existing_typed_payload GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "100");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree") &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                       "aggregate_update.resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT fused join payloads materialize group keys after descriptor lookup misses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_group_lookup_fact AS "
	                          "SELECT i::BIGINT AS row_id, (i % 17)::INTEGER AS region_id, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS price, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_group_lookup_bridge AS "
	                          "SELECT i::BIGINT AS row_id, (i % 17)::INTEGER AS region_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_group_lookup_region AS "
	                          "SELECT i::INTEGER AS region_id, 'region-' || i::VARCHAR AS region_name "
	                          "FROM range(17) tbl(i)"));

	const string query = "SELECT region_name, sum(price * (1.00::DECIMAL(15,2) - discount)) AS revenue "
	                     "FROM jit_group_lookup_fact f "
	                     "JOIN jit_group_lookup_bridge b ON f.row_id = b.row_id AND f.region_id = b.region_id "
	                     "JOIN jit_group_lookup_region r ON b.region_id = r.region_id "
	                     "GROUP BY region_name";
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_group_lookup_reference AS " + query));

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_group_lookup_output AS " + query));
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_group_lookup_output EXCEPT ALL "
	                            "   SELECT * FROM jit_group_lookup_reference) UNION ALL "
	                            "  (SELECT * FROM jit_group_lookup_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_group_lookup_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedMachineCodeRegion(event); });

	string observed_runtime_paths;
	string observed_generated_stages;
	bool descriptor_lookup_miss = false;
	bool materialized_group_update = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		const auto runtime_paths = EventJitRuntimePathCounts(event);
		const auto generated_stages = EventGeneratedStageCountBreakdown(event);
		observed_runtime_paths += runtime_paths + "\n";
		observed_generated_stages += generated_stages + "\n";
		descriptor_lookup_miss =
		    descriptor_lookup_miss ||
		    StringUtil::Contains(runtime_paths, "aggregate_update.direct_input_vector_group_payload_update_miss=");
		materialized_group_update =
		    materialized_group_update ||
		    StringUtil::Contains(generated_stages, "aggregate_update.direct_projected_group_payload_update=");
	}
	INFO("fused join payload runtime paths:\n" + observed_runtime_paths);
	INFO("fused join payload generated stages:\n" + observed_generated_stages);
	REQUIRE(descriptor_lookup_miss);
	REQUIRE(materialized_group_update);
}

TEST_CASE("JIT grouped aggregate consumes rewritten payloads through generated update", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_regular_hash_typed_payload AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       DATE '1996-01-01' + CAST(i % 180 AS INTEGER) AS shipdate, "
	                          "       CAST(10000 + (i % 1000) AS DECIMAL(15,2)) AS price, "
	                          "       CAST(i % 11 AS DECIMAL(15,2)) AS discount "
	                          "FROM range(20000) tbl(i)"));

	const string query = "SELECT count(*), sum(revenue)::HUGEINT "
	                     "FROM (SELECT supplier_no, sum(price * (1.00::DECIMAL(15,2) - discount)) AS revenue "
	                     "      FROM (SELECT CAST(k AS INTEGER) AS supplier_no, price, discount "
	                     "            FROM jit_filtered_regular_hash_typed_payload "
	                     "            WHERE shipdate >= DATE '1996-02-01' AND shipdate < DATE '1996-05-01') q "
	                     "      GROUP BY supplier_no) grouped_revenue";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 1);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payload_projection_partially_composed=true") &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
		    REQUIRE(StringUtil::Contains(event.ir, "columns=3"));
	    });

	bool found_generated_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsGeneratedAggregateUpdateRuntime(event)) {
			continue;
		}
		RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		found_generated_update = true;
	}
	REQUIRE(found_generated_update);
}

TEST_CASE("JIT regular hash aggregate fuses INT32 CASE payloads into hugeint grouped reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_regular_hash_int32_case_payload AS "
	                          "SELECT CASE WHEN i % 4 = 0 THEN 'MAIL' "
	                          "            WHEN i % 4 = 1 THEN 'SHIP' "
	                          "            WHEN i % 4 = 2 THEN 'RAIL' "
	                          "            ELSE 'AIR' END AS shipmode, "
	                          "       CASE WHEN i % 5 = 0 THEN '1-URGENT' "
	                          "            WHEN i % 5 = 1 THEN '2-HIGH' "
	                          "            WHEN i % 5 = 2 THEN '3-MEDIUM' "
	                          "            ELSE '4-NOT SPECIFIED' END AS priority "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT shipmode, "
	                     "       sum(CASE WHEN priority = '1-URGENT' OR priority = '2-HIGH' THEN 1 ELSE 0 END) "
	                     "           AS high_line_count, "
	                     "       sum(CASE WHEN priority = '1-URGENT' THEN 1 ELSE 0 END) AS urgent_line_count "
	                     "FROM jit_regular_hash_int32_case_payload GROUP BY shipmode ORDER BY shipmode";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 4);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree") &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		    REQUIRE(StringUtil::Contains(event.ir, "case<"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
	    });

	RequireJitEvent(
	    manager, [](const ExecutionRegionEvent &event) { return IsGeneratedAggregateUpdateRuntime(event); },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT grouped aggregate uses native state addresses under high cardinality", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_high_cardinality_grouped AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(150000) tbl(i)"));

	auto reference = con.Query("SELECT count(*), sum(c)::HUGEINT, sum(s)::HUGEINT "
	                           "FROM (SELECT k, count(*) AS c, sum(v) AS s "
	                           "      FROM jit_high_cardinality_grouped GROUP BY k)");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "150000");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(c)::HUGEINT, sum(s)::HUGEINT "
	                        "FROM (SELECT k, count(*) AS c, sum(v) AS s "
	                        "      FROM jit_high_cardinality_grouped GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).ToString() == reference->GetValue(2, 0).ToString());

	bool found_high_cardinality_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			found_high_cardinality_state_address_update = true;
			RequireGeneratedMachineCodeRegion(event);
			REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
			REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
			REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
			REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		}
	}
	REQUIRE(found_high_cardinality_state_address_update);
}

TEST_CASE("JIT fuses scaled decimal expression payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_aggregate_payload AS "
	                          "SELECT i, CAST(i % 1000 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum((((((((((d * CAST(2 AS DECIMAL(15,2))) + CAST(5 AS DECIMAL(15,2))) "
	                        "* CAST(3 AS DECIMAL(15,2))) - CAST(7 AS DECIMAL(15,2))) "
	                        "+ CAST(11 AS DECIMAL(15,2))) * CAST(2 AS DECIMAL(15,2))) "
	                        "- CAST(13 AS DECIMAL(15,2))) + CAST(17 AS DECIMAL(15,2))) "
	                        "* CAST(2 AS DECIMAL(15,2))) - CAST(19 AS DECIMAL(15,2))) "
	                        "FROM jit_decimal_aggregate_payload WHERE i >= 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "120530000.0000000000");

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT fuses nullable BIGINT case payloads into primitive hugeint aggregate reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_bigint_case_aggregate_payload AS "
	                          "SELECT i::BIGINT AS a, (i * 3 + 17)::BIGINT AS b, "
	                          "(i * 5 - 11)::BIGINT AS c, "
	                          "CASE WHEN i % 10 = 0 THEN NULL ELSE (i * 7 + 19)::BIGINT END AS d "
	                          "FROM range(10000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(CASE "
	                           "WHEN ((a + (b * 2)) < (c + coalesce(d, a)) "
	                           "      AND coalesce(d, a) BETWEEN (a + b - c - 1000000) "
	                           "      AND (a + b + c + 1000000)) "
	                           "THEN (((a * 3) + (b * 5) - (c * 7) + (coalesce(d, 0) * 11)) * 13) "
	                           "   + (((a - b + c) * 17) - ((a + coalesce(d, 0)) * 19)) "
	                           "   + (((b - c + coalesce(d, 0)) * 23) - ((a - coalesce(d, 0)) * 29)) "
	                           "WHEN ((a + c) > (b + coalesce(d, c)) OR "
	                           "      (a + b + c) < (coalesce(d, 0) * 2)) "
	                           "THEN (((a * 31) - (b * 37) + (c * 41) - (coalesce(d, 0) * 43)) "
	                           "   + (((a + b + c + coalesce(d, 0)) * 47) - ((a - c) * 53))) "
	                           "ELSE (((a + 7) * (b - 3)) - ((c + 5) * (coalesce(d, a) - 11))) "
	                           "END) FROM jit_bigint_case_aggregate_payload");
	REQUIRE_NO_FAIL(*reference);

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query("SELECT sum(CASE "
	                        "WHEN ((a + (b * 2)) < (c + coalesce(d, a)) "
	                        "      AND coalesce(d, a) BETWEEN (a + b - c - 1000000) "
	                        "      AND (a + b + c + 1000000)) "
	                        "THEN (((a * 3) + (b * 5) - (c * 7) + (coalesce(d, 0) * 11)) * 13) "
	                        "   + (((a - b + c) * 17) - ((a + coalesce(d, 0)) * 19)) "
	                        "   + (((b - c + coalesce(d, 0)) * 23) - ((a - coalesce(d, 0)) * 29)) "
	                        "WHEN ((a + c) > (b + coalesce(d, c)) OR "
	                        "      (a + b + c) < (coalesce(d, 0) * 2)) "
	                        "THEN (((a * 31) - (b * 37) + (c * 41) - (coalesce(d, 0) * 43)) "
	                        "   + (((a + b + c + coalesce(d, 0)) * 47) - ((a - c) * 53))) "
	                        "ELSE (((a + 7) * (b - 3)) - ((c + 5) * (coalesce(d, a) - 11))) "
	                        "END) FROM jit_bigint_case_aggregate_payload");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
		REQUIRE(StringUtil::Contains(event.ir, "coalesce<"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT typed-tree aggregate payloads preserve all-NULL branch results", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_typed_tree_sum_null_branch AS "
	                          "SELECT i::BIGINT AS a FROM range(10000) tbl(i)"));

	const string query = "SELECT sum(CASE WHEN a < 0 THEN a ELSE NULL::BIGINT END) "
	                     "FROM jit_typed_tree_sum_null_branch";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).IsNull());

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).IsNull());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
		REQUIRE(StringUtil::Contains(event.ir, "validity=constant-null"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT generic BIGINT sum uses hugeint local accumulation", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='statistics_propagation'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_bigint_sum_hugeint_local AS "
	                          "SELECT i, 9223372036854775807::BIGINT AS v FROM range(4096) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "37778931862957161705472");

	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	auto result = con.Query("SELECT sum(CASE WHEN i >= 0 THEN v ELSE 0 END) AS s "
	                        "FROM jit_bigint_sum_hugeint_local");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
		REQUIRE(StringUtil::Contains(event.ir, "case<"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT preserves stats-proven non-overflowing decimal arithmetic in expression reducers", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_decimal_safe_aggregate_payload AS "
	                          "SELECT i, CAST(i % 1000 AS DECIMAL(15,2)) AS d "
	                          "FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT sum((((((((((d * CAST(2 AS DECIMAL(15,2))) + CAST(5 AS DECIMAL(15,2))) "
	                        "* CAST(3 AS DECIMAL(15,2))) - CAST(7 AS DECIMAL(15,2))) "
	                        "+ CAST(11 AS DECIMAL(15,2))) * CAST(2 AS DECIMAL(15,2))) "
	                        "- CAST(13 AS DECIMAL(15,2))) + CAST(17 AS DECIMAL(15,2))) "
	                        "* CAST(2 AS DECIMAL(15,2))) - CAST(19 AS DECIMAL(15,2))) "
	                        "FROM jit_decimal_safe_aggregate_payload WHERE i >= 0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "120530000.0000000000");

	bool found_compile = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree")) {
			continue;
		}
		found_compile = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:expression-tree"));
		REQUIRE(StringUtil::Contains(event.ir, "overflow_check=false"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "overflow_check=true"));
	}
	REQUIRE(found_compile);
}

TEST_CASE("JIT fuses non-null multi-key perfect-hash aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_multi_key_perfect_hash_payload("
	                          "group_flag VARCHAR NOT NULL, group_status VARCHAR NOT NULL, "
	                          "qty_value DECIMAL(15,2) NOT NULL, gross_value DECIMAL(15,2) NOT NULL, "
	                          "discount_rate DECIMAL(15,2) NOT NULL, tax_rate DECIMAL(15,2) NOT NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_multi_key_perfect_hash_payload "
	                          "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END, "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END, "
	                          "       CAST(1 + (i % 50) AS DECIMAL(15,2)), "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)), "
	                          "       CAST(i % 10 AS DECIMAL(15,2)), "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) "
	                          "FROM range(120000) tbl(i)"));

	const string query = "SELECT group_flag, group_status, "
	                     "       sum(qty_value), "
	                     "       sum(gross_value), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate)), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate) * "
	                     "           (1.00::DECIMAL(15,2) + tax_rate)), "
	                     "       sum(discount_rate), "
	                     "       count(*) "
	                     "FROM jit_multi_key_perfect_hash_payload "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.reason, "backend_source_validity=not-null:6"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_group_key_type=uint8:2"));
		    REQUIRE(StringUtil::Contains(event.reason, "backend_payload_type=int64:1|decimal64:5"));
		    REQUIRE(StringUtil::Contains(
		        event.reason,
		        "backend_aggregate=perfect_hash_update:1|primitive_payload_update:1|grouped_state_address_lookup:1|"
		        "generated_perfect_hash_lookup:1"));
		    REQUIRE(StringUtil::Contains(event.ir, "group_expressions=native:string-compress"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "op0=projection("));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "projection"));
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT join-selected perfect-hash aggregate payloads survive selection views", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_selected_perfect_hash_payload AS "
	                          "SELECT (1995 + (i % 2))::INTEGER AS bucket_year, "
	                          "       (i % 5)::INTEGER AS segment_id, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS volume "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_join_selected_dimension AS "
	                          "SELECT i::INTEGER AS segment_id, "
	                          "       CASE WHEN i = 0 THEN 'target' ELSE 'other' END AS segment_name "
	                          "FROM range(5) tbl(i)"));

	const string query = "SELECT bucket_year, "
	                     "       sum(CASE WHEN segment_name = 'target' "
	                     "                THEN volume ELSE 0.00::DECIMAL(15,2) END) AS target_sum, "
	                     "       sum(volume) AS total_sum "
	                     "FROM jit_join_selected_perfect_hash_payload "
	                     "JOIN jit_join_selected_dimension USING (segment_id) "
	                     "GROUP BY bucket_year "
	                     "ORDER BY bucket_year";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 2);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "hash_join_probe"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:typed-expression-tree"));
		    REQUIRE(StringUtil::Contains(event.ir, "case<"));
		    REQUIRE(StringUtil::Contains(event.ir, "sum_hugeint"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsGeneratedAggregateUpdateRuntime(event) && HasGeneratedAggregateUpdateStage(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedAggregateUpdateRuntimeOwnership(event);
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.join_aggregate.input.hash_join_lhs_input="));
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                       "aggregate_update.resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT generates single typed perfect-hash aggregate payloads", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_typed_payload_l AS "
	                          "SELECT i::BIGINT AS k, (i % 100)::BIGINT AS g, (i % 7)::BIGINT AS v "
	                          "FROM range(10000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_single_typed_payload_r AS "
	                          "SELECT i::BIGINT AS k, (i % 11)::BIGINT AS x "
	                          "FROM range(10000) tbl(i)"));

	const string query = "SELECT g, sum(v + x) AS s "
	                     "FROM jit_single_typed_payload_l JOIN jit_single_typed_payload_r USING (k) "
	                     "GROUP BY g "
	                     "ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 100);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "native:typed-expression-tree");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		    REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=native-state-address"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(ExecutionRegionEventProfileCodeSize(event) > 0);
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.fused_payload_update_owns_perfect_hash_group_lookup="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		    REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                       "aggregate_update.resolve_grouped_state_addresses="));
	    });
}

TEST_CASE("JIT grouped distinct aggregate uses explicit distinct key sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_grouped_distinct_input AS "
	                          "SELECT (i % 31)::INTEGER AS brand_id, "
	                          "       (i % 17)::INTEGER AS type_id, "
	                          "       (i % 11)::INTEGER AS size_id, "
	                          "       (i % 251)::INTEGER AS supp_id "
	                          "FROM range(65536) tbl(i)"));

	const string query =
	    "SELECT brand_id + 1 AS brand_key, type_id, size_id, count(DISTINCT supp_id) AS supplier_count "
	    "FROM jit_grouped_distinct_input "
	    "GROUP BY brand_key, type_id, size_id "
	    "ORDER BY brand_key, type_id, size_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "distinct_aggregate_count=1");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.materialization_elision_count == 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.generated_backend_stage_count > 0);
		    REQUIRE(event.runner_cost.generated_grouped_aggregate_stage_count > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "distinct_key_fast_insert:1"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.backend_name == "sljit" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.distinct_key_fast_insert=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.distinct_key_fast_insert."));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT grouped distinct preserves pairs across compiled region executions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_distinct_region_boundary AS "
	                          "SELECT (i % 20000)::INTEGER AS group_id, "
	                          "       (i % 120000)::INTEGER AS supp_id "
	                          "FROM range(240000) tbl(i)"));

	const string query = "SELECT group_id, count(DISTINCT supp_id) AS supplier_count "
	                     "FROM jit_distinct_region_boundary GROUP BY group_id ORDER BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_distinct_region_boundary_reference AS " + query));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE jit_distinct_region_boundary_output AS " + query));

	idx_t distinct_runtime_count = 0;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) == "runtime" && EventStatus(event) == "executed" && event.backend_name == "sljit" &&
		    StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.distinct_key_fast_insert=")) {
			distinct_runtime_count++;
		}
	}
	REQUIRE(distinct_runtime_count > 1);
	auto shape = con.Query("SELECT count(*), min(group_id), max(group_id), sum(group_id), "
	                       "       min(supplier_count), max(supplier_count), sum(supplier_count) "
	                       "FROM jit_distinct_region_boundary_output");
	REQUIRE_NO_FAIL(*shape);
	REQUIRE(shape->GetValue(0, 0).ToString() == "20000");
	REQUIRE(shape->GetValue(1, 0).ToString() == "0");
	REQUIRE(shape->GetValue(2, 0).ToString() == "19999");
	REQUIRE(shape->GetValue(3, 0).ToString() == "199990000");
	REQUIRE(shape->GetValue(4, 0).ToString() == "6");
	REQUIRE(shape->GetValue(5, 0).ToString() == "6");
	REQUIRE(shape->GetValue(6, 0).ToString() == "120000");
	auto difference = con.Query("SELECT count(*) FROM ("
	                            "  (SELECT * FROM jit_distinct_region_boundary_output EXCEPT ALL "
	                            "   SELECT * FROM jit_distinct_region_boundary_reference) UNION ALL "
	                            "  (SELECT * FROM jit_distinct_region_boundary_reference EXCEPT ALL "
	                            "   SELECT * FROM jit_distinct_region_boundary_output)"
	                            ") differences");
	REQUIRE_NO_FAIL(*difference);
	REQUIRE(difference->GetValue(0, 0).ToString() == "0");
}

TEST_CASE("JIT grouped distinct direct count handles nullable and variable-width keys", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_nullable_distinct_input AS "
	                          "SELECT CASE WHEN i % 19 = 0 THEN NULL ELSE (i % 257)::INTEGER END AS group_id, "
	                          "       CASE WHEN i % 13 = 0 THEN NULL ELSE (i % 509)::INTEGER END AS supp_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_string_distinct_input AS "
	                          "SELECT 'group-' || (i % 257)::VARCHAR AS group_id, "
	                          "       'supplier-' || (i % 509)::VARCHAR AS supp_id "
	                          "FROM range(65536) tbl(i)"));

	auto verify_query = [&](const string &query, bool require_fixed_pair) {
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
		auto reference = con.Query(query);
		REQUIRE_NO_FAIL(*reference);
		REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
		ClearJitTrace(manager, true);
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == reference->RowCount());
		for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
			for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
				REQUIRE(result->GetValue(col_idx, row_idx).ToString() ==
				        reference->GetValue(col_idx, row_idx).ToString());
			}
		}
		RequireJitEvent(
		    manager,
		    [](const ExecutionRegionEvent &event) {
			    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
			           event.backend_name == "sljit" &&
			           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
			                                "aggregate_update.distinct_key_fast_insert.direct_group_identity_count=");
		    },
		    [require_fixed_pair](const ExecutionRegionEvent &event) {
			    auto stage_counts = EventGeneratedStageCountBreakdown(event);
			    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
			    REQUIRE(StringUtil::Contains(stage_counts,
			                                 "aggregate_update.distinct_key_fast_insert.direct_fixed_pair_probe=") ==
			            require_fixed_pair);
		    });
	};

	verify_query("SELECT group_id, count(DISTINCT supp_id) FROM jit_nullable_distinct_input "
	             "GROUP BY group_id ORDER BY group_id NULLS FIRST",
	             true);
	verify_query("SELECT group_id, count(DISTINCT supp_id) FROM jit_string_distinct_input "
	             "GROUP BY group_id ORDER BY group_id",
	             false);
}

TEST_CASE("JIT grouped distinct keeps exact parallel fallback", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_parallel_distinct_input AS "
	                          "SELECT (i % 127)::INTEGER AS group_id, (i % 1009)::INTEGER AS supp_id "
	                          "FROM range(262144) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_parallel_distinct_blocked AS "
	                          "SELECT (i * 17 % 1009)::INTEGER AS supp_id FROM range(29) tbl(i)"));

	const string query = "SELECT group_id, count(DISTINCT supp_id) FROM jit_parallel_distinct_input "
	                     "WHERE supp_id NOT IN (SELECT supp_id FROM jit_parallel_distinct_blocked) "
	                     "GROUP BY group_id ORDER BY group_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.backend_name == "sljit" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.distinct_key_fast_insert=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.distinct_key_fast_insert.direct_group_identity_count="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT projected grouped distinct aggregate uses selected reference distinct key sink", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET perfect_ht_threshold=0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_distinct_item AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       (i % 251)::BIGINT AS supp_id, "
	                          "       (i % 31)::TINYINT AS brand_key, "
	                          "       (i % 17)::TINYINT AS type_id, "
	                          "       (i % 11)::TINYINT AS size_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_projected_distinct_blocked AS "
	                          "SELECT (i * 13 % 251)::BIGINT AS supp_id "
	                          "FROM range(23) tbl(i)"));

	const string query = "SELECT brand_key, type_id, size_id, count(DISTINCT supp_id) AS supplier_count "
	                     "FROM jit_projected_distinct_item "
	                     "WHERE supp_id NOT IN (SELECT supp_id FROM jit_projected_distinct_blocked) "
	                     "GROUP BY brand_key, type_id, size_id "
	                     "ORDER BY brand_key, type_id, size_id";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.candidate_signature.shape, "operator-filter-projection") &&
		           StringUtil::Contains(event.ir, "distinct_aggregate_count=1");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.runner_cost.materialization_elision_count == 0);
		    REQUIRE(event.runner_cost.native_aggregate_stage_count == 0);
		    REQUIRE(event.runner_cost.generated_backend_stage_count > 0);
		    REQUIRE(event.runner_cost.generated_grouped_aggregate_stage_count > 0);
		    REQUIRE(StringUtil::Contains(event.reason, "distinct_key_fast_insert:1"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           event.backend_name == "sljit" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.projected_distinct_key_reference_sink=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "aggregate_update.distinct_key_fast_insert."));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT grouped distinct aggregate feeds MARK probe from selected join view", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 20000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_generated_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_materialization_elision_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("SET jit_cbo_native_operator_stage_benefit=1048576"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_distinct_two_probe_fact AS "
	                          "SELECT (i % 4096)::BIGINT AS item_id, "
	                          "       (i % 251)::BIGINT AS supp_id "
	                          "FROM range(65536) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_distinct_two_probe_dim AS "
	                          "SELECT i::BIGINT AS item_id, "
	                          "       'family_' || CAST(i % 37 AS VARCHAR) AS family_name, "
	                          "       'class_' || CAST(i % 19 AS VARCHAR) AS class_name, "
	                          "       (1 + (i % 48))::INTEGER AS size_code "
	                          "FROM range(4096) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_distinct_two_probe_blocked AS "
	                          "SELECT (i * 13 % 251)::BIGINT AS supp_id "
	                          "FROM range(23) tbl(i)"));

	const string query = "SELECT d.family_name, d.class_name, d.size_code, count(DISTINCT f.supp_id) AS supplier_count "
	                     "FROM jit_distinct_two_probe_fact f "
	                     "JOIN jit_distinct_two_probe_dim d ON f.item_id = d.item_id "
	                     "WHERE f.supp_id NOT IN (SELECT supp_id FROM jit_distinct_two_probe_blocked) "
	                     "GROUP BY d.family_name, d.class_name, d.size_code "
	                     "ORDER BY supplier_count DESC, d.family_name, d.class_name, d.size_code";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() > 0);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	REQUIRE(result->ColumnCount() == reference->ColumnCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           EventExecutionMode(event) == "native" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event), "mark_probe_input_view=") &&
		           HasJitAggregateUpdatePath(event);
	    },
	    [](const ExecutionRegionEvent &event) {
		    auto stage_counts = EventGeneratedStageCountBreakdown(event);
		    REQUIRE(StringUtil::Contains(stage_counts, "mark_probe_input_view="));
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "aggregate_update.projected_distinct_key_sink="));
		    REQUIRE(
		        StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.distinct_key_fast_insert="));
		    REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
	    });
}

TEST_CASE("JIT gates perfect-hash aggregate updates with generated source filters", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_filtered_perfect_hash_payload AS "
	                          "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS group_flag, "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS group_status, "
	                          // Four passing rows, four rejected rows, then a mixed group exercise every packed-mask
	                          // control class. The conjunction also exercises packed-group short-circuiting; the final
	                          // three rows exercise the scalar tail.
	                          "       DATE '1998-08-30' + CASE i % 12 "
	                          "           WHEN 8 THEN 4 WHEN 9 THEN 5 WHEN 10 THEN 0 WHEN 11 THEN 1 "
	                          "           ELSE CAST(i % 12 AS INTEGER) END AS event_date, "
	                          "       CAST(1 + (i % 50) AS DECIMAL(15,2)) AS qty_value, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)) AS gross_value, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount_rate, "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) AS tax_rate "
	                          "FROM range(120003) tbl(i)"));

	const string query = "SELECT group_flag, group_status, "
	                     "       sum(qty_value), "
	                     "       sum(gross_value), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate)), "
	                     "       sum(gross_value * (1.00::DECIMAL(15,2) - discount_rate) * "
	                     "           (1.00::DECIMAL(15,2) + tax_rate)), "
	                     "       sum(discount_rate), "
	                     "       count(*) "
	                     "FROM jit_filtered_perfect_hash_payload "
	                     "WHERE event_date <= DATE '1998-09-02' "
	                     "  AND tax_rate <= 3.00::DECIMAL(15,2) "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    RequireGeneratedSourceFilterContract(event);
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
		    REQUIRE(StringUtil::Contains(event.ir, "aggregate_update(kind=perfect-hash"));
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && EventStatus(event) == "executed" &&
		           StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                "aggregate_update.filtered_perfect_hash_update=");
	    },
	    [](const ExecutionRegionEvent &event) { REQUIRE(EventJitRuntimeDelegationCounts(event).empty()); });

	auto high_selectivity_query = StringUtil::Replace(query, "DATE '1998-09-02'", "DATE '1998-09-05'");
	high_selectivity_query = StringUtil::Replace(high_selectivity_query, "tax_rate <= 3.00", "tax_rate <= 7.00");
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto high_selectivity_reference = con.Query(high_selectivity_query);
	REQUIRE_NO_FAIL(*high_selectivity_reference);
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto high_selectivity_result = con.Query(high_selectivity_query);
	REQUIRE_NO_FAIL(*high_selectivity_result);
	REQUIRE(high_selectivity_result->ToString() == high_selectivity_reference->ToString());
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			continue;
		}
		REQUIRE_FALSE(
		    StringUtil::Contains(EventJitRuntimePathCounts(event), "aggregate_update.filtered_perfect_hash_update="));
	}
}

TEST_CASE("JIT perfect hash keeps payload and group vector shapes independent", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	auto db_path = TestCreatePath("jit_perfect_hash_independent_vector_shapes.db");
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS shape_test (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE shape_test.payload AS "
	                          "SELECT CASE i % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END AS group_flag, "
	                          "       CASE i % 2 WHEN 0 THEN 'F' ELSE 'O' END AS group_status, "
	                          "       DATE '2024-01-01' + CASE i % 12 "
	                          "           WHEN 8 THEN 4 WHEN 9 THEN 5 WHEN 10 THEN 0 WHEN 11 THEN 1 "
	                          "           ELSE CAST(i % 12 AS INTEGER) END AS event_date, "
	                          "       CAST(1 + i % 50 AS DECIMAL(15,2)) AS quantity, "
	                          "       CAST(100 + i % 1000 AS DECIMAL(15,2)) AS price, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) AS tax "
	                          "FROM range(240003) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT shape_test"));
	REQUIRE_NO_FAIL(con.Query("DETACH shape_test"));
	REQUIRE_NO_FAIL(con.Query("ATTACH " + SQLString(db_path) + " AS shape_test"));

	const string query = "SELECT group_flag, group_status, sum(quantity), sum(price), "
	                     "       sum(price * (1.00::DECIMAL(15,2) - discount)), "
	                     "       sum(price * (1.00::DECIMAL(15,2) - discount) * "
	                     "           (1.00::DECIMAL(15,2) + tax)), "
	                     "       sum(discount), count(*) "
	                     "FROM shape_test.payload "
	                     "WHERE event_date <= DATE '2024-01-04' "
	                     "  AND tax <= 3.00::DECIMAL(15,2) "
	                     "  AND quantity <= 25.00::DECIMAL(15,2) "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	for (idx_t run = 0; run < 3; run++) {
		ClearJitTrace(manager, true);
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->ToString() == reference->ToString());
	}
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    RequireGeneratedSourceFilterContract(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
	    });

	REQUIRE_NO_FAIL(con.Query("DETACH shape_test"));
	TestDeleteFile(db_path);
	TestDeleteFile(db_path + ".wal");
}

TEST_CASE("JIT perfect hash aggregate generates primitive decimal sum and count star updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_count_star AS "
	                          "SELECT (i % 4)::UTINYINT AS g1, (i % 3)::UTINYINT AS g2, "
	                          "CAST(i % 100 AS DECIMAL(15,2)) AS v FROM range(100000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference =
	    con.Query("SELECT g1, g2, sum(v), count(*) FROM jit_perfect_hash_count_star GROUP BY g1, g2 ORDER BY g1, g2");
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 12);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result =
	    con.Query("SELECT g1, g2, sum(v), count(*) FROM jit_perfect_hash_count_star GROUP BY g1, g2 ORDER BY g1, g2");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	bool found_compiled_primitive_sink = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event)) {
			continue;
		}
		if (!event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
			continue;
		}
		if (EventPhase(event) != "compile") {
			continue;
		}
		found_compiled_primitive_sink = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "native aggregate update sink contract"));
		REQUIRE(StringUtil::Contains(event.ir, "aggregate_contract<"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_layout_ready=true"));
		REQUIRE(StringUtil::Contains(event.ir, "count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_update_kind=count_star"));
		REQUIRE(StringUtil::Contains(event.ir, "native_aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.reason, "aggregate_state_update_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:"));
		REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
		REQUIRE_FALSE(StringUtil::Contains(event.reason, "missing-native-state-update-contract"));
	}
	REQUIRE(found_compiled_primitive_sink);

	bool found_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || event.backend_name != "sljit") {
			continue;
		}
		if (!StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                          "aggregate_update.primitive_payload_update_fused=")) {
			continue;
		}
		found_runtime = true;
		REQUIRE(event.kernel_code_size > 0);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_payload_update_fused="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.resolve_grouped_state_addresses="));
		REQUIRE_FALSE(
		    StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "resolve_grouped_state_addresses"));
	}
	REQUIRE(found_runtime);
}

TEST_CASE("JIT perfect hash direct payloads use their canonical combined source layout", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_direct_payload_sources("
	                          "qty_value DECIMAL(15,2) NOT NULL, group_flag VARCHAR NOT NULL, "
	                          "discount_rate DECIMAL(15,2) NOT NULL, group_status VARCHAR NOT NULL, "
	                          "gross_value DECIMAL(15,2) NOT NULL, tax_rate DECIMAL(15,2) NOT NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO jit_perfect_hash_direct_payload_sources "
	                          "SELECT CAST(1 + (i % 50) AS DECIMAL(15,2)), "
	                          "       CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)), "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END, "
	                          "       CAST(100 + (i % 1000) AS DECIMAL(15,2)), "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) "
	                          "FROM range(120000) tbl(i)"));

	const string query = "SELECT group_flag, group_status, "
	                     "       sum(qty_value), sum(gross_value), sum(discount_rate), sum(tax_rate), count(*) "
	                     "FROM jit_perfect_hash_direct_payload_sources "
	                     "WHERE qty_value + tax_rate > 10.00::DECIMAL(15,2) "
	                     "GROUP BY group_flag, group_status "
	                     "ORDER BY group_flag, group_status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 6);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ToString() == reference->ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_payloads=native:reference"));
		    REQUIRE(StringUtil::Contains(event.ir, "count_star"));
	    });
	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return EventPhase(event) == "runtime" && event.backend_name == "sljit" &&
		           StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) { RequireGeneratedAggregateUpdateRuntimeOwnership(event); });
}

TEST_CASE("JIT production CBO keeps reference-only string perfect hash aggregate vectorized", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto");
	ConfigureJitDecisionTrace(con);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_reference_only AS "
	                          "SELECT CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'N' ELSE 'R' END AS flag, "
	                          "       CASE WHEN i % 2 = 0 THEN 'F' ELSE 'O' END AS status, "
	                          "       CAST(1 + i % 50 AS DECIMAL(15,2)) AS quantity, "
	                          "       CAST(100 + i % 1000 AS DECIMAL(15,2)) AS price, "
	                          "       CAST(i % 10 AS DECIMAL(15,2)) AS discount, "
	                          "       CAST(i % 8 AS DECIMAL(15,2)) AS tax "
	                          "FROM range(300000) tbl(i)"));

	const string query = "SELECT flag, status, sum(quantity), sum(price), sum(discount), sum(tax), count(*) "
	                     "FROM jit_perfect_hash_reference_only "
	                     "GROUP BY flag, status ORDER BY flag, status";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ToString() == reference->ToString());

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsSljitRegionEvent(event) && EventStatus(event) == "skipped" && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.reason, "backend_cost=reference_only_string_perfect_hash_aggregate:1");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
		    REQUIRE_FALSE(event.runner_cost.SelectedAcceleratedRunner());
		    REQUIRE(event.runner_cost.generated_backend_stage_count == 0);
		    REQUIRE(event.runner_cost.materialization_elision_count == 0);
		    REQUIRE(event.runner_cost.selection_reason == "rejected_no_accelerated_work");
	    });
	for (auto &event : manager.GetEvents()) {
		REQUIRE_FALSE((IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		               event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE));
	}
}

TEST_CASE("JIT perfect hash aggregate uses exact wide and double payload ABIs", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_wide_payload AS "
	                          "SELECT (i % 8)::UTINYINT AS g, "
	                          "       CAST((i % 2001) - 1000 AS DECIMAL(15,2)) AS d, "
	                          "       ((i % 100000) - 50000)::BIGINT AS q, "
	                          "       (i % 101)::DOUBLE AS x "
	                          "FROM range(200000) tbl(i)"));

	const string query = "SELECT g, sum(d * q), sum(x) FROM jit_perfect_hash_wide_payload GROUP BY g ORDER BY g";
	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 8);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "native:decimal128-widening-multiply"));
		    REQUIRE(StringUtil::Contains(event.ir, "primitive_update_kind=sum_double"));
	    });
}

TEST_CASE("JIT perfect hash aggregate fuses computed smallint group expressions", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_perfect_hash_smallint_group AS "
	                          "SELECT i::BIGINT AS i FROM range(200000) tbl(i)"));
	const string query = "SELECT i % 1000 AS key, sum(i * 31) AS value "
	                     "FROM jit_perfect_hash_smallint_group GROUP BY key ORDER BY key";

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='off'"));
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->RowCount() == 1000);

	REQUIRE_NO_FAIL(con.Query("SET jit_policy='auto'"));
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == reference->RowCount());
	for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			REQUIRE(result->GetValue(col_idx, row_idx).ToString() == reference->GetValue(col_idx, row_idx).ToString());
		}
	}

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		           event.candidate_traits.sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
		           StringUtil::Contains(event.ir, "perfect_hash_group_projection_composed=true");
	    },
	    [](const ExecutionRegionEvent &event) {
		    RequireGeneratedMachineCodeRegion(event);
		    REQUIRE(StringUtil::Contains(event.ir, "grouped_state_lookup=generated-perfect-hash"));
		    REQUIRE(StringUtil::Contains(event.ir, "native:typed-expression-tree"));
	    });

	RequireJitEvent(
	    manager,
	    [](const ExecutionRegionEvent &event) {
		    return IsGeneratedAggregateUpdateRuntime(event) &&
		           StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                "aggregate_update.primitive_payload_update_fused=");
	    },
	    [](const ExecutionRegionEvent &event) {
		    REQUIRE(StringUtil::Contains(EventJitRuntimePathCounts(event),
		                                 "fused_payload_update_owns_perfect_hash_group_lookup="));
		    REQUIRE_FALSE(
		        StringUtil::Contains(EventGeneratedStageRuntimeBreakdown(event), "resolve_grouped_state_addresses"));
	    });
}

TEST_CASE("JIT hash aggregate cast-only keys use native state addresses", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljitForCoverage(con, false, true, true, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_hash_aggregate_cast_projection AS "
	                          "SELECT i::BIGINT AS k, CAST(i % 100 AS DECIMAL(15,2)) AS v "
	                          "FROM range(100000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT cast_key, sum(v) "
	                        "FROM (SELECT CAST(k AS INTEGER) AS cast_key, v "
	                        "      FROM jit_hash_aggregate_cast_projection) t "
	                        "GROUP BY cast_key");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 100000);

	bool found_hash_state_address_update = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || !event.has_candidate) {
			continue;
		}
		if (event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
		if (!IsCompiledSljitRegionEvent(event)) {
			continue;
		}
		found_hash_state_address_update = true;
		RequireGeneratedMachineCodeRegion(event);
		REQUIRE(StringUtil::Contains(event.reason, "backend_aggregate=hash_update:1|primitive_payload_update:1|"
		                                           "grouped_state_address_lookup:1|native_state_address_lookup:1"));
		REQUIRE(StringUtil::Contains(event.reason, "grouped_state_lookup=native-state-address"));
		REQUIRE(StringUtil::Contains(event.ir, "native_grouped_state_contract_status=ready"));
		REQUIRE(StringUtil::Contains(event.ir, "payload_update=generated-primitive"));
		REQUIRE_FALSE(StringUtil::Contains(event.ir, "grouped_state_lookup=vectorized-address-contract"));
	}
	REQUIRE(found_hash_state_address_update);

	bool found_direct_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsGeneratedAggregateUpdateRuntime(event)) {
			continue;
		}
		found_direct_runtime = true;
		RequireGeneratedAggregateUpdateRuntimeOwnership(event);
	}
	REQUIRE(found_direct_runtime);
}

TEST_CASE("JIT combines grouped aggregate states without finalizing payload vectors", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_aggregate_state_source AS "
	                          "SELECT k, v, d, v + 1 AS v1, v + 2 AS v2, v + 3 AS v3, "
	                          "       9223372036854775807::BIGINT AS extreme_max_v, "
	                          "       (-9223372036854775807::BIGINT - 1) AS extreme_min_v "
	                          "FROM (SELECT i::BIGINT AS k, "
	                          "             CASE WHEN i % 7 = 0 THEN NULL "
	                          "                  ELSE ((i % 17) - 8)::BIGINT END AS v, "
	                          "             CASE WHEN i % 11 = 0 THEN NULL "
	                          "                  ELSE ((i % 23) - 11)::DOUBLE / 7 END AS d "
	                          "      FROM range(200000) tbl(i)) source"));
	const string query = "SELECT count(*), sum(s), sum(ds), sum(c), sum(cv), "
	                     "       sum(s1), sum(s2), sum(s3), sum(extreme_max_sum), sum(extreme_min_sum) "
	                     "FROM (SELECT k, sum(v) AS s, sum(d) AS ds, count(*) AS c, count(v) AS cv, "
	                     "             sum(v1) AS s1, sum(v2) AS s2, sum(v3) AS s3, "
	                     "             sum(extreme_max_v) AS extreme_max_sum, sum(extreme_min_v) AS extreme_min_sum "
	                     "      FROM jit_aggregate_state_source GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);
	REQUIRE(reference->GetValue(0, 0).ToString() == "200000");

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());
	REQUIRE(result->GetValue(1, 0).ToString() == reference->GetValue(1, 0).ToString());
	REQUIRE(result->GetValue(2, 0).GetValue<double>() ==
	        Approx(reference->GetValue(2, 0).GetValue<double>()).epsilon(1e-12));
	for (idx_t col_idx = 3; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	bool found_state_source_contract = false;
	bool found_state_combine_runtime = false;
	string observed_state_source_events;
	for (auto &event : manager.GetEvents()) {
		if (event.has_candidate && event.candidate_traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			observed_state_source_events += "phase=" + EventPhase(event) + ";status=" + EventStatus(event) +
			                                ";reason=" + event.reason + ";ir=" + event.ir +
			                                ";paths=" + EventJitRuntimePathCounts(event) + "\n";
		}
		if (IsCompiledSljitRegionEvent(event) && event.has_candidate &&
		    event.candidate_traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
			found_state_source_contract = true;
			REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_primitive_aggregate_batch=true"));
		}
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" ||
		    !StringUtil::Contains(EventJitRuntimePathCounts(event), "source.hash_aggregate.primitive_state_combine=")) {
			continue;
		}
		found_state_combine_runtime = true;
		RequireNativeGeneratedRuntimeWork(event);
		REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION));
		REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP));
		REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		bool found_primitive_batch_scan = false;
		for (auto &stage : event.source_stage_runtime) {
			if (stage.stage.name == "source_contract.hash_aggregate_state_scan.primitive_batch_execute_task") {
				found_primitive_batch_scan = true;
			}
			REQUIRE(stage.stage.name != "source_contract.hash_aggregate_state_scan.execute_task");
		}
		REQUIRE(found_primitive_batch_scan);
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_state_source_combine="));
		REQUIRE_FALSE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                                   "aggregate_update.primitive_payload_update="));
	}
	INFO(observed_state_source_events);
	REQUIRE(found_state_source_contract);
	REQUIRE(found_state_combine_runtime);
}

TEST_CASE("JIT homogeneous aggregate-state combine preserves signed hugeint limits", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_homogeneous_aggregate_state_source AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       9223372036854775807::BIGINT AS max_v, "
	                          "       (-9223372036854775807::BIGINT - 1) AS min_v, "
	                          "       CASE WHEN i % 2 = 0 THEN 9223372036854775807::BIGINT "
	                          "            ELSE (-9223372036854775807::BIGINT - 1) END AS mixed_v, "
	                          "       i::BIGINT AS rising_v, -i::BIGINT AS falling_v, "
	                          "       17::BIGINT AS constant_v, "
	                          "       CASE WHEN i % 7 = 0 THEN NULL "
	                          "            ELSE 9223372036854775807::BIGINT END AS nullable_max_v, "
	                          "       CASE WHEN i % 11 = 0 THEN NULL "
	                          "            ELSE (-9223372036854775807::BIGINT - 1) END AS nullable_min_v "
	                          "FROM range(200000) tbl(i)"));
	const string query = "SELECT sum(s0), sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), sum(s6), sum(s7) "
	                     "FROM (SELECT k, sum(max_v) AS s0, sum(min_v) AS s1, sum(mixed_v) AS s2, "
	                     "             sum(rising_v) AS s3, sum(falling_v) AS s4, sum(constant_v) AS s5, "
	                     "             sum(nullable_max_v) AS s6, sum(nullable_min_v) AS s7 "
	                     "      FROM jit_homogeneous_aggregate_state_source GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		REQUIRE(result->GetValue(col_idx, 0).ToString() == reference->GetValue(col_idx, 0).ToString());
	}

	bool found_state_combine_runtime = false;
	for (auto &event : manager.GetEvents()) {
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed" ||
		    !StringUtil::Contains(EventJitRuntimePathCounts(event), "source.hash_aggregate.primitive_state_combine=")) {
			continue;
		}
		found_state_combine_runtime = true;
		RequireNativeGeneratedRuntimeWork(event);
		REQUIRE(HasJitRuntimeProof(event, ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION));
		REQUIRE(EventJitRuntimeDelegationCounts(event).empty());
		REQUIRE(StringUtil::Contains(EventGeneratedStageCountBreakdown(event),
		                             "aggregate_update.primitive_state_source_combine="));
	}
	REQUIRE(found_state_combine_runtime);
}

TEST_CASE("JIT retains finalized vectors for narrow grouped aggregate states", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "off");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_narrow_aggregate_state_source AS "
	                          "SELECT i::BIGINT AS k, "
	                          "       CASE WHEN i % 7 = 0 THEN NULL ELSE ((i % 17) - 8)::BIGINT END AS v "
	                          "FROM range(200000) tbl(i)"));
	const string query = "SELECT sum(s) FROM (SELECT k, sum(v) AS s FROM jit_narrow_aggregate_state_source GROUP BY k)";
	auto reference = con.Query(query);
	REQUIRE_NO_FAIL(*reference);

	ConfigureSljitForCoverageSettings(con, false, true, true, 10000);
	ClearJitTrace(manager, true);
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == reference->GetValue(0, 0).ToString());

	bool found_state_source_contract = false;
	bool found_finalized_state_scan = false;
	string observed_state_source_events;
	for (auto &event : manager.GetEvents()) {
		const bool state_source_candidate =
		    event.has_candidate && event.candidate_traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
		    event.candidate_traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
		if (state_source_candidate || EventPhase(event) == "runtime") {
			observed_state_source_events += "phase=" + EventPhase(event) + ";status=" + EventStatus(event) +
			                                ";reason=" + event.reason + ";paths=" + EventJitRuntimePathCounts(event) +
			                                "\n";
		}
		if (state_source_candidate && IsCompiledSljitRegionEvent(event)) {
			found_state_source_contract = true;
			REQUIRE(event.selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
			REQUIRE(StringUtil::Contains(event.ir, "native_state_scan_primitive_aggregate_batch=true"));
		}
		if (EventPhase(event) != "runtime" || EventStatus(event) != "executed") {
			continue;
		}
		REQUIRE_FALSE(
		    StringUtil::Contains(EventJitRuntimePathCounts(event), "source.hash_aggregate.primitive_state_combine="));
		for (auto &stage : event.source_stage_runtime) {
			if (stage.stage.name == "source_contract.hash_aggregate_state_scan.execute_task") {
				found_finalized_state_scan = true;
			}
			REQUIRE(stage.stage.name != "source_contract.hash_aggregate_state_scan.primitive_batch_execute_task");
		}
	}
	INFO(observed_state_source_events);
	REQUIRE(found_state_source_contract);
	REQUIRE(found_finalized_state_scan);
}

TEST_CASE("JIT auto skips grouped hash aggregate address-only updates", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_aggregate_small AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(10000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                        "FROM (SELECT k, sum(v) AS s FROM jit_auto_hash_aggregate_small GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "10000");

	bool found_hash_aggregate_decision = false;
	for (auto &event : manager.GetEvents()) {
		if (!IsSljitRegionEvent(event) || EventPhase(event) != "decision" ||
		    !(StringUtil::Contains(event.ir, "hash-aggregate-update") ||
		      StringUtil::Contains(event.ir, "sink=hash-group-by") ||
		      StringUtil::Contains(event.reason, "hash-aggregate-update") ||
		      StringUtil::Contains(event.reason, "sink=hash-group-by") ||
		      event.candidate_traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE)) {
			continue;
		}
		found_hash_aggregate_decision = true;
		REQUIRE(event.selected_runner == ExecutionRunnerKind::VECTORIZED);
	}
	REQUIRE(found_hash_aggregate_decision);
}

TEST_CASE("JIT auto planner cost does not treat filtered hash aggregates as pure aggregate work", "[api][jit]") {
	DuckDB db;
	Connection con(db);
	auto &manager = ExecutionRegionManager::Get(*con.context);

	ConfigureSljit(con, "auto", false, true, false, 10000);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE jit_auto_hash_aggregate_filtered AS "
	                          "SELECT i::BIGINT AS k, (i % 17)::BIGINT AS v FROM range(1000000) tbl(i)"));

	ClearJitTrace(manager, true);
	auto result = con.Query("SELECT count(*), sum(s)::HUGEINT "
	                        "FROM (SELECT k, sum(v) AS s "
	                        "      FROM jit_auto_hash_aggregate_filtered WHERE k >= 0 GROUP BY k)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).ToString() == "1000000");

	for (auto &event : manager.GetEvents()) {
		if (!IsCompiledSljitRegionEvent(event) || !event.has_candidate ||
		    event.candidate_traits.sink_kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			continue;
		}
	}
}
