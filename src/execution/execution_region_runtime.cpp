#include "duckdb/execution/execution_region_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

static mutex &ExecutionRegionStageRegistryLock() {
	static mutex lock;
	return lock;
}

static unordered_map<string, idx_t> &ExecutionRegionStageRegistryIndex() {
	static unordered_map<string, idx_t> index;
	return index;
}

static vector<string> &ExecutionRegionStageRegistryNames() {
	static vector<string> names;
	return names;
}

ExecutionRegionStageId::ExecutionRegionStageId() {
}

ExecutionRegionStageId::ExecutionRegionStageId(const char *name_p)
    : ExecutionRegionStageId(string(name_p ? name_p : "")) {
}

ExecutionRegionStageId::ExecutionRegionStageId(string name_p) {
	if (name_p.empty()) {
		return;
	}
	auto &lock = ExecutionRegionStageRegistryLock();
	lock_guard<mutex> guard(lock);
	auto &index = ExecutionRegionStageRegistryIndex();
	auto entry = index.find(name_p);
	auto &names = ExecutionRegionStageRegistryNames();
	if (entry != index.end()) {
		key = entry->second;
		name = names[key - 1];
		return;
	}
	key = names.size() + 1;
	names.push_back(std::move(name_p));
	name = names.back();
	index[name] = key;
}

bool ExecutionRegionStageId::IsValid() const {
	return key != 0 && !name.empty();
}

const char *ExecutionRegionJitRuntimeProofName(ExecutionRegionJitRuntimeProof proof) {
	switch (proof) {
	case ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK:
		return "generated_stage_work";
	case ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK:
		return "generated_backend_work";
	case ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION:
		return "materialization_elision";
	case ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP:
		return "full_pipeline_ownership";
	case ExecutionRegionJitRuntimeProof::DELEGATED_RUNTIME_WORK:
		return "delegated_runtime_work";
	case ExecutionRegionJitRuntimeProof::NO_WORK:
		return "no_work";
	default:
		throw InternalException("unknown JIT runtime proof");
	}
}

string RenderExecutionRegionJitRuntimeProofRequirements(ExecutionRegionJitRuntimeProofMask requirements) {
	string result;
	for (uint8_t proof_idx = 0; proof_idx < static_cast<uint8_t>(ExecutionRegionJitRuntimeProof::COUNT); proof_idx++) {
		auto proof = static_cast<ExecutionRegionJitRuntimeProof>(proof_idx);
		if (!ExecutionRegionJitRuntimeProofRequired(requirements, proof)) {
			continue;
		}
		if (!result.empty()) {
			result += "|";
		}
		result += ExecutionRegionJitRuntimeProofName(proof);
	}
	return result;
}

ExecutionOperatorStageTimer::ExecutionOperatorStageTimer(optional_ptr<ExecutionOperatorStageRecorder> recorder_p,
                                                         ExecutionRegionStageId stage_p)
    : recorder(recorder_p), stage(std::move(stage_p)),
      start(recorder ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point()) {
}

ExecutionOperatorStageTimer::ExecutionOperatorStageTimer(optional_ptr<ExecutionOperatorStageRecorder> recorder_p,
                                                         const char *stage_name)
    : recorder(recorder_p), stage(recorder ? ExecutionRegionStageId(stage_name) : ExecutionRegionStageId()),
      start(recorder ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point()) {
}

ExecutionOperatorStageTimer::~ExecutionOperatorStageTimer() {
	if (!recorder) {
		return;
	}
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	recorder->RecordStageRuntime(stage, elapsed);
}

void AddExecutionRegionStageRuntime(vector<ExecutionRegionRecordedStageRuntime> &stages, ExecutionRegionStageId stage,
                                    int64_t runtime_time_us, idx_t count) {
	if (!stage.IsValid() || runtime_time_us < 0) {
		return;
	}
	if (count == 0) {
		return;
	}
	for (auto &entry : stages) {
		if (entry.stage.key == stage.key) {
			entry.runtime_time_us += runtime_time_us;
			entry.count += count;
			return;
		}
	}
	ExecutionRegionRecordedStageRuntime entry;
	entry.stage = stage;
	entry.runtime_time_us = runtime_time_us;
	entry.count = count;
	stages.push_back(std::move(entry));
}

void MergeExecutionRegionStageRuntime(vector<ExecutionRegionRecordedStageRuntime> &target,
                                      const vector<ExecutionRegionRecordedStageRuntime> &source) {
	for (auto &entry : source) {
		AddExecutionRegionStageRuntime(target, entry.stage, entry.runtime_time_us, entry.count);
	}
}

void AddExecutionRegionRecordedCounter(vector<ExecutionRegionRecordedCounter> &counters, ExecutionRegionStageId counter,
                                       idx_t count) {
	if (!counter.IsValid() || count == 0) {
		return;
	}
	for (auto &entry : counters) {
		if (entry.counter.key == counter.key) {
			entry.count += count;
			return;
		}
	}
	ExecutionRegionRecordedCounter entry;
	entry.counter = std::move(counter);
	entry.count = count;
	counters.push_back(std::move(entry));
}

void MergeExecutionRegionRecordedCounters(vector<ExecutionRegionRecordedCounter> &target,
                                          const vector<ExecutionRegionRecordedCounter> &source) {
	for (auto &entry : source) {
		AddExecutionRegionRecordedCounter(target, entry.counter, entry.count);
	}
}

void AddExecutionRegionLazyCodegenMetrics(ExecutionRegionLazyCodegenMetrics &target,
                                          const ExecutionRegionLazyCodegenMetrics &source) {
	target.codegen_time_us += source.codegen_time_us;
	target.machine_codegen_time_us += source.machine_codegen_time_us;
	target.code_size += source.code_size;
}

string RenderExecutionRegionStageRuntimeBreakdown(const vector<ExecutionRegionRecordedStageRuntime> &stages) {
	string result;
	for (auto &entry : stages) {
		if (!result.empty()) {
			result += ";";
		}
		result += entry.stage.name + "=" + std::to_string(entry.runtime_time_us);
	}
	return result;
}

string RenderExecutionRegionStageCountBreakdown(const vector<ExecutionRegionRecordedStageRuntime> &stages) {
	string result;
	for (auto &entry : stages) {
		if (!result.empty()) {
			result += ";";
		}
		result += entry.stage.name + "=" + std::to_string(entry.count);
	}
	return result;
}

string RenderExecutionRegionCounterBreakdown(const vector<ExecutionRegionRecordedCounter> &counters) {
	string result;
	for (auto &entry : counters) {
		if (!result.empty()) {
			result += ";";
		}
		result += entry.counter.name + "=" + std::to_string(entry.count);
	}
	return result;
}

void ExecutionRegionSourceContractMetrics::RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) {
	AddExecutionRegionStageRuntime(get_data_stages, stage, runtime_time_us);
}

int64_t ExecutionRegionSourceContractMetrics::GetDataStageRuntimeSum() const {
	int64_t result = 0;
	for (auto &entry : get_data_stages) {
		result += entry.runtime_time_us;
	}
	return result;
}

const ExecutionPrimitiveAggregateUpdateLane *
ExecutionPrimitiveAggregateUpdateBinding::FindLane(idx_t aggregate_index) const {
	for (auto &lane : lanes) {
		if (lane.aggregate_index == aggregate_index) {
			return &lane;
		}
	}
	return nullptr;
}

ExecutionOperatorBindResult ExecutionAppendSinkState::PrepareDirectAppend(const vector<LogicalType> &, idx_t,
                                                                          DirectAppendReservation &, string &blocker,
                                                                          optional_ptr<DirectAppendProfile>) {
	blocker = "direct-append-not-supported";
	return ExecutionOperatorBindResult::INVALID;
}

SinkResultType ExecutionAppendSinkState::CommitDirectAppend(const DirectAppendReservation &,
                                                            optional_ptr<DirectAppendProfile>) {
	throw InternalException("direct append commit called on a sink without direct append support");
}

SinkResultType ExecutionSinkAppend(const ExecutionAppendSinkBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution append sink binding is incomplete");
	}
	return binding.state->Append(input);
}

ExecutionOperatorBindResult ExecutionPrepareDirectAppend(const ExecutionAppendSinkBinding &binding,
                                                         const vector<LogicalType> &types, idx_t count,
                                                         DirectAppendReservation &reservation, string &blocker,
                                                         optional_ptr<DirectAppendProfile> profile) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution append sink binding is incomplete");
	}
	return binding.state->PrepareDirectAppend(types, count, reservation, blocker, profile);
}

SinkResultType ExecutionCommitDirectAppend(const ExecutionAppendSinkBinding &binding,
                                           const DirectAppendReservation &reservation,
                                           optional_ptr<DirectAppendProfile> profile) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution append sink binding is incomplete");
	}
	return binding.state->CommitDirectAppend(reservation, profile);
}

SinkResultType ExecutionSinkDelimJoin(const ExecutionDelimJoinSinkBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution delimiter join sink binding is incomplete");
	}
	return binding.state->Sink(input);
}

ExecutionOperatorRuntime::~ExecutionOperatorRuntime() {
}

ExecutionRegionRuntime::~ExecutionRegionRuntime() {
}

void ExecutionRegionRuntime::RecordHashJoinProbeLayout(const char *) {
}

void ExecutionRegionRuntime::RecordJitRuntimePath(const char *, idx_t) {
}

void ExecutionRegionRuntime::RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof, idx_t) {
}

void ExecutionRegionRuntime::RecordJitRuntimeProofDetail(const char *, idx_t) {
}

void ExecutionRegionRuntime::RecordJitRuntimeDelegation(const char *, idx_t) {
}

void ExecutionRegionRuntime::RecordLazyCodegen(const ExecutionRegionLazyCodegenMetrics &) {
}

bool ExecutionRegionRuntime::TryMarkOnce(ExecutionRegionRuntimeOnceFlag, idx_t) {
	return true;
}

} // namespace duckdb
