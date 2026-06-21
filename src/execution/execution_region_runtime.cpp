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

SinkResultType ExecutionSinkAppend(const ExecutionAppendSinkBinding &binding, DataChunk &input) {
	if (!binding.ready || !binding.state) {
		throw InternalException("execution append sink binding is incomplete");
	}
	return binding.state->Append(input);
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

} // namespace duckdb
