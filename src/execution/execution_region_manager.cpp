#include "duckdb/execution/execution_region_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

namespace {

struct ExecutionRegionBackendCandidate {
	ExecutionRegionBackendCandidate(ExecutionRegionBackend &backend_p, string name_p)
	    : backend(backend_p), name(std::move(name_p)) {
	}

	reference<ExecutionRegionBackend> backend;
	string name;
};

struct ExecutionRegionBackendSnapshot {
	ExecutionRegionBackendSnapshot(ExecutionRegionBackend &backend_p, const string &normalized_name_p,
	                               const string &name_p, const string &description_p, ExecutionRunnerKind runner_kind_p,
	                               bool supports_regions_p)
	    : backend(backend_p), normalized_name(normalized_name_p) {
		info.name = name_p;
		info.description = description_p;
		info.runner_kind = runner_kind_p;
		info.supports_regions = supports_regions_p;
	}

	reference<ExecutionRegionBackend> backend;
	string normalized_name;
	ExecutionRegionBackendInfo info;
};

} // namespace

ExecutionRegionManager::ExecutionRegionManager(DatabaseInstance &db) : db(db) {
}

void ExecutionRegionManager::RegisterBackend(unique_ptr<ExecutionRegionBackend> backend, uint64_t backend_abi_version) {
	if (backend_abi_version != EXECUTION_REGION_BACKEND_ABI_VERSION) {
		throw InvalidInputException("Execution region backend ABI version mismatch: backend uses %llu, DuckDB requires "
		                            "%llu; rebuild the backend against this DuckDB version",
		                            static_cast<unsigned long long>(backend_abi_version),
		                            static_cast<unsigned long long>(EXECUTION_REGION_BACKEND_ABI_VERSION));
	}
	if (!backend) {
		throw InvalidInputException("Cannot register a NULL execution region backend");
	}
	auto name = backend->Name();
	auto normalized_name = StringUtil::Lower(name);
	if (normalized_name.empty()) {
		throw InvalidInputException("Cannot register an execution region backend with an empty name");
	}
	auto description = backend->Description();
	auto runner_kind = backend->RunnerKind();
	auto supports_regions = backend->SupportsRegions();

	lock_guard<mutex> guard(lock);
	for (auto &entry : backends) {
		if (entry.normalized_name == normalized_name) {
			throw InvalidInputException("Execution region backend \"%s\" is already registered", normalized_name);
		}
	}
	backends.emplace_back(std::move(backend), std::move(name), std::move(normalized_name), std::move(description),
	                      runner_kind, supports_regions);
}

void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend,
                                    uint64_t backend_abi_version) {
	ExecutionRegionManager::Get(db).RegisterBackend(std::move(backend), backend_abi_version);
}

bool ExecutionRegionManager::HasAvailableBackendForRunner(ClientContext &context,
                                                          ExecutionRunnerKind runner_kind) const {
	auto requested = StringUtil::Lower(ExecutionRegionSettings::RequestedBackend(context));
	vector<reference<ExecutionRegionBackend>> candidates;
	{
		lock_guard<mutex> guard(lock);
		for (auto &entry : backends) {
			if (requested != "auto" && entry.normalized_name != requested) {
				continue;
			}
			if (entry.runner_kind == runner_kind && entry.supports_regions) {
				candidates.emplace_back(*entry.backend);
			}
		}
	}
	for (auto &backend : candidates) {
		if (backend.get().IsAvailable()) {
			return true;
		}
	}
	return false;
}

optional_ptr<ExecutionRegionBackend> ExecutionRegionManager::SelectBackend(ClientContext &context, string &backend_name,
                                                                           ExecutionRunnerKind runner_kind) const {
	auto requested_name = ExecutionRegionSettings::RequestedBackend(context);
	auto requested = StringUtil::Lower(requested_name);
	if (requested == "auto") {
		vector<ExecutionRegionBackendCandidate> candidates;
		{
			lock_guard<mutex> guard(lock);
			for (auto &entry : backends) {
				if (entry.runner_kind == runner_kind && entry.supports_regions) {
					candidates.emplace_back(*entry.backend, entry.name);
				}
			}
		}
		for (auto &candidate : candidates) {
			if (candidate.backend.get().IsAvailable()) {
				backend_name = candidate.name;
				return candidate.backend.get();
			}
		}
		backend_name = "auto";
		return nullptr;
	}
	optional_ptr<ExecutionRegionBackend> selected_backend;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	{
		lock_guard<mutex> guard(lock);
		for (auto &entry : backends) {
			if (entry.normalized_name != requested) {
				continue;
			}
			backend_name = entry.name;
			selected_backend = *entry.backend;
			selected_runner = entry.runner_kind;
			break;
		}
	}
	if (!selected_backend) {
		throw InvalidInputException("Execution region backend \"%s\" is not registered", requested_name);
	}
	if (!selected_backend->IsAvailable()) {
		throw InvalidInputException("Execution region backend \"%s\" is registered but not available", requested_name);
	}
	if (selected_runner != runner_kind) {
		return nullptr;
	}
	return selected_backend;
}

vector<ExecutionRegionBackendInfo> ExecutionRegionManager::GetBackends(ClientContext *context) const {
	vector<ExecutionRegionBackendSnapshot> snapshots;
	{
		lock_guard<mutex> guard(lock);
		snapshots.reserve(backends.size());
		for (auto &entry : backends) {
			snapshots.emplace_back(*entry.backend, entry.normalized_name, entry.name, entry.description,
			                       entry.runner_kind, entry.supports_regions);
		}
	}

	for (auto &snapshot : snapshots) {
		snapshot.info.available = snapshot.backend.get().IsAvailable();
	}

	if (context && ExecutionRegionSettings::Enabled(*context)) {
		auto requested = StringUtil::Lower(ExecutionRegionSettings::RequestedBackend(*context));
		if (requested == "auto") {
			bool selected = false;
			for (auto runner_kind : {ExecutionRunnerKind::COMPILED_VECTORIZED, ExecutionRunnerKind::COMPILED_GPU}) {
				for (auto &snapshot : snapshots) {
					if (snapshot.info.available && snapshot.info.supports_regions &&
					    snapshot.info.runner_kind == runner_kind) {
						snapshot.info.selected = true;
						selected = true;
						break;
					}
				}
				if (selected) {
					break;
				}
			}
		} else {
			for (auto &snapshot : snapshots) {
				if (snapshot.normalized_name == requested && snapshot.info.available) {
					snapshot.info.selected = true;
					break;
				}
			}
		}
	}

	vector<ExecutionRegionBackendInfo> result;
	result.reserve(snapshots.size());
	for (auto &snapshot : snapshots) {
		result.push_back(std::move(snapshot.info));
	}
	return result;
}

ExecutionRegionManager &ExecutionRegionManager::Get(DatabaseInstance &db) {
	return db.GetExecutionRegionManager();
}

ExecutionRegionManager &ExecutionRegionManager::Get(ClientContext &context) {
	return ExecutionRegionManager::Get(DatabaseInstance::GetDatabase(context));
}

} // namespace duckdb
