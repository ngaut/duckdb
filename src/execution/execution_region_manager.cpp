#include "duckdb/execution/execution_region_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

ExecutionRegionManager::ExecutionRegionManager(DatabaseInstance &db) : db(db) {
}

void ExecutionRegionManager::RegisterBackend(unique_ptr<ExecutionRegionBackend> backend) {
	if (!backend) {
		throw InvalidInputException("Cannot register a NULL execution region backend");
	}
	auto name = StringUtil::Lower(backend->Name());
	if (name.empty()) {
		throw InvalidInputException("Cannot register an execution region backend with an empty name");
	}
	lock_guard<mutex> guard(lock);
	for (auto &entry : backends) {
		if (StringUtil::Lower(entry->Name()) == name) {
			throw InvalidInputException("Execution region backend \"%s\" is already registered", name);
		}
	}
	backends.push_back(std::move(backend));
}

void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend) {
	ExecutionRegionManager::Get(db).RegisterBackend(std::move(backend));
}

optional_ptr<ExecutionRegionBackend> ExecutionRegionManager::SelectBackend(ClientContext &context,
                                                                           string &backend_name) const {
	auto requested = ExecutionRegionSettings::RequestedBackend(context);
	lock_guard<mutex> guard(lock);
	if (requested == "auto") {
		for (auto &backend : backends) {
			if (backend->IsAvailable()) {
				backend_name = backend->Name();
				return *backend;
			}
		}
		backend_name = "auto";
		return nullptr;
	}
	for (auto &backend : backends) {
		if (StringUtil::Lower(backend->Name()) != requested) {
			continue;
		}
		backend_name = backend->Name();
		if (!backend->IsAvailable()) {
			throw InvalidInputException("Execution region backend \"%s\" is registered but not available", requested);
		}
		return *backend;
	}
	throw InvalidInputException("Execution region backend \"%s\" is not registered", requested);
}

vector<ExecutionRegionBackendInfo> ExecutionRegionManager::GetBackends(ClientContext *context) const {
	string selected_name;
	if (context && ExecutionRegionSettings::Enabled(*context)) {
		try {
			SelectBackend(*context, selected_name);
		} catch (...) {
			selected_name.clear();
		}
	}
	selected_name = StringUtil::Lower(selected_name);

	lock_guard<mutex> guard(lock);
	vector<ExecutionRegionBackendInfo> result;
	result.reserve(backends.size());
	for (auto &backend : backends) {
		ExecutionRegionBackendInfo info;
		info.name = backend->Name();
		info.description = backend->Description();
		info.available = backend->IsAvailable();
		info.supports_regions = backend->SupportsRegions();
		info.selected = StringUtil::Lower(info.name) == selected_name;
		result.push_back(std::move(info));
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
