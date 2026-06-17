#include "duckdb/execution/execution_region_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_region_registration.hpp"
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

void ExecutionRegionManager::AddAdmissionProfileRule(string backend_name, ExecutionRegionAdmissionRule rule) {
	auto normalized_backend_name = StringUtil::Lower(std::move(backend_name));
	if (normalized_backend_name.empty()) {
		throw InvalidInputException("Cannot add an execution region admission rule with an empty backend name");
	}
	if (rule.admission_key.empty()) {
		throw InvalidInputException("Cannot add an execution region admission rule with an empty admission key");
	}
	if (rule.proof.empty()) {
		throw InvalidInputException("Cannot add an execution region admission rule without proof");
	}
	lock_guard<mutex> guard(lock);
	for (auto &entry : admission_profile_rules) {
		if (entry.backend_name != normalized_backend_name || entry.rule.target != rule.target ||
		    entry.rule.admission_key != rule.admission_key) {
			continue;
		}
		entry.rule = std::move(rule);
		return;
	}
	ExecutionRegionAdmissionProfileRule entry;
	entry.backend_name = std::move(normalized_backend_name);
	entry.rule = std::move(rule);
	admission_profile_rules.push_back(std::move(entry));
}

vector<ExecutionRegionAdmissionProfileRule> ExecutionRegionManager::GetAdmissionProfileRules() const {
	lock_guard<mutex> guard(lock);
	return admission_profile_rules;
}

bool ExecutionRegionManager::HasAdmissionProfileRules(const string &backend_name,
                                                      ExecutionRegionCompileTarget target) const {
	auto normalized_backend_name = StringUtil::Lower(backend_name);
	lock_guard<mutex> guard(lock);
	for (auto &entry : admission_profile_rules) {
		if (entry.backend_name == normalized_backend_name && entry.rule.target == target) {
			return true;
		}
	}
	return false;
}

bool ExecutionRegionManager::GetAdmissionProfileRule(const string &backend_name, ExecutionRegionCompileTarget target,
                                                     const string &admission_key,
                                                     ExecutionRegionAdmissionRule &rule) const {
	auto normalized_backend_name = StringUtil::Lower(backend_name);
	lock_guard<mutex> guard(lock);
	for (auto &entry : admission_profile_rules) {
		if (entry.backend_name != normalized_backend_name || entry.rule.target != target ||
		    entry.rule.admission_key != admission_key) {
			continue;
		}
		rule = entry.rule;
		return true;
	}
	return false;
}

void ExecutionRegionManager::ClearAdmissionProfileRules() {
	lock_guard<mutex> guard(lock);
	admission_profile_rules.clear();
}

void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend) {
	ExecutionRegionManager::Get(db).RegisterBackend(std::move(backend));
}

ExecutionRegionPolicyMode ExecutionRegionManager::GetPolicy(ClientContext &context) const {
	return ExecutionRegionSettings::Policy(context);
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
