#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp" // For ClientConfig options

namespace duckdb {

void PhysicalSet::SetExtensionVariable(ClientContext &context, ExtensionOption &extension_option, const string &name,
                                       SetScope scope, const Value &value) {
	auto &config = DBConfig::GetConfig(context);
	auto &target_type = extension_option.type;
	Value target_value = value.CastAs(context, target_type);
	if (extension_option.set_function) {
		extension_option.set_function(context, scope, target_value);
	}
	if (scope == SetScope::GLOBAL) {
		config.SetOption(name, std::move(target_value));
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		client_config.set_variables[name] = std::move(target_value);
	}
}

SourceResultType PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	// Special handling for 'enable_luajit_jit'
	if (StringUtil::Lower(name) == "enable_luajit_jit") {
		if (scope == SetScope::GLOBAL) {
			throw InvalidInputException("Cannot SET GLOBAL enable_luajit_jit. This setting is session-local.");
		}
		// The value should already be a BOOLEAN due to binder actions.
		// If not, BooleanValue::Get will throw or an explicit cast is needed.
		// For robustness, as per prompt, ensure cast if somehow not boolean.
		ClientConfig::GetConfig(context.client).options.enable_luajit_jit =
		    BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
		return SourceResultType::FINISHED;
	} else if (StringUtil::Lower(name) == "luajit_jit_complexity_threshold") {
		if (scope == SetScope::GLOBAL) {
			throw InvalidInputException("Cannot SET GLOBAL luajit_jit_complexity_threshold. This setting is session-local.");
		}
		// Value should be BIGINT from binder, validation (>=0) also done in binder and SetLocal.
		// Directly call the SetLocal logic via the option object to ensure any other side effects are triggered.
		auto option = DBConfig::GetOptionByName(name);
		D_ASSERT(option && option->set_local); // Should be registered
		option->set_local(context.client, value.DefaultCastAs(LogicalType::BIGINT));
		return SourceResultType::FINISHED;
	} else if (StringUtil::Lower(name) == "luajit_jit_trigger_count") {
		if (scope == SetScope::GLOBAL) {
			throw InvalidInputException("Cannot SET GLOBAL luajit_jit_trigger_count. This setting is session-local.");
		}
		auto option = DBConfig::GetOptionByName(name);
		D_ASSERT(option && option->set_local);
		option->set_local(context.client, value.DefaultCastAs(LogicalType::BIGINT));
		return SourceResultType::FINISHED;
	}

	auto &config = DBConfig::GetConfig(context.client);
	// check if we are allowed to change the configuration option
	config.CheckLock(name);
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		auto entry = config.extension_parameters.find(name);
		if (entry == config.extension_parameters.end()) {
			Catalog::AutoloadExtensionByConfigName(context.client, name);
			entry = config.extension_parameters.find(name);
			// if entry is still not found, throw an error
			if (entry == config.extension_parameters.end()) {
				throw BinderException("Unrecognized configuration parameter \"%s\"", name);
			}
			// if entry is still not found, throw an error
			if (entry == config.extension_parameters.end()) {
				throw BinderException("Unrecognized configuration parameter \"%s\"", name);
			}
			D_ASSERT(entry != config.extension_parameters.end());
		}
		SetExtensionVariable(context.client, entry->second, name, scope, value);
		return SourceResultType::FINISHED;
	}
	SetScope variable_scope = scope;
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option->set_local) {
			variable_scope = SetScope::SESSION;
		} else {
			D_ASSERT(option->set_global);
			variable_scope = SetScope::GLOBAL;
		}
	}

	Value input_val = value.CastAs(context.client, DBConfig::ParseLogicalType(option->parameter_type));
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		auto &config = DBConfig::GetConfig(context.client);
		config.SetOption(&db, *option, input_val);
		break;
	}
	case SetScope::SESSION:
		if (!option->set_local) {
			throw CatalogException("option \"%s\" cannot be set locally", name);
		}
		option->set_local(context.client, input_val);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
