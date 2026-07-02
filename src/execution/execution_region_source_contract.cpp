#include "execution_region_source_contract.hpp"

namespace duckdb {

string DescribeExecutionSourceProtocolContract(const ExecutionSourceProtocolContract &contract) {
	if (contract.status == ExecutionRegionSourceContractStatus::NONE) {
		return string();
	}
	string result = "source_contract<status=" + string(ExecutionRegionSourceContractStatusToString(contract.status));
	result += ",required_capability=" + contract.required_capability;
	result += ",contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		result += ",blocker=" + contract.blocker;
	}
	result += ">";
	return result;
}

ExecutionSourceProtocolContract BuildExecutionSourceProtocolContract(ExecutionRegionSourceKind kind,
                                                                     ExecutionRegionSourceExecutionKind execution) {
	ExecutionSourceProtocolContract result;
	if (kind == ExecutionRegionSourceKind::NONE) {
		return result;
	}
	result.status = execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
	                    ? ExecutionRegionSourceContractStatus::READY
	                    : ExecutionRegionSourceContractStatus::BLOCKED;
	result.contract_version = "v1";
	switch (kind) {
	case ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN:
		result.required_capability = "duckdb-table-scan-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "duckdb-table-scan-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN:
		result.required_capability = "table-function-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "table-function-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::GENERIC_SCAN:
		result.required_capability = "generic-scan-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "generic-scan-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::STATEFUL_OPERATOR:
		result.required_capability = "stateful-operator-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "stateful-source-contract-boundary";
		}
		break;
	default:
		break;
	}
	result.ir = DescribeExecutionSourceProtocolContract(result);
	return result;
}

} // namespace duckdb
