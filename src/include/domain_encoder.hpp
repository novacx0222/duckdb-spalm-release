#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value_map.hpp"

#include <limits>

namespace duckdb {

// Query-local dictionary encoder and decoder for a matrix dimension. Codes are dense,
// zero-based integers suitable for CSR indices. The reverse mapping is maintained
// so result dimensions can be decoded to their original SQL values.
class DomainEncoder {
public:
	int Encode(const Value &value) {
		auto entry = value_to_code.find(value);
		if (entry != value_to_code.end()) {
			return entry->second;
		}

		if (code_to_value.size() >= static_cast<idx_t>(std::numeric_limits<int>::max())) {
			throw OutOfRangeException("SPALM domain contains too many distinct values for 32-bit CSR indices");
		}

		const auto code = static_cast<int>(code_to_value.size());
		code_to_value.push_back(value);
		value_to_code.emplace(code_to_value.back(), code);
		return code;
	}

	const Value &Decode(idx_t code) const {
		if (code >= code_to_value.size()) {
			throw InternalException("SPALM domain code %llu is out of range", code);
		}
		return code_to_value[code];
	}

	idx_t Size() const {
		return code_to_value.size();
	}

private:
	value_map_t<int> value_to_code;
	vector<Value> code_to_value;
};

} // namespace duckdb
