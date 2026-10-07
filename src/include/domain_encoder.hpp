#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <limits>
#include <numeric>

namespace duckdb {

class DomainEncoder {
public:
	vector<int> EncodeBatch(vector<Value> values) {
		const auto old_count = code_to_value.size();
		const auto input_count = values.size();
		values.reserve(input_count + old_count);
		for (auto &value : code_to_value) {
			values.push_back(std::move(value));
		}
		vector<idx_t> order(values.size());
		std::iota(order.begin(), order.end(), idx_t(0));
		std::sort(order.begin(), order.end(), [&](idx_t left, idx_t right) {
			const auto &lhs = values[left];
			const auto &rhs = values[right];
			return rhs.IsNull() ? !lhs.IsNull() : !lhs.IsNull() && lhs < rhs;
		});
		vector<int> codes(input_count);
		old_to_new.resize(old_count);
		code_to_value.clear();
		for (auto index : order) {
			if (code_to_value.empty() || !Value::NotDistinctFrom(code_to_value.back(), values[index])) {
				if (code_to_value.size() >= static_cast<idx_t>(std::numeric_limits<int>::max())) {
					throw OutOfRangeException("SPALM domain contains too many distinct values for 32-bit CSR indices");
				}
				code_to_value.push_back(std::move(values[index]));
			}
			const auto code = static_cast<int>(code_to_value.size() - 1);
			if (index < input_count) {
				codes[index] = code;
			} else {
				old_to_new[index - input_count] = code;
			}
		}
		return codes;
	}

	int Remap(int code) const {
		return old_to_new[code];
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
	vector<Value> code_to_value;
	vector<int> old_to_new;
};

} // namespace duckdb
