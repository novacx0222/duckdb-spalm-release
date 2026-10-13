#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value_map.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <limits>
#include <numeric>

namespace duckdb {

class DomainEncoder {
public:
	vector<int> EncodeBatch(vector<Value> values, const int timing_side = 0, const char *domain = "domain") {
		auto stage_begin = std::chrono::steady_clock::now();
		auto mark = [&](const char *stage) {
			const auto end = std::chrono::steady_clock::now();
			const double ms = std::chrono::duration<double, std::milli>(end - stage_begin).count();

			if (timing_side > 0) {
				std::printf("[%d-%s-encode-%s] %.3f ms\n", timing_side, domain, stage, ms);
			}
			stage_begin = std::chrono::steady_clock::now();
		};

		const auto old_count = code_to_value.size();
		const auto input_count = values.size();

		// Temporary codes follow encounter order.
		// Only distinct Values enter the sorting stage.
		value_map_t<int> value_to_temp;
		vector<Value> unique_values;

		value_to_temp.reserve(old_count);
		unique_values.reserve(old_count);

		// Seed the dictionary with the previous domain.
		// Its temporary codes match its previous sorted codes.
		for (idx_t i = 0; i < old_count; ++i) {
			value_to_temp.emplace(code_to_value[i], static_cast<int>(i));
			unique_values.push_back(code_to_value[i]);
		}
		mark("append-old-domain");

		// Record one temporary code per input record.
		vector<int> codes(input_count);

		for (idx_t i = 0; i < input_count; ++i) {
			auto entry = value_to_temp.find(values[i]);
			if (entry != value_to_temp.end()) {
				codes[i] = entry->second;
				continue;
			}

			if (unique_values.size() >= static_cast<idx_t>(std::numeric_limits<int>::max())) {
				throw OutOfRangeException("SPALM domain contains too many distinct "
				                          "values for 32-bit CSR indices");
			}

			const auto temp_code = static_cast<int>(unique_values.size());

			// The map owns its key; the vector owns the value
			// used for sorting and eventual decoding.
			value_to_temp.emplace(values[i], temp_code);
			unique_values.push_back(std::move(values[i]));
			codes[i] = temp_code;
		}
		mark("deduplicate-input");

		vector<idx_t> order(unique_values.size());
		std::iota(order.begin(), order.end(), static_cast<idx_t>(0));
		mark("order-allocate-fill");

		std::sort(order.begin(), order.end(), [&](const idx_t left, const idx_t right) {
			const auto &lhs = unique_values[left];
			const auto &rhs = unique_values[right];
			return rhs.IsNull() ? !lhs.IsNull() : !lhs.IsNull() && lhs < rhs;
		});
		mark("sort");

		vector<int> temp_to_sorted(unique_values.size());
		old_to_new.resize(old_count);

		code_to_value.clear();
		code_to_value.reserve(unique_values.size());
		mark("codes-allocate-reset");

		// Values are already distinct: assign sorted codes
		// and rebuild the reverse mapping for Decode().
		for (idx_t sorted_code = 0; sorted_code < order.size(); ++sorted_code) {
			const auto temp_code = order[sorted_code];

			temp_to_sorted[temp_code] = static_cast<int>(sorted_code);
			code_to_value.push_back(std::move(unique_values[temp_code]));
		}
		mark("assign-codes");

		// Restore the original input record order.
		for (auto &code : codes) {
			code = temp_to_sorted[code];
		}

		// Translate previous codes to the new sorted domain.
		for (idx_t old_code = 0; old_code < old_count; ++old_code) {
			old_to_new[old_code] = temp_to_sorted[old_code];
		}
		mark("remap-codes");

		// Explicitly account for temporary dictionary cleanup.
		value_map_t<int>().swap(value_to_temp);
		vector<Value>().swap(unique_values);
		vector<idx_t>().swap(order);
		vector<int>().swap(temp_to_sorted);
		vector<Value>().swap(values);
		mark("temporary-deallocate");

		if (timing_side > 0) {
			std::printf("[%d-%s-encode-stats] "
			            "input=%llu old_keys=%llu unique_keys=%llu\n",
			            timing_side, domain, static_cast<unsigned long long>(input_count),
			            static_cast<unsigned long long>(old_count),
			            static_cast<unsigned long long>(code_to_value.size()));
		}

		return codes;
	}

	int Remap(const int code) const {
		return old_to_new[code];
	}

	const Value &Decode(const idx_t code) const {
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
