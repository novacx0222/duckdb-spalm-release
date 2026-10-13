#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value_map.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <unordered_map>
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

	// Select once from the SQL column type, not from individual records.
	static bool SupportsIntegerEncoding(const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
			return true;
		default:
			return false;
		}
	}

	vector<int> EncodeIntegerBatch(vector<int64_t> values, vector<uint8_t> valid, const LogicalType &type,
	                               const int timing_side = 0, const char *domain = "domain") {
		if (!SupportsIntegerEncoding(type) || values.size() != valid.size()) {
			throw InternalException("Invalid typed domain encoder input");
		}
		auto stage_begin = std::chrono::steady_clock::now();
		auto mark = [&](const char *stage) {
			const auto end = std::chrono::steady_clock::now();
			if (timing_side > 0) {
				const double ms = std::chrono::duration<double, std::milli>(end - stage_begin).count();
				std::printf("[%d-%s-encode-%s] %.3f ms\n", timing_side, domain, stage, ms);
			}
			stage_begin = std::chrono::steady_clock::now();
		};

		const auto input_count = values.size();
		const auto old_count = code_to_value.size();
		vector<int64_t> old_values(old_count);
		vector<uint8_t> old_valid(old_count, 0);
		bool have_value = false;
		int64_t minimum = 0, maximum = 0;
		auto inspect = [&](int64_t value) {
			if (!have_value) {
				minimum = maximum = value;
				have_value = true;
			} else {
				minimum = std::min(minimum, value);
				maximum = std::max(maximum, value);
			}
		};
		for (idx_t i = 0; i < old_count; ++i) {
			if (code_to_value[i].type() != type) {
				throw InternalException("Shared domain encoder SQL type changed");
			}
			if (!code_to_value[i].IsNull()) {
				old_valid[i] = 1;
				old_values[i] = code_to_value[i].GetValue<int64_t>();
				inspect(old_values[i]);
			}
		}
		for (idx_t i = 0; i < input_count; ++i) {
			if (valid[i])
				inspect(values[i]);
		}
		mark("range-scan");

		// Limit direct-map storage to about 4 MiB. The unsigned
		// difference handles negative IDs and avoids signed overflow.
		constexpr uint64_t MAX_DIRECT_RANGE = 1ULL << 20;
		const uint64_t distance = static_cast<uint64_t>(maximum) - static_cast<uint64_t>(minimum);
		const bool direct = have_value && distance < MAX_DIRECT_RANGE;
		const idx_t range = direct ? static_cast<idx_t>(distance + 1) : 0;
		vector<int> direct_to_temp;
		if (direct)
			direct_to_temp.assign(range, -1);
		mark("direct-map-allocate");

		std::unordered_map<int64_t, int> value_to_temp;
		if (!direct)
			value_to_temp.reserve(old_count);
		vector<int64_t> unique_values;
		unique_values.reserve(old_count);
		vector<int> old_temp_codes(old_count);
		int null_code = -1;
		auto append_unique = [&](int64_t value) {
			if (unique_values.size() >= static_cast<idx_t>(std::numeric_limits<int>::max())) {
				throw OutOfRangeException("SPALM domain contains too many distinct values for 32-bit CSR indices");
			}
			const auto code = static_cast<int>(unique_values.size());
			unique_values.push_back(value);
			return code;
		};
		auto encode = [&](int64_t value, bool is_valid) {
			if (!is_valid) {
				if (null_code < 0)
					null_code = append_unique(0);
				return null_code;
			}
			if (direct) {
				const auto offset = static_cast<uint64_t>(value) - static_cast<uint64_t>(minimum);
				auto &code = direct_to_temp[static_cast<idx_t>(offset)];
				if (code < 0)
					code = append_unique(value);
				return code;
			}
			auto entry = value_to_temp.find(value);
			if (entry != value_to_temp.end())
				return entry->second;
			const int code = append_unique(value);
			value_to_temp.emplace(value, code);
			return code;
		};
		for (idx_t i = 0; i < old_count; ++i) {
			old_temp_codes[i] = encode(old_values[i], old_valid[i] != 0);
		}
		mark("append-old-domain");
		vector<int> codes(input_count);
		for (idx_t i = 0; i < input_count; ++i)
			codes[i] = encode(values[i], valid[i] != 0);
		mark("deduplicate-input");

		vector<idx_t> order(unique_values.size());
		std::iota(order.begin(), order.end(), static_cast<idx_t>(0));
		mark("order-allocate-fill");
		if (direct) {
			idx_t position = 0;
			for (const auto code : direct_to_temp) {
				if (code >= 0)
					order[position++] = static_cast<idx_t>(code);
			}
			if (null_code >= 0)
				order[position++] = static_cast<idx_t>(null_code);
			if (position != order.size())
				throw InternalException("Direct domain encoder key count mismatch");
		} else {
			std::sort(order.begin(), order.end(), [&](idx_t left, idx_t right) {
				if (static_cast<int>(right) == null_code)
					return static_cast<int>(left) != null_code;
				if (static_cast<int>(left) == null_code)
					return false;
				return unique_values[left] < unique_values[right];
			});
		}
		mark("sort");
		vector<int> temp_to_sorted(unique_values.size());
		old_to_new.resize(old_count);
		code_to_value.clear();
		code_to_value.reserve(unique_values.size());
		mark("codes-allocate-reset");
		for (idx_t sorted = 0; sorted < order.size(); ++sorted) {
			const auto temporary = order[sorted];
			temp_to_sorted[temporary] = static_cast<int>(sorted);
			code_to_value.push_back(static_cast<int>(temporary) == null_code
			                            ? Value(type)
			                            : MakeIntegerValue(unique_values[temporary], type));
		}
		mark("assign-codes");
		for (auto &code : codes)
			code = temp_to_sorted[code];
		for (idx_t i = 0; i < old_count; ++i)
			old_to_new[i] = temp_to_sorted[old_temp_codes[i]];
		mark("remap-codes");

		std::unordered_map<int64_t, int>().swap(value_to_temp);
		vector<int>().swap(direct_to_temp);
		vector<int>().swap(old_temp_codes);
		vector<int>().swap(temp_to_sorted);
		vector<idx_t>().swap(order);
		vector<int64_t>().swap(unique_values);
		vector<int64_t>().swap(old_values);
		vector<uint8_t>().swap(old_valid);
		vector<int64_t>().swap(values);
		vector<uint8_t>().swap(valid);
		mark("temporary-deallocate");
		if (timing_side > 0) {
			std::printf("[%d-%s-encode-stats] path=%s input=%llu old_keys=%llu unique_keys=%llu range=%llu\n",
			            timing_side, domain, direct ? "integer-direct" : "integer-hash",
			            static_cast<unsigned long long>(input_count), static_cast<unsigned long long>(old_count),
			            static_cast<unsigned long long>(code_to_value.size()), static_cast<unsigned long long>(range));
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
	static Value MakeIntegerValue(int64_t value, const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			return Value::TINYINT(static_cast<int8_t>(value));
		case LogicalTypeId::SMALLINT:
			return Value::SMALLINT(static_cast<int16_t>(value));
		case LogicalTypeId::INTEGER:
			return Value::INTEGER(static_cast<int32_t>(value));
		case LogicalTypeId::BIGINT:
			return Value::BIGINT(value);
		case LogicalTypeId::UTINYINT:
			return Value::UTINYINT(static_cast<uint8_t>(value));
		case LogicalTypeId::USMALLINT:
			return Value::USMALLINT(static_cast<uint16_t>(value));
		case LogicalTypeId::UINTEGER:
			return Value::UINTEGER(static_cast<uint32_t>(value));
		default:
			throw InternalException("Unsupported typed domain encoder SQL type");
		}
	}

	vector<Value> code_to_value;
	vector<int> old_to_new;
};

} // namespace duckdb
