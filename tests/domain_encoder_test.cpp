#include "domain_encoder.hpp"
#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>

using namespace duckdb;

static vector<int> Encode(DomainEncoder &encoder, const vector<Value> &values) {
#ifdef SORT_ENCODER
	return encoder.EncodeBatch(values);
#else
	vector<int> codes;
	for (auto &value : values) codes.push_back(encoder.Encode(value));
	return codes;
#endif
}

static void Check(bool valid) {
	if (!valid) throw std::runtime_error("encoder regression");
}

int main() {
	DomainEncoder encoder;
	vector<Value> values;
	for (int i = 999; i >= 0; --i) values.push_back(Value::BIGINT(int64_t(i) * 10000000000LL - 500));
	values.push_back(values[0]);
	auto codes = Encode(encoder, values);
	Check(encoder.Size() == 1000 && codes.size() == 1001 && codes.front() == codes.back());
	for (idx_t i = 0; i < values.size(); ++i) Check(Value::NotDistinctFrom(values[i], encoder.Decode(codes[i])));
#ifdef SORT_ENCODER
	for (int i = 0; i < 1000; ++i) Check(codes[i] == 999 - i);
#endif
	vector<Value> extension = {Value::BIGINT(-1000), values[400], Value::BIGINT(10000000000000LL)};
	auto next = Encode(encoder, extension);
	Check(encoder.Size() == 1002);
#ifdef SORT_ENCODER
	for (idx_t i = 0; i < values.size(); ++i) codes[i] = encoder.Remap(codes[i]);
#endif
	for (idx_t i = 0; i < values.size(); ++i) Check(Value::NotDistinctFrom(values[i], encoder.Decode(codes[i])));
	for (idx_t i = 0; i < extension.size(); ++i) Check(Value::NotDistinctFrom(extension[i], encoder.Decode(next[i])));
	DomainEncoder strings;
	vector<Value> text = {Value("z"), Value("a"), Value("z"), Value(""), Value(LogicalType::VARCHAR)};
	auto text_codes = Encode(strings, text);
	Check(strings.Size() == 4 && text_codes[0] == text_codes[2]);
	for (idx_t i = 0; i < text.size(); ++i) Check(Value::NotDistinctFrom(text[i], strings.Decode(text_codes[i])));
	DomainEncoder floating;
	vector<Value> floats = {Value::DOUBLE(std::numeric_limits<double>::quiet_NaN()), Value::DOUBLE(-0.0), Value::DOUBLE(0.0), Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())};
	auto float_codes = Encode(floating, floats);
	Check(floating.Size() == 2 && float_codes[0] == float_codes[3] && float_codes[1] == float_codes[2]);
	DomainEncoder empty;
	Check(Encode(empty, {}).empty() && empty.Size() == 0);
	std::cout << "PASS: 1000 distinct keys, duplicates, shared-domain extension, decode, strings, NULL, NaN, signed zero, empty input\n";
}
