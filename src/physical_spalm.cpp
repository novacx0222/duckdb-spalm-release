#include "physical_spalm.hpp"
#include "domain_encoder.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <dlfcn.h>
#include <iostream>
#include <cmath>
#include <algorithm>
#include <time.h>
#include <omp.h>
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <sstream>
#include <dirent.h>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstdio>

namespace duckdb {

double get_ms(struct timespec start, struct timespec end) {
	double seconds, nanoseconds, elapsed_ms;
	seconds = end.tv_sec - start.tv_sec;
	nanoseconds = end.tv_nsec - start.tv_nsec;
	return (seconds * 1000.0) + (nanoseconds / 1000000.0);
}

static timespec timing_now() {
  timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t;
}
static void print_stage(const char *name, timespec begin, timespec end) {
  printf("[%s] %.3f ms\n", name, get_ms(begin, end));
}
static void print_side(int side, const char *name, timespec begin, timespec end) {
  printf("[%d-%s] %.3f ms\n", side, name, get_ms(begin, end));
}

template<typename T>
void sort_row_indices(int start, int end, int* cols, T* vals) {
    int n = end - start;
    if (n <= 1) return;

    std::vector<int> p(n);
    for (int i = 0; i < n; i++) p[i] = i;

    std::sort(p.begin(), p.end(), [&](int a, int b) {
        return cols[start + a] < cols[start + b];
    });

    std::vector<int> tmp_cols(n);
    std::vector<T> tmp_vals(n);
    for (int i = 0; i < n; i++) {
        tmp_cols[i] = cols[start + p[i]];
        tmp_vals[i] = vals[start + p[i]];
    }
    for (int i = 0; i < n; i++) {
        cols[start + i] = tmp_cols[i];
        vals[start + i] = tmp_vals[i];
    }
}

template<typename T>
struct EncodedMatrix {
	vector<int> row_codes;
	vector<int> join_codes;
	vector<T> values;

	idx_t Count() const {
		return row_codes.size();
	}
};


// Integer columns retain primitive values; other SQL types retain Value.
// Dispatch is chosen once from column metadata, before scanning records.
struct DomainInputBuffer {
	explicit DomainInputBuffer(const LogicalType &type, idx_t count) : type(type), reader(nullptr) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT: reader = Read<int8_t>; break;
		case LogicalTypeId::SMALLINT: reader = Read<int16_t>; break;
		case LogicalTypeId::INTEGER: reader = Read<int32_t>; break;
		case LogicalTypeId::BIGINT: reader = Read<int64_t>; break;
		case LogicalTypeId::UTINYINT: reader = Read<uint8_t>; break;
		case LogicalTypeId::USMALLINT: reader = Read<uint16_t>; break;
		case LogicalTypeId::UINTEGER: reader = Read<uint32_t>; break;
		default: break;
		}
		if (reader) {
			integers.reserve(count);
			valid.reserve(count);
		} else {
			values.reserve(count);
		}
	}

	template <class T>
	static int64_t Read(const UnifiedVectorFormat &format, idx_t index) {
		return static_cast<int64_t>(UnifiedVectorFormat::GetData<T>(format)[index]);
	}

	void Append(Vector &column, const UnifiedVectorFormat &format, idx_t logical_index) {
		if (reader) {
			const auto index = format.sel->get_index(logical_index);
			const bool is_valid = format.validity.RowIsValid(index);
			valid.push_back(is_valid ? 1 : 0);
			integers.push_back(is_valid ? reader(format, index) : 0);
		} else {
			values.push_back(column.GetValue(logical_index));
		}
	}

	vector<int> Encode(DomainEncoder &encoder, int side, const char *domain) {
		if (reader) {
			return encoder.EncodeIntegerBatch(std::move(integers), std::move(valid), type, side, domain);
		}
		return encoder.EncodeBatch(std::move(values), side, domain);
	}

	LogicalType type;
	int64_t (*reader)(const UnifiedVectorFormat &, idx_t);
	vector<int64_t> integers;
	vector<uint8_t> valid;
	vector<Value> values;
};



// Immutable membership snapshot for filtering either input against a common
// INNER JOIN domain. Bounded integers use a bitmap; other values use a set.
struct JoinDomainFilter {
	JoinDomainFilter(const DomainEncoder &encoder, const LogicalType &type, bool requested) {
		if (!requested) return;
		enabled = true;
		integer_keys = DomainEncoder::SupportsIntegerEncoding(type);
		for (idx_t code = 0; code < encoder.Size(); ++code) {
			if (encoder.Decode(code).type() != type) integer_keys = false;
		}
		if (!integer_keys) {
			generic_keys.reserve(encoder.Size());
			for (idx_t code = 0; code < encoder.Size(); ++code) {
				if (!encoder.Decode(code).IsNull()) generic_keys.insert(encoder.Decode(code));
			}
			key_count = generic_keys.size();
			return;
		}
		vector<int64_t> keys;
		keys.reserve(encoder.Size());
		for (idx_t code = 0; code < encoder.Size(); ++code) {
			if (!encoder.Decode(code).IsNull()) keys.push_back(encoder.Decode(code).GetValue<int64_t>());
		}
		key_count = keys.size();
		if (keys.empty()) return;
		const auto bounds = std::minmax_element(keys.begin(), keys.end());
		minimum = *bounds.first;
		const uint64_t distance = static_cast<uint64_t>(*bounds.second) - static_cast<uint64_t>(minimum);
		constexpr uint64_t MAX_DIRECT_RANGE = 1ULL << 20;
		if (distance < MAX_DIRECT_RANGE) {
			direct = true;
			range = static_cast<idx_t>(distance + 1);
			full_range = key_count == range;
			if (!full_range) {
				present.assign(range, 0);
				for (auto key : keys) {
					present[static_cast<idx_t>(static_cast<uint64_t>(key) - static_cast<uint64_t>(minimum))] = 1;
				}
			}
		} else {
			hash_keys.reserve(key_count);
			for (auto key : keys) hash_keys.insert(key);
		}
	}
	bool Contains(int64_t key) const {
		if (!enabled) return true;
		if (key_count == 0) return false;
		if (direct) {
			const uint64_t offset = static_cast<uint64_t>(key) - static_cast<uint64_t>(minimum);
			return offset < range && (full_range || present[static_cast<idx_t>(offset)] != 0);
		}
		return hash_keys.find(key) != hash_keys.end();
	}
	bool ContainsValue(const Value &key) const {
		return !enabled || generic_keys.find(key) != generic_keys.end();
	}
	const char *Path() const {
		if (!enabled) return "disabled";
		if (key_count == 0) return "empty";
		if (!integer_keys) return "value-hash";
		if (!direct) return "integer-hash";
		return full_range ? "integer-range" : "integer-bitmap";
	}
	bool enabled = false, integer_keys = false, direct = false, full_range = false;
	idx_t key_count = 0, range = 0;
	int64_t minimum = 0;
	vector<uint8_t> present;
	std::unordered_set<int64_t> hash_keys;
	value_set_t generic_keys;
};

// Scan primitive vectors and retain only distinct keys, not one Value per
// record. Apply the same NULL key/value eligibility as matrix extraction.
static DomainEncoder CollectJoinDomain(ColumnDataCollection &data, idx_t join_col, idx_t value_col, int side) {
	const auto begin = timing_now();
	DataChunk chunk;
	data.InitializeScanChunk(chunk);
	DomainInputBuffer input(chunk.data[join_col].GetType(), 0);
	std::unordered_set<int64_t> integer_keys;
	value_set_t generic_keys;
	for (idx_t chunk_index = 0; chunk_index < data.ChunkCount(); ++chunk_index) {
		data.FetchChunk(chunk_index, chunk);
		UnifiedVectorFormat keys, values;
		chunk.data[join_col].ToUnifiedFormat(chunk.size(), keys);
		chunk.data[value_col].ToUnifiedFormat(chunk.size(), values);
		for (idx_t i = 0; i < chunk.size(); ++i) {
			const auto key_index = keys.sel->get_index(i);
			if (!keys.validity.RowIsValid(key_index) || !values.validity.RowIsValid(values.sel->get_index(i))) continue;
			if (input.reader) integer_keys.insert(input.reader(keys, key_index));
			else generic_keys.insert(chunk.data[join_col].GetValue(i));
		}
	}
	if (input.reader) {
		input.integers.reserve(integer_keys.size());
		for (auto key : integer_keys) input.integers.push_back(key);
		input.valid.assign(input.integers.size(), 1);
	} else {
		input.values.reserve(generic_keys.size());
		for (const auto &key : generic_keys) input.values.push_back(key);
	}
	print_side(side, "join-domain-collect", begin, timing_now());
	DomainEncoder result;
	const auto encode_begin = timing_now();
	input.Encode(result, side, "join-domain");
	print_side(side, "join-domain-encode", encode_begin, timing_now());
	return result;
}

static DomainEncoder IntersectJoinDomains(const DomainEncoder &first, const DomainEncoder &second) {
	const auto begin = timing_now();
	const auto &small = first.Size() <= second.Size() ? first : second;
	const auto &large = first.Size() <= second.Size() ? second : first;
	value_set_t keys;
	keys.reserve(small.Size());
	for (idx_t i = 0; i < small.Size(); ++i) keys.insert(small.Decode(i));
	vector<Value> common;
	common.reserve(small.Size());
	for (idx_t i = 0; i < large.Size(); ++i) {
		if (keys.find(large.Decode(i)) != keys.end()) common.push_back(large.Decode(i));
	}
	DomainEncoder result;
	result.EncodeBatch(std::move(common));
	print_stage("join-domain-intersect", begin, timing_now());
	printf("[join-intersection-stats] keys1=%llu keys2=%llu common_keys=%llu\n",
	       static_cast<unsigned long long>(first.Size()), static_cast<unsigned long long>(second.Size()),
	       static_cast<unsigned long long>(result.Size()));
	return result;
}

template<typename T>
EncodedMatrix<T> EncodeMatrix(ColumnDataCollection &data, idx_t join_col_idx,
                              idx_t agg_col_idx, idx_t val_col_idx,
                              DomainEncoder &row_encoder, DomainEncoder &join_encoder, int timing_side,
                              bool restrict_join_domain = false) {
	const auto encoding_begin = timing_now();
	EncodedMatrix<T> result;
	const auto input_count = data.Count();
	DataChunk chunk;
	data.InitializeScanChunk(chunk);
	const auto filter_begin = timing_now();
	JoinDomainFilter join_filter(join_encoder, chunk.data[join_col_idx].GetType(), restrict_join_domain);
	if (restrict_join_domain) print_side(timing_side, "join-filter-setup", filter_begin, timing_now());
	const auto allocate_begin = timing_now();
	// Avoid reserving the complete second input when most keys may be rejected.
	const auto reserve_count = join_filter.enabled && !join_filter.full_range
	                              ? std::min(input_count, static_cast<idx_t>(65536)) : input_count;
	DomainInputBuffer row_values(chunk.data[agg_col_idx].GetType(), reserve_count);
	DomainInputBuffer join_values(chunk.data[join_col_idx].GetType(), reserve_count);
	result.values.reserve(reserve_count);
	idx_t join_candidates = 0, rejected_join_rows = 0;

	print_side(timing_side, "array-allocate", allocate_begin, timing_now());
	const auto extract_begin = timing_now();
	for (idx_t chunk_idx = 0; chunk_idx < data.ChunkCount(); ++chunk_idx) {
		data.FetchChunk(chunk_idx, chunk);
		UnifiedVectorFormat row_format, join_format;
		chunk.data[agg_col_idx].ToUnifiedFormat(chunk.size(), row_format);
		chunk.data[join_col_idx].ToUnifiedFormat(chunk.size(), join_format);
		for (idx_t row_idx = 0; row_idx < chunk.size(); ++row_idx) {
			const auto join_index = join_format.sel->get_index(row_idx);
			if (!join_format.validity.RowIsValid(join_index)) continue;
			++join_candidates;
			if (join_filter.enabled && !(join_filter.integer_keys
			    ? join_filter.Contains(join_values.reader(join_format, join_index))
			    : join_filter.ContainsValue(chunk.data[join_col_idx].GetValue(row_idx)))) {
				++rejected_join_rows;
				continue;
			}
			auto matrix_value = chunk.data[val_col_idx].GetValue(row_idx);
			// Equality joins reject NULL keys; SUM ignores NULL products.
			// NULL grouping keys remain valid domain members.
			if (matrix_value.IsNull()) continue;
			row_values.Append(chunk.data[agg_col_idx], row_format, row_idx);
			join_values.Append(chunk.data[join_col_idx], join_format, row_idx);
			result.values.push_back(matrix_value.GetValue<T>());
		}
	}
	print_side(timing_side, "array-extract", extract_begin, timing_now());
	if (restrict_join_domain && timing_side > 0) {
		printf("[%d-join-filter-stats] path=%s domain_keys=%llu input_rows=%llu join_candidates=%llu "
		       "rejected_join_rows=%llu encoded_rows=%llu\n", timing_side, join_filter.Path(),
		       static_cast<unsigned long long>(join_filter.key_count), static_cast<unsigned long long>(input_count),
		       static_cast<unsigned long long>(join_candidates), static_cast<unsigned long long>(rejected_join_rows),
		       static_cast<unsigned long long>(result.values.size()));
	}
	const auto row_begin = timing_now();
	result.row_codes = row_values.Encode(row_encoder, timing_side, "row");
	print_side(timing_side, "row-encode", row_begin, timing_now());
	const auto join_begin = timing_now();
	result.join_codes = join_values.Encode(join_encoder, timing_side, "join");
	print_side(timing_side, "join-encode", join_begin, timing_now());
	print_side(timing_side, "encoding-total", encoding_begin, timing_now());
	return result;
}

template<typename T>
void BuildCSR(int *row_ptr, int *col_idx, T *vals, const EncodedMatrix<T> &matrix,
              int num_rows, int num_threads, int timing_side) {

	struct timespec f_start, f_end;

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	std::vector<std::vector<int>> local_hist(num_threads, std::vector<int>(num_rows + 1, 0));
	print_side(timing_side, "csr-histogram-allocate", f_start, timing_now());
	const auto count_begin = timing_now();
	const auto entry_count = matrix.Count();

	#pragma omp parallel
	{
		int tid = omp_get_thread_num();
		#pragma omp for
		for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
			const int agg_idx = matrix.row_codes[entry_idx];
			local_hist[tid][agg_idx + 1]++;
		}
	}

	print_side(timing_side, "csr-row-count", count_begin, timing_now());
	const auto prefix_begin = timing_now();
	for (int t = 0; t < num_threads; t++) {
		for (int r = 0; r <= num_rows; r++) {
			row_ptr[r] += local_hist[t][r];
		}
	}

	for (int i = 0; i < num_rows; i++) {
		row_ptr[i+1] += row_ptr[i];
	}

	std::vector<std::vector<int>> thread_offsets(num_threads, std::vector<int>(num_rows, 0));
	for (int r = 0; r < num_rows; r++) {
		int current_offset = 0;
		for (int t = 0; t < num_threads; t++) {
			int count = local_hist[t][r+1];
			thread_offsets[t][r] = current_offset;
			current_offset += count;
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &f_end);
	print_side(timing_side, "csr-prefix-offset", prefix_begin, f_end);
	print_side(timing_side, "csr-count-total", f_start, f_end);

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	#pragma omp parallel
	{
		int tid = omp_get_thread_num();
		#pragma omp for
		for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
			const int agg_idx = matrix.row_codes[entry_idx];
			const int join_idx = matrix.join_codes[entry_idx];

			int pos = row_ptr[agg_idx] + thread_offsets[tid][agg_idx]++;
			col_idx[pos] = join_idx;
			vals[pos] = matrix.values[entry_idx];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &f_end);
	print_side(timing_side, "csr-write", f_start, f_end);

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	#pragma omp parallel for schedule(dynamic)
	for (int r = 0; r < num_rows; r++) {
		sort_row_indices<T>(row_ptr[r], row_ptr[r+1], col_idx, vals);
	}
	clock_gettime(CLOCK_MONOTONIC, &f_end);
	print_side(timing_side, "csr-sort", f_start, f_end);
}

// Parse spalm.conf into a key->value map.
// Looks for the file at $SPALM_CONF env var first, then ./spalm.conf.
static std::unordered_map<std::string, std::string> ReadSpalmConf() {
	std::unordered_map<std::string, std::string> config;

	const char *env_path = getenv("SPALM_CONF");
	std::string path = env_path ? env_path : "spalm.conf";

	std::ifstream f(path);
	if (!f.is_open()) {
		std::cerr << "spalm: could not open config file: " << path << "\n";
		return config;
	}

	std::string line;
	while (std::getline(f, line)) {
		if (line.empty() || line[0] == '#') continue;
		auto eq = line.find('=');
		if (eq == std::string::npos) continue;
		std::string key = line.substr(0, eq);
		std::string val = line.substr(eq + 1);
		// Trim whitespace
		key.erase(0, key.find_first_not_of(" \t"));
		key.erase(key.find_last_not_of(" \t") + 1);
		val.erase(0, val.find_first_not_of(" \t"));
		val.erase(val.find_last_not_of(" \t") + 1);
		if (!key.empty()) config[key] = val;
	}
	return config;
}

// Find the first *.so file in a directory.
static std::string FindSoInDir(const std::string &dir) {
	DIR *d = opendir(dir.c_str());
	if (!d) {
		std::cerr << "spalm: cannot open so_dir: " << dir << "\n";
		return "";
	}
	std::string result;
	struct dirent *ent;
	while ((ent = readdir(d)) != nullptr) {
		std::string name = ent->d_name;
		if (name.size() > 3 && name.substr(name.size() - 3) == ".so") {
			result = dir + "/" + name;
			break;
		}
	}
	closedir(d);
	return result;
}

PhysicalSpalm::PhysicalSpalm(vector<LogicalType> types, PhysicalOperator &left, PhysicalOperator &right,
                               SpalmColumnBindings col_bindings,
                               bool use_float_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      rhs_join_idx(col_bindings.rhs_join_idx), rhs_agg_idx(col_bindings.rhs_agg_idx),
      rhs_val_idx(col_bindings.rhs_val_idx), lhs_join_idx(col_bindings.lhs_join_idx),
      lhs_agg_idx(col_bindings.lhs_agg_idx), lhs_val_idx(col_bindings.lhs_val_idx),
      use_float(use_float_p), rhs_output_idx(col_bindings.rhs_output_idx),
      lhs_output_idx(col_bindings.lhs_output_idx),
      rhs_child_types(right.types), lhs_child_types(left.types) {
	children.push_back(left);
	children.push_back(right);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PhysicalSpalmGlobalState : public GlobalSinkState {
public:
	explicit PhysicalSpalmGlobalState(ClientContext &context, const PhysicalSpalm &op)
		: use_float(op.GetUseFloat()),
		  lhs_child_types(op.GetLhsChildTypes()),
		  rhs_join_idx(op.GetRhsJoinIdx()), rhs_agg_idx(op.GetRhsAggIdx()), rhs_val_idx(op.GetRhsValIdx()),
		  lhs_join_idx(op.GetLhsJoinIdx()), lhs_agg_idx(op.GetLhsAggIdx()), lhs_val_idx(op.GetLhsValIdx()),
		  context_ref(context),
		  mat_data(make_uniq<ColumnDataCollection>(context, op.GetRhsChildTypes())) {
		mat_data->InitializeAppend(append_state);

		threads = (int)TaskScheduler::GetScheduler(context).NumberOfThreads();

		total_parallel_operators = 0;
		threads_done = 0;

		M = 0; K = 0; N = 0;

		clock_gettime(CLOCK_MONOTONIC, &start);
		prepare_start = start;
	}

	~PhysicalSpalmGlobalState() {
		printf("~PhysicalSpalmGlobalState() called\n");
	}

	bool use_float;
	vector<LogicalType> lhs_child_types;
	idx_t rhs_join_idx, rhs_agg_idx, rhs_val_idx;
	idx_t lhs_join_idx, lhs_agg_idx, lhs_val_idx;
	ClientContext &context_ref;

	const int slice_size = 512;
	const int tile_size_agg_dim = 128;

	int M, K, N;
	DomainEncoder rhs_agg_encoder;
	DomainEncoder lhs_agg_encoder;
	DomainEncoder join_encoder;

	unique_ptr<int[]> Ri;
	unique_ptr<int[]> Rk;
	unique_ptr<float[]> Rv_f;
	unique_ptr<double[]> Rv_d;

	unique_ptr<int[]> Sj;
	unique_ptr<int[]> Sk;
	unique_ptr<float[]> Sv_f;
	unique_ptr<double[]> Sv_d;

	unique_ptr<float[]> result_f;
	unique_ptr<double[]> result_d;
	bool done = false;

	int threads;

	int total_parallel_operators;

	struct timespec start, end;
	struct timespec prepare_start;

	unique_ptr<ColumnDataCollection> mat_data;
	unique_ptr<ColumnDataCollection> first_input;
	double first_collect_ms = 0.0;
	ColumnDataAppendState append_state;
	double combine_ms = 0.0;
	double operator_combine_ms = 0.0;
	mutex lock;
	std::condition_variable condition;
	int threads_done = 0;
};

class PhysicalSpalmLocalState : public LocalSinkState {
public:
  explicit PhysicalSpalmLocalState(ExecutionContext &context, const PhysicalSpalm &op)
		: l_mat_data(context.client, op.GetRhsChildTypes()){
		l_mat_data.InitializeAppend(append_state);
  }

  ~PhysicalSpalmLocalState() {
  }

	ColumnDataCollection l_mat_data;
	ColumnDataAppendState append_state;
};

unique_ptr<LocalSinkState> PhysicalSpalm::GetLocalSinkState(ExecutionContext &context) const {
  return make_uniq<PhysicalSpalmLocalState>(context, *this);
}

unique_ptr<GlobalSinkState> PhysicalSpalm::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PhysicalSpalmGlobalState>(context, *this);
}

SinkResultType PhysicalSpalm::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalSpalmLocalState>();
	lstate.l_mat_data.Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalSpalm::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	auto &lstate = input.local_state.Cast<PhysicalSpalmLocalState>();
	lock_guard<mutex> client_guard(sink.lock);
	const auto combine_begin = timing_now();
	sink.mat_data->Combine(lstate.l_mat_data);
	sink.combine_ms += get_ms(combine_begin, timing_now());
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalSpalm::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	sink.first_collect_ms = get_ms(sink.start, timing_now());
	printf("[1-collect-input] %.3f ms\n", sink.first_collect_ms);
	printf("[1-combine-input] %.3f ms\n", sink.combine_ms);
	// Keep the first input until the second is collected. CSR construction
	// is deferred so both inputs can use the same intersection dictionary.
	sink.first_input = std::move(sink.mat_data);
	sink.mat_data = make_uniq<ColumnDataCollection>(sink.context_ref, sink.lhs_child_types);
	sink.mat_data->InitializeAppend(sink.append_state);
	clock_gettime(CLOCK_MONOTONIC, &sink.start);
	return SinkFinalizeType::READY;
}

class PhysicalSpalmOperatorState : public OperatorState {
public:
    explicit PhysicalSpalmOperatorState(ClientContext &context, const vector<LogicalType> &lhs_types)
        : r_mat_data(context, lhs_types) {
        r_mat_data.InitializeAppend(append_state);
    }

    ColumnDataCollection r_mat_data;
    ColumnDataAppendState append_state;
};

unique_ptr<OperatorState> PhysicalSpalm::GetOperatorState(ExecutionContext &context) const {
	auto &sink = sink_state->Cast<PhysicalSpalmGlobalState>();
	lock_guard<mutex> client_guard(sink.lock);
	sink.total_parallel_operators++;
	return make_uniq<PhysicalSpalmOperatorState>(context.client, lhs_child_types);
}

class PhysicalSpalmGlobalSourceState : public GlobalSourceState {
public:
	explicit PhysicalSpalmGlobalSourceState(const PhysicalSpalm &op)
	    : next_row(0), num_threads(1) {
		if (op.sink_state) {
			auto &sink = op.sink_state->Cast<PhysicalSpalmGlobalState>();
			num_threads = static_cast<idx_t>(sink.threads);
		}
	}

	idx_t MaxThreads() override { return num_threads; }
	std::atomic<idx_t> next_row;
	std::atomic<idx_t> threads_finished{0};
	idx_t num_threads;
};

class PhysicalSpalmLocalSourceState : public LocalSourceState {
public:
	PhysicalSpalmLocalSourceState() : curr_pos(0), end_pos(0) {}
	idx_t curr_pos;
	idx_t end_pos;
};

OperatorResultType PhysicalSpalm::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &op_state = state_p.Cast<PhysicalSpalmOperatorState>();
	op_state.r_mat_data.Append(op_state.append_state, input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType PhysicalSpalm::FinalExecute(ExecutionContext &context, DataChunk &output,
                                                        GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &sink = sink_state->Cast<PhysicalSpalmGlobalState>();
	auto &op_state = state_p.Cast<PhysicalSpalmOperatorState>();

	if (!sink.done) {
		unique_lock<mutex> l(sink.lock);
		const auto combine_begin = timing_now();
		sink.mat_data->Combine(op_state.r_mat_data);
		sink.operator_combine_ms += get_ms(combine_begin, timing_now());
		sink.threads_done++;

		// If we aren't the last thread, we wait.
		if (sink.threads_done < sink.total_parallel_operators) {
			sink.condition.wait(l, [&]{
				return sink.threads_done >= sink.total_parallel_operators;
			});
		} else {
			struct timespec f_start;
			const int num_threads = sink.threads;
			omp_set_num_threads(num_threads);

			const auto second_collect_ms = get_ms(sink.start, timing_now());
			printf("[2-collect-input] %.3f ms\n", second_collect_ms);
			printf("[2-combine-input] %.3f ms\n", sink.operator_combine_ms);
			const auto intersection_begin = timing_now();
			{
				auto first_domain = CollectJoinDomain(*sink.first_input, rhs_join_idx, rhs_val_idx, 1);
				auto second_domain = CollectJoinDomain(*sink.mat_data, lhs_join_idx, lhs_val_idx, 2);
				sink.join_encoder = IntersectJoinDomains(first_domain, second_domain);
			}
			print_stage("join-intersection-total", intersection_begin, timing_now());
			const auto first_prepare_begin = timing_now();
			{
				auto encoded_float = EncodedMatrix<float>();
				auto encoded_double = EncodedMatrix<double>();
				if (sink.use_float) {
					encoded_float = EncodeMatrix<float>(*sink.first_input, rhs_join_idx, rhs_agg_idx, rhs_val_idx,
					                                    sink.rhs_agg_encoder, sink.join_encoder, 1, true);
				} else {
					encoded_double = EncodeMatrix<double>(*sink.first_input, rhs_join_idx, rhs_agg_idx, rhs_val_idx,
					                                      sink.rhs_agg_encoder, sink.join_encoder, 1, true);
				}

				const int sink_nnz = sink.use_float ? static_cast<int>(encoded_float.Count())
				                                    : static_cast<int>(encoded_double.Count());
				const int M = static_cast<int>(sink.rhs_agg_encoder.Size());
				sink.M = M;
				printf("[SPALM Finalize] encoded M=%d, K_so_far=%llu, nnz=%d\n", M,
				       static_cast<unsigned long long>(sink.join_encoder.Size()), sink_nnz);

				const auto csr_allocate_begin = timing_now();
				// Allocate sink CSR arrays
				sink.Ri = make_uniq_array<int>(M + 1);
				memset(sink.Ri.get(), 0, sizeof(int) * (M + 1));
				sink.Rk = make_uniq_array<int>(sink_nnz);
				if (sink.use_float) {
					sink.Rv_f = make_uniq_array<float>(sink_nnz);
				} else {
					sink.Rv_d = make_uniq_array<double>(sink_nnz);
				}

				print_stage("1-csr-allocate", csr_allocate_begin, timing_now());
				clock_gettime(CLOCK_MONOTONIC, &f_start);
				if (sink.use_float) {
					BuildCSR<float>(sink.Ri.get(), sink.Rk.get(), sink.Rv_f.get(), encoded_float, M, num_threads, 1);
				} else {
					BuildCSR<double>(sink.Ri.get(), sink.Rk.get(), sink.Rv_d.get(), encoded_double, M, num_threads, 1);
				}

				const auto release_begin = timing_now();
				sink.first_input.reset();
				print_side(1, "input-deallocate", release_begin, timing_now());
			}
			printf("[1-prepare-total] %.3f ms\n", sink.first_collect_ms + get_ms(first_prepare_begin, timing_now()));
			const auto second_prepare_begin = timing_now();
			auto encoded_float = EncodedMatrix<float>();
			auto encoded_double = EncodedMatrix<double>();
			if (sink.use_float) {
				encoded_float = EncodeMatrix<float>(*sink.mat_data, lhs_join_idx, lhs_agg_idx, lhs_val_idx,
				                                    sink.lhs_agg_encoder, sink.join_encoder, 2, true);
			} else {
				encoded_double = EncodeMatrix<double>(*sink.mat_data, lhs_join_idx, lhs_agg_idx, lhs_val_idx,
				                                      sink.lhs_agg_encoder, sink.join_encoder, 2, true);
			}

			const int op_nnz = sink.use_float ? static_cast<int>(encoded_float.Count())
			                                  : static_cast<int>(encoded_double.Count());
			const auto remap_begin = timing_now();
			#pragma omp parallel for
			for (idx_t entry = 0; entry < static_cast<idx_t>(sink.Ri[sink.M]); ++entry) {
				sink.Rk[entry] = sink.join_encoder.Remap(sink.Rk[entry]);
			}
			print_stage("2-remap-first-join", remap_begin, timing_now());
			const int N = static_cast<int>(sink.lhs_agg_encoder.Size());
			const int K = static_cast<int>(sink.join_encoder.Size());
			sink.N = N;
			sink.K = K;
			printf("[SPALM FinalExecute] encoded N=%d, K=%d, op_nnz=%d\n", N, K, op_nnz);

			// An empty input produces no non-zero relation rows. Avoid calling
			// the SPALM library with zero-sized dimensions; not every backend
			// accepts them even though the SQL result is well-defined.
			if (sink.M == 0 || N == 0 || K == 0) {
				const auto release_begin = timing_now();
				sink.mat_data.reset();
				print_side(2, "input-deallocate", release_begin, timing_now());
				printf("[2-prepare-total] %.3f ms\n", second_collect_ms + get_ms(second_prepare_begin, timing_now()));
				print_stage("prepare-total", sink.prepare_start, timing_now());
				sink.done = true;
				clock_gettime(CLOCK_MONOTONIC, &sink.start);
				sink.condition.notify_all();
				output.SetCardinality(0);
				return OperatorFinalizeResultType::FINISHED;
			}

			const auto csr_allocate_begin = timing_now();
			// Allocate operator CSR arrays
			sink.Sj = make_uniq_array<int>(N + 1);
			memset(sink.Sj.get(), 0, sizeof(int) * (N + 1));
			sink.Sk = make_uniq_array<int>(op_nnz);
			if (sink.use_float) {
				sink.Sv_f = make_uniq_array<float>(op_nnz);
			} else {
				sink.Sv_d = make_uniq_array<double>(op_nnz);
			}

			print_stage("2-csr-allocate", csr_allocate_begin, timing_now());
			// Allocate result array (M x N)
			{
				struct timespec alloc_start, alloc_end;
				clock_gettime(CLOCK_MONOTONIC, &alloc_start);
				if (sink.use_float) {
					sink.result_f = make_uniq_array<float>((idx_t)sink.M * N);
					const auto zero_begin = timing_now();
					print_stage("result-allocate", alloc_start, zero_begin);
					memset(sink.result_f.get(), 0, sizeof(float) * (idx_t)sink.M * N);
					print_stage("result-zero", zero_begin, timing_now());
				} else {
					sink.result_d = make_uniq_array<double>((idx_t)sink.M * N);
					const auto zero_begin = timing_now();
					print_stage("result-allocate", alloc_start, zero_begin);
					memset(sink.result_d.get(), 0, sizeof(double) * (idx_t)sink.M * N);
					print_stage("result-zero", zero_begin, timing_now());
				}
				clock_gettime(CLOCK_MONOTONIC, &alloc_end);
				print_stage("result-allocate-zero-total", alloc_start, alloc_end);
			}

			clock_gettime(CLOCK_MONOTONIC, &f_start);
			if (sink.use_float) {
				BuildCSR<float>(sink.Sj.get(), sink.Sk.get(), sink.Sv_f.get(), encoded_float, N, num_threads, 2);
			} else {
				BuildCSR<double>(sink.Sj.get(), sink.Sk.get(), sink.Sv_d.get(), encoded_double, N, num_threads, 2);
			}

			const auto release_begin = timing_now();
			sink.mat_data.reset();
			print_side(2, "input-deallocate", release_begin, timing_now());
			clock_gettime(CLOCK_MONOTONIC, &sink.end);
			printf("[2-prepare-total] %.3f ms\n", second_collect_ms + get_ms(second_prepare_begin, sink.end));

			const auto library_setup_begin = timing_now();
			// Load spalm library from path configured in spalm.conf.
			// Keep the handle cached for the process lifetime to avoid
			// unloading MKL (which corrupts its internal thread pool state).
			using spalm_fn_t = void (*)(int, int *, int *, void *,
			                            int *, int *, void *,
			                            const int, const int, const int,
			                            bool,
			                            void *, int *, int *, void *,
			                            int, const char **, const char **);
			static void *cached_handle = nullptr;
			static spalm_fn_t spalm_fn = nullptr;

			if (!cached_handle) {
				auto conf_load = ReadSpalmConf();
				std::string so_path;
				if (conf_load.count("so_dir")) {
					so_path = FindSoInDir(conf_load["so_dir"]);
				}
				if (so_path.empty()) {
					std::cerr << "spalm: no .so file found. Set so_dir in spalm.conf (or $SPALM_CONF).\n";
					return OperatorFinalizeResultType::FINISHED;
				}
				cached_handle = dlopen(so_path.c_str(), RTLD_LAZY);
				if (!cached_handle) {
					std::cerr << "spalm: cannot open library " << so_path << ": " << dlerror() << '\n';
					return OperatorFinalizeResultType::FINISHED;
				}
				spalm_fn = (spalm_fn_t)dlsym(cached_handle, "spalm");
				if (!spalm_fn) {
					std::cerr << "spalm: symbol 'spalm' not found in " << so_path << ": " << dlerror() << '\n';
					dlclose(cached_handle);
					cached_handle = nullptr;
					return OperatorFinalizeResultType::FINISHED;
				}
			}

			auto conf = ReadSpalmConf();

			std::vector<std::string> kv_keys, kv_vals;
			std::string threads_str = std::to_string(sink.threads);
			kv_keys.push_back("threads"); kv_vals.push_back(threads_str);
			for (auto &kv : conf) {
				if (kv.first == "so_dir") continue;
				kv_keys.push_back(kv.first);
				kv_vals.push_back(kv.second);
			}
			std::vector<const char *> c_keys, c_vals;
			for (size_t i = 0; i < kv_keys.size(); i++) {
				c_keys.push_back(kv_keys[i].c_str());
				c_vals.push_back(kv_vals[i].c_str());
			}

			int spalm_type = sink.use_float ? 0 : 1;
			void *rv_ptr = sink.use_float ? (void *)sink.Rv_f.get() : (void *)sink.Rv_d.get();
			void *sv_ptr = sink.use_float ? (void *)sink.Sv_f.get() : (void *)sink.Sv_d.get();
			void *result_ptr = sink.use_float ? (void *)sink.result_f.get() : (void *)sink.result_d.get();

			const auto library_call_begin = timing_now();
			print_stage("library-config-setup", library_setup_begin, library_call_begin);
			print_stage("prepare-total", sink.prepare_start, library_call_begin);
			spalm_fn(spalm_type, sink.Ri.get(), sink.Rk.get(), rv_ptr,
			         sink.Sj.get(), sink.Sk.get(), sv_ptr,
			         sink.M, sink.K, sink.N,
			         false,
			         result_ptr, nullptr, nullptr, nullptr,
			         (int)c_keys.size(), c_keys.data(), c_vals.data());

			print_stage("library-call-total", library_call_begin, timing_now());
			sink.done = true;
			clock_gettime(CLOCK_MONOTONIC, &sink.start);
			sink.condition.notify_all();
		}
	}

	output.SetCardinality(0);
	return OperatorFinalizeResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source Interface
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSourceState> PhysicalSpalm::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PhysicalSpalmGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalSpalm::GetLocalSourceState(ExecutionContext &context,
                                                                  GlobalSourceState &gstate) const {
	return make_uniq<PhysicalSpalmLocalSourceState>();
}

SourceResultType PhysicalSpalm::GetData(ExecutionContext &context, DataChunk &output,
                                          OperatorSourceInput &input) const {
	auto &sink   = sink_state->Cast<PhysicalSpalmGlobalState>();
	auto &gstate = input.global_state.Cast<PhysicalSpalmGlobalSourceState>();
	auto &lstate = input.local_state.Cast<PhysicalSpalmLocalSourceState>();

	const idx_t N         = (idx_t)sink.N;
	// Dimensions are finalized after both inputs have been collected.
	const idx_t total_rows = static_cast<idx_t>(sink.M);
	const idx_t chunk_rows = std::max(static_cast<idx_t>(1),
	    std::min(static_cast<idx_t>(200), (total_rows + gstate.num_threads - 1) / gstate.num_threads));
	const idx_t vec_size  = 2048;

	output.SetCapacity(vec_size);
	idx_t idx = 0;

	if (sink.use_float) {
		float *out_val = (float *)output.data[2].GetData();
		float *res = sink.result_f.get();

		while (idx < vec_size) {
			if (lstate.curr_pos >= lstate.end_pos) {
				idx_t row_start = gstate.next_row.fetch_add(chunk_rows);
				if (row_start >= total_rows) break;
				idx_t row_end    = std::min(row_start + chunk_rows, total_rows);
				lstate.curr_pos  = row_start * N;
				lstate.end_pos   = row_end   * N;
			}
			while (lstate.curr_pos < lstate.end_pos && idx < vec_size) {
				if (res[lstate.curr_pos] != 0) {
					const auto m_code = lstate.curr_pos / N;
					const auto n_code = lstate.curr_pos % N;
					output.data[rhs_output_idx].SetValue(idx, sink.rhs_agg_encoder.Decode(m_code));
					output.data[lhs_output_idx].SetValue(idx, sink.lhs_agg_encoder.Decode(n_code));
					out_val[idx] = res[lstate.curr_pos];
					idx++;
				}
				lstate.curr_pos++;
			}
		}
	} else {
		double *out_val = (double *)output.data[2].GetData();
		double *res = sink.result_d.get();

		while (idx < vec_size) {
			if (lstate.curr_pos >= lstate.end_pos) {
				idx_t row_start = gstate.next_row.fetch_add(chunk_rows);
				if (row_start >= total_rows) break;
				idx_t row_end    = std::min(row_start + chunk_rows, total_rows);
				lstate.curr_pos  = row_start * N;
				lstate.end_pos   = row_end   * N;
			}
			while (lstate.curr_pos < lstate.end_pos && idx < vec_size) {
				if (res[lstate.curr_pos] != 0) {
					const auto m_code = lstate.curr_pos / N;
					const auto n_code = lstate.curr_pos % N;
					output.data[rhs_output_idx].SetValue(idx, sink.rhs_agg_encoder.Decode(m_code));
					output.data[lhs_output_idx].SetValue(idx, sink.lhs_agg_encoder.Decode(n_code));
					out_val[idx] = res[lstate.curr_pos];
					idx++;
				}
				lstate.curr_pos++;
			}
		}
	}

	output.SetCardinality(idx);

	bool all_done = (gstate.next_row.load() >= total_rows) && (lstate.curr_pos >= lstate.end_pos);
	if (all_done) {
		if (++gstate.threads_finished == gstate.num_threads) {
			clock_gettime(CLOCK_MONOTONIC, &sink.end);
			print_stage("output-total", sink.start, sink.end);
		}
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalSpalm::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const_reference<PhysicalOperator>> PhysicalSpalm::GetSources() const {
	return {*this};
}
} // namespace duckdb
