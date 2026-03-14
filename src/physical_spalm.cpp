#include "physical_spalm.hpp"
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
#include <string>

namespace duckdb {

double get_ms(struct timespec start, struct timespec end) {
	double seconds, nanoseconds, elapsed_ms;
	seconds = end.tv_sec - start.tv_sec;
	nanoseconds = end.tv_nsec - start.tv_nsec;
	return (seconds * 1000.0) + (nanoseconds / 1000000.0);
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
void BuildCSR(int* row_ptr, int* col_idx, T* vals,
              ColumnDataCollection& data, int num_rows, int num_threads,
              idx_t join_col_idx, idx_t agg_col_idx, idx_t val_col_idx,
              std::vector<DataChunk>& t_chunks) {

	struct timespec f_start, f_end;

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	std::vector<std::vector<int>> local_hist(num_threads, std::vector<int>(num_rows + 1, 0));
	const int num_chunks = data.ChunkCount();

	#pragma omp parallel
	{
		int tid = omp_get_thread_num();
		#pragma omp for
		for (idx_t ci = 0; ci < (idx_t)num_chunks; ci++) {
			data.FetchChunk(ci, t_chunks[tid]);
			const int *agg_col_input_ptr = (int *)t_chunks[tid].data[agg_col_idx].GetData();
			const idx_t chunk_size = t_chunks[tid].size();
			for (idx_t i = 0; i < chunk_size; i++) {
				const int agg_idx = agg_col_input_ptr[i];
				local_hist[tid][agg_idx + 1]++;
			}
		}
	}

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
	printf("CSR offset counting time: %.3f ms\n", get_ms(f_start, f_end));

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	#pragma omp parallel
	{
		int tid = omp_get_thread_num();
		#pragma omp for
		for (idx_t ci = 0; ci < (idx_t)num_chunks; ci++) {
			data.FetchChunk(ci, t_chunks[tid]);
			const int *join_col_input_ptr = (int *)t_chunks[tid].data[join_col_idx].GetData();
			const int *agg_col_input_ptr = (int *)t_chunks[tid].data[agg_col_idx].GetData();
			const T *val_col_input_ptr = (T *)t_chunks[tid].data[val_col_idx].GetData();
			const idx_t chunk_size = t_chunks[tid].size();
			for (idx_t i = 0; i < chunk_size; i++) {
				const int agg_idx = agg_col_input_ptr[i];
				const int join_idx = join_col_input_ptr[i];
				const T val = val_col_input_ptr[i];

				int pos = row_ptr[agg_idx] + thread_offsets[tid][agg_idx]++;
				col_idx[pos] = join_idx;
				vals[pos] = val;
			}
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &f_end);
	printf("CSR writing time: %.3f ms\n", get_ms(f_start, f_end));

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	#pragma omp parallel for schedule(dynamic)
	for (int r = 0; r < num_rows; r++) {
		sort_row_indices<T>(row_ptr[r], row_ptr[r+1], col_idx, vals);
	}
	clock_gettime(CLOCK_MONOTONIC, &f_end);
	printf("CSR sorting time: %.3f ms\n", get_ms(f_start, f_end));
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
	int sink_max_join;  // max value of join column on sink side

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

	unique_ptr<ColumnDataCollection> mat_data;
	ColumnDataAppendState append_state;
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
	sink.mat_data->Combine(lstate.l_mat_data);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalSpalm::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	double seconds, nanoseconds, elapsed_ms;
	struct timespec f_start;
	clock_gettime(CLOCK_MONOTONIC, &sink.end);
	seconds = sink.end.tv_sec - sink.start.tv_sec;
	nanoseconds = sink.end.tv_nsec - sink.start.tv_nsec;
	elapsed_ms = (seconds * 1000.0) + (nanoseconds / 1000000.0);
	printf("Start to finalize time: %.3f ms\n", elapsed_ms);

	const int sink_nnz = (int)sink.mat_data->Count();
	printf("[SPALM Finalize] mat_data chunk count=%lu, total rows (nnz)=%d\n",
	       sink.mat_data->ChunkCount(), sink_nnz);

	// Scan the collected data to find max(agg_col) and max(join_col)
	const int num_threads = sink.threads;
	omp_set_num_threads(num_threads);

	int global_max_agg = 0;
	int global_max_join = 0;
	{
		const int num_chunks = (int)sink.mat_data->ChunkCount();
		std::vector<DataChunk> scan_chunks(num_threads);
		for (auto &chunk : scan_chunks) {
			sink.mat_data->InitializeScanChunk(chunk);
		}
		#pragma omp parallel
		{
			int tid = omp_get_thread_num();
			int local_max_agg = 0;
			int local_max_join = 0;
			#pragma omp for
			for (idx_t ci = 0; ci < (idx_t)num_chunks; ci++) {
				sink.mat_data->FetchChunk(ci, scan_chunks[tid]);
				const int *agg_ptr = (int *)scan_chunks[tid].data[sink.rhs_agg_idx].GetData();
				const int *join_ptr = (int *)scan_chunks[tid].data[sink.rhs_join_idx].GetData();
				const idx_t chunk_size = scan_chunks[tid].size();
				for (idx_t i = 0; i < chunk_size; i++) {
					if (agg_ptr[i] > local_max_agg) local_max_agg = agg_ptr[i];
					if (join_ptr[i] > local_max_join) local_max_join = join_ptr[i];
				}
			}
			#pragma omp critical
			{
				if (local_max_agg > global_max_agg) global_max_agg = local_max_agg;
				if (local_max_join > global_max_join) global_max_join = local_max_join;
			}
		}
	}

	const int M = global_max_agg + 1;
	sink.M = M;
	sink.sink_max_join = global_max_join;
	printf("[SPALM Finalize] derived M=%d, max_join=%d, nnz=%d\n", M, global_max_join, sink_nnz);

	// Allocate sink CSR arrays
	sink.Ri = make_uniq_array<int>(M + 1);
	memset(sink.Ri.get(), 0, sizeof(int) * (M + 1));
	sink.Rk = make_uniq_array<int>(sink_nnz);
	if (sink.use_float) {
		sink.Rv_f = make_uniq_array<float>(sink_nnz);
	} else {
		sink.Rv_d = make_uniq_array<double>(sink_nnz);
	}

	clock_gettime(CLOCK_MONOTONIC, &f_start);
	std::vector<DataChunk> t_chunks(num_threads);
	for (auto& chunk : t_chunks) {
		sink.mat_data->InitializeScanChunk(chunk);
	}

	if (sink.use_float) {
		BuildCSR<float>(sink.Ri.get(), sink.Rk.get(), sink.Rv_f.get(),
		                *sink.mat_data, M, num_threads,
		                rhs_join_idx, rhs_agg_idx, rhs_val_idx, t_chunks);
	} else {
		BuildCSR<double>(sink.Ri.get(), sink.Rk.get(), sink.Rv_d.get(),
		                 *sink.mat_data, M, num_threads,
		                 rhs_join_idx, rhs_agg_idx, rhs_val_idx, t_chunks);
	}

	clock_gettime(CLOCK_MONOTONIC, &sink.end);
	printf("1 CSR init time: %.3f ms\n", get_ms(sink.start, sink.end));

	// Reinitialize mat_data with LHS child types for the operator phase
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
	    : next_row(0), total_rows(0), num_threads(1), chunk_rows(200) {
		if (op.sink_state) {
			auto &sink = op.sink_state->Cast<PhysicalSpalmGlobalState>();
			total_rows  = (idx_t)sink.M;
			num_threads = (idx_t)sink.threads;
			chunk_rows  = std::max((idx_t)1,
			              std::min((idx_t)200, (total_rows + num_threads - 1) / num_threads));
		}
	}

	idx_t MaxThreads() override { return num_threads; }

	std::atomic<idx_t> next_row;
	std::atomic<idx_t> threads_finished{0};
	idx_t total_rows;
	idx_t num_threads;
	idx_t chunk_rows;
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
		sink.mat_data->Combine(op_state.r_mat_data);
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

			// Derive operator-side dimensions from collected data
			const int op_nnz = (int)sink.mat_data->Count();
			int op_max_agg = 0;
			int op_max_join = 0;
			{
				const int num_chunks = (int)sink.mat_data->ChunkCount();
				std::vector<DataChunk> scan_chunks(num_threads);
				for (auto &chunk : scan_chunks) {
					sink.mat_data->InitializeScanChunk(chunk);
				}
				#pragma omp parallel
				{
					int tid = omp_get_thread_num();
					int local_max_agg = 0;
					int local_max_join = 0;
					#pragma omp for
					for (idx_t ci = 0; ci < (idx_t)num_chunks; ci++) {
						sink.mat_data->FetchChunk(ci, scan_chunks[tid]);
						const int *agg_ptr = (int *)scan_chunks[tid].data[sink.lhs_agg_idx].GetData();
						const int *join_ptr = (int *)scan_chunks[tid].data[sink.lhs_join_idx].GetData();
						const idx_t chunk_size = scan_chunks[tid].size();
						for (idx_t i = 0; i < chunk_size; i++) {
							if (agg_ptr[i] > local_max_agg) local_max_agg = agg_ptr[i];
							if (join_ptr[i] > local_max_join) local_max_join = join_ptr[i];
						}
					}
					#pragma omp critical
					{
						if (local_max_agg > op_max_agg) op_max_agg = local_max_agg;
						if (local_max_join > op_max_join) op_max_join = local_max_join;
					}
				}
			}

			const int N = op_max_agg + 1;
			// K is the bigger of the two max join column values (+1)
			const int K = std::max(sink.sink_max_join, op_max_join) + 1;
			sink.N = N;
			sink.K = K;
			printf("[SPALM FinalExecute] derived N=%d, K=%d, op_nnz=%d\n", N, K, op_nnz);

			// Allocate operator CSR arrays
			sink.Sj = make_uniq_array<int>(N + 1);
			memset(sink.Sj.get(), 0, sizeof(int) * (N + 1));
			sink.Sk = make_uniq_array<int>(op_nnz);
			if (sink.use_float) {
				sink.Sv_f = make_uniq_array<float>(op_nnz);
			} else {
				sink.Sv_d = make_uniq_array<double>(op_nnz);
			}

			// Allocate result array (M x N)
			{
				struct timespec alloc_start, alloc_end;
				clock_gettime(CLOCK_MONOTONIC, &alloc_start);
				if (sink.use_float) {
					sink.result_f = make_uniq_array<float>((idx_t)sink.M * N);
					memset(sink.result_f.get(), 0, sizeof(float) * (idx_t)sink.M * N);
				} else {
					sink.result_d = make_uniq_array<double>((idx_t)sink.M * N);
					memset(sink.result_d.get(), 0, sizeof(double) * (idx_t)sink.M * N);
				}
				clock_gettime(CLOCK_MONOTONIC, &alloc_end);
				printf("Result memset + allocation time: %.3f ms\n", get_ms(alloc_start, alloc_end));
			}

			clock_gettime(CLOCK_MONOTONIC, &f_start);
			std::vector<DataChunk> t_chunks(num_threads);
			for (auto& chunk : t_chunks) {
				sink.mat_data->InitializeScanChunk(chunk);
			}

			if (sink.use_float) {
				BuildCSR<float>(sink.Sj.get(), sink.Sk.get(), sink.Sv_f.get(),
				                *sink.mat_data, N, num_threads,
				                lhs_join_idx, lhs_agg_idx, lhs_val_idx, t_chunks);
			} else {
				BuildCSR<double>(sink.Sj.get(), sink.Sk.get(), sink.Sv_d.get(),
				                 *sink.mat_data, N, num_threads,
				                 lhs_join_idx, lhs_agg_idx, lhs_val_idx, t_chunks);
			}

			clock_gettime(CLOCK_MONOTONIC, &sink.end);
			printf("2 CSR init time: %.3f ms\n", get_ms(sink.start, sink.end));

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

			spalm_fn(spalm_type, sink.Ri.get(), sink.Rk.get(), rv_ptr,
			         sink.Sj.get(), sink.Sk.get(), sv_ptr,
			         sink.M, sink.K, sink.N,
			         false,
			         result_ptr, nullptr, nullptr, nullptr,
			         (int)c_keys.size(), c_keys.data(), c_vals.data());

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
	const idx_t total_rows = gstate.total_rows;  // M
	const idx_t vec_size  = 2048;

	output.SetCapacity(vec_size);
	// Use dynamic output indices: rhs_output_idx for M dimension (ai), lhs_output_idx for N dimension (bj)
	int *out_m_dim = (int *)output.data[rhs_output_idx].GetData();  // ai
	int *out_n_dim = (int *)output.data[lhs_output_idx].GetData();  // bj
	idx_t idx = 0;

	if (sink.use_float) {
		float *out_val = (float *)output.data[2].GetData();
		float *res = sink.result_f.get();

		while (idx < vec_size) {
			if (lstate.curr_pos >= lstate.end_pos) {
				idx_t row_start = gstate.next_row.fetch_add(gstate.chunk_rows);
				if (row_start >= total_rows) break;
				idx_t row_end    = std::min(row_start + gstate.chunk_rows, total_rows);
				lstate.curr_pos  = row_start * N;
				lstate.end_pos   = row_end   * N;
			}
			while (lstate.curr_pos < lstate.end_pos && idx < vec_size) {
				if (res[lstate.curr_pos] != 0) {
					out_m_dim[idx] = (int)(lstate.curr_pos / N);
					out_n_dim[idx] = (int)(lstate.curr_pos % N);
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
				idx_t row_start = gstate.next_row.fetch_add(gstate.chunk_rows);
				if (row_start >= total_rows) break;
				idx_t row_end    = std::min(row_start + gstate.chunk_rows, total_rows);
				lstate.curr_pos  = row_start * N;
				lstate.end_pos   = row_end   * N;
			}
			while (lstate.curr_pos < lstate.end_pos && idx < vec_size) {
				if (res[lstate.curr_pos] != 0) {
					out_m_dim[idx] = (int)(lstate.curr_pos / N);
					out_n_dim[idx] = (int)(lstate.curr_pos % N);
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
			printf("Output time: %.3f ms\n", get_ms(sink.start, sink.end));
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
