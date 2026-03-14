// spalm_extension.hpp
#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class SpalmExtension {
public:
	static void Load(DuckDB &db);
	static void InjectSpalm(unique_ptr<LogicalOperator> &plan);

	// Required extension functions
	static const char *Name() {
		return "spalm";
	}

	static const char *Version() {
		return "v0.0.1";
	}
};

} // namespace duckdb
