//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb-spalm/src/include/logical_spalm.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "physical_spalm.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class LogicalSpalm : public LogicalExtensionOperator {
public:
	explicit LogicalSpalm(unique_ptr<LogicalAggregate> agg_op, unique_ptr<LogicalComparisonJoin> comp_join_op,
	                       unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);
	unique_ptr<LogicalAggregate> agg_op;
	unique_ptr<LogicalComparisonJoin> comp_join_op;

	string GetName() const override {
		return "SPALM";
	}

	string GetExtensionName() const;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

protected:
	void ResolveTypes() override;

private:
	// Find the output position of a ColumnBinding in a logical child's output
	static idx_t FindBindingPos(const vector<ColumnBinding> &bindings, ColumnBinding target);
};
} // namespace duckdb
