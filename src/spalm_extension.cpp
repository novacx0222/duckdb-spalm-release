// spalm_extension.cpp
#define DUCKDB_EXTENSION_MAIN
#include "spalm_extension.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "logical_spalm.hpp"

namespace duckdb {

static bool IsMatMulPattern(LogicalAggregate &agg, LogicalComparisonJoin &join) {
	// Must have exactly 1 aggregate expression: SUM(...)
	if (agg.expressions.size() != 1) {
		return false;
	}
	if (agg.expressions[0]->expression_class != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &agg_expr = agg.expressions[0]->Cast<BoundAggregateExpression>();
	if (agg_expr.function.name != "sum") {
		return false;
	}
	// SUM must have exactly 1 child: the multiply function
	if (agg_expr.children.size() != 1) {
		return false;
	}
	if (agg_expr.children[0]->expression_class != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &mul_expr = agg_expr.children[0]->Cast<BoundFunctionExpression>();
	if (mul_expr.function.name != "*") {
		return false;
	}
	// Multiply must have exactly 2 children, both column refs
	if (mul_expr.children.size() != 2) {
		return false;
	}
	if (mul_expr.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF ||
	    mul_expr.children[1]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}

	// Must have exactly 2 group-by expressions, both column refs
	if (agg.groups.size() != 2) {
		return false;
	}
	if (agg.groups[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF ||
	    agg.groups[1]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}

	// Must have exactly 1 join condition with COMPARE_EQUAL
	if (join.conditions.size() != 1) {
		return false;
	}
	if (join.conditions[0].comparison != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	if (join.conditions[0].left->expression_class != ExpressionClass::BOUND_COLUMN_REF ||
	    join.conditions[0].right->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}

	// Verify that columns come from different sides of the join
	auto &join_left = join.conditions[0].left->Cast<BoundColumnRefExpression>();
	auto &join_right = join.conditions[0].right->Cast<BoundColumnRefExpression>();
	auto lhs_table = join_left.binding.table_index;
	auto rhs_table = join_right.binding.table_index;

	auto &grp0 = agg.groups[0]->Cast<BoundColumnRefExpression>();
	auto &grp1 = agg.groups[1]->Cast<BoundColumnRefExpression>();
	auto &val0 = mul_expr.children[0]->Cast<BoundColumnRefExpression>();
	auto &val1 = mul_expr.children[1]->Cast<BoundColumnRefExpression>();

	// Join key columns must differ from group-by and value columns
	auto join_left_binding = join_left.binding;
	auto join_right_binding = join_right.binding;

	// Each group-by column and each value column should come from one side of the join
	// One group-by + one value from LHS, one group-by + one value from RHS
	int lhs_grp_count = (grp0.binding.table_index == lhs_table ? 1 : 0) +
	                    (grp1.binding.table_index == lhs_table ? 1 : 0);
	int rhs_grp_count = (grp0.binding.table_index == rhs_table ? 1 : 0) +
	                    (grp1.binding.table_index == rhs_table ? 1 : 0);
	if (lhs_grp_count != 1 || rhs_grp_count != 1) {
		return false;
	}

	int lhs_val_count = (val0.binding.table_index == lhs_table ? 1 : 0) +
	                    (val1.binding.table_index == lhs_table ? 1 : 0);
	int rhs_val_count = (val0.binding.table_index == rhs_table ? 1 : 0) +
	                    (val1.binding.table_index == rhs_table ? 1 : 0);
	if (lhs_val_count != 1 || rhs_val_count != 1) {
		return false;
	}

	return true;
}

void SpalmExtension::InjectSpalm(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	if (plan->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if (plan->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &agg_ref = plan->Cast<LogicalAggregate>();
			auto &join_ref = plan->children[0]->Cast<LogicalComparisonJoin>();
			if (!IsMatMulPattern(agg_ref, join_ref)) {
				// Not a matmul pattern, skip
				for (auto &child : plan->children) {
					InjectSpalm(child);
				}
				return;
			}
			auto agg_op = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
			auto comparison_join =
			    unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(agg_op->children[0]));
			// Extract children before moving comparison_join
			auto left_child = std::move(comparison_join->children[0]);
			auto right_child = std::move(comparison_join->children[1]);
			auto spalm = make_uniq<LogicalSpalm>(std::move(agg_op), std::move(comparison_join),
			                                       std::move(left_child),
			                                       std::move(right_child));
			plan = std::move(spalm);
		}
	}

	for (auto &child : plan->children) {
		InjectSpalm(child);
	}
}

static void OptimizePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	SpalmExtension::InjectSpalm(plan);
}

void SpalmExtension::Load(DuckDB &db) {
	try {
		printf("Loading Spalm extension...\n");

		auto optimizer_extension = make_uniq<OptimizerExtension>();
		optimizer_extension->optimize_function = OptimizePlan;
		db.instance->config.optimizer_extensions.push_back(std::move(*optimizer_extension));

		printf("Spalm extension loaded successfully\n");
	} catch (const std::exception &e) {
		printf("Error loading Spalm extension: %s\n", e.what());
	}
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void spalm_extension_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SpalmExtension>();
}

DUCKDB_EXTENSION_API const char *spalm_extension_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
