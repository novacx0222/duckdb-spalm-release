#include "logical_spalm.hpp"
#include "physical_spalm.hpp"

namespace duckdb {
LogicalSpalm::LogicalSpalm(unique_ptr<LogicalAggregate> agg_op, unique_ptr<LogicalComparisonJoin> comp_join_op,
                             unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : agg_op(std::move(agg_op)), comp_join_op(std::move(comp_join_op)) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

idx_t LogicalSpalm::FindBindingPos(const vector<ColumnBinding> &bindings, ColumnBinding target) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i] == target) {
			return i;
		}
	}
	throw InternalException("SPALM: column binding not found in child output");
}

PhysicalOperator &LogicalSpalm::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	D_ASSERT(children.size() == 2);
	idx_t lhs_cardinality = children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = children[1]->EstimateCardinality(context);

	// === Resolve column bindings using logical children ===
	auto lhs_bindings = children[0]->GetColumnBindings();
	auto rhs_bindings = children[1]->GetColumnBindings();

	// Get join condition column bindings
	auto &join_left_expr = comp_join_op->conditions[0].left->Cast<BoundColumnRefExpression>();
	auto &join_right_expr = comp_join_op->conditions[0].right->Cast<BoundColumnRefExpression>();
	auto lhs_table_idx = join_left_expr.binding.table_index;
	auto rhs_table_idx = join_right_expr.binding.table_index;

	SpalmColumnBindings col_bindings;
	col_bindings.lhs_join_idx = FindBindingPos(lhs_bindings, join_left_expr.binding);
	col_bindings.rhs_join_idx = FindBindingPos(rhs_bindings, join_right_expr.binding);

	// Resolve group-by columns
	auto &grp0 = agg_op->groups[0]->Cast<BoundColumnRefExpression>();
	auto &grp1 = agg_op->groups[1]->Cast<BoundColumnRefExpression>();

	if (grp0.binding.table_index == rhs_table_idx) {
		col_bindings.rhs_agg_idx = FindBindingPos(rhs_bindings, grp0.binding);
		col_bindings.rhs_output_idx = 0;
		col_bindings.lhs_agg_idx = FindBindingPos(lhs_bindings, grp1.binding);
		col_bindings.lhs_output_idx = 1;
	} else {
		col_bindings.rhs_agg_idx = FindBindingPos(rhs_bindings, grp1.binding);
		col_bindings.rhs_output_idx = 1;
		col_bindings.lhs_agg_idx = FindBindingPos(lhs_bindings, grp0.binding);
		col_bindings.lhs_output_idx = 0;
	}

	// Resolve multiply value columns
	auto &agg_expr = agg_op->expressions[0]->Cast<BoundAggregateExpression>();
	auto &mul_expr = agg_expr.children[0]->Cast<BoundFunctionExpression>();
	auto &val0 = mul_expr.children[0]->Cast<BoundColumnRefExpression>();
	auto &val1 = mul_expr.children[1]->Cast<BoundColumnRefExpression>();

	if (val0.binding.table_index == rhs_table_idx) {
		col_bindings.rhs_val_idx = FindBindingPos(rhs_bindings, val0.binding);
		col_bindings.lhs_val_idx = FindBindingPos(lhs_bindings, val1.binding);
	} else {
		col_bindings.rhs_val_idx = FindBindingPos(rhs_bindings, val1.binding);
		col_bindings.lhs_val_idx = FindBindingPos(lhs_bindings, val0.binding);
	}

	// === Type resolution ===
	LogicalType rhs_val_type = (val0.binding.table_index == rhs_table_idx) ? val0.return_type : val1.return_type;
	LogicalType lhs_val_type = (val0.binding.table_index == lhs_table_idx) ? val0.return_type : val1.return_type;
	bool use_float = (rhs_val_type == LogicalType::FLOAT && lhs_val_type == LogicalType::FLOAT);

	// === Create physical plans for children ===
	auto &left = generator.CreatePlan(*children[0]);
	auto &right = generator.CreatePlan(*children[1]);
	left.estimated_cardinality = lhs_cardinality;
	right.estimated_cardinality = rhs_cardinality;

	return generator.Make<PhysicalSpalm>(types, left, right, col_bindings,
	                                      use_float, estimated_cardinality);
}

vector<ColumnBinding> LogicalSpalm::GetColumnBindings() {
	D_ASSERT(agg_op->groupings_index != DConstants::INVALID_INDEX || agg_op->grouping_functions.empty());
	vector<ColumnBinding> result;
	result.reserve(agg_op->groups.size() + agg_op->expressions.size() + agg_op->grouping_functions.size());
	for (idx_t i = 0; i < agg_op->groups.size(); i++) {
		result.emplace_back(agg_op->group_index, i);
	}
	for (idx_t i = 0; i < agg_op->expressions.size(); i++) {
		result.emplace_back(agg_op->aggregate_index, i);
	}
	for (idx_t i = 0; i < agg_op->grouping_functions.size(); i++) {
		result.emplace_back(agg_op->groupings_index, i);
	}
	return result;
}

void LogicalSpalm::ResolveTypes() {
	if (children.empty()) {
		throw InternalException("Spalm operator needs a child");
	}
	D_ASSERT(agg_op->groupings_index != DConstants::INVALID_INDEX || agg_op->grouping_functions.empty());
	for (auto &expr : agg_op->groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : agg_op->expressions) {
		types.push_back(expr->return_type);
	}
	for (idx_t i = 0; i < agg_op->grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalSpalm::GetExtensionName() const {
	return "spalm";
}
} // namespace duckdb
