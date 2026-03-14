//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb-spalm/src/include/physical_spalm.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct SpalmColumnBindings {
	idx_t lhs_join_idx;
	idx_t lhs_agg_idx;
	idx_t lhs_val_idx;
	idx_t rhs_join_idx;
	idx_t rhs_agg_idx;
	idx_t rhs_val_idx;
	idx_t lhs_output_idx;
	idx_t rhs_output_idx;
};

// Our actual physical operator
class PhysicalSpalm : public PhysicalOperator {
public:
	PhysicalSpalm(vector<LogicalType> types, PhysicalOperator &left, PhysicalOperator &right,
	               SpalmColumnBindings col_bindings,
	               bool use_float, idx_t estimated_cardinality);

	string GetName() const override {
		return "PHYSICAL_SPALM";
	}

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::INSERTION_ORDER;
	}
	bool ParallelOperator() const override {
		return true;
	}


protected:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}
	bool RequiresFinalExecute() const override {
		return true;
	}

public:
	// Source interface
	bool IsSource() const override { return true; }
	bool ParallelSource() const override { return true; }
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                  GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
	                          OperatorSourceInput &input) const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

	bool GetUseFloat() const { return use_float; }
	const vector<LogicalType> &GetRhsChildTypes() const { return rhs_child_types; }
	const vector<LogicalType> &GetLhsChildTypes() const { return lhs_child_types; }
	idx_t GetRhsJoinIdx() const { return rhs_join_idx; }
	idx_t GetRhsAggIdx() const { return rhs_agg_idx; }
	idx_t GetRhsValIdx() const { return rhs_val_idx; }
	idx_t GetLhsJoinIdx() const { return lhs_join_idx; }
	idx_t GetLhsAggIdx() const { return lhs_agg_idx; }
	idx_t GetLhsValIdx() const { return lhs_val_idx; }

private:
	idx_t rhs_join_idx;
	idx_t rhs_agg_idx;
	idx_t rhs_val_idx;

	idx_t lhs_join_idx;
	idx_t lhs_agg_idx;
	idx_t lhs_val_idx;

	bool use_float;
	idx_t rhs_output_idx;
	idx_t lhs_output_idx;

	vector<LogicalType> rhs_child_types;
	vector<LogicalType> lhs_child_types;
};
} // namespace duckdb
