//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()){}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tuple;
    RID rid;
    while(child_->Next(&tuple, &rid)){
        AggregateKey key = MakeKey(&tuple);
        AggregateValue value = MakeVal(&tuple);
        aht_.InsertCombine(key, value);
    }
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
    while(aht_iterator_ != aht_.End()){
        if(plan_->GetHaving() == nullptr || 
        plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()){
            std::vector<Value> values;
            for (auto& i : GetOutputSchema()->GetColumns()){
                values.emplace_back(i.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
            }
            *tuple = Tuple(values, GetOutputSchema());
            ++aht_iterator_;
            return true;
        }
        ++aht_iterator_;

    }
    return false;
}

}  // namespace bustub
