//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), 
    right_executor_(std::move(right_executor)){}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
}

Tuple NestedLoopJoinExecutor::Join(Tuple *left_tuple, Tuple *right_tuple){
    std::vector<Value> values;
    auto output_schema = GetOutputSchema();
    for(size_t i = 0; i < output_schema->GetColumnCount(); i++){
        values.push_back(output_schema->GetColumn(i).GetExpr()->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple, right_executor_->GetOutputSchema()));
    }

    return Tuple(values, output_schema);
    
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple t1, t2;
    RID r1, r2;
    // ? while or if
    while(left_executor_->Next(&t1, &r1)){
        if(right_executor_->Next(&t2, &r2)){
            if(plan_->Predicate()->EvaluateJoin(&t1, left_executor_->GetOutputSchema(), &t2, right_executor_->GetOutputSchema()).GetAs<bool>()){
                *tuple = Join(&t1, &t2);
                return true;
            }
        }
    }
    return false;
    
}

}  // namespace bustub
