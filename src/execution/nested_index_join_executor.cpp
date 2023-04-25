//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), table_info_(nullptr), index_info_(nullptr), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid());
    index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexName(), table_info_->name_);
}

Tuple NestIndexJoinExecutor::Index_Join(Tuple *left_tuple, Tuple *right_tuple){
    std::vector<Value> values;
    auto output_schema = GetOutputSchema();
    for(size_t i = 0; i < output_schema->GetColumnCount(); i++){
        values.push_back(output_schema->GetColumn(i).GetExpr()->EvaluateJoin(left_tuple, plan_->OuterTableSchema(), right_tuple, plan_->InnerTableSchema()));
    }

    return Tuple(values, output_schema);
    
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple left_tuple, right_tuple;
    while (true){
        if(!rids_.empty()){
            auto right_rid = rids_.back();
            table_info_->table_->GetTuple(right_rid, &right_tuple, GetExecutorContext()->GetTransaction());

            *tuple = Index_Join(&left_tuple, &right_tuple); 
            return true;
        }
    
        if(!child_executor_->Next(&left_tuple, rid)){
            return false;
        }
        // ? left_tuple
        index_info_->index_->ScanKey(left_tuple, &rids_, GetExecutorContext()->GetTransaction());
    }
    return false;
}

}  // namespace bustub
