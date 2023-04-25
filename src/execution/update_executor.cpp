//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr) {}

void UpdateExecutor::Init() {
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::update(Tuple* tuple, RID* rid){
    auto index_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto& index_ptr_ : index_info_){
        index_ptr_->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, index_ptr_->key_schema_, index_ptr_->index_->GetKeyAttrs()),
        *rid, GetExecutorContext()->GetTransaction());
    }
    *tuple = GenerateUpdatedTuple(*tuple);
    auto table_ = table_info_->table_.get();
    table_->UpdateTuple(*tuple, *rid, GetExecutorContext()->GetTransaction());

    for(auto& index_ptr_ : index_info_){
        index_ptr_->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, index_ptr_->key_schema_, index_ptr_->index_->GetKeyAttrs()),
        *rid, GetExecutorContext()->GetTransaction());
    }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    if(child_executor_->Next(tuple, rid)){
        update(tuple, rid);
        return true;
    }
    return false;
}
}  // namespace bustub
