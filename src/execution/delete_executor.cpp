//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(std::move(child_executor)), table_info_(nullptr) {}

void DeleteExecutor::Init() {
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
    child_executor_->Init();

}

bool DeleteExecutor::delete_(Tuple* tuple, RID* rid){
    auto index_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto& index_ptr_ : index_info_){
        index_ptr_->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, index_ptr_->key_schema_, index_ptr_->index_->GetKeyAttrs()),
        *rid, GetExecutorContext()->GetTransaction());
    }

    auto table_ = table_info_->table_.get();
    bool ans = table_->MarkDelete(*rid, GetExecutorContext()->GetTransaction());
    return ans;
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    if(child_executor_->Next(tuple, rid)){
        return delete_(tuple, rid);
    }
    return false;
}

}  // namespace bustub
