//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor(std::move(child_executor)), table_info_(nullptr),
    table_(nullptr) {}

void InsertExecutor::Init() {
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
    table_ = table_info_->table_.get();
    if(plan_->IsRawInsert()){
        iter_ = plan_->RawValues().begin();
    }else{
        child_executor->Init();
    }
    index_info_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);

}

void InsertExecutor::Insert(Tuple* tuple, RID* rid){
    table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction());
    for(auto& index_ptr : index_info_){
        index_ptr->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, index_ptr->key_schema_, index_ptr->index_->GetKeyAttrs()),
        *rid, GetExecutorContext()->GetTransaction());
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid){
    if(plan_->IsRawInsert()){
        if(iter_ != plan_->RawValues().end()){
            auto t = Tuple(*iter_++, &table_info_->schema_);
            Insert(&t, rid);
            return true;
        }
        return false;
    }else{
        if(child_executor->Next(tuple, rid)){
            Insert(tuple, rid);
            return true;
        }
        return false;
    }
    return false;
}
}  // namespace bustub
