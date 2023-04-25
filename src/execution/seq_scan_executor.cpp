//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan)
,iter_(nullptr, RID{}, nullptr), table_heap_(nullptr), table_info_(nullptr){}



void SeqScanExecutor::Init() {
    table_info_ =exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()); 
    table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
    if(iter_ == table_heap_->End()){
        return false;
    }
    auto predicate = plan_->GetPredicate();
    const Schema *output_schema = plan_->OutputSchema();
    *tuple  = *iter_;
    RID original_rid = iter_->GetRid();

    std::vector<Value> values;
    values.reserve(output_schema->GetColumnCount());
    for(size_t i = 0; i < output_schema->GetColumnCount(); ++i){
        values.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(tuple, &(table_info_->schema_)));
    }
    Tuple new_tuple(values, output_schema);
    ++iter_;
    if(predicate == nullptr || predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()){
        *tuple = new_tuple;
        *rid = original_rid;
        return true;
    }
    return Next(tuple, rid);
}

}  // namespace bustub


