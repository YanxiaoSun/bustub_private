//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), iter_(nullptr, 0, nullptr) {}

void IndexScanExecutor::Init() {
    index_info_ =exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()); 
    index_ = reinterpret_cast<BPlusTreeIndexType*>(index_info_->index_.get());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    table_heap_ = table_info_->table_.get(); 
    iter_ = index_->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
    auto predicate = plan_->GetPredicate();
    const Schema* output_schema = GetOutputSchema();
    while(iter_ != index_->GetEndIterator()){
        *rid = (*iter_).second;
        
        /*std::vector<Value> values;
        for(size_t i = 0; i < output_schema->GetColumnCount(); ++i){

        }*/
        Transaction *tx = GetExecutorContext()->GetTransaction();
        table_heap_->GetTuple(*rid, tuple, tx);

        std::vector<Value> values;
        for(size_t i = 0; i < output_schema->GetColumnCount(); ++i){
            values.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(tuple, &table_info_->schema_));
        }
        Tuple new_tuple(values, output_schema);
        ++iter_;
        // ? output_schema or table_info->schema_
        if(predicate == nullptr || predicate->Evaluate(tuple, output_schema).GetAs<bool>()){
            *tuple = new_tuple;
            return true;
        }
    }
    return false;

}

}  // namespace bustub
