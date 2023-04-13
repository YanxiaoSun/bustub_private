/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager* bpm, int index, Page* page)
    :buffer_pool_manager_(bpm), page_(page), index_(index){
    leaf_page = reinterpret_cast<LeafPage*>(page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    if(leaf_page->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page->GetSize()){
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    return leaf_page->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    index_++;
    if(leaf_page->GetNextPageId() != INVALID_PAGE_ID && index_ == leaf_page->GetSize()){
        Page* next_page = buffer_pool_manager_->FetchPage(leaf_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
        page_ = next_page;
        leaf_page = reinterpret_cast<LeafPage*>(page_->GetData());
        index_ = 0;
    }
    return *this;
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const{
    return (leaf_page->GetPageId() == itr.leaf_page->GetPageId() && index_ == itr.index_);
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const{
    //return (leaf_page->GetPageId() != itr.leaf_page->GetPageID() || index_ != itr.index_);
    return !(*this == itr);
}



template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
