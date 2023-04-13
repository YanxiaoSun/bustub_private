//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#include "common/logger.h"
#include <thread>
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page* leaf_page = FindLeafPageByOperation(key, Operation::FIND, transaction, false).first;
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  
  ValueType value{};
  bool exist = leaf_node->Lookup(key, &value, comparator_);
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false); 
  if(!exist){
    return false;
  }
  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  const std::lock_guard<std::mutex> guard(root_latch_); 
  if(IsEmpty()){
    StartNewTree(key, value);
    // UnlockUnpinPages(transaction);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 新建root
  page_id_t new_page_id;
  Page* root_page = buffer_pool_manager_->NewPage(&new_page_id);
  
  if(root_page == nullptr){
    throw std::runtime_error("Out of memory!");
  }

  root_page_id_ = new_page_id;
  LeafPage* root_node = reinterpret_cast<LeafPage*>(root_page->GetData());
  UpdateRootPageId(1);
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
  LOG_INFO("END StartNewTree key=%lld", key.ToString());

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // LOG_INFO("ENTER InsertIntoLeaf key=%lld", key.ToString());
  auto [page, root_page_latch_]= FindLeafPageByOperation(key, Operation::INSERT, transaction, false);
  LeafPage* leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  // key已经存在
  if(leaf_page->Lookup(key, nullptr, comparator_)){
    if(root_page_latch_){
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  // key不存在，进行插入
  // LOG_INFO("ENTER InsertIntoLeaf key=%lld", key.ToString());
  int new_size = leaf_page->Insert(key, value, comparator_);
  // 需要进行分裂
  LOG_INFO("new_size is %d and leaf_max_size is %d", new_size, leaf_max_size_);
  if(new_size < leaf_max_size_){
    if(root_page_latch_){
      root_latch_.unlock();
    }
    // UnlockUnpinPages(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    LOG_INFO("END InsertIntoLeaf no split! key=%lld ", key.ToString());
    return true;
  } 
  LOG_INFO("END InsertIntoLeaf with split! key=%lld", key.ToString());
  LeafPage* new_leaf_page =  Split(leaf_page);
  bool pointer_root_page_latch_ = root_page_latch_;
  InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction, pointer_root_page_latch_);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  // LOG_INFO("END InsertIntoLeaf with split! key=%lld", key.ToString());
  return true;
}


/*
 * 注意node分为internal page和leaf page
 * 若是leaf page 则更新双向链表
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id = INVALID_PAGE_ID;
  Page* new_page = buffer_pool_manager_->NewPage(&page_id);
  if(new_page == nullptr){
    throw std::runtime_error("out of memory");
  }
  N* new_node = reinterpret_cast<N*>(new_page->GetData());
  if(node->IsLeafPage()){
    LeafPage* new_leaf_node = reinterpret_cast<LeafPage*>(new_node);
    LeafPage* old_leaf_node = reinterpret_cast<LeafPage*>(node);
    new_leaf_node->Init(page_id, old_leaf_node->GetParentPageId(), leaf_max_size_);
    old_leaf_node->MoveHalfTo(new_leaf_node);
    page_id_t next_page_id = old_leaf_node->GetNextPageId();
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_leaf_node->SetNextPageId(next_page_id);

    new_node = reinterpret_cast<N*>(new_leaf_node);
  }else{
    InternalPage* old_internal_node = reinterpret_cast<InternalPage*>(node);
    InternalPage* new_internal_node = reinterpret_cast<InternalPage*>(new_node);
    new_internal_node->Init(page_id, old_internal_node->GetParentPageId(), internal_max_size_);
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N*>(new_internal_node);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool root_page_latch_) {
  if(old_node->IsRootPage()){

    page_id_t new_page_id = INVALID_PAGE_ID;
    Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
    root_page_id_ = new_page_id;
    InternalPage* new_root_node = reinterpret_cast<InternalPage*>(new_page->GetData());

    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);// 初始化
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);

    UpdateRootPageId(0);
    if(root_page_latch_){
      root_page_latch_ = false;
      root_latch_.unlock();
    }
    Unlock(transaction);
    return;
  }else{
    LOG_INFO("InsertIntoParent old node is NOT root: completed key=%lld ", key.ToString());
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if(parent_node->GetSize() < parent_node->GetMaxSize() + 1){
      // InternalPage* new_parent_node = Split(parent_node);
      // InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node);
      if(root_page_latch_){
        root_page_latch_ = false;
        root_latch_.unlock();
      }
      Unlock(transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);      // unpin parent page
      // buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // unpin new parent node
      LOG_INFO("InsertIntoParent old node is NOT root: completed key=%lld ", key.ToString());
      return;
    }
    InternalPage* new_parent_node = Split(parent_node);
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction, root_page_latch_);
    //UnlockUnpinPages(transaction);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true); 
    
    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
int BPLUSTREE_TYPE::maxSize(N *node) {
  return node->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_INFO("ENTER Remove key %lld", key.ToString());
  auto [leaf_page, root_page_latch_] = FindLeafPageByOperation(key, Operation::DELETE, transaction, false);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  if(size == old_size){
    if(root_page_latch_){
      root_page_latch_ = false;
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return;
  }
  bool leaf_delete = CoalesceOrRedistribute(leaf_node, transaction, root_page_latch_);
  leaf_page->WUnlatch();
  if(leaf_delete){
    transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
    // buffer_pool_manager_->DeletePage(leaf_node->GetPageId());
  }

  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool root_page_latch_) {
  if(node->IsRootPage()){
    UnlockUnpinPages(transaction);
    return AdjustRoot(node, root_page_latch_);
  }
  if(node->GetSize() >= node->GetMinSize()){
    UnlockUnpinPages(transaction);
    return false;
  }
  page_id_t parent_page_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int index = parent_node->ValueIndex(node->GetPageId());
  page_id_t sibling_page_id = parent_node->ValueAt(index == 0 ? 1 : index - 1);
  Page* sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());
  sibling_page->WLatch();

  if(sibling_node->GetSize() + node->GetSize() > maxSize(node)){
    Redistribute(sibling_node, node, index, root_page_latch_);
    UnlockUnpinPages(transaction);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return false;
  }

  bool delete_parent = Coalesce(&sibling_node, &node, &parent_node, index, transaction,root_page_latch_);
  if(delete_parent){
    transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    // buffer_pool_manager_->DeletePage(parent_node->GetPageId());
  }
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool root_page_latch_) {
  int key_index = index;
  if(index == 0){
    std::swap(neighbor_node, node);
    key_index = 1;
  }
  if((*node)->IsLeafPage()){
    LeafPage* leaf_node = reinterpret_cast<LeafPage*>(*node);
    LeafPage* neighbor_leaf_node = reinterpret_cast<LeafPage*>(*neighbor_node);
    page_id_t page_id = leaf_node->GetNextPageId();
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(page_id);
  }else{
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(*node);
    InternalPage* neighbor_internal_node = reinterpret_cast<InternalPage*>(*neighbor_node);
    internal_node->MoveAllTo(neighbor_internal_node, (*parent)->KeyAt(key_index), buffer_pool_manager_);
  }
  (*parent)->Remove(key_index);

  return CoalesceOrRedistribute((*parent), transaction, root_page_latch_);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index, bool root_page_latch_) {
  if(root_page_latch_){
    root_latch_.unlock();
  }
  page_id_t page_id = node->GetParentPageId();
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(page->GetData());
  if(node->IsLeafPage()){
    LeafPage* leaf_node = reinterpret_cast<LeafPage*>(node);
    LeafPage* neighbor_leaf_node = reinterpret_cast<LeafPage*>(neighbor_node);
    if(index == 0){
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    }else{
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  }else{
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(node);
    InternalPage* neighbor_internal_node = reinterpret_cast<InternalPage*>(neighbor_node);
    if(index == 0){
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent_node->KeyAt(1), buffer_pool_manager_);
      parent_node->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    }else{
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent_node->KeyAt(index), buffer_pool_manager_);
      parent_node->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, bool root_page_latch_) {
  // case 1:old_root_node是内部结点，大小为1，应将孩子结点更新为根结点
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1){
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);

    if(root_page_latch_){
      root_latch_.unlock();
    }
    Page* new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage* new_root_node = reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  // case 2:old_root_node是叶结点，大小为0
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    if(root_page_latch_){
      root_latch_.unlock();
    }
    return true;
  }

  return false; 
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // root_latch_.RLock();
  Page* page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  //LeafPage* leaf = reinterpret_cast<LeafPage*>(page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, 0, page);
}
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // root_latch_.RLock();
  Page* page = FindLeafPageByOperation(key, Operation::FIND, nullptr, false).first;
  LeafPage* leaf = reinterpret_cast<LeafPage*>(page->GetData());
  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, index, page);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  // root_latch_.RLock();
  Page* leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  while(leaf->GetNextPageId() != INVALID_PAGE_ID){
    Page* next_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    leaf_page = next_page;
    leaf = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf->GetSize(), leaf_page);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */



INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost).first;
}

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page*, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation op, Transaction* transaction, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if(root_page_id_ == INVALID_PAGE_ID){
    throw std::runtime_error("Unexpected. root_page_id is INVALID_PAGE_ID");
  }
  // LOG_INFO("ENTER FindLeaf1 key=%lld", key.ToString()); 
  root_latch_.try_lock();
  // LOG_INFO("Lock is %d", a);
  bool root_page_latch_ = true;
  // LOG_INFO("ENTER FindLeaf2 key=%lld", key.ToString());
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  // assert(node->IsRootPage() == true);
  if(op == Operation::FIND){
    root_latch_.unlock();
    root_page_latch_ = false;
    page->RLatch();
  }else{
    // LOG_INFO("ENTER FindLeaf1 key=%lld, page = %d", key.ToString(), page->GetPageId());
    page->WLatch();
    // LOG_INFO("ENTER FindLeaf2 key=%lld", key.ToString());
    if(IsSafe(node, op)){
      root_page_latch_ = false;
      root_latch_.unlock();
    }
  }
  while(!node->IsLeafPage()){
    InternalPage *internal_node = reinterpret_cast<InternalPage*>(node);
    auto next_page_id = leftMost ? internal_node->ValueAt(0) : internal_node->Lookup(key, comparator_);
    Page* next_page = buffer_pool_manager_->FetchPage(next_page_id);
    BPlusTreePage* next_node = reinterpret_cast<BPlusTreePage*>(next_page->GetData());
    // buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
    if(op == Operation::FIND){
      next_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }else{
      next_page->WLatch();
      transaction->AddIntoPageSet(page);
      if(IsSafe(next_node, op)){
        if(root_page_latch_){
          root_latch_.unlock();
          root_page_latch_ = false;
        }
        UnlockUnpinPages(transaction);
      }
    }
    page = next_page;
    node = next_node;
  }
  LOG_INFO("ENTER FindLeafPageByOPeration key=%lld ", key.ToString());
  return std::make_pair(page, root_page_latch_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Unlock(Transaction* transaction){
  if(transaction == nullptr){
    return;
  }
  for(auto page:*transaction->GetPageSet()){
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction* transaction){
  // root_latch_.WUnlock();
  if(transaction == nullptr){
    return;
  }
  for(Page* page : *transaction->GetPageSet()){
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N* Node, Operation op){
  if(Node->IsRootPage()){
    return (op == Operation::INSERT && Node->GetSize() < maxSize(Node)) ||
           (op == Operation::DELETE && Node->GetSize() > 2);
  }
  if(op == Operation::INSERT){
    return Node->GetSize() < maxSize(Node);
  }
  if(op == Operation::DELETE){
    return Node->GetSize() > Node->GetMinSize();
  }
  return true;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}

