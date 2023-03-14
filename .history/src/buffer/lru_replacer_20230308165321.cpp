//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    max_size = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::scoped_lock lock{mtx};
    if(LRUlist.empty()){
        return false;
    }
    *frame_id = LRUlist.front();
    LRUlist.pop_front();
    maps.erase(*frame_id);
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock{mtx};
    if(maps.count(frame_id) == 0){
        return;
    }
    LRUlist.remove(frame_id);
    maps.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    
}

size_t LRUReplacer::Size() { return LRUlist.size(); }

}  // namespace bustub
