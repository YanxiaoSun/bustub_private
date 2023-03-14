//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    lists.clear();
    maps.clear();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    std::scoped_lock lock{mtx};
    if(lists.empty()){
        return false;
    }
    frame_id_t a = lists.front();
    lists.pop_front();
    maps.erase(a);
    return true;


}

void ClockReplacer::Pin(frame_id_t frame_id) {
    if(maps.count(frame_id) == 0){
        return;
    }
    lists.remove()
}

void ClockReplacer::Unpin(frame_id_t frame_id) {}

size_t ClockReplacer::Size() { return 0; }

}  // namespace bustub
