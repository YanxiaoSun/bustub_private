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
    maksize = num
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
    std::scoped_lock lock{mtx};
    lists.remove(frame_id);
    maps.erase(frame_id);

}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    if(maps.count(frame_id) != 0){
        return;
    }
    std::scoped_lock lock{mtx};
    lists.push_back(frame_id);
    maps[frame_id] = 1;
}

size_t ClockReplacer::Size() { return lists.size(); }

}  // namespace bustub
