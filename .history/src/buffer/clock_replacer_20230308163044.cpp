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
    max_size = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    //std::scoped_lock lock{mtx};
    if (lists.empty()) return false;
    mtx.lock();
    frame_id_t last_frame = lists.back();
    maps.erase(last_frame);
    lists.pop_back();
    *frame_id = last_frame;
    mtx.unlock();
    return true;


}

void ClockReplacer::Pin(frame_id_t frame_id) {
    //std::scoped_lock lock{mtx};
    if (maps.count(frame_id) == 0) return;
    mtx.lock();
    lists.remove(frame_id);
    maps.erase(frame_id);
    mtx.unlock();

}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    //std::scoped_lock lock{mtx};
    if (maps.count(frame_id) != 0) return;
    mtx.lock();
    if (lists.size() == max_size) {
        frame_id_t last_frame = lists.back();
        maps.erase(last_frame);
        lists.pop_back();
    }
    lists.push_front(frame_id);
    maps[frame_id] = 1;
    mtx.unlock();
}

size_t ClockReplacer::Size() { return lists.size(); }

}  // namespace bustub
