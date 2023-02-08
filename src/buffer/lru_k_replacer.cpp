//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  LOG_INFO("initialize LRUKReplacer(%ld, %ld)", num_frames, k);
  BUSTUB_ASSERT(k != 0, "elicit value of k!");
  record_.assign(num_frames, std::vector<int>());  // initialize the num_frames of empty vector to record K's access.
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (lru_replacer_.empty()) {
    LOG_INFO("Evict failed");
    return false;
  }
  frame_id_t f_id = lru_replacer_.back().first;
  record_[f_id].clear();     // remove the frame's access history.
  lru_replacer_.pop_back();  // evict the frame with largest backward k-distance.
  hash_.erase(f_id);
  curr_size_--;
  *frame_id = f_id;
  LOG_INFO("Evict frame %d and success!", *frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "frame id is invalid.");
  auto &timestamp_arr = record_[frame_id];
  timestamp_arr.push_back(current_timestamp_);  // update elements of record array.
  auto it = hash_.find(frame_id);
  LOG_INFO("RecordAccess(%d) at timestamp %ld", frame_id, current_timestamp_);
  if (it != hash_.end()) {  // exist. LRUreplacer needed to be adjusted.
    int curr_k_distance = INF;
    if (timestamp_arr.size() >= k_) {
      curr_k_distance = timestamp_arr.back() - timestamp_arr.at(timestamp_arr.size() - k_);
      it->second->second = curr_k_distance;  // update k_distance.
      LOG_INFO("the frame_id %d current k-distance is %d", frame_id, curr_k_distance);
    }
    auto l = lru_replacer_.begin();
    while (l != lru_replacer_.end()) {
      if ((l->second == curr_k_distance && timestamp_arr.at(0) < record_[l->first].at(0)) ||
          (l->second < curr_k_distance)) {  // multiple +INF
        std::advance(l, 1);
      } else {
        break;
      }
    }
    lru_replacer_.splice(l, lru_replacer_, it->second);
  }
  ++current_timestamp_;  // every access with incrementing time stamp.
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  LOG_INFO("SetEvictable(%d, %s)", frame_id, set_evictable ? "true" : "false");
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "frame id is invalid.");
  BUSTUB_ASSERT(!record_[frame_id].empty(), "frame id is invalid.");
  auto timestamp_arr = record_[frame_id];
  auto it = hash_.find(frame_id);
  if (set_evictable && it == hash_.end()) {  // non-evictable to evictable
    // insert suitable location in list.
    int curr_k_distance = INF;
    if (timestamp_arr.size() >= k_) {
      curr_k_distance = timestamp_arr.back() - timestamp_arr.at(timestamp_arr.size() - k_);
    }
    if (lru_replacer_.empty()) {
      lru_replacer_.push_front({frame_id, curr_k_distance});
      hash_.insert({frame_id, lru_replacer_.begin()});  // update iterator.
    } else {
      auto l = lru_replacer_.begin();
      while (l != lru_replacer_.end()) {
        if ((l->second == curr_k_distance && timestamp_arr.at(0) < record_[l->first].at(0)) ||
            (l->second < curr_k_distance)) {  // multiple +INF
          std::advance(l, 1);
        } else {
          break;
        }
      }
      lru_replacer_.insert(l, {frame_id, curr_k_distance});  // insert before l.
      hash_.insert({frame_id, std::prev(l, 1)});             // update the current frame's iterator in hash table.
      LOG_INFO("not evictable to evictable: put {frame_id: %d, k-distance: %d}", frame_id, curr_k_distance);
    }
    curr_size_++;                                    // increase the size of replacer.
  } else if (!set_evictable && it != hash_.end()) {  // evictable to non-evictable
    lru_replacer_.remove({frame_id, it->second->second});
    hash_.erase(frame_id);
    curr_size_--;
    LOG_INFO("evictable to non-evictable: frame_id: %d out of lru replacer", frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  LOG_INFO("Remove(%d)", frame_id);
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(hash_, "not be able to remove non-evictable frame.");
  auto it = hash_.find(frame_id);
  if (it == hash_.end()) {  // frame_id not found.
    return;
  }
  record_[frame_id].clear();  // remove the frame's access history.
  lru_replacer_.remove({frame_id, it->second->second});
  hash_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  LOG_INFO("the size of LRU Replacer is %ld", curr_size_);
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
