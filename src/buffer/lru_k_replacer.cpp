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
  // LOG_INFO("initialize LRUKReplacer(%ld, %ld)", num_frames, k);
  BUSTUB_ASSERT(k != 0, "elicit value of k!");
  record_.assign(num_frames,
                 std::vector<int>());       // initialize the num_frames of empty vector to record K's access.
  is_evictable_.assign(num_frames, false);  // initialize non-evictable;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f_id = INVALID_PAGE_ID;
  // prior to evict the frame in lru_1_replacer_.
  if (!lru_1_replacer_.empty()) {  // lru_1_cache_ is not empty.
    auto ptr = lru_1_replacer_.end();
    ptr = std::prev(ptr);
    while (ptr != lru_1_replacer_.begin() && !is_evictable_[*ptr]) {
      ptr = std::prev(ptr);
    }
    // check the front elements of lru_1_replacer.
    if (!is_evictable_[*ptr]) {  // no frames can be evicted.
      int earliest_time_stamp = INT_MAX;
      for (size_t i = 0; i < record_.size(); ++i) {
        size_t log_num = record_[i].size();
        if (log_num < k_ ||
            !is_evictable_[i]) {  // jump the frame that access history times less than k or not-evictable frame.
          continue;
        }
        if (record_[i][log_num - k_] < earliest_time_stamp) {
          earliest_time_stamp = record_[i][log_num - k_];
          f_id = i;
        }
      }
    } else {
      f_id = *ptr;
      lru_1_replacer_.remove(f_id);
    }
  } else {  // >= k
    int earliest_time_stamp = INT_MAX;
    for (size_t i = 0; i < record_.size(); ++i) {
      size_t log_num = record_[i].size();
      if (log_num < k_ || !is_evictable_[i]) {
        continue;
      }
      if (record_[i][log_num - k_] < earliest_time_stamp) {
        earliest_time_stamp = record_[i][log_num - k_];
        f_id = i;
      }
    }
  }
  if (f_id == INVALID_PAGE_ID) {  // no frames can be evicted.
    return false;
  }
  record_[f_id].clear();  // remove the frame's access history.
  is_evictable_[f_id] = false;
  hash_.erase(f_id);
  curr_size_--;
  *frame_id = f_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "frame id is invalid.");
  auto &timestamp_arr = record_[frame_id];
  timestamp_arr.push_back(current_timestamp_);  // update elements of record array.
  // >= k_ do nothing.
  auto it = hash_.find(frame_id);
  if (timestamp_arr.size() < k_) {
    if (it == hash_.end()) {
      lru_1_replacer_.push_front(frame_id);
      hash_.insert({frame_id, lru_1_replacer_.begin()});
    }
  } else {  // >= k
    // move it from LRU-1 mode to LRU-k mode.
    if (it != hash_.end()) {
      hash_.erase(frame_id);
      lru_1_replacer_.remove(frame_id);
    }
  }
  ++current_timestamp_;  // every access with incrementing time stamp.
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (int)(replacer_size_), "frame id is invalid.");
  auto timestamp_arr = record_[frame_id];
  // teminate if the frame does not have record.
  if (timestamp_arr.empty()) {
    return;
  }
  bool pre_status = is_evictable_[frame_id];  // the previous evictable status of the frame.
  if (!pre_status && set_evictable) {         // non-evictable to evictable
    is_evictable_[frame_id] = set_evictable;
    curr_size_++;                             // increase the size of replacer.
  } else if (pre_status && !set_evictable) {  // evictable to non-evictable
    is_evictable_[frame_id] = set_evictable;
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // LOG_INFO("Remove(%d)", frame_id);
  std::scoped_lock<std::mutex> lock(latch_);
  auto timestamp_arr = record_[frame_id];
  if (timestamp_arr.empty()) {  // frame is not found.
    return;
  }
  BUSTUB_ASSERT(is_evictable_[frame_id], "not be abled to remove non-evictable frame.");
  if (timestamp_arr.size() < k_) {
    lru_1_replacer_.remove(frame_id);
    hash_.erase(frame_id);
  }
  // >= k_  do nothing.
  is_evictable_[frame_id] = false;
  record_[frame_id].clear();  // remove the frame's access history.
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  // LOG_INFO("the size of LRU Replacer is %ld", curr_size_);
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
