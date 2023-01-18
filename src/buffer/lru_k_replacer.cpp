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
  BUSTUB_ASSERT(k != 0, "elicit value of k!");
  record_.assign(num_frames, std::vector<int>());  // initialize the num_frames of empty vector to record K's access.
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (lru_replacer_.empty()) {
    return false;
  }
  *frame_id = lru_replacer_.back().first;
  record_[*frame_id].clear();  // remove the frame's access history.
  lru_replacer_.pop_back();  // evict the frame with largest backward k-distance.
  hash_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "frame id is invalid.");
  auto& timestamp_arr = record_[frame_id];
  timestamp_arr.push_back(current_timestamp_);  // update elements of record array.
  auto it = hash_.find(frame_id);
  if (it != hash_.end()) {  // exist. lrureplacer needed to be adjusted.
    int curr_k_distance = INF;
    if (timestamp_arr.size() >= k_) {
      curr_k_distance = timestamp_arr.back() - timestamp_arr.at(timestamp_arr.size()-k_);
      it->second->second = curr_k_distance;  // update k_distance.
    }
    auto l = lru_replacer_.begin();
    while (l != lru_replacer_.end()) {
      if (l->second == curr_k_distance && timestamp_arr.at(0) < record_[l->first].at(0)) {  // multiple +INF
        std::advance(l, 1);
      } else if (l->second < curr_k_distance) {
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
  // BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "frame id is invalid.");
  BUSTUB_ASSERT(!record_[frame_id].empty(), "frame id is invalid.");
  auto timestamp_arr = record_[frame_id];
  auto it = hash_.find(frame_id);
  if (set_evictable && it == hash_.end()) {  // non-evictable to evictable
    // insert suitable location in list.
    int curr_k_distance = INF;
    if (timestamp_arr.size() >= k_) {
      curr_k_distance = timestamp_arr.back() - timestamp_arr.at(timestamp_arr.size()-k_);
    }
    if (lru_replacer_.empty()) {
      lru_replacer_.push_front({frame_id, curr_k_distance});
      hash_.insert({frame_id, lru_replacer_.begin()});  // update iterator.
    } else {
      auto l = lru_replacer_.begin();
      while (l != lru_replacer_.end()) {
        if (l->second == curr_k_distance && timestamp_arr.at(0) < record_[l->first].at(0)) {  // multiple +INF
          std::advance(l, 1);
        } else if (l->second < curr_k_distance) {
          std::advance(l, 1);
        } else {
          break;
        }
      }
      lru_replacer_.insert(l, {frame_id, curr_k_distance});  // insert before l.
      hash_.insert({frame_id, std::prev(l, 1)});  // update the current frame's iterator in hash table.
    }
    curr_size_++;
  } else if (!set_evictable && it != hash_.end()) {  // evictable to non-evictable
    lru_replacer_.remove({frame_id, it->second->second});
    hash_.erase(frame_id);
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
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

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub