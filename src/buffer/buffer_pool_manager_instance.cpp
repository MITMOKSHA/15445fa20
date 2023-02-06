//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every frames are recorded in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t f_id;
  *page_id = next_page_id_;
  if (!free_list_.empty()) {  // found free frames in buffer pool.
    f_id = free_list_.front();
    page_table_->Insert(next_page_id_, f_id);
    free_list_.pop_front();
  } else if (replacer_->Evict(&f_id)) {
    if (pages_[f_id].is_dirty_) {
      FlushPgImp(next_page_id_);
    }
    page_table_->Insert(next_page_id_, f_id);
  } else {
    return nullptr;  // all frame are current in use and not evictable.
  }
  AllocatePage();  // set page_id to the new page's id.
  // record access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);  // pin the frame.
  pages_[f_id].page_id_ = next_page_id_;
  pages_[f_id].pin_count_++;
  return pages_ + f_id;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // test whether if all frames are currently pinned.
  size_t i = 0;
  for (; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {  // frame not use.
      break;
    }
  }
  if (i == pool_size_) {  // all frames are currently in use.
    return nullptr;
  }
  frame_id_t f_id = 0;  // index of frame which is not used.
  if (!page_table_->Find(page_id, f_id)) {
    if (!free_list_.empty()) {
      f_id = free_list_.front();
      page_table_->Insert(page_id, f_id);
      free_list_.pop_front();
      disk_manager_->ReadPage(page_id,
                              pages_[f_id].data_);  // read the page from the disk and replace the old frame's data.
    } else if (replacer_->Evict(&f_id)) {
      if (pages_[f_id].is_dirty_) {
        FlushPgImp(page_id);
      }
      disk_manager_->ReadPage(page_id, pages_[f_id].data_);
      page_table_->Insert(page_id, f_id);
    } else {
      return nullptr;
    }
  }
  // record access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);  // pin the frame.
  pages_[f_id].pin_count_++;
  return pages_ + f_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {  // page_id is not in the buffer pool.
    return false;
  }
  Page *pg = pages_ + f_id;
  if (pg->pin_count_ == 0) {
    return false;
  }
  pg->pin_count_--;
  if (pg->pin_count_ == 0) {
    replacer_->SetEvictable(f_id, true);
  }
  if (!pg->is_dirty_) {
    pg->is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id >= 0, "INVALID_PAGE_ID");
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[f_id].data_);
  pages_[f_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(i);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
