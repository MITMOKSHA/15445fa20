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
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f_id;
  *page_id = INVALID_PAGE_ID;

  if (!free_list_.empty()) {    // found free frames in buffer pool.
    f_id = free_list_.front();  // get free frame.
    free_list_.pop_front();
  } else if (replacer_->Evict(&f_id)) {  // or if replacer can evict one frame.
  } else {
    return nullptr;  // all frame are current in use and not evictable.
  }
  auto fm = pages_ + f_id;  // the evicted frame address.
  if (fm->is_dirty_) {      // if the frame is dirty, flush it to the disk.
    disk_manager_->WritePage(fm->page_id_, fm->data_);
    fm->is_dirty_ = false;
  }
  page_table_->Remove(fm->page_id_);  // unmap the original frame id and page id.

  page_table_->Insert(next_page_id_, f_id);  // map the page id to frame id.

  *page_id = next_page_id_;  // put the new page id into the out parameter.
  // record access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);  // pin the frame.
  // update the metadata of the new page.
  fm->page_id_ = next_page_id_;
  fm->pin_count_++;
  memset(fm->data_, '\0', BUSTUB_PAGE_SIZE);
  AllocatePage();  // prepare for getting next page id.
  return pages_ + f_id;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f_id;  // index of frame which is not used.
  if (!page_table_->Find(page_id, f_id)) {
    if (!free_list_.empty()) {
      f_id = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&f_id)) {
    } else {
      return nullptr;
    }
    auto fm = pages_ + f_id;
    if (fm->is_dirty_) {
      disk_manager_->WritePage(fm->page_id_, fm->data_);
      fm->is_dirty_ = false;
    }
    page_table_->Remove(fm->page_id_);
    disk_manager_->ReadPage(page_id, fm->data_);  // read the page data from the disk and replace the old frame's data.
    page_table_->Insert(page_id, f_id);
  }
  // record access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);  // pin the frame.
  pages_[f_id].page_id_ = page_id;
  pages_[f_id].pin_count_++;
  return pages_ + f_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {  // page is not in the buffer pool.
    return false;
  }
  Page *pg = pages_ + f_id;
  if (pg->pin_count_ <= 0) {  // page's pin count less or equal 0 before unpin.
    return false;
  }
  if (--pg->pin_count_ == 0) {
    replacer_->SetEvictable(f_id, true);  // if the pin count is 0, the frame should be evicted by replacer.
  }
  if (!pg->is_dirty_) {
    pg->is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id >= 0, "INVALID_PAGE_ID");
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[f_id].data_);
  pages_[f_id].is_dirty_ = false;  // unset the dirty flag after flush.
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    disk_manager_->WritePage(i, pages_[i].data_);
    pages_[i].is_dirty_ = false;  // unset the dirty flag after flush.
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {  // page id is not in the buffer pool.
    return true;
  }
  if (pages_[f_id].pin_count_ > 0) {  // page is pinned in buffer pool which is not be delete.
    return false;
  }
  page_table_->Remove(page_id);
  // stop tracking the frame in replacer.
  replacer_->Remove(f_id);
  // add the frame back to the free list.
  free_list_.push_back(f_id);
  // reset page's memory and metadata.
  pages_[f_id].is_dirty_ = false;
  pages_[f_id].pin_count_ = 0;
  pages_[f_id].page_id_ = INVALID_PAGE_ID;
  memset(pages_[f_id].data_, '\0', BUSTUB_PAGE_SIZE);
  // free the page on the disk.
  DeallocatePage(page_id);
  // deletion succeed.
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
