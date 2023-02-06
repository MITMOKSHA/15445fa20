//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  for (int i = 0; i < num_buckets_; ++i) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size));  // add bucket(defualt depth: 0)
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;  // get low global_depth_ bits.
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)].get()->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto &bucket : dir_) {
    Bucket *l = bucket.get();
    if (l->Remove(key)) {
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  Bucket *bucket = dir_[IndexOf(key)].get();
  // V& val = const_cast<V&>(value);  // convert non-const to const to call Find().
  std::scoped_lock<std::mutex> lock(latch_);
  if (bucket->IsFull()) {
    int origin = IndexOf(key);  // original index of bucket.
    if (GetLocalDepth(IndexOf(key)) == GetGlobalDepth()) {
      global_depth_++;  // increment global depth.
    }
    bucket->IncrementDepth();  // increment local depth.
    // wrong implementation.
    // dir_.resize(global_depth_*2, std::make_shared<Bucket>(bucket_size_, global_depth_));  // double the size of
    // directory.
    int before_len = dir_.size();  // the dir_ length before resize() calling.
    // resize() will change the number of elements and capacity of vector.
    dir_.resize(global_depth_ * 2, nullptr);  // double the size of directory.
    int len = dir_.size();                    // the dir_ length before resize() calling.
    // initialize the augment slot in dir_.
    for (int i = before_len; i < len; ++i) {
      dir_[i] = std::make_shared<Bucket>(bucket_size_, global_depth_);
    }
    RedistributeBucket(dir_[origin]);  // pass original bucket.
    bucket = dir_[IndexOf(key)].get();
  }
  bucket->Insert(key, value);  // updated.
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
  auto &list = bucket->GetItems();
  std::vector<std::pair<K, V>> del;  // elements will be deleted.
  for (auto &elem : list) {
    std::shared_ptr<Bucket> b = dir_[IndexOf(elem.first)];
    // reshuffle the elements in the bucket which will be overflowed to other bucket.
    if (b != bucket) {
      b->Insert(elem.first, elem.second);
      del.push_back(elem);  // bookkeeping the elements will be deleted
    }
  }
  for (auto &e : del) {
    list.remove(e);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto list = GetItems();
  for (auto &c : list) {
    if (c.first == key) {
      value = c.second;  // store the V to value
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto &list = GetItems();
  int i = 0;
  for (auto &c : list) {
    i++;
    if (c.first == key) {
      list.remove(c);  // c++ 20 trait.
      return true;
    }
  }
  // key does not exist.
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {  // bucket is full.
    return false;
  }
  auto &list = GetItems();  // it should be use reference.
  bool is_exist = false;
  for (auto &c : list) {
    if (c.first == key) {
      c.second = value;  // if the key exist, update it.
      is_exist = true;
    }
  }
  if (!is_exist) {
    list.push_back({key, value});  // if the key does not exist, push the KV pair to bucket.
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
