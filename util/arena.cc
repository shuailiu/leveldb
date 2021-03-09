// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

// 在LevelDB里面为了防止出现内存分配的碎片，采用了自己写的一套内存分配管理器,
// 名称为Arena。但是需要注意的是，这个内存分配器并不是为整个LevelDB项目考虑的。
// 主要是为skiplist也就是memtable服务

// skiplist里面记录的是用户传进来的key/value，这些字符串有长有短，放到内存中的时候，
// 很容易导致内存碎片。所以这里写了一个统一的内存管理器。

// skiplist/memtable要申请内存的时候，就利用Arena分配器来分配内存。
// 当skiplist/memtable要释放的时候，就直接通过Arena类的block_把所有申请的内存释放

// skiplist/memtable是没有删除接口的。所以里面的元素总是不断地添加进来。
// skiplist/memtable会在生成L0的文件之后，统一销毁掉。所以内存块可以直接
// 由Arena来统一销毁

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}
// 1. 申请一个新块。
// 2. 如果要的内存空间大于1K，那么直接返回相应的大小
// 3. 否则从4K的block里面扣内存
char* Arena::AllocateFallback(size_t bytes) {
  // 只有当要申请的内存数大于1K的时候，才直接用NewBlock
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }
  // 当要申请的内存数小于1K的时
  // 1. 直接申请一个新的内存块
  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;
  // 2. 从这个新块里面扣一个内存出来
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// 分配对齐的内存块
char* Arena::AllocateAligned(size_t bytes) {
  // 如果sizeof(void*)比8还要大，也就是遇到更高位的机器了？
  // 如果更大，就用sizeof(void*)来对齐
  // 否则就是用8来进行对齐
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 确保是2的指数次方
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  // 取得当前地址未对齐的尾数
  // 比如，要对齐的要求是8bytes
  // 但是当前指针指向的是0x017这里。
  // 那么余下的current_mod就是1
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 如果当前地址是0x17, 要求对齐是8 bytes
  // 那么current_mod = 1, slop 就是7
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // bytes + slop，
  // 就是把余下的这个slop = 7算在新的申请者头上
  // 返回的时候，直接向前移动slop个bytes
  // 就完成了对齐。
  size_t needed = bytes + slop;
  char* result;
  // 这里的逻辑就与Allocate完全一样的了。
  // 除了会移动一下slop以外
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // 这里断言一下，返回地址result肯定是对齐的
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

// 1. new一个新块
// 2. 把这个块指针放到vector里面记录着，析构的时候一起全部释放掉。
// 3. 增加已分配的内存
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
