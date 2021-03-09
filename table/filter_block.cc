// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
// 对于2k这个值：
// 这个参数的本意应该是，如果多个key－value的总长度超过了2KB，我就应该计算
// 这些key的位图了。但是无奈发起的时机是有Flush决定，因此，并非2KB的数据就会
// 发起一轮 Bloom Filter的计算,比如block_offset等于7KB，可能造成多轮的
// GenerateFilter函数调用，而除了第一轮的调用会产生位图，
// 其它2轮相当于轮空，只是将result_的size再次放入filter_offsets_。
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

/*
                  data block offset,    data block index,  对应位图
handle()->offset->
                  0 ~ 2k-1              0                  boom filter bitmap 0
                  2k ~ 4k               0                  boom filter bitmap 0
                  4k ~ 6k               0                  boom filter bitmap 0
handle()->offset->
                  6k ~ 7k-1             0                  boom filter bitmap 1
                  7k ~ 8k               1                  boom filter bitmap 1

*/

// 如果（0～7K-1)是第一个data block的范围，（7K～14K－1）是第二个 data block的范围，
// （0～6K－1）没啥问题，可是（6K～7K－1）属于第一个data block，但是存放
// bloom filter的时候，指向的是第二个 bloom filter，将来可能会带来问题。实际上不会，
// 因为 meta block是data block的辅助，应用层绝不会问 data block offset为6K
// 的block位图在何方。从查找的途径来看，先根据key通过第二篇的index block，找到
// 对应的data block，而data block的offset，只会是0或者7K，绝不会是6K。当传入
// data block的offset是7K的时候，根据上表，就会返回第二个bloom filter，而第二个
// bloom filter会负责整个第二个data block的全部key，即data block的（7K～14K－1）
// 范围内的所有key，都可以利用第二个bloom filter找到


FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// 什么时候发起一轮keys位图的计算？
// 这个函数负责一轮计算Bloom Filter的位图
// 触发的时机是Flush函数（TableBuilder::Add），而Flush的时机是预测Data Block的
// size超过options.block_size
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 如果key-value的总长度超过了2k，就该计算一次filter位图了
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}
// 因为sstable中key的个数可能很多，当攒了足够多个key值，就会计算一批位图，
// 再攒一批key，又计算一批位图那么这么多bloom filter的位图，必需分隔开，也就说，
// 位图与位图的边界必需清晰
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  // 得到本轮的所有的keys，放入tmp_keys_数组
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 先记录下上一轮位图截止位置，防止位图的边界混淆
  filter_offsets_.push_back(result_.size());
  // 将本轮keys计算得来的位图追加到result_字符串
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
  // 注意本轮keys产生的位图计算完毕后，会将keys_, start_ ,还有tmp_keys_ 清空
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}
// 根据输入的data block offset，查找对应data block 的bloom filter bitmap
// 按照上面的分析，如果block_offset为6KB的时候，就会找不到对应位图，因为 start和
// limit指向的都是bitmap 1,函数中的filter最终为空，但是没关系，绝不会传进来6K，
// 因为6K不是两个data block的边界
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
