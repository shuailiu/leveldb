// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {
// BytewiseComparatorImpl
// 作用：
// 因为Slice并没有规定Key具体类型，所以leveldb是支持用户自定义比较器的，在
// 创建leveldb数据库对象的时候通过Option指定。
// BytewiseComparatorImpl是leveldb默认的比较器，基于二进制比较。
// 默认配置相见options.cc。
Comparator::~Comparator() = default;

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }
  // 如果start<limit,就把start修改为*start和limit的共同前缀后面多一个字符加1。例如：

  // *start:    helloleveldb        上一个data block的最后一个key
  // limit:     helloworld          下一个data block的第一个key
  // 由于 *start < limit,所以调用FindShortSuccessor(start, limit)之后，start变成：
  // hellom (保留前缀，第一个不相同的字符+1)

  //  *start:    hello               上一个data block的最后一个key
  // limit:     helloworld          下一个data block的第一个key
  // 由于*start < limit,所以调用 FindShortSuccessor(start, limit)之后，start变成：
  // hello (保留前缀，第一个不相同的字符+1)
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }
    // 上一个data block的key是下一个data block的子串，则不处理
    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
