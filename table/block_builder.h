// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <stdint.h>

#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  // buffer_最终存放一个完整的data block：
  //    [Entry 1]
  //    ...
  //    [Enety N]
  //    [Restart Point 1]
  //    ...
  //    [Restart Point M]
  //    Restart Point Count
  // 其中，一个Enety是：
  //  [shared key len][unshared key len][value len][unshared key content][value]
  std::string buffer_;              // Destination buffer
  // key采用前缀压缩，但每隔16个key强制存储一个完整的key，存储整个key的点叫做重启点
  std::vector<uint32_t> restarts_;  // Restart points
  // 重新开始记录一个完整的key（新建重启点后），向data block添加的key-value数量
  int counter_;                     // Number of entries emitted since restart
  // 是否则调用了Finish去构造了一个完整的data block
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;  // 上次插入key-value时，最后一次插入的key
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
