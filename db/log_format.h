// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,     // record 完全在一个block中

  // For fragments
  kFirstType = 2,   // 当前block容纳不下所有的内容，record 的第一片在本block中
  kMiddleType = 3,  // record 的内容的起始位置不在本block，结束未知也不在本block
  kLastType = 4     // record 的内容起始位置不在本block，但 结束位置在本block
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;  // log中一个block的大小为32k

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
