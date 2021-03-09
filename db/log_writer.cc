// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}
// 恒等不变式:
// 0 <= block_offset_ <= kBlockSize - kHeaderSize
// block_offset_(0)： 恒等不变式成立
Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}
// 注意这里对kBlockSize进行了取模
// 这里不是表示的是文件的偏移量？
// 那表示的是什么？
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {  // 恒等不变式
                                                              // 可能不成立
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;
// 当调用AddRecord写入一个Slice（data_）时，Slice中data_大小可能很大也可能很小，
// 而当前待写入的block剩余空间不定，所以data_可能要分为多次写入多个block中

// 写的时候是以Record（header+data）为当为，不是以block为单位

// 每次写入时，都要先写header，block的剩余空间大小不同
// block剩余空间大于7字节：正常写 header+slice（slice大小大于0）
// block剩余空间小于7字节：通过填充剩余的几个字节
// block剩余空间等于7字节：正常写入header，然后写入大小为0的slice（由于slice大小
//                        为0，相当于Append中没有任何写入操作），写入的是一个没有
//                        数据kHeaderSize大小的Record，只是这个Record没有任何
//                        数据，并且type可以是任意类型。

// 在一个block中：
// Q1. 如果还余下7个bytes的时候：
//     会把一个record的header写在这7个byte里面，写入的header的length=0，写完
//     heade后，然后写入一个大小为0的Slice，由于该slice的大小为0，调用
//     WritableFile的Append时，其实没有任何写入操作。
//     写完header和大小为0的slice后，block_offset_=0，移到下一个行的block。
// Q2. 如果余下的小于7个byte：
//     直接用0来进行填充。
// Q3: 给定一个空的block，给特别多的数据。那么中间block尾巴上的7个byte：
//     直接用来填数据。下一个block的开头就是一个header。
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();  // 待写入的数据
  size_t left = slice.size();      // 待写入数据的大小

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  // 每次客户端调用AddRecord的时候，开始设置 block_offset_
  do {
    // 余下还需要多少才能填满一个块。
    const int leftover = kBlockSize - block_offset_;  // 当前block总的剩余空间
    assert(leftover >= 0);
    // 当前block剩余空间无法写入一个完整的header
    if (leftover < kHeaderSize) {
      // Case1：当前block有空间，但是不足以写入一个完整的header，
      //        把剩下的空间用0来填满的
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");  // 编译期间的断言（静态断言）
        // TODO: 为啥是6个0，而不是7个0？？
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // Case2：如果余下的等于0，什么都不用做

      // 最后，移动到一个新的block的头上
      block_offset_ = 0;  // 一旦发现block_offset_ + kHeaderSize > blockSize
                          // 的时候。就直接把block_offset_设置为0
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 当前block剩余空间可以写入一个完整的header
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 该block剩余可写入record的空间大小（不包括header）
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 当前block需要写入的数据大小 = min(block剩余空间大小, 传入的数据大小)
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 根据能写的数据的情况，来决定当前的这个record的类型。
    const bool end = (left == fragment_length);  // true：当前block剩余空间足以
                                                 //       一次把slice数据写完
    if (begin && end) {
      // 从头写入，且block剩余空间=传入的数据大小，可以直接把slice数据写完
      type = kFullType;
    } else if (begin) {  // 不能写完，但是是从头开始写
      type = kFirstType;
    } else if (end) {  // 不是从头开始写，但是可以把数据写完
      type = kLastType;
    } else {  // 不能从头开始写，也不能把数据写完。
      type = kMiddleType;
    }
    // 这里提交一个物理的记录
    // 注意：可能这里并没有把一个slice写完。
    s = EmitPhysicalRecord(type, ptr, fragment_length);  // 写入 record，并落盘
    ptr += fragment_length;   // 指向下次要写入的数据
    left -= fragment_length;  // 剩余待写入数据的大小
    begin = false;            // 不是从头写，因为已经写过一次了
  } while (s.ok() && left > 0);
  return s;
}
// 拼装 log 记录头，并写入磁盘。 写入操作需要注意block_offset_

// 写入文件的时候，是以Record为单位。而不是以Block为单位。但是需要注意的是，
// 有可能Record只是Slice的一部分。也就是说，写入的时候，有可能不是一个
// 完整的Slice被写到了文件中。
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
                             // length 占用2个字节，2^16 - 1
  // length 为待写入数据的大小，不包括 Slice 中head的大小
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);
  // LevelDB使用的是小端字节序存储，低位字节排放在内存的低地址端
  // buf前面那个int是用来存放crc32的。
  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);  // 先写入低8位， 0xff = 1111 1111
  buf[5] = static_cast<char>(length >> 8);    // 再写入高8位， length 占用2个字节
  buf[6] = static_cast<char>(t);              // 再写入类型

  // Compute the crc of the record type and the payload.
  // 计算header和数据区的CRC32的值。具体过程不去关心。
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 当写完一个record之后，这里就立马flush
  // 但是有可能这个slice并不是完整的。
  Status s = dest_->Append(Slice(buf, kHeaderSize));  // 写入 header
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));  // 写入 record
    if (s.ok()) {
      s = dest_->Flush();  // 落盘
    }
  }
  block_offset_ += kHeaderSize + length;  // header 大小 + 写入的数据的大小
  return s;
}

}  // namespace log
}  // namespace leveldb
