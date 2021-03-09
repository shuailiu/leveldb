// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }
// log reader/writer 都是在当前工作为在开始，至于是不是文件真正的起始位置，并不关心
// 所以 initial_offset_ 设定时需要从SEEK_CUR进行读取位置偏移
bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  // 如果给定的初始位置的块中偏移，刚好掉在了尾巴上的6个bytes以内。
  // 那么，应该是需要直接切入到下一个block的
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;  // TODO：加上一个完整的kBlockSize，
                                         // 不还是会调入下一个block的尾巴上吗？
  }
  // 注意end_of_buffer_offset的设置是块的开始地址
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // TODO: ？？？？？？
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }
  // 当文件中的record是<firstRecord, middleRecord, lastRecord>的时候。
  // scratch需要做一个缓冲区，把一个一个record的数据缓存起来。最后拼接成一个大的
  // Slice返回给客户端
  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;  // 表示当前不是在处理 “kMiddleType”
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;  // 记录下最后处理的一个record的
                                           // 偏移量（缓冲区中的偏移）

  // skip的时候，跳过的都是block的整数倍。但是有可能存在这种较大的Slice。一个slice就是
  // N个block。有可能跳过block之后，还是处在slice的中间。这个时候，也是读不了一个完整
  // 的slice数据。那么，需要判断header类型，保证后面读一个完整的slice数据

  // 正确的记录应该如下：
  // <firstRecord, lastRecord>
  // <firstRecord, middleRecord（1个或多个）, lastRecord>
  // <fullRecord>

  Slice fragment;
  while (true) {
    // 这里是读一个物理上的record。并不是一个完整的slice信息
    // 一个block上可能有多个record（header+data），一个完整的slice可能在
    // 写入时被分成了多个record写入block
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.

    // 这里记录下读入的物理record的起始位置
    // ReadPhysicalRecord读取了32kb的block到buffer_
    // physical_record_offset记录的是一个物理record的偏移量，不是在文件里面的
    // 偏移量，而是在当前这个Reader里面的偏移量，那么这个物理偏移量可能指向的位置是：
    // <firstRecord, middleRecord, lastRecord>
    // 1            2             3

    // 这里记录下读入的物理record的起始位置
    // Read后，end_of_buffer_offset_ += buffer_.size();
    // 但是这里end_of_buffer_offset_ - buffer_.size()
    // 减了之后，减去的不是刚读出来的数据块的大小。比如32KB。
    // 这个时候的buffer_size.指的是未读的record的数据大小

    // end_of_buffer_offset_ - buffer_.size： 该次读取完的record在log文件中结束
    // 偏移位置physical_record_offset 是该次读取的record在log文件中的起始偏移

    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }
    // 到这里的时候，读取的就是一个完整的slice的开头了。
    // 所以这里才开始正常的处理。

    // 只有读完<firstRecord, middleRecord, lastRecord> 或者<fullRecord>后，
    // 才会设置last_record_offset_
    switch (record_type) {
      case kFullType:
      // 如果处理“读在中间”的状态。应变报错。
        if (in_fragmented_record) {  // in_fragmented_record初始值为false
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        // scratch就是用来缓存<firstRecord, middleRecord, lastRecord>
        // 不断地把这些record的数据区放到scratch里面缓存并且拼接起来。
        // 如果读到的是一个full type的record，不需要拼接
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        // 记录下最后一个record的偏移量
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        // “读在中间”的状态也不应该遇到kFirstType，也就是不应该读到
        // 下一个record的开头。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        // 将first record行道scratch
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        // 当遇到middle type的时候。必然是“读在中间”状态。如果不是，报错
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          // scratch拼接读出来的record
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        // 读到lastType的时候，也必然是处在“读在中间”的状态。如果不是，报错
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          // 将last record放入record完成全部拼接
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);  // 完成全部拼接后生成Slice返回
          // 记录下最后一个record的偏移量
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        // 文件都读结束了，还处在“读在中间”状态。说明写入的时候没有写入一个完整的record
        // 直接向客户端返回没有完整的slice数据
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        // 如果读到了坏的record，又刚好处理“读在中间”的状态。那么返回出错
        // 如果这个坏掉的record不是在读的record范围里面。直接返回读失败
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}
// 读取一个物理block
// 写入的时是按照32KB一个block来写入，所以在读入的时候，肯定是以32KB为单位来读的
// 这里是读一个物理上的record。并不是一个完整的slice信息
// 一个block上可能有多个record（header+data），一个完整的slice可能在写入时
// 被分成了多个record写入block
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // 如果发现buffer的数据大小已经小于kHeaderSize了
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        // 如果还没有遇到结束
        // 上一次的读是一个完整的读。那么buffer_的大小应该是kBlockSize
        buffer_.clear();
        // 这里是读kBlockSize个字符串到数组backing_store_中，buffer_中
        // 的data_指向backing_store_，这里虽然是读取kBlockSize个字节，但是真正
        // 读取的可能小于kBlockSize，所以buffer_中data_的大小可能小于kBlockSize
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          // 上面Read了kBlockSize个字节到buffer_，如果没有到eof，那么buffer_的
          // 大小应该是kBlockSize
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }
    // 如果buffer_.size() >= kHeaderSize，说明上次读取到buffer_中的数据还
    // 没有处理完，即buffer_中还存在record
    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    // 如果头部记录的数据长度比实际的buffer_.size还要大。那肯定是出错了
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }
    // 如果是zero type。那么返回Bad Record
    // 这种情况是有可能的。比如写入record到block里面之后。可能会遇到
    // 还余下7个bytes的情况。这个时候只能写入一个空的record
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    // 移除头部
    buffer_.remove_prefix(kHeaderSize + length);  // data_ += n; size_ -= n;

    // Skip physical record that started before initial_offset_
    // Read()..之后有end_of_buffer_offset_ += buffer_.size();
    // 而上面buffer_通过remove_prefix，data_和size_都去掉了当前需要读取的
    // record（header+data），所以这里end_of_buffer_offset_ - buffer_.size()，
    // 减去的不是刚读出来的数据块的大小（比如32KB），这个时候的buffer_.size()
    // 指的是待处理的record的数据大小。

    // end_of_buffer_offset_ - buffer_.size()就是已经读取完的一个
    // record（header+data）在log中的结束偏移位置

    // end_of_buffer_offset_
    //      - buffer_.size()
    //     - kHeaderSize
    //     - length
    // 这里得到的就是刚读出来的record在log文件中的起始偏移位置

    // Read从log文件中读取32k书记到buffer_，这时buffer_中可能有
    // 多个record（header+data）
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
