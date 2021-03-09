// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;   // 数据块
  BlockBuilder index_block;  // 索引块
  std::string last_key;      // 最近一次添加的key
  int64_t num_entries;       // 对Add函数的调用次数（用于判断该data block是否
                             // 添加过key-value）
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;  // 当data block是空时为true，用于判断时候是第一才
                             // 向该data block添加key-value
  BlockHandle pending_handle;  // Handle to add to index block
                               // 指向存储索引块的文件

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}
// 向 ldb 文件写入key-value的入口
// 根据比较器，key在之前添加的任何key之后
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // num_entries是对Add的调用次数，如果num_entries>0，表示添加过key-value
  // leveldb对key排序后添加，那么后添加的key肯定大于上次最近一次添加的key：last_key
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  // pending_index_entry 标志位用来判定是不是data block的第一个key
  // 因此在上一个data block Flush的时候，会将该标志位置位
  // 当下一个data block第一个key-value到来后，成功往index block插入分割key之后，
  // 就会清零
  // 1.如果是第一次向当前data block添加key-value，那么需要向index block中添加
  //   该data block对应的index
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 根据前一个data block中添加的最后一个key，寻找分割key
    // 例如，last_key = "helloleveldb"，当前待添加的key = "helloworld"
    // 那么分割key = "hellom"，这里的m是l后的第一个字母
    // 那么小于分割key的键都在上一个data block中，大于分割key的可能在当前data block

    // 找个分割key，保存到r->last_key中
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 写入上个data block的offset_和size_
    r->pending_handle.EncodeTo(&handle_encoding);
    // 将key=分割key，value=offset_(varint64)+size_(varint64) 写入index block
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;  // 表示该block不是第一个写入数据了
    // 写入的Index Block内容之后，记录了Index Block在sstable中的offset和size，
    // 存放在index_block_handle中，然后将index_block_handle记录在footer中，由此，
    // 就可以完成查询过程：从footer，可以找到index block，而index存放的
    // 是{“分割key”：data block的位置信息}，
    // 所以根据index block可以找到任何一个data block的起始位置和结束位置
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }
  // 更新last_key
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  // 2.向data block中添加一组key-value
  r->data_block.Add(key, value);
  // 3.估算当前data block的长度，如果超过了阈值，就要Flush，追加到ldb文件中
  // CurrentSizeEstimate：获取该data block的完整大小：N个Entry的大小 + M个
  // Researt Point的大小 + 1个Restart Point Count的大小
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}
// 将任何缓冲的键/值对刷新到文件
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();  // 调用write函数
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);  // 计算boom filter的为图
  }
}

// 获取一个Slice，根据配置决定是否压缩处理
// BlockHandle是指向存储data block或meta block的文件范围的指针
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 获取一个完整的data block：
  //    [Entry 1]
  //    ...
  //    [Enety N]
  //    [Restart Point 1]
  //    ...
  //    [Restart Point M]
  //    Restart Point Count
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}
// 将处理后（是否雅思）的Slice，最终写入ldb文件
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }
// 落盘
// 完成table的构建。 停止使用传递给的文件
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();  // 写入尚未Flush的Block块
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 1.写入filter_block块，即图中的meta block
  if (ok() && r->filter_block != nullptr) {
    // filter_block->Finish()： 计算boom filter中的位图
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // 2.Write metaindex block
  // metaindex_block组织形式和data_block相同，key值是filter.leveldb.BuiltinBloomFilter2，
  // 而value是meta_data的偏移offset_和大小size_
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);  // 写入offset_和size_
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // 3.Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // 4.Write footer
  // footer为固定长度，在文件的最尾部
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    // 将metaindex、index、padding、magic写入footer_encoding
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}
// 到目前为止，对Add（）的调用次数
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }
// 到目前为止生成的文件大小。 如果成功后调用Finish（）调用，返回最终生成的文件的大小
uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
