// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

// User:              [Slice key]
// Internal:          [Slice key] + [seq_num << 8 | type](固定8个字节大小)
// Memtalbe:

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  // internal_key = user_key + [seq_num << 8 | type](固定的8个字节大小)
  size_t internal_key_size = key_size + 8;
  // 计算：保存internal_key的大小需要占用的字节数 + internal_key字节数 +
  //      保存value的大小需要占用的字节数 + value占用的字节数量
  // VarintLength： 需要使用几个varint来表示（varint最高位是标志位，
  //                因此一个字节最大表示2^7=128）
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  // 申请一块内存用于存放: internal_key_size + internal_key + value_size + value
  char* buf = arena_.Allocate(encoded_len);
  // 将internal_key_size按照varint的方式，写入buf中。
  // buf： [internal_key_size(varint)]
  char* p = EncodeVarint32(buf, internal_key_size);
  // buf中先写入key_size后，然后把key的值写入。
  // buf：[internal_key_size(varint)] + [key]
  memcpy(p, key.data(), key_size);
  p += key_size;
  // 将固定的8字节序列号（seq_num << 8 | type）写入buf。
  // buf：[internal_key_size(varint)] + [key] + [seq_num(8字节)]
  EncodeFixed64(p, (s << 8) | type);
  // 写入变长的value_size。buf：
  // [internal_key_size(varint)] + [key] + [seq_num(8字节)] +
  //  [vlaue_size(varint)]
  p += 8;
  p = EncodeVarint32(p, val_size);
  // 写入value的值到buf。buf：
  // [internal_key_size(varint)] + [key] + [seq_num(8字节)] +
  //  [vlaue_size(varint)]+[value]
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);  // table_: SkipList
  iter.Seek(memkey.data());       // skiplist中的FindGreaterOrEqual
  if (iter.Valid()) {             // iter != nullptr
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    // 从前5个字节中获取internal_key_size
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    // TODO: 上面key_ptr指向entry向后移动一个字节的位置？
    //       为什么是1个字节，不应该是varint(inter_key_size)个字节？
    if (comparator_.comparator.user_comparator()->Compare(
            // key_length-8是因为key_length包含了：user_key + seq_num(固定8字节)
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      // type（最后一个字节是type）
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {  // 0000 0000 .... 1111 1111
        // 如果找到了，但是该key已经打上了删除tag，那么设置状态NotFound
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
