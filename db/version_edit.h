// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// FileMetaData记录了每个sstable文件的信息
struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}
  // 该文件应用次数
  int refs;
  // 表示最多容忍seek miss多少次（查询时，在level-n没有查询到，
  // 只能到level-n+1中查询，因为n和n+1可能存在key范围重叠）
  int allowed_seeks;  // Seeks allowed until compaction
  // 该文件的编号
  uint64_t number;
  // 文件大小
  uint64_t file_size;    // File size in bytes
  // 该文件中的最小key
  InternalKey smallest;  // Smallest internal key served by table
  // 该文件中的最大key
  InternalKey largest;   // Largest internal key served by table
};

// VersionEdit：它记录的是两个Version之间的差异，编辑或修改Version
// 简单说：Version0 + VersionEdit = Version1
// 注意，从一个版本到到另一个版本的过渡，是由Compaction引起的
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;  //level-filenumber

  std::string comparator_;        // 比较器
  uint64_t log_number_;           // log文件的序号
  uint64_t prev_log_number_;      // 前一个WAL日志文件的编号
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;  // 用户insert key/value的时候，会使用的seq
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;  // 保存要被删除的文件的 level 和 filenumber
  std::vector<std::pair<int, FileMetaData>> new_files_;  //新生成的sstable文件的
                                                         //level和FileMetaData

  // Minor Compaction：用户输入的key－value足够多了，需要讲memtable转成immutable memtable，
  // 然后讲immutable memtable dump成 sstable，而这种情况下，sstable文件多了一个，而能属于level0，
  // 也能属于level1或者level2。这种情况下，我们成为version发生了变化，需要升级版本。
  // 这种情况比较简单，基本上是新增一个文件new_files_。

  // Major Compaction 要复杂一些，它牵扯到两个Level的文件。它会计算出重叠部分的文件，然后归并排序，
  // merge成新的sstable文件，一旦新的文件merge完毕，老的文件也就没啥用了。
  // 因此对于这种Compaction除了new_files_还有deleted_files_（当然还有compaction_pointers_）

  // minor compaction: new_files_
  // major compaction: delete_files_、new_files_、compact_pointers_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
