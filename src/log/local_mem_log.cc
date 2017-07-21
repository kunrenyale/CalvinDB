// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "log/local_mem_log.h"

#include <cstdlib>
#include <string>


using std::string;

class LocalMemLogReader : public Log::Reader {
 public:
  virtual ~LocalMemLogReader() {}
  virtual bool Valid();
  virtual void Reset();
  virtual bool Next();
  virtual bool Seek(uint64 target);
  virtual uint64 Version();
  virtual string Entry();

 private:
  friend class LocalMemLog;

  // Constructor called by LocalMemLog::GetReader();
  explicit LocalMemLogReader(LocalMemLog* log);

  // Log whose entries this reader exposes.
  LocalMemLog* log_;

  // False iff positioned before the first entry.
  bool started_;

  // Vector offset of log entry at which reader is currently positioned.
  uint64 offset_;

  // Copy of current entry.
  LocalMemLog::Entry entry_;

  // DISALLOW_COPY_AND_ASSIGN
  LocalMemLogReader(const LocalMemLogReader&);  // NOLINT
  LocalMemLogReader& operator=(const LocalMemLogReader&);
};

//////////////////////////      LocalMemLog      ///////////////////////////////

LocalMemLog::LocalMemLog() : max_version_(0), size_(0), allocated_(1024*1024) {
  entries_ = reinterpret_cast<LocalMemLog::Entry*>(malloc(allocated_ * sizeof(Entry)));
}

LocalMemLog::~LocalMemLog() {
  free(entries_);
}

void LocalMemLog::Append(uint64 version, const string& entry) {
  CHECK_GE(version, max_version_);

  // Resize if necessary.
  if (size_.load() >= allocated_) {
    CHECK_EQ(allocated_, size_.load());
    WriteLock l(&mutex_);
    allocated_ *= 2;
    entries_ = reinterpret_cast<LocalMemLog::Entry*>(
                        realloc(entries_, allocated_ * sizeof(Entry)));
  }

  entries_[size_.load()] = Entry(version, entry);
  size_++;
  max_version_ = version;
}

uint64 LocalMemLog::LastVersion() {
  return max_version_;
}

typename Log::Reader* LocalMemLog::GetReader() {
  return new LocalMemLogReader(this);
}

//////////////////////////   LocalMemLogReader   ///////////////////////////////

LocalMemLogReader::LocalMemLogReader(LocalMemLog* log)
    : log_(log), started_(false), offset_(0) {
}

bool LocalMemLogReader::Valid() {
  return started_;
}

void LocalMemLogReader::Reset() {
  started_ = false;
}

bool LocalMemLogReader::Next() {
  if (log_->size_.load() == 0 ||
      (started_ && offset_ == log_->size_.load() - 1)) {
    return false;
  }
  if (!started_) {
    started_ = true;
    offset_ = 0;
  } else {
    offset_++;
  }
  CHECK(offset_ < log_->size_.load());

  // Prevent resizing of array while reading entry.
  ReadLock l(&log_->mutex_);
  entry_ = log_->entries_[offset_];

  return true;
}

bool LocalMemLogReader::Seek(uint64 target) {
  CHECK(target > 0) << "seeking to invalid version 0";
  if (target > log_->LastVersion()) {
    return false;
  }

  // Use snapshot of size as of when seek was called.
  uint64 size = log_->size_.load();

  // Seek WILL succeed.
  started_ = true;

  // Prevent array resizing.
  ReadLock l(&log_->mutex_);

  // Okay, let's see if I remember how to implement binary search. =)
  uint64 min = 0;
  uint64 max = size - 1;
  LocalMemLog::Entry* e = log_->entries_;  // For brevity.

  // Check min and max so we can use strict inequalities in invariant.
  if (e[min].version == target) {
    offset_ = min;
    entry_ = e[offset_];
    return true;
  } else if (e[max].version == target) {
    offset_ = max;
    entry_ = e[offset_];
    return true;
  }

  // Invariant: e[min].version < target < e[max].version
  while (max > min + 1) {
    uint64 mid = (min + max) / 2;
    if (e[mid].version == target) {
      offset_ = mid;
      entry_ = e[offset_];
      return true;
    } else if (target < e[mid].version) {
      max = mid;
    } else {  // target > e[mid].version
      min = mid;
    }
  }

  // Just to be safe....
  CHECK(e[min].version < target);
  CHECK(e[max].version > target);

  offset_ = max;
  entry_ = e[offset_];
  return true;
}

uint64 LocalMemLogReader::Version() {
  CHECK(Valid()) << "version called on invalid LogReader";
  return entry_.version;
}

string LocalMemLogReader::Entry() {
  CHECK(Valid()) << "entry called on invalid LogReader";
  return *(entry_.entry);
}

