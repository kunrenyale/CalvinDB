// Author Alexander Thomson <thomson@cs.yale.edu>
// Author Thaddeus Diamond <diamond@cs.yale.edu>
//
// Some miscellaneous commonly-used utility functions.
//
// TODO(alex): Organize these into reasonable categories, etc.
// TODO(alex): MORE/BETTER UNIT TESTING!

#ifndef CALVIN_COMMON_UTILS_H_
#define CALVIN_COMMON_UTILS_H_

#include <glog/logging.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string>
#include <vector>
#include <tr1/unordered_map>

#include "common/types.h"
#include "common/mutex.h"

using std::string;
using std::vector;
using std::tr1::unordered_map;

template<typename T> string TypeName();
#define ADD_TYPE_NAME(T) \
template<> string TypeName<T>() { return #T; }

// Splits a string on 'delimiter'.
vector<string> SplitString(const string& input, char delimiter);

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
double GetTime();

// The FNV-1 and FNV-1a hashes as described by Fowler-Noll-Vo.
uint32 FNVHash(const string& key);
uint32 FNVModHash(const string& key);

// Busy-wait (or yield) for 'duration' seconds.
void Spin(double duration);

// Busy-wait (or yield) until GetTime() >= 'time'.
void SpinUntil(double time);

// Produces a random alphabetic string of 'length' characters.
string RandomString(int length);

// Random byte sequence. Any byte may appear.
string RandomBytes(int length);

// Random byte sequence. Any byte except '\0' may appear.
string RandomBytesNoZeros(int length);

double RandomGaussian(double s);

// Returns human-readable numeric string representation of an (u)int{32,64}.
string Int32ToString(int32 n);
string Int64ToString(int64 n);
string UInt32ToString(uint32 n);
string UInt64ToString(uint64 n);
string DoubleToString(double n);

string IntToString(int n);

// Converts a human-readable numeric string to an (u)int{32,64}. Dies on bad
// inputs.
int StringToInt(const string& s);

static inline void DeleteString(void* data, void* hint) {
  delete reinterpret_cast<string*>(hint);
}

// Used for fooling the g++ optimizer.
template<typename T>
void SpinUntilNE(T& t, const T& v);


template<typename T>
class AtomicQueue {
 public:
  AtomicQueue() {
    queue_.resize(256);
    size_ = 256;
    front_ = 0;
    back_ = 0;
  }

  // Returns the number of elements currently in the queue.
  inline size_t Size() {
    Lock l(&size_mutex_);
    return (back_ + size_ - front_) % size_;
  }

  // Returns true iff the queue is empty.
  inline bool Empty() {
    return front_ == back_;
  }

  // Atomically pushes 'item' onto the queue.
  inline void Push(const T& item) {
    Lock l(&back_mutex_);
    // Check if the buffer has filled up. Acquire all locks and resize if so.
    if (front_ == (back_+1) % size_) {
      Lock m(&front_mutex_);
      Lock n(&size_mutex_);
      uint32 count = (back_ + size_ - front_) % size_;
      queue_.resize(size_ * 2);
      for (uint32 i = 0; i < count; i++) {
        queue_[size_+i] = queue_[(front_ + i) % size_];
      }
      front_ = size_;
      back_ = size_ + count;
      size_ *= 2;
    }
    // Push item to back of queue.
    queue_[back_] = item;
    back_ = (back_ + 1) % size_;
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  inline bool Pop(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      front_ = (front_ + 1) % size_;
      return true;
    }
    return false;
  }

  // Sets *result equal to the front element and returns true, unless the
  // queue is empty, in which case does nothing and returns false.
  inline bool Front(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      return true;
    }
    return false;
  }

 private:
  vector<T> queue_;  // Circular buffer containing elements.
  uint32 size_;      // Allocated size of queue_, not number of elements.
  uint32 front_;     // Offset of first (oldest) element.
  uint32 back_;      // First offset following all elements.

  // Mutexes for synchronization.
  Mutex front_mutex_;
  Mutex back_mutex_;
  Mutex size_mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicQueue(const AtomicQueue<T>&);
  AtomicQueue& operator=(const AtomicQueue<T>&);
};

template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline V Lookup(const K& k) {
    ReadLock l(&mutex_);
    //CHECK(map_.count(k) > 0);
    return map_[k];
  }

  inline void Put(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_[k] = v;
  }

  inline void EraseAndPut(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_.erase(k);
    map_[k] = v;
  }

  inline void Erase(const K& k) {
    WriteLock l(&mutex_);
    map_.erase(k);
  }

  // Puts (k, v) if there is no record for k. Returns the value of v that is
  // associated with k afterwards (either the inserted value or the one that
  // was there already).
  inline V PutNoClobber(const K& k, const V& v) {
    WriteLock l(&mutex_);
    if (map_.count(k) != 0) {
      return map_[k];
    } else {
      map_[k] = v;
      return v;
    }
  }

  inline uint32 Size() {
    ReadLock l(&mutex_);
    return map_.size();
  }

  inline uint32 Count(const K& k) {
    ReadLock l(&mutex_);
    return map_.count(k);
  }

  inline void Destroy() {
    WriteLock l(&mutex_);
    for (auto it = map_.begin(); it != map_.end(); ++it) {
      if (it->second == NULL) {
        delete it->second;
      }
      Erase(it->first);
    }
  }

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};

#endif  // CALVIN_COMMON_UTILS_H_

