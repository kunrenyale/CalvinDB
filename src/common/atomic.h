// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun Ren <kun@cs.yale.edu>
//
//

#ifndef CALVIN_COMMON_ATOMIC_H_
#define CALVIN_COMMON_ATOMIC_H_

#include <map>
#include <queue>
#include <vector>
#include <tr1/unordered_map>
#include "btree/btree_map.h"
#include "common/mutex.h"
#include "common/types.h"
#include "common/utils.h"

using btree::btree_map;
using std::map;
using std::pair;
using std::queue;
using std::vector;
using std::tr1::unordered_map;

// Queue with atomic push and pop operations. One Push and one Pop can occur
// concurrently, so consumers will not block producers, and producers will not
// block consumers.
//
// Note: Elements are not guaranteed to have their destructors called
//       immeduately upon removal from the queue.
//
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

// TODO(agt): This could be implemented with fewer mutexes....
template<typename T>
class DelayQueue {
 public:
  DelayQueue() : delay_(0) {}
  explicit DelayQueue(double delay) : delay_(delay) {}

  inline void Push(const T& item) {
    queue_.Push(make_pair(item, GetTime()));
  }
  inline bool Pop(T* result) {
    pair<T, double> t;
    Lock l(&mutex_);
    if (queue_.Front(&t) && GetTime() > t.second + delay_) {
      queue_.Pop(&t);
      *result = t.first;
      return true;
    }
    return false;
  }

 private:
  double delay_;
  AtomicQueue<pair<T, double> > queue_;
  Mutex mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  DelayQueue(const DelayQueue<T>&);
  DelayQueue& operator=(const DelayQueue<T>&);
};

/**template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline bool Lookup(const K& k, V* v) {
    ReadLock l(&mutex_);
    typename btree_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup == map_.end()) {
      return false;
    }
    *v = lookup->second;
    return true;
  }

  inline void Put(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_.insert(std::make_pair(k, v));
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
    typename btree_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup != map_.end()) {
      return lookup->second;
    }
    map_.insert(std::make_pair(k, v));
    return v;
  }

  inline uint32 Size() {
    ReadLock l(&mutex_);
    return map_.size();
  }

 private:
  btree_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};**/

template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline bool Lookup(const K& k, V* v) {
    ReadLock l(&mutex_);
    if (map_.count(k) == 0) {
      return false;
    } else {
      *v = map_[k];
      return true;
    }
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

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};

#endif  // CALVIN_COMMON_ATOMIC_H_

