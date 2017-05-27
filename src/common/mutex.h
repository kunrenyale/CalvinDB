// Author: Alex Thomson (thomson@cs.yale.edu)
//
// Mutex is currently a thin wrapper around pthread_mutex_t, but this may
// change in the future, so please use this Mutex class rather than
// pthread_mutex_t for critical sections in Calvin.
//
// Standard usage idiom:
//
//    Mutex m;                // create a new mutex
//
//    void DoSomething() {
//      Lock l(&m);           // creating a 'Lock' object locks the mutex
//      <do stuff>
//    }                       // when the Lock object goes out of scope and is
//                            // deallocated, the mutex is automatically
//                            // unlocked
//

#ifndef CALVIN_COMMON_MUTEX_H_
#define CALVIN_COMMON_MUTEX_H_

#include <pthread.h>
#include <glog/logging.h>

class Mutex {
 public:
  // Mutexes come into the world unlocked.
  Mutex() {
    pthread_mutex_init(&mutex_, NULL);
  }

 private:
  friend class Lock;
  // Actual pthread mutex wrapped by Mutex class.
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);
};

class Lock {
 public:
  explicit Lock(Mutex* mutex) : mutex_(mutex) {
    pthread_mutex_lock(&mutex_->mutex_);
  }
  ~Lock() {
    pthread_mutex_unlock(&mutex_->mutex_);
  }

 private:
  Mutex* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  Lock();

  // DISALLOW_COPY_AND_ASSIGN
  Lock(const Lock&);
  Lock& operator=(const Lock&);
};

class MutexRW {
 public:
  // Mutexes come into the world unlocked.
  MutexRW() {
    pthread_rwlock_init(&mutex_, NULL);
  }

 private:
  friend class ReadLock;
  friend class WriteLock;
  // Actual pthread rwlock wrapped by MutexRW class.
  pthread_rwlock_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  MutexRW(const MutexRW&);
  MutexRW& operator=(const MutexRW&);
};

class ReadLock {
 public:
  explicit ReadLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_rdlock(&mutex_->mutex_);
  }
  ~ReadLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  ReadLock();

  // DISALLOW_COPY_AND_ASSIGN
  ReadLock(const ReadLock&);
  ReadLock& operator=(const ReadLock&);
};

class WriteLock {
 public:
  explicit WriteLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_wrlock(&mutex_->mutex_);
  }
  ~WriteLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  WriteLock();

  // DISALLOW_COPY_AND_ASSIGN
  WriteLock(const WriteLock&);
  WriteLock& operator=(const WriteLock&);
};

#endif  // CALVIN_COMMON_MUTEX_H_

