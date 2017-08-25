// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// The Storage class provides an interface for writing and accessing data
// objects stored by the system.

#ifndef _DB_BACKEND_STORAGE_H_
#define _DB_BACKEND_STORAGE_H_

#include <vector>
#include <utility> 
#include "common/types.h"

#define REPLICA_SIZE 3
#define LAST_N_TOUCH 100
#define ACCESS_PATTERN_THRESHOLD  0.80

using std::vector;
using std::pair;
using std::make_pair;

struct Record {
  Record(Value v, uint32 m) : value(v), master(m), counter(0) {
    for (uint32 i = 0; i < REPLICA_SIZE; i++) {
      access_pattern[i] = 0;
    }
    remastering = false;
    access_cnt = 0;
  }

  // The actual value
  Value value;
  uint32 master;
  uint64 counter;

  // access pattern
  uint32 access_pattern[REPLICA_SIZE];
  bool remastering;
  uint32 access_cnt;

};


class Storage {
 public:
  virtual ~Storage() {}

  // If the object specified by 'key' exists, copies the object into '*result'
  // and returns true. If the object does not exist, false is returned.
  virtual Record* ReadObject(const Key& key) = 0;

  // Sets the object specified by 'key' equal to 'value'. Any previous version
  // of the object is replaced. Returns true if the write succeeds, or false if
  // it fails for any reason.
  virtual bool PutObject(const Key& key, Record* record) = 0;

  // Removes the object specified by 'key' if there is one. Returns true if the
  // deletion succeeds (or if no object is found with the specified key), or
  // false if it fails for any reason.
  virtual bool DeleteObject(const Key& key) = 0;

  virtual pair<uint32, uint64> GetMasterCounter(const Key& key) = 0;

};

#endif  // _DB_BACKEND_STORAGE_H_

