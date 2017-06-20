// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// The Storage class provides an interface for writing and accessing data
// objects stored by the system.

#ifndef _DB_BACKEND_STORAGE_H_
#define _DB_BACKEND_STORAGE_H_

#include <vector>

#include "common/types.h"

using std::vector;

struct Record {
  Record(Value v, uint32 m) : value(v), master(m), counter(0) {}

  // The actual value
  Value value;
  uint32 master;
  uint32 counter;
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

};

#endif  // _DB_BACKEND_STORAGE_H_

