// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A simple implementation of the storage interface using an stl map.

#ifndef _DB_BACKEND_SIMPLE_STORAGE_H_
#define _DB_BACKEND_SIMPLE_STORAGE_H_

#include <tr1/unordered_map>
#include "common/utils.h"
#include "common/mutex.h"
#include "backend/storage.h"
#include "common/types.h"
#include <pthread.h>

using std::tr1::unordered_map;

class SimpleStorage : public Storage {
 public:
  virtual ~SimpleStorage() {}

  virtual Record* ReadObject(const Key& key);
  virtual bool PutObject(const Key& key, Record* record);
  virtual bool DeleteObject(const Key& key);

 private:
  unordered_map<Key, Record*> objects_;
  Mutex mutex_;

};
#endif  // _DB_BACKEND_SIMPLE_STORAGE_H_

