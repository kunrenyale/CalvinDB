// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

Record* SimpleStorage::ReadObject(const Key& key) {
  if (objects_.count(key) != 0) {
    return objects_[key];
  } else {
    return NULL;
  }
}

bool SimpleStorage::PutObject(const Key& key, Record* record) {
  Lock l(&mutex_);
  objects_[key] = record;
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key) {
  Lock l(&mutex_);
  objects_.erase(key);
  return true;
}

