// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

Record* SimpleStorage::ReadObject(const Key& key) {
  CHECK(objects_.count(key) != 0);
  return objects_[key];
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

pair<uint32, uint64> SimpleStorage::GetMasterCounter(const Key& key) {
  CHECK(objects_.count(key) != 0);
  Record* record = objects_[key];
  return make_pair(record->master, record->counter);
}
