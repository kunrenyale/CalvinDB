// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "backend/storage_manager.h"

StorageManager::StorageManager(ClusterConfig* config, ConnectionMultiplexer* connection,
                               Storage* actual_storage, TxnProto* txn, uint32 mode)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), relative_node_id_(config->relative_node_id()), mode_(mode) {
  MessageProto message;

  local_replica_id_ = config->local_replica_id();

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
  set<uint64> writers;
  uint32 origin = txn->origin_replica();

  bool reader = false;
  for (int i = 0; i < txn->readers_size(); i++) {
    if (txn->readers(i) == relative_node_id_)
      reader = true;
  }

  if (reader) {
    message.set_destination_channel(IntToString(txn->txn_id()));
    message.set_type(MessageProto::READ_RESULT);

    // Execute local reads.
    for (int i = 0; i < txn->read_set_size(); i++) {
      const Key& key = txn->read_set(i);
      uint64 mds = configuration_->LookupPartition(key);

      if (mode_ == 1 && configuration_->LookupMaster(key) != origin) {
LOG(ERROR) <<configuration_->local_node_id()<< ":!!!!!! In StorageManager: wrong  "<<txn->txn_id();
        continue;
      }

      if (mds == relative_node_id_) {
        Record* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : val->value);
      }
    }

    for (int i = 0; i < txn->read_write_set_size(); i++) {
      const Key& key = txn->read_write_set(i);
      uint64 mds = configuration_->LookupPartition(key);
      writers.insert(mds);

      if (mode_ == 1 && configuration_->LookupMaster(key) != origin) {
LOG(ERROR) <<configuration_->local_node_id()<< ":!!!!!! In StorageManager: wrong  "<<txn->txn_id();
        continue;
      }

      if (mds == relative_node_id_) {
        Record* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : val->value);
      } else {
LOG(ERROR) <<configuration_->local_node_id()<< ":!!!!!! In StorageManager: wrong  "<<key;
}
    }

    // Broadcast local reads to (other) writers.
    for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
      if (*it != relative_node_id_) {
        message.set_destination_node(configuration_->LookupMachineID(*it, configuration_->local_replica_id()));
        connection_->Send(message);
      }
    }

  }

  // Note whether this node is a writer. If not, no need to do anything further.
  writer = false;
  for (int i = 0; i < txn->writers_size(); i++) {
    if (txn->writers(i) == relative_node_id_)
      writer = true;
  }

  // Scheduler is responsible for calling HandleReadResponse. We're done here.
}

void StorageManager::HandleReadResult(const MessageProto& message) {
  CHECK(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Record* val = new Record(message.values(i), local_replica_id_);
    objects_[message.keys(i)] = val;
    remote_reads_.push_back(val);
  }
}

bool StorageManager::ReadyToExecute() {
LOG(ERROR) <<configuration_->local_node_id()<< ":^^^^^^^^^ In StorageManager: bojects size is:  "<<static_cast<int>(objects_.size());
  return static_cast<int>(objects_.size()) ==
         txn_->read_set_size() + txn_->read_write_set_size();
}

StorageManager::~StorageManager() {
  for (vector<Record*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it) {
    delete *it;
  }
}

Record* StorageManager::ReadObject(const Key& key) {
  return objects_[key];
}

bool StorageManager::PutObject(const Key& key, Record* value) {
  // Write object to storage if applicable.
  if (configuration_->LookupPartition(key) == relative_node_id_) {
    return actual_storage_->PutObject(key, value);
  } else {
    return true;  // Not this node's problem.
  }
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == relative_node_id_) {
    return actual_storage_->DeleteObject(key);
  } else {
    return true;  // Not this node's problem.
  }
}

