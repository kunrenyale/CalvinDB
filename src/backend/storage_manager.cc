// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "backend/storage_manager.h"

StorageManager::StorageManager(ClusterConfig* config, ConnectionMultiplexer* connection,
                               Storage* actual_storage, TxnProto* txn, uint32 mode)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), relative_node_id_(config->relative_node_id()), mode_(mode) {
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
    remote_result_message_.set_type(MessageProto::READ_RESULT);

    // Execute local reads.
    for (int i = 0; i < txn->read_set_size(); i++) {
      KeyEntry key_entry = txn->read_set(i);
      const Key& key = key_entry.key();
      uint64 mds = configuration_->LookupPartition(key);
      
      if (mode_ == 2) {
        involved_machines_.insert(mds);     
      }

      if (mode_ != 0 && key_entry.master() != origin) {
        continue;
      }

      if (mds == relative_node_id_) {
        Record* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        remote_result_message_.add_keys(key);
        remote_result_message_.add_values(val == NULL ? "" : val->value);

        // For remaster: collect local entries
        if (mode_ == 2) {
          KeyEntry* key_entry = local_entries_.add_entries();
          key_entry->set_key(key);
          key_entry->set_master(val->master);
          key_entry->set_counter(val->counter);
        }
      }
    }

    for (int i = 0; i < txn->read_write_set_size(); i++) {
      KeyEntry key_entry = txn->read_write_set(i);
      const Key& key = key_entry.key();
      uint64 mds = configuration_->LookupPartition(key);
      writers.insert(mds);
      
      if (mode_ == 2) {
        involved_machines_.insert(mds);     
      }

      uint32 replica_id = key_entry.master();
      if (mode_ != 0 && replica_id != origin) {
        remote_replica_writers_.insert(make_pair(mds, replica_id));
        continue;
      } else if (mode_ != 0 && mds != relative_node_id_) {
        remote_replica_writers_.insert(make_pair(mds, origin));
        continue;
      }

      if (mds == relative_node_id_) {
        Record* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        remote_result_message_.add_keys(key);
        remote_result_message_.add_values(val == NULL ? "" : val->value);

        // For remaster: collect local entries
        if (mode_ == 2) {
          KeyEntry* key_entry = local_entries_.add_entries();
          key_entry->set_key(key);
          key_entry->set_master(val->master);
          key_entry->set_counter(val->counter);
        }
      } 
    }

    if (mode_ == 0) {
      // Original CalvinDB: Broadcast local reads to (other) writers.
      remote_result_message_.set_destination_channel(IntToString(txn->txn_id()) + "-" + IntToString(origin));
      for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
        if (*it != relative_node_id_) {
          remote_result_message_.set_destination_node(configuration_->LookupMachineID(*it, configuration_->local_replica_id()));
          connection_->Send(remote_result_message_);
        }
      }
    } else if (mode_ == 1){
      // Basic request chopping: Broadcast local reads to (other) writers.
      for (auto remote_writer : remote_replica_writers_) {
        uint64 mds = remote_writer.first;
        uint64 replica = remote_writer.second;
        string destination_channel = IntToString(txn->txn_id()) + "-" + IntToString(replica);
        remote_result_message_.set_destination_channel(destination_channel);
        remote_result_message_.set_destination_node(configuration_->LookupMachineID(mds, configuration_->local_replica_id()));
        connection_->Send(remote_result_message_);        
      }
    } else {
      // Request chopping with remaster: do the one-phase commit protocol
      min_involved_machine_ = *(involved_machines_.begin());
      if (min_involved_machine_ != relative_node_id_) {
        // non-min machine: sent local entries to min machine
        uint64 machine_sent = configuration_->LookupMachineID(min_involved_machine_, local_replica_id_);

        local_key_entries_message_.set_type(MessageProto::LOCAL_ENTRIES_TO_MIM_MACHINE);
        local_key_entries_message_.set_destination_channel(destination_channel); // TODO:
        local_key_entries_message_.set_destination_node(machine_sent);

        string local_entries_string;
        local_entries_.SerializeToString(&local_entries_string);
        local_key_entries_message_.add_data(local_entries_string);
        connection_->Send(local_key_entries_message_);   
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
//LOG(ERROR) <<configuration_->local_node_id()<< ":^^^^^^^^^ In StorageManager: bojects size is:  "<<static_cast<int>(objects_.size());
  return static_cast<int>(objects_.size()) == txn_->read_set_size() + txn_->read_write_set_size();
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

