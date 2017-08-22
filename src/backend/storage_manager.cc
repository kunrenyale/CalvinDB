// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "backend/storage_manager.h"
#include <algorithm> 

// Compares two pairs
bool ComparePair(pair<uint64, uint32> p1, pair<uint64, uint32> p2)
{  
    if (p1.first != p2.first) {
      return (p1.first - p2.first);
    } else {
      return (p1.second - p2.second);
    }
}

StorageManager::StorageManager(ClusterConfig* config, ConnectionMultiplexer* connection,
                               Storage* actual_storage, TxnProto* txn, uint32 mode)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), mode_(mode), relative_node_id_(config->relative_node_id()){
  local_replica_id_ = config->local_replica_id();

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
  set<uint64> writers;
  txn_origin_replica_ = txn->origin_replica();
  commit_ = true;

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
        involved_machines_.push_back(make_pair(mds, key_entry.master()));     
      }

      if (mode_ != 0 && key_entry.master() != txn_origin_replica_) {
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
        involved_machines_.push_back(make_pair(mds, key_entry.master())); 
      }

      uint32 replica_id = key_entry.master();
      if (mode_ != 0 && replica_id != txn_origin_replica_) {
        remote_replica_writers_.insert(make_pair(mds, replica_id));
        continue;
      } else if (mode_ != 0 && mds != relative_node_id_) {
        remote_replica_writers_.insert(make_pair(mds, txn_origin_replica_));
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
      remote_result_message_.set_destination_channel(IntToString(txn->txn_id()) + "-" + IntToString(txn_origin_replica_));
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
      sort(involved_machines_.begin(), involved_machines_.end(), ComparePair);
      
      min_involved_machine_ = (involved_machines_.begin())->first;
      min_involved_machine_origin_ = (involved_machines_.begin())->second;

      if (!(min_involved_machine_ == relative_node_id_ && min_involved_machine_origin_ == txn_origin_replica_)) {
        // non-min machine: sent local entries to min machine
        uint64 machine_sent = configuration_->LookupMachineID(min_involved_machine_, local_replica_id_);

        local_key_entries_message_.set_type(MessageProto::LOCAL_ENTRIES_TO_MIN_MACHINE);

        string destination_channel = IntToString(txn->txn_id()) + "-" + IntToString(min_involved_machine_origin_);
        local_key_entries_message_.set_destination_channel(destination_channel);
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

void StorageManager::HandleRemoteEntries(const MessageProto& message) {
  CHECK(message.type() == MessageProto::LOCAL_ENTRIES_TO_MIN_MACHINE);

  KeyEntries remote_entries;
  remote_entries.ParseFromString(message.data(0));
  for (uint32 i = 0; i < (uint32)remote_entries.entries_size(); i++) {
    KeyEntry* key_entry = local_entries_.add_entries();
    key_entry->set_key(remote_entries.entries(i).key());
    key_entry->set_master(remote_entries.entries(i).master());
    key_entry->set_counter(remote_entries.entries(i).counter());
  }

  if (local_entries_.entries_size() == txn_->read_set_size() + txn_->read_write_set_size()) {
    // Received all remote entries. check whether commit the txn or abort it
    set<uint32> involved_replicas;
    for (uint32 i = 0; i < (uint32)local_entries_.entries_size(); i++) {
      KeyEntry key_entry = local_entries_.entries(i);
      records_in_storege_[key_entry.key()] = make_pair(key_entry.master(), key_entry.counter());
      involved_replicas.insert(key_entry.master());
    }

    for (int i = 0; i < txn_->read_set_size(); i++) {
      KeyEntry key_entry = txn_->read_set(i);
      pair<uint32, uint64> map_counter = records_in_storege_[key_entry.key()];
      if (key_entry.master() != map_counter.first || key_entry.counter() != map_counter.second) {
        commit_ = false;
   
        // update to the latest master/counter
        txn_->mutable_read_set(i)->set_master(map_counter.first);
        txn_->mutable_read_set(i)->set_counter(map_counter.second);
      }
    }

    for (int i = 0; i < txn_->read_write_set_size(); i++) {
      KeyEntry key_entry = txn_->read_write_set(i);
      pair<uint32, uint64> map_counter = records_in_storege_[key_entry.key()];
      if (key_entry.master() != map_counter.first || key_entry.counter() != map_counter.second) {
        commit_ = false;
   
        // update to the latest master/counter
        txn_->mutable_read_write_set(i)->set_master(map_counter.first);
        txn_->mutable_read_write_set(i)->set_counter(map_counter.second);
      }     
    }
    
    //send decisions to all non-min machines; 
    MessageProto abort_or_commit_decision_message_;
    abort_or_commit_decision_message_.set_type(MessageProto::COMMIT_OR_ABORT_DECISION);
    for (auto machine : involved_machines_) {
      if (machine.first == relative_node_id_ && machine.second == txn_origin_replica_) {
        continue;
      }

      uint64 machine_sent = configuration_->LookupMachineID(machine.first, local_replica_id_);
      string destination_channel = IntToString(txn_->txn_id()) + "-" + IntToString(machine.second);
      abort_or_commit_decision_message_.set_destination_channel(destination_channel);
      abort_or_commit_decision_message_.set_destination_node(machine_sent);
      abort_or_commit_decision_message_.clear_misc_bool();
      abort_or_commit_decision_message_.add_misc_bool(commit_);
      connection_->Send(abort_or_commit_decision_message_); 
    }

    if (commit_ == true) {
      // commit this txns: begin sending out local reads to other writers
      SendLocalResults();
    } else {
      // abort the txn and send it to the related replica.
      txn_->clear_involved_replicas();
      for (auto replica : involved_replicas) {
        txn_->add_involved_replicas(replica);
      }

      MessageProto forward_txn_message_;
      forward_txn_message_.set_destination_channel("sequencer_txn_receive_");
      forward_txn_message_.set_type(MessageProto::TXN_FORWORD);

      string txn_string;
      txn_->SerializeToString(&txn_string);

      if (txn_->involved_replicas_size() == 1) {
        uint64 machine_sent = txn_->involved_replicas(0) * configuration_->nodes_per_replica() + rand() % configuration_->nodes_per_replica();
        forward_txn_message_.clear_data();
        forward_txn_message_.add_data(txn_string);
        forward_txn_message_.set_destination_node(machine_sent);
        connection_->Send(forward_txn_message_);
      } else {
        uint64 machine_sent = rand() % configuration_->nodes_per_replica();
        forward_txn_message_.clear_data();
        forward_txn_message_.add_data(txn_string);
        forward_txn_message_.set_destination_node(machine_sent);
        connection_->Send(forward_txn_message_);     
      }
    }
    
  }
}

void StorageManager::SendLocalResults() {
  for (auto remote_writer : remote_replica_writers_) {
    uint64 mds = remote_writer.first;
    uint64 replica = remote_writer.second;
    string destination_channel = IntToString(txn_->txn_id()) + "-" + IntToString(replica);
    remote_result_message_.set_destination_channel(destination_channel);
    remote_result_message_.set_destination_node(configuration_->LookupMachineID(mds, configuration_->local_replica_id()));
    connection_->Send(remote_result_message_);        
  }
}

bool StorageManager::AbortTxn() {
  return commit_ == false;
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

