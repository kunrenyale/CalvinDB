// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "backend/storage_manager.h"
#include <algorithm> 

StorageManager::StorageManager(ClusterConfig* config, ConnectionMultiplexer* connection,
                               Storage* actual_storage, TxnProto* txn, uint32 mode)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), mode_(mode), relative_node_id_(config->relative_node_id()){
  local_replica_id_ = config->local_replica_id();

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
  set<uint64> writers;
  txn_origin_replica_ = txn->origin_replica();
  local_commit_ = true;

  reached_decision_ = false;

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
        involved_machines_.insert(make_pair(mds, key_entry.master()));     
      }

      if (mode_ != 0 && key_entry.master() != txn_origin_replica_) {
        continue;
      }

      if (mds == relative_node_id_) {
        Record* val = actual_storage_->ReadObject(key);
        objects_[key] = val;

        // For remaster: collect local entries
        if (mode_ == 2) {
          RemoteResultsEntry* results_entry = local_entries_.add_entries();
          results_entry->set_key(key);
          results_entry->set_value(val->value);
          results_entry->set_master(val->master);
          results_entry->set_counter(val->counter);

          if (val->master != key_entry.master() || val->counter != key_entry.counter()) {
            local_commit_ = false;
          }
        }
      }
    }

    for (int i = 0; i < txn->read_write_set_size(); i++) {
      KeyEntry key_entry = txn->read_write_set(i);
      const Key& key = key_entry.key();
      uint64 mds = configuration_->LookupPartition(key);
      writers.insert(mds);
      
      if (mode_ == 2) {
        involved_machines_.insert(make_pair(mds, key_entry.master())); 
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

        // For remaster: collect local entries
        if (mode_ == 2) {
          RemoteResultsEntry* results_entry = local_entries_.add_entries();
          results_entry->set_key(key);
          results_entry->set_value(val->value);
          results_entry->set_master(val->master);
          results_entry->set_counter(val->counter);

          if (val->master != key_entry.master() || val->counter != key_entry.counter()) {
            local_commit_ = false;
          }
        }
      } 
    }

    if (mode_ == 0) {
      // Original CalvinDB: Broadcast local reads to (other) writers.
      string local_entries_string;
      local_entries_->SerializeToString(&local_entries_string);
      remote_result_message_.add_data(local_entries_string);

      remote_result_message_.set_destination_channel(IntToString(txn->txn_id()) + "-" + IntToString(txn_origin_replica_));
      for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
        if (*it != relative_node_id_) {
          remote_result_message_.set_destination_node(configuration_->LookupMachineID(*it, configuration_->local_replica_id()));
          connection_->Send(remote_result_message_);
        }
      }
    } else if (mode_ == 1){
      // Basic request chopping: Broadcast local reads to (other) writers.
      string local_entries_string;
      local_entries_->SerializeToString(&local_entries_string);
      remote_result_message_.add_data(local_entries_string);

      for (auto remote_writer : remote_replica_writers_) {
        uint64 mds = remote_writer.first;
        uint64 replica = remote_writer.second;
        string destination_channel = IntToString(txn->txn_id()) + "-" + IntToString(replica);
        remote_result_message_.set_destination_channel(destination_channel);
        remote_result_message_.set_destination_node(configuration_->LookupMachineID(mds, configuration_->local_replica_id()));
        connection_->Send(remote_result_message_);        
      }
    } else {
      // (for mp or mr txns) Request chopping with remaster: get the min_involved_machine_ and min_involved_machine_origin_(will generate new txn if aborted)
      pair<uint64, uint32> min_machine = make_pair(INT_MAX, INT_MAX);
      for (auto machine : involved_machines_) {
        if (machine.first < min_machine.first) {
          min_machine.first = machine.first;
          min_machine.second = machine.second; 
        } else if (machine.first == min_machine.first && machine.second < min_machine.second) {
          min_machine.first = machine.first;
          min_machine.second = machine.second;        
        }
      }
      
      min_involved_machine_ = min_machine.first;
      min_involved_machine_origin_ = min_machine.second;

      string local_entries_string;
      local_entries_->SerializeToString(&local_entries_string);
      remote_result_message_.add_data(local_entries_string);

      for (auto remote_writer : remote_replica_writers_) {
        uint64 mds = remote_writer.first;
        uint64 replica = remote_writer.second;
        string destination_channel = IntToString(txn->txn_id()) + "-" + IntToString(replica);
        remote_result_message_.set_destination_channel(destination_channel);
        remote_result_message_.set_destination_node(configuration_->LookupMachineID(mds, configuration_->local_replica_id()));
        connection_->Send(remote_result_message_);        
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

// Return true if it will commit, or return false if it will abort
bool StorageManager::CheckCommitOrAbort() {
  bool decision = true;

  if (txn->status() == TxnProto::ABORTED_WITHOUT_LOCK) {
    decision = false;
  } else if (local_commit_ == false) {
    txn_->set_status(TxnProto::ABORTED);
    decision = false;
  } else if (involved_machines_.size() == 1) {
    decision = true;
  } else {
    // check the master/counter
    for (int i = 0; i < txn_->read_set_size(); i++) {
      KeyEntry key_entry = txn_->read_set(i);
      Record* val = objects_[key_entry.key()];
      if (key_entry.master() != val->master || key_entry.counter() != val->counter) {
        decision = false;
        break;
      }
    }

    if (decision == true) {
      for (int i = 0; i < txn_->read_write_set_size(); i++) {
        KeyEntry key_entry = txn_->read_write_set(i);
        Record* val = objects_[key_entry.key()];
        if (key_entry.master() != val->master || key_entry.counter() != val->counter) {
          decision = false;
          break;
        }   
      }
    }

  } // end if else

  // If we need to generate new transction for the aborted txn
  if (decision == false && local_replica_id_ == txn_origin_replica_ && min_involved_machine_ == relative_node_id_ && min_involved_machine_origin_ == txn_origin_replica_) {
    // abort the txn and send it to the related replica.
    TxnProto txn;
    txn.CopyFrom(*txn_);
    txn.clear_involved_replicas();

    txn.set_txn_id(configuration_->GetGUID());
    txn.set_status(TxnProto::NEW);

    set<uint32> involved_replicas;

    for (int i = 0; i < txn.read_set_size(); i++) {
      KeyEntry key_entry = txn.read_set(i);
      Record* val = objects_[key_entry.key()];
      if (key_entry.master() != val->master || key_entry.counter() != val->counter) {  
        // update to the latest master/counter
        txn.mutable_read_set(i)->set_master(val->master);
        txn.mutable_read_set(i)->set_counter(val->counter);
      }
      involved_replicas.insert(val->master);
    }

    for (int i = 0; i < txn.read_write_set_size(); i++) {
      KeyEntry key_entry = txn.read_write_set(i);
      Record* val = objects_[key_entry.key()];
      if (key_entry.master() != val->master || key_entry.counter() != val->counter) {  
        // update to the latest master/counter
        txn.mutable_read_write_set(i)->set_master(val->master);
        txn.mutable_read_write_set(i)->set_counter(val->counter);
      }
      involved_replicas.insert(val->master);
    }

    for (auto replica : involved_replicas) {
      txn.add_involved_replicas(replica);
    }

    MessageProto forward_txn_message_;
    forward_txn_message_.set_destination_channel("sequencer_txn_receive_");
    forward_txn_message_.set_type(MessageProto::TXN_FORWORD);

    string txn_string;
    txn.SerializeToString(&txn_string);

    if (txn.involved_replicas_size() == 1) {
//LOG(ERROR) << configuration_->local_node_id()<< " :"<<txn.txn_id() << ":In storageManager: will abort this txn) , replica size == 1: old txn id:"<<txn_->txn_id()<<"  new id:"<<txn.txn_id();
      txn.set_client_replica(txn.involved_replicas(0));
      uint64 machine_sent = txn.involved_replicas(0) * configuration_->nodes_per_replica() + rand() % configuration_->nodes_per_replica();
      forward_txn_message_.clear_data();
      forward_txn_message_.add_data(txn_string);
      forward_txn_message_.set_destination_node(machine_sent);
      connection_->Send(forward_txn_message_);
    } else {
//LOG(ERROR) << configuration_->local_node_id()<< " :"<<txn.txn_id() << ":In storageManager:  received remote entries (will abort this txn) , replica size == 2: ";
      txn.set_client_replica(0);
      uint64 machine_sent = rand() % configuration_->nodes_per_replica();
      forward_txn_message_.clear_data();
      forward_txn_message_.add_data(txn_string);
      forward_txn_message_.set_destination_node(machine_sent);
      connection_->Send(forward_txn_message_);     
    }
  } // end if

  return decision;
}

void StorageManager::HandleReadResult(const MessageProto& message) {
  CHECK(message.type() == MessageProto::READ_RESULT);

  RemoteResultsEntries remote_entries;
  remote_entries.ParseFromString(message.data(0));

  for (uint32 i = 0; i < (uint32)remote_entries.entries_size(); i++) {
    RemoteResultsEntry key_entry = remote_entries.entries(i);
    Record* val = new Record(key_entry.value(), key_entry.master(), key_entry.counter());
    objects_[key_entry.key()] = val;
    remote_reads_.push_back(val);
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

