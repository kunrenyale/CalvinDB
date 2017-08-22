// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "log/local_paxos.h"


LocalPaxos::LocalPaxos(ClusterConfig* config, ConnectionMultiplexer* connection)
    : configuration_(config), connection_(connection) {
  go_ = true;
  local_count_ = 0;
  
  local_log_ = new LocalMemLog();
  global_log_ = new LocalMemLog();

  this_machine_id_ = configuration_->local_node_id();
  this_replica_id_ = configuration_->local_replica_id();

  machines_per_replica_ = configuration_->nodes_per_replica();
  local_replica_ = configuration_->local_replica_id();

  for (uint32 i = 0; i < 3; i++) {
    uint64 id = this_replica_id_ * configuration_->nodes_per_replica() + i;
    if (id < (this_replica_id_ + 1) * configuration_->nodes_per_replica()) {
      participants_.push_back(this_replica_id_ * configuration_->nodes_per_replica() + i);
    }
  }
  
  connection_->NewChannel("paxos_log_");
  connection_->NewChannel("paxos_ack_");
  
  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
//  CPU_SET(2, &cpuset);
//  CPU_SET(6, &cpuset);
  CPU_SET(7, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

  if (IsLeader()) {
    pthread_create(&leader_thread_, &attr_writer, RunLeaderThread, reinterpret_cast<void*>(this));
  } else {
    pthread_create(&follower_thread_, &attr_writer, RunFollowerThread, reinterpret_cast<void*>(this));  
  }
}

LocalPaxos::~LocalPaxos() {
  Stop();
  if (IsLeader()) {
    pthread_join(leader_thread_, NULL);
  } else {
    pthread_join(follower_thread_, NULL);
  }
}

void* LocalPaxos::RunLeaderThread(void *arg) {
  reinterpret_cast<LocalPaxos*>(arg)->RunLeader();
  return NULL;
}

void* LocalPaxos::RunFollowerThread(void *arg) {
  reinterpret_cast<LocalPaxos*>(arg)->RunFollower();
  return NULL;
}

bool LocalPaxos::IsLeader() {
  return this_machine_id_ == participants_[0];
}

void LocalPaxos::Append(uint64 blockid) {
//LOG(ERROR) << "In paxos log:  append a batch: "<<blockid;
    Lock l(&mutex_);
    sequence_.add_batch_ids(blockid);
    local_count_ += 1;
//LOG(ERROR) << configuration_->local_node_id()<< "+++In paxos Append: append a new batch:"<<blockid;
}

void LocalPaxos::ReceiveMessage() {
  MessageProto message;
     
  // Receive the messages.
  while (connection_->GotMessage("paxos_log_", &message) == true) {
    if (message.type() == MessageProto::MR_TXNS_BATCH) {
      MessageProto* mr_message = new MessageProto();
      mr_message->CopyFrom(message);
      mr_txn_batches_[message.misc_int(0)] = mr_message;
    } else if (message.type() == MessageProto::NEW_SEQUENCE) {
      uint32 from_replica = message.misc_int(0);
      uint64 latest_version = message.misc_int(1);

      latest_received_version_for_replicas_[from_replica] = latest_version;
      SequenceBatch sequence_batch;
      sequence_batch.ParseFromString(message.data(0));
        
      for (int i = 0; i < sequence_batch.sequence_batch_size(); i++) {
        sequences_other_replicas_.Push(make_pair(sequence_batch.sequence_batch(i), from_replica));
      }

      sent_sequence_ack[from_replica] = false;
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  receive  NEW_SEQUENCE from: "<<from_replica * machines_per_replica_<<"  . latest_version is:"<<latest_version;
    } else if (message.type() == MessageProto::NEW_SEQUENCE_ACK) {
      SequenceBatch sequence_batch;
      uint32 from_replica = message.misc_int(0);
      Sequence current_sequence_;
      Log::Reader* r = readers_for_local_log_[from_replica];
      uint64 latest_version = message.misc_int(1);

      bool find = r->Next();
      while (find == true) {
        latest_version = r->Version();
        current_sequence_.ParseFromString(r->Entry());
        sequence_batch.add_sequence_batch()->CopyFrom(current_sequence_);
        find = r->Next();
      }

      if (latest_version == message.misc_int(1)) {
        new_sequence_todo.insert(from_replica);
      } else {
        string sequence_batch_string;
        sequence_batch.SerializeToString(&sequence_batch_string);

        MessageProto sequence_batch_message;
        sequence_batch_message.add_data(sequence_batch_string);
        sequence_batch_message.set_destination_channel("paxos_log_");
        sequence_batch_message.set_destination_node(from_replica * machines_per_replica_);
        sequence_batch_message.set_type(MessageProto::NEW_SEQUENCE);
        sequence_batch_message.add_misc_int(local_replica_);
        sequence_batch_message.add_misc_int(latest_version);
        connection_->Send(sequence_batch_message);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  send  NEW_SEQUENCE to: "<<from_replica * machines_per_replica_<<"  . latest_version is:"<<latest_version;
      }
    }
  } // End receiving messages
}

void LocalPaxos::Stop() {
  go_ = false;
}


void LocalPaxos::RunLeader() {
  local_next_version = 0;
  global_next_version = 0;

  for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
    readers_for_local_log_[i] = local_log_->GetReader();
  }

  uint64 quorum = static_cast<int>(participants_.size()) / 2 + 1;
  MessageProto sequence_message;

  MessageProto message;
  string encoded;
  bool isLocal = false;
  pair<Sequence, uint32> remote_sequence_pair;
  Sequence remote_sequence;
  uint32 remote_replica;

  MessageProto batch_message;
  batch_message.set_destination_channel("sequencer_");
  batch_message.set_type(MessageProto::TXN_BATCH);
  batch_message.set_source_node(this_machine_id_);
  batch_message.add_misc_bool(false);

  bool isFirst = true;

  for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
    if (i == local_replica_) {
      continue;
    }
    latest_received_version_for_replicas_[i] = 0;
    latest_processed_version_for_replicas_[i] = 0;
    sent_sequence_ack[i] = false;
  }

  while (go_) {
    
    // Sleep while there are NO requests.
    while (local_count_.load() == 0 && sequences_other_replicas_.Size() == 0) {
      usleep(20);
     
      // Receive messages
      ReceiveMessage();
    } // End while
    
    if (local_count_.load() >  0) {
      // Propose a new sequence.
      Lock l(&mutex_);
      local_next_version ++;
      global_next_version ++;
      sequence_.SerializeToString(&encoded);
      sequence_.Clear();
      local_count_ = 0;
      isLocal = true;
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  will handle the version from local: "<<global_next_version;
    } else if (sequences_other_replicas_.Size() > 0) {
      isLocal = false;
      global_next_version ++;
      sequences_other_replicas_.Pop(&remote_sequence_pair);
      remote_sequence = remote_sequence_pair.first;
      remote_replica = remote_sequence_pair.second;
      remote_sequence.SerializeToString(&encoded);

//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "### In paxos:  will handle remote sequence, version: "<<global_next_version;

      if (local_replica_ != 0 && remote_replica == 0) {
        // Generate new txns for multi-replica txns.
        for (int i = 0; i < remote_sequence.batch_ids_size(); i++) {
          uint64 batch_id = remote_sequence.batch_ids(i);
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos: before handle remote_sequence:"<<batch_id;
          while (mr_txn_batches_.find(batch_id) == mr_txn_batches_.end()) {
            usleep(20);
   
            // Receive messages
            ReceiveMessage();

          }; // end while


          MessageProto* mr_message = mr_txn_batches_[batch_id];

          if (mr_message->data_size() == 0) {
            continue;
          }

//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos: after handle remote_sequence:"<<batch_id<<"  size is:"<<mr_message->data_size();

          batch_message.clear_data();
          for (int i = 0; i < mr_message->data_size(); i++) {
            TxnProto txn;
            txn.ParseFromString(mr_message->data(i));
       
            if (txn.fake_txn() == true) {
              txn.set_fake_txn(false);
            }

            txn.set_new_generated(true);
            txn.set_origin_replica(local_replica_);

            string txn_string;
            txn.SerializeToString(&txn_string);
            batch_message.add_data(txn_string);
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos: generated a new txn:"<<txn.txn_id();
          }

          if (batch_message.data_size() > 0) {
            uint64 batch_number = configuration_->GetGUID();
            batch_message.set_batch_number(batch_number);
            Append(batch_number);
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos: append a new batch:"<<batch_number;

            for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
              uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_number), i);
              batch_message.set_destination_node(machine_id);
              connection_->Send(batch_message);
            }
          } // end if

        } // end for loop

      } // end if
    }  

    // Handle this sequence
    sequence_message.add_data(encoded);
    sequence_message.add_misc_int(global_next_version);
    if (isLocal == true) {
      sequence_message.add_misc_int(local_next_version);
    }
    sequence_message.set_type(MessageProto::PAXOS_DATA);
    sequence_message.set_destination_channel("paxos_ack_");

    for (uint32 i = 1; i < participants_.size(); i++) {
      sequence_message.set_destination_node(participants_[i]);
      connection_->Send(sequence_message);
    }

    uint64 acks = 1;

    // Collect Acks.
    MessageProto message;
    while (acks < quorum) {
      while (connection_->GotMessage("paxos_ack_", &message) == false) {
        usleep(10);
        if (!go_) {
          return;
        }
      }

      CHECK(message.type() == MessageProto::PAXOS_DATA_ACK);
      if (message.misc_int(0) == global_next_version) {
        acks++;
      }
      message.Clear();
    }

    // Send the order to the locking thread
    sequence_message.set_type(MessageProto::PAXOS_BATCH_ORDER);
    sequence_message.set_destination_channel("scheduler_");
    for (uint64 i = local_replica_ * machines_per_replica_; i < (local_replica_ + 1)*machines_per_replica_ ;i++) {
      sequence_message.set_destination_node(i);
      connection_->Send(sequence_message);
    }

    sequence_message.clear_data();

    // Commit!
    sequence_message.set_type(MessageProto::PAXOS_COMMIT);
    sequence_message.set_destination_channel("paxos_log_");
    for (uint32 i = 1; i < participants_.size(); i++) {
      sequence_message.set_destination_node(participants_[i]);
      connection_->Send(sequence_message);
    }
   
    sequence_message.Clear();

    // Actually append the request into the log
    if (isLocal == true) {
      local_log_->Append(local_next_version, encoded);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  Append to local log. version: "<<local_next_version;
    }
    global_log_->Append(global_next_version, encoded);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  Append to global log. version: "<<global_next_version;

    // Send its local sequences to other replicas for the first time.
    if (isLocal == true && isFirst == true) {
      for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
        if (i == local_replica_) {
          continue;
        }
        SequenceBatch sequence_batch;
        Sequence current_sequence_;
        Log::Reader* r = readers_for_local_log_[i];
        uint64 latest_version = 0;

        bool find = r->Next();
        while (find == true) {
          latest_version = r->Version();
          current_sequence_.ParseFromString(r->Entry());
          sequence_batch.add_sequence_batch()->CopyFrom(current_sequence_);
          find = r->Next();
        }

        string sequence_batch_string;
        sequence_batch.SerializeToString(&sequence_batch_string);

        MessageProto sequence_batch_message;
        sequence_batch_message.add_data(sequence_batch_string);
        sequence_batch_message.set_destination_channel("paxos_log_");
        sequence_batch_message.set_destination_node(i * machines_per_replica_);
        sequence_batch_message.set_type(MessageProto::NEW_SEQUENCE);
        sequence_batch_message.add_misc_int(local_replica_);
        sequence_batch_message.add_misc_int(latest_version);
        connection_->Send(sequence_batch_message);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  send  NEW_SEQUENCE to: "<<i * machines_per_replica_<<"  . latest_version is:"<<latest_version;
      }

      isFirst = false;
    } else if (isLocal == true && isFirst == false) {
      for (uint32 pending_replica : new_sequence_todo) {
        SequenceBatch sequence_batch;
        Sequence current_sequence_;
        Log::Reader* r = readers_for_local_log_[pending_replica];
        uint64 latest_version = 0;

        bool find = r->Next();
        while (find == true) {
          latest_version = r->Version();
          current_sequence_.ParseFromString(r->Entry());
          sequence_batch.add_sequence_batch()->CopyFrom(current_sequence_);
          find = r->Next();
        }
    
        CHECK(latest_version != 0);
        
        string sequence_batch_string;
        sequence_batch.SerializeToString(&sequence_batch_string);

        MessageProto sequence_batch_message;
        sequence_batch_message.add_data(sequence_batch_string);
        sequence_batch_message.set_destination_channel("paxos_log_");
        sequence_batch_message.set_destination_node(pending_replica * machines_per_replica_);
        sequence_batch_message.set_type(MessageProto::NEW_SEQUENCE);
        sequence_batch_message.add_misc_int(local_replica_);
        sequence_batch_message.add_misc_int(latest_version);
        connection_->Send(sequence_batch_message);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  send  NEW_SEQUENCE to: "<<pending_replica * machines_per_replica_<<"  . latest_version is:"<<latest_version;  

      }
    } else if (isLocal == false) {
      latest_processed_version_for_replicas_[remote_replica]++;
      if (sent_sequence_ack[remote_replica] == false && latest_received_version_for_replicas_[remote_replica] - latest_processed_version_for_replicas_[remote_replica] < 20) {
        sent_sequence_ack[remote_replica] = true;
        MessageProto new_sequence_ack_message;

        new_sequence_ack_message.set_destination_channel("paxos_log_");
        new_sequence_ack_message.set_destination_node(remote_replica * machines_per_replica_);
        new_sequence_ack_message.set_type(MessageProto::NEW_SEQUENCE_ACK);
        new_sequence_ack_message.add_misc_int(local_replica_);
        new_sequence_ack_message.add_misc_int(latest_received_version_for_replicas_[remote_replica]);
        connection_->Send(new_sequence_ack_message);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<< "---In paxos:  send  NEW_SEQUENCE_ACK to: "<<remote_replica * machines_per_replica_<<"  . latest_version is:"<<latest_received_version_for_replicas_[remote_replica];   
      }
    }

    // Receive messages
    ReceiveMessage();
  }
}

void LocalPaxos::RunFollower() {
 
  MessageProto message;
  MessageProto ack_message;
  MessageProto append_message;
  queue<MessageProto> uncommitted;

  while (go_) {
    // Get message from leader.
    while (connection_->GotMessage("paxos_ack_", &message) == false) {
      usleep(20);
      if (!go_) {
        return;
      }
    }

    if (message.type() == MessageProto::PAXOS_DATA) {
      // New proposal.
      uncommitted.push(message);
      // Send ack to leader.
      ack_message.set_destination_node(participants_[0]);
      ack_message.set_type(MessageProto::PAXOS_DATA_ACK);
      ack_message.set_destination_channel("paxos_ack_");
      ack_message.add_misc_int(message.misc_int(0));
      connection_->Send(ack_message);
 
      ack_message.Clear();

    } else if (message.type() == MessageProto::PAXOS_COMMIT){
      // Commit message.
      CHECK(!uncommitted.empty());
      append_message = uncommitted.front();
      uncommitted.pop();
      
      uint64 global_version = append_message.misc_int(0);
      string data = append_message.data(0);

      append_message.Clear();

      global_log_->Append(global_version, data);
      if (append_message.misc_int_size() > 1) {
        uint64 local_version = append_message.misc_int(1);
        local_log_->Append(local_version, data);  
      }
    }
  }
}

