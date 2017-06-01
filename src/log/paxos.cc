// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "log/paxos.h"


Paxos::Paxos(Log* log, ClusterConfig* config, ConnectionMultiplexer* connection)
    : log_(log), configuration_(config), connection_(connection) {
  // Init participants_
  participants_.push_back(0);
  go_ = true;
  count_ = 0;

  for (uint32 i = 1; i < configuration_->replicas_size(); i++) {
    participants_.push_back(i * configuration_->nodes_per_replica());
  }

  this_machine_id_ = configuration_->local_node_id();
  
  connection_->NewChannel("paxos_log_");
  
  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

  if (IsLeader()) {
    pthread_create(&leader_thread_, &attr_writer, RunLeaderThread, reinterpret_cast<void*>(this));
  } else {
    pthread_create(&follower_thread_, &attr_writer, RunFollowerThread, reinterpret_cast<void*>(this));  
  }
}

Paxos::~Paxos() {
  Stop();
  if (IsLeader()) {
    pthread_join(leader_thread_, NULL);
  } else {
    pthread_join(follower_thread_, NULL);
  }
}

void* Paxos::RunLeaderThread(void *arg) {
  reinterpret_cast<Paxos*>(arg)->RunLeader();
  return NULL;
}

void* Paxos::RunFollowerThread(void *arg) {
  reinterpret_cast<Paxos*>(arg)->RunFollower();
  return NULL;
}

bool Paxos::IsLeader() {
  return this_machine_id_ == participants_[0];
}

void Paxos::Append(uint64 blockid) {
//LOG(ERROR) << "In paxos log:  append a batch: "<<blockid;
    Lock l(&mutex_);
    sequence_.add_batch_ids(blockid);
    count_ += 1;
}

void Paxos::Stop() {
  go_ = false;
}


void Paxos::RunLeader() {
  uint64 next_version = 0;
  uint64 quorum = static_cast<int>(participants_.size()) / 2 + 1;

  MessageProto sequence_message;
  
  uint64 machines_per_replica = configuration_->nodes_per_replica();
  uint32 local_replica = configuration_->local_replica_id(); 

  while (go_) {
    // Sleep while there are NO requests.
    while (count_.load() == 0) {
      usleep(20);
      if (!go_) {
        return;
      }
    }

    // Propose a new sequence.
    uint64 version;
    string encoded;
    {
      Lock l(&mutex_);
      version = next_version;
      next_version ++;
      sequence_.SerializeToString(&encoded);
      sequence_.Clear();
      count_ = 0;
    }

    sequence_message.add_data(encoded);
    sequence_message.add_misc_int(version);
    sequence_message.set_type(MessageProto::PAXOS_DATA);
    sequence_message.set_destination_channel("paxos_log_");

    for (uint32 i = 1; i < participants_.size(); i++) {
      sequence_message.set_destination_node(participants_[i]);
      connection_->Send(sequence_message);
    }

    uint64 acks = 1;

    // Collect Acks.
    MessageProto message;
    while (acks < quorum) {
      while (connection_->GotMessage("paxos_log_", &message) == false) {
        usleep(10);
        if (!go_) {
          return;
        }
      }

      assert(message.type() == MessageProto::PAXOS_DATA_ACK);
      acks++;
      message.Clear();
    }

    // Send the order to the locking thread
    sequence_message.set_type(MessageProto::PAXOS_BATCH_ORDER);
    sequence_message.set_destination_channel("scheduler_");
    for (uint64 i = local_replica * machines_per_replica; i < (local_replica + 1)*machines_per_replica ;i++) {
//LOG(ERROR) <<this_machine_id_<< ":In paxos log:  send PAXOS_BATCH_ORDER: "<<version<<"  to node:"<<i;
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
//LOG(ERROR) << "In paxos log:  append a sequence: "<<version;
    log_->Append(version, encoded);

  }
}

void Paxos::RunFollower() {
 
  MessageProto message;
  MessageProto ack_message;
  MessageProto append_message;
  queue<MessageProto> uncommitted;

  uint64 machines_per_replica = configuration_->nodes_per_replica();
  uint32 local_replica = configuration_->local_replica_id(); 

  while (go_) {
    // Get message from leader.
    while (connection_->GotMessage("paxos_log_", &message) == false) {
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
      ack_message.set_destination_channel("paxos_log_");
      connection_->Send(ack_message);

    } else if (message.type() == MessageProto::PAXOS_COMMIT){
      // Commit message.
      CHECK(!uncommitted.empty());
      append_message = uncommitted.front();
      uncommitted.pop();
      
      uint64 version = append_message.misc_int(0);
      string data = append_message.data(0);

      // Send the order to the locking thread
      append_message.set_type(MessageProto::PAXOS_BATCH_ORDER);
      append_message.set_destination_channel("scheduler_");
      for (uint64 i = local_replica * machines_per_replica; i < (local_replica + 1)*machines_per_replica ;i++) {
        append_message.set_destination_node(i);
        connection_->Send(append_message);
//LOG(ERROR) <<this_machine_id_<< ":In paxos log:  send PAXOS_BATCH_ORDER: "<<version<<"  to node:"<<i;
      }


      log_->Append(version, data);
    }
  }
}

