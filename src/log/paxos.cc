// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "log/paxos.h"


Paxos::Paxos(Log* log, ClusterConfig* config, Connection* paxos_connection)
    : log_(log), configuration_(config), paxos_connection_(paxos_connection) {
  // Init participants_
  participants_[0] = 0;
  go_ = true;
  count_ = 0;
  for (uint32 i = 1; i < configuration_->replicas_size(); i++) {
    participants_[i] = i * configuration_->nodes_per_replica();
  }

  this_machine_id_ = configuration_->local_node_id();

  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

LOG(ERROR) << "In paxos log before create paxos thread...";


  if (IsLeader()) {
    pthread_create(&leader_thread_, &attr_writer, RunLeaderThread, reinterpret_cast<void*>(this));
LOG(ERROR) << "In paxos log after create paxos thread...";
  } else {
    pthread_create(&follower_thread_, &attr_writer, RunFollowerThread, reinterpret_cast<void*>(this));  
  }
}

Paxos::~Paxos() {
    Stop();
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
    Lock l(&mutex_);
    sequence_.add_batch_ids(blockid);
}

void Paxos::Stop() {
  go_ = false;
}


void Paxos::RunLeader() {
LOG(ERROR) << "In paxos log:  in paxos thread...";
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
    }

    sequence_message.add_data(encoded);
    sequence_message.add_misc_int(version);
    sequence_message.set_type(MessageProto::PAXOS_DATA);
    sequence_message.set_destination_channel("paxos_log_");

    for (uint32 i = 1; i < participants_.size(); i++) {
      sequence_message.set_destination_node(participants_[i]);
      paxos_connection_->Send(sequence_message);
    }

    uint64 acks = 1;

    // Collect Acks.
    MessageProto message;
    while (acks < quorum) {
      while (paxos_connection_->GetMessage(&message) == false) {
        usleep(10);
        if (!go_) {
          return;
        }
      }

      assert(message->type() == MessageProto::PAXOS_DATA_ACK);
      acks++;
      message.Clear();
    }

    // Send the order to the locking thread
    sequence_message.set_type(MessageProto::PAXOS_BATCH_ORDER);
    sequence_message.set_destination_channel("scheduler_");
    for (uint64 i = local_replica * machines_per_replica; i < (local_replica + 1)*machines_per_replica ;i++) {
      sequence_message.set_destination_node(i);
      paxos_connection_->Send(sequence_message);
    }

    sequence_message.clear_data();

    // Commit!
    sequence_message.set_type(MessageProto::PAXOS_COMMIT);
    sequence_message.set_destination_channel("paxos_log_");
    for (uint32 i = 1; i < participants_.size(); i++) {
      sequence_message.set_destination_node(participants_[i]);
      paxos_connection_->Send(sequence_message);
    }
   
    sequence_message.Clear();

    log_->Append(version, encoded);

  }
}

void Paxos::RunFollower() {
 
  MessageProto message;
  MessageProto ack_message;
  queue<MessageProto> uncommitted;

  uint64 machines_per_replica = configuration_->nodes_per_replica();
  uint32 local_replica = configuration_->local_replica_id(); 

  while (go_) {
    // Get message from leader.
    while (!paxos_connection_->GetMessage(&message)) {
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
      paxos_connection_->Send(ack_message);

    } else if (message.type() == MessageProto::PAXOS_COMMIT){
      // Commit message.
      CHECK(!uncommitted.empty());
      message = uncommitted.front();
      uncommitted.pop();
      
      uint64 version = message.misc_int(0);
      string data = message.data(0);

      log_->Append(version, data);

      // Send the order to the locking thread
      message.set_type(MessageProto::PAXOS_BATCH_ORDER);
      message.set_destination_channel("scheduler_");
      for (uint64 i = local_replica * machines_per_replica; i < (local_replica + 1)*machines_per_replica ;i++) {
        message.set_destination_node(i);
        paxos_connection_->Send(message);
      }
    }
  }
}

