// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//

#include "machine/sequencer.h"

#ifdef LATENCY_TEST
map<uint64, double> sequencer_recv;
map<uint64, double> scheduler_unlock;
std::atomic<uint64> latency_counter;
vector<double> measured_latency;
#endif

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

Sequencer::Sequencer(ClusterConfig* conf, ConnectionMultiplexer* connection, Client* client, Paxos* paxos, uint32 max_batch_size)
          : epoch_duration_(0.005), configuration_(conf), connection_(connection),
          client_(client), deconstructor_invoked_(false), paxos_log_(paxos), max_batch_size_(max_batch_size) {
  // Start Sequencer main loops running in background thread.


  connection_->NewChannel("sequencer_");

  start_working_ = false;

  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
CPU_SET(0, &cpuset);
//  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter, reinterpret_cast<void*>(this));

  CPU_ZERO(&cpuset);
//  CPU_SET(2, &cpuset);
CPU_SET(0, &cpuset);
  pthread_attr_t attr_reader;
  pthread_attr_init(&attr_reader);
  pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader, reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}


void Sequencer::RunWriter() {

  // Set up batch messages for each system node.
  MessageProto batch_message;
  batch_message.set_destination_channel("sequencer_");
  batch_message.set_type(MessageProto::TXN_BATCH);
  uint64 batch_number;
  uint32 txn_id_offset;

  uint32 local_replica = configuration_->local_replica_id();

/**if (configuration_->local_node_id() == 2 || configuration_->local_node_id() == 3) {
epoch_duration_ = 0.01;
} else {
epoch_duration_ = 100;
}**/

#ifdef LATENCY_TEST
latency_counter = 0;
uint64 local_machine = configuration_->local_node_id();
#endif

  connection_->NewChannel("synchronization_sequencer_channel");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("synchronization_sequencer_channel");
  for (uint64 i = 0; i < (uint64)(configuration_->all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint64>(configuration_->local_node_id())) {
      connection_->Send(synchronization_message);
    }
  }

  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint64)(configuration_->all_nodes_size())) {
    synchronization_message.Clear();
    if (connection_->GotMessage("synchronization_sequencer_channel", &synchronization_message)) {
      CHECK(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  connection_->DeleteChannel("synchronization_sequencer_channel");
LOG(INFO) << "In sequencer:  After synchronization. Starting sequencer writer.";
  start_working_ = true;

  while (!deconstructor_invoked_) {
    // Begin epoch.
    batch_number = configuration_->GetGUID();
    double epoch_start = GetTime();
    batch_message.set_batch_number(batch_number);
    batch_message.clear_data();

    // Collect txn requests for this epoch.
    txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if ((uint32)(batch_message.data_size()) < max_batch_size_) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * max_batch_size_ + txn_id_offset);

        txn->set_origin_replica(local_replica);
        txn->add_involved_replicas(local_replica);

#ifdef LATENCY_TEST
    if (txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES) {
      sequencer_recv[txn->txn_id()] = GetTime();
      txn->set_generated_machine(local_machine);
    }
#endif

        txn->SerializeToString(&txn_string);
        batch_message.add_data(txn_string);
        txn_id_offset++;

        delete txn;
      } else {
        usleep(50);
      }
    }


    // Send this epoch's transactions to the central machine of each replica
    for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
      uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_message.batch_number()), i);
//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader:  will send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
      batch_message.set_destination_node(machine_id);
      connection_->Send(batch_message);
//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader:  after send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
    }
  }

}

void Sequencer::RunReader() {

//LOG(INFO) << "In sequencer:  Starting sequencer reader.";
  // Set up batch messages for each system node.
  map<uint64, MessageProto> batches;

  uint32 local_replica = configuration_->local_replica_id();
  for (uint64 i = 0; i < configuration_->nodes_per_replica();i++) {
    batches[i].set_destination_channel("scheduler_");
    batches[i].set_destination_node(configuration_->LookupMachineID(i, local_replica));
    batches[i].set_type(MessageProto::TXN_SUBBATCH);
  }
  
  MessageProto message;
  uint64 batch_number;

  while (start_working_ != true) {
    usleep(100);
  }

  while (!deconstructor_invoked_) {

    bool got_message = connection_->GotMessage("sequencer_", &message);
    if (got_message == true) {
      if (message.type() == MessageProto::TXN_BATCH) {
//LOG(INFO) << configuration_->local_node_id()<< ":In sequencer reader:  recevie TXN_BATCH message:"<<message.batch_number();
        batch_number = message.batch_number();
        // If received TXN_BATCH: Parse batch and forward sub-batches to relevant readers (same replica only).
        for (int i = 0; i < message.data_size(); i++) {
          TxnProto txn;
          txn.ParseFromString(message.data(i));

          // Compute readers & writers; store in txn proto.
          set<uint64> readers;
          set<uint64> writers;
          for (uint32 i = 0; i < (uint32)(txn.read_set_size()); i++) {
            KeyEntry key_entry = txn.read_set(i);
            uint64 mds = configuration_->LookupPartition(key_entry.key());
            readers.insert(mds);
          }
          for (uint32 i = 0; i < (uint32)(txn.read_write_set_size()); i++) {
            KeyEntry key_entry = txn.read_write_set(i);
            uint64 mds = configuration_->LookupPartition(key_entry.key());
            writers.insert(mds);
            readers.insert(mds);
          }

          for (set<uint64>::iterator it = readers.begin(); it != readers.end(); ++it) {
            txn.add_readers(*it);
          }
          for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
            txn.add_writers(*it);
          }

          string txn_data;
          txn.SerializeToString(&txn_data);

          // Compute union of 'readers' and 'writers' (store in 'readers').
          for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
            readers.insert(*it);
          }

          // Insert txn into appropriate batches.
          for (set<uint64>::iterator it = readers.begin(); it != readers.end(); ++it) {
            batches[*it].add_data(txn_data);
          }
        }

        // Send this epoch's requests to all schedulers.
        for (map<uint64, MessageProto>::iterator it = batches.begin(); it != batches.end(); ++it) {
          it->second.set_batch_number(batch_number);
          connection_->Send(it->second);
          it->second.clear_data();
        }

        // Send “vote” to the head machines;
        MessageProto vote_message;
        vote_message.set_destination_channel("sequencer_");
        vote_message.set_destination_node(0);
        vote_message.set_type(MessageProto::BATCH_VOTE);
        vote_message.add_misc_int(message.batch_number());
        connection_->Send(vote_message);

      } else if (message.type() == MessageProto::BATCH_VOTE) {
//LOG(INFO) << configuration_->local_node_id()<< ":In sequencer reader:  recevie BATCH_VOTE message:"<<message.misc_int(0);
        uint64 batch_id = message.misc_int(0);
        uint32 votes = 0;
        
        if (batch_votes_.count(batch_id) == 0) {
          votes = 1;
          batch_votes_[batch_id] = 1;
        } else {
          votes = batch_votes_[batch_id] + 1;
          batch_votes_[batch_id] = votes;
        }

        // Remove from map if all servers are accounted for.
        if (votes == configuration_->replicas_size()) {
          batch_votes_.erase(batch_id);
        }

        // If block is now written to (exactly) a majority of replicas, submit
        // to paxos leader.
        if (votes == configuration_->replicas_size() / 2 + 1) {
//LOG(INFO) << configuration_->local_node_id()<< ":In sequencer reader:  recevie BATCH_VOTE message, will append:"<<batch_id;
          paxos_log_->Append(batch_id);
        }        
      }
    }

    if (got_message == false) {
      usleep(50);
    }
  }
}
