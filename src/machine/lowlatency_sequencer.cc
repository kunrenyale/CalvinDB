// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//

#include "machine/lowlatency_sequencer.h"


void* LowlatencySequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<LowlatencySequencer*>(arg)->RunWriter();
  return NULL;
}

void* LowlatencySequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<LowlatencySequencer*>(arg)->RunReader();
  return NULL;
}

LowlatencySequencer::LowlatencySequencer(ClusterConfig* conf, ConnectionMultiplexer* connection, Client* client, LocalPaxos* paxos, uint32 max_batch_size)
          : epoch_duration_(0.1), configuration_(conf), connection_(connection),
          client_(client), deconstructor_invoked_(false), paxos_log_(paxos), max_batch_size_(max_batch_size) {
  // Start Sequencer main loops running in background thread.


  connection_->NewChannel("sequencer_");
  connection_->NewChannel("sequencer_txn_receive_");

  start_working_ = false;

  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter, reinterpret_cast<void*>(this));

  CPU_ZERO(&cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_t attr_reader;
  pthread_attr_init(&attr_reader);
  pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader, reinterpret_cast<void*>(this));
}

LowlatencySequencer::~LowlatencySequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}


void LowlatencySequencer::RunWriter() {

  uint32 local_replica = configuration_->local_replica_id();
  uint64 local_machine = configuration_->local_node_id();
  uint64 nodes_per_replica = configuration_->nodes_per_replica();

  // Set up batch messages for each system node.
  MessageProto batch_message;
  batch_message.set_destination_channel("sequencer_");
  batch_message.set_type(MessageProto::TXN_BATCH);
  batch_message.set_source_node(local_machine);
  batch_message.add_misc_bool(true);

  uint64 batch_number;
  uint32 txn_id_offset;

  MessageProto txn_message;
  txn_message.set_destination_channel("sequencer_txn_receive_");
  txn_message.set_type(MessageProto::TXN_FORWORD);

/**if (configuration_->local_node_id() == 2 || configuration_->local_node_id() == 3) {
epoch_duration_ = 0.01;
} else {
epoch_duration_ = 100;
}**/

#ifdef LATENCY_TEST
latency_counter = 0;
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
LOG(ERROR) << configuration_->local_node_id()<< "---In sequencer:  After synchronization. Starting sequencer writer.";

  start_working_ = true;
  MessageProto message;
  while (!deconstructor_invoked_) {
    // Begin epoch.
    batch_number = configuration_->GetGUID();
    double epoch_start = GetTime();
    batch_message.set_batch_number(batch_number);
    batch_message.clear_data();

if (configuration_->local_node_id() == 0)
LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader:  will generate a new batch";

    // Collect txn requests for this epoch.
    txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if ((uint32)(batch_message.data_size()) < max_batch_size_) {
        bool got_message = connection_->GotMessage("sequencer_txn_receive_", &message);
        if (got_message == true) {
          TxnProto txn;
          txn.ParseFromString(message.data(0));
          txn.set_origin_replica(local_replica);
          batch_message.add_data(message.data(0));
          txn_id_offset++; 
        } else {
          TxnProto* txn;
          string txn_string;
          client_->GetTxn(&txn, batch_number * max_batch_size_ + txn_id_offset);

          txn->set_origin_replica(local_replica);
          txn->SerializeToString(&txn_string);

          if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) == local_replica) {
            batch_message.add_data(txn_string);
            txn_id_offset++;

#ifdef LATENCY_TEST
    if (txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES) {
      sequencer_recv[txn->txn_id()] = GetTime();
    }
#endif
          } else if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) != local_replica) {
            uint64 machine_sent = txn->involved_replicas(0) * nodes_per_replica + rand() % nodes_per_replica;
            txn_message.clear_data();
            txn_message.add_data(txn_string);
            txn_message.set_destination_node(machine_sent);
            connection_->Send(txn_message);
LOG(ERROR) << configuration_->local_node_id()<<": In sequencer writer:  wrong1";
          } else {
            uint64 machine_sent = rand() % nodes_per_replica;
            txn_message.clear_data();
            txn_message.add_data(txn_string);
            txn_message.set_destination_node(machine_sent);
            connection_->Send(txn_message); 
LOG(ERROR) << configuration_->local_node_id()<<": In sequencer writer:  wrong2";
          }

          delete txn;
        }
      } else {
        usleep(50);
      }
    }


    // Send this epoch's transactions to the central machine of each replica
    for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
      uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_message.batch_number()), i);
//if (configuration_->local_node_id() == 0 && i == 0)
//LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader:  will send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
      batch_message.set_destination_node(machine_id);
      connection_->Send(batch_message);
//LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader:  after send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
    }
  }

}

void LowlatencySequencer::RunReader() {

//LOG(ERROR) << "In sequencer:  Starting sequencer reader.";
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

  uint64 local_paxos_leader_ = local_replica * configuration_->nodes_per_replica();

  while (start_working_ != true) {
    usleep(100);
  }

  while (!deconstructor_invoked_) {

    bool got_message = connection_->GotMessage("sequencer_", &message);
    if (got_message == true) {
      if (message.type() == MessageProto::TXN_BATCH) {
//LOG(ERROR) << configuration_->local_node_id()<< ":In sequencer reader:  recevie TXN_BATCH message:"<<message.batch_number();
        batch_number = message.batch_number();

        //  If (This batch come from this replica) → send BATCH_SUBMIT to the the master node of the local paxos participants; Ignore the new_generated txns
        if (configuration_->LookupReplica(message.source_node()) == local_replica && message.misc_bool(0) == true) {
          // Send “BATCH_SUBMIT” to the head machines;
          MessageProto batch_submit_message;
          batch_submit_message.set_destination_channel("sequencer_");
          batch_submit_message.set_destination_node(local_paxos_leader_);
          batch_submit_message.set_type(MessageProto::BATCH_SUBMIT);
          batch_submit_message.add_misc_int(message.batch_number());
          connection_->Send(batch_submit_message);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader: send BATCH_SUBMIT, id:"<<message.batch_number();    
        }

        // If received TXN_BATCH: Parse batch and forward sub-batches to relevant readers (same replica only).
        for (int i = 0; i < message.data_size(); i++) {
          TxnProto txn;
          txn.ParseFromString(message.data(i));

          if (txn.fake_txn() == true) {
            continue;
          }
          // Compute readers & writers; store in txn proto.
          set<uint64> readers;
          set<uint64> writers;
          for (uint32 i = 0; i < (uint32)(txn.read_set_size()); i++) {
            if (configuration_->LookupMaster(txn.read_set(i)) == txn.origin_replica()) {
              uint64 mds = configuration_->LookupPartition(txn.read_set(i));
              readers.insert(mds);
            }
          }
          for (uint32 i = 0; i < (uint32)(txn.write_set_size()); i++) {
            if (configuration_->LookupMaster(txn.write_set(i)) == txn.origin_replica()) {
              uint64 mds = configuration_->LookupPartition(txn.write_set(i));
              writers.insert(mds);
            }
          }
          for (uint32 i = 0; i < (uint32)(txn.read_write_set_size()); i++) {
            if (configuration_->LookupMaster(txn.read_write_set(i)) == txn.origin_replica()) {
              uint64 mds = configuration_->LookupPartition(txn.read_write_set(i));
              writers.insert(mds);
              readers.insert(mds);
            } else {
LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader:  wrong3, LookupMaster:"<< configuration_->LookupMaster(txn.read_write_set(i)) << "  txn.origin: "<<txn.origin_replica();
}
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

        // Forward "relevant multi-replica action" to the head node
        if (configuration_->LookupReplica(message.source_node()) == 0 && local_replica != 0) {
          MessageProto mr_message;
          for (int i = 0; i < message.data_size(); i++) {
            TxnProto txn;
            txn.ParseFromString(message.data(i));
            if (txn.involved_replicas_size() > 1 && txn.new_generated() == false) {
              for (int j = 0; j < txn.involved_replicas_size(); j++) {
                if (txn.involved_replicas(j) == local_replica) {
                  string txn_data;
                  txn.SerializeToString(&txn_data);
                  mr_message.add_data(txn_data);
                }
              }
            }
          }

          mr_message.set_destination_channel("paxos_log_");
          mr_message.set_destination_node(local_paxos_leader_);
          mr_message.set_type(MessageProto::MR_TXNS_BATCH);
          mr_message.set_source_node(message.source_node());
          mr_message.add_misc_int(message.batch_number());
          connection_->Send(mr_message);
        }

      } else if (message.type() == MessageProto::BATCH_SUBMIT) {
        uint64 batch_id = message.misc_int(0);
        paxos_log_->Append(batch_id);
//if (configuration_->local_node_id() == 0)
//LOG(ERROR) << configuration_->local_node_id()<<": In sequencer reader: append batch_id:"<<batch_id;
      }
    }

    if (got_message == false) {
      usleep(50);
    }
  }
}
