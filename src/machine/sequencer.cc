// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//

#include "machine/sequencer.h"

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

Sequencer::Sequencer(ClusterConfig* conf, Connection* send_connection, Connection* receive_connection, Client* client, Paxos* paxos, uint32 max_batch_size)
          : epoch_duration_(0.01), configuration_(conf), sending_connection_(send_connection), receiving_connection_(receive_connection),
          client_(client), deconstructor_invoked_(false), paxos_log_(paxos), max_batch_size_(max_batch_size) {
  // Start Sequencer main loops running in background thread.

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

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}


void Sequencer::RunWriter() {
  Spin(1);

//LOG(ERROR) << "In sequencer:  Starting sequencer writer.";

  // Set up batch messages for each system node.
  MessageProto batch_message;
  batch_message.set_destination_channel("sequencer_receiving");
  batch_message.set_type(MessageProto::TXN_BATCH);
  uint64 batch_number;
  uint32 txn_id_offset;

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

        txn->SerializeToString(&txn_string);
        batch_message.add_data(txn_string);
        txn_id_offset++;
        delete txn;
      } else {
        usleep(20);
      }
    }

    // Send this epoch's transactions to the central machine of each replica
    for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
      uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_message.batch_number()), i);
LOG(ERROR) << "In sequencer reader:  will send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
      batch_message.set_destination_node(machine_id);
      sending_connection_->Send(batch_message);
    }
  }

  Spin(1);
}

void Sequencer::RunReader() {
  Spin(1);

//LOG(ERROR) << "In sequencer:  Starting sequencer reader.";
  // Set up batch messages for each system node.
  map<uint64, MessageProto> batches;
  for (map<uint64, MachineInfo>::iterator it = configuration_->machines_.begin();
       it != configuration_->machines_.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_SUBBATCH);
  }

  MessageProto message;
  uint64 batch_number;

  while (!deconstructor_invoked_) {

    bool got_message = receiving_connection_->GetMessage(&message);
    if (got_message == true) {
      if (message.type() == MessageProto::TXN_BATCH) {
LOG(ERROR) << "In sequencer reader:  recevie TXN_BATCH message:"<<message.batch_number();
        batch_number = message.batch_number();
        // If received TXN_BATCH: Parse batch and forward sub-batches to relevant readers (same replica only).
        for (int i = 0; i < message.data_size(); i++) {
          TxnProto txn;
          txn.ParseFromString(message.data(i));

          // Compute readers & writers; store in txn proto.
          set<uint64> readers;
          set<uint64> writers;
          for (uint32 i = 0; i < (uint32)(txn.read_set_size()); i++)
            readers.insert(configuration_->LookupPartition(txn.read_set(i)));
          for (uint32 i = 0; i < (uint32)(txn.write_set_size()); i++)
            writers.insert(configuration_->LookupPartition(txn.write_set(i)));
          for (uint32 i = 0; i < (uint32)(txn.read_write_set_size()); i++) {
            writers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
            readers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
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
          sending_connection_->Send(it->second);
          it->second.clear_data();
        }

        // Send “vote” to the head machines;
        MessageProto vote_message;
        vote_message.set_destination_channel("sequencer_receiving");
        vote_message.set_destination_node(0);
        vote_message.set_type(MessageProto::BATCH_VOTE);
        vote_message.add_misc_int(message.batch_number());
        sending_connection_->Send(vote_message);

      } else if (message.type() == MessageProto::BATCH_VOTE) {
LOG(ERROR) << "In sequencer reader:  recevie BATCH_VOTE message:"<<message.misc_int(0);
        uint64 batch_id = message.misc_int(0);
        uint32 votes;

        votes = ++batch_votes_[batch_id];

        // Remove from map if all servers are accounted for.
        if (votes == configuration_->replicas_size()) {
          batch_votes_.erase(batch_id);
        }

        // If block is now written to (exactly) a majority of replicas, submit
        // to paxos leader.
        if (votes == configuration_->replicas_size() / 2 + 1) {
LOG(ERROR) << "In sequencer reader:  recevie BATCH_VOTE message, will append:"<<batch_id;
          paxos_log_->Append(batch_id);
        }        
      }
    }

    if (got_message == false) {
      usleep(100);
    }
  }
}
