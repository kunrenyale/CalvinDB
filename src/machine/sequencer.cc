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

Sequencer::Sequencer(ClusterConfig* conf, Connection* connection, Client* client, Paxos* paxos, uint32 max_batch_size)
          : epoch_duration_(0.01), configuration_(conf), connection_(connection),
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

  // Synchronization loadgen start with other sequencers.
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < (uint32)(configuration_->all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(configuration_->local_node_id()))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint32)(configuration_->all_nodes_size())) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
  std::cout << "Starting sequencer.\n" << std::flush;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("SequencerReader");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);
  uint64 batch_number;
  uint32 txn_id_offset;

  while (!deconstructor_invoked_) {
    // Begin epoch.
    batch_number = configuration_->GetGUID();
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

    // Collect txn requests for this epoch.
    txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if ((uint32)(batch.data_size()) < max_batch_size_) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * max_batch_size_ + txn_id_offset);

        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        txn_id_offset++;
        delete txn;
      } else {
        usleep(20);
      }
    }

    // Send this epoch's requests to Paxos service.
    batch.SerializeToString(&batch_string);

    {
      Lock l(&batch_mutex_);
      batch_queue_.push(batch_string);
    }
  }

  Spin(1);
}

void Sequencer::RunReader() {
  Spin(1);


  // Set up batch messages for each system node.
  map<uint64, MessageProto> batches;
  for (map<uint64, MachineInfo>::iterator it = configuration_->machines_.begin();
       it != configuration_->machines_.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_SUBBATCH);
  }

  bool found_new_batch = false;
  string batch_string;
  MessageProto batch_message;
  MessageProto message;
  uint64 batch_number;

  while (!deconstructor_invoked_) {

    //1. Get batch from Sequencer reader, send it to its central machine   
    {
      Lock l(&batch_mutex_);
      if (batch_queue_.size()) {
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        batch_message.ParseFromString(batch_string);
        found_new_batch = true;
      }
    }

    if (found_new_batch == true) {
      for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
        uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_message.batch_number()), i);
        batch_message.set_destination_node(machine_id);
        batch_message.set_type(MessageProto::TXN_BATCH);
        batch_message.set_destination_channel("SequencerReader");
        connection_->Send(batch_message);
      }
      found_new_batch = false;  
    }

    bool got_message = connection_->GetMessage(&message);
    if (got_message == true) {
      if (message.type() == MessageProto::TXN_BATCH) {

        batch_number = message.batch_number();
        // If received TXN_BATCH: Parse batch and forward sub-batches to relevant readers (same replica only).
        for (int i = 0; i < message.data_size(); i++) {
          TxnProto txn;
          txn.ParseFromString(batch_message.data(i));

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
          connection_->Send(it->second);
          it->second.clear_data();
        }

        // Send “vote” to the head machines;
        MessageProto vote_message;
        vote_message.set_destination_channel("SequencerReader");
        vote_message.set_destination_node(0);
        vote_message.set_type(MessageProto::BATCH_VOTE);
        vote_message.add_misc_int(message.batch_number());
        connection_->Send(vote_message);

      } else if (message.type() == MessageProto::BATCH_VOTE) {
        uint64 batch_id = message.misc_int(0);
        uint32 votes;
        {
          Lock l(&batch_votes_mutex_);
          votes = ++batch_votes_[batch_id];

          // Remove from map if all servers are accounted for.
          if (votes == configuration_->replicas_size()) {
            batch_votes_.erase(batch_id);
          }
        }

        // If block is now written to (exactly) a majority of replicas, submit
        // to paxos leader.
        if (votes == configuration_->replicas_size() / 2 + 1) {
          paxos_log_->Append(batch_id);
        }        
      }
    }

    if (found_new_batch == false && got_message == false) {
      usleep(1000);
    }
  }
}
