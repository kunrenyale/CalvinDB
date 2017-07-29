// Author: Alex Thomson (thomson@cs.yale.edu)
//
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//

#include "scheduler/deterministic_scheduler.h"
#include "machine/sequencer.h"  //  LATENCY_TEST
#include <fstream>
#include <iostream>

using std::pair;
using std::string;
using zmq::socket_t;
using std::map;


DeterministicScheduler::DeterministicScheduler(ClusterConfig* conf,
                                               Storage* storage,
                                               const Application* application,
                                               ConnectionMultiplexer* connection,
                                               uint32 mode)
    : configuration_(conf), storage_(storage), application_(application),connection_(connection), mode_(mode) {
  
  ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_, mode_);
  txns_queue = new AtomicQueue<TxnProto*>();
  done_queue = new AtomicQueue<TxnProto*>();

  connection_->NewChannel("scheduler_");

  for (int i = 0; i < NUM_THREADS; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    connection_->NewChannel(channel);
  }

  start_working_ = false;

  // start lock manager thread
  cpu_set_t cpuset;
  pthread_attr_t attr1;
  pthread_attr_init(&attr1);
  CPU_ZERO(&cpuset);
  CPU_SET(7, &cpuset);
  pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset); 
  pthread_create(&lock_manager_thread_, &attr1, LockManagerThread, reinterpret_cast<void*>(this));

  // Start all worker threads.
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    if (i == 0 || i == 1) {
      CPU_SET(i, &cpuset);
    } else {
      CPU_SET(i+2, &cpuset);
    }

    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, DeterministicScheduler*>(i, this)));
  }
}


void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<string, StorageManager*> active_txns;

  string channel("scheduler");
  channel.append(IntToString(thread));

  uint32 mode = scheduler->mode_;

  while (scheduler->start_working_ != true) {
    usleep(100);
  }

  // Begin main loop.
  MessageProto message;
  while (true) {
    bool got_message = scheduler->connection_->GotMessage(channel, &message);
    if (got_message == true) {
      // Remote read result.
      CHECK(message.type() == MessageProto::READ_RESULT);
      CHECK(active_txns.count(message.destination_channel()) > 0);
      StorageManager* manager = active_txns[message.destination_channel()];
      manager->HandleReadResult(message);
//LOG(ERROR) <<scheduler->configuration_->local_node_id()<< ":In worker: received remote read "<<manager->txn_->txn_id();
      if (manager->ReadyToExecute()) {
        // Execute and clean up.
        TxnProto* txn = manager->txn_;
        scheduler->application_->Execute(txn, manager);
        delete manager;

        scheduler->connection_->UnlinkChannel(message.destination_channel());
        active_txns.erase(message.destination_channel());
        // Respond to scheduler;
        scheduler->done_queue->Push(txn);
/**if (scheduler->configuration_->local_node_id() == 2 || scheduler->configuration_->local_node_id() == 3)
LOG(ERROR) <<scheduler->configuration_->local_node_id()<< ":In worker: finish "<<txn->txn_id();**/
      }
    } else {
      // No remote read result found, start on next txn if one is waiting.
      TxnProto* txn;
      bool got_it = scheduler->txns_queue->Pop(&txn);
      if (got_it == true) {
//LOG(ERROR) <<scheduler->configuration_->local_node_id()<< ":----In worker: find "<<txn->txn_id();
        // Create manager.
        StorageManager* manager = new StorageManager(scheduler->configuration_,
                                      scheduler->connection_,
                                      scheduler->storage_, txn, mode);

        // Writes occur at this node.
        if (manager->ReadyToExecute()) {
          // No remote reads. Execute and clean up.
          scheduler->application_->Execute(txn, manager);
          delete manager;

          // Respond to scheduler;
          scheduler->done_queue->Push(txn);
        } else {
//LOG(ERROR) <<scheduler->configuration_->local_node_id()<< ":~~~~~~~~~~~~~~~In worker: not ready  "<<txn->txn_id();
          string origin_channel = IntToString(txn->txn_id()) + "-" + IntToString(txn->origin_replica());
          scheduler->connection_->LinkChannel(origin_channel, channel);
          // There are outstanding remote reads.
          active_txns[origin_channel] = manager;
        }
      }
    }
  }
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
  pthread_join(lock_manager_thread_, NULL);

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads_[i], NULL);
  }
}


unordered_map<uint64, MessageProto*> batches_data;
unordered_map<uint64, MessageProto*> global_batches_order;

Sequence* current_sequence_ = NULL;
uint32 current_sequence_id_ = 1;
uint32 current_sequence_batch_index_ = 0;
uint64 current_batch_id_ = 0;

MessageProto* GetBatch(ConnectionMultiplexer* connection) {
   if (current_sequence_ == NULL) {
     if (global_batches_order.count(current_sequence_id_) > 0) {
       MessageProto* sequence_message = global_batches_order[current_sequence_id_];
       current_sequence_ = new Sequence();
       current_sequence_->ParseFromString(sequence_message->data(0));
       current_sequence_batch_index_ = 0;
//if (sequence_message->destination_node() == 0)
LOG(ERROR) << sequence_message->destination_node()<< ":In scheduler:  find the sequence: "<<current_sequence_id_;
       delete sequence_message;
       global_batches_order.erase(current_sequence_id_);
     } else {
       // Receive the batch data or global batch order
       MessageProto* message = new MessageProto();
       while (connection->GotMessage("scheduler_", message)) {
         if (message->type() == MessageProto::TXN_SUBBATCH) {
LOG(ERROR) << message->destination_node()<<"In scheduler:  receive a subbatch: "<<message->batch_number();
//CHECK(message->data_size() > 0);
           batches_data[message->batch_number()] = message;
           message = new MessageProto();
         } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
LOG(ERROR)<< message->destination_node()<< ":In scheduler:  receive a sequence: "<<message->misc_int(0);
           if (message->misc_int(0) == current_sequence_id_) {
//if (message->destination_node() == 0)
LOG(ERROR) << message->destination_node()<< ":In scheduler:  find the sequence: "<<message->misc_int(0);
             current_sequence_ = new Sequence();
             current_sequence_->ParseFromString(message->data(0));
             current_sequence_batch_index_ = 0;
             break;
           } else {
             global_batches_order[message->misc_int(0)] = message;
             message = new MessageProto();
           }       
         }
       }
       delete message;
     }
   }

   if (current_sequence_ == NULL) {
     return NULL;
   }
   
   CHECK(current_sequence_batch_index_ < (uint32)(current_sequence_->batch_ids_size()));
   current_batch_id_ = current_sequence_->batch_ids(current_sequence_batch_index_);


   if (batches_data.count(current_batch_id_) > 0) {
     // Requested batch has already been received.
     MessageProto* batch = batches_data[current_batch_id_];
     batches_data.erase(current_batch_id_);
LOG(ERROR) <<batch->destination_node()<< ": ^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;
   
     if (++current_sequence_batch_index_ >= (uint32)(current_sequence_->batch_ids_size())) {
       delete current_sequence_;
       current_sequence_ = NULL;
       current_sequence_id_++;
LOG(ERROR) << "^^^^^In scheduler:  will work on next sequence: "<<current_sequence_id_;
     }

     return batch; 
   } else {
     // Receive the batch data or global batch order
     MessageProto* message = new MessageProto();
     while (connection->GotMessage("scheduler_", message)) {
       if (message->type() == MessageProto::TXN_SUBBATCH) {
LOG(ERROR) << message->destination_node()<<"In scheduler:  receive a subbatch: "<<message->batch_number();
//CHECK(message->data_size() > 0);
         if ((uint64)(message->batch_number()) == current_batch_id_) {
LOG(ERROR) << message->destination_node()<<": ^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;

           if (++current_sequence_batch_index_ >= (uint32)(current_sequence_->batch_ids_size())) {
             delete current_sequence_;
             current_sequence_ = NULL;
             current_sequence_id_++;
LOG(ERROR) << "^^^^^In scheduler:  will work on next sequence: "<<current_sequence_id_;
           }

           return message;
         } else {
           batches_data[message->batch_number()] = message;
           message = new MessageProto();
         }
       } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
LOG(ERROR)<< message->destination_node()<< ":In scheduler:  receive a sequence: "<<message->misc_int(0);
         global_batches_order[message->misc_int(0)] = message;
         message = new MessageProto();
       }
     }
     delete message;
     return NULL;
   }
}

void* DeterministicScheduler::LockManagerThread(void* arg) {

  DeterministicScheduler* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);

    // Synchronization loadgen start with other machines.
  scheduler->connection_->NewChannel("synchronization_scheduler_channel");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("synchronization_scheduler_channel");
  for (uint64 i = 0; i < (uint64)(scheduler->configuration_->all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint64>(scheduler->configuration_->local_node_id())) {
      scheduler->connection_->Send(synchronization_message);
    }
  }

  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint64)(scheduler->configuration_->all_nodes_size())) {
    synchronization_message.Clear();
    if (scheduler->connection_->GotMessage("synchronization_scheduler_channel", &synchronization_message)) {
      CHECK(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  scheduler->connection_->DeleteChannel("synchronization_scheduler_channel");
LOG(ERROR) << "In LockManagerThread:  After synchronization. Starting scheduler thread.";

  scheduler->start_working_ = true;

  // Run main loop.
  MessageProto message;
  MessageProto* batch_message = NULL;
  int txns = 0;
  double time = GetTime();
  int executing_txns = 0;
  int pending_txns = 0;
  int batch_offset = 0;
  uint64 machine_id = scheduler->configuration_->local_node_id();

  while (true) {
    TxnProto* done_txn;
    while (scheduler->done_queue->Pop(&done_txn) == true) {
      // We have received a finished transaction back, release the lock
      scheduler->lock_manager_->Release(done_txn);
      executing_txns--;

      if(done_txn->writers_size() == 0 || (rand() % done_txn->writers_size() == 0 && rand() % done_txn->involved_replicas_size() == 0)) {
        txns++;       
      }
LOG(ERROR) <<machine_id<< ":*********In LockManagerThread:  Finish executing the  txn: "<<done_txn->txn_id()<<"  origin:"<<done_txn->origin_replica();
#ifdef LATENCY_TEST
    if (done_txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES && done_txn->origin_machine() == machine_id) {
      scheduler_unlock[done_txn->txn_id()] = GetTime();
      latency_counter++;
      measured_latency.push_back(scheduler_unlock[done_txn->txn_id()] - sequencer_recv[done_txn->txn_id()]);
    }

    // Write out the latency results
    if (latency_counter == SAMPLES) {
      latency_counter++;
      string report;
      for (uint64 i = 100; i < measured_latency.size(); i++) {
        report.append(DoubleToString(measured_latency[i]*1000) + "\n");
      }

      string filename = "/tmp/report." + UInt64ToString(machine_id);

      std::ofstream file(filename);
      file << report;

LOG(ERROR) << machine_id<<": reporting latencies to " << filename;

    }
#endif
      delete done_txn;
    }

    // Have we run out of txns in our batch? Let's get some new ones.
    if (batch_message == NULL) {
      batch_message = GetBatch(scheduler->connection_);
if (batch_message != NULL) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch(1): "<<batch_message->batch_number()<<" size:"<<batch_message->data_size();
} 
    // Done with current batch, get next.
    } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        delete batch_message;
        batch_message = GetBatch(scheduler->connection_);
if (batch_message != NULL) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch(2): "<<batch_message->batch_number()<<" size:"<<batch_message->data_size();
} 
    // Current batch has remaining txns, grab up to 10.
    } else if (executing_txns + pending_txns < 2000) {
      for (int i = 0; i < 100; i++) {
        if (batch_offset >= batch_message->data_size()) {
          // Oops we ran out of txns in this batch. Stop adding txns for now.
          break;
        }
        TxnProto* txn = new TxnProto();
        txn->ParseFromString(batch_message->data(batch_offset));
        batch_offset++;

        scheduler->lock_manager_->Lock(txn);
        pending_txns++;
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a txn: "<<txn->txn_id()<<"  origin:"<<txn->origin_replica();
      }
    }

    // Start executing any and all ready transactions to get them off our plate
    while (!scheduler->ready_txns_->empty()) {
      TxnProto* txn = scheduler->ready_txns_->front();
      scheduler->ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;

      scheduler->txns_queue->Push(txn);

LOG(ERROR) <<machine_id<< ":In LockManagerThread:  Start executing the ready txn: "<<txn->txn_id()<<"  origin:"<<txn->origin_replica();
    }

    // Report throughput.
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      LOG(ERROR) << "Machine: "<<machine_id<<" Completed "<< (static_cast<double>(txns) / total_time)
                 << " txns/sec, "<< executing_txns << " executing, "<< pending_txns << " pending";

      // Reset txn count.
      time = GetTime();
      txns = 0;
    }
  }
  return NULL;
}

