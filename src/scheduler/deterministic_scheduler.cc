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
                                               ConnectionMultiplexer* connection)
    : configuration_(conf), storage_(storage), application_(application),connection_(connection) {
  
  ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
  txns_queue = new AtomicQueue<TxnProto*>();
  done_queue = new AtomicQueue<TxnProto*>();

  connection_->NewChannel("scheduler_");

  for (int i = 0; i < NUM_THREADS; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    connection_->NewChannel(channel);
  }

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
  Spin(1);
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<string, StorageManager*> active_txns;

  string channel("scheduler");
  channel.append(IntToString(thread));

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
      if (manager->ReadyToExecute()) {
        // Execute and clean up.
        TxnProto* txn = manager->txn_;
        scheduler->application_->Execute(txn, manager);
        delete manager;

        scheduler->connection_->UnlinkChannel(IntToString(txn->txn_id()));
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
/**if (txn->txn_type() == 2 && (scheduler->configuration_->local_node_id() == 2 || scheduler->configuration_->local_node_id() == 3))
LOG(ERROR) <<scheduler->configuration_->local_node_id()<< ":----In worker: find "<<txn->txn_id();**/
        // Create manager.
        StorageManager* manager = new StorageManager(scheduler->configuration_,
                                      scheduler->connection_,
                                      scheduler->storage_, txn);

        // Writes occur at this node.
        if (manager->ReadyToExecute()) {
          // No remote reads. Execute and clean up.
          scheduler->application_->Execute(txn, manager);
          delete manager;

          // Respond to scheduler;
          scheduler->done_queue->Push(txn);
        } else {
          scheduler->connection_->LinkChannel(IntToString(txn->txn_id()), channel);
          // There are outstanding remote reads.
          active_txns[IntToString(txn->txn_id())] = manager;
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
uint32 current_sequence_id_ = 0;
uint32 current_sequence_batch_index_ = 0;;
uint64 current_batch_id_ = 0;

MessageProto* GetBatch(ConnectionMultiplexer* connection) {
   if (current_sequence_ == NULL) {
     if (global_batches_order.count(current_sequence_id_) > 0) {
       MessageProto* sequence_message = global_batches_order[current_sequence_id_];
       current_sequence_ = new Sequence();
       current_sequence_->ParseFromString(sequence_message->data(0));
       current_sequence_batch_index_ = 0;
//LOG(ERROR) << sequence_message->destination_node()<< ":In scheduler:  find the sequence: "<<current_sequence_id_;
       delete sequence_message;
       global_batches_order.erase(current_sequence_id_);
     } else {
       // Receive the batch data or global batch order
       MessageProto* message = new MessageProto();
       while (connection->GotMessage("scheduler_", message)) {
         if (message->type() == MessageProto::TXN_SUBBATCH) {
//LOG(ERROR) << "In scheduler:  receive a subbatch: "<<message->batch_number();
           batches_data[message->batch_number()] = message;
           message = new MessageProto();
         } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
           if (message->misc_int(0) == current_sequence_id_) {
//LOG(ERROR) << message->destination_node()<< ":In scheduler:  find the sequence: "<<message->misc_int(0);
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
   if (++current_sequence_batch_index_ >= (uint32)(current_sequence_->batch_ids_size())) {
     delete current_sequence_;
     current_sequence_ = NULL;
     current_sequence_id_++;
//LOG(ERROR) << "^^^^^In scheduler:  will work on next sequence: "<<current_sequence_id_;
   }


   if (batches_data.count(current_batch_id_) > 0) {
     // Requested batch has already been received.
     MessageProto* batch = batches_data[current_batch_id_];
     batches_data.erase(current_batch_id_);
//LOG(ERROR) << "^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;
     return batch; 
   } else {
     // Receive the batch data or global batch order
     MessageProto* message = new MessageProto();
     while (connection->GotMessage("scheduler_", message)) {
       if (message->type() == MessageProto::TXN_SUBBATCH) {
//LOG(ERROR) << "In scheduler:  receive a subbatch: "<<message->batch_number();
         if ((uint64)(message->batch_number()) == current_batch_id_) {
//LOG(ERROR) << "^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;
           return message;
         } else {
           batches_data[message->batch_number()] = message;
           message = new MessageProto();
         }
       } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
//LOG(ERROR)<< message->destination_node()<< ":In scheduler:  receive a sequence: "<<message->misc_int(0);
         global_batches_order[message->misc_int(0)] = message;
         message = new MessageProto();
       }
     }
     delete message;
     return NULL;
   }
}

void* DeterministicScheduler::LockManagerThread(void* arg) {
  Spin(1);
  DeterministicScheduler* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);

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

      if(done_txn->writers_size() == 0 || rand() % done_txn->writers_size() == 0)
        txns++;       

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
/**if (batch_message != NULL && (machine_id == 2 || machine_id == 3)) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch: "<<batch_message->batch_number();
}**/
    // Done with current batch, get next.
    } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        delete batch_message;
        batch_message = GetBatch(scheduler->connection_);
/**if (batch_message != NULL && (machine_id == 2 || machine_id == 3)) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch: "<<batch_message->batch_number();
}**/
    // Current batch has remaining txns, grab up to 10.
    } else if (executing_txns + pending_txns < 200) {
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
      }
    }

    // Start executing any and all ready transactions to get them off our plate
    while (!scheduler->ready_txns_->empty()) {
      TxnProto* txn = scheduler->ready_txns_->front();
      scheduler->ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;

      scheduler->txns_queue->Push(txn);
//if (machine_id > 7)
//LOG(ERROR) <<machine_id<< ":In LockManagerThread:  Start executing the ready txn: "<<txn->txn_id();
    }

    // Report throughput.
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      LOG(ERROR) << "Machine: "<<machine_id<<" Completed "<< (static_cast<double>(txns) / total_time)
                 << " txns/sec, "<< executing_txns << " executing, "<< pending_txns << " pending"<<"  . Reived(chan):"<<scheduler->connection_->receive_channel_remote_result<<"  . Received(un):"<<scheduler->connection_->receive_undeliver_remote_result<<"  .send:"<<scheduler->connection_->send_remote_result;

      // Reset txn count.
      time = GetTime();
      txns = 0;
    }
  }
  return NULL;
}

