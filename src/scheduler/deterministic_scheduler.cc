// Author: Kun Ren <renkun.nwpu@gmail.com>
//         Alex Thomson (thomson@cs.yale.edu)
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
using std::map;

//----------------------------------------------------------------  constructor --------------------------------------------------------------------
DeterministicScheduler::DeterministicScheduler(ClusterConfig* conf,
                                               Storage* storage,
                                               const Application* application,
                                               ConnectionMultiplexer* connection,
                                               uint32 mode)
    : configuration_(conf), storage_(storage), application_(application),connection_(connection), mode_(mode) {
  
  ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_, mode_);
  txns_queue_ = new AtomicQueue<TxnProto*>();
  done_queue_ = new AtomicQueue<TxnProto*>();

  connection_->NewChannel("scheduler_");

  for (int i = 0; i < NUM_THREADS; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    connection_->NewChannel(channel);
  }

  start_working_ = false;

  // For GetBatch method
  current_sequence_ = NULL;
  current_sequence_id_ = 1;
  current_sequence_batch_index_ = 0;
  current_batch_id_ = 0;

  // start lock manager thread
  cpu_set_t cpuset;
  pthread_attr_t attr1;
  pthread_attr_init(&attr1);
  CPU_ZERO(&cpuset);
  CPU_SET(5, &cpuset);
  pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset); 
  pthread_create(&lock_manager_thread_, &attr1, LockManagerThread, reinterpret_cast<void*>(this));

  // Start all worker threads.
  for (uint32 i = 0; i < NUM_THREADS; i++) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    if (i == 0 || i == 1) {
      CPU_SET(i, &cpuset);
    } else {
      CPU_SET(i+2, &cpuset);
    }

    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(threads_[i]), &attr, WorkerThread, reinterpret_cast<void*>(new pair<uint32, DeterministicScheduler*>(i, this)));
  }
}

void* DeterministicScheduler::LockManagerThread(void *arg) {
  reinterpret_cast<DeterministicScheduler*>(arg)->RunLockManagerThread();
  return NULL;
}

void* DeterministicScheduler::WorkerThread(void *arg) {
  (reinterpret_cast<pair<uint32, DeterministicScheduler*>*>(arg))->second->RunWorkerThread((reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg))->first);
  return NULL;
}

//----------------------------------------------------------------  RunWorkerThread --------------------------------------------------------------------
void DeterministicScheduler::RunWorkerThread(uint32 thread) {
  unordered_map<string, StorageManager*> active_txns;

  string channel("scheduler");
  channel.append(IntToString(thread));

  while (start_working_ != true) {
    usleep(100);
  }

  // Begin main loop.
  MessageProto message;
  while (true) {
    bool got_message = connection_->GotMessage(channel, &message);
    if (got_message == true) {
      if (message.type() == MessageProto::READ_RESULT) {
//LOG(ERROR) <<configuration_->local_node_id()<<" :"<<message.destination_channel()<<" :In RunWorkerThread:  received READ_RESULT";
        // Remote read result.
        CHECK(active_txns.count(message.destination_channel()) > 0);
        StorageManager* manager = active_txns[message.destination_channel()];
        // Check whether it already got the decision
        if (mode_ == 2) {
          CHECK(manager->ReachedDecision() == true);
        }

        manager->HandleReadResult(message);

        if (manager->ReadyToExecute()) {
          // Execute and clean up.
          TxnProto* txn = manager->txn_;
          application_->Execute(txn, manager);
          delete manager;

          connection_->UnlinkChannel(message.destination_channel());
          active_txns.erase(message.destination_channel());

          // Respond to scheduler;
          txn->set_status(TxnProto::COMMITTED);
          done_queue_->Push(txn);
        }
      } else if (message.type() == MessageProto::LOCAL_ENTRIES_TO_MIN_MACHINE) {
        // message sent from non-min machines to min-machine
        CHECK(active_txns.count(message.destination_channel()) > 0);
        StorageManager* manager = active_txns[message.destination_channel()];
        manager->HandleRemoteEntries(message);

        if (manager->AbortTxn()) {
CHECK(true == false);
          connection_->UnlinkChannel(message.destination_channel());
          active_txns.erase(message.destination_channel());
          // Respond to scheduler;

          manager->txn_->set_status(TxnProto::ABORTED);
          done_queue_->Push(manager->txn_);
          delete manager;     
        } else {
          // will commit this txn and send local results
          manager->SendLocalResults();       
        }
        
      } else if (message.type() == MessageProto::COMMIT_OR_ABORT_DECISION) {
        // min-machine sent decision messages to all non-min machines
        CHECK(active_txns.count(message.destination_channel()) > 0);
        StorageManager* manager = active_txns[message.destination_channel()];

        manager->UpdateReachedDecision();
        bool commit = message.misc_bool(0);
        if (commit == true) {
          manager->SendLocalResults();
//LOG(ERROR) <<configuration_->local_node_id()<<" :"<<manager->txn_->txn_id() <<" :In RunWorkerThread:  will commit, will SendLocalResults()";
        } else {
//LOG(ERROR) <<configuration_->local_node_id()<<" :"<<manager->txn_->txn_id() <<" :In RunWorkerThread:  will abort";
          connection_->UnlinkChannel(message.destination_channel());
          active_txns.erase(message.destination_channel());
          // Respond to scheduler;

          manager->txn_->set_status(TxnProto::ABORTED);
          done_queue_->Push(manager->txn_);
          delete manager;        
        }
      }
    } else {
      // No remote read result found, start on next txn if one is waiting.
      TxnProto* txn;
      bool got_it = txns_queue_->Pop(&txn);
      if (got_it == true) {
        // Create manager.
        StorageManager* manager = new StorageManager(configuration_,
                                      connection_,
                                      storage_, txn, mode_);

        // Writes occur at this node.
        if (manager->ReadyToExecute()) {
          // No remote reads. Execute and clean up.
          application_->Execute(txn, manager);
          delete manager;

          // Respond to scheduler;
          txn->set_status(TxnProto::COMMITTED);
          done_queue_->Push(txn);
        } else {
          string origin_channel = IntToString(txn->txn_id()) + "-" + IntToString(txn->origin_replica());
          connection_->LinkChannel(origin_channel, channel);
          // There are outstanding remote reads.
          active_txns[origin_channel] = manager;
        }
      }
    }
  }
  return ;
}

DeterministicScheduler::~DeterministicScheduler() {
  pthread_join(lock_manager_thread_, NULL);

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads_[i], NULL);
  }
}

//----------------------------------------------------------------  GetBatch --------------------------------------------------------------------
// Get the next batch deterministicly
MessageProto* DeterministicScheduler::GetBatch() {
   if (current_sequence_ == NULL) {
     if (global_batches_order_.count(current_sequence_id_) > 0) {
       MessageProto* sequence_message = global_batches_order_[current_sequence_id_];
       current_sequence_ = new Sequence();
       current_sequence_->ParseFromString(sequence_message->data(0));
       current_sequence_batch_index_ = 0;
//if (sequence_message->destination_node() == 0)
//LOG(ERROR) << sequence_message->destination_node()<< ":In scheduler:  find the sequence: "<<current_sequence_id_;
       delete sequence_message;
       global_batches_order_.erase(current_sequence_id_);
     } else {
       // Receive the batch data or global batch order
       MessageProto* message = new MessageProto();
       while (connection_->GotMessage("scheduler_", message)) {
         if (message->type() == MessageProto::TXN_SUBBATCH) {
//LOG(ERROR) << message->destination_node()<<"In scheduler:  receive a subbatch: "<<message->batch_number();
//CHECK(message->data_size() > 0);
           batches_data_[message->batch_number()] = message;
           message = new MessageProto();
         } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
//LOG(ERROR)<< message->destination_node()<< ":In scheduler:  receive a sequence: "<<message->misc_int(0);
           if (message->misc_int(0) == current_sequence_id_) {
//if (message->destination_node() == 0)
//LOG(ERROR) << message->destination_node()<< ":In scheduler:  find the sequence: "<<message->misc_int(0);
             current_sequence_ = new Sequence();
             current_sequence_->ParseFromString(message->data(0));
             current_sequence_batch_index_ = 0;
             break;
           } else {
             global_batches_order_[message->misc_int(0)] = message;
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


   if (batches_data_.count(current_batch_id_) > 0) {
     // Requested batch has already been received.
     MessageProto* batch = batches_data_[current_batch_id_];
     batches_data_.erase(current_batch_id_);
//LOG(ERROR) <<batch->destination_node()<< ": ^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;
   
     if (++current_sequence_batch_index_ >= (uint32)(current_sequence_->batch_ids_size())) {
       delete current_sequence_;
       current_sequence_ = NULL;
       current_sequence_id_++;
//LOG(ERROR) << "^^^^^In scheduler:  will work on next sequence: "<<current_sequence_id_;
     }

     return batch; 
   } else {
     // Receive the batch data or global batch order
     MessageProto* message = new MessageProto();
     while (connection_->GotMessage("scheduler_", message)) {
       if (message->type() == MessageProto::TXN_SUBBATCH) {
//LOG(ERROR) << message->destination_node()<<"In scheduler:  receive a subbatch: "<<message->batch_number();
//CHECK(message->data_size() > 0);
         if ((uint64)(message->batch_number()) == current_batch_id_) {
//LOG(ERROR) << message->destination_node()<<": ^^^^^In scheduler:  got the batch_id wanted: "<<current_batch_id_;

           if (++current_sequence_batch_index_ >= (uint32)(current_sequence_->batch_ids_size())) {
             delete current_sequence_;
             current_sequence_ = NULL;
             current_sequence_id_++;
//LOG(ERROR) << "^^^^^In scheduler:  will work on next sequence: "<<current_sequence_id_;
           }

           return message;
         } else {
           batches_data_[message->batch_number()] = message;
           message = new MessageProto();
         }
       } else if (message->type() == MessageProto::PAXOS_BATCH_ORDER) {
//LOG(ERROR)<< message->destination_node()<< ":In scheduler:  receive a sequence: "<<message->misc_int(0);
         global_batches_order_[message->misc_int(0)] = message;
         message = new MessageProto();
       }
     }
     delete message;
     return NULL;
   }
}

bool  DeterministicScheduler::IsLocal(const Key& key) {
    return configuration_->LookupPartition(key) == configuration_->relative_node_id();
}

//----------------------------------------------------------------  VerifyStorageCounters --------------------------------------------------------------------
bool DeterministicScheduler::VerifyStorageCounters(TxnProto* txn, set<pair<string,uint64>>& keys) {
  bool can_execute_now = true;
  uint32 origin = txn->origin_replica();

  for (int i = 0; i < txn->read_set_size(); i++) {
    KeyEntry key_entry = txn->read_set(i);
    if (IsLocal(key_entry.key()) && key_entry.master() == origin) {
      pair<uint32, uint64> master_counter = storage_->GetMasterCounter(key_entry.key());
      
      if (key_entry.counter() > master_counter.second) {
        keys.insert(make_pair(key_entry.key(), key_entry.counter()));
        can_execute_now = false;    
      }
    }
  }

  for (int i = 0; i < txn->read_write_set_size(); i++) {
    KeyEntry key_entry = txn->read_write_set(i);
    if (IsLocal(key_entry.key()) && key_entry.master() == origin) {
      pair<uint32, uint64> master_counter = storage_->GetMasterCounter(key_entry.key());
      
      if (key_entry.counter() > master_counter.second) {
        keys.insert(make_pair(key_entry.key(), key_entry.counter()));
CHECK(true == false);
        can_execute_now = false;    
      }
    }
  }

  return can_execute_now;
}

//----------------------------------------------------------------  RunLockManagerThread --------------------------------------------------------------------
void DeterministicScheduler::RunLockManagerThread() {
  // Synchronization loadgen start with other machines.
  connection_->NewChannel("synchronization_scheduler_channel");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("synchronization_scheduler_channel");
  for (uint64 i = 0; i < (uint64)(configuration_->all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint64>(configuration_->local_node_id())) {
      connection_->Send(synchronization_message);
    }
  }

  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint64)(configuration_->all_nodes_size())) {
    synchronization_message.Clear();
    if (connection_->GotMessage("synchronization_scheduler_channel", &synchronization_message)) {
      CHECK(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  connection_->DeleteChannel("synchronization_scheduler_channel");
LOG(ERROR) << "In LockManagerThread:  After synchronization. Starting scheduler thread.";

  start_working_ = true;

#ifdef LATENCY_TEST
  uint32 local_replica = configuration_->local_replica_id();
  uint64 local_machine = configuration_->local_node_id();
#endif

  // Run main loop.
  MessageProto message;
  MessageProto* batch_message = NULL;
  int txns = 0;
  double time = GetTime();
  uint64 executing_txns = 0;
  uint64 pending_txns = 0;
  int batch_offset = 0;
  uint64 machine_id = configuration_->local_node_id();
  uint64 maximum_txns = 200000000;
  

  while (true) {
    TxnProto* done_txn;
    while (done_queue_->Pop(&done_txn) == true) {
      // Handle remaster transactions     
      if (mode_ == 2 && done_txn->remaster_txn() == true) {
        uint64 txn_id = done_txn->txn_id();
      
        // Check whether remaster txn can wake up some blocking txns
        KeyEntry key_entry = done_txn->read_write_set(0);
        pair <string, uint64> key_info = make_pair(key_entry.key(), key_entry.counter()+1);

        if (waiting_txns_by_key_.find(key_info) != waiting_txns_by_key_.end()) {
          vector<TxnProto*> blocked_txns = waiting_txns_by_key_[key_info];

          for (auto it = blocked_txns.begin(); it != blocked_txns.end(); it++) {
            TxnProto* a = *it;
            (waiting_txns_by_txnid_[txn_id]).erase(key_info);

            if ((waiting_txns_by_txnid_[txn_id]).size() == 0) {
              a->set_wait_for_remaster_pros(false);
              waiting_txns_by_txnid_.erase(txn_id);

              if (blocking_txns_[a->origin_replica()].front() == a) {
                ready_txns_->push_back(a); 
                blocking_txns_[a->origin_replica()].pop();

                while (!blocking_txns_[a->origin_replica()].empty() && blocking_txns_[a->origin_replica()].front()->wait_for_remaster_pros() == false) {
                  ready_txns_->push_back(blocking_txns_[a->origin_replica()].front()); 
                  blocking_txns_[a->origin_replica()].pop();
                }
              }
            }
          } // end for

          waiting_txns_by_key_.erase(key_info);
        } 
      } // end  if (mode_ == 2 && done_txn->remaster_txn() == true) 

      // We have received a finished transaction back, release the lock
      lock_manager_->Release(done_txn);
      executing_txns--;

      if((done_txn->status() == TxnProto::COMMITTED) && 
         (done_txn->writers_size() == 0 || (rand() % done_txn->writers_size() == 0 && rand() % done_txn->involved_replicas_size() == 0))) {
        txns++;       
      }
//LOG(ERROR) <<machine_id<< ":*********In LockManagerThread:  Finish executing the  txn: "<<done_txn->txn_id()<<"  origin:"<<done_txn->origin_replica();
#ifdef LATENCY_TEST
    if (done_txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES && done_txn->origin_replica() == local_replica && done_txn->generated_machine() == local_machine) {
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
      batch_message = GetBatch();
/**if (batch_message != NULL) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch(1): "<<batch_message->batch_number()<<" size:"<<batch_message->data_size();
} **/
    // Done with current batch, get next.
    } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        delete batch_message;
        batch_message = GetBatch();
/**if (batch_message != NULL) {
LOG(ERROR) <<machine_id<< ":In LockManagerThread:  got a batch(2): "<<batch_message->batch_number()<<" size:"<<batch_message->data_size();
}**/ 
    // Current batch has remaining txns, grab up to 10.
    } else if (executing_txns + pending_txns < maximum_txns) {
      for (int i = 0; i < 100; i++) {
        if (batch_offset >= batch_message->data_size()) {
          // Oops we ran out of txns in this batch. Stop adding txns for now.
          break;
        }

        TxnProto* txn = new TxnProto();
        txn->ParseFromString(batch_message->data(batch_offset));
        batch_offset++;
/**
        if (mode_ == 2 && txn->remaster_txn() == false) {
          blocking_txns_[txn->origin_replica()].push(txn);
          txn->set_wait_for_remaster_pros(true);

          // Check the mastership of the records without locking
          set<pair<string,uint64>> keys;
          bool can_execute_now = VerifyStorageCounters(txn, keys);
          if (can_execute_now == false) {
            // Put it into the queue and wait for the remaster action come
            waiting_txns_by_txnid_[txn->txn_id()] = keys;
            for (auto it = keys.begin(); it != keys.end(); it++) {
              waiting_txns_by_key_[*it].push_back(txn);
            }

             // Working on next txn
             continue;
          } else {
            txn->set_wait_for_remaster_pros(false);
            // It is the first txn and can be executed right now
            if (txn == blocking_txns_[txn->origin_replica()].front()) {
              blocking_txns_[txn->origin_replica()].pop();
            } else {
              // Working on next txn
              continue;
            }
          }
        } // end if (mode_ == 2)
**/
        lock_manager_->Lock(txn);
        pending_txns++;
      }
    }

    // Start executing any and all ready transactions to get them off our plate
    while (!ready_txns_->empty()) {
      TxnProto* txn = ready_txns_->front();
      ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;

      txns_queue_->Push(txn);

//LOG(ERROR) <<machine_id<< ":In LockManagerThread:  Start executing the ready txn: "<<txn->txn_id()<<"  origin:"<<txn->origin_replica();
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
  return ;
}

