// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <deque>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <sched.h>
#include <map>

#include "applications/application.h"
#include "common/utils.h"
#include "machine/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"
#include "scheduler/scheduler.h"
#include "proto/scalar.pb.h"

using std::deque;

namespace zmq {
class socket_t;
class message_t;
}
using zmq::socket_t;

class ClusterConfig;
class Connection;
class DeterministicLockManager;
class Storage;
class TxnProto;

#define NUM_THREADS 4

class DeterministicScheduler : public Scheduler {
 public:
  DeterministicScheduler(ClusterConfig* conf, Storage* storage, const Application* application,ConnectionMultiplexer* connection, uint32 mode);
  virtual ~DeterministicScheduler();

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  // Configuration specifying node & system settings.
  ClusterConfig* configuration_;

  // Thread contexts and their associated Connection objects.
  pthread_t threads_[NUM_THREADS];

  pthread_t lock_manager_thread_;

  // Storage layer used in application execution.
  Storage* storage_;
  
  // Application currently being run.
  const Application* application_;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.
  DeterministicLockManager* lock_manager_;

  // Queue of transaction ids of transactions that have acquired all locks that
  // they have requested.
  std::deque<TxnProto*>* ready_txns_;

  
  AtomicQueue<TxnProto*>* txns_queue;
  AtomicQueue<TxnProto*>* done_queue;
  
  // Connection for receiving txn batches from sequencer.
  ConnectionMultiplexer* connection_;

  bool start_working_;

  uint32 mode_;
  
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
