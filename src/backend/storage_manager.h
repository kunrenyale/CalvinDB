// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A wrapper for a storage layer that can be used by an Application to simplify
// application code by hiding all inter-node communication logic. By using this
// class as the primary interface for applications to interact with storage of
// actual data objects, applications can be written without paying any attention
// to partitioning at all.
//
// StorageManager use:
//  - Each transaction execution creates a new StorageManager and deletes it
//    upon completion.
//  - No ReadObject call takes as an argument any value that depends on the
//    result of a previous ReadObject call.
//  - In any transaction execution, a call to DoneReading must follow ALL calls
//    to ReadObject and must precede BOTH (a) any actual interaction with the
//    values 'read' by earlier calls to ReadObject and (b) any calls to
//    PutObject or DeleteObject.

#ifndef _DB_BACKEND_STORAGE_MANAGER_H_
#define _DB_BACKEND_STORAGE_MANAGER_H_

#include <ucontext.h>

#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"
#include "backend/storage.h"
#include "machine/cluster_config.h"
#include "machine/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::vector;
using std::tr1::unordered_map;

class ClusterConfig;
class Connection;
class MessageProto;
class Scheduler;
class Storage;
class TxnProto;

class StorageManager {
 public:
  StorageManager(ClusterConfig* config, ConnectionMultiplexer* connection,
                 Storage* actual_storage, TxnProto* txn, uint32 mode);

  ~StorageManager();

  Record* ReadObject(const Key& key);
  bool PutObject(const Key& key, Record* record);
  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);
  bool ReadyToExecute();

  Storage* GetStorage() { return actual_storage_; }

  // Set by the constructor, indicating whether 'txn' involves any writes at
  // this node.
  bool writer;

// private:
  friend class DeterministicScheduler;

  // Pointer to the configuration object for this node.
  ClusterConfig* configuration_;

  // A Connection object that can be used to send and receive messages.
  ConnectionMultiplexer* connection_;

  // Storage layer that *actually* stores data objects on this node.
  Storage* actual_storage_;

  // Transaction that corresponds to this instance of a StorageManager.
  TxnProto* txn_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // StorageManager construction time.
  //
  unordered_map<Key, Record*> objects_;

  vector<Record*> remote_reads_;

  uint64 relative_node_id_;

  uint32 local_replica_id_;

  uint32 mode_;

};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

