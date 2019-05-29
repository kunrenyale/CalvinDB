// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "machine/cluster_config.h"
#include "machine/connection.h"

using std::set;
using std::string;

class Microbenchmark : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
    MICROTXN_SRSP = 3,
    MICROTXN_SRMP = 4,
    MICROTXN_MRSP = 5,
    MICROTXN_MRMP = 6,
  };

  Microbenchmark(ClusterConfig* conf, uint32 hotcount) {
    nparts = conf->nodes_per_replica();
    hot_records = hotcount;
    replica_size = conf->replicas_size();
    config_ = conf;
    local_replica_ = config_->local_replica_id();
  }

  Microbenchmark(ClusterConfig* conf, ConnectionMultiplexer* multiplexer, uint32 hotcount) {
    nparts = conf->nodes_per_replica();
    hot_records = hotcount;
    replica_size = conf->replicas_size();
    config_ = conf;
    local_replica_ = config_->local_replica_id();

    connection_ = multiplexer;
  }

  virtual ~Microbenchmark() {}

  virtual TxnProto* NewTxn(int64 txn_id, int txn_type, string args,
                           ClusterConfig* config = NULL) const;

  virtual int Execute(TxnProto* txn, StorageManager* storage) const;

  TxnProto* MicroTxnSP(int64 txn_id, uint64 part);
  TxnProto* MicroTxnMP(int64 txn_id, uint64 part1, uint64 part2);
  TxnProto* MicroTxnSRSP(int64 txn_id, uint64 part, uint32 replica);
  TxnProto* MicroTxnSRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica);
  TxnProto* MicroTxnMRSP(int64 txn_id, uint64 part, uint32 replica1, uint32 replica2);
  TxnProto* MicroTxnMRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica1, uint32 replica2);

  uint32 nparts;
  uint32 hot_records;
  uint32 replica_size;

  ClusterConfig* config_;
  uint32 local_replica_;
  ConnectionMultiplexer* connection_;

  static const uint32 kRWSetSize = 10;  // MUST BE EVEN
  static const uint64 kDBSize = 1000000000;
  static const uint32 kRecordSize = 100;


  virtual void InitializeStorage(Storage* storage, ClusterConfig* conf) const;

 private:
  void GetRandomKeys(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                     uint64 key_limit, uint64 part);
  void GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                            uint64 key_limit, uint64 part, uint32 replica);
  Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
