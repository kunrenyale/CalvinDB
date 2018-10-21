// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// A simulated TPC-C application

#ifndef _DB_APPLICATIONS_TPCC_H_
#define _DB_APPLICATIONS_TPCC_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "machine/cluster_config.h"
#include "machine/connection.h"

using std::set;
using std::string;

#define DISTRICTS_PER_WAREHOUSE 10
#define CUSTOMERS_PER_DISTRICT 3000
#define NUMBER_OF_ITEMS 100000

class Tpcc : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    TPCCTXN_SP = 1,
    TPCCTXN_MP = 2,
    TPCCTXN_SRSP = 3,
    TPCCTXN_SRMP = 4,
    TPCCTXN_MRSP = 5,
    TPCCTXN_MRMP = 6,
  };

  Tpcc(ClusterConfig* conf, uint32 warehouses) {
    nparts = conf->nodes_per_replica();
    warehouses_per_node = warehouses;
    replica_size = conf->replicas_size();
    config_ = conf;
    local_replica_ = config_->local_replica_id();

    // For tpcc
    warehouse_end = warehouses_per_node * nparts;
    district_end = warehouse_end + nparts * (warehouses_per_node * DISTRICTS_PER_WAREHOUSE);
    customer_end = district_end + nparts * (warehouses_per_node * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT);
    item_end = customer_end + nparts * (warehouses_per_node * NUMBER_OF_ITEMS);

    kDBSize = item_end;
  }

  Tpcc(ClusterConfig* conf, ConnectionMultiplexer* multiplexer, uint32 warehouses) {
    nparts = conf->nodes_per_replica();
    warehouses_per_node = warehouses;
    replica_size = conf->replicas_size();
    config_ = conf;
    local_replica_ = config_->local_replica_id();

    connection_ = multiplexer;

    // For tpcc
    warehouse_end = warehouses_per_node * nparts;
    district_end = warehouse_end + nparts * (warehouses_per_node * DISTRICTS_PER_WAREHOUSE);
    customer_end = district_end + nparts * (warehouses_per_node * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT);
    item_end = customer_end + nparts * (warehouses_per_node * NUMBER_OF_ITEMS);

    kDBSize = item_end;
  }

  virtual ~Tpcc() {}

  virtual TxnProto* NewTxn(int64 txn_id, int txn_type, string args,
                           ClusterConfig* config = NULL) const;

  virtual int Execute(TxnProto* txn, StorageManager* storage) const;

  TxnProto* TpccTxnSP(int64 txn_id, uint64 part);
  TxnProto* TpccTxnMP(int64 txn_id, uint64 part1, uint64 part2);
  TxnProto* TpccTxnSRSP(int64 txn_id, uint64 part, uint32 replica);
  TxnProto* TpccTxnSRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica);
  TxnProto* TpccTxnMRSP(int64 txn_id, uint64 part, uint32 replica1, uint32 replica2);
  TxnProto* TpccTxnMRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica1, uint32 replica2);

  uint32 nparts;
  uint32 warehouses_per_node;
  uint32 replica_size;

  // For tpcc
  uint64 warehouse_end;
  uint64 district_end;
  uint64 customer_end;
  uint64 item_end;

  ClusterConfig* config_;
  uint32 local_replica_;
  ConnectionMultiplexer* connection_;

  uint64 kDBSize;
  static const uint32 kRecordSize = 100;


  virtual void InitializeStorage(Storage* storage, ClusterConfig* conf) const;

 private:
  void GetRandomKeys(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                     uint64 key_limit, uint64 part);
  void GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                            uint64 key_limit, uint64 part, uint32 replica);
  Tpcc() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
