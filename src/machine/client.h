// Author: Kun Ren <renkun.nwpu@gmail.com>
//


#ifndef _DB_MACHINE_CLIENT_H_
#define _DB_MACHINE_CLIENT_H_

#include <set>
#include <string>
#include <queue>
#include <iostream>
#include <map>
#include <utility>

#include "machine/cluster_config.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "applications/microbenchmark.h"
#include "applications/tpcc.h"

using std::set;
using std::string;
using std::queue;
using std::map;

#define SAMPLES  180
#define SAMPLE_RATE 199

#define LATENCY_TEST

#ifdef LATENCY_TEST
extern map<uint64, double> sequencer_recv;
extern map<uint64, double> scheduler_unlock;
extern vector<double> measured_latency;
extern std::atomic<uint64> latency_counter;
#endif

class ClusterConfig;
class TxnProto;

// Client
class Client {
 public:
  virtual ~Client() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) = 0;
};

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(ClusterConfig* config, uint32 mp, uint32 hot_records)
      : microbenchmark(config, hot_records), config_(config), percent_mp_(mp),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
      // Multipartition txn.
      uint64 other;
      do {
        other = (uint64)(rand() % nodes_per_replica_);
      } while (other == replative_node_id_);
      *txn = microbenchmark.MicroTxnMP(txn_id, replative_node_id_, other);
    } else {
      // Single-partition txn.
      *txn = microbenchmark.MicroTxnSP(txn_id, replative_node_id_);
    }
  }

 private:
  Microbenchmark microbenchmark;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// TPCC load generation client.
class TClient : public Client {
 public:
  TClient(ClusterConfig* config, uint32 mp, uint32 hot_records)
      : tpcc(config, hot_records), config_(config), percent_mp_(mp),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
  }
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
	  // Right now only test 10% multi-warehouse txn (percent_mp_ is used to how much multi-warehouse txn)
    if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
      // Multipartition txn.
      uint64 other;
      do {
        other = (uint64)(rand() % nodes_per_replica_);
      } while (other == replative_node_id_);
      *txn = tpcc.TpccTxnMP(txn_id, replative_node_id_, other);
    } else {
      // Single-partition txn.
      *txn = tpcc.TpccTxnSP(txn_id, replative_node_id_);
    }
  }

 private:
  Tpcc tpcc;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// Microbenchmark load generation client.
class Lowlatency_MClient : public Client {
 public:
  Lowlatency_MClient(ClusterConfig* config, uint32 mp, uint32 mr, uint32 hot_records)
      : microbenchmark(config, hot_records), config_(config), percent_mp_(mp), percent_mr_(mr),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
    local_replica_ = config_->local_replica_id();
    num_replicas_ = config_->replicas_size();
  }
  virtual ~Lowlatency_MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if ((uint32)(rand() % 100) < percent_mr_) {
      // Multi-replica txn.
      uint32 other_replica;
      do {
        other_replica = (uint32)(rand() % num_replicas_);
      } while (other_replica == local_replica_); 
//other_replica = (local_replica_ + 1)%3;
      if (nodes_per_replica_ > 1 && uint32(rand() % 100) < percent_mp_) {
        // Multi-replica multi-partition txn
        uint64 other_node;
        do {
          other_node = (uint64)(rand() % nodes_per_replica_);
        } while (other_node == replative_node_id_);

        *txn = microbenchmark.MicroTxnMRMP(txn_id, replative_node_id_, other_node, local_replica_, other_replica);
        //*txn = microbenchmark.MicroTxnMRMP(txn_id, 0, 1, 0, 1);
      } else {
        // Multi-replica single-partition txn
        *txn = microbenchmark.MicroTxnMRSP(txn_id, replative_node_id_, local_replica_, other_replica);    
//*txn = microbenchmark.MicroTxnMRSP(txn_id, 0, 0, 1);   
      }
    } else {
      // Single-replica txn.
      if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
        // Single-replica multi-partition txn
        uint64 other_node;
        do {
          other_node = (uint64)(rand() % nodes_per_replica_);
        } while (other_node == replative_node_id_);

        *txn = microbenchmark.MicroTxnSRMP(txn_id, replative_node_id_, other_node, local_replica_);
      } else {
        // Single-replica single-partition txn
        *txn = microbenchmark.MicroTxnSRSP(txn_id, replative_node_id_, local_replica_);         
      }
    }
  }

 private:
  Microbenchmark microbenchmark;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint32 percent_mr_;
  uint32 local_replica_;
  uint32 num_replicas_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

// Tpcc load generation client for slog.
class Lowlatency_TClient : public Client {
 public:
  Lowlatency_TClient(ClusterConfig* config, uint32 mp, uint32 mr, uint32 hot_records)
      : tpcc(config, hot_records), config_(config), percent_mp_(mp), percent_mr_(mr),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
    local_replica_ = config_->local_replica_id();
    num_replicas_ = config_->replicas_size();
  }
  virtual ~Lowlatency_TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
	  // Currently use 10% multi-warehouse txn (percent_mp_ is used to how many multi-warehouse txn)
	if ((uint32)(rand() % 100) < percent_mp_) {
        if ((uint32)(rand() % 100) < percent_mr_) {
            // Multi-replica txn.
            uint32 other_replica;
            do {
                other_replica = (uint32)(rand() % num_replicas_);
            } while (other_replica == local_replica_);
            // Multi-replica multi-partition txn
            uint64 other_node;
            do {
                other_node = (uint64)(rand() % nodes_per_replica_);
            } while (other_node == replative_node_id_);

            *txn = tpcc.TpccTxnMRMP(txn_id, replative_node_id_, other_node, local_replica_, other_replica);
        } else {
            // Single-replica txn.
            if (nodes_per_replica_ > 1 && (uint32)(rand() % 100) < percent_mp_) {
                // Single-replica multi-partition txn
                uint64 other_node;
                do {
                   other_node = (uint64)(rand() % nodes_per_replica_);
                } while (other_node == replative_node_id_);

                *txn = tpcc.TpccTxnSRMP(txn_id, replative_node_id_, other_node, local_replica_);
            }
	    }
    } else {
        // Single-replica single-partition txn
        *txn = tpcc.TpccTxnSRSP(txn_id, replative_node_id_, local_replica_);
    }
  }

 private:
  Tpcc tpcc;
  ClusterConfig* config_;
  uint32 percent_mp_;
  uint32 percent_mr_;
  uint32 local_replica_;
  uint32 num_replicas_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};


#endif  // _DB_MACHINE_CLIENT_H_
