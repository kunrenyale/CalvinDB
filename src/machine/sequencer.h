// Author: Kun Ren <renkun.nwpu@gmail.com>
//


#ifndef _DB_MACHINE_SEQUENCER_H_
#define _DB_MACHINE_SEQUENCER_H_

#include <set>
#include <string>
#include <queue>
#include <iostream>
#include <map>
#include <utility>

#include "machine/cluster_config.h"
#include "machine/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "applications/microbenchmark.h"
#include "log/paxos.h"

using std::set;
using std::string;
using std::queue;
using std::map;

#define SAMPLES  2000
#define SAMPLE_RATE 299

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
  MClient(ClusterConfig* config, int mp, int hot_records)
      : microbenchmark(config->nodes_per_replica(), hot_records), config_(config), percent_mp_(mp),
        nodes_per_replica_(config->nodes_per_replica()), replative_node_id_(config->relative_node_id()) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if (nodes_per_replica_ > 1 && rand() % 100 < percent_mp_) {
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
  int percent_mp_;
  uint64 nodes_per_replica_;
  uint64 replative_node_id_;
};

class Sequencer {
 public:
  // The constructor creates background threads and starts the Sequencer's main
  // loops running.
  Sequencer(ClusterConfig* conf, ConnectionMultiplexer* connection, Client* client, Paxos* paxos, uint32 max_batch_size);

  // Halts the main loops.
  ~Sequencer();

 private:
  // Sequencer's main loops:
  //
  // RunWriter:
  //  while true:
  //    Spend epoch_duration collecting client txn requests into a batch.
  //
  // RunReader:
  //  while true:
  //    Distribute the txns to relevant machines;
  //    Send txns to other replicas;
  //    Append the batch id to paxos log
  //
  // Executes in a background thread created and started by the constructor.
  void RunWriter();
  void RunReader();

  // Functions to start the Multiplexor's main loops, called in new pthreads by
  // the Sequencer's constructor.
  static void* RunSequencerWriter(void *arg);
  static void* RunSequencerReader(void *arg);

  // Sets '*nodes' to contain the node_id of every node participating in 'txn'.
  void FindParticipatingNodes(const TxnProto& txn, set<int>* nodes);

  // Length of time spent collecting client requests before they are ordered,
  // batched, and sent out to schedulers.
  double epoch_duration_;

  // Configuration specifying node & system settings.
  ClusterConfig* configuration_;

  // Connection for sending and receiving protocol messages.
  ConnectionMultiplexer* connection_;

  // Client from which to get incoming txns.
  Client* client_;

  // Separate pthread contexts in which to run the sequencer's main loops.
  pthread_t writer_thread_;
  pthread_t reader_thread_;

  // False until the deconstructor is called. As soon as it is set to true, the
  // main loop sees it and stops.
  bool deconstructor_invoked_;

  Paxos* paxos_log_;

  uint32 max_batch_size_;

  // Number of votes for each batch (used only by machine 0).
  map<uint64, uint32> batch_votes_;
};
#endif  // _DB_MACHINE_SEQUENCER_H_
