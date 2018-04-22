// Author: Kun Ren <renkun.nwpu@gmail.com>
//
#ifndef CALVIN_LOG_LOCALPAXOS_H_
#define CALVIN_LOG_LOCALPAXOS_H_

#include <atomic>
#include <glog/logging.h>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "proto/scalar.pb.h"
#include "common/mutex.h"
#include "common/types.h"
#include "log/local_mem_log.h"
#include "proto/scalar.pb.h"
#include "machine/connection.h"
#include "proto/txn.pb.h"

using std::vector;
using std::atomic;
using std::make_pair;
using std::pair;
using std::queue;
using std::set;


class LocalPaxos {
 public:
  LocalPaxos(ClusterConfig* config, ConnectionMultiplexer* connection, uint32 type);

  ~LocalPaxos();

  void Stop();
  void Append(uint64 blockid);

  void ReceiveMessage();

  void HandleRemoteBatch();
 private:

  // Functions to start the Multiplexor's main loops, called in new pthreads by
  // the Sequencer's constructor.
  static void* RunLeaderThread(void *arg);
  static void* RunFollowerThread(void *arg);

  static void* RunLeaderThreadStrong(void *arg);

  // Returns true iff leader.
  bool IsLeader();

  // Leader's main loop.
  void RunLeader();

  void RunLeaderStrong();

  // Followers' main loop.
  void RunFollower();

  // Participant list.
  vector<uint64> participants_;

  // True iff main thread SHOULD run.
  bool go_;

  // check if it is strong availability or not
  uint32 type_;

  // Current request sequence that will get replicated.
  Sequence sequence_;
  std::atomic<uint64> local_count_;
  Mutex mutex_;

  Log* local_log_;
  Log* global_log_;
  ClusterConfig* configuration_;
  uint64 this_machine_id_;

  ConnectionMultiplexer* connection_;

  // Separate pthread contexts in which to run the leader or follower thread.
  pthread_t leader_thread_;
  pthread_t follower_thread_;

  map<uint64, MessageProto*> mr_txn_batches_;
  AtomicQueue<pair<Sequence, uint32>> sequences_other_replicas_;

  map<uint32, Log::Reader*> readers_for_local_log_;

  set<uint32> new_sequence_todo;

  uint64 local_next_version;
  uint64 global_next_version;

  uint64 machines_per_replica_;
  uint32 local_replica_;

  bool received_synchronize_ack;

  // for strong availbility
  uint64 quorum_;
  MessageProto remote_batch_message_;
};

#endif  // CALVIN_LOG_LOCALPAXOS_H_
