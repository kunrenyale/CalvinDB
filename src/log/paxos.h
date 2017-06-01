// Author: Kun Ren <renkun.nwpu@gmail.com>
//
#ifndef CALVIN_LOG_PAXOS2_H_
#define CALVIN_LOG_PAXOS2_H_

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

using std::vector;
using std::atomic;
using std::make_pair;
using std::pair;
using std::queue;
using std::set;


class Paxos {
 public:
  Paxos(Log* log, ClusterConfig* config, ConnectionMultiplexer* connection);

  ~Paxos();

  void Stop();
  void Append(uint64 blockid);

 private:

  // Functions to start the Multiplexor's main loops, called in new pthreads by
  // the Sequencer's constructor.
  static void* RunLeaderThread(void *arg);
  static void* RunFollowerThread(void *arg);

  // Returns true iff leader.
  bool IsLeader();

  // Leader's main loop.
  void RunLeader();

  // Followers' main loop.
  void RunFollower();

  // Participant list.
  vector<uint64> participants_;

  // True iff main thread SHOULD run.
  bool go_;

  // Current request sequence that will get replicated.
  Sequence sequence_;
  std::atomic<uint64> count_;
  Mutex mutex_;

  Log* log_;
  ClusterConfig* configuration_;
  uint64 this_machine_id_;

  ConnectionMultiplexer* connection_;

  // Separate pthread contexts in which to run the leader or follower thread.
  pthread_t leader_thread_;
  pthread_t follower_thread_;

};

#endif  // CALVIN_LOG_PAXOS2_H_
