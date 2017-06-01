// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// Library for handling messaging between system nodes. Each node generally owns
// a ConnectionMultiplexer object as well as a Configuration object.

#ifndef _DB_MACHINE_CONNECTION_H_
#define _DB_MACHINE_CONNECTION_H_

#include <pthread.h>

#include <map>
#include <set>
#include <string>
#include <vector>
#include <tr1/unordered_map>
#include <cstdio>
#include <iostream>

#include "machine/cluster_config.h"
#include "common/utils.h"
#include "common/mutex.h"
#include "machine/zmq.hpp"
#include "proto/message.pb.h"

using std::map;
using std::set;
using std::string;
using std::vector;
using std::tr1::unordered_map;

class ClusterConfig;


class Connection;
class ConnectionMultiplexer {
 public:
  // Create a ConnectionMultiplexer that establishes two-way communication with
  // Connections for every other node specified by '*config' to exist.
  explicit ConnectionMultiplexer(ClusterConfig* config);

  ~ConnectionMultiplexer();

  // Creates and registers a new channel with channel name 'channel', unless
  // the channel name is already in use, in which case NULL is returned. The
  // caller (not the multiplexer) owns of the newly created Connection object.
  
  void NewChannel(const string& channel);

  void DeleteChannel(const string& channel);

  void LinkChannel(const string& channel, const string& main_channel);

  void UnlinkChannel(const string& channel);

  bool GotMessage(const string& channel, MessageProto* message);

  void Send(const MessageProto& message);

  zmq::context_t* context() { return &context_; }

  uint64 Local_node_id() {return local_node_id_;}

 private:
  friend class Connection;

  // Runs the Multiplexer's main loop. Run() is called in a new thread by the
  // constructor.
  void Run();

  // Function to call multiplexer->Run() in a new pthread.
  static void* RunMultiplexer(void *multiplexer);

  // Separate pthread context in which to run the multiplexer's main loop.
  pthread_t thread_;

  // Pointer to Configuration instance used to construct this Multiplexer.
  // (Currently used primarily for finding 'this_node_id'.)
  ClusterConfig* configuration_;

  // Context shared by all Connection objects with channels to this
  // multiplexer.
  zmq::context_t context_;

  // Port on which to listen for incoming messages from other nodes.
  int port_;

  // Socket listening for messages from other nodes. Type = ZMQ_PULL.
  zmq::socket_t* remote_in_;

  // Sockets for outgoing traffic to other nodes. Keyed by node_id.
  // Type = ZMQ_PUSH.
  unordered_map<uint64, zmq::socket_t*> remote_out_;
  
  unordered_map<string, AtomicQueue<MessageProto>*> channel_results_;
  
  AtomicQueue<MessageProto>* link_unlink_queue_;

  AtomicQueue<string>* new_channel_queue_;

  AtomicQueue<string>* delete_channel_queue_;

  AtomicQueue<MessageProto>* send_message_queue_;

  // Stores messages addressed to local channels that do not exist at the time
  // the message is received (so that they may be delivered if a connection is
  // ever created with the specified channel name).
  //
  unordered_map<string, vector<MessageProto> > undelivered_messages_;

  // False until the deconstructor is called. As soon as it is set to true, the
  // main loop sees it and stops.
  bool deconstructor_invoked_;

  uint64 local_node_id_;

  // DISALLOW_COPY_AND_ASSIGN
  ConnectionMultiplexer(const ConnectionMultiplexer&);
  ConnectionMultiplexer& operator=(const ConnectionMultiplexer&);
};



#endif  // _DB_MACHINE_CONNECTION_H_

