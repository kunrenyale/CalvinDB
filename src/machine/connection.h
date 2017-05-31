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

  // Creates and registers a new connection with channel name 'channel', unless
  // the channel name is already in use, in which case NULL is returned. The
  // caller (not the multiplexer) owns of the newly created Connection object.
  Connection* NewConnection(const string& channel);
  
  Connection* NewConnection(const string& channel, AtomicQueue<MessageProto>** aa);

  void DeleteConnection(const string& channel);

  zmq::context_t* context() { return &context_; }

  uint64 Local_node_id() {return local_node_id_;}

 private:
  friend class Connection;

  // Runs the Multiplexer's main loop. Run() is called in a new thread by the
  // constructor.
  void Run();

  // Function to call multiplexer->Run() in a new pthread.
  static void* RunMultiplexer(void *multiplexer);

  void Send(const MessageProto& message);

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
  unordered_map<int, zmq::socket_t*> remote_out_;

  // Socket listening for messages from Connections. Type = ZMQ_PULL.
  zmq::socket_t* inproc_in_;

  // Sockets for forwarding messages to Connections. Keyed by channel
  // name. Type = ZMQ_PUSH.
  unordered_map<string, zmq::socket_t*> inproc_out_;
  
  unordered_map<string, AtomicQueue<MessageProto>*> remote_result_;
  
  unordered_map<string, AtomicQueue<MessageProto>*> link_unlink_queue_;

  // Stores messages addressed to local channels that do not exist at the time
  // the message is received (so that they may be delivered if a connection is
  // ever created with the specified channel name).
  //
  unordered_map<string, vector<MessageProto> > undelivered_messages_;

  // Protects concurrent calls to NewConnection() and DeleteConnection
  Mutex new_connection_mutex_;
  Mutex delete_connection_mutex_;
  
  Mutex* send_mutex_;

  // False until the deconstructor is called. As soon as it is set to true, the
  // main loop sees it and stops.
  bool deconstructor_invoked_;

  uint64 local_node_id_;

  // DISALLOW_COPY_AND_ASSIGN
  ConnectionMultiplexer(const ConnectionMultiplexer&);
  ConnectionMultiplexer& operator=(const ConnectionMultiplexer&);
};

class Connection {
 public:
  // Closes all sockets.
  ~Connection();

  // Sends 'message' to the Connection specified by
  // 'message.destination_node()' and 'message.destination_channel()'.
  void Send(const MessageProto& message);

  // Loads the next incoming MessageProto into 'message'. Returns true, unless
  // no message is queued up to be delivered, in which case false is returned.
  // 'message->Clear()' is NOT called. Non-blocking.
  bool GetMessage(MessageProto* message);

  // Loads the next incoming MessageProto into 'message'. If no message is
  // queued up to be delivered, GetMessageBlocking waits at most 'max_wait_time'
  // seconds for a message to arrive. If no message arrives, false is returned.
  // 'message->Clear()' is NOT called.
  bool GetMessageBlocking(MessageProto* message, double max_wait_time);

  // Links 'channel' to this Connection object so that messages sent to
  // 'channel' will be forwarded to this Connection.
  //
  // Requires: The requested channel name is not already in use.
  void LinkChannel(const string& channel);

  // Links 'channel' from this Connection object so that messages sent to
  // 'channel' will no longer be forwarded to this Connection.
  //
  // Requires: The requested channel name was previously linked to this
  // Connection by LinkChannel.
  void UnlinkChannel(const string& channel);

  // Returns a pointer to this Connection's multiplexer.
  ConnectionMultiplexer* multiplexer() { return multiplexer_; }

  // Return a const ref to this Connection's channel name.
  const string& channel() { return channel_; }

 private:
  friend class ConnectionMultiplexer;

  // Channel name that 'multiplexer_' uses to identify which messages to
  // forward to this Connection object.
  string channel_;

  // Additional channels currently linked to this Connection object.
  set<string> linked_channels_;

  // Pointer to the main ConnectionMultiplexer with which the Connection
  // communicates. Not owned by the Connection.
  ConnectionMultiplexer* multiplexer_;

  // Socket for sending messages to 'multiplexer_'. Type = ZMQ_PUSH.
  zmq::socket_t* socket_out_;

  // Socket for getting messages from 'multiplexer_'. Type = ZMQ_PUSH.
  zmq::socket_t* socket_in_;

  zmq::message_t msg_;

  Mutex socket_out_mutex_;

  Mutex socket_in_mutex_;
};

#endif  // _DB_MACHINE_CONNECTION_H_

