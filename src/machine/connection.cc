// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "machine/connection.h"

using zmq::socket_t;

ConnectionMultiplexer::ConnectionMultiplexer(ClusterConfig* config)
    : configuration_(config), context_(1), deconstructor_invoked_(false) {
  local_node_id_ = config->local_node_id();
  port_ = config->machines_.find(local_node_id_)->second.port();

  // Bind local (inproc) incoming socket.
  inproc_in_ = new socket_t(context_, ZMQ_PULL);
  inproc_in_->bind("inproc://__inproc_in_endpoint__");

  // Bind port for remote incoming socket.
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port_);
  remote_in_ = new socket_t(context_, ZMQ_PULL);
  remote_in_->bind(endpoint);

  // Wait for other nodes to bind sockets before connecting to them.
  Spin(0.1);

  send_mutex_ = new Mutex[(int)config->all_nodes_size()];

  // Connect to remote outgoing sockets.
  for (map<uint64, MachineInfo>::const_iterator it = config->machines_.begin();
       it != config->machines_.end(); ++it) {
    if (it->second.id() != local_node_id_) {  // Only remote nodes.
      snprintf(endpoint, sizeof(endpoint), "tcp://%s:%d",
               it->second.host().c_str(), it->second.port());
      remote_out_[it->second.id()] = new socket_t(context_, ZMQ_PUSH);
      remote_out_[it->second.id()]->connect(endpoint);
    }
  }

cpu_set_t cpuset;
pthread_attr_t attr;
pthread_attr_init(&attr);

CPU_ZERO(&cpuset);
CPU_SET(3, &cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
//CPU_SET(6, &cpuset);
//CPU_SET(7, &cpuset);
pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);


  // Start Multiplexer main loop running in background thread.
  pthread_create(&thread_, &attr, RunMultiplexer, reinterpret_cast<void*>(this));

  // Just to be safe, wait a bit longer for all other nodes to finish
  // multiplexer initialization before returning to the caller, who may start
  // sending messages immediately.
  Spin(0.1);
}

ConnectionMultiplexer::~ConnectionMultiplexer() {
  // Stop the multixplexer's main loop.
  deconstructor_invoked_ = true;
  pthread_join(thread_, NULL);

  // Close tcp sockets.
  delete remote_in_;
  for (unordered_map<int, zmq::socket_t*>::iterator it = remote_out_.begin();
       it != remote_out_.end(); ++it) {
    delete it->second;
  }

  // Close inproc sockets.
  delete inproc_in_;
  for (unordered_map<string, zmq::socket_t*>::iterator it = inproc_out_.begin();
       it != inproc_out_.end(); ++it) {
    delete it->second;
  }
  
  for (unordered_map<string, AtomicQueue<MessageProto>*>::iterator it = remote_result_.begin();
       it != remote_result_.end(); ++it) {
    delete it->second;
  }
  
  for (unordered_map<string, AtomicQueue<MessageProto>*>::iterator it = link_unlink_queue_.begin();
       it != link_unlink_queue_.end(); ++it) {
    delete it->second;
  }
}

Connection* ConnectionMultiplexer::NewConnection(const string& channel) {
//LOG(ERROR) << "main thread: will create new connection---- ";
  // Disallow concurrent calls to NewConnection/~Connection.
  Lock l(&new_connection_mutex_);
  
  Connection* new_connection = new Connection();

  if (inproc_out_.count(channel) > 0) {
    // Channel name already in use. Report an error and set new_connection_
    // (which NewConnection() will return) to NULL.
    std::cerr << "Attempt to create channel that already exists: "
              << channel << "\n" << std::flush;
    new_connection = NULL;
   } else {
     // Channel name is not already in use. Create a new Connection object
     // and connect it to this multiplexer.
//LOG(ERROR) << "connection thread: will create new connection---- ";
     new_connection->channel_ = channel;
     new_connection->multiplexer_ = this;
     char endpoint[256];
     snprintf(endpoint, sizeof(endpoint), "inproc://%s", channel.c_str());
     inproc_out_[channel] = new socket_t(context_, ZMQ_PUSH);
     inproc_out_[channel]->bind(endpoint);
     new_connection->socket_in_ = new socket_t(context_, ZMQ_PULL);
     new_connection->socket_in_->connect(endpoint);
     new_connection->socket_out_ = new socket_t(context_, ZMQ_PUSH);
     new_connection->socket_out_->connect("inproc://__inproc_in_endpoint__");

     // Forward on any messages sent to this channel before it existed.
     vector<MessageProto>::iterator i;
     for (i = undelivered_messages_[channel].begin();
          i != undelivered_messages_[channel].end(); ++i) {
       Send(*i);
     }
     undelivered_messages_.erase(channel);
   }

   if ((channel.substr(0, 9) == "scheduler") && (channel.substr(9,1) != "_")) {
     link_unlink_queue_[channel] = new AtomicQueue<MessageProto>();
   }
//LOG(ERROR) << "main thread: finish create new connection---- ";
  return new_connection;
}

Connection* ConnectionMultiplexer::NewConnection(const string& channel, AtomicQueue<MessageProto>** aa) {
  // Disallow concurrent calls to NewConnection/~Connection.
  Lock l(&new_connection_mutex_);
  remote_result_[channel] = *aa;

  Connection* new_connection = new Connection();

  if (inproc_out_.count(channel) > 0) {
    // Channel name already in use. Report an error and set new_connection_
    // (which NewConnection() will return) to NULL.
    std::cerr << "Attempt to create channel that already exists: "
              << channel << "\n" << std::flush;
    new_connection = NULL;
   } else {
     // Channel name is not already in use. Create a new Connection object
     // and connect it to this multiplexer.
//LOG(ERROR) << "connection thread: will create new connection---- ";
     new_connection->channel_ = channel;
     new_connection->multiplexer_ = this;
     char endpoint[256];
     snprintf(endpoint, sizeof(endpoint), "inproc://%s", channel.c_str());
     inproc_out_[channel] = new socket_t(context_, ZMQ_PUSH);
     inproc_out_[channel]->bind(endpoint);
     new_connection->socket_in_ = new socket_t(context_, ZMQ_PULL);
     new_connection->socket_in_->connect(endpoint);
     new_connection->socket_out_ = new socket_t(context_, ZMQ_PUSH);
     new_connection->socket_out_->connect("inproc://__inproc_in_endpoint__");

     // Forward on any messages sent to this channel before it existed.
     vector<MessageProto>::iterator i;
     for (i = undelivered_messages_[channel].begin();
          i != undelivered_messages_[channel].end(); ++i) {
       Send(*i);
     }
     undelivered_messages_.erase(channel);
   }

   if ((channel.substr(0, 9) == "scheduler") && (channel.substr(9,1) != "_")) {
     link_unlink_queue_[channel] = new AtomicQueue<MessageProto>();
   }
//LOG(ERROR) << "main thread: finish create new connection---- ";
  return new_connection;
}


void ConnectionMultiplexer::DeleteConnection(const string& channel) {
  // Serve any pending (valid) connection deletion request.
  Lock l(&delete_connection_mutex_);
  if (inproc_out_.count(channel) > 0) {
    delete inproc_out_[channel];
    inproc_out_.erase(channel);
  }
}


void ConnectionMultiplexer::Run() {
  MessageProto message;
  zmq::message_t msg;

  while (!deconstructor_invoked_) {
    // Forward next message from a remote node (if any).
    if (remote_in_->recv(&msg, ZMQ_NOBLOCK)) {
      message.ParseFromArray(msg.data(), msg.size());
      Send(message);
    }

    // Forward next message from a local component (if any), intercepting
    // local Link/UnlinkChannel requests.
    if (inproc_in_->recv(&msg, ZMQ_NOBLOCK)) {
      message.ParseFromArray(msg.data(), msg.size());
      // Normal message. Forward appropriately.
      Send(message);
    }

   for (unordered_map<string, AtomicQueue<MessageProto>*>::iterator it = link_unlink_queue_.begin();
        it != link_unlink_queue_.end(); ++it) {
      
     MessageProto message;
     bool got_it = it->second->Pop(&message);
     if (got_it == true) {
       if (message.type() == MessageProto::LINK_CHANNEL) {
         remote_result_[message.channel_request()] = remote_result_[it->first];
         // Forward on any messages sent to this channel before it existed.
         vector<MessageProto>::iterator i;
         for (i = undelivered_messages_[message.channel_request()].begin();
              i != undelivered_messages_[message.channel_request()].end();
              ++i) {
           Send(*i);
         }
         undelivered_messages_.erase(message.channel_request());
       } else if (message.type() == MessageProto::UNLINK_CHANNEL) {
         remote_result_.erase(message.channel_request());
       }
     }
   }
       
  }
}

// Function to call multiplexer->Run() in a new pthread.
void* ConnectionMultiplexer::RunMultiplexer(void *multiplexer) {
  reinterpret_cast<ConnectionMultiplexer*>(multiplexer)->Run();
  return NULL;
}

void ConnectionMultiplexer::Send(const MessageProto& message) {

  if (message.type() == MessageProto::READ_RESULT) {
    if (remote_result_.count(message.destination_channel()) > 0) {
      remote_result_[message.destination_channel()]->Push(message);
    } else {
      undelivered_messages_[message.destination_channel()].push_back(message);
    }
  } else {

    // Prepare message.
    string* message_string = new string();
    message.SerializeToString(message_string);
    zmq::message_t msg(reinterpret_cast<void*>(
                       const_cast<char*>(message_string->data())),
                       message_string->size(),
                       DeleteString,
                       message_string);

    // Send message.
    if (message.destination_node() == local_node_id_) {
      // Message is addressed to a local channel. If channel is valid, send the
      // message on, else store it to be delivered if the channel is ever created.
      if (inproc_out_.count(message.destination_channel()) > 0)
        inproc_out_[message.destination_channel()]->send(msg);
      else
        undelivered_messages_[message.destination_channel()].push_back(message);
    } else {
      // Message is addressed to valid remote node. Channel validity will be
      // checked by the remote multiplexer.
      Lock l(&send_mutex_[message.destination_node()]);  
      remote_out_[message.destination_node()]->send(msg);
    } 
  }
}

Connection::~Connection() {
  // Unlink any linked channels.
  for (set<string>::iterator it = linked_channels_.begin();
       it != linked_channels_.end(); ++it) {
    UnlinkChannel(*it);
  }

  // Delete socket on Connection end.
  delete socket_in_;
  delete socket_out_;

  // Prompt multiplexer to delete socket on its end.
  multiplexer_->DeleteConnection(channel_);
}

/**void Connection::Send(const MessageProto& message) {
  // Prepare message.
  string* message_string = new string();
  message.SerializeToString(message_string);
  zmq::message_t msg(reinterpret_cast<void*>(
                       const_cast<char*>(message_string->data())),
                     message_string->size(),
                     DeleteString,
                     message_string);
  // Send message.
  socket_out_->send(msg);
} **/

void Connection::Send(const MessageProto& message) {
  // Prepare message.
  string* message_string = new string();
  message.SerializeToString(message_string);
  zmq::message_t msg(reinterpret_cast<void*>(
                       const_cast<char*>(message_string->data())),
                     message_string->size(),
                     DeleteString,
                     message_string);

  if (message.destination_node() == multiplexer()->Local_node_id()) {
    Lock l(&socket_out_mutex_);  
    socket_out_->send(msg);
  } else {
    // Message is addressed to valid remote node. Channel validity will be
    // checked by the remote multiplexer.
    Lock l(&(multiplexer_->send_mutex_[message.destination_node()]));                   
    multiplexer()->remote_out_[message.destination_node()]->send(msg);
  }
}

bool Connection::GetMessage(MessageProto* message) {
  Lock l(&socket_in_mutex_);
  zmq::message_t msg_;
  if (socket_in_->recv(&msg_, ZMQ_NOBLOCK)) {
    // Received a message.
    message->ParseFromArray(msg_.data(), msg_.size());
    return true;
  } else {
    // No message received at this time.
    return false;
  }
}

bool Connection::GetMessageBlocking(MessageProto* message,
                                    double max_wait_time) {
  double start = GetTime();
  do {
    if (GetMessage(message)) {
      // Received a message.
      return true;
    }
  } while (GetTime() < start + max_wait_time);

  // Waited for max_wait_time, but no message was received.
  return false;
}

void Connection::LinkChannel(const string& channel) {
  MessageProto m;
  m.set_type(MessageProto::LINK_CHANNEL);
  m.set_channel_request(channel);
  multiplexer()->link_unlink_queue_[channel_]->Push(m);
}

void Connection::UnlinkChannel(const string& channel) {
  MessageProto m;
  m.set_type(MessageProto::UNLINK_CHANNEL);
  m.set_channel_request(channel);
  multiplexer()->link_unlink_queue_[channel_]->Push(m);
}

