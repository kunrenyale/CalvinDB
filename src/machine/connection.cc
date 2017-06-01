// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "machine/connection.h"

using zmq::socket_t;

ConnectionMultiplexer::ConnectionMultiplexer(ClusterConfig* config)
    : configuration_(config), context_(1), deconstructor_invoked_(false) {
  local_node_id_ = config->local_node_id();
  port_ = config->machines_.find(local_node_id_)->second.port();

  // Bind port for remote incoming socket.
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port_);
  remote_in_ = new socket_t(context_, ZMQ_PULL);
  remote_in_->bind(endpoint);

  // Initialize mutexes.
  for (map<uint64, MachineInfo>::const_iterator it = config->machines_.begin();
       it != config->machines_.end(); ++it) {
      mutexes_[it->second.id()] = new Mutex();
  }

  new_channel_mutex_ = new Mutex();

  link_unlink_queue_ = new AtomicQueue<MessageProto>();

  // Wait for other nodes to bind sockets before connecting to them.
  Spin(0.1);

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
  for (unordered_map<uint64, zmq::socket_t*>::iterator it = remote_out_.begin();
       it != remote_out_.end(); ++it) {
    delete it->second;
  }
  
  for (unordered_map<string, AtomicQueue<MessageProto>*>::iterator it = channel_results_.begin();
       it != channel_results_.end(); ++it) {
    delete it->second;
  }
  
  delete link_unlink_queue_;

  for (unordered_map<uint64, Mutex*>::iterator it = mutexes_.begin();it != mutexes_.end(); ++it) {
    delete it->second;
  }
}


AtomicQueue<MessageProto>* ConnectionMultiplexer::NewChannel(const string& channel) {
  // Disallow concurrent calls to NewConnection/~Connection.
  Lock l(new_channel_mutex_);

  if (channel_results_.count(channel) > 0) {
    // Channel name already in use. Report an error and set new_connection_
    // (which NewConnection() will return) to NULL.
    std::cerr << "Attempt to create channel that already exists: "
              << channel << "\n" << std::flush;
    return NULL;
  }
  
  AtomicQueue<MessageProto>* channel_queue = new AtomicQueue<MessageProto>(); 
  channel_results_[channel] = channel_queue;

  // Forward on any messages sent to this channel before it existed.
  vector<MessageProto>::iterator i;
  for (i = undelivered_messages_[channel].begin(); i != undelivered_messages_[channel].end(); ++i) {
    channel_results_[channel]->Push(*i);
  }
  
  undelivered_messages_.erase(channel);

  return channel_queue;
}


void ConnectionMultiplexer::DeleteChannel(const string& channel) {
  // Serve any pending (valid) connection deletion request.
  Lock l(new_channel_mutex_);
  if (channel_results_.count(channel) > 0) {
    delete channel_results_[channel];
    channel_results_.erase(channel);
  }
}


void ConnectionMultiplexer::Run() {
  MessageProto message;
  zmq::message_t msg;
  bool got_message= false;
  bool got_request = false;

  while (!deconstructor_invoked_) {
    // Forward next message from a remote node (if any).
    got_message = remote_in_->recv(&msg, ZMQ_NOBLOCK);
    if (got_message == true) {
      message.ParseFromArray(msg.data(), msg.size());
        
      if (channel_results_.count(message.destination_channel()) > 0) {
        channel_results_[message.destination_channel()]->Push(message);
LOG(ERROR) << local_node_id_ << ":ConnectionMultiplexer::Run(), receive a meesage, channel:"<<message.destination_channel();   
      } else {
        undelivered_messages_[message.destination_channel()].push_back(message);
LOG(ERROR) << local_node_id_ << ":ConnectionMultiplexer::Run(), receive a meesage(undeliver), channel:"<<message.destination_channel();   
      }
      message.Clear();
    }
    
     got_request = link_unlink_queue_->Pop(&message);
     if (got_request == true) {
       if (message.type() == MessageProto::LINK_CHANNEL) {
         channel_results_[message.channel_request()] = channel_results_[message.main_channel()];
         // Forward on any messages sent to this channel before it existed.
         vector<MessageProto>::iterator i;
         for (i = undelivered_messages_[message.channel_request()].begin();
              i != undelivered_messages_[message.channel_request()].end();
              ++i) {
           channel_results_[message.main_channel()]->Push(*i);
         }
         undelivered_messages_.erase(message.channel_request());
       } else if (message.type() == MessageProto::UNLINK_CHANNEL) {
         channel_results_.erase(message.channel_request());
       }
       message.Clear();
     }

     if (got_message == false && got_request == false) {
       usleep(20);
     }     
  }
}

// Function to call multiplexer->Run() in a new pthread.
void* ConnectionMultiplexer::RunMultiplexer(void *multiplexer) {
  reinterpret_cast<ConnectionMultiplexer*>(multiplexer)->Run();
  return NULL;
}

void ConnectionMultiplexer::Send(const MessageProto& message) {

  if (message.destination_node() == local_node_id_) {
    // Message is addressed to a local channel. If channel is valid, send the
    // message on, else store it to be delivered if the channel is ever created.
    if (channel_results_.count(message.destination_channel()) > 0) {
      channel_results_[message.destination_channel()]->Push(message);
    } else {
      undelivered_messages_[message.destination_channel()].push_back(message);
    }
  } else {
    Lock l(mutexes_[message.destination_node()]);
LOG(ERROR) << local_node_id_ << ":ConnectionMultiplexer::Run(), before send a meesage, send to :"<<message.destination_node();
    // Prepare message.
    string* message_string = new string();
    message.SerializeToString(message_string);
    zmq::message_t msg(reinterpret_cast<void*>(
                       const_cast<char*>(message_string->data())),
                       message_string->size(),
                       DeleteString,
                       message_string);

    remote_out_[message.destination_node()]->send(msg);
LOG(ERROR) << local_node_id_ << ":ConnectionMultiplexer::Run(), after send a meesage, send to :"<<message.destination_node();
  }
}


void ConnectionMultiplexer::LinkChannel(const string& channel, const string& main_channel) {
  MessageProto m;
  m.set_type(MessageProto::LINK_CHANNEL);
  m.set_channel_request(channel);
  m.set_main_channel(main_channel);
  link_unlink_queue_->Push(m);
}

void ConnectionMultiplexer::UnlinkChannel(const string& channel) {
  MessageProto m;
  m.set_type(MessageProto::LINK_CHANNEL);
  m.set_channel_request(channel);
  link_unlink_queue_->Push(m);
}

