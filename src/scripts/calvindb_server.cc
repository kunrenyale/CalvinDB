// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "log/local_mem_log.h"
#include "log/paxos.h"
#include "backend/simple_storage.h"
#include "machine/connection.h"
#include "machine/sequencer.h"
#include "applications/microbenchmark.h"
#include "scheduler/deterministic_scheduler.h"
#include "scripts/script_utils.h"

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "calvindb_server", "Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_int32(experiment, 0, "experiment that you want to run");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 10, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Print Calvin version
  if (FLAGS_calvin_version) {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (Geo-replicated CalvinDB) 0.1 (c) Yale University 2017.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  LOG(ERROR) <<FLAGS_machine_id<<":Preparing to start Calvin node ";


  // Build this node's configuration object.
  ClusterConfig config(FLAGS_machine_id);
  config.FromFile(FLAGS_config);

  LOG(ERROR)<<FLAGS_machine_id <<":Created config ";

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);

  Spin(1);

  LOG(ERROR) << FLAGS_machine_id <<":Created connection "; 

  Client* client = NULL;
  // Artificial loadgen clients. Right now only microbenchmark
  if (FLAGS_experiment == 0) {
    client = reinterpret_cast<Client*>(new MClient(&config, FLAGS_percent_mp, FLAGS_hot_records));
  }

  Storage* storage;
  storage = new SimpleStorage();

  LOG(ERROR) << FLAGS_machine_id<< ":Created storage "; 
  
  Application* application = NULL; 
  if (FLAGS_experiment == 0) {
    application = new Microbenchmark(config.nodes_per_replica(), FLAGS_hot_records);
    application->InitializeStorage(storage, &config);
  } else {
    // Other benchmark
  }

  LOG(ERROR) << FLAGS_machine_id << ":Created application "; 

  // Synchronization loadgen start with other machines.
  Connection* synchronization_connection = multiplexer.NewConnection("synchronization_connection");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("synchronization_connection");
  for (uint32 i = 0; i < (uint32)(config.all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(config.local_node_id())) {
      synchronization_connection->Send(synchronization_message);
    }
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint32)(config.all_nodes_size())) {
    synchronization_message.Clear();
    if (synchronization_connection->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
  
  delete synchronization_connection;
  LOG(ERROR) << FLAGS_machine_id << ":After synchronization"; 

  // Create Paxos
  Paxos* paxos = NULL;
  if (FLAGS_machine_id % config.nodes_per_replica() == 0) {
    paxos = new Paxos(new LocalMemLog(), &config, multiplexer.NewConnection("paxos_log_"));
  }

  LOG(ERROR) << FLAGS_machine_id << ":Created paxos log "; 

  Spin(1);

  // Initialize sequencer component and start sequencer thread running.
  Sequencer sequencer(&config, multiplexer.NewConnection("sequencer_sending"), multiplexer.NewConnection("sequencer_receiving"), client, paxos, FLAGS_max_batch_size);

  LOG(ERROR) << FLAGS_machine_id << ":Created sequencer ";
 
  Spin(1);

   // Run scheduler in main thread.
  if (FLAGS_experiment == 0) {
    DeterministicScheduler scheduler(&config,
                                     multiplexer.NewConnection("scheduler_"),
                                     storage,
                                     application);
  } else {
    // Other benchmark
  }

  LOG(ERROR) << FLAGS_machine_id << ":Created scheduler "; 

  while (!config.Stopped()) {
    usleep(1000000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

