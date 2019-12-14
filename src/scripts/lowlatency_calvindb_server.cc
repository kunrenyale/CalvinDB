// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "log/paxos.h"
#include "backend/simple_storage.h"
#include "machine/connection.h"
#include "machine/lowlatency_sequencer.h"
#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "scheduler/deterministic_scheduler.h"
#include "scripts/script_utils.h"

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "lowlatency_calvindb_server", "Lowlatency Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_int32(mode, 1, "0: Origin CalvinDB; 1: Low latency CalvinDB; 2; Low latency CalvinDB with access pattern remaster");
DEFINE_int32(type, 0, "[CalvinDB: 0: 3 replicas; 1: 6 replicas]; [Low latency: 0: 3 replicas normal; 1: 6 replicas normal; 2: 6 replicas strong availbility ] ");
DEFINE_int32(experiment, 0, "the experiment that you want to run, default is microbenchmark");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(percent_mr, 0, "percent of multi-replica txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 100, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Print Calvin version
  if (FLAGS_calvin_version) {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (Geo-replicated CalvinDB) 0.1 (c) UMD 2017.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  LOG(INFO) <<FLAGS_machine_id<<":Preparing to start Calvin node ";


  // Build this node's configuration object.
  ClusterConfig* config = new ClusterConfig(FLAGS_machine_id);
  config->FromFile(FLAGS_config);

  LOG(INFO)<<FLAGS_machine_id <<":Created config ";

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(config);

  Spin(1);

  LOG(INFO) << FLAGS_machine_id <<":Created connection "; 

  Client* client = NULL;
  // Artificial loadgen clients. Right now only microbenchmark
  if (FLAGS_experiment == 0) {
    client = reinterpret_cast<Client*>(new Lowlatency_MClient(config, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records));
  } else if (FLAGS_experiment == 1) {
	client = reinterpret_cast<Client*>(new Lowlatency_TClient(config, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records));
  } if (FLAGS_experiment == 2) {
    client = reinterpret_cast<Client*>(new MockClient(config, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records));
  } else {
    LOG(FATAL)<<"Unknown experiment flag";
  }

  Storage* storage;
  storage = new SimpleStorage();

  LOG(INFO) << FLAGS_machine_id<< ":Created storage "; 
  
  Application* application = NULL; 
  if (FLAGS_experiment == 0) {
    application = new Microbenchmark(config, multiplexer, FLAGS_hot_records);
    application->InitializeStorage(storage, config);
  } else if (FLAGS_experiment == 1) {
    // Other benchmark
	application = new Tpcc(config, multiplexer, FLAGS_hot_records);
	application->InitializeStorage(storage, config);
  } else if (FLAGS_experiment == 2) {
    application = new Microbenchmark(config, multiplexer, FLAGS_hot_records);
    application->InitializeStorage(storage, config);
  }

  LOG(INFO) << FLAGS_machine_id << ":Created application "; 

  // Create Paxos
  LocalPaxos* paxos = NULL;
  if (FLAGS_machine_id % config->nodes_per_replica() < 3) {
    paxos = new LocalPaxos(config, multiplexer, FLAGS_type);
  }

  LOG(INFO) << FLAGS_machine_id << ":Created paxos log "; 

  // Initialize sequencer component and start sequencer thread running.
  LowlatencySequencer sequencer(config, multiplexer, client, paxos, storage, FLAGS_max_batch_size);

  LOG(INFO) << FLAGS_machine_id << ":Created sequencer ";

  // Run scheduler in main thread.
  DeterministicScheduler scheduler(config,
                                   storage,
                                   application,
                                   multiplexer,
                                   FLAGS_mode);

  LOG(INFO) << FLAGS_machine_id << ":Created scheduler "; 

  while (!config->Stopped()) {
    usleep(1000000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

