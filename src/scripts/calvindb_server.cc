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
DEFINE_double(time, 0, "start time");
DEFINE_int32(experiment, 0, "experiment that you want to run");
DEFINE_int32(percent_mp, 20, "percent of distributed txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 200, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Print Calvin version
  if (FLAGS_calvin_version) {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (Geo-replicated Calvin) 0.1 (c) Yale University 2017.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  LOG(ERROR) << "Preparing to start Calvin node "
             << FLAGS_machine_id << "...";


  // Build this node's configuration object.
  ClusterConfig config;
  config.FromFile(FLAGS_config);

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);
  
  // Create Paxos
  Paxos* paxos = new Paxos(new LocalMemLog(), &config, multiplexer.NewConnection("paxos_log_"));

  Client* client = NULL;
  // Artificial loadgen clients. Right now only microbenchmark
  if (FLAGS_experiment == 0) {
    client = reinterpret_cast<Client*>(new MClient(&config, FLAGS_percent_mp, FLAGS_hot_records));
  }

  Storage* storage;
  storage = new SimpleStorage();
  
  Application* application = NULL; 
  if (FLAGS_experiment == 0) {
    application = new Microbenchmark(config.nodes_per_replica(), FLAGS_hot_records);
    application->InitializeStorage(storage, &config);
  } else {
    // TPCC()
  }

  // Initialize sequencer component and start sequencer thread running.
  Sequencer sequencer(&config, multiplexer.NewConnection("sequencer"), client, paxos, FLAGS_max_batch_size);


   // Run scheduler in main thread.
  if (FLAGS_experiment == 0) {
    DeterministicScheduler scheduler(&config,
                                     multiplexer.NewConnection("scheduler_"),
                                     storage,
                                     application);
  } else {
    // TPCC
  }

  Spin(1800);

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

