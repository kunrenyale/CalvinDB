// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_manager.h"

DEFINE_string(command, "status", "cluster command");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_string(calvin_path, "/home/ubuntu/CalvinDB", "path to the main calvin directory");
DEFINE_string(binary, "calvindb_server", "Calvin binary executable program");
DEFINE_string(lowlatency_binary, "lowlatency_calvindb_server", "Lowlatency Calvin binary executable program");
DEFINE_string(ssh_key1, "-i ~/Virginia.pem", "ssh_key for the first data center(Virginia)");
DEFINE_string(ssh_key2, "-i ~/Oregon.pem", "ssh_key for the second data center(Oregon)");
DEFINE_string(ssh_key3, "-i ~/Ireland.pem", "ssh_key for the third data center(Ireland)");
DEFINE_string(ssh_key4, "-i ~/Ohio.pem", "ssh_key for the first data center(Virginia)");
DEFINE_string(ssh_key5, "-i ~/California.pem", "ssh_key for the second data center(Oregon)");
DEFINE_string(ssh_key6, "-i ~/London.pem", "ssh_key for the third data center(Ireland)");
DEFINE_int32(lowlatency, 0, "0: Original CalvinDB ; 1: low latency version of CalvinDB; 2: low latency with access pattern remasters");
DEFINE_int32(type, 0, "[CalvinDB: 0: 3 replicas; 1: 6 replicas]; [Low latency: 0: 3 replicas normal; 1: 6 replicas normal; 2: 6 replicas strong availbility ] ");
DEFINE_int32(experiment, 0, "the experiment that you want to run, default is microbenchmark");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(percent_mr, 0, "percent of multi-replica txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 100, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  ClusterManager* cm;
  if (FLAGS_lowlatency == 0) {
    if (FLAGS_type == 0) {
      // 3 replicas original CalvinDB
      cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3);
    } else {
      cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3,
                              FLAGS_ssh_key4, FLAGS_ssh_key5, FLAGS_ssh_key6); 
    }
  } else {
    if (FLAGS_type == 0) {
        cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_lowlatency_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3);
    } else {
        cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_lowlatency_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3,
                 FLAGS_ssh_key4, FLAGS_ssh_key5, FLAGS_ssh_key6);    
    } 
  }

  if (FLAGS_command == "update") {
    cm->Update();

  } else if (FLAGS_command == "put-config") {
    cm->PutConfig();

  } else if (FLAGS_command == "get-data") {
    cm->GetTempFiles("report.");

  } else if (FLAGS_command == "start") {
    cm->DeployCluster(FLAGS_experiment, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records, FLAGS_max_batch_size);

  } else if (FLAGS_command == "kill") {
    cm->KillCluster();

  } else if (FLAGS_command == "status") {
    cm->ClusterStatus();

  } else {
    // LOG(FATAL) << "unknown command: " << FLAGS_command;
    cm->RunArbitrary(FLAGS_command);
  }
  return 0;
}

