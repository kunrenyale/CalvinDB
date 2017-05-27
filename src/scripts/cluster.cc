// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_manager.h"

DEFINE_string(command, "status", "cluster command");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_string(calvin_path, "/home/ubuntu/CalvinDB", "path to the main calvin directory");
DEFINE_string(binary, "calvindb_server", "Calvin binary executable program");
DEFINE_string(ssh_key1, "-i ~/Virginia.pem", "ssh_key for the first data center(Virginia)");
DEFINE_string(ssh_key2, "-i ~/Oregon.pem", "ssh_key for the second data center(Oregon)");
DEFINE_string(ssh_key3, "-i ~/Ireland.pem", "ssh_key for the third data center(Ireland)");
DEFINE_int32(experiment, 0, "the experiment that you want to run");
DEFINE_int32(percent_mp, 20, "percent of distributed txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 200, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  ClusterManager cm(FLAGS_config, FLAGS_calvin_path, FLAGS_binary, FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3);

  if (FLAGS_command == "update") {
    cm.Update();

  } else if (FLAGS_command == "put-config") {
    cm.PutConfig();

  } else if (FLAGS_command == "get-data") {
    cm.GetTempFiles("report.");

  } else if (FLAGS_command == "start") {
    cm.DeployCluster(GetTime() + 10, FLAGS_experiment, FLAGS_percent_mp, FLAGS_hot_records, FLAGS_max_batch_size);

  } else if (FLAGS_command == "kill") {
    cm.KillCluster();

  } else if (FLAGS_command == "kill-partial") {
    cm.KillReplica(2);

  } else if (FLAGS_command == "status") {
    cm.ClusterStatus();

  } else {
    LOG(FATAL) << "unknown command: " << FLAGS_command;
  }
  return 0;
}

