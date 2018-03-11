// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// A ClusterManager is a tool for deploying, tracking, maintaining, modifying,
// and tearing down machine clusters.
//
//

#ifndef CALVIN_MACHINE_CLUSTER_MANAGER_H_
#define CALVIN_MACHINE_CLUSTER_MANAGER_H_

#include <string>

#include "machine/cluster_config.h"


using std::string;

class ClusterManager {
 public:
  // Sets initial target config.
  ClusterManager(const string& config_file, const string& calvin_path,
                 const string& binary, const uint32& mode, const uint32& type,
                 const string& ssh_key1, const string& ssh_key2, const string& ssh_key3)
      : config_file_(config_file), calvin_path_(calvin_path), binary_(binary), mode_(mode), type_(type),
        ssh_username_("ubuntu"), ssh_key1_(ssh_key1), ssh_key2_(ssh_key2), ssh_key3_(ssh_key3) {
    config_.FromFile(config_file_);
    num_replicas_ = 3;
  }

  // Sets initial target config.
  ClusterManager(const string& config_file, const string& calvin_path,
                 const string& binary, const uint32& mode, const uint32& type,
                 const string& ssh_key1, const string& ssh_key2, const string& ssh_key3,
                 const string& ssh_key4, const string& ssh_key5, const string& ssh_key6)
      : config_file_(config_file), calvin_path_(calvin_path), binary_(binary), mode_(mode), type_(type),
        ssh_username_("ubuntu"), ssh_key1_(ssh_key1), ssh_key2_(ssh_key2), ssh_key3_(ssh_key3),
        ssh_key4_(ssh_key4), ssh_key5_(ssh_key5), ssh_key6_(ssh_key6) {
    config_.FromFile(config_file_);
    num_replicas_ = 6;
  }

  ~ClusterManager() {
  }

  // Runs "svn up" and rebuilds calvin on every machine in the cluster.
  void Update();

  // Attempts to deploy the cluster according to config....
  //
  // First, performs several checks (and dies with a useful error message if
  // any of them fail):
  //  - checks that all participants are reachable by ssh
  //  - checks that all participants have calvin (with same version as server)
  //  - checks that all participants are NOT already running calvin instances
  //
  // Next, Run "svn up;make clean;make -j" to get the latest code and compile.
  //
  // Finally, ssh into all machines and start 'binary' running.
  //
  //
  // TODO(kun): FUTURE WORK - don't implement now:
  //  Also start a monitoring thread going that occasionally polls machines
  //  in the cluster to generate cluster status reports, repair problems, etc.
  void DeployCluster(int experiment, int percent_mp, int percent_mr, int hot_records, int max_batch_size);

  // Kills all participating machine processes (using 'ssh killall', so they do
  // not need to exit gracefully).
  void KillCluster();

  // Returns a human-readable report about cluster status including:
  //  - what participants are currently unreachable by ssh (if any)
  //  - what participants are reachable by ssh but NOT running an instance of
  //    the server binary
  void ClusterStatus();

  const ClusterConfig& GetConfig();

  void PutConfig();
  void GetTempFiles(const string& base);

 private:
  // Returns ssh key for machine m.
  const string& ssh_key(uint64 m);

  // Configuration of machines managed by this ClusterManager.
  ClusterConfig config_;

  string config_file_;

  string calvin_path_;

  string binary_;

  uint32 mode_;

  uint32 type_;

  // Username with which to ssh to machines.
  // Default: 'ubuntu'
  // TODO(kun): Make this more easily configurable.
  string ssh_username_;

  // For ssh authentication, used for EC2, if test on zoo, just set it " ";
  // If test on EC2, set it to "-i YOUR_KEY.pem"
  string ssh_key1_;
  string ssh_key2_;
  string ssh_key3_;
  string ssh_key4_;
  string ssh_key5_;
  string ssh_key6_;

  // Number of replicas
  uint32 num_replicas_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  ClusterManager();

  // DISALLOW_COPY_AND_ASSIGN
  ClusterManager(const ClusterManager&);
  ClusterManager& operator=(const ClusterManager&);
};

#endif  // CALVIN_MACHINE_CLUSTER_MANAGER_H_

