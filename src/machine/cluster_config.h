// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// A ClusterConfig represents a local view of a collection of Machines.
// Constructing a ClusterConfig object does not actually deploy an application
// across the collection of physical servers (use ClusterManager for that).
//


#ifndef CALVIN_MACHINE_CLUSTER_CONFIG_H_
#define CALVIN_MACHINE_CLUSTER_CONFIG_H_

#include <map>
#include <string>
#include <atomic>

#include "common/types.h"
#include "common/utils.h"
#include "proto/cluster_config.pb.h"

using std::map;
using std::string;

class ClusterConfig {
 public:
  // Default constructor creates a ClusterConfig consisting of no Machines.
  // Note that this is completely useless until 'FromFile()' or 'FromString()'
  // is called to populate it with information about the actual cluster
  // configuration.
  ClusterConfig(uint64 local_node_id): local_node_id_(local_node_id), next_guid_(1000) {}

  ClusterConfig(): local_node_id_(0), next_guid_(1000) {}

  // Populates a ClusterConfig using a specification consisting of zero or
  // more (newline delimited) lines of the format:
  //
  //    <machine-id>:<replica-id>:<ip>:<port>
  //
  // The specification can either be read from a file, provided as a string,
  // or provided as a protobuf (see proto/cluster_config.proto).
  //
  // Each MachineID that appears in the specification must be unique, as must
  // each (ip, port) pair.
  void FromFile(const string& filename);
  void FromString(const string& config);
  void FromProto(const ClusterConfigProto& config);

  // ClusterConfigs can also be written out to files, strings, or protos.
  void ToFile(const string& filename);
  void ToString(string* out);
  void ToProto(ClusterConfigProto* out);

  uint64 HashBatchID(uint64 batch_id);

  uint64 LookupMachineID(uint64 relative_id, uint64 replica);

  uint64 LookupPartition(const Key& key);

  // Returns the number of machines that appear in the config.
  inline int all_nodes_size() const {
    return static_cast<int>(machines_.size());
  }

  inline uint64 nodes_per_replica() const {
    return static_cast<uint64>(machines_.size()/replicas_size_);
  }

  inline uint64 local_node_id() const {
    return local_node_id_;
  }

  inline uint64 relative_node_id() const {
    return relative_node_id_;
  }

  inline uint32 replicas_size() const {
    return replicas_size_;
  }

  inline uint32 local_replica_id() const {
    return local_replica_;
  }

  // Returns a globally unique ID (no ordering guarantees though).
  uint64 GetGUID() {
    return 1 + local_node_id_ + (all_nodes_size() * (next_guid_++));
  }

  // Returns true and populates '*info' accordingly iff the config tracks a
  // machine with id 'id'.
  inline bool lookup_machine(uint64 id, MachineInfo* info) {
    if (machines_.count(id)) {
      *info = machines_[id];
      return true;
    }
    return false;
  }

  // Returns a const ref to the underlying collection of machine records.
  const map<uint64, MachineInfo>& machines() {
    return machines_;
  }

  // Contains all machines.
  map<uint64, MachineInfo> machines_;

 private:

  map<uint64, uint32> machines_replica_;

  uint32 replicas_size_;

  uint64 local_node_id_;

  uint32 local_replica_;

  uint64 relative_node_id_;

  // Globally unique ID source.
  std::atomic<uint64> next_guid_;

  // Intentionally copyable.
};

#endif  // CALVIN_MACHINE_CLUSTER_CONFIG_H_

