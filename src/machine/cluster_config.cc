// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "machine/cluster_config.h"

#include <map>
#include <set>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>


using std::map;
using std::set;
using std::string;
using std::ifstream;
using std::ostringstream;

uint64 ClusterConfig::HashBatchID(uint64 batch_id) {
  return FNVHash(UInt64ToString(33 * batch_id)) % nodes_per_replica();
}

uint64 ClusterConfig::LookupMachineID(uint64 relative_id, uint64 replica) {
   return replica * nodes_per_replica() + relative_id;
}

// TODO(kun): Implement better (application-specific?) partitioning.
uint64 ClusterConfig::LookupPartition(const Key& key) {
  // For microbenchmark
  return StringToInt(key) % nodes_per_replica();
}

uint32 ClusterConfig::LookupMaster(const Key& key) {
  // For microbenchmark
  return (StringToInt(key) / nodes_per_replica()) % replicas_size_;
}

// Checks to see if a config string appears to be a valid cluster config repr.
void CheckString(const string& config) {
  // Track machine ids and host-port pairs that we have already seen to ensure
  // that none are repeated.
  set<uint64> machine_ids;
  set<string> host_port_pairs;

  // Split config string into lines.
  vector<string> entries = SplitString(config, '\n');
  for (uint32 i = 0; i < entries.size(); i++) {
    // Skip empty lines.
    if (!entries[i].empty()) {
      // Each entry should consist of three colon-delimited parts: id, host and
      // port. Parse entry and check validity of id/host/port.
      vector<string> entry = SplitString(entries[i], ':');
      CHECK(entry.size() == 4) << "bad config line: " << entries[i];
      uint64 id = static_cast<uint64>(StringToInt(entry[0]));
      uint32 replica = static_cast<uint32>(StringToInt(entry[1]));
      string host = entry[2];
      int port = StringToInt(entry[3]);
      CHECK(static_cast<int64>(id) >= 0)
          << "bad machine id: " << static_cast<int64>(id);
      CHECK(static_cast<int64>(replica) >= 0)
          << "bad replica id: " << static_cast<int64>(replica);
      CHECK(host.size() > 0) << "empty hostname";
      CHECK(port >= 0) << "bad port: " << port;

      // Check for repeated machine ids.
      CHECK(machine_ids.count(id) == 0)
          << "repeated machine id: " << id;
      machine_ids.insert(id);


      // Check for repeated host/port pairs.
      string hostport = host + ":" + IntToString(port);
      CHECK(host_port_pairs.count(hostport) == 0)
          << "repeated host/port pair: " << hostport;
      host_port_pairs.insert(hostport);
    }
  }
}

// Checks to see if a config proto appears to be a valid cluster config repr.
void CheckProto(const ClusterConfigProto& config) {
  // Track machine ids and host-port pairs that we have already seen to ensure
  // that none are repeated.
  set<uint64> machine_ids;
  set<string> host_port_pairs;

  for (int i = 0; i < config.machines_size(); i++) {
    // Check validity of id/host/port.
    CHECK(config.machines(i).has_id()) << "missing machind id";
    CHECK(config.machines(i).has_replica()) << "missing replica id";
    CHECK(config.machines(i).has_host())<< "missing host";
    CHECK(config.machines(i).has_port())<< "missing port";
    CHECK(static_cast<int64>(config.machines(i).id()) >= 0)
        << "bad machine id: " << static_cast<int64>(config.machines(i).id());
    CHECK(static_cast<int64>(config.machines(i).replica()) >= 0)
        << "bad replica id: " << static_cast<int64>(config.machines(i).replica());
    CHECK(config.machines(i).host().size() > 0)
        << "empty hostname";
    CHECK(config.machines(i).port() >= 0)
        << "bad port: " << config.machines(i).port();

    // Check for repeated machine ids.
    CHECK(machine_ids.count(config.machines(i).id()) == 0)
        << "repeated machine id: " << config.machines(i).id();
    machine_ids.insert(config.machines(i).id());

    // Check for repeated host/port pairs.
    string hostport = config.machines(i).host() + ":" +
                      IntToString(config.machines(i).port());
    CHECK(host_port_pairs.count(hostport) == 0)
        << "repeated host/port pair: " << hostport;
    host_port_pairs.insert(hostport);
  }
}


void ClusterConfig::FromFile(const string& filename) {
  string config;

  ifstream ifile(filename.c_str());
  ostringstream buf;
  char ch;
  while(buf&&ifile.get(ch)) {
    buf.put(ch);
  }

  config = buf.str();

  FromString(config);
}

void ClusterConfig::FromString(const string& config) {
  CheckString(config);

  // Clear any previous machine information.
  machines_.clear();
  machines_replica_.clear();
  set<uint32> replicas;

  // Split config string into lines.
  vector<string> entries = SplitString(config, '\n');
  for (uint32 i = 0; i < entries.size(); i++) {
    // Skip empty lines.
    if (!entries[i].empty()) {
      // Each entry should consist of three colon-delimited parts: id, host and
      // port. Parse entry and check validity of id/host/port.
      vector<string> entry = SplitString(entries[i], ':');

      // Add entry.
      uint64 id = static_cast<uint64>(StringToInt(entry[0]));
      uint32 replica = static_cast<uint32>(StringToInt(entry[1]));

      machines_[id].set_id(id);
      machines_[id].set_replica(replica);
      machines_[id].set_host(entry[2]);
      machines_[id].set_port(StringToInt(entry[3]));

      machines_replica_[id] = replica;
      replicas.insert(replica);

      if (id == local_node_id_) {
        local_replica_ = replica;
      }
    }
  }

  replicas_size_ = replicas.size();
  relative_node_id_ = local_node_id_ % (machines_.size() / replicas_size_);
}

void ClusterConfig::FromProto(const ClusterConfigProto& config) {
  machines_.clear();
  machines_replica_.clear();
  set<uint32> replicas;

  for (int i = 0; i < config.machines_size(); i++) {
    machines_[config.machines(i).id()] = config.machines(i);
    machines_replica_[config.machines(i).id()] = config.machines(i).replica();
    replicas.insert(config.machines(i).replica());

    if (config.machines(i).id() == local_node_id_) {
      local_replica_ = config.machines(i).replica();
    }
  }

  replicas_size_ = replicas.size();
  relative_node_id_ = local_node_id_ % (machines_.size() / replicas_size_);
}


