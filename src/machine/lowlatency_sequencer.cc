// Author: Kun Ren <renkun.nwpu@gmail.com>
//
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//

#include "machine/lowlatency_sequencer.h"

void* LowlatencySequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<LowlatencySequencer*>(arg)->RunWriter();
  return NULL;
}

void* LowlatencySequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<LowlatencySequencer*>(arg)->RunReader();
  return NULL;
}

LowlatencySequencer::LowlatencySequencer(ClusterConfig* conf, ConnectionMultiplexer* connection, Client* client, LocalPaxos* paxos, Storage* storage, uint32 max_batch_size)
          : epoch_duration_(0.005), configuration_(conf), connection_(connection),
          client_(client), deconstructor_invoked_(false), paxos_log_(paxos), storage_(storage), max_batch_size_(max_batch_size) {
  // Start Sequencer main loops running in background thread.

  connection_->NewChannel("sequencer_");
  connection_->NewChannel("sequencer_txn_receive_");

  start_working_ = false;

  cpu_set_t cpuset;
  pthread_attr_t attr_writer;
  pthread_attr_init(&attr_writer);
  CPU_ZERO(&cpuset);
CPU_SET(0, &cpuset);
//  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter, reinterpret_cast<void*>(this));

  CPU_ZERO(&cpuset);
//  CPU_SET(2, &cpuset);
CPU_SET(0, &cpuset);
  pthread_attr_t attr_reader;
  pthread_attr_init(&attr_reader);
  pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader, reinterpret_cast<void*>(this));
}

LowlatencySequencer::~LowlatencySequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}


void LowlatencySequencer::RunWriter() {

  uint32 local_replica = configuration_->local_replica_id();
  uint64 local_machine = configuration_->local_node_id();
  uint64 relative_machine = configuration_->relative_node_id();
  uint64 nodes_per_replica = configuration_->nodes_per_replica();

  // Set up batch messages for each system node.
  MessageProto batch_message;
  batch_message.set_destination_channel("sequencer_");
  batch_message.set_type(MessageProto::TXN_BATCH);
  batch_message.set_source_node(local_machine);
  batch_message.add_misc_bool(true);

  uint64 batch_number;
  uint32 txn_id_offset;

  // txn_id, pair<expected_remote_lookup, txn> 
  map<uint64, pair<uint64, TxnProto*>> expected_master_lookups;
  map<uint64, set<uint32>> involved_replicas;

  map<uint64, MessageProto> lookup_master_batch;

  for (uint64 i = 0; i < configuration_->nodes_per_replica();i++) {
    lookup_master_batch[i].set_destination_channel("sequencer_");
    lookup_master_batch[i].set_destination_node(configuration_->LookupMachineID(i, local_replica));
    lookup_master_batch[i].set_type(MessageProto::MASTER_LOOKUP);
    lookup_master_batch[i].set_source_node(local_machine);
  }

  MessageProto txn_message;
  txn_message.set_destination_channel("sequencer_txn_receive_");
  txn_message.set_type(MessageProto::TXN_FORWORD);
  txn_message.set_source_node(local_machine);

#ifdef LATENCY_TEST
latency_counter = 0;
#endif

  connection_->NewChannel("synchronization_sequencer_channel");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("synchronization_sequencer_channel");
  for (uint64 i = 0; i < (uint64)(configuration_->all_nodes_size()); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint64>(configuration_->local_node_id())) {
      connection_->Send(synchronization_message);
    }
  }

  uint32 synchronization_counter = 1;
  while (synchronization_counter < (uint64)(configuration_->all_nodes_size())) {
    synchronization_message.Clear();
    if (connection_->GotMessage("synchronization_sequencer_channel", &synchronization_message)) {
      CHECK(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  connection_->DeleteChannel("synchronization_sequencer_channel");
LOG(INFO) << configuration_->local_node_id()<< "---In sequencer:  After synchronization. Starting sequencer writer.";

  start_working_ = true;
  MessageProto message;

  bool sent_lookup_request = false;

  while (!deconstructor_invoked_) {
    // Begin epoch.
    batch_number = configuration_->GetGUID();
    double epoch_start = GetTime();
    batch_message.set_batch_number(batch_number);
    batch_message.clear_data();

    sent_lookup_request = false;

    // Collect txn requests for this epoch.
    txn_id_offset = 0;
    while (!deconstructor_invoked_ && GetTime() < epoch_start + epoch_duration_) {
      // Check messages
      bool got_message = connection_->GotMessage("sequencer_txn_receive_", &message);
      if (got_message == true) {
        if (message.type() == MessageProto::TXN_FORWORD) {
          // Got forwarded txn message
          TxnProto txn;
          txn.ParseFromString(message.data(0));
          txn.set_origin_replica(local_replica);
          
          string txn_string;
          txn.SerializeToString(&txn_string);
          batch_message.add_data(txn_string);
/**if (txn.remaster_txn() == true)
LOG(INFO) << configuration_->local_node_id()<<": ----In sequencer writer:  received a remaster txn: "<<txn.txn_id();
else 
LOG(INFO) << configuration_->local_node_id()<<": ----In sequencer writer:  received a aborted txn;"<<txn.txn_id()<<" replica size:"<<txn.involved_replicas_size();**/
        } else if (message.type() == MessageProto::MASTER_LOOKUP_RESULT) {
          // Got master lookup result message
          LookupMasterResultEntry lookup_result_entry;

          for (uint32 i = 0; i < (uint32)message.data_size(); i++) {
            lookup_result_entry.ParseFromString(message.data(i)); 

            uint64 txn_id = lookup_result_entry.txn_id();

            TxnProto* txn = expected_master_lookups[txn_id].second;
            CHECK(txn != NULL);
            uint32 expected_remote = expected_master_lookups[txn_id].first;
        
            map<Key, KeyEntry> remote_entries_map;
            for (uint32 j = 0; j < (uint32)lookup_result_entry.key_entries_size(); j++) {
              KeyEntry key_entry = lookup_result_entry.key_entries(j);
              remote_entries_map[key_entry.key()] = key_entry;
            }

            for (uint32 i = 0; i < (uint32)(txn->read_set_size()); i++) {
              KeyEntry key_entry = txn->read_set(i);
              if (remote_entries_map.find(key_entry.key()) != remote_entries_map.end()) {
                txn->mutable_read_set(i)->set_master(remote_entries_map[key_entry.key()].master());
                txn->mutable_read_set(i)->set_counter(remote_entries_map[key_entry.key()].counter());
                expected_remote--;

                involved_replicas[txn_id].insert(remote_entries_map[key_entry.key()].master());
              }
            }

            for (uint32 i = 0; i < (uint32)(txn->read_write_set_size()); i++) {
              KeyEntry key_entry = txn->read_write_set(i);
              if (remote_entries_map.find(key_entry.key()) != remote_entries_map.end()) {
                txn->mutable_read_write_set(i)->set_master(remote_entries_map[key_entry.key()].master());
                txn->mutable_read_write_set(i)->set_counter(remote_entries_map[key_entry.key()].counter());
                expected_remote--;

                involved_replicas[txn_id].insert(remote_entries_map[key_entry.key()].master());
              }
            }

            if (expected_remote == 0) {
//LOG(INFO) << configuration_->local_node_id()<<": ----In sequencer writer:  received all master info;";
              expected_master_lookups.erase(txn_id);

              // Add involved replicas
              for (uint32 replica : involved_replicas[txn_id]) {
                txn->add_involved_replicas(replica);
              }  
              involved_replicas.erase(txn_id);

              // ready to put into the batch
              string txn_string;
              txn->SerializeToString(&txn_string);

              if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) == local_replica) {
                batch_message.add_data(txn_string);
              } else if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) != local_replica) {
                uint64 machine_sent = txn->involved_replicas(0) * nodes_per_replica + rand() % nodes_per_replica;
                txn_message.clear_data();
                txn_message.add_data(txn_string);
                txn_message.set_destination_node(machine_sent);
                connection_->Send(txn_message);
              } else if (txn->involved_replicas_size() > 1 && local_replica == 0) {
                batch_message.add_data(txn_string);
              } else {
                uint64 machine_sent = rand() % nodes_per_replica;
                txn_message.clear_data();
                txn_message.add_data(txn_string);
                txn_message.set_destination_node(machine_sent);
                connection_->Send(txn_message);
              }

              delete txn;
            } else {
              expected_master_lookups[txn_id].first = expected_remote;
            } 
          }  // end for
        } // end MASTER_LOOKUP_RESULT  
      } else if (txn_id_offset < max_batch_size_) {  // end if (got_message == true)
//      } else if (local_machine == 0 &&  txn_id_offset < max_batch_size_) {  // end if (got_message == true)
        // Add next txn request to batch.
        TxnProto* txn;

        LOG(INFO) << "inside sequencer: getting txn, offset: " << txn_id_offset << " max: " << max_batch_size_;
        client_->GetTxn(&txn, batch_number * max_batch_size_ + txn_id_offset);

        // no more local txns
        if (txn == NULL) {
          continue;
        }

        txn->set_origin_replica(local_replica);
        txn->set_client_replica(local_replica);
        txn_id_offset++;
        LOG(INFO) << "inside sequencer: inc offset: " << txn_id_offset;

#ifdef LATENCY_TEST
    if (txn->txn_id() % SAMPLE_RATE == 0 && latency_counter < SAMPLES) {
      sequencer_recv[txn->txn_id()] = GetTime();
      txn->set_generated_machine(local_machine);
    }
#endif
        // Lookup the masters for keys
        map<uint64, set<string>> remote_keys;
        uint32 remote_expected = 0;
        uint64 txn_id = txn->txn_id();

        for (uint32 i = 0; i < (uint32)(txn->read_set_size()); i++) {
          KeyEntry key_entry = txn->read_set(i);
          uint64 mds = configuration_->LookupPartition(key_entry.key());

          if (mds == relative_machine) {
            // Get <master, counter> pair for local keys
            pair<uint32, uint64> key_info = storage_->GetMasterCounter(key_entry.key());
            txn->mutable_read_set(i)->set_master(key_info.first);
            txn->mutable_read_set(i)->set_counter(key_info.second);
            involved_replicas[txn_id].insert(key_info.first);
          } else {
            remote_expected++;
            remote_keys[mds].insert(key_entry.key());
          }
        }

        for (uint32 i = 0; i < (uint32)(txn->read_write_set_size()); i++) {
          KeyEntry key_entry = txn->read_write_set(i);
          uint64 mds = configuration_->LookupPartition(key_entry.key());

          if (mds == relative_machine) {
            // Get <master, counter> pair for local keys
            pair<uint32, uint64> key_info = storage_->GetMasterCounter(key_entry.key());
            txn->mutable_read_write_set(i)->set_master(key_info.first);
            txn->mutable_read_write_set(i)->set_counter(key_info.second);
            involved_replicas[txn_id].insert(key_info.first);
          } else {
            remote_expected++;
            remote_keys[mds].insert(key_entry.key());
          }
        }

        // All keys are on local machine (All keys within this partition, but might be mastered
        // in other region/sreplicas). If the keys aren't in our partition, we can't ask our own
        // storage "GetMasterCounter"
        if (remote_expected == 0) {
          // Add involved replicas
          for (uint32 replica : involved_replicas[txn_id]) {
            txn->add_involved_replicas(replica);
          }
          involved_replicas.erase(txn_id);

          // ready to put into the batch
          string txn_string;
          txn->SerializeToString(&txn_string);

          // Single-home txn on in this region
          if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) == local_replica) {
            batch_message.add_data(txn_string);
          }
          // Single-home txn in a different region. Send directly to a random machine within that region
          else if (txn->involved_replicas_size() == 1 && txn->involved_replicas(0) != local_replica) {
            uint64 machine_sent = txn->involved_replicas(0) * nodes_per_replica + rand() % nodes_per_replica;
            txn_message.clear_data();
            txn_message.add_data(txn_string);
            txn_message.set_destination_node(machine_sent);
            connection_->Send(txn_message);
          }
          // Multi-home txn, and we are the sequencer for mh txns (hard-coded as region 0)
          else if (txn->involved_replicas_size() > 1 && local_replica == 0) {
            batch_message.add_data(txn_string);
          }
          // Multi-home, send to a random machine in region 0
          else {
            uint64 machine_sent = rand() % nodes_per_replica;
            txn_message.clear_data();
            txn_message.add_data(txn_string);
            txn_message.set_destination_node(machine_sent);
            connection_->Send(txn_message);
          }

          delete txn;
        } 
        // Txn access set is split among partitions. Add lookup_master requests to batch, and save the txn for later
        else {
          expected_master_lookups[txn_id] = make_pair(remote_expected, txn);

          LookupMasterEntry lookup_master_entry;
          lookup_master_entry.set_txn_id(txn_id);

          for(auto it = remote_keys.begin(); it != remote_keys.end();it++) {
            // Send message to remote machines to get the mastership of remote records
            uint64 remote_machineid = it->first;
            set<string> keys = it->second;
            
            lookup_master_entry.clear_keys();
            for (auto it2 = keys.begin(); it2 != keys.end(); it2++) {
              lookup_master_entry.add_keys(*it2);
            }

            string entry_string;
            lookup_master_entry.SerializeToString(&entry_string);

            lookup_master_batch[remote_machineid].add_data(entry_string);
          } // end for
        } // end if (remote_expected == 0) 
      } else { //if (txn_id_offset < max_batch_size_) 
        // Send this epoch's lookup_master requests.
        LOG(INFO) << "inside sequencer: sending batch, offset: " << txn_id_offset;
        if (sent_lookup_request == false) {
          for (map<uint64, MessageProto>::iterator it = lookup_master_batch.begin(); it != lookup_master_batch.end(); ++it) {
            if (it->second.data_size() > 0) {
              connection_->Send(it->second);
              it->second.clear_data();
            } 
          }
          sent_lookup_request = true;
        }

         if (got_message == false) {
           usleep(50);
         }
      }
    }

//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader:  batch size:"<<(uint32)(batch_message.data_size());
    if (batch_message.data_size() == 0) {
      continue;
    } 

    // Send this epoch's transactions to the central machine of each replica
    for (uint32 i = 0; i < configuration_->replicas_size(); i++) {
      uint64 machine_id = configuration_->LookupMachineID(configuration_->HashBatchID(batch_message.batch_number()), i);
//if (configuration_->local_node_id() == 0 && i == 0)
//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader:  will send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
      batch_message.set_destination_node(machine_id);
      connection_->Send(batch_message);
//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader:  after send TXN_BATCH to :"<<machine_id<<"  batch_id:"<<batch_number;
    }

  }
}


void LowlatencySequencer::RunReader() {

//LOG(INFO) << "In sequencer:  Starting sequencer reader.";
  // Set up batch messages for each system node.
  map<uint64, MessageProto> batches;

  // region id
  uint32 local_replica = configuration_->local_replica_id();
  for (uint64 i = 0; i < configuration_->nodes_per_replica();i++) {
    batches[i].set_destination_channel("scheduler_");
    batches[i].set_destination_node(configuration_->LookupMachineID(i, local_replica));
    batches[i].set_type(MessageProto::TXN_SUBBATCH);
  }
   
  MessageProto message;
  uint64 batch_number;

  // paxos leader is first node in each region
  uint64 local_paxos_leader_ = local_replica * configuration_->nodes_per_replica();

  MessageProto master_lookup_result_message;
  master_lookup_result_message.set_destination_channel("sequencer_txn_receive_");
  master_lookup_result_message.set_type(MessageProto::MASTER_LOOKUP_RESULT);

  while (start_working_ != true) {
    usleep(100);
  }

  while (!deconstructor_invoked_) {

    bool got_message = connection_->GotMessage("sequencer_", &message);
    if (got_message == true) {
      if (message.type() == MessageProto::TXN_BATCH) {
//LOG(INFO) << configuration_->local_node_id()<< ":In sequencer reader:  recevie TXN_BATCH message:"<<message.batch_number();
        batch_number = message.batch_number();

        //  If (This batch come from this replica) → send BATCH_SUBMIT to the the master node of the local paxos participants; Ignore the new_generated txns
        if (configuration_->LookupReplica(message.source_node()) == local_replica && message.misc_bool(0) == true) {
          // Send “BATCH_SUBMIT” to the head machines;
          MessageProto batch_submit_message;
          batch_submit_message.set_destination_channel("sequencer_");
          batch_submit_message.set_destination_node(local_paxos_leader_);
          batch_submit_message.set_type(MessageProto::BATCH_SUBMIT);
          batch_submit_message.add_misc_int(message.batch_number());
          connection_->Send(batch_submit_message);
//if (configuration_->local_node_id() == 0)
//LOG(INFO) << configuration_->local_node_id()<<": In sequencer reader: send BATCH_SUBMIT, id:"<<message.batch_number();    
        }

        // If received TXN_BATCH: Parse batch and forward sub-batches to relevant readers (same replica only).
        for (int i = 0; i < message.data_size(); i++) {
          TxnProto txn;
          txn.ParseFromString(message.data(i));

          if (txn.fake_txn() == true) {
            continue;
          }
          // Compute readers & writers; store in txn proto.
          set<uint64> readers;
          set<uint64> writers;
          for (uint32 i = 0; i < (uint32)(txn.read_set_size()); i++) {
            KeyEntry key_entry = txn.read_set(i);
            if (key_entry.master() == txn.origin_replica()) {
              uint64 mds = configuration_->LookupPartition(key_entry.key());
              readers.insert(mds);
            }
          }
          for (uint32 i = 0; i < (uint32)(txn.read_write_set_size()); i++) {
            KeyEntry key_entry = txn.read_write_set(i);
            if (key_entry.master() == txn.origin_replica()) {
              uint64 mds = configuration_->LookupPartition(key_entry.key());
              writers.insert(mds);
              readers.insert(mds);
            }
          }

          for (set<uint64>::iterator it = readers.begin(); it != readers.end(); ++it) {
            txn.add_readers(*it);
          }
          for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
            txn.add_writers(*it);
          }

          string txn_data;
          txn.SerializeToString(&txn_data);

          // Compute union of 'readers' and 'writers' (store in 'readers').
          for (set<uint64>::iterator it = writers.begin(); it != writers.end(); ++it) {
            readers.insert(*it);
          }

          // Insert txn into appropriate batches.
          for (set<uint64>::iterator it = readers.begin(); it != readers.end(); ++it) {
            batches[*it].add_data(txn_data);
          }
        }

        // Send this epoch's requests to all schedulers.
        for (map<uint64, MessageProto>::iterator it = batches.begin(); it != batches.end(); ++it) {
          it->second.set_batch_number(batch_number);
          connection_->Send(it->second);
          it->second.clear_data();
        }

        // Forward "relevant multi-replica action" to the head node
        if (configuration_->LookupReplica(message.source_node()) == 0 && local_replica != 0) {
          MessageProto mr_message;
          for (int i = 0; i < message.data_size(); i++) {
            TxnProto txn;
            txn.ParseFromString(message.data(i));
            if (txn.involved_replicas_size() > 1 && txn.new_generated() == false) {
              for (int j = 0; j < txn.involved_replicas_size(); j++) {
                if (txn.involved_replicas(j) == local_replica) {
                  string txn_data;
                  txn.SerializeToString(&txn_data);
                  mr_message.add_data(txn_data);
//LOG(INFO) << configuration_->local_node_id()<<":--- In sequencer reader: append txn into mr_message:"<<txn.txn_id();
                }
              }
            }
          }

          mr_message.set_destination_channel("paxos_log_");
          mr_message.set_destination_node(local_paxos_leader_);
          mr_message.set_type(MessageProto::MR_TXNS_BATCH);
          mr_message.set_source_node(message.source_node());
          mr_message.add_misc_int(message.batch_number());
          connection_->Send(mr_message);
        }

      } else if (message.type() == MessageProto::BATCH_SUBMIT) {
        uint64 batch_id = message.misc_int(0);
        paxos_log_->Append(batch_id);
      } else if (message.type() == MessageProto::MASTER_LOOKUP) {
        // Lookup local KeyEntries and sent it back
        master_lookup_result_message.set_destination_node(message.source_node());
        master_lookup_result_message.clear_data();
        LookupMasterEntry lookup_master_entry;

        for (uint32 i = 0; i < (uint32)message.data_size(); i++) {
          lookup_master_entry.ParseFromString(message.data(i));
          uint64 txn_id  = lookup_master_entry.txn_id();

          LookupMasterResultEntry result_entry;
          result_entry.set_txn_id(txn_id);

          for (uint32 j = 0; j < (uint32)lookup_master_entry.keys_size(); j++) {
            string key = lookup_master_entry.keys(j);

            pair<uint32, uint64> replica_counter = storage_->GetMasterCounter(key);

            KeyEntry* e = result_entry.add_key_entries();
            e->set_key(key);
            e->set_master(replica_counter.first);
            e->set_counter(replica_counter.second);
          }

          string result_entry_string;
          result_entry.SerializeToString(&result_entry_string);
          master_lookup_result_message.add_data(result_entry_string);
        }

        connection_->Send(master_lookup_result_message);

      } // end else
    } // end if (got_message == true) 

    if (got_message == false) {
      usleep(50);
    }
  }
}
