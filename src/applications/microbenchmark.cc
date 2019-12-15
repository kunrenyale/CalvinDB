// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "proto/txn.pb.h"


// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                                   uint64 key_limit, uint64 part) {
  CHECK(key_start % nparts == 0);
  keys->clear();
  for (uint32 i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    uint64 key;
    do {
      key = key_start + part + nparts * (rand() % ((key_limit - key_start)/nparts));
    } while (keys->count(key));
    keys->insert(key);
  }
}

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts), keys's master == replica
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                                          uint64 key_limit, uint64 part, uint32 replica) {
  CHECK(key_start % (nparts*replica_size) == 0);
  keys->clear();

  for (uint32 i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    uint64 key;
    uint64 order = rand() % ((key_limit - key_start)/(nparts*replica_size));
    key = key_start + part + nparts * (order * replica_size + replica);
    
    while (keys->count(key)) {
      order = rand() % ((key_limit - key_start)/(nparts*replica_size));
      key = key_start + part + nparts * (order * replica_size + replica);   
    }

    keys->insert(key);
  }
}


//--------- Create a  single-partition transaction -------------------------
TxnProto* Microbenchmark::MicroTxnSP(int64 txn_id, uint64 part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

  // Add two hot keys to read/write set.
  uint64 hotkey1 = part + nparts * (rand() % hot_records);

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  uint64 hotkey2 = part + nparts * (rand() % hot_records);
  while (hotkey2 == hotkey1) {
    hotkey2 = part + nparts * (rand() % hot_records);    
  };

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
  // read/write set.
  set<uint64> keys;
  GetRandomKeys(&keys,
                kRWSetSize - 2,
                nparts * hot_records,
                nparts * kDBSize,
                part);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  return txn;
}

//----------- Create a multi-partition transaction -------------------------
TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, uint64 part1, uint64 part2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey1 = part1 + nparts * (rand() % hot_records);
  uint64 hotkey2 = part2 + nparts * (rand() % hot_records);

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize/2 - 1 random cold keys from each partition into
  // read/write set.
  set<uint64> keys;
  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part2);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  return txn;
}

//------------- Create a single-replica single-partition transaction------------
TxnProto* Microbenchmark::MicroTxnSRSP(int64 txn_id, uint64 part, uint32 replica) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

//if (replica == 0)
//LOG(INFO) << ": In Microbenchmark::MicroTxnSRSP:  1";

  // Add two hot keys to read/write set.
  uint64 hotkey_order1 = (rand() % (hot_records/replica_size)) * replica_size + replica;

  uint64 hotkey_order2 = (rand() % (hot_records/replica_size)) * replica_size + replica;
  while (hotkey_order2 == hotkey_order1) {
    hotkey_order2 = (rand() % (hot_records/replica_size)) * replica_size + replica;
  };


//if (replica == 0)
//LOG(INFO) << ": In Microbenchmark::MicroTxnSRSP:  2";

  uint64 hotkey1 = part + nparts * hotkey_order1;
  uint64 hotkey2 = part + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
  // read/write set.
  uint64 key_start = nparts * hot_records;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                       kRWSetSize - 2,
                       key_start,
                       nparts * kDBSize,
                       part,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

//if (replica == 0)
//LOG(INFO) << ": In Microbenchmark::MicroTxnSRSP:  3";

  return txn;
}

//------------- Create a single-replica multi-partition transaction------------
TxnProto* Microbenchmark::MicroTxnSRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey_order1 = (rand() % (hot_records/replica_size)) * replica_size + replica;
  uint64 hotkey_order2 = (rand() % (hot_records/replica_size)) * replica_size + replica;


  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize/2 - 1 random cold keys from each partition into
  // read/write set.
  uint64 key_start = nparts * hot_records;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part1,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part2,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  return txn;
}

//------------- Create a multi-replica single-partition transaction------------
TxnProto* Microbenchmark::MicroTxnMRSP(int64 txn_id, uint64 part, uint32 replica1, uint32 replica2) {
  CHECK(replica1 != replica2);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

  if (replica1 > replica2) {
    uint32 tmp = replica1;
    replica1 = replica2;
    replica2 = tmp;
  } 

  if (replica1 != 0 && replica2 != 0) {
    txn->set_fake_txn(true);
  }

  // Add two hot keys to read/write set.
  uint64 hotkey_order1 = (rand() % (hot_records/replica_size)) * replica_size + replica1;
  uint64 hotkey_order2 = (rand() % (hot_records/replica_size)) * replica_size + replica2;

  uint64 hotkey1 = part + nparts * hotkey_order1;
  uint64 hotkey2 = part + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica1);
  key_entry->set_counter(0);

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica2);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize/2 - 1 random cold keys from specified replica/partition into
  // read/write set.

  uint64 key_start = nparts * hot_records;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part,
                       replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Insert set of kRWSetSize/2 - 1 random cold keys from specified replica/partition into
  // read/write set.
  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part,
                       replica2);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica2);
    key_entry->set_counter(0);
  }

  return txn;
}

//------------- Create a multi-replica multi-partition transaction------------
TxnProto* Microbenchmark::MicroTxnMRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica1, uint32 replica2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);

  if (replica1 > replica2) {
    uint32 tmp = replica1;
    replica1 = replica2;
    replica2 = tmp;

    uint64 tmp2 = part1;
    part1 = part2;
    part2 = tmp2;
  }

  if (replica1 != 0 && replica2 != 0) {
    txn->set_fake_txn(true);
  }

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey_order1 = (rand() % (hot_records/replica_size)) * replica_size + replica1;
  uint64 hotkey_order2 = (rand() % (hot_records/replica_size)) * replica_size + replica2;

  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica1);
  key_entry->set_counter(0);

  key_entry = txn->add_read_write_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica2);
  key_entry->set_counter(0);

  // Insert set of kRWSetSize/2 - 1 random cold keys from each replica/partition into
  // read/write set.

  uint64 key_start = nparts * hot_records;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }


  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part1,
                       replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Insert set of kRWSetSize/2 - 1 random cold keys from each replica/partition into
  // read/write set.
  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part2,
                       replica2);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica2);
    key_entry->set_counter(0);
  }

  return txn;
}


// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
TxnProto* Microbenchmark::NewTxn(int64 txn_id, int txn_type,
                                 string args, ClusterConfig* config) const {
  return NULL;
}

int Microbenchmark::Execute(TxnProto* txn, StorageManager* storage) const {  
  LOG(INFO) << "Execute txn id: " << txn->txn_id()<<"-"<<txn->origin_replica()<<"-"<<txn->lock_only()<< ", access set: " << txn->read_write_set_size();

  // Remaster txn
  if (txn->remaster_txn() == true) {
LOG(INFO) <<local_replica_<< ":*********In Execute:  handle remaster txn: "<<txn->txn_id();
    KeyEntry key_entry = txn->read_write_set(0);
    Record* val = storage->ReadObject(key_entry.key());

    CHECK(key_entry.master() == val->master);
    CHECK(key_entry.counter() == val->counter);

    val->master = txn->remaster_to();
    val->counter = key_entry.counter() + 1;

    if (local_replica_ == txn->remaster_from()) {
      val->remastering = false;
    }

    return 0;
  }

  // Normal txns, read the record and do some computations
  double factor = 1.0;
  uint32 txn_type = txn->txn_type();
  if (txn_type == MICROTXN_MP || (txn_type == MICROTXN_SP && txn->involved_replicas_size() > 1)) {
    factor = 2.0;
  }
  double execution_start = GetTime();


  for (uint32 i = 0; i < kRWSetSize; i++) {
    KeyEntry key_entry = txn->read_write_set(i);
    Record* val = storage->ReadObject(key_entry.key());
    // Not necessary since storage already has a pointer to val.
    //   storage->PutObject(txn->read_write_set(i), val);
 
    // Check whether we need to remaster this record
    if (storage->GetMode() == 2 && local_replica_ == val->master && val->remastering == false) {
      val->access_pattern[txn->client_replica()] = val->access_pattern[txn->client_replica()] + 1;

      if (txn->client_replica() != local_replica_ && val->access_pattern[txn->client_replica()]/(LAST_N_TOUCH*1.0) > ACCESS_PATTERN_THRESHOLD) {
        // Reach the threadhold, do the remaster
        val->remastering = true;

        // Create the remaster transction and sent to local sequencer
        TxnProto* remaster_txn = new TxnProto();
 
        remaster_txn->set_txn_id(config_->GetGUID());
        KeyEntry* remaster_key_entry = remaster_txn->add_read_write_set();
        remaster_key_entry->set_key(key_entry.key());
        remaster_key_entry->set_master(val->master);
        remaster_key_entry->set_counter(val->counter);

        remaster_txn->set_remaster_txn(true);
        remaster_txn->set_remaster_from(local_replica_);
        remaster_txn->set_remaster_to(txn->client_replica());

        remaster_txn->add_involved_replicas(local_replica_);

        string txn_string;
        remaster_txn->SerializeToString(&txn_string);

        MessageProto txn_message;
        txn_message.set_destination_channel("sequencer_txn_receive_");
        txn_message.set_type(MessageProto::TXN_FORWORD);
        txn_message.set_destination_node(config_->local_node_id());
        txn_message.add_data(txn_string);

        connection_->Send(txn_message);
LOG(INFO) <<local_replica_<< ":*********In Execute:  Generate a remaster  txn, on record: "<<key_entry.key()<<"  txn id:"<<txn->txn_id();
      }


      if (++val->access_cnt > LAST_N_TOUCH) {
        for (uint32 j = 0; j < REPLICA_SIZE; j++) {
          val->access_pattern[j] = 0;
        }
        val->access_cnt = 0;
      }

    } 


    for (int j = 0; j < 8; j++) {
      if ((val->value)[j] + 1 > 'z') {
        (val->value)[j] = 'a';
      } else {
        (val->value)[j] = (val->value)[j] + 1;
      }
    }

  }

  // The following code is for microbenchmark "long" transaction, uncomment it if for "long" transaction
  while (GetTime() - execution_start < 0.00012/factor) {
    int x = 1;
    for(int i = 0; i < 10000; i++) {
      x = x+10;
      x = x-2;
    }
  }

  return 0;
}

void Microbenchmark::InitializeStorage(Storage* storage, ClusterConfig* conf) const {
  char* int_buffer = (char *)malloc(sizeof(char)*kRecordSize);
  for (uint32 j = 0; j < kRecordSize - 1; j++) {
    int_buffer[j] = (rand() % 26 + 'a');
  }
  int_buffer[kRecordSize - 1] = '\0';

  for (uint64 i = 0; i < nparts*kDBSize; i++) {
    if (conf->LookupPartition(IntToString(i)) == conf->relative_node_id()) {
      string value(int_buffer);
      uint32 master = conf->LookupMaster(IntToString(i));
      storage->PutObject(IntToString(i), new Record(value, master));
    }

    //if (i % 1000000 == 0) {
    // 	LOG(INFO) <<conf->relative_node_id()<< ":*********In InitializeStorage:  Finish 100000 records ";
    //}
  }
}

