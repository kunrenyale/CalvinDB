// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "machine/cluster_config.h"
#include "proto/txn.pb.h"


// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<uint64>* keys, uint32 num_keys, uint64 key_start,
                                   uint64 key_limit, uint32 part) {
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
                                          uint64 key_limit, uint32 part, uint32 replica) {
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
TxnProto* Microbenchmark::MicroTxnSP(int64 txn_id, uint32 part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

  // Add two hot keys to read/write set.
  uint64 hotkey1 = part + nparts * (rand() % hot_records);
  txn->add_read_write_set(IntToString(hotkey1));

  uint64 hotkey2 = part + nparts * (rand() % hot_records);
  while (hotkey2 == hotkey1) {
    hotkey2 = part + nparts * (rand() % hot_records);    
  };
  txn->add_read_write_set(IntToString(hotkey2));

  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
  // read/write set.
  set<uint64> keys;
  GetRandomKeys(&keys,
                kRWSetSize - 2,
                nparts * hot_records,
                nparts * kDBSize,
                part);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->add_read_write_set(IntToString(*it));
  }

  return txn;
}

//----------- Create a multi-partition transaction -------------------------
TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, uint32 part1, uint32 part2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey1 = part1 + nparts * (rand() % hot_records);
  uint64 hotkey2 = part2 + nparts * (rand() % hot_records);
  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

  // Insert set of kRWSetSize/2 - 1 random cold keys from each partition into
  // read/write set.
  set<uint64> keys;
  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->add_read_write_set(IntToString(*it));
  }
  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part2);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->add_read_write_set(IntToString(*it));
  }

  return txn;
}

//------------- Create a single-replica single-partition transaction------------
TxnProto* Microbenchmark::MicroTxnSRSP(int64 txn_id, uint32 part, uint32 replica) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SRSP);
  txn->add_involved_replicas(replica);

//if (replica == 0)
//LOG(ERROR) << ": In Microbenchmark::MicroTxnSRSP:  1";

  // Add two hot keys to read/write set.
  uint64 hotkey_order1 = rand() % hot_records;
  while (hotkey_order1 % replica_size != replica) {
    hotkey_order1 = rand() % hot_records; 
  };

  uint64 hotkey_order2 = rand() % hot_records;
  while (hotkey_order2 % replica_size != replica || hotkey_order2 == hotkey_order1) {
    hotkey_order2 = rand() % hot_records; 
  };


//if (replica == 0)
//LOG(ERROR) << ": In Microbenchmark::MicroTxnSRSP:  2";

  uint64 hotkey1 = part + nparts * hotkey_order1;
  uint64 hotkey2 = part + nparts * hotkey_order2;
  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

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
    txn->add_read_write_set(IntToString(*it));
  }

//if (replica == 0)
//LOG(ERROR) << ": In Microbenchmark::MicroTxnSRSP:  3";

  return txn;
}

//------------- Create a single-replica multi-partition transaction------------
TxnProto* Microbenchmark::MicroTxnSRMP(int64 txn_id, uint32 part1, uint32 part2, uint32 replica) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SRMP);
  txn->add_involved_replicas(replica);

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey_order1 = rand() % hot_records;
  while (hotkey_order1 % replica_size != replica) {
    hotkey_order1 = rand() % hot_records; 
  };

  uint64 hotkey_order2 = rand() % hot_records;
  while (hotkey_order2 % replica_size != replica) {
    hotkey_order2 = rand() % hot_records; 
  };

  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

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
    txn->add_read_write_set(IntToString(*it));
  }

  GetRandomKeysReplica(&keys,
                       kRWSetSize/2 - 1,
                       key_start,
                       nparts * kDBSize,
                       part2,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->add_read_write_set(IntToString(*it));
  }

  return txn;
}

//------------- Create a multi-replica single-partition transaction------------
TxnProto* Microbenchmark::MicroTxnMRSP(int64 txn_id, uint32 part, uint32 replica1, uint32 replica2) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MRSP);
  txn->add_involved_replicas(replica1);
  txn->add_involved_replicas(replica2);

  if (replica1 != 0 && replica2 != 0) {
    txn->set_fake_txn(true);
  }

  // Add two hot keys to read/write set.
  uint64 hotkey_order1 = rand() % hot_records;
  while (hotkey_order1 % replica_size != replica1) {
    hotkey_order1 = rand() % hot_records; 
  };

  uint64 hotkey_order2 = rand() % hot_records;
  while (hotkey_order2 % replica_size != replica2) {
    hotkey_order2 = rand() % hot_records; 
  };

  uint64 hotkey1 = part + nparts * hotkey_order1;
  uint64 hotkey2 = part + nparts * hotkey_order2;

  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

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
    txn->add_read_write_set(IntToString(*it));
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
    txn->add_read_write_set(IntToString(*it));
  }

  return txn;
}

//------------- Create a multi-replica multi-partition transaction------------
TxnProto* Microbenchmark::MicroTxnMRMP(int64 txn_id, uint32 part1, uint32 part2, uint32 replica1, uint32 replica2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MRMP);
  txn->add_involved_replicas(replica1);
  txn->add_involved_replicas(replica2);

  if (replica1 != 0 && replica2 != 0) {
    txn->set_fake_txn(true);
  }

  // Add two hot keys to read/write set---one in each partition.
  uint64 hotkey_order1 = rand() % hot_records;
  while (hotkey_order1 % replica_size != replica1) {
    hotkey_order1 = rand() % hot_records; 
  };

  uint64 hotkey_order2 = rand() % hot_records;
  while (hotkey_order2 % replica_size != replica2) {
    hotkey_order2 = rand() % hot_records; 
  };

  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

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
    txn->add_read_write_set(IntToString(*it));
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
    txn->add_read_write_set(IntToString(*it));
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
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.

double execution_start = GetTime();

  for (uint32 i = 0; i < kRWSetSize; i++) {
    Record* val = storage->ReadObject(txn->read_write_set(i));
    // Not necessary since storage already has a pointer to val.
    //   storage->PutObject(txn->read_write_set(i), val);

    for (int j = 0; j < 8; j++) {
      if ((val->value)[j] + 1 > 'z') {
        (val->value)[j] = 'a';
      } else {
        (val->value)[j] = (val->value)[j] + 1;
      }
    }

  }

  // The following code is for microbenchmark "long" transaction, uncomment it if for "long" transaction
  while (GetTime() - execution_start < 0.00008) {
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
  }
}

