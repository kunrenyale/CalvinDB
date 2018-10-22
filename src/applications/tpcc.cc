// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "applications/tpcc.h"

#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "proto/txn.pb.h"


// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Tpcc::GetRandomKeys(set<uint64>* keys, uint32 num_keys, uint64 key_start,
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
void Tpcc::GetRandomKeysReplica(set<uint64>* keys, uint32 num_keys, uint64 key_start,
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
TxnProto* Tpcc::TpccTxnSP(int64 txn_id, uint64 part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_SP);

  // Add warehouse to read set.
  uint64 hotkey1 = part + nparts * (rand() % warehouses_per_node);

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeys(&keys,
                1,
                warehouse_end,
                district_end,
                part);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  // Add comstomer to the read set
  keys.clear();
  GetRandomKeys(&keys,
                1,
                district_end,
                customer_end,
                part);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  keys.clear();
  GetRandomKeys(&keys,
                10,
                customer_end,
                item_end,
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
TxnProto* Tpcc::TpccTxnMP(int64 txn_id, uint64 part1, uint64 part2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_MP);

  // Add warehouse to read set.
  uint64 hotkey1 = part1 + nparts * (rand() % warehouses_per_node);
  uint64 hotkey2 = part2 + nparts * (rand() % warehouses_per_node);

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(0);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeys(&keys,
                1,
                warehouse_end,
                district_end,
                part1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  // Add comstomer to the read set
  keys.clear();
  GetRandomKeys(&keys,
                1,
                district_end,
                customer_end,
                part1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(0);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  keys.clear();
  GetRandomKeys(&keys,
                10,
                customer_end,
                item_end,
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
TxnProto* Tpcc::TpccTxnSRSP(int64 txn_id, uint64 part, uint32 replica) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_SP);

  // Add warehouse to read set.
  uint64 hotkey_order1 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica;

  uint64 hotkey1 = part + nparts * hotkey_order1;

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                1,
                warehouse_end,
                district_end,
                part,
                replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  // Add customer to the read set
  uint64 key_start = district_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  keys.clear();
  GetRandomKeysReplica(&keys,
                       1,
                       key_start,
                       customer_end,
                       part,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  key_start = customer_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }
  keys.clear();

  GetRandomKeysReplica(&keys,
                10,
                key_start,
                item_end,
                part,
                replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  return txn;
}

//------------- Create a single-replica multi-partition transaction------------
TxnProto* Tpcc::TpccTxnSRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_MP);

  // Add two warehouse to read set---one in each partition.
  uint64 hotkey_order1 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica;
  uint64 hotkey_order2 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica;


  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                1,
                warehouse_end,
                district_end,
                part1,
                replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  // Add customer to the read set
  uint64 key_start = district_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  keys.clear();
  GetRandomKeysReplica(&keys,
                       1,
                       key_start,
                       customer_end,
                       part1,
                       replica);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  key_start = customer_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }
  keys.clear();

  GetRandomKeysReplica(&keys,
                10,
                key_start,
                item_end,
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
TxnProto* Tpcc::TpccTxnMRSP(int64 txn_id, uint64 part, uint32 replica1, uint32 replica2) {
  CHECK(replica1 != replica2);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_SP);

  if (replica1 > replica2) {
    uint32 tmp = replica1;
    replica1 = replica2;
    replica2 = tmp;
  } 

  if (replica1 != 0 && replica2 != 0) {
    txn->set_fake_txn(true);
  }

  // Add two hot keys to read/write set.
  uint64 hotkey_order1 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica1;
  uint64 hotkey_order2 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica2;

  uint64 hotkey1 = part + nparts * hotkey_order1;
  uint64 hotkey2 = part + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica1);
  key_entry->set_counter(0);

  key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica2);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                1,
                warehouse_end,
                district_end,
                part,
                replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Add customer to the read set
  uint64 key_start = district_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  keys.clear();
  GetRandomKeysReplica(&keys,
                       1,
                       key_start,
                       customer_end,
                       part,
                       replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  key_start = customer_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }
  keys.clear();

  GetRandomKeysReplica(&keys,
                10,
                key_start,
                item_end,
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
TxnProto* Tpcc::TpccTxnMRMP(int64 txn_id, uint64 part1, uint64 part2, uint32 replica1, uint32 replica2) {
  CHECK(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(TPCCTXN_MP);

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
  uint64 hotkey_order1 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica1;
  uint64 hotkey_order2 = (rand() % (warehouses_per_node/replica_size)) * replica_size + replica2;

  uint64 hotkey1 = part1 + nparts * hotkey_order1;
  uint64 hotkey2 = part2 + nparts * hotkey_order2;

  KeyEntry* key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey1));
  key_entry->set_master(replica1);
  key_entry->set_counter(0);

  key_entry = txn->add_read_set();
  key_entry->set_key(IntToString(hotkey2));
  key_entry->set_master(replica2);
  key_entry->set_counter(0);

  // Add district to the read-write set
  set<uint64> keys;
  GetRandomKeysReplica(&keys,
                1,
                warehouse_end,
                district_end,
                part1,
                replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_write_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Add customer to the read set
  uint64 key_start = district_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }

  keys.clear();
  GetRandomKeysReplica(&keys,
                       1,
                       key_start,
                       customer_end,
                       part1,
                       replica1);
  for (set<uint64>::iterator it = keys.begin(); it != keys.end(); ++it) {
    key_entry = txn->add_read_set();
    key_entry->set_key(IntToString(*it));
    key_entry->set_master(replica1);
    key_entry->set_counter(0);
  }

  // Add item stock to the read_write set
  key_start = customer_end;
  if (key_start % (replica_size*nparts) != 0) {
    key_start = key_start + (replica_size*nparts - (key_start % (replica_size*nparts)));
  }
  keys.clear();

  GetRandomKeysReplica(&keys,
                10,
                key_start,
                item_end,
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
TxnProto* Tpcc::NewTxn(int64 txn_id, int txn_type,
                                 string args, ClusterConfig* config) const {
  return NULL;
}

int Tpcc::Execute(TxnProto* txn, StorageManager* storage) const {

  // Normal txns, read the record and do some computations
  double factor = 1.0;
  uint32 txn_type = txn->txn_type();
  if (txn_type == TPCCTXN_MP || (txn_type == TPCCTXN_SP && txn->involved_replicas_size() > 1)) {
    factor = 2.0;
  }
  double execution_start = GetTime();

  for (uint32 i = 0; i < (uint32)(txn->read_write_set_size()); i++) {
    KeyEntry key_entry = txn->read_write_set(i);
    Record* val = storage->ReadObject(key_entry.key());
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

  for (uint32 i = 0; i < (uint32)(txn->read_set_size()); i++) {
	  KeyEntry key_entry = txn->read_set(i);
	  Record* val = storage->ReadObject(key_entry.key());
	  // Not necessary since storage already has a pointer to val.
	  //   storage->PutObject(txn->read_write_set(i), val);
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

void Tpcc::InitializeStorage(Storage* storage, ClusterConfig* conf) const {
  char* int_buffer = (char *)malloc(sizeof(char)*kRecordSize);
  for (uint32 j = 0; j < kRecordSize - 1; j++) {
    int_buffer[j] = (rand() % 26 + 'a');
  }
  int_buffer[kRecordSize - 1] = '\0';

  for (uint64 i = 0; i < kDBSize; i++) {
    if (conf->LookupPartition(IntToString(i)) == conf->relative_node_id()) {
      string value(int_buffer);
      uint32 master = conf->LookupMaster(IntToString(i));
      storage->PutObject(IntToString(i), new Record(value, master));
    }
  }
}

