// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// The Application abstract class
//
// Application execution logic in the system is coded into

#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include <string>

#include "common/types.h"

using std::string;

class ClusterConfig;
class Storage;
class StorageManager;
class TxnProto;

enum TxnStatus {
  SUCCESS = 0,
  FAILURE = 1,
  REDO = 2,
};

class Application {
 public:
  virtual ~Application() {}

  // Load generation.
  virtual TxnProto* NewTxn(int64 txn_id, int txn_type, string args,
                           ClusterConfig* config) const = 0;

  // Execute a transaction's application logic given the input 'txn'.
  virtual int Execute(TxnProto* txn, StorageManager* storage) const = 0;

  // Storage initialization method.
  virtual void InitializeStorage(Storage* storage,
                                 ClusterConfig* conf) const = 0;
};

#endif  // _DB_APPLICATIONS_APPLICATION_H_
