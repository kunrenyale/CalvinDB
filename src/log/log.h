// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#ifndef CALVIN_LOG_LOG_H_
#define CALVIN_LOG_LOG_H_

#include "common/types.h"

// Interface for a components Log.
class Log {
 public:
  virtual ~Log() {}
  // Request to append to the log. Currently only synchronous appends are
  // supported. Implementations of the Log interface are NOT required to allow
  // multiple concurrent Appends (i.e. multiple Appends generally require
  // external synchronization), but concurrent calls to GetReader and
  // methods of Log::Reader must be allowed from different threads without
  // external synchronization with one another or with Appenders.
  //
  // Requires: version > 0
  // Requires: version > any previously appended version
  virtual void Append(uint64 version, const string& entry) = 0;

  // Returns the highest version that has been appended to the log so far, or
  // 0 if the log is empty.
  virtual uint64 LastVersion() = 0;

  // The Log<T>::Reader functions like an iterator over a Log's entries. Newly
  // created LogReaders are positioned logically BEFORE the first log entry.
  //
  // Log<T>::Readers need not be thread safe. Multiple threads may NOT access
  // a single Reader concurrently without external synchronization.
  // HOWEVER, a Reader must continue to work even as the underlying Log<T> is
  // modified. In addition, as entries are appended to the underlying Log<T>,
  // all existing Readers of that log must thereafter reflect the new state of
  // the underlying Log.
  class Reader {
   public:
    virtual ~Reader() {}

    // Returns true iff the LogReader currently exposes a valid log entry.
    virtual bool Valid() = 0;

    // Resets the LogReader to be positioned before the first log entry.
    virtual void Reset() = 0;

    // Attempts to advance the LogReader to the next entry. Returns true on
    // success. If no entry exists following the current one, or if the reader
    // is unable to advance for some other reason, the LogReader stays
    // positioned where it is, and false is returned.
    virtual bool Next() = 0;

    // Attempts to position the LogReader at the first entry with whose version
    // is greater than or equal to 'target'. Returns true on success.
    // If no such exists exists, the LogReader stays positioned where it is,
    // and false is returned.
    virtual bool Seek(uint64 target) = 0;

    // Return the version of the current entry.
    //
    // Requires: Valid()
    virtual uint64 Version() = 0;

    // Returns the current entry.
    //
    // Requires: Valid()
    virtual string Entry() = 0;
  };

  // Returns a reader for reading the Log. This is required to be thread-safe:
  // multiple threads must be able to call 'GetReader' concurrently without
  // external synchronization.
  virtual Reader* GetReader() = 0;
};

#endif  // CALVIN_LOG_LOG_H_

