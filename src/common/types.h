// Author: Kun Ren <renkun.nwpu@gmail.com>
//
// Just some shorthand typedefs for commonly used types.

#ifndef CALVIN_COMMON_TYPES_H_
#define CALVIN_COMMON_TYPES_H_

#include <stdint.h>
#include <iostream>  // NOLINT
#include <string>

// Slice and string are common enough that they're worth including here.
using std::string;

// Abbreviated signed int types.
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

// Abbreviated unsigned int types.
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

// 'bytes' is an arbitrary sequence of bytes, represented as a string.
typedef string bytes;

// Key type for database objects.
// Note: if this changes from bytes, the types need to be updated for the
// following fields in .proto files:
//    proto/txn.proto:
//      TxnProto::'read_set'
//      TxnProto::'write_set'
typedef bytes Key;

// Value type for database objects.
typedef bytes Value;

#endif  // CALVIN_COMMON_TYPES_H_

