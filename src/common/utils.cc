// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Thaddeus Diamond <diamond@cs.yale.edu>

#include "common/utils.h"

#include <cstdio>

template<typename T> string TypeName() {
  return "?";
}
ADD_TYPE_NAME(int32);
ADD_TYPE_NAME(string);

vector<string> SplitString(const string& input, char delimiter) {
  string current;
  vector<string> result;
  for (uint32 i = 0; i < input.size(); i++) {
    if (input[i] == delimiter) {
      // reached delimiter
      result.push_back(current);
      current.clear();
    } else {
      current.push_back(input[i]);
    }
  }
  result.push_back(current);
  return result;
}

double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

uint32 FNVHash(const string& key) {
  uint32 hash = 2166136261;                       // FNV Hash offset
  for (uint32 i = 0; i < key.size(); i++) {
    hash = (hash * 1099511628211) ^ key[i];       // H(x) = H(x-1) * FNV' XOR x
  }
  return hash;
}

uint32 FNVModHash(const string& key) {
  uint32 hash = 2166136261;                       // FNV Hash offset
  for (uint32 i = 0; i < key.size(); i++) {
    hash = (hash ^ key[i]) * 1099511628211;       // H(x) = H(x-1) * FNV' XOR x
  }
  return hash;
}

void Spin(double duration) {
  usleep(1000000 * duration);
}

void SpinUntil(double time) {
  if (time > GetTime()) {
    usleep(1000000 * (time - GetTime()));
  }
}

string RandomString(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += rand() % 26 + 'A';
  }
  return s;
}

string RandomBytes(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += rand() % 256;
  }
  return s;
}

string RandomBytesNoZeros(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += 1 + rand() % 255;
  }
  return s;
}

#define RANF() ((double)rand()/(1.0+(double)RAND_MAX))
double RandomGaussian(double s) {
  double x1, x2, w, z;
  do {
      x1 = 2.0 * RANF() - 1.0;
      x2 = 2.0 * RANF() - 1.0;
      w = x1 * x1 + x2 * x2;
  } while ( w >= 1.0 );
  return s * x1 * sqrt( (-2.0 * log( w ) ) / w );
  z = 0.5 + s * x1 * sqrt( (-2.0 * log( w ) ) / w );
  if (z < 0 || z >= 1) return RANF();
  else return z;
}

string Int32ToString(int32 n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

string Int64ToString(int64 n) {
  char s[64];
  snprintf(s, sizeof(s), "%ld", n);
  return string(s);
}

string UInt32ToString(uint32 n) {
  char s[64];
  snprintf(s, sizeof(s), "%u", n);
  return string(s);
}

string UInt64ToString(uint64 n) {
  char s[64];
  snprintf(s, sizeof(s), "%lu", n);
  return string(s);
}

// Returns a human-readable string representation of an int.
string IntToString(int n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

// Converts a human-readable numeric string to an int.
int StringToInt(const string& s) {
  return atoi(s.c_str());
}

string DoubleToString(double n) {
  char s[64];
  snprintf(s, sizeof(s), "%f", n);
  return string(s);
}


template<typename T>
void SpinUntilNE(T& t, const T& v) {
  while (t == v) {
    usleep(10);
  }
}

class MessageBuffer;
template<>
void SpinUntilNE<MessageBuffer*>(
    MessageBuffer*& t,
    MessageBuffer* const& v) {
  while (t == v) {
    usleep(10);
  }
}



