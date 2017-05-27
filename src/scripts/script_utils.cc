// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include "scripts/script_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>

string HostName() {
  char hname[128];
  gethostname(hname, sizeof(hname));
  struct hostent* hent = gethostbyname(hname);
  return hent->h_name;
}

// Get the basename of the process name
char *basename(char *path) {
  char *s;
  char *p;
  p = s = path;
  while (*s) {
    if (*s++ == '/') {
      p = s;
    }
  }
  return (char *) p;
}

// find all pid of process by name, only compare base name of pid_name
// list_size: the size of pid_list
// RETURN:
// < 0: error number
// >=0: how many pid found
int get_pid_by_name(char* process_name, int list_size) {
  #define  MAX_BUF_SIZE   256
  DIR *dir;
  struct dirent *next;
  int count = 0;
  pid_t pid;
  FILE *fp;
  char *base_pname = NULL;
  char *base_fname = NULL;
  char cmdline[MAX_BUF_SIZE];
  char path[MAX_BUF_SIZE];

  if (process_name == NULL) {
    return -EINVAL;
  }

  base_pname = basename(process_name);
  if (strlen(base_pname) <= 0) {
    return -EINVAL;
  }

  dir = opendir("/proc");
  if (!dir) {
    return -EIO;
  }

  while ((next = readdir(dir)) != NULL) {
    // Skip non-number
    if (!isdigit(*next->d_name)) {
      continue;
    }

    pid = strtol(next->d_name, NULL, 0);
    snprintf(path, sizeof(path), "/proc/%u/cmdline", pid);
    fp = fopen(path, "r");
    if (fp == NULL) {
      continue;
    }

    memset(cmdline, 0, sizeof(cmdline));
    size_t result = fread(cmdline, MAX_BUF_SIZE - 1, 1, fp);
    if (result < 0) {
      fclose(fp);
      continue;
    }
    fclose(fp);
    base_fname = basename(cmdline);

    if (strcmp(base_fname, base_pname) == 0) {
      if (count >= list_size) {
        break;
      } else {
        count++;
      }
    }
  }
  closedir(dir);
  return count;
}

// If process is existed, return true
bool is_process_exist(char* process_name) {
  int count = get_pid_by_name(process_name, 2);
  if (count > 1) {
    return true;
  } else {
    return false;
  }
}

