OBJDIRS += log

# Create template specific variables
UPPERC_DIR := LOG
LOWERC_DIR := log

LOG_SRCS := log/paxos.cc \
                log/local_mem_log.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
