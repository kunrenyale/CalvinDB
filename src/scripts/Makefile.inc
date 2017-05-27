OBJDIRS += scripts

# Create template specific variables
UPPERC_DIR := SCRIPTS
LOWERC_DIR := scripts

SCRIPTS_PROG :=
SCRIPTS_SRCS := scripts/cluster.cc \
                scripts/calvindb_server.cc \

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) $(APPLICATION_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

