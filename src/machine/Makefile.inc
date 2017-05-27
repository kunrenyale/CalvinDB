OBJDIRS += machine

# Create template specific variables
UPPERC_DIR := MACHINE
LOWERC_DIR := machine

MACHINE_SRCS := machine/cluster_config.cc \
                machine/cluster_manager.cc \
                machine/connection.cc \
                machine/sequencer.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(LOG_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
