# Create template specific variables
UC_DIR := MACHINE
LC_DIR := machine

SRCS := machine/cluster_config.cc \
        machine/cluster_manager.cc \
        machine/connection.cc \
        machine/sequencer.cc \
        machine/lowlatency_sequencer.cc 

EXES := 

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(LOG_OBJS) $(BACKEND_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
