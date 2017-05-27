OBJDIRS += scheduler

# Create template specific variables
UPPERC_DIR := SCHEDULER
LOWERC_DIR := scheduler

SCHEDULER_PROG :=
SCHEDULER_SRCS := scheduler/deterministic_lock_manager.cc \
                  scheduler/deterministic_scheduler.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) $(APPLICATION_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

