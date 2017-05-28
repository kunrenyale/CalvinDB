# Create template specific variables
UC_DIR := SCHEDULER
LC_DIR := scheduler

SRCS := scheduler/deterministic_lock_manager.cc \
                  scheduler/deterministic_scheduler.cc

EXES := 
DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) $(APPLICATION_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

