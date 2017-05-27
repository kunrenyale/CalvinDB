OBJDIRS += applications

# Create template specific variables
UPPERC_DIR := APPLICATIONS
LOWERC_DIR := applications

APPLICATIONS_SRCS := applications/microbenchmark.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

