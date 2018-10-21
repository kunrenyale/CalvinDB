# Create template specific variables
UC_DIR := APPLICATIONS
LC_DIR := applications

SRCS := applications/microbenchmark.cc \
        applications/tpcc.cc

EXES :=

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
