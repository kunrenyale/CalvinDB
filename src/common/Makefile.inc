OBJDIRS += common

# Create template specific variables
UPPERC_DIR := COMMON
LOWERC_DIR := common

COMMON_SRCS := common/utils.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

