OBJDIRS += backend

# Create template specific variables
UPPERC_DIR := BACKEND
LOWERC_DIR := backend

BACKEND_SRCS := backend/simple_storage.cc \
                backend/storage_manager.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
