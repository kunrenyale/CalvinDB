# Create template specific variables
UC_DIR := BACKEND
LC_DIR := backend

SRCS := backend/simple_storage.cc \
                backend/storage_manager.cc

EXES := 
DEPS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
