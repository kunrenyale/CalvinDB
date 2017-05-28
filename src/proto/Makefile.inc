# Create template specific variables
UC_DIR := PROTO
LC_DIR := proto

SRCS :=
EXES :=
TEST :=
PROTOS := proto/scalar.proto $(wildcard proto/*.proto)
DEPS :=

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)


