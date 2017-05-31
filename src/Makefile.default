#
# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf
#

# We create a listing of the root directories for access into
OBJDIR := ../obj
BINDIR := ../bin
EXTDIR := ../ext
SRCDIR := ../src
LOGDIR := ../logs

# Next, we define the external libraries we use and their directories
PROTOB  := $(EXTDIR)/protobuf
GLOG    := $(EXTDIR)/glog
GFLAGS  := $(EXTDIR)/gflags
ZEROMQ  := $(EXTDIR)/zeromq

# Executable for protocol buffers
PROTOCEXE := $(PROTOB)/src/.libs/protoc

# '$(V)' controls whether the lab makefiles print verbose commands (the
# actual shell commands run by Make), as well as the "overview" commands
# (such as '+ cc lib/readline.c').
#
# For overview commands only, the line should read 'V = @'.
# For overview and verbose commands, the line should read 'V ='.
V = @

# Set the compiler and compile-time loaded libraries
CXX           := g++
LDLIBRARYPATH := LD_LIBRARY_PATH=$(ZEROMQ)/src/.libs:$(PROTOB)/src/.libs:$(GLOG)/.libs:$(GFLAGS)/.libs

#-----------------------------------------------
# OPT MODES
PROD := -O2 -DNDEBUG            # (A) Production use (optimized mode)
DEBUG := -g2                    # (B) Debug mode, w/ line-level debug symbols
PROFILING := -O2 -g2 -DNDEBUG   # (C) opt, but w/debug symbols

OPT_MODE := $(PROD)

# Set the flags for C++ to compile with (namely where to look for external
# libraries) and the linker libraries (again to look in the ext/ library)
CXXFLAGS := -g -MD -I$(SRCDIR) -I$(OBJDIR) \
            -I$(ZEROMQ)/include \
            -I$(PROTOB)/src \
            -I$(GLOG)/include \
            -I$(GFLAGS)/include \
            -Wall -std=c++0x

LDFLAGS := -lglog -L$(GLOG)/.libs \
           -lgflags -L$(GFLAGS)/.libs \
           -lprotobuf -L$(PROTOB)/src/.libs \
           -lrt -lreadline -lpthread

ZMQLDFLAGS := -lzmq -L$(ZEROMQ)/src/.libs

# Lists that the */Makefile.inc makefile fragments will add to
OBJDIRS :=
TESTS :=

# Make sure that 'all' is the first target
all:
	@mkdir -p $(LOGDIR)

# Makefile template so that the makefrags are far less redundant
MAKEFILE_TEMPLATE := Makefile.template

# python obj file used by some components
PYTHON_OBJS := /usr/lib64/libpython2.7.so

# Makefile fragments for specific submodules
include proto/Makefile.inc
include common/Makefile.inc
include machine/Makefile.inc
include log/Makefile.inc
include backend/Makefile.inc
include applications/Makefile.inc
include scheduler/Makefile.inc
include scripts/Makefile.inc

test: $(TESTS)
	@mkdir -p $(LOGDIR)
clean:
	rm -rf $(OBJDIR) $(BINDIR) $(LOGDIR)

# This magic automatically generates makefile dependencies
# for header files included from C source files we compile,
# and keeps those dependencies up-to-date every time we recompile.
# See 'mergedep.pl' for more information.
$(OBJDIR)/.deps: $(foreach dir, $(OBJDIRS), $(wildcard $(OBJDIR)/$(dir)/*.d))
	@mkdir -p $(@D)
	@cat $^ /dev/null > $@
#	@$(PERL) mergedep.pl $@ $^
-include $(OBJDIR)/.deps

always:

# Eliminate default suffix rules
.SUFFIXES:

# make it so that no intermediate .o files are ever deleted
.PRECIOUS: $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.o) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.cc) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.h)

.PHONY: all always clean test
