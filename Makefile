# Auto-dependency Makefile
# Copyright (C) 2015 Ahsen Uppal
# This program can be distributed under the terms of the GNU GPL.
# See the file LICENSE.


PROG:=$(notdir $(CURDIR))
CFLAGS+=-Wall -g -O0

# See:
# http://scottmcpeak.com/autodepend/autodepend.html

# maybe it's not such a good idea to mingle cxx and c flags
CXXFLAGS+=$(CFLAGS)
SRC:=$(wildcard *.c) $(wildcard *.cc)

# extra libs for link stage, also see $(LOADLIBES), $(LDLIBS)
LDFLAGS+=-lstdc++ -lpthread
LDLIBS+=
# include directives are pre-processor related, so they should be in CPPFLAGS
CPPFLAGS+=
CXXCPPFLAGS+=-std=c++11

BASE:=$(basename $(SRC))
OBJ:=$(addsuffix .o,$(BASE))
DEP:=$(addsuffix .d,$(BASE))

.PHONY: all
all: $(PROG)

# link stage
# it's also possible to use the auto-generated rule for $(PROG)
$(PROG): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# the problem with this approach is when different source files need
# different include flags
%.o: %.c %.d
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

%.o: %.cc %.d
	$(CXX) $(CPPFLAGS) $(CXXCPPFLAGS) $(CXXFLAGS) -c $< -o $@


# automatically create prerequistes, gcc will take care of headers
# which include other headers

%.d: %.c
	$(CC) $(CPPFLAGS) -M $< -o $@

# the dep rule for %.cc doesn't work if added to the %.c rule
# as in "%.d: %.c %.cc"
%.d: %.cc
	$(CXX) $(CPPFLAGS) $(CXXCPPFLAGS) -M $< -o $@

# adding a leading - supresses error message
-include $(DEP)

.PHONY: clean
clean:
	$(RM) $(PROG) $(OBJ) $(DEP)
