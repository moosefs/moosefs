CONTRIB_DIR = .
TEST_DIR = ./tests
LLQUEUE_DIR = $(CONTRIB_DIR)/CLinkedListQueue
VPATH = src
BUILDDIR ?= .

GCOV_OUTPUT = $(BUILDDIR)/*.gcda $(BUILDDIR)/*.gcno $(BUILDDIR)/*.gcov
GCOV_CCFLAGS ?= -fprofile-arcs -ftest-coverage
SHELL  = /bin/bash
CFLAGS += -Iinclude -Werror -Werror=return-type -Werror=uninitialized -Wcast-align \
	  -Wno-pointer-sign -fno-omit-frame-pointer -fno-common -fsigned-char \
	  -Wunused-variable -Wshadow \
	  $(GCOV_CCFLAGS) -I$(LLQUEUE_DIR) -Iinclude -g -O2 -fPIC

UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
SHAREDFLAGS = -dynamiclib
SHAREDEXT = dylib
# We need to include the El Capitan specific /usr/includes, aargh
CFLAGS += -I/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.11.sdk/usr/include/
CFLAGS += -I/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.12.sdk/usr/include
CFLAGS += -fsanitize=address
else
SHAREDFLAGS = -shared
SHAREDEXT = so
endif

OBJECTS = $(BUILDDIR)/raft_server.o $(BUILDDIR)/raft_server_properties.o $(BUILDDIR)/raft_node.o $(BUILDDIR)/raft_log.o

all: static shared

$(BUILDDIR):
	mkdir -p $@

$(BUILDDIR)/%.o: %.c $(wildcard include/*.h) | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

clinkedlistqueue:
	mkdir -p $(LLQUEUE_DIR)/.git
	git --git-dir=$(LLQUEUE_DIR)/.git init 
	pushd $(LLQUEUE_DIR); git pull http://github.com/willemt/CLinkedListQueue master; popd

download-contrib: clinkedlistqueue

$(TEST_DIR)/main_test.c: $(TEST_DIR)/test_*.c
	if test -d $(LLQUEUE_DIR); \
	then echo have contribs; \
	else make download-contrib; \
	fi
	cd $(TEST_DIR) && sh make-tests.sh "test_*.c" > main_test.c && cd ..

.PHONY: shared
shared: $(OBJECTS)
	$(CC) $(OBJECTS) $(LDFLAGS) $(CFLAGS) -fPIC $(SHAREDFLAGS) -o $(BUILDDIR)/libraft.$(SHAREDEXT)

.PHONY: static
static: $(OBJECTS)
	ar -r $(BUILDDIR)/libraft.a $(OBJECTS)


tests_main: src/raft_server.c src/raft_server_properties.c src/raft_log.c src/raft_node.c $(TEST_DIR)/main_test.c $(TEST_DIR)/test_*.c $(TEST_DIR)/mock_send_functions.c $(TEST_DIR)/CuTest.c $(LLQUEUE_DIR)/linked_list_queue.c
	$(CC) $(CFLAGS) -o $(BUILDDIR)/$@ $^

.PHONY: tests
tests: tests_main
	./tests_main
	if [ -n "$(GCOV_CCFLAGS)" ]; then \
	    gcov raft_server.c;           \
	fi

.PHONY: fuzzer_tests
fuzzer_tests:
	python tests/log_fuzzer.py

.PHONY: amalgamation
amalgamation:
	./scripts/amalgamate.sh > raft.h

.PHONY: infer
infer: do_infer

.PHONY: do_infer
do_infer:
	make clean
	infer -- make static

clean:
	@rm -f $(TEST_DIR)/main_test.c $(BUILDDIR)/*.o $(GCOV_OUTPUT); \
	if [ -f "$(BUILDDIR)/libraft.$(SHAREDEXT)" ]; then rm $(BUILDDIR)/libraft.$(SHAREDEXT); fi;\
	if [ -f $(BUILDDIR)/libraft.a ]; then rm $(BUILDDIR)/libraft.a; fi;\
	if [ -f $(BUILDDIR)/tests_main ]; then rm $(BUILDDIR)/tests_main; fi;
