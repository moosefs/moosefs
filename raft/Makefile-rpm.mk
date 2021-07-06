NAME    := raft
SRC_EXT := gz

GIT_SHA1        := $(shell git rev-parse HEAD)
GIT_SHORT       := $(shell git rev-parse --short HEAD)
GIT_NUM_COMMITS := $(shell git rev-list HEAD --count)

GIT_INFO := $(GIT_NUM_COMMITS).g$(GIT_SHORT)

BUILD_DEFINES := --define "relval $(GIT_INFO)"
RPM_BUILD_OPTIONS := $(BUILD_DEFINES)

include packaging/Makefile_packaging.mk

$(NAME)-$(VERSION).tar.gz:
	git archive --format tar --prefix $(NAME)-$(VERSION)/ HEAD | gzip > $@
