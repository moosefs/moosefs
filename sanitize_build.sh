#!/usr/bin/env bash

# linux gcc with thread sanitizer
CC=gcc CFLAGS='-O1 -g -fsanitize=thread -fno-omit-frame-pointer' LDFLAGS='-O1 -g -fsanitize=thread -fno-omit-frame-pointer' LIBS='-ltsan' ./configure --prefix=/usr --mandir=/share/man --sysconfdir=/etc --localstatedir=/var/lib --with-default-user=mfs --with-default-group=mfs --enable-externalcflags
make V=1

# linux gcc with address sanitizer
CC=gcc CFLAGS='-O1 -g -fsanitize=address -fno-omit-frame-pointer' LDFLAGS='-O1 -g -fsanitize=address -fno-omit-frame-pointer' ./configure --prefix=/usr --mandir=/share/man --sysconfdir=/etc --localstatedir=/var/lib --with-default-user=mfs --with-default-group=mfs --enable-externalcflags
make V=1

