#!/usr/bin/env bash

./configure --prefix=/usr --mandir=/share/man --sysconfdir=/etc --localstatedir=/var/lib --with-default-user=mfs --with-default-group=mfs
if ! make; then
	autoreconf -f -i
	./configure --prefix=/usr --mandir=/share/man --sysconfdir=/etc --localstatedir=/var/lib --with-default-user=mfs --with-default-group=mfs
	make
fi
