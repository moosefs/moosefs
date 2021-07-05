#!/usr/bin/env bash

./configure --localstatedir=/var --with-mfscgiserv-dir=/usr/local/sbin --with-mfscgi-dir=/usr/local/share/mfscgi --with-default-user=mfs --with-default-group=mfs
make
