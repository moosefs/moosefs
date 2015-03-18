#!/usr/bin/env bash

set -e

if [ "$0X" == "./makepkg.shX" ]; then
	cd ..
elif [ "$0X" != "./macosx/makepkg.shX" ]; then
	echo "Run this script as ./makepkg.sh or ./macosx/makepkg.sh"
	exit 1
fi

if [ "$1X" != "X" ]; then
	EXTRA="-$1"
else
	EXTRA=""
fi

VERSION=`cat configure.ac | awk '/^AC_INIT/ {split($0,v,"[][]");} /^release=/ {split($0,r,"="); printf "%s-%u",v[4],r[2];}'`
PKGVERSION=`echo $VERSION | tr '-' '.'`

./configure -C --prefix=/usr --sysconfdir=/private/etc
make
make install DESTDIR=/tmp/moosefs/
pkgbuild --root /tmp/moosefs/ --identifier com.moosefs --version $PKGVERSION --ownership recommended ../moosefs${EXTRA}-${VERSION}.pkg
./configure -C
