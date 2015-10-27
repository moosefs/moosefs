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

if [ `uname -r | cut -d'.' -f1` -gt 14 ]; then 
	PREFIX="/usr/local"
else
	PREFIX="/usr"
fi

VERSION=`cat configure.ac | awk '/^AC_INIT/ {split($0,v,"[][]");} /^release=/ {split($0,r,"="); printf "%s-%u",v[4],r[2];}'`
PKGVERSION=`echo $VERSION | tr '-' '.'`

if [ -d "/tmp/moosefs" ]; then
	rm -rf "/tmp/moosefs"
fi

./configure -C --prefix=$PREFIX --sysconfdir=/private/etc
make
make install DESTDIR=/tmp/moosefs/
pkgbuild --root /tmp/moosefs/ --identifier com.moosefs --version $PKGVERSION --ownership recommended ../moosefs${EXTRA}-${VERSION}.pkg
./configure -C
