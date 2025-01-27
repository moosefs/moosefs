#!/usr/bin/env bash

# to make ports simply:
#  - copy distfile "moosefs-*.tar.gz" to /usr/ports/distfiles
#  - untar distfile
#  - run this script from untared distfile

if [ `uname -r | cut -d '.' -f1` -ge 14 ]; then
	PORTBASESUBDIR="filesystems"
else
	PORTBASESUBDIR="sysutils"
fi

if [ "x$1" == "x" ]; then
	PORTBASE="/usr/ports/$PORTBASESUBDIR"
else
	PORTBASE="$1"
fi
if [ "x$2" == "x" ]; then
	DISTFILEBASE="/usr/ports/distfiles"
else
	DISTFILEBASE="$2"
fi
if [ ! -d "$PORTBASE" ]; then
	echo "specified port base is not a directory"
	exit 1
fi
if [ ! \( -f "$DISTFILEBASE" -o -d "$DISTFILEBASE" \) ]; then
	echo "specified dist file nor dist directory doesn't exist"
	exit 1
fi
FILEBASEDIR=`dirname "$0"`

PORTNAMES="master chunkserver client metalogger cgi cgiserv cli netdump"

PORTFILES="Makefile pkg-descr pkg-plist files"

VERSION=4.57.1
RELEASE=1

cat "${FILEBASEDIR}/files/Makefile.master" | sed "s/^DISTVERSION=.*$/DISTVERSION=		${VERSION}/" | sed "s/^DISTVERSIONSUFFIX=.*$/DISTVERSIONSUFFIX=	${RELEASE}/" | uniq > .tmp
mv .tmp "${FILEBASEDIR}/files/Makefile.master"
if [ `uname -r | cut -d '.' -f1` -ge 14 ]; then
	cat "${FILEBASEDIR}/files/Makefile.master" | sed "s/sysutils/filesystems/g" | sed "s/^CATEGORIES=.*$/CATEGORIES=		filesystems sysutils/" > .tmp
	mv .tmp "${FILEBASEDIR}/files/Makefile.master"
else
	cat "${FILEBASEDIR}/files/Makefile.master" | sed "s/^CATEGORIES=.*$/CATEGORIES=		sysutils/" | sed "s/filesystems/sysutils/g" > .tmp
	mv .tmp "${FILEBASEDIR}/files/Makefile.master"
fi

for portname in ${PORTNAMES}; do
	portdir="${PORTBASE}/moosefs-${portname}"
	if [ -d "$portdir" ]; then
		rm -rf "$portdir"
	fi
	mkdir -p "${portdir}"
	for portfile in ${PORTFILES}; do
		if [ -f "${FILEBASEDIR}/files/${portfile}.${portname}" ]; then
			cp "${FILEBASEDIR}/files/${portfile}.${portname}" "${portdir}/${portfile}"
		fi
		if [ -d "${FILEBASEDIR}/files/${portfile}.${portname}" ]; then
			cp -R "${FILEBASEDIR}/files/${portfile}.${portname}" "${portdir}/${portfile}"
		fi
		if [ `uname -r | cut -d '.' -f1` -ge 14 -a "x${portfile}" == "xpkg-plist" ]; then
			cat "${portdir}/${portfile}" | sed 's/^man\(.*\)$/share\/man\1/' > .tmp
			mv .tmp "${portdir}/${portfile}"
		fi
	done
	if [ "x${portname}" == "xmaster" ]; then
		make -C ${portdir} makesum
	fi
done
