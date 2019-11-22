#!/usr/bin/env bash

# to make ports simply:
#  - copy distfile "moosefs-*.tar.gz" to /usr/ports/distfiles
#  - untar distfile
#  - run this script from untared distfile

if [ "x$1" == "x" ]; then
	PORTBASE="/usr/ports/sysutils"
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

VERSION=3.0.109
RELEASE=1

cat "${FILEBASEDIR}/files/Makefile.master" | sed "s/^PORTVERSION=.*$/PORTVERSION=		${VERSION}/" | sed "s/^DISTNAME=.*$/DISTNAME=		moosefs-\${PORTVERSION}-${RELEASE}/" | uniq > .tmp
mv .tmp "${FILEBASEDIR}/files/Makefile.master"

for portname in ${PORTNAMES}; do
	portdir="${PORTBASE}/moosefs3-${portname}"
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
	done
	if [ "x${portname}" == "xmaster" ]; then
		make -C ${portdir} makesum
	fi
done
