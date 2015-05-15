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

VERSION=2.0.68
RELEASE=1

if [ -f "$DISTFILEBASE" ]; then
	SHA256=`sha256 "${DISTFILEBASE}" | cut -d' ' -f4`
	SIZE=`stat -f %z "${DISTFILEBASE}"`
else
	SHA256=`sha256 "${DISTFILEBASE}/moosefs-${VERSION}-${RELEASE}.tar.gz" | cut -d' ' -f4`
	SIZE=`stat -f %z "${DISTFILEBASE}/moosefs-${VERSION}-${RELEASE}.tar.gz"`
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
	done
	cat "${FILEBASEDIR}/files/Makefile.common" | sed "s/^PORTVERSION=.*$/PORTVERSION=	${VERSION}/" > "${portdir}/Makefile.common"
	(
		echo "SHA256 (moosefs-${VERSION}-${RELEASE}.tar.gz) = $SHA256"
		echo "SIZE (moosefs-${VERSION}-${RELEASE}.tar.gz) = $SIZE"
	) > "${portdir}/distinfo"
done
