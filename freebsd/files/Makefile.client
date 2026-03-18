PORTREVISION=	0
PKGNAMESUFFIX=	-${MFS_COMPONENT}

COMMENT=	MooseFS client tools

BROKEN_powerpc64=	fails to compile: mfs_fuse.c: internal compiler error: Segmentation fault

MFS_COMPONENT=	client

MASTERDIR=	${.CURDIR}/../moosefs-master

.include "${MASTERDIR}/Makefile"
