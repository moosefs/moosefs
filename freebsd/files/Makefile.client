# Created by: Jakub Kruszona-Zawadzki <acid@moosefs.com> ; Based on port created by: Chifeng Qu <chifeng@gmail.com>
# $FreeBSD$

PKGNAMESUFFIX=	-client

COMMENT=	MooseFS client tools

LICENSE=	GPLv2

MFS_COMPONENT=	client

BROKEN_powerpc64=	fails to compile: mfs_fuse.c: internal compiler error: Segmentation fault

MASTERDIR=	${.CURDIR}/../moosefs3-master

.include "${MASTERDIR}/Makefile"
