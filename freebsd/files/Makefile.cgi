# Created by: Jakub Kruszona-Zawadzki <acid@moosefs.com> ; Based on port created by: Chifeng Qu <chifeng@gmail.com>
# $FreeBSD$

PKGNAMESUFFIX=	-cgi

COMMENT=	Cgi moosefs interface

LICENSE=	GPLv2

MFS_COMPONENT=	cgi

MASTERDIR=	${.CURDIR}/../moosefs-master

.include "${MASTERDIR}/Makefile"
