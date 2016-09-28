# Created by: Jakub Kruszona-Zawadzki <acid@moosefs.com> ; Based on port created by: Chifeng Qu <chifeng@gmail.com>
# $FreeBSD$

PKGNAMESUFFIX=	-cgi

COMMENT=	MooseFS CGI interface

LICENSE=	GPLv2

MFS_COMPONENT=	cgi

MASTERDIR=	${.CURDIR}/../moosefs2-master

.include "${MASTERDIR}/Makefile"
