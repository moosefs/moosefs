# Created by: Jakub Kruszona-Zawadzki <acid@moosefs.com> ; Based on port created by: Chifeng Qu <chifeng@gmail.com>
# $FreeBSD$

PORTNAME=	moosefs
#PORTVERSION=	# set in 'include'
#PORTREVISION=	# set in 'include'
CATEGORIES=	sysutils
PKGNAMESUFFIX=	-cgi

COMMENT=	Cgi moosefs interface

.include "Makefile.common"

USES=		python:2.5+

CONFIGURE_ARGS+=	--disable-mfsmaster \
			--disable-mfsmetalogger \
			--disable-mfscgiserv \
			--enable-mfscgi \
			--disable-mfscli \
			--disable-mfsnetdump \
			--disable-mfssupervisor \
			--disable-mfschunkserver \
			--disable-mfsmount

.include <bsd.port.mk>
