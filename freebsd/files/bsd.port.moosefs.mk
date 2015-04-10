PORTVERSION=	%%VERSION%%
PORTREVISION=	1

MASTER_SITES=	http://ppa.moosefs.com/src/

LICENSE=	GPLv2

LICENSE_FILE=	${WRKSRC}/COPYING

GNU_CONFIGURE=	yes

MFS_USER=	mfs
MFS_GROUP=	mfs
MFS_WORKDIR=	/var
MFS_CGIDIR=	${PREFIX}/share/mfscgi
MFS_CGISERVDIR=	${PREFIX}/sbin

DISTNAME=	${PORTNAME}-${PORTVERSION}-${PORTREVISION}
WRKSRC=		${WRKDIR}/${PORTNAME}-${PORTVERSION}

MAINTAINER=	freebsd@moosefs.com

USERS=		${MFS_USER}
GROUPS=		${MFS_GROUP}

CONFIGURE_ARGS+=	--localstatedir=${MFS_WORKDIR} \
			--with-default-user=${MFS_USER} \
			--with-default-group=${MFS_GROUP} \
			--with-mfscgiserv-dir=${MFS_CGISERVDIR} \
			--with-mfscgi-dir=${MFS_CGIDIR}
