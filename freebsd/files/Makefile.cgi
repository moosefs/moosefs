PORTNAME=		moosefs-cgi
PORTVERSION=		0.0.0
CATEGORIES=		filesystems

MAINTAINER=		freebsd@moosefs.com
COMMENT=		Meta package to redirect old -cgi package to new -gui package
WWW=			https://moosefs.com/

LICENSE=		GPLv2

DEPRECATED=		Renamed to moosefs-gui
EXPIRATION_DATE=	2026-12-31

RUN_DEPENDS=		moosefs-gui>0:filesystems/moosefs-gui

NO_BUILD=		yes
NO_ARCH=		yes
USES=			metaport

.include <bsd.port.mk>
