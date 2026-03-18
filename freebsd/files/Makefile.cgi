PORTNAME=		moosefs-cgi
DISTVERSION=		0.0.0
CATEGORIES=		filesystems

MAINTAINER=		freebsd@moosefs.com
COMMENT=		Meta package to redirect old -cgi package to new -gui package
WWW=			https://moosefs.com/

LICENSE=		GPLv2

DEPRECATED=		Renamed to moosefs-gui
EXPIRATION_DATE=	2026-12-31

RUN_DEPENDS=		moosefs-gui>0:filesystems/moosefs-gui
USES=			metaport
NO_ARCH=		yes
NO_BUILD=		yes

.include <bsd.port.mk>
