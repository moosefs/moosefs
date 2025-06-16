PORTNAME= 		moosefs-cgi
PORTVERSION=		0.0.0
CATEGORIES= 		filesystems
MAINTAINER=		freebsd@moosefs.com
COMMENT=		Meta package to redirect old -cgi package to new -gui package
MASTER_SITES=

RUN_DEPENDS=		moosefs-gui>0:filesystems/moosefs-gui

NO_BUILD= 		yes
NO_ARCH= 		yes
USES=			metaport

DEPRECATED=		Renamed to moosefs-gui
EXPIRATION_DATE=	2026-12-31

.include <bsd.port.mk>
