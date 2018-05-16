#!/usr/bin/env bash

cat debian/control | sed 's/^Build-Depends: /Build-Depends: dh-systemd | debhelper (>= 10.0), /' > debian/control_systemd
mv debian/control_systemd debian/control

rm -f debian/*.init debian/*.default

cat debian/rules | sed 's/dh_installinit/cp systemd\/*.service debian\
	dh_installinit\
	dh_systemd_start --no-start --no-restart-on-upgrade/' > debian/rules_systemd
mv debian/rules_systemd debian/rules

cp debian/systemd_extra/* debian/

echo "lib/systemd/system/moosefs-master@.service" >> debian/moosefs-master.install
echo "lib/systemd/system/moosefs-chunkserver@.service" >> debian/moosefs-chunkserver.install
echo "lib/systemd/system/moosefs-metalogger@.service" >> debian/moosefs-metalogger.install
