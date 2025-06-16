#!/usr/bin/env bash

cat debian/control | sed 's/^Build-Depends: /Build-Depends: dh-systemd | debhelper (>= 10.0), /' > debian/control_systemd
mv debian/control_systemd debian/control

rm -f debian/*.init debian/*.default

#cat debian/rules | sed 's/dh_installinit/cp systemd\/*.service debian\
#	dh_installinit\
#	dh_systemd_start --no-start --no-restart-on-upgrade/' > debian/rules_systemd
#mv debian/rules_systemd debian/rules

#echo "
#override_dh_installsystemd:
#	dh_installsystemd --no-start --no-restart-on-upgrade
#"

echo "
override_dh_systemd_start:
	dh_systemd_start --no-start --no-restart-on-upgrade
" >> debian/rules

cp debian/systemd_extra/* debian/

systemdunitdir=`pkg-config --variable=systemdsystemunitdir systemd`

echo "${systemdunitdir}/moosefs-gui@.service" >> debian/moosefs-gui.install
echo "${systemdunitdir}/moosefs-master@.service" >> debian/moosefs-master.install
echo "${systemdunitdir}/moosefs-chunkserver@.service" >> debian/moosefs-chunkserver.install
echo "${systemdunitdir}/moosefs-metalogger@.service" >> debian/moosefs-metalogger.install
echo "${systemdunitdir}/moosefs-gui.service" >> debian/moosefs-gui.install
echo "${systemdunitdir}/moosefs-master.service" >> debian/moosefs-master.install
echo "${systemdunitdir}/moosefs-chunkserver.service" >> debian/moosefs-chunkserver.install
echo "${systemdunitdir}/moosefs-metalogger.service" >> debian/moosefs-metalogger.install
