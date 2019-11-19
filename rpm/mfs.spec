# build with "--define 'distro XXX' set to:
# "rhsysv" for RHEL / CentOS / Fedora<15
# "rhsystemd" for Fedora>=15
# ... (other awaiting contribution)

#define	distro	rhsysv

%define	_with_sysv	0
%define	_with_systemd	0
%if "%distro" == "rhsysv"
%define	_with_sysv	1
%define _relname	.rhsysv
%endif
%if "%distro" == "rhsystemd"
%define	_with_systemd	1
%define _relname	.rhsystemd
%endif
%define _groupname	mfs
%define _username	mfs

%define rpm_maj_v %(eval "rpm --version | cut -d' ' -f3 | cut -d'.' -f1")
%define rpm_min_v %(eval "rpm --version | cut -d' ' -f3 | cut -d'.' -f2")
%define rpm_has_bool_ops %(eval "if [ %{rpm_maj_v} -ge 5 -o %{rpm_maj_v} -ge 4 -a %{rpm_min_v} -ge 13 ]; then echo 1; else echo 0; fi")

Summary:	MooseFS - distributed, fault tolerant file system
Name:		moosefs
Version:	3.0.108
Release:	1%{?_relname}
License:	commercial
Group:		System Environment/Daemons
URL:		http://www.moosefs.com/
Source0:	%{name}-%{version}.tar.gz
%if %{rpm_has_bool_ops}
BuildRequires:	(fuse-devel or fuse3-devel >= 3.2.1)
%else
BuildRequires:	fuse-devel
%endif
BuildRequires:	pkgconfig
BuildRequires:	zlib-devel
BuildRequires:	libpcap-devel
%if %{rpm_has_bool_ops}
BuildRequires:	(python3 or python2 or /usr/bin/python3 or /usr/bin/python2 or /usr/bin/python)
%else
BuildRequires:	python
%endif
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Requires(pre):	shadow-utils


%define		_localstatedir	/var/lib
%define		mfsconfdir	%{_sysconfdir}/mfs
%if %{_with_systemd}
%define		systemdunitdir	%{?_unitdir}%{!?_unitdir:/lib/systemd/system}
%endif

%description
MooseFS is an Open Source, easy to deploy and maintain, distributed,
fault tolerant file system for POSIX compliant OSes.



%package master
Summary:	MooseFS master server
Group:		System Environment/Daemons

%description master
MooseFS master (metadata) server together with mfssupervisor utility.




%package metalogger
Summary:	MooseFS metalogger server
Group:		System Environment/Daemons

%description metalogger
MooseFS metalogger (metadata replication) server.




%package chunkserver
Summary:	MooseFS data server
Group:		System Environment/Daemons

%description chunkserver
MooseFS data server.




%package client
Summary:	MooseFS client
Group:		System Environment/Daemons

%description client
MooseFS client: mfsmount and mfstools.




%package cli
Summary:	MooseFS CLI Utility
Group:		System Environment/Daemons
Requires:	python >= 2.5

%description cli
MooseFS CLI utilities.




%package cgi
Summary:	MooseFS CGI Monitor
Group:		System Environment/Daemons
Requires:	python >= 2.5

%description cgi
MooseFS CGI monitor.




%package cgiserv
Summary:	Simple CGI-capable HTTP server to run MooseFS CGI Monitor
Group:		System Environment/Daemons
Requires:	%{name}-cgi

%description cgiserv
Simple CGI-capable HTTP server to run MooseFS CGI monitor.




%package netdump
Summary:	MooseFS network packet dump utility
Group:		System Environment/Daemons
Requires:	libpcap

%description netdump
MooseFS network packet dump utility




%prep
%setup -q

%build
%configure --with-default-user=%{_username} --with-default-group=%{_groupname} \
%if %{_with_systemd}
	--with-systemdsystemunitdir=%{systemdunitdir}
%endif

make %{?_smp_mflags}

%install
getent group %{_groupname} >/dev/null || groupadd -r %{_groupname}
getent passwd %{_username} >/dev/null || \
    useradd -r -g %{_groupname} -d %{_localstatedir}/mfs -s /sbin/nologin \
    -c "MooseFS" %{_username}

rm -rf $RPM_BUILD_ROOT

make install \
	DESTDIR=$RPM_BUILD_ROOT

EXTRA_FILES=$RPM_BUILD_ROOT/ExtraFiles.list
touch %{EXTRA_FILES}

if [ -x %{buildroot}/%{_sbindir}/mfsbdev ]; then
	echo '%attr(755,root,root) %{_sbindir}/mfsbdev' > %{EXTRA_FILES}
	echo '%{_mandir}/man8/mfsbdev.8*' >> %{EXTRA_FILES}
fi

%if "%{distro}" == "rhsysv"
install -d $RPM_BUILD_ROOT%{_initrddir}
for f in rpm/rh/*.init ; do
	sed -e 's,@sysconfdir@,%{_sysconfdir},g;
		s,@sbindir@,%{_sbindir},g;
		s,@initddir@,%{_initrddir},g' $f > $RPM_BUILD_ROOT%{_initrddir}/$(basename $f .init)
done
%endif

%clean
rm -rf $RPM_BUILD_ROOT




%pre master
getent group %{_groupname} >/dev/null || groupadd -r %{_groupname}
getent passwd %{_username} >/dev/null || \
    useradd -r -g %{_groupname} -d %{_localstatedir}/mfs -s /sbin/nologin \
    -c "MooseFS" %{_username}
exit 0

%post master
for fname in mfsexports mfstopology mfsmaster; do
	if [ -f %{mfsconfdir}/${fname}.cfg.dist ]; then
		rm -f %{mfsconfdir}/${fname}.cfg.dist
	fi
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
if [ ! -f %{_localstatedir}/mfs/metadata.mfs -a ! -f %{_localstatedir}/mfs/metadata.mfs.back -a -f %{_localstatedir}/mfs/metadata.mfs.empty ]; then
	cp %{_localstatedir}/mfs/metadata.mfs.empty %{_localstatedir}/mfs/metadata.mfs
fi
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rw %{_localstatedir}/mfs
chmod u+x %{_localstatedir}/mfs
exit 0




%pre metalogger
getent group %{_groupname} >/dev/null || groupadd -r %{_groupname}
getent passwd %{_username} >/dev/null || \
    useradd -r -g %{_groupname} -d %{_localstatedir}/mfs -s /sbin/nologin \
    -c "MooseFS" %{_username}
exit 0

%post metalogger
for fname in mfsmetalogger; do
	if [ -f %{mfsconfdir}/${fname}.cfg.dist ]; then
		rm -f %{mfsconfdir}/${fname}.cfg.dist
	fi
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0




%pre chunkserver
getent group %{_groupname} >/dev/null || groupadd -r %{_groupname}
getent passwd %{_username} >/dev/null || \
    useradd -r -g %{_groupname} -d %{_localstatedir}/mfs -s /sbin/nologin \
    -c "MooseFS" %{_username}
exit 0

%post chunkserver
for fname in mfschunkserver mfshdd; do
	if [ -f %{mfsconfdir}/${fname}.cfg.dist ]; then
		rm -f %{mfsconfdir}/${fname}.cfg.dist
	fi
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0




%post client
for fname in mfsmount; do
	if [ -f %{mfsconfdir}/${fname}.cfg.dist ]; then
		rm -f %{mfsconfdir}/${fname}.cfg.dist
	fi
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
exit 0




%pre cgiserv
getent group %{_groupname} >/dev/null || groupadd -r %{_groupname}
getent passwd %{_username} >/dev/null || \
    useradd -r -g %{_groupname} -d %{_localstatedir}/mfs -s /sbin/nologin \
    -c "MooseFS" %{_username}
exit 0

%post cgiserv
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0




%files master
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_sbindir}/mfsmaster
%attr(755,root,root) %{_sbindir}/mfsmetadump
%attr(755,root,root) %{_sbindir}/mfsmetadirinfo
%attr(755,root,root) %{_sbindir}/mfsmetarestore
%attr(755,root,root) %{_sbindir}/mfsstatsdump
%{_mandir}/man5/mfsexports.cfg.5*
%{_mandir}/man5/mfstopology.cfg.5*
%{_mandir}/man5/mfsmaster.cfg.5*
%{_mandir}/man8/mfsmaster.8*
%{_mandir}/man8/mfsmetarestore.8*
%{_mandir}/man8/mfsmetadump.8*
%{_mandir}/man8/mfsmetadirinfo.8*
%{_mandir}/man8/mfsstatsdump.8*
%{mfsconfdir}/mfsexports.cfg.sample
%{mfsconfdir}/mfstopology.cfg.sample
%{mfsconfdir}/mfsmaster.cfg.sample
%dir %{_localstatedir}/mfs
%{_localstatedir}/mfs/metadata.mfs.empty
%if %{_with_sysv}
%attr(754,root,root) %{_initrddir}/moosefs-master
%endif
%if %{_with_systemd}
%{systemdunitdir}/moosefs-master.service
%{systemdunitdir}/moosefs-master@.service
%endif




%files metalogger
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_sbindir}/mfsmetalogger
%{_mandir}/man5/mfsmetalogger.cfg.5*
%{_mandir}/man8/mfsmetalogger.8*
%{mfsconfdir}/mfsmetalogger.cfg.sample
%dir %{_localstatedir}/mfs
%if %{_with_sysv}
%attr(754,root,root) %{_initrddir}/moosefs-metalogger
%endif
%if %{_with_systemd}
%{systemdunitdir}/moosefs-metalogger.service
%{systemdunitdir}/moosefs-metalogger@.service
%endif




%files chunkserver
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_sbindir}/mfschunkserver
%attr(755,root,root) %{_sbindir}/mfschunktool
%attr(755,root,root) %{_sbindir}/mfscsstatsdump
%{_mandir}/man5/mfschunkserver.cfg.5*
%{_mandir}/man5/mfshdd.cfg.5*
%{_mandir}/man8/mfschunkserver.8*
%{_mandir}/man8/mfschunktool.8*
%{_mandir}/man8/mfscsstatsdump.8*
%{mfsconfdir}/mfschunkserver.cfg.sample
%{mfsconfdir}/mfshdd.cfg.sample
%dir %{_localstatedir}/mfs
%if %{_with_sysv}
%attr(754,root,root) %{_initrddir}/moosefs-chunkserver
%endif
%if %{_with_systemd}
%{systemdunitdir}/moosefs-chunkserver.service
%{systemdunitdir}/moosefs-chunkserver@.service
%endif




%files client -f %{EXTRA_FILES}
%defattr(644,root,root,755)
%doc NEWS README
%{_bindir}/mfsappendchunks
%{_bindir}/mfscheckfile
%{_bindir}/mfsdirinfo
%{_bindir}/mfsfileinfo
%{_bindir}/mfsfilerepair
%{_bindir}/mfsmakesnapshot
%{_bindir}/mfsrmsnapshot
%{_bindir}/mfsgetgoal
%{_bindir}/mfssetgoal
%{_bindir}/mfscopygoal
%{_bindir}/mfsrgetgoal
%{_bindir}/mfsrsetgoal
%{_bindir}/mfsgetsclass
%{_bindir}/mfssetsclass
%{_bindir}/mfscopysclass
%{_bindir}/mfsxchgsclass
%{_bindir}/mfslistsclass
%{_bindir}/mfsgettrashtime
%{_bindir}/mfssettrashtime
%{_bindir}/mfscopytrashtime
%{_bindir}/mfsrgettrashtime
%{_bindir}/mfsrsettrashtime
%{_bindir}/mfsgeteattr
%{_bindir}/mfsseteattr
%{_bindir}/mfsdeleattr
%{_bindir}/mfscopyeattr
%{_bindir}/mfsgetquota
%{_bindir}/mfssetquota
%{_bindir}/mfsdelquota
%{_bindir}/mfscopyquota
%{_bindir}/mfschkarchive
%{_bindir}/mfsclrarchive
%{_bindir}/mfssetarchive
%{_bindir}/mfsfilepaths
%{_bindir}/mfsscadmin
%attr(755,root,root) %{_bindir}/mfstools
%attr(755,root,root) %{_bindir}/mfsmount
# %attr(755,root,root) %{_sbindir}/mfsbdev - moved to EXTRA_FILES
/sbin/mount.moosefs
%{_includedir}/mfsio.h
%{_libdir}/libmfsio.a
%{_libdir}/libmfsio.la
%{_libdir}/libmfsio.so
%{_libdir}/libmfsio.so.1
%{_libdir}/libmfsio.so.1.0.0
%{_mandir}/man1/mfsappendchunks.1*
%{_mandir}/man1/mfscheckfile.1*
%{_mandir}/man1/mfsdirinfo.1*
%{_mandir}/man1/mfsfileinfo.1*
%{_mandir}/man1/mfsfilerepair.1*
%{_mandir}/man1/mfsmakesnapshot.1*
%{_mandir}/man1/mfsrmsnapshot.1*
%{_mandir}/man1/mfsgetgoal.1*
%{_mandir}/man1/mfssetgoal.1*
%{_mandir}/man1/mfscopygoal.1*
%{_mandir}/man1/mfsrgetgoal.1*
%{_mandir}/man1/mfsrsetgoal.1*
%{_mandir}/man1/mfsgetsclass.1*
%{_mandir}/man1/mfssetsclass.1*
%{_mandir}/man1/mfscopysclass.1*
%{_mandir}/man1/mfsxchgsclass.1*
%{_mandir}/man1/mfslistsclass.1*
%{_mandir}/man1/mfsgettrashtime.1*
%{_mandir}/man1/mfssettrashtime.1*
%{_mandir}/man1/mfscopytrashtime.1*
%{_mandir}/man1/mfsrgettrashtime.1*
%{_mandir}/man1/mfsrsettrashtime.1*
%{_mandir}/man1/mfsgeteattr.1*
%{_mandir}/man1/mfsseteattr.1*
%{_mandir}/man1/mfsdeleattr.1*
%{_mandir}/man1/mfscopyeattr.1*
%{_mandir}/man1/mfsgetquota.1*
%{_mandir}/man1/mfssetquota.1*
%{_mandir}/man1/mfsdelquota.1*
%{_mandir}/man1/mfscopyquota.1*
%{_mandir}/man1/mfschkarchive.1*
%{_mandir}/man1/mfsclrarchive.1*
%{_mandir}/man1/mfssetarchive.1*
%{_mandir}/man1/mfsfilepaths.1*
%{_mandir}/man1/mfsgoal.1*
%{_mandir}/man1/mfstrashtime.1*
%{_mandir}/man1/mfseattr.1*
%{_mandir}/man1/mfsquota.1*
%{_mandir}/man1/mfsarchive.1*
%{_mandir}/man1/mfssnapshots.1*
%{_mandir}/man1/mfsdiagtools.1*
%{_mandir}/man1/mfssclass.1*
%{_mandir}/man1/mfsscadmin.1*
%{_mandir}/man1/mfstools.1*
%{_mandir}/man8/mfsmount.8*
# %{_mandir}/man8/mfsbdev.8* - moved to EXTRA_FILES
%{_mandir}/man8/mount.moosefs.8*
%{mfsconfdir}/mfsmount.cfg.sample




%files cli
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_bindir}/mfscli
%{_mandir}/man1/mfscli.1*




%files cgi
%defattr(644,root,root,755)
%doc NEWS README
%dir %{_datadir}/mfscgi
%attr(755,root,root) %{_datadir}/mfscgi/*.cgi
%{_datadir}/mfscgi/*.css
%{_datadir}/mfscgi/*.gif
%{_datadir}/mfscgi/*.html
%{_datadir}/mfscgi/*.ico
%{_datadir}/mfscgi/*.js
%{_datadir}/mfscgi/*.png




%files cgiserv
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_sbindir}/mfscgiserv
%{_mandir}/man8/mfscgiserv.8*
%dir %{_localstatedir}/mfs
%if %{_with_sysv}
%attr(754,root,root) %{_initrddir}/moosefs-cgiserv
%endif
%if %{_with_systemd}
%{systemdunitdir}/moosefs-cgiserv.service
%endif




%files netdump
%defattr(644,root,root,755)
%doc NEWS README
%attr(755,root,root) %{_sbindir}/mfsnetdump
%{_mandir}/man8/mfsnetdump.8*




%changelog
* Fri Jun 19 2015 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 3.0.30-1
- added mfsstatsdumps and some man pages

* Wed Oct 15 2014 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 2.0.40-1
- fixed paths in systemd/*.service files

* Tue Jun  3 2014 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 2.0.10-1
- added new package: mfsnetdump

* Thu Feb 20 2014 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 1.7.25-1
- mfssupervisor moved to separate package
- added dependencies for cli and cgi

* Wed Feb 19 2014 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 1.7.24-1
- added new files
- added default user and group (mfs:mfs)
- fixed working directory attributes
- added working directory for chunkserver and metalogger
- added pre/post scripts

* Fri Jan 04 2013 Jakub Bogusz <contact@moosefs.com> - 1.6.28-1
- MFS update
- mfscli packaging

* Wed Oct 24 2012 Jakub Bogusz <contact@moosefs.com> - 1.6.27-2
- preliminary systemd support (included in RPMs when building with
  --define "distro rhsystemd"; SysV scripts condition changed to
  --define "distro rhsysv")

* Thu Feb 16 2012 Jakub Bogusz <contact@moosefs.com> - 1.6.27-1
- adjusted to keep configuration files in /etc/mfs
- require just mfsexports.cfg (master) and mfshdd.cfg (chunkserver) in RH-like
  init scripts; for other files defaults are just fine to run services
- moved mfscgiserv to -cgiserv subpackage (-cgi alone can be used with any
  external CGI-capable HTTP server), added mfscgiserv init script

* Fri Nov 19 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.19-1
- separated metalogger subpackage (following Debian packaging)

* Fri Oct  8 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.17-1
- added init scripts based on work of Steve Huff (Dag Apt Repository)
  (included in RPMs when building with --define "distro rh")

* Mon Jul 19 2010 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 1.6.16-1
- added mfscgiserv man page

* Fri Jun 11 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.15-1
- initial spec file, based on Debian packaging;
  partially inspired by spec file by Kirby Zhou
