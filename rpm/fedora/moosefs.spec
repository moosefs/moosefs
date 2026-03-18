%define _groupname	mfs
%define _username	mfs

# Turn off lto
%global _lto_cflags %{nil}
%global _lto_ldflags %{nil}

%define rpm_maj_v %(eval "rpm --version | cut -d' ' -f3 | cut -d'.' -f1")
%define rpm_min_v %(eval "rpm --version | cut -d' ' -f3 | cut -d'.' -f2")
%define rpm_has_bool_ops %(eval "if [ %{rpm_maj_v} -ge 5 -o %{rpm_maj_v} -ge 4 -a %{rpm_min_v} -ge 13 ]; then echo 1; else echo 0; fi")

Summary:	Distributed, scalable, fault tolerant file system
Name:		moosefs
Version:	4.58.3
Release:	%autorelease
License:	GPL-2.0-only
URL:		http://www.moosefs.com/
Source:		https://repository.moosefs.com/src/moosefs-%{version}-1.tar.gz
Source1:	moosefs.sysusers
BuildRequires:	gcc
BuildRequires:	make
BuildRequires:	fuse3-devel >= 3.2.1
BuildRequires:	pkgconfig
BuildRequires:	zlib-devel
BuildRequires:	libpcap-devel
BuildRequires:	python3 >= 3.4
BuildRequires:	systemd-rpm-macros
%{?sysusers_requires_compat}
Requires(pre):	shadow-utils


%define		_localstatedir	/var/lib
%define		mfsconfdir	%{_sysconfdir}/mfs
%define		systemdunitdir	%{?_unitdir}%{!?_unitdir:/lib/systemd/system}

%description
MooseFS is an Open Source, easy to deploy and maintain, distributed,
fault tolerant file system for POSIX compliant OSes.



%package master
Summary:	Master (metadata) server
Group:		System Environment/Daemons

%description master
MooseFS master (metadata) server together with mfssupervisor utility.




%package metalogger
Summary:	Metalogger (backup) server
Group:		System Environment/Daemons

%description metalogger
MooseFS metalogger (metadata replication) server.




%package chunkserver
Summary:	Chunk (data) server
Group:		System Environment/Daemons

%description chunkserver
MooseFS data server.




%package client
Summary:	Client (mount + block device)
Group:		System Environment/Daemons

%description client
MooseFS client: mounting tool, block device manager and various utilities.




%package libmfsio-devel
Summary:	MFSio library
Group:		Development/C

%description libmfsio-devel
MooseFS I/O library source.




%package cli
Summary:	CLI Utility
BuildArch:	noarch
Group:		System Environment/Daemons

%description cli
MooseFS CLI utilities.




%package gui
Summary:	GUI Monitor
Group:		System Environment/Daemons

%description gui
MooseFS web-based GUI.




%package netdump
Summary:	Network packet dump utility
Group:		System Environment/Daemons

%description netdump
MooseFS network packet dump utility




%prep
%setup -q

%build
%configure --with-default-user=%{_username} --with-default-group=%{_groupname} \
	--with-systemdsystemunitdir=%{systemdunitdir} --with-rootsbindir=%{_bindir} \
	--disable-static

%make_build

%install

install -Dpm0644 %{SOURCE1} %{buildroot}%{_sysusersdir}/%{name}.conf

%make_install

EXTRA_FILES=$RPM_BUILD_ROOT/ExtraFiles.list
touch %{EXTRA_FILES}

if [ -x %{buildroot}/%{_bindir}/mfsbdev ]; then
	echo '%attr(755,root,root) %{_bindir}/mfsbdev' > %{EXTRA_FILES}
	echo '%{_mandir}/man8/mfsbdev.8*' >> %{EXTRA_FILES}
	echo '%{_mandir}/man5/mfsbdev.cfg.5*' >> %{EXTRA_FILES}
fi



%pre master
%sysusers_create_compat %{SOURCE1}
exit 0

%post master
for fname in mfsexports mfstopology mfsipmap mfsmaster; do
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
%sysusers_create_compat %{SOURCE1}
exit 0

%post metalogger
for fname in mfsmetalogger; do
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0




%pre chunkserver
%sysusers_create_compat %{SOURCE1}
exit 0

%post chunkserver
for fname in mfschunkserver mfshdd; do
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0




%post client
for fname in mfsmount; do
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
exit 0



%pre gui
%sysusers_create_compat %{SOURCE1}
exit 0

%post gui
for fname in mfsgui; do
	if [ -f %{mfsconfdir}/${fname}.cfg.sample -a ! -f %{mfsconfdir}/${fname}.cfg ]; then
		cp %{mfsconfdir}/${fname}.cfg.sample %{mfsconfdir}/${fname}.cfg
	fi
done
chown -R %{_username}:%{_groupname} %{_localstatedir}/mfs
chmod -R u+rwx %{_localstatedir}/mfs
exit 0



%files master
%{_sysusersdir}/%{name}.conf
%doc NEWS README
%{_bindir}/mfsmaster
%{_bindir}/mfsmetadump
%{_bindir}/mfsmetadirinfo
%{_bindir}/mfsmetasearch
%{_bindir}/mfsmetarestore
%{_bindir}/mfsstatsdump
%{_bindir}/mfssupervisor
%{_mandir}/man5/mfsexports.cfg.5*
%{_mandir}/man5/mfstopology.cfg.5*
%{_mandir}/man5/mfsipmap.cfg.5*
%{_mandir}/man5/mfsmaster.cfg.5*
%{_mandir}/man7/moosefs.7*
%{_mandir}/man8/mfsmaster.8*
%{_mandir}/man8/mfsmetarestore.8*
%{_mandir}/man8/mfsmetadump.8*
%{_mandir}/man8/mfsmetadirinfo.8*
%{_mandir}/man8/mfsmetasearch.8*
%{_mandir}/man8/mfsstatsdump.8*
%{_mandir}/man8/mfssupervisor.8*
%{mfsconfdir}/mfsexports.cfg.sample
%{mfsconfdir}/mfstopology.cfg.sample
%{mfsconfdir}/mfsipmap.cfg.sample
%{mfsconfdir}/mfsmaster.cfg.sample
%dir %{_localstatedir}/mfs
%{_localstatedir}/mfs/metadata.mfs.empty
%{systemdunitdir}/moosefs-master.service
%{systemdunitdir}/moosefs-master@.service




%files metalogger
%{_sysusersdir}/%{name}.conf
%doc NEWS README
%{_bindir}/mfsmetalogger
%{_mandir}/man5/mfsmetalogger.cfg.5*
%{_mandir}/man8/mfsmetalogger.8*
%{mfsconfdir}/mfsmetalogger.cfg.sample
%dir %{_localstatedir}/mfs
%{systemdunitdir}/moosefs-metalogger.service
%{systemdunitdir}/moosefs-metalogger@.service




%files chunkserver
%{_sysusersdir}/%{name}.conf
%doc NEWS README
%{_bindir}/mfschunkserver
%{_bindir}/mfschunktool
%{_bindir}/mfschunkdbdump
%{_bindir}/mfscsstatsdump
%{_mandir}/man5/mfschunkserver.cfg.5*
%{_mandir}/man5/mfshdd.cfg.5*
%{_mandir}/man8/mfschunkserver.8*
%{_mandir}/man8/mfschunktool.8*
%{_mandir}/man8/mfschunkdbdump.8*
%{_mandir}/man8/mfscsstatsdump.8*
%{mfsconfdir}/mfschunkserver.cfg.sample
%{mfsconfdir}/mfshdd.cfg.sample
%dir %{_localstatedir}/mfs
%{systemdunitdir}/moosefs-chunkserver.service
%{systemdunitdir}/moosefs-chunkserver@.service




%files client -f %{EXTRA_FILES}
%doc NEWS README
%{_bindir}/mfscheckfile
%{_bindir}/mfsdirinfo
%{_bindir}/mfsfileinfo
%{_bindir}/mfsfilerepair
%{_bindir}/mfsfilepaths
%{_bindir}/mfsmakesnapshot
%{_bindir}/mfsrmsnapshot
%{_bindir}/mfsappendchunks
%{_bindir}/mfsgetfacl
%{_bindir}/mfssetfacl
%{_bindir}/mfsgetsclass
%{_bindir}/mfssetsclass
%{_bindir}/mfscopysclass
%{_bindir}/mfsxchgsclass
%{_bindir}/mfsgettrashtime
%{_bindir}/mfssettrashtime
%{_bindir}/mfscopytrashtime
%{_bindir}/mfsgettrashretention
%{_bindir}/mfssettrashretention
%{_bindir}/mfscopytrashretention
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
%{_bindir}/mfscreatesclass
%{_bindir}/mfsmodifysclass
%{_bindir}/mfsdeletesclass
%{_bindir}/mfsclonesclass
%{_bindir}/mfsrenamesclass
%{_bindir}/mfslistsclass
%{_bindir}/mfsimportsclass
%{_bindir}/mfscreatepattern
%{_bindir}/mfsdeletepattern
%{_bindir}/mfslistpattern
%{_bindir}/mfsgetgoal
%{_bindir}/mfssetgoal
%{_bindir}/mfscopygoal
%{_bindir}/mfsdiagtools
%{_bindir}/mfssnapshots
%{_bindir}/mfsfacl
%{_bindir}/mfssclass
%{_bindir}/mfstrashtime
%{_bindir}/mfstrashretention
%{_bindir}/mfseattr
%{_bindir}/mfsquota
%{_bindir}/mfsarchive
%{_bindir}/mfsscadmin
%{_bindir}/mfspatadmin
%{_bindir}/mfstrashtool
%{_bindir}/mfsmount
%{_bindir}/mount.moosefs
%{_libdir}/libmfsio.so
%{_libdir}/libmfsio.so.1
%attr(755,root,root) %{_libdir}/libmfsio.so.1.0.0
%{_mandir}/man1/mfscheckfile.1*
%{_mandir}/man1/mfsdirinfo.1*
%{_mandir}/man1/mfsfileinfo.1*
%{_mandir}/man1/mfsfilepaths.1*
%{_mandir}/man1/mfsfilerepair.1*
%{_mandir}/man1/mfsappendchunks.1*
%{_mandir}/man1/mfsmakesnapshot.1*
%{_mandir}/man1/mfsrmsnapshot.1*
%{_mandir}/man1/mfsgetfacl.1*
%{_mandir}/man1/mfssetfacl.1*
%{_mandir}/man1/mfsgetgoal.1*
%{_mandir}/man1/mfssetgoal.1*
%{_mandir}/man1/mfscopygoal.1*
%{_mandir}/man1/mfsgetsclass.1*
%{_mandir}/man1/mfssetsclass.1*
%{_mandir}/man1/mfscopysclass.1*
%{_mandir}/man1/mfsxchgsclass.1*
%{_mandir}/man1/mfscreatesclass.1*
%{_mandir}/man1/mfsmodifysclass.1*
%{_mandir}/man1/mfsdeletesclass.1*
%{_mandir}/man1/mfsclonesclass.1*
%{_mandir}/man1/mfsrenamesclass.1*
%{_mandir}/man1/mfsimportsclass.1*
%{_mandir}/man1/mfslistsclass.1*
%{_mandir}/man1/mfscreatepattern.1*
%{_mandir}/man1/mfsdeletepattern.1*
%{_mandir}/man1/mfslistpattern.1*
%{_mandir}/man1/mfsgettrashtime.1*
%{_mandir}/man1/mfssettrashtime.1*
%{_mandir}/man1/mfscopytrashtime.1*
%{_mandir}/man1/mfsgettrashretention.1*
%{_mandir}/man1/mfssettrashretention.1*
%{_mandir}/man1/mfscopytrashretention.1*
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
%{_mandir}/man1/mfsfacl.1*
%{_mandir}/man1/mfsgoal.1*
%{_mandir}/man1/mfstrashtime.1*
%{_mandir}/man1/mfstrashretention.1*
%{_mandir}/man1/mfseattr.1*
%{_mandir}/man1/mfsquota.1*
%{_mandir}/man1/mfsarchive.1*
%{_mandir}/man1/mfssnapshots.1*
%{_mandir}/man1/mfsdiagtools.1*
%{_mandir}/man1/mfssclass.1*
%{_mandir}/man1/mfsscadmin.1*
%{_mandir}/man1/mfspatadmin.1*
%{_mandir}/man1/mfstools.1*
%{_mandir}/man8/mfsmount.8*
%{_mandir}/man5/mfsmount.cfg.5*
%{_mandir}/man8/mount.moosefs.8*
%{mfsconfdir}/mfsmount.cfg.sample




%files libmfsio-devel
%doc NEWS README
%{_includedir}/mfsio.h




%files cli
%doc NEWS README
%{_bindir}/mfscli
%{_mandir}/man1/mfscli.1*




%files gui
%{_sysusersdir}/%{name}.conf
%doc NEWS README
%{_bindir}/mfscgiserv
%{_bindir}/mfsgui
%{_mandir}/man5/mfsgui.cfg.5*
%{_mandir}/man8/mfsgui.8*
%{mfsconfdir}/mfsgui.cfg.sample
%dir %{_localstatedir}/mfs
%{systemdunitdir}/moosefs-gui.service
%{systemdunitdir}/moosefs-gui@.service
%dir %{_datadir}/mfscgi
%attr(755,root,root) %{_datadir}/mfscgi/*.cgi
%{_datadir}/mfscgi/requests.cfg
%{_datadir}/mfscgi/assets/*
%{_datadir}/mfscgi/common/*
%{_datadir}/mfscgi/views/*






%files netdump
%doc NEWS README
%{_bindir}/mfsnetdump
%{_mandir}/man8/mfsnetdump.8*




%changelog
* Wed Nov 19 2025 Agata Kruszona-Zawadzka <fedora@moosefs.com> - 4.58.3-1
- initial spec file for Fedora, based on MooseFS's universal spec file
