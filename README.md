<img align="right" alt="MooseFS logo" src="https://moosefs.com/Content/Images/moosefs.png" />

# MooseFS - A Petabyte Distributed File System
MooseFS is a Petabyte Open Source Network Distributed File System. It is easy to deploy and maintain, fault tolerant, highly performing, easily scalable, POSIX compliant.

MooseFS spreads data over several physical commodity servers, which are visible to the user as one big volume. For standard file operations MooseFS acts like ordinary Unix-like file system:

* A hierarchical structure (directory tree)
* Stores POSIX file attributes (permissions, last access and modification times, etc.)
* Supports ACLs
* Supports POSIX and BSD locks
* Supports special files (block and character devices, pipes and sockets)
* Symbolic links (file names pointing to target files, not necessarily on MooseFS) and hard links (different names of files which refer to the same data on MooseFS)

Distinctive MooseFS features:

* High reliability - configurable number of files' copies are stored on separate servers
* No Single Point of Failure - all hardware and software components may be redundant
* Parallel data operations - many clients can access many files concurrently
* Capacity can be dynamically expanded by simply adding new computers/disks on the fly
* Retired hardware may be removed on the fly
* Deleted files are retained for a configurable period of time (a file system level "trash bin")
* Coherent, "atomic" snapshots of files, even while the files are being written/accessed
* Access to the file system can be limited based on IP address and/or password (similarly as in NFS)
* Data tiering - supports different storage policies for different files/directories
* Efficient, pure C implementation
* Ethernet support

## Supported platforms
MooseFS can be installed on any POSIX compliant operating system including different Linux distributions, FreeBSD, OS X:

* Ubuntu
* Debian
* RHEL/CentOS
* OpenSUSE
* FreeBSD
* macOS
* Raspbian - Raspberry Pi 3

MooseFS Linux Client uses [FUSE](https://github.com/libfuse/libfuse). MooseFS macOS Client uses [FUSE for macOS](https://github.com/osxfuse/osxfuse).

There is a separate MooseFS Client for Microsoft Windows available, built on top of [Dokany](https://github.com/dokan-dev/dokany).

## Getting started
You can install MooseFS using your favourite package manager on one of the following platforms using [officially supported repositories](https://moosefs.com/download):

* Ubuntu 14/16/18
* Debian 8/9
* RHEL/CentOS 6/7
* FreeBSD 9.3/10/11
* macOS 10.9-10.14
* Raspbian 8 (Jessie) - Raspberry Pi 3

Minimal set of packages, which are needed to run MooseFS:

* `moosefs-master` MooseFS Master Server for metadata servers,
* `moosefs-chunkserver` MooseFS Chunkserver for data storage servers,
* `moosefs-client` MooseFS Client - client side package to mount the filesystem.

### Source code
Feel free to download the source code from our GitHub code repository!

Install the following dependencies before building MooseFS from sources:

* Debian/Ubuntu: `sudo apt install build-essential libpcap-dev zlib1g-dev libfuse-dev pkg-config`
* CentOS/RHEL: `sudo yum install gcc make libpcap-devel zlib-devel fuse-devel pkgconfig`

Recommended packages:

* Debian/Ubuntu: `sudo apt install fuse`
* CentOS/RHEL: `sudo yum install fuse`

Building MooseFS on Linux can be easily done by running `./linux_build.sh`. Similarly, use `./freebsd_build.sh` in order to build MooseFS on FreeBSD and respectively `./macosx_build.sh` on macOS. Remember, that these scripts do not install binaries (i.e. do not run `make install`) at the end. Run this command manually.

### Minimal setup
Just three steps to have MooseFS up and running:

#### 1. Install at least one Master Server
1. Install `moosefs-master` package
2. Prepare default config (as `root`):
```
cd /etc/mfs
cp mfsmaster.cfg.sample mfsmaster.cfg
cp mfsexports.cfg.sample mfsexports.cfg
```
3. Prepare the metadata file (as `root`):
```
cd /var/lib/mfs
cp metadata.mfs.empty metadata.mfs
chown mfs:mfs metadata.mfs
rm metadata.mfs.empty
```
4. Run Master Server (as `root`): `mfsmaster start`
5. Make this machine visible under `mfsmaster` name (e.g. by adding a DNS entry or adding it in `/etc/hosts` on all servers)

#### 2. Install at least two Chunkservers
1. Install `moosefs-chunkserver` package
2. Prepare default config (as `root`):
```
cd /etc/mfs
cp mfschunkserver.cfg.sample mfschunkserver.cfg
cp mfshdd.cfg.sample mfshdd.cfg
```
3. At the end of `mfshdd.cfg` file make one or more entries containing paths to HDDs / partitions designated for storing chunks, e.g.:
```
/mnt/chunks1
/mnt/chunks2
/mnt/chunks3
```
It is recommended to use XFS as an underlying filesystem for disks designated to store chunks.

4. Change the ownership and permissions to `mfs:mfs` to above mentioned locations, e.g.:
```
chown mfs:mfs /mnt/chunks1 /mnt/chunks2 /mnt/chunks3
chmod 770 /mnt/chunks1 /mnt/chunks2 /mnt/chunks3
```
5. Start the Chunkserver: `mfschunkserver start`

Repeat steps above for second (third, ...) Chunkserver.

#### 3. Client side: mount MooseFS filesystem
1. Install `moosefs-client fuse libfuse2` packages
2. Mount MooseFS (as `root`):
```
mkdir /mnt/mfs
mount -t moosefs mfsmaster: /mnt/mfs
```
or: `mfsmount -H mfsmaster /mnt/mfs`

3. You can also add an `/etc/fstab` entry to mount MooseFS during the system boot:
```
mfsmaster:    /mnt/mfs    moosefs    defaults,mfsdelayedinit    0 0
```

There are more configuration parameters available but most of them may stay with defaults. We do our best to keep MooseFS easy to deploy and maintain.

MooseFS, for testing purposes, can even be installed on a single machine!

#### Additional tools
Setting up `moosefs-cli` or `moosefs-cgi` both with `moosefs-cgiserv` is also recommended - it gives you a possibility to monitor the cluster online:
1. Install `moosefs-cli moosefs-cgi moosefs-cgiserv` packages (they are typically set up on the Master Server)
2. Run MooseFS CGI Server (as `root`): `mfscgiserv start`
3. Open http://mfsmaster:9425 in your web browser

It is also strongly recommended to set up at least one Metalogger on a different machine than Master Server (e.g. on one of Chunkservers). Metalogger constantly synchronizes and backups the metadata:
1. Install `moosefs-metalogger` package
2. Prepare default config (as `root`):
```
cd /etc/mfs
cp mfsmetalogger.cfg.sample mfsmetalogger.cfg
```
3. Run Metalogger (as `root`): `mfsmetalogger start`



Refer to [installation guides](https://moosefs.com/support/#documentation) for more details.

## Some facts
* Date of the first public release: 2008-05-30
* The project web site: https://moosefs.com
* Installation and using MooseFS: https://moosefs.com/support
* (Old) Sourceforge project site: http://sourceforge.net/projects/moosefs

## Contact us
* Reporting bugs: [GitHub issue](https://github.com/moosefs/moosefs/issues) or [support@moosefs.com](mailto:support@moosefs.com)
* General: [contact@moosefs.com](mailto:contact@moosefs.com)


## Copyright
Copyright (c) 2008-2019 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.

This file is part of MooseFS.

MooseFS is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 2 (only).

MooseFS is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with MooseFS; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA or visit http://www.gnu.org/licenses/gpl-2.0.html.
