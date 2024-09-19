/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifndef _MFS_COMMUNICATION_H_
#define _MFS_COMMUNICATION_H_

// all data field transferred in network order.
// packet structure:
// type:32 length:32 data:lengthB
//

#ifndef PROTO_BASE
# ifdef HAVE_CONFIG_H
#  include "config.h"
# else
#  define PROTO_BASE 0
# endif
#endif

#define MFSBLOCKSINCHUNK 0x400
#if LIGHT_MFS == 1
# define MFSSIGNATURE "LFS"
# define MFSCHUNKSIZE 0x00400000
# define MFSCHUNKMASK 0x003FFFFF
# define MFSCHUNKBITS 22
# define MFSCHUNKBLOCKMASK 0x003FF000
# define MFSBLOCKSIZE 0x1000
# define MFSBLOCKMASK 0x0FFF
# define MFSBLOCKNEGMASK 0x7FFFF000
# define MFSBLOCKBITS 12
# define MFSCRCEMPTY 0xC71C0011U
# define MFSHDRSIZE 0x1080
#else
# define MFSSIGNATURE "MFS"
# define MFSCHUNKSIZE 0x04000000
# define MFSCHUNKMASK 0x03FFFFFF
# define MFSCHUNKBITS 26
# define MFSCHUNKBLOCKMASK 0x03FF0000
# define MFSBLOCKSIZE 0x10000
# define MFSBLOCKMASK 0x0FFFF
# define MFSBLOCKNEGMASK 0x7FFF0000
# define MFSBLOCKBITS 16
# define MFSCRCEMPTY 0xD7978EEBU
# define MFSHDRSIZE 0x2000
#endif

//UNIVERSAL
#define VERSION_ANY 0

#define CRC_POLY 0xEDB88320U

#define MFS_ROOT_ID 1

#define MASKORGROUP 4  // obsolete (see SCLASS_EXPR_MAX_SIZE) - only for compatibility with version 3.0
#define MAXSCLASS 256

#define SCLASS_EXPR_MAX_SIZE 128
#define MAXSCLASSNLENG 256
#define MAXLABELSCNT 9

#define MAXPATTERNLENG 256

#define MFS_NAME_MAX 255
#define MFS_SYMLINK_MAX 4096
#define MFS_PATH_MAX 1024

#define MFS_MAX_FILE_SIZE (((uint64_t)(MFSCHUNKSIZE))<<31)

#define EDGEID_MAX UINT64_C(0x7FFFFFFFFFFFFFFF)

#define TRASH_BUCKETS 4096
#define SUSTAINED_BUCKETS 256

#define MFS_STATUS_OK              0    // OK

#define MFS_ERROR_EPERM            1    // Operation not permitted
#define MFS_ERROR_ENOTDIR          2    // Not a directory
#define MFS_ERROR_ENOENT           3    // No such file or directory
#define MFS_ERROR_EACCES           4    // Permission denied
#define MFS_ERROR_EEXIST           5    // File exists
#define MFS_ERROR_EINVAL           6    // Invalid argument
#define MFS_ERROR_ENOTEMPTY        7    // Directory not empty
#define MFS_ERROR_CHUNKLOST        8    // Chunk lost
#define MFS_ERROR_OUTOFMEMORY      9    // Out of memory

#define MFS_ERROR_INDEXTOOBIG     10    // Index too big
#define MFS_ERROR_LOCKED          11    // Chunk locked
#define MFS_ERROR_NOCHUNKSERVERS  12    // No chunk servers
#define MFS_ERROR_NOCHUNK         13    // No such chunk
#define MFS_ERROR_CHUNKBUSY       14    // Chunk is busy
#define MFS_ERROR_REGISTER        15    // Incorrect register BLOB
#define MFS_ERROR_NOTDONE         16    // Operation not completed
#define MFS_ERROR_NOTOPENED       17    // File not opened
#define MFS_ERROR_NOTSTARTED      18    // Write not started

#define MFS_ERROR_WRONGVERSION    19    // Wrong chunk version
#define MFS_ERROR_CHUNKEXIST      20    // Chunk already exists
#define MFS_ERROR_NOSPACE         21    // No space left
#define MFS_ERROR_IO              22    // IO error
#define MFS_ERROR_BNUMTOOBIG      23    // Incorrect block number
#define MFS_ERROR_WRONGSIZE       24    // Incorrect size
#define MFS_ERROR_WRONGOFFSET     25    // Incorrect offset
#define MFS_ERROR_CANTCONNECT     26    // Can't connect
#define MFS_ERROR_WRONGCHUNKID    27    // Incorrect chunk id
#define MFS_ERROR_DISCONNECTED    28    // Disconnected
#define MFS_ERROR_CRC             29    // CRC error
#define MFS_ERROR_DELAYED         30    // Operation delayed
#define MFS_ERROR_CANTCREATEPATH  31    // Can't create path

#define MFS_ERROR_MISMATCH        32    // Data mismatch

#define MFS_ERROR_EROFS           33    // Read-only file system
#define MFS_ERROR_QUOTA           34    // Quota exceeded
#define MFS_ERROR_BADSESSIONID    35    // Bad session id
#define MFS_ERROR_NOPASSWORD      36    // Password is needed
#define MFS_ERROR_BADPASSWORD     37    // Incorrect password

#define MFS_ERROR_ENOATTR         38    // Attribute not found
#define MFS_ERROR_ENOTSUP         39    // Operation not supported
#define MFS_ERROR_ERANGE          40    // Result too large

#define MFS_ERROR_NOTFOUND        41    // Entity not found
#define MFS_ERROR_ACTIVE          42    // Entity is active

#define MFS_ERROR_CSNOTPRESENT    43    // Chunkserver not present

#define MFS_ERROR_WAITING         44    // Waiting on lock
#define MFS_ERROR_EAGAIN          45    // Resource temporarily unavailable
#define MFS_ERROR_EINTR           46    // Interrupted system call
#define MFS_ERROR_ECANCELED       47    // Operation canceled

#define MFS_ERROR_ENOENT_NOCACHE  48    // No such file or directory (do not store in cache)

#define MFS_ERROR_EPERM_NOTADMIN  49    // Operation not permitted (mfs admin only)

#define MFS_ERROR_CLASSEXISTS     50    // Class name already in use
#define MFS_ERROR_CLASSLIMITREACH 51    // Maximum number of classes reached
#define MFS_ERROR_NOSUCHCLASS     52    // No such class
#define MFS_ERROR_CLASSINUSE      53    // Class in use
#define MFS_ERROR_INCOMPATVERSION 54    // Incompatible version

#define MFS_ERROR_PATTERNEXISTS   55    // Pattern already defined
#define MFS_ERROR_PATLIMITREACHED 56    // Maximum number of patterns reached
#define MFS_ERROR_NOSUCHPATTERN   57    // No such pattern

#define MFS_ERROR_ENAMETOOLONG    58    // File name too long
#define MFS_ERROR_EMLINK          59    // Too many links

#define MFS_ERROR_ETIMEDOUT       60    // Operation timed out

#define MFS_ERROR_EBADF           61    // Bad file descriptor
#define MFS_ERROR_EFBIG           62    // File too large
#define MFS_ERROR_EISDIR          63    // Is a directory

#define MFS_ERROR_MAX             64

#define MFS_ERROR_STRINGS \
	"OK", \
	"Operation not permitted", \
	"Not a directory", \
	"No such file or directory", \
	"Permission denied", \
	"File exists", \
	"Invalid argument", \
	"Directory not empty", \
	"Chunk lost", \
	"Out of memory", \
	"Index too big", \
	"Chunk locked", \
	"No chunk servers", \
	"No such chunk", \
	"Chunk is busy", \
	"Incorrect register BLOB", \
	"Operation not completed", \
	"File not opened", \
	"Write not started", \
	"Wrong chunk version", \
	"Chunk already exists", \
	"No space left", \
	"IO error", \
	"Incorrect block number", \
	"Incorrect size", \
	"Incorrect offset", \
	"Can't connect", \
	"Incorrect chunk id", \
	"Disconnected", \
	"CRC error", \
	"Operation delayed", \
	"Can't create path", \
	"Data mismatch", \
	"Read-only file system", \
	"Quota exceeded", \
	"Bad session id", \
	"Password is needed", \
	"Incorrect password", \
	"Attribute not found", \
	"Operation not supported", \
	"Result too large", \
	"Entity not found", \
	"Entity is active", \
	"Chunkserver not present", \
	"Waiting on lock", \
	"Resource temporarily unavailable", \
	"Interrupted system call", \
	"Operation canceled", \
	"No such file or directory (not cacheable)", \
	"Operation not permitted (mfs admin only)", \
	"Class name already in use", \
	"Maximum number of classes reached", \
	"No such class", \
	"Class in use", \
	"Incompatible version", \
	"Pattern already defined", \
	"Maximum number of patterns reached", \
	"No such pattern", \
	"File name too long", \
	"Too many links", \
	"Operation timed out", \
	"Bad file descriptor", \
	"File too large", \
	"Is a directory", \
	"Unknown MFS error"

#define MFSLOG_DEBUG                       0
#define MFSLOG_INFO                        1
#define MFSLOG_NOTICE                      2
#define MFSLOG_WARNING                     3
#define MFSLOG_ERR                         4

#define MFSLOG_STRINGS \
	"debug", \
	"info", \
	"notice", \
	"warning", \
	"error"

#define SCLASS_CHG_ADMIN_ONLY              0x0001
#define SCLASS_CHG_LABELS_MODE             0x0002
#define SCLASS_CHG_CREATE_MASKS            0x0004
#define SCLASS_CHG_KEEP_MASKS              0x0008
#define SCLASS_CHG_ARCH_MASKS              0x0010
#define SCLASS_CHG_ARCH_DELAY              0x0020
#define SCLASS_CHG_TRASH_MASKS             0x0040
#define SCLASS_CHG_MIN_TRASHRETENTION      0x0080
#define SCLASS_CHG_ARCH_MODE               0x0100
#define SCLASS_CHG_ARCH_MIN_SIZE           0x0200
#define SCLASS_CHG_FORCE                   0x8000

#define SCLASS_ARCH_MODE_CTIME             0x01
#define SCLASS_ARCH_MODE_MTIME             0x02
#define SCLASS_ARCH_MODE_ATIME             0x04
#define SCLASS_ARCH_MODE_REVERSIBLE        0x08
#define SCLASS_ARCH_MODE_FAST              0x10
#define SCLASS_ARCH_MODE_CHUNK             0x20

#define SCLASS_EXPR_TYPE_MASK              0xC0
#define SCLASS_EXPR_VALUE_MASK             0x3F
#define SCLASS_EXPR_SYMBOL                 0xC0
#define SCLASS_EXPR_SYMBOL_ANY             0xFF
#define SCLASS_EXPR_OP_AND                 0x80
#define SCLASS_EXPR_OP_OR                  0x40
#define SCLASS_EXPR_OP_ONE                 0x00
#define SCLASS_EXPR_OP_NOT                 0x01

#define UNIQ_MASK_IP                       (1U<<(1+'Z'-'A'))
#define UNIQ_MASK_RACK                     (1U<<(2+'Z'-'A'))

#define LIST_UID_ANY                       UINT32_C(0xFFFFFFFF)

#define PATTERN_EUGID_ANY                  UINT32_C(0xFFFFFFFF)
#define PATTERN_OMASK_SCLASS               0x01
#define PATTERN_OMASK_TRASHRETENTION       0x02
#define PATTERN_OMASK_EATTR                0x04

/* type for readdir command */
#define DISP_TYPE_FILE                     'f'
#define DISP_TYPE_DIRECTORY                'd'
#define DISP_TYPE_SYMLINK                  'l'
#define DISP_TYPE_FIFO                     'q'
#define DISP_TYPE_BLOCKDEV                 'b'
#define DISP_TYPE_CHARDEV                  'c'
#define DISP_TYPE_SOCKET                   's'
// 't' and 'r' are only for internal master use - they are in readdir shown as 'f'
#define DISP_TYPE_TRASH                    't'
#define DISP_TYPE_SUSTAINED                'r'
#define DISP_TYPE_UNKNOWN                  '?'

#define DISP_TYPE_REMAP_STR                "?fdlqbcstr??????"

#define TYPE_FILE                          1
#define TYPE_DIRECTORY                     2
#define TYPE_SYMLINK                       3
#define TYPE_FIFO                          4
#define TYPE_BLOCKDEV                      5
#define TYPE_CHARDEV                       6
#define TYPE_SOCKET                        7
#define TYPE_TRASH                         8
#define TYPE_SUSTAINED                     9

// bits in 'winattr' field of attributes
#define WINATTR_READ_ONLY 1
#define WINATTR_HIDDEN 2
#define WINATTR_SYSTEM 4
#define WINATTR_ARCHIVE 8

// mode mask: "modemask" field in "CLTOMA_FUSE_ACCESS"
#define MODE_MASK_R                        4
#define MODE_MASK_W                        2
#define MODE_MASK_X                        1

// flags: "setmask" field in "CLTOMA_FUSE_SETATTR"
// SET_GOAL_FLAG,SET_DELETE_FLAG are no longer supported
// SET_LENGTH_FLAG,SET_OPENED_FLAG are deprecated
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETGOAL command
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETTRASH_TIMEOUT command
// instead of using FUSE_SETATTR with SET_LENGTH_FLAG/SET_OPENED_FLAG use FUSE_TRUNCATE command
//
// SET_CURRENTTIME_FLAG - version > 2.1.12

// lookup.lflags (acl's must be checked separatelly - A:r,B:w doesn't give user belonging to A and B both r and w rights)
#define LOOKUP_ACCESS_MODE_0               0x0001
#define LOOKUP_ACCESS_MODE_X               0x0002
#define LOOKUP_ACCESS_MODE_W               0x0004
#define LOOKUP_ACCESS_MODE_WX              0x0008
#define LOOKUP_ACCESS_MODE_R               0x0010
#define LOOKUP_ACCESS_MODE_RX              0x0020
#define LOOKUP_ACCESS_MODE_RW              0x0040
#define LOOKUP_ACCESS_MODE_RWX             0x0080
#define LOOKUP_ACCESS_MODES_IO             0x00FC
#define LOOKUP_ACCESS_MODES_RO             0xFF33
#define LOOKUP_ACCESS_BITS                 0x38FF
#define LOOKUP_CHUNK_ZERO_DATA             0x0100
#define LOOKUP_RO_FILESYSTEM               0x0200
#define LOOKUP_KEEPCACHE                   0x0400
#define LOOKUP_IMMUTABLE                   0x0800
#define LOOKUP_DIRECTMODE                  0x1000
#define LOOKUP_APPENDONLY                  0x2000

// combinations of MODE_MASK to LOOKUP_ACCESS_MODE
#define MODE_TO_ACCMODE {0x01,0x03,0x05,0x0F,0x11,0x33,0x55,0xFF}

#define SET_WINATTR_FLAG                   0x01
#define SET_MODE_FLAG                      0x02
#define SET_UID_FLAG                       0x04
#define SET_GID_FLAG                       0x08
#define SET_MTIME_NOW_FLAG                 0x10
#define SET_MTIME_FLAG                     0x20
#define SET_ATIME_FLAG                     0x40
#define SET_ATIME_NOW_FLAG                 0x80

#define MAX_EC_PARTS                       8

// rename.mode
#define MFS_RENAME_STD                     0
#define MFS_RENAME_EXCHANGE                1
#define MFS_RENAME_NOREPLACE               2

// check.type:
#define CHECK_VALID                        0
#define CHECK_MARKEDFORREMOVAL             1
#define CHECK_WRONGVERSION                 2
#define CHECK_WV_AND_MFR                   3
#define CHECK_INVALID                      4

// flock.cmd:
#define FLOCK_UNLOCK                       0
#define FLOCK_TRY_SHARED                   1
#define FLOCK_LOCK_SHARED                  2
#define FLOCK_TRY_EXCLUSIVE                3
#define FLOCK_LOCK_EXCLUSIVE               4
#define FLOCK_INTERRUPT                    5
#define FLOCK_RELEASE                      6

// posix_locks.cmd:
#define POSIX_LOCK_CMD_GET                 0
#define POSIX_LOCK_CMD_SET                 1
#define POSIX_LOCK_CMD_TRY                 2
#define POSIX_LOCK_CMD_INT                 3

// posix_locks.type:
#define POSIX_LOCK_UNLCK                   0
#define POSIX_LOCK_RDLCK                   1
#define POSIX_LOCK_WRLCK                   2

// dtypes:
#define DTYPE_UNKNOWN                      0
#define DTYPE_TRASH                        1
#define DTYPE_SUSTAINED                    2
#define DTYPE_ISVALID(x)                   (((uint32_t)(x))<=2)

// smode:
#define SMODE_SET                          0
#define SMODE_INCREASE                     1
#define SMODE_DECREASE                     2
#define SMODE_EXCHANGE                     3
#define SMODE_RSET                         4
#define SMODE_RINCREASE                    5
#define SMODE_RDECREASE                    6
#define SMODE_REXCHANGE                    7
#define SMODE_TMASK                        3
#define SMODE_RMASK                        4
#define SMODE_ISVALID(x)                   (((uint32_t)(x))<=7)

// gmode:
#define GMODE_NORMAL                       0
#define GMODE_RECURSIVE                    1
#define GMODE_ISVALID(x)                   (((uint32_t)(x))<=1)

// labels_mode:
// loose = use other labels when servers are overloaded or full
// std = use other labels when servers are full
// strict = never use other labels
#define LABELS_MODE_LOOSE                  0
#define LABELS_MODE_STD                    1
#define LABELS_MODE_STRICT                 2
#define LABELS_MODE_GLOBAL                 0xFF

// extraattr:

#define EATTR_BITS                         8

#define EATTR_NOOWNER                      0x01
#define EATTR_NOACACHE                     0x02
#define EATTR_NOECACHE                     0x04
#define EATTR_NODATACACHE                  0x08
#define EATTR_SNAPSHOT                     0x10
#define EATTR_UNDELETABLE                  0x20
#define EATTR_APPENDONLY                   0x40
#define EATTR_IMMUTABLE                    0x80

#define EATTR_STRINGS \
	"noowner", \
	"noattrcache", \
	"noentrycache", \
	"nodatacache", \
	"snapshot", \
	"undeletable", \
	"appendonly", \
	"immutable"

#define EATTR_DESCRIPTIONS \
	"every user (except root) sees object as his (her) own", \
	"prevent standard object attributes from being stored in kernel cache", \
	"prevent directory entries from being stored in kernel cache", \
	"prevent file data from being kept in kernel cache", \
	"node was created using makesnapshot command (or inside snapshot)", \
	"prevent unlinking", \
	"only appending to file and adding new nodes to directory is allowed", \
	"prevent any change to object"

// mode attr / attribute flags
#define MATTR_NOACACHE                     0x01
#define MATTR_NOECACHE                     0x02
/* MATTR_ALLOWDATACACHE is deprecated - moved to open/lookup flags */
#define MATTR_ALLOWDATACACHE               0x04
#define MATTR_NOXATTR                      0x08
/* MATTR_DIRECTMODE is deprecated - moved to open/lookup flags */
#define MATTR_DIRECTMODE                   0x10
#define MATTR_UNDELETABLE                  0x20

// quota:
#define QUOTA_FLAG_SINODES                 0x01
#define QUOTA_FLAG_SLENGTH                 0x02
#define QUOTA_FLAG_SSIZE                   0x04
#define QUOTA_FLAG_SREALSIZE               0x08
#define QUOTA_FLAG_SALL                    0x0F
#define QUOTA_FLAG_HINODES                 0x10
#define QUOTA_FLAG_HLENGTH                 0x20
#define QUOTA_FLAG_HSIZE                   0x40
#define QUOTA_FLAG_HREALSIZE               0x80
#define QUOTA_FLAG_HALL                    0xF0
#define QUOTA_PERIOD_DEFAULT               0x0
#define QUOTA_PERIOD_DONT_CHANGE           UINT32_C(0xFFFFFFFF)

// append slice
#define APPEND_SLICE_FROM_NEG              0x01
#define APPEND_SLICE_TO_NEG                0x02

// acl:
#define POSIX_ACL_NONE                     0
#define POSIX_ACL_ACCESS                   1
#define POSIX_ACL_DEFAULT                  2

// archctl:
#define ARCHCTL_CLR                        0
#define ARCHCTL_SET                        1
#define ARCHCTL_GET                        2

// getdir:
#define GETDIR_FLAG_WITHATTR               0x01
#define GETDIR_FLAG_ADDTOCACHE             0x02

// truncate:
#define TRUNCATE_FLAG_OPENED               0x01
#define TRUNCATE_FLAG_UPDATE               0x02
#define TRUNCATE_FLAG_TIMEFIX              0x04
#define TRUNCATE_FLAG_RESERVE              0x08

// register sesflags:
#define SESFLAG_READONLY                   0x01	// meaning is obvious
#define SESFLAG_DYNAMICIP                  0x02	// sessionid can be used by any IP - dangerous for high privileged sessions - one could connect from different computer using stolen session id
#define SESFLAG_IGNOREGID                  0x04	// gid is ignored during access testing (when user id is different from object's uid then or'ed 'group' and 'other' rights are used)
#define SESFLAG_ADMIN                      0x08	// extra permissions (currently only used for quotas - quota can be set and deleted)
#define SESFLAG_MAPALL                     0x10	// all users (except root) are mapped to specific uid and gid

#define SESFLAG_ATTRBIT                    0x40	// client can understand new attr record (ver >= 1.7.29)

#define SESFLAG_METARESTORE                0x80	// this is metarestore session

#define SESFLAG_POS_STRINGS \
	"read-only", \
	"not_restricted_ip", \
	"ignore_gid", \
	"admin", \
	"map_all", \
	"undefined_flag_5", \
	"reserved (attr bit)", \
	"reserved (metarestore)"

#define SESFLAG_NEG_STRINGS \
	"read-write", \
	"restricted_ip", \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL

// sugicclearmode in fs_setattr
#define SUGID_CLEAR_MODE_NEVER             0
#define SUGID_CLEAR_MODE_ALWAYS            1
#define SUGID_CLEAR_MODE_OSX               2
#define SUGID_CLEAR_MODE_BSD               3
#define SUGID_CLEAR_MODE_EXT               4
#define SUGID_CLEAR_MODE_XFS               5

#define SUGID_CLEAR_MODE_OPTIONS           6

#define SUGID_CLEAR_MODE_STRINGS \
	"never", \
	"always", \
	"osx", \
	"bsd", \
	"ext", \
	"xfs"

// snapshot mode: "smode" field in "CLTOMA_FUSE_SNAPSHOT"
#define SNAPSHOT_MODE_CAN_OVERWRITE        0x01
#define SNAPSHOT_MODE_CPLIKE_ATTR          0x02
#define SNAPSHOT_MODE_FORCE_REMOVAL        0x04
#define SNAPSHOT_MODE_PRESERVE_HARDLINKS   0x08
#define SNAPSHOT_MODE_DELETE               0x80

// OK,OVERLOADED and REBALANCE are used as hlstatus field in "CSTOMA_CURRENT_LOAD", others only internally in master
#define HLSTATUS_DEFAULT                   0
#define HLSTATUS_OK                        1
#define HLSTATUS_OVERLOADED                2
#define HLSTATUS_LSREBALANCE               3
#define HLSTATUS_GRACEFUL                  4
#define HLSTATUS_HSREBALANCE               5

// "flags" fileld in "CLTOMA_FUSE_OPEN"
#define OPEN_READ                          0x01
#define OPEN_WRITE                         0x02
#define OPEN_AFTER_CREATE                  0x04
#define OPEN_TRUNCATE                      0x08
#define OPEN_CACHE_CLEARED                 0x10

// "flags" field in "MATOCL_FUSE_OPEN/MATOCL_FUSE_CREATE"
#define OPEN_KEEPCACHE                     0x01
#define OPEN_DIRECTMODE                    0x02
#define OPEN_APPENDONLY                    0x04

#define MFS_XATTR_CREATE_OR_REPLACE        0
#define MFS_XATTR_CREATE_ONLY              1
#define MFS_XATTR_REPLACE_ONLY             2
#define MFS_XATTR_REMOVE                   3

#define MFS_XATTR_GETA_DATA                0
#define MFS_XATTR_LENGTH_ONLY              1

#define MFS_CSSERV_COMMAND_REMOVE          0
#define MFS_CSSERV_COMMAND_BACKTOWORK      1
#define MFS_CSSERV_COMMAND_MAINTENANCEON   2
#define MFS_CSSERV_COMMAND_MAINTENANCEOFF  3
#define MFS_CSSERV_COMMAND_TMPREMOVE       4

#define MFS_SESSION_COMMAND_REMOVE         0

// MFS uses Linux limits
#define MFS_XATTR_NAME_MAX                 255
#define MFS_XATTR_SIZE_MAX                 65536
#define MFS_XATTR_LIST_MAX                 65536

// CLTOMA_FULL_DIRECTORY_DATA.flags
#define FULL_DIRECTORY_ADD_CHUNKID         1
#define FULL_DIRECTORY_ADD_SYMLINK         2
#define FULL_DIRECTORY_ADD_XATTR           4
#define FULL_DIRECTORY_ADD_FACL            8
#define FULL_DIRECTORY_ADD_EATTR           16

// CLTOMA_SET_ALL_NODE_ATTRIBUTES.flags
#define SET_ALL_WINATTR                    1
#define SET_ALL_EATTR                      2
#define SET_ALL_XATTR                      4
#define SET_ALL_FACL                       8

// CLTOMA_FUSE_READ_CHUNK,CLTOMA_FUSE_WRITE_CHUNK,CLTOMA_FUSE_WRITE_CHUNK_END - chunkopflags
#define CHUNKOPFLAG_CANMODTIME             1
#define CHUNKOPFLAG_CONTINUEOP             2
#define CHUNKOPFLAG_CANUSERESERVESPACE     4

#define MODULE_TYPE_UNKNOWN                0
#define MODULE_TYPE_MASTER                 1
#define MODULE_TYPE_CHUNKSERVER            2

// atime mode
#define ATIME_ALWAYS                       0
#define ATIME_FILES_ONLY                   1
#define ATIME_RELATIVE_ONLY                2
#define ATIME_FILES_AND_RELATIVE_ONLY      3
#define ATIME_NEVER                        4


// current_load.transferingchunks
#define TRANSFERING_LOST_CHUNKS            1
#define TRANSFERING_NEW_CHUNKS             2

// chunk_info.requestedinfo
#define REQUEST_BLOCKS                     1
#define REQUEST_CHECKSUM                   2
#define REQUEST_CHECKSUM_TAB               4
#define REQUEST_FILE_PATH                  8

// wflags
#define WFLAG_INVALIDATE_CACHE             1

#define ATTR_RECORD_SIZE                   36

#define DISABLE_BIT_CHOWN                  0
#define DISABLE_BIT_CHMOD                  1
#define DISABLE_BIT_SYMLINK                2
#define DISABLE_BIT_MKFIFO                 3
#define DISABLE_BIT_MKDEV                  4
#define DISABLE_BIT_MKSOCK                 5
#define DISABLE_BIT_MKDIR                  6
#define DISABLE_BIT_UNLINK                 7
#define DISABLE_BIT_RMDIR                  8
#define DISABLE_BIT_RENAME                 9
#define DISABLE_BIT_MOVE                   10
#define DISABLE_BIT_LINK                   11
#define DISABLE_BIT_CREATE                 12
#define DISABLE_BIT_READDIR                13
#define DISABLE_BIT_READ                   14
#define DISABLE_BIT_WRITE                  15
#define DISABLE_BIT_TRUNCATE               16
#define DISABLE_BIT_SETLENGTH              17
#define DISABLE_BIT_APPENDCHUNKS           18
#define DISABLE_BIT_SNAPSHOT               19
#define DISABLE_BIT_SETTRASH               20
#define DISABLE_BIT_SETSCLASS              21
#define DISABLE_BIT_SETEATTR               22
#define DISABLE_BIT_SETXATTR               23
#define DISABLE_BIT_SETFACL                24

#define DISABLE_CHOWN                      (UINT32_C(1)<<DISABLE_BIT_CHOWN)
#define DISABLE_CHMOD                      (UINT32_C(1)<<DISABLE_BIT_CHMOD)
#define DISABLE_SYMLINK                    (UINT32_C(1)<<DISABLE_BIT_SYMLINK)
#define DISABLE_MKFIFO                     (UINT32_C(1)<<DISABLE_BIT_MKFIFO)
#define DISABLE_MKDEV                      (UINT32_C(1)<<DISABLE_BIT_MKDEV)
#define DISABLE_MKSOCK                     (UINT32_C(1)<<DISABLE_BIT_MKSOCK)
#define DISABLE_MKDIR                      (UINT32_C(1)<<DISABLE_BIT_MKDIR)
#define DISABLE_UNLINK                     (UINT32_C(1)<<DISABLE_BIT_UNLINK)
#define DISABLE_RMDIR                      (UINT32_C(1)<<DISABLE_BIT_RMDIR)
#define DISABLE_RENAME                     (UINT32_C(1)<<DISABLE_BIT_RENAME)
#define DISABLE_MOVE                       (UINT32_C(1)<<DISABLE_BIT_MOVE)
#define DISABLE_LINK                       (UINT32_C(1)<<DISABLE_BIT_LINK)
#define DISABLE_CREATE                     (UINT32_C(1)<<DISABLE_BIT_CREATE)
#define DISABLE_READDIR                    (UINT32_C(1)<<DISABLE_BIT_READDIR)
#define DISABLE_READ                       (UINT32_C(1)<<DISABLE_BIT_READ)
#define DISABLE_WRITE                      (UINT32_C(1)<<DISABLE_BIT_WRITE)
#define DISABLE_TRUNCATE                   (UINT32_C(1)<<DISABLE_BIT_TRUNCATE)
#define DISABLE_SETLENGTH                  (UINT32_C(1)<<DISABLE_BIT_SETLENGTH)
#define DISABLE_APPENDCHUNKS               (UINT32_C(1)<<DISABLE_BIT_APPENDCHUNKS)
#define DISABLE_SNAPSHOT                   (UINT32_C(1)<<DISABLE_BIT_SNAPSHOT)
#define DISABLE_SETTRASH                   (UINT32_C(1)<<DISABLE_BIT_SETTRASH)
#define DISABLE_SETSCLASS                  (UINT32_C(1)<<DISABLE_BIT_SETSCLASS)
#define DISABLE_SETEATTR                   (UINT32_C(1)<<DISABLE_BIT_SETEATTR)
#define DISABLE_SETXATTR                   (UINT32_C(1)<<DISABLE_BIT_SETXATTR)
#define DISABLE_SETFACL                    (UINT32_C(1)<<DISABLE_BIT_SETFACL)

#define DISABLE_STRINGS \
	"chown", \
	"chmod", \
	"symlink", \
	"mkfifo", \
	"mkdev", \
	"mksock", \
	"mkdir", \
	"unlink", \
	"rmdir", \
	"rename", \
	"move", \
	"link", \
	"create", \
	"readdir", \
	"read", \
	"write", \
	"truncate", \
	"setlength", \
	"appendchunks", \
	"snapshot", \
	"settrash", \
	"setsclass", \
	"seteattr", \
	"setxattr", \
	"setfacl", \
	NULL

#define CSTOMA_MAXPACKETSIZE 500000000
#define CLTOMA_MAXPACKETSIZE 50000000
#define ANTOMA_MAXPACKETSIZE 1500000
#define MATOAN_MAXPACKETSIZE 1500000
#define MATOCS_MAXPACKETSIZE 10000
#define CSTOCS_MAXPACKETSIZE 100000
#define CLTOCS_MAXPACKETSIZE 100000
#define ANTOCS_MAXPACKETSIZE 100000
#define CSTOCL_MAXPACKETSIZE 100000

// ANY <-> ANY

#define ANTOAN_NOP 0
// [ msgid:32 ] (msgid - only in communication from master to client)

// these packets are acceptable since version 1.6.27 (but not send)
#define ANTOAN_UNKNOWN_COMMAND 1
// [ msgid:32 ] cmdno:32 size:32 version:32 (msgid - only in communication from master to client)

#define ANTOAN_BAD_COMMAND_SIZE 2
// [ msgid:32 ] cmdno:32 size:32 version:32 (msgid - only in communication from master to client)

#define ANTOAN_FORCE_TIMEOUT 5
// [ msgid:32 ] timeout_in_seconds:16 (intr. in version 4.12.0)

#define ANTOAN_GET_VERSION 10
// [ msgid:32 ]

#define ANTOAN_VERSION 11
// [ msgid:32 ] version:32 strversion:string ( N*[ char:8 ] )



// INTERNAL PROXY PACKETS

// packet conversion (encapsulation)
// N:32 (leng) C:32 (command) [N bytes] (data) <-> (N+8):32 INTERN_PACKET:32 ID:32 (connection id) C:32 (command) [N bytes] (data)
// ID: 0xCCIIIIII - 0xCC - simple count, 0xIIIIII - tab index

// PROXY -> CORE
#define INTERN_CONNECT 20
// connid:32 ip:32

// PROXY <-> CORE
#define INTERN_DISCONNECT 21
// connid:32

// PROXY <-> CORE
#define INTERN_PACKET 22
// connid:32 command:32 data:....

// CORE -> PROXY
#define INTERN_CONNFLAGS 23
// connid:32 flags:32

// CORE -> PROXY
#define INTERN_FINISH 24
// -

// CORE -> PROXY
#define INTERN_RELOAD 25
// -


// METALOGGERS/MASTERS/MANAGERS <-> MASTER


// 0x0032
#define ANTOMA_REGISTER (PROTO_BASE+50)
// rver:8
// 	rver==1:
// 		( rver:8 ) version:32 timeout:16

// 0x0033
#define MATOAN_METACHANGES_LOG (PROTO_BASE+51)
// maxsize=250000
// 0xFF:8 version:64 logdata:string ( N*[ char:8 ] ) = LOG_DATA
// 0xAA:8 version:64 logdata:string ( N*[ char:8 ] ) = LOG_DATA with ack (intr. in version 3.0.10)
// 0x55:8 = LOG_ROTATE

// 0x0034
#define MATOAN_MASTER_ACK (PROTO_BASE+52)
// atype:8

// 0x0035
#define MATOAN_STATE (PROTO_BASE+53)
// signature1:32 signature2:32 signature3:32 metaversion:64 metaid:64 utimestamp:64 tmp:32

// 0x0038
#define ANTOMA_FORCE_METADATA_SAVE (PROTO_BASE+56)
// - (since 4.4.0)

// 0x0039
#define MATOMA_METADATA_CHECKSUM (PROTO_BASE+57)
// metaversion:64 metaid:64 N * [ section:32 checksum:32 ] (since 4.4.0)

// 0x003A
#define MATOMA_FORCE_DESYNC (PROTO_BASE+58)
// - (since 4.4.0)


// 0x003C
#define ANTOMA_DOWNLOAD_START (PROTO_BASE+60)
// -
// filenum:8

// 0x003D
#define MATOAN_DOWNLOAD_INFO (PROTO_BASE+61)
// status:8
// length:64

// 0x003E
#define ANTOMA_DOWNLOAD_REQUEST (PROTO_BASE+62)
// offset:64 leng:32

// 0x003F
#define MATOAN_DOWNLOAD_DATA (PROTO_BASE+63)
// maxsize=2000000
// offset:64 leng:32 crc:32 data:lengB

// 0x0040
#define ANTOMA_DOWNLOAD_END (PROTO_BASE+64)
// -

// 0x0041
#define ANTOMA_STORE_METADATA (PROTO_BASE+65)
// -

// 0x0047
#define ANTOMA_SYSLOG (PROTO_BASE+71)
// priority:8 msgsize:16 message:msgsizeB


// 0x0050
#define ANTOAN_GET_CONFIG (PROTO_BASE+80)
// msgid:32 option_name:NAME

// 0x0051
#define ANTOAN_CONFIG_VALUE (PROTO_BASE+81)
// msgid:32 [option_name:NAME] option_value:NAME
// option name exists when msgid==0

// 0x0052
#define ANTOAN_GET_CONFIG_FILE (PROTO_BASE+82)
// msgid:32 option_name:NAME

// 0x0053
#define ANTOAN_CONFIG_FILE_CONTENT (PROTO_BASE+83)
// msgid:32 [option_name:NAME] size:16 data:sizeB
// option name exists when msgid==0

// 0x0054
#define ANTOAN_GET_CONFIG_FILE_MD5 (PROTO_BASE+84)
// msgid:32 txtmode:8 option_name:NAME
// txtmode == 1 - parse file pointed by option as a text file - skip empty lines and comments (starting with '#')

// 0x0055
#define ANTOAN_CONFIG_FILE_MD5 (PROTO_BASE+85)
// msgid:32 [option_name:NAME] md5_digest:16B
// option name exists when msgid==0



// CHUNKSERVER <-> MASTER

// 0x0060
#define CSTOMA_CHUNK_DOESNT_EXIST (PROTO_BASE+96)
// N * [ chunkid:64 ]

// 0x0061
#define MATOCS_CHUNK_STATUS (PROTO_BASE+97)
// chunkid:64

// 0x0062
#define CSTOMA_CHUNK_STATUS (PROTO_BASE+98)
// N * [ ecid:8 status:8 version:32 ]

// 0x0063
#define MATOCS_REGISTER_FIRST (PROTO_BASE+99)
// chunkid:64

// 0x0064
#define CSTOMA_REGISTER (PROTO_BASE+100)
// - version 0:
// myip:32 myport:16 usedspace:64 totalspace:64 N*[ chunkid:64 version:32 ]
// - version 1-4:
// rver:8
// 	rver==1:
// 		( rver:8 ) myip:32 myport:16 usedspace:64 totalspace:64 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// 	rver==2:
// 		( rver:8 ) myip:32 myport:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// 	rver==3:
// 		( rver:8 ) myip:32 myport:16 tpctimeout:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// 	rver==4:
// 		( rver:8 ) version:32 myip:32 myport:16 tcptimeout:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// - version 5:
//	rver==50:	// version 5 / BEGIN
//		( rver:8 ) version:32 myip:32 myport:16 tcptimeout:16
//	rver==51:	// version 5 / CHUNKS
//		( rver:8 ) N*[chunkid:64 version:32]
//	rver==52:	// version 5 / END
//		( rver:8 ) usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32
// - version 6:
//	rver==60:	// version 6 / BEGIN
//		( rver:8 ) [ passcode:16B ] version:32 myip:32 myport:16 tcptimeout:16 csid:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32
//	rver==61:	// version 6 / CHUNKS
//		( rver:8 ) N*[chunkid:64 version:32]
//	rver==62:	// version 6 / END
//		( rver:8 ) -
//	rver==63:	// version 6 / DISCONNECT


// 0x0065
#define CSTOMA_SPACE (PROTO_BASE+101)
// usedspace:64 totalspace:64
// usedspace:64 totalspace:64 tdusedspace:64 tdtotalspace:64
// usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32

// 0x0066
#define CSTOMA_CHUNK_DAMAGED (PROTO_BASE+102)
// N * [chunkid:64]

// 0x0067
// #define MATOCS_STRUCTURE_LOG (PROTO_BASE+103)
// version:32 logdata:string ( N*[ char:8 ] )
// 0xFF:8 version:64 logdata:string ( N*[ char:8 ] )
// since version 1.6.28:
#define CSTOMA_CURRENT_LOAD (PROTO_BASE+103)
// load:32 - (version < 3.0.7)
// load:32 hlstatus:8 - (version >= 3.0.7 && version < 4.32.0)
// load:32 hlstatus:8 transferingchunks:8 - (version >= 4.32.0)

// 0x0068
// #define MATOCS_STRUCTURE_LOG_ROTATE (PROTO_BASE+104)
// since version 1.6.28:
#define MATOCS_MASTER_ACK (PROTO_BASE+104)
// atype:8 master_version:32
// atype:8 master_version:32 tcptimeout:16 csid:16
// atype:8 master_version:32 tcptimeout:16 csid:16 metadataid:64 (both versions >= 2.0.33)

// 0x0069
#define CSTOMA_CHUNK_LOST (PROTO_BASE+105)
// N * [ chunkid:64 ]

// 0x006A
#define CSTOMA_ERROR_OCCURRED (PROTO_BASE+106)
// -

// 0x006B
#define CSTOMA_CHUNK_NEW (PROTO_BASE+107)
// N * [ chunkid:64 version:32 ]


// 0x006D
#define CSTOMA_LABELS (PROTO_BASE+109)
// labelmask:32

// 0x006E
#define MATOCS_CREATE (PROTO_BASE+110)
// chunkid:64 version:32

// 0x006F
#define CSTOMA_CREATE (PROTO_BASE+111)
// chunkid:64 status:8

// 0x0078
#define MATOCS_DELETE (PROTO_BASE+120)
// chunkid:64 version:32

// 0x0079
#define CSTOMA_DELETE (PROTO_BASE+121)
// chunkid:64 status:8

// 0x0082
#define MATOCS_DUPLICATE (PROTO_BASE+130)
// chunkid:64 version:32 oldchunkid:64 oldversion:32

// 0x0083
#define CSTOMA_DUPLICATE (PROTO_BASE+131)
// chunkid:64 status:8

// 0x008C
#define MATOCS_SET_VERSION (PROTO_BASE+140)
// chunkid:64 version:32 oldversion:32

// 0x008D
#define CSTOMA_SET_VERSION (PROTO_BASE+141)
// chunkid:64 status:8

// 0x0096
#define MATOCS_REPLICATE (PROTO_BASE+150)
// chunkid:64 version:32 ip:32 port:16

// 0x0097
#define CSTOMA_REPLICATE (PROTO_BASE+151)
// chunkid:64 version:32 status:8

// 0x0098
#define MATOCS_CHUNKOP (PROTO_BASE+152)
// all chunk operations
// newversion>0 && length==0xFFFFFFFF && copychunkid==0              -> change version
// newversion>0 && length==0xFFFFFFFF && copychunkid>0               -> duplicate
// newversion>0 && length&0x80000000 -> split
// newversion>0 && length>=0 && length<=MFSCHUNKSIZE && copychunkid==0  -> truncate
// newversion>0 && length>=0 && length<=MFSCHUNKSIZE && copychunkid>0   -> duplicate and truncate
// newversion==0 && length==0                                        -> delete
// newversion==0 && length==1                                        -> create
// newversion==0 && length==2                                        -> test
// chunkid:64 version:32 newversion:32 copychunkid:64 copyversion:32 length:32

// 0x0099
#define CSTOMA_CHUNKOP (PROTO_BASE+153)
// chunkid:64 version:32 newversion:32 copychunkid:64 copyversion:32 length:32 status:8

// 0x009A
#define MATOCS_REPLICATE_SPLIT (PROTO_BASE+154)
// chunkid:64 version:32 ip:32 port:16 srcchunkid:64 partno:8 parts:8

// 0x009B
#define CSTOMA_REPLICATE_SPLIT (PROTO_BASE+155)
// chunkid:64 version:32 status:8

// 0x009C
#define MATOCS_REPLICATE_RECOVER (PROTO_BASE+156)
// chunkid:64 version:32 matrix:4*32 parts:8 parts * [ip:32 port:16 srcchunkid:64]

// 0x009D
#define CSTOMA_REPLICATE_RECOVER (PROTO_BASE+157)
// chunkid:64 version:32 status:8

// 0x009E
#define MATOCS_REPLICATE_JOIN (PROTO_BASE+158)
// chunkid:64 version:32 parts:8 parts * [ip:32 port:16 srcchunkid:64]

// 0x009F
#define CSTOMA_REPLICATE_JOIN (PROTO_BASE+159)
// chunkid:64 version:32 status:8

// 0x00A0
#define MATOCS_TRUNCATE (PROTO_BASE+160)
// chunkid:64 length:32 version:32 oldversion:32

// 0x00A1
#define CSTOMA_TRUNCATE (PROTO_BASE+161)
// chunkid:64 status:8

// 0x00AA
#define MATOCS_DUPTRUNC (PROTO_BASE+170)
// chunkid:64 version:32 oldchunkid:64 oldversion:32 length:32

// 0x00AB
#define CSTOMA_DUPTRUNC (PROTO_BASE+171)
// chunkid:64 status:8

// 0x00B4
#define MATOCS_LOCALSPLIT (PROTO_BASE+180)
// chunkid:64 version:32 missingmask:32

// 0x00B5
#define CSTOMA_LOCALSPLIT (PROTO_BASE+181)
// chunkid:64 version:32 status:8




// CHUNKSERVER <-> CLIENT/CHUNKSERVER

// 0x00C8
#define CLTOCS_READ (PROTO_BASE+200)
// chunkid:64 version:32 offset:32 size:32
// protocolid:8 chunkid:64 version:32 offset:32 size:32 (both versions >= 1.7.32)

// 0x00C9
#define CSTOCL_READ_STATUS (PROTO_BASE+201)
// chunkid:64 status:8

// 0x00CA
#define CSTOCL_READ_DATA (PROTO_BASE+202)
// chunkid:64 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

// 0x00D2
#define CLTOCS_WRITE (PROTO_BASE+210)
// chunkid:64 version:32 N*[ ip:32 port:16 ]
// protocolid:8 chunkid:64 version:32 N*[ ip:32 port:16 ] (both versions >= 1.7.32)

// 0x00D3
#define CSTOCL_WRITE_STATUS (PROTO_BASE+211)
// chunkid:64 writeid:32 status:8

// 0x00D4
#define CLTOCS_WRITE_DATA (PROTO_BASE+212)
// chunkid:64 writeid:32 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

// 0x00D5
#define CLTOCS_WRITE_FINISH (PROTO_BASE+213)
// chunkid:64 version:32

//CHUNKSERVER <-> CHUNKSERVER

// 0x00FA
#define ANTOCS_GET_CHUNK_BLOCKS (PROTO_BASE+250)
// chunkid:64 version:32

// 0x00FB
#define CSTOAN_CHUNK_BLOCKS (PROTO_BASE+251)
// chunkid:64 version:32 blocks:16 status:8

//ANY <-> CHUNKSERVER

// 0x012C
#define ANTOCS_GET_CHUNK_CHECKSUM (PROTO_BASE+300)
// chunkid:64 version:32

// 0x012D
#define CSTOAN_CHUNK_CHECKSUM (PROTO_BASE+301)
// chunkid:64 version:32 checksum:32
// chunkid:64 version:32 status:8

// 0x012E
#define ANTOCS_GET_CHUNK_CHECKSUM_TAB (PROTO_BASE+302)
// chunkid:64 version:32

// 0x012F
#define CSTOAN_CHUNK_CHECKSUM_TAB (PROTO_BASE+303)
// maxsize=4108
// chunkid:64 version:32 1024*[checksum:32]
// chunkid:64 version:32 status:8

#define ANTOCS_GET_CHUNK_INFO (PROTO_BASE+304)
// chunkid:64 version:32 requestedinfo:8

#define CSTOAN_CHUNK_INFO (PROTO_BASE+305)
// chunkid:64 version:32 status:8
// chunkid:64 version:32 [blocks:16] [checksum:32] [1024*[checksum:32] [pleng:32 path:plengB]

#define ANTOCS_CLEAR_ERRORS (PROTO_BASE+306)
// pleng:32 path:plengB

#define CSTOAN_CLEAR_ERRORS (PROTO_BASE+307)
// cleared_folders:8

// CLIENT <-> MASTER

// Storage Class

// sclass packet changes:
// 0: first approach with labels defined as and/or groups (in list - lists only names)
// 1: in create/change same as 2, in list same as 0 in create/change
// 2: added trash, labels defined as expressions, arch_delay changed from days to hours, added EC support
// 3: added 'arch_mode' (MFS 4.2.0)
// 4: added 'arch_min_size' (MFS 4.34.0)
// 5: added 'label_mode' override for CREATE,KEEP,ARCH and TRASH (MFS 4.53.0)

#define CLTOMA_SCLASS_CREATE (PROTO_BASE+350)
// msgid:32 storage_class_name:NAME fver:8
// fver==0: (arch_delay in days)
//	admin_only:8 labels_mode:8 arch_delay:16 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 create_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] keep_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] arch_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ]
// fver==1,2: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr:SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==3: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr:SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==4: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr:SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==5: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_mode:8 keep_labels_mode:8 arch_labels_mode:8 trash_labels_mode:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr:SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]

#define MATOCL_SCLASS_CREATE (PROTO_BASE+351)
// msgid:32 status:8

#define CLTOMA_SCLASS_CHANGE (PROTO_BASE+352)
// msgid:32 storage_class_name:NAME fver:8
// fver==0: (arch_delay in days)
//	chgmask:16 admin_only:8 labels_mode:8 arch_delay:16 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 create_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] keep_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] arch_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ]
// fver==1,2: (arch_delay in hours)
//	chgmask:16 admin_only:8 labels_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==3: (arch_delay in hours)
//	chgmask:16 admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==4: (arch_delay in hours)
//	chgmask:16 admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==5: (arch_delay in hours)
//	chgmask:16 admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_mode:8 keep_labels_mode:8 arch_labels_mode:8 trash_labels_mode:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]

#define MATOCL_SCLASS_CHANGE (PROTO_BASE+353)
// msgid:32 status:8
// msgid:32 fver:8
// fver==0: (arch_delay in days !!!)
//	admin_only:8 labels_mode:8 arch_delay:16 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 create_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] keep_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] arch_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ]
// fver==1,2: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==3: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==4: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]
// fver==5: (arch_delay in hours)
//	admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_mode:8 keep_labels_mode:8 arch_labels_mode:8 trash_labels_mode:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ]

#define CLTOMA_SCLASS_DELETE (PROTO_BASE+354)
// msgid:32 storage_class_name:NAME

#define MATOCL_SCLASS_DELETE (PROTO_BASE+355)
// msgid:32 status:8

#define CLTOMA_SCLASS_DUPLICATE (PROTO_BASE+356)
// msgid:32 storage_class_oldname:NAME storage_class_newname:NAME

#define MATOCL_SCLASS_DUPLICATE (PROTO_BASE+357)
// msgid:32 status:8

#define CLTOMA_SCLASS_RENAME (PROTO_BASE+358)
// msgid:32 storage_class_oldname:NAME storage_class_newname:NAME

#define MATOCL_SCLASS_RENAME (PROTO_BASE+359)
// msgid:32 status:8

#define CLTOMA_SCLASS_LIST (PROTO_BASE+360)
// msgid:32 fver:8

#define MATOCL_SCLASS_LIST (PROTO_BASE+361)
// msgid:32
// fver==0:
//	N * [ storage_class_name:NAME ]
// (fver!=0 && masterversion < 4.0) || (fver==1 && masterversion >= 4.0):
//	N * [ storage_class_name:NAME admin_only:8 labels_mode:8 arch_delay:16 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 create_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] keep_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] arch_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] ]
// fver==2 && masterversion >= 4.0:
//	N * [ storage_class_name:NAME admin_only:8 labels_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] ]
// fver==3 && masterversion >= 4.2.0:
//	N * [ storage_class_name:NAME admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] ]
// fver==4 && masterversion >= 4.34.2:
//	N * [ classid:8 storage_class_name:NAME admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] ]
// fver==5 && masterversion >= 4.53.0:
//	N * [ classid:8 storage_class_name:NAME admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16 arch_redundancy_level:8 trash_redundancy_level:8 create_labels_mode:8 keep_labels_mode:8 arch_labels_mode:8 trash_labels_mode:8 create_labels_uniqmask:32 keep_labels_uniqmask:32 arch_labels_uniqmask:32 trash_labels_uniqmask:32 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 trash_labelscnt:8 create_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] keep_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] arch_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] trash_labelscnt * [ labelexpr: SCLASS_EXPR_MAX_SIZE bytes ] ]
// fvar==0xFF && masterversion >= 4.34.2
//	N * [ classid:8 storage_class_name:NAME ]



// GLOB patterns

// (glob,euid,egid) -> (sclass,trashretention,eattr) with given priority

#define CLTOMA_PATTERN_ADD (PROTO_BASE+370)
// msgid:32 fver:8
// fver==0:
//	glob:NAME euid:32 egid:32 priority:8 omask:8 sclass_name:NAME trashretention:16 seteattr:8 clreattr:8

#define MATOCL_PATTERN_ADD (PROTO_BASE+371)
// msgid:32 status:8

#define CLTOMA_PATTERN_DELETE (PROTO_BASE+372)
// msgid:32 fver:8
// fver==0:
//	glob:NAME euid:32 egid:32

#define MATOCL_PATTERN_DELETE (PROTO_BASE+373)
// msgid:32 status:8

#define CLTOMA_PATTERN_LIST (PROTO_BASE+374)
// msgid:32 fver:8
// fver==0:
//	-

#define MATOCL_PATTERN_LIST (PROTO_BASE+375)
// msgid:32
// fver==0:
//	N * [ glob:NAME euid:32 egid:32 priority:8 omask:8 sclass_name:NAME trashretention:16 seteattr:8 clreattr:8 ]


// TRASH tools (min version 4.6.0)
#define CLTOMA_TRASH_LIST (PROTO_BASE+380)
// msgid:32 partno:8 format:8 uid:32 mints:32 maxts:32 (version <= 4.19)
// msgid:32 partno:8 format:8 uid:32 mints:32 maxts:32 glob:NAME (version >= 4.20)

#define MATOCL_TRASH_LIST (PROTO_BASE+381)
// msgid:32 status:8 - in case of error
// msgid:32 totalcount:32 N * [ inode:32 pleng:32 path:plengB ] - format = 0 (version >= 4.6 && version <= 4.14)
// msgid:32 totalcount:32 N * [ inode:32 attr:ATTR trashretention:16 pleng:32 path:plengB ] - format = 1 (version >= 4.6 && version <= 4.14)
// msgid:32 totalcount:32 userfiles:32 matchedfiles:32 matchedfiles * [ inode:32 pleng:32 path:plengB ] - format = 0 (version >= 4.15)
// msgid:32 totalcount:32 userfiles:32 matchedfiles:32 matchedfiles * [ inode:32 attr:ATTR trashretention:16 pleng:32 path:plengB ] - format = 1 (version >= 4.15)

#define CLTOMA_TRASH_RECOVER (PROTO_BASE+382)
// msgid:32 inode:32 path_root:32 pleng:32 path:plengB umask:16 uid:32 gcnt:32 gcnt * [ gid:32 ] copysgid:8

#define MATOCL_TRASH_RECOVER (PROTO_BASE+383)
// msgid:32 status:8
// msgid:32 pleng:32 path:plengB - only when the path has been changed

#define CLTOMA_TRASH_REMOVE (PROTO_BASE+384)
// msgid:32 inode:32 uid:32

#define MATOCL_TRASH_REMOVE (PROTO_BASE+385)
// msgid:32 status:8

#define CLTOMA_SUSTAINED_LIST (PROTO_BASE+388)
// msgid:32 partno:8 uid:32 (version <= 4.19)
// msgid:32 partno:8 uid:32 glob:NAME (version >= 4.20)

#define MATOCL_SUSTAINED_LIST (PROTO_BASE+389)
// msgid:32 status:8 - in case of error
// msgid:32 totalcount:32 N * [ inode:32 sessionscount:32 sessionscount * [ sessionid:32 ] pleng:32 path:plengB ] (version >= 4.6 && version <= 4.14)
// msgid:32 totalcount:32 matchedfiles:32 matchedfiles * [ inode:32 sessionscount:32 sessionscount * [ sessionid:32 ] pleng:32 path:plengB ] (version >= 4.15)



// libmfsio extra packets
#define CLTOMA_PATH_LOOKUP (PROTO_BASE+390)
// msgid:32 base_inode:32 length:32 path:lengthB uid:32 gcnt:32 gcnt * [ gid:32 ]

#define MATOCL_PATH_LOOKUP (PROTO_BASE+391)
// msgid:32 status:8
// msgid:32 parent_inode:32 last_name:NAME last_inode:32 attr:ATTR

// Fuse

// attr record (deprecated - versions < 1.7.32)
//   type:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 nlink:32 length:64
//   total: 35B
//
//   mode: FFFFMMMMMMMMMMMM
//         \--/\----------/
//           \       \------- mode
//            \-------------- flags
//
// attr record (1.7.32 to 3.0.92) - 35B
//
//   flags:8 type:4 mode:12 uid:32 gid:32 atime:32 mtime:32 ctime:32 nlink:32 [ length:64 | major:16 minor:16 empty:32 ]
//
//   in case of BLOCKDEV and CHARDEV instead of 'length:64' on the end there is 'major:16 minor:16 empty:32'
//
// attr record (3.0.93 and up) - 36B:
//
//   flags:8 type:4 mode:12 uid:32 gid:32 atime:32 mtime:32 ctime:32 nlink:32 [ length:64 | major:16 minor:16 empty:32 ] winattr:8
//
//   in case of BLOCKDEV and CHARDEV instead of 'length:64' on the end there is 'major:16 minor:16 empty:32'



// NAME type:
// ( leng:8 data:lengB )



#define FUSE_REGISTER_BLOB_NOACL       "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx64"
// CLTOMA:
//  clientid:32 [ version:32 ]
// MATOCL:
//  clientid:32
//  status:8

#define FUSE_REGISTER_BLOB_TOOLS_NOACL "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx63"
// CLTOMA:
//  -
// MATOCL:
//  status:8

#define FUSE_REGISTER_BLOB_ACL         "DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0"

#define REGISTER_GETRANDOM 1
// rcode==1: generate random blob
// CLTOMA:
//  rcode:8
// MATOCL:
//  randomblob:32B

#define REGISTER_NEWSESSION 2
// rcode==2: first register
// CLTOMA:
//  rcode:8 version:32 ileng:32 info:ilengB pleng:32 path:plengB [ sessionid:32 [ metaid:64 ]] [ passcode:16B ]
// MATOCL:
//  sessionid:32 sesflags:8 rootuid:32 rootgid:32 ( version < 1.6.1)
//  sessionid:32 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 ( version >= 1.6.1 && version < 1.6.21 )
//  version:32 sessionid:32 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 ( version >= 1.6.21 && version < 1.6.26 )
//  version:32 sessionid:32 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ( version >= 1.6.26 )
//  version:32 sessionid:32 metaid:64 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ( version >= 3.0.11 )
//  version:32 sessionid:32 metaid:64 sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ( version >= 3.0.72 )
//  version:32 sessionid:32 metaid:64 sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 disables:32 ( version >= 3.0.112 / 4.21.0 )
//  status:8

#define REGISTER_RECONNECT 3
// rcode==3: mount reconnect
// CLTOMA:
//  rcode:8 sessionid:32 version:32 [ metaid:64 ]
// MATOCL:
//  status:8
//  version:32 status:8 ( both versions >= 3.0.95 / 4.0.7 )

#define REGISTER_TOOLS 4
// rcode==4: tools connect
// CLTOMA:
//  rcode:8 sessionid:32 version:32
// MATOCL:
//  status:8

#define REGISTER_NEWMETASESSION 5
// rcode==5: first register
// CLTOMA:
//  rcode:8 version:32 ileng:32 info:ilengB [ sessionid:32 [ metaid:64 ]] [ passcode:16B ]
// MATOCL:
//  sessionid:32 sesflags:8 ( version < 1.6.21 )
//  version:32 sessionid:32 sesflags:8 ( version >= 1.6.21 && version < 1.6.26 )
//  version:32 sessionid:32 sesflags:8 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ( version >= 1.6.26 )
//  version:32 sessionid:32 metaid:64 sesflags:8 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ( version >= 3.0.11 )
//  status:8

#define REGISTER_CLOSESESSION 6
// rcode==6: close session
// CLTOMA:
//  rcode:8 sessionid:32 [ metaid:64 ]
// MATOCL:
//  status:8

// 0x0190
#define CLTOMA_FUSE_REGISTER (PROTO_BASE+400)
// blob:64B ... (depends on blob - see blob descriptions above)

// 0x0191
#define MATOCL_FUSE_REGISTER (PROTO_BASE+401)
// maxsize=45 minsize=1
// depends on blob - see blob descriptions above

// 0x0192
#define CLTOMA_FUSE_STATFS (PROTO_BASE+402)
// msgid:32 -

// 0x0193
#define MATOCL_FUSE_STATFS (PROTO_BASE+403)
// deprecated (version < 3.0.102 in 3.x and < 4.9.0 in 4.x):
//	msgid:32 totalspace:64 availspace:64 trashspace:64 inodes:32
// current:
//	msgid:32 totalspace:64 availspace:64 trashspace:64 sustainedspace:64 inodes:32

// 0x0194
#define CLTOMA_FUSE_ACCESS (PROTO_BASE+404)
// msgid:32 inode:32 uid:32 gid:32 modemask:8 - version < 2.0.0
// msgid:32 inode:32 uid:32 gcnt:32 gcnt * [ gid:32 ] perm:16

// 0x0195
#define MATOCL_FUSE_ACCESS (PROTO_BASE+405)
// msgid:32 status:8

// 0x0196
#define CLTOMA_FUSE_LOOKUP (PROTO_BASE+406)
// msgid:32 inode:32 name:NAME uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 name:NAME uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x0197
#define MATOCL_FUSE_LOOKUP (PROTO_BASE+407)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR - (master version or client version < 3.0.40)
// msgid:32 inode:32 attr:ATTR lflags:16 [ protocolid:8 chunkid:64 version:32 N * [ ip:32 port:16 cs_ver:32 labelmask:32 ] ] - (master and client both versions >= 3.0.40 - protocolid==2 ; chunk 0 data only for one-chunk files with unlocked chunk)
// msgid:32 inode:32 attr:ATTR lflags:16 [ protocolid:8 chunkid:64 version:32 8 * [ ip:32 port:16 cs_ver:32 labelmask:32 ] ] - (master and client both versions >= 4.0.0 and chunk is split into eight parts - protocolid==3 ; chunk 0 data only for one-chunk files with unlocked chunk)

// 0x0198
#define CLTOMA_FUSE_GETATTR (PROTO_BASE+408)
// msgid:32 inode:32
// msgid:32 inode:32 uid:32 gid:32 - version <= 1.6.27
// msgid:32 inode:32 opened:8 uid:32 gid:32

// 0x0199
#define MATOCL_FUSE_GETATTR (PROTO_BASE+409)
// msgid:32 status:8
// msgid:32 attr:ATTR

// 0x019A
#define CLTOMA_FUSE_SETATTR (PROTO_BASE+410)
// msgid:32 inode:32 uid:32 gid:32 setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32 - versions < 1.6.25
// msgid:32 inode:32 uid:32 gid:32 setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32 sugidclearmode:8 - version <= 1.6.27
// msgid:32 inode:32 opened:8 uid:32 gid:32 setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32 sugidclearmode:8 - version < 2.0.0
// msgid:32 inode:32 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32 sugidclearmode:8 - version < 3.0.93
// msgid:32 inode:32 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32 winattr:8 sugidclearmode:8

// 0x019B
#define MATOCL_FUSE_SETATTR (PROTO_BASE+411)
// msgid:32 status:8
// msgid:32 attr:ATTR

// 0x019C
#define CLTOMA_FUSE_READLINK (PROTO_BASE+412)
// msgid:32 inode:32

// 0x019D
#define MATOCL_FUSE_READLINK (PROTO_BASE+413)
// msgid:32 status:8
// msgid:32 length:32 path:lengthB

// 0x019E
#define CLTOMA_FUSE_SYMLINK (PROTO_BASE+414)
// msgid:32 inode:32 name:NAME length:32 path:lengthB uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 name:NAME length:32 path:lengthB uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x019F
#define MATOCL_FUSE_SYMLINK (PROTO_BASE+415)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR

// 0x01A0
#define CLTOMA_FUSE_MKNOD (PROTO_BASE+416)
// msgid:32 inode:32 name:NAME type:8 mode:16 uid:32 gid:32 rdev:32 - version < 2.0.0
// msgid:32 inode:32 name:NAME type:8 mode:16 umask:16 uid:32 gcnt:32 gcnt * [ gid:32 ] rdev:32

// 0x01A1
#define MATOCL_FUSE_MKNOD (PROTO_BASE+417)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR

// 0x01A2
#define CLTOMA_FUSE_MKDIR (PROTO_BASE+418)
// msgid:32 inode:32 name:NAME mode:16 uid:32 gid:32 - version < 1.6.25
// msgid:32 inode:32 name:NAME mode:16 uid:32 gid:32 copysgid:8 - version < 2.0.0
// msgid:32 inode:32 name:NAME mode:16 umask:16 uid:32 gcnt:32 gcnt * [ gid:32 ] copysgid:8

// 0x01A3
#define MATOCL_FUSE_MKDIR (PROTO_BASE+419)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR

// 0x01A4
#define CLTOMA_FUSE_UNLINK (PROTO_BASE+420)
// msgid:32 inode:32 name:NAME uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 name:NAME uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x01A5
#define MATOCL_FUSE_UNLINK (PROTO_BASE+421)
// msgid:32 status:8
// since 3.0.107/4.18.0 (after successful remove):
// msgid:32 inode:32

// 0x01A6
#define CLTOMA_FUSE_RMDIR (PROTO_BASE+422)
// msgid:32 inode:32 name:NAME uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 name:NAME uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x01A7
#define MATOCL_FUSE_RMDIR (PROTO_BASE+423)
// msgid:32 status:8
// since 3.0.107/4.18.0 (after successful remove):
// msgid:32 inode:32

// 0x01A8
#define CLTOMA_FUSE_RENAME (PROTO_BASE+424)
// msgid:32 inode_src:32 name_src:NAME inode_dst:32 name_dst:NAME uid:32 gid:32 - version < 2.0.0
// msgid:32 inode_src:32 name_src:NAME inode_dst:32 name_dst:NAME uid:32 gcnt:32 gcnt * [ gid:32 ] - version < 4.18.0
// msgid:32 inode_src:32 name_src:NAME inode_dst:32 name_dst:NAME uid:32 gcnt:32 gcnt * [ gid:32 ] mode:8

// 0x01A9
#define MATOCL_FUSE_RENAME (PROTO_BASE+425)
// msgid:32 status:8
// since 1.6.21 (after successful rename):
// msgid:32 inode:32 attr:ATTR

// 0x01AA
#define CLTOMA_FUSE_LINK (PROTO_BASE+426)
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x01AB
#define MATOCL_FUSE_LINK (PROTO_BASE+427)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR

// 0x01AC
#define CLTOMA_FUSE_READDIR (PROTO_BASE+428)
// msgid:32 inode:32 uid:32 gid:32 [ flags:8 [ maxentries:32 nedgeid:64 ] ] - version < 2.0.0
// msgid:32 inode:32 uid:32 gcnt:32 gcnt * [ gid:32 ] flags:8 maxentries:32 nedgeid:64

// 0x01AD
#define MATOCL_FUSE_READDIR (PROTO_BASE+429)
// msgid:32 status:8
// msgid:32 [ nedgeid:64 ] N*[ name:NAME inode:32 type:8 ]	- when GETDIR_FLAG_WITHATTR in flags is not set
// msgid:32 [ nedgeid:64 ] N*[ name:NAME inode:32 attr:ATTR ]	- when GETDIR_FLAG_WITHATTR in flags is set


// 0x01AE
#define CLTOMA_FUSE_OPEN (PROTO_BASE+430)
// msgid:32 inode:32 uid:32 gid:32 flags:8 - version < 2.0.0
// msgid:32 inode:32 uid:32 gcnt:32 gcnt * [ gid:32 ] flags:8

// 0x01AF
#define MATOCL_FUSE_OPEN (PROTO_BASE+431)
// msgid:32 status:8
// msgid:32 attr:ATTR (version < 3.0.113, version < 4.22)
// msgid:32 flags:8 attr:ATTR (version >= 3.0.113, version >= 4.22)

// 0x01B0
#define CLTOMA_FUSE_READ_CHUNK (PROTO_BASE+432)
// msgid:32 inode:32 chunkindx:32 - version < 3.0.4
// msgid:32 inode:32 chunkindx:32 chunkopflags:8

// 0x01B1
#define MATOCL_FUSE_READ_CHUNK (PROTO_BASE+433)
// maxsize=4096
// msgid:32 status:8
// msgid:32 length:64 chunkid:64 version:32 N*[ ip:32 port:16 ]
// msgid:32 protocolid:8 length:64 chunkid:64 version:32 N * [ ip:32 port:16 cs_ver:32 ] (master and client both versions >= 1.7.32 - protocolid==1)
// msgid:32 protocolid:8 length:64 chunkid:64 version:32 N * [ ip:32 port:16 cs_ver:32 labelmask:32 ] (master and client both versions >= 3.0.10 - protocolid==2)
// msgid:32 protocolid:8 length:64 chunkid:64 version:32 (4 or 8) * [ ip:32 port:16 cs_ver:32 labelmask:32 ] (master and client both versions >= 4.0.0 and chunk is split into eight parts - protocolid==3)

// 0x01B2
#define CLTOMA_FUSE_WRITE_CHUNK (PROTO_BASE+434)
// msgid:32 inode:32 chunkindx:32 - version < 3.0.4
// msgid:32 inode:32 chunkindx:32 chunkopflags:8

// 0x01B3
#define MATOCL_FUSE_WRITE_CHUNK (PROTO_BASE+435)
// maxsize=4096
// msgid:32 status:8
// msgid:32 length:64 chunkid:64 version:32 N*[ ip:32 port:16 ]
// msgid:32 protocolid:8 length:64 chunkid:64 version:32 N * [ ip:32 port:16 cs_ver:32 ] (master and client both versions >= 1.7.32 - protocolid==1)
// msgid:32 protocolid:8 length:64 chunkid:64 version:32 N * [ ip:32 port:16 cs_ver:32 labelmask:32 ] (master and client both versions >= 3.0.10 - protocolid==2)

// 0x01B4
#define CLTOMA_FUSE_WRITE_CHUNK_END (PROTO_BASE+436)
// msgid:32 chunkid:64 inode:32 length:64 - version < 3.0.4
// msgid:32 chunkid:64 inode:32 length:64 chunkopflags:8 - version < 3.0.74
// msgid:32 chunkid:64 inode:32 chunkindx:32 length:64 chunkopflags:8

// 0x01B5
#define MATOCL_FUSE_WRITE_CHUNK_END (PROTO_BASE+437)
// msgid:32 status:8



// 0x01B6
#define CLTOMA_FUSE_APPEND_SLICE (PROTO_BASE+438)
// msgid:32 inode:32 srcinode:32 uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 srcinode:32 uid:32 gcnt:32 gcnt * [ gid:32 ] - version < 3.0.91
// msgid:32 flags:8 inode:32 srcinode:32 slice_from:32 slice_to:32 uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x01B7
#define MATOCL_FUSE_APPEND_SLICE (PROTO_BASE+439)
// msgid:32 status:8


// 0x01B8
#define CLTOMA_FUSE_CHECK (PROTO_BASE+440)
// msgid:32 inode:32
// msgid:32 inode:32 chunkindx:32 (version >= 3.0.26)
// msgid:32 mode:8 inode:32 [ chunkindx:32 ] (version >= 4.0.0)

// 0x01B9
#define MATOCL_FUSE_CHECK (PROTO_BASE+441)
// maxsize=1000
// msgid:32 status:8 (common)
// msgid:32 N*[ copies:8 chunks:16 ] (version < 1.6.23)
// msgid:32 11*[ chunks:32 ] - 0 copies, 1 copy, 2 copies, ..., 10+ copies (version >= 1.6.23 and no chunkindx)
// msgid:32 12*[ chunks:32 ] - 0 copies, 1 copy, 2 copies, ..., 10+ copies, 'empty' copies (version >= 3.0.30 and no chunkindx)
// msgid:32 chunkid:64 version:32 N*[ ip:32 port:16 type:8 ] (version >= 3.0.26 and chunkindx present)
// msgid:32 chunkid:64 version:32 N*[ ip:32 port:16 type:8 ecid:8 ] (version >= 4.0.0 and mode is equal to 1)
// msgid:32 N * [ storage_index:8 chunks:32 ] ; storage_index: 0 - chunk not found, 1 - 'empty' chunk, 2 + fullcopies + 11 * ec_parts (version >= 4.0.0 and mode is equal to 2)
// msgid:32 N * [ storage_index:16 chunks:32 ] ; storage_index: 0 - chunk not found, 1 - 'empty' chunk, 2 + fullcopies + 11 * ec8_parts + 198 * ec4_parts (version >= 4.36.0 and mode is equal to 3)


// 0x01BA
#define CLTOMA_FUSE_GETTRASHRETENTION (PROTO_BASE+442)
// msgid:32 inode:32 gmode:8

// 0x01BB
#define MATOCL_FUSE_GETTRASHRETENTION (PROTO_BASE+443)
// maxsize=100000
// msgid:32 status:8
// msgid:32 tdirs:32 tfiles:32 tdirs*[ trashretention:32 dirs:32 ] tfiles*[ trashretention:32 files:32 ]


// 0x01BC
#define CLTOMA_FUSE_SETTRASHRETENTION (PROTO_BASE+444)
// msgid:32 inode:32 uid:32 trashretentionout:32 smode:8

// 0x01BD
#define MATOCL_FUSE_SETTRASHRETENTION (PROTO_BASE+445)
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32


// 0x01BE
#define CLTOMA_FUSE_GETSCLASS (PROTO_BASE+446)
// msgid:32 inode:32 gmode:8

// 0x01BF
#define MATOCL_FUSE_GETSCLASS (PROTO_BASE+447)
// maxsize=100000
// msgid:32 status:8
// msgid:32 gdirs:8 gfiles:8 gdirs*[ goal:8 dirs:32 ] gfiles*[ goal:8 files:32 ] (version < 2.1.0)
// msgid:32 gdirs:8 gfiles:8 gdirs*[ goal:8 dirs:32 | zero:8 labelscnt:8 labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] dirs:32 ] gfiles*[ goal:8 files:32 | zero:8 labelscnt:8 labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] files:32 ] (version >= 2.1.0)
// msgid:32 gdirs:8 gfiles:8 gdirs*[ goal:8 dirs:32 | 0xFF:8 storage_class:NAME ] gfiles*[ goal:8 files:32 | 0xFF storage_class:NAME ] (version >= 3.0.75)


// 0x01C0
#define CLTOMA_FUSE_SETSCLASS (PROTO_BASE+448)
// msgid:32 inode:32 uid:32 goal:8 smode:8 (any version)
// msgid:32 inode:32 uid:32 labelscnt:8 smode:8 labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] (version >= 2.1.0)
// msgid:32 inode:32 uid:32 zero:8 smode:8 labels_mode:8 arch_delay:16 create_labelscnt:8 keep_labelscnt:8 arch_labelscnt:8 create_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] keep_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] arch_labelscnt * [ MASKORGROUP * [ labelmask:32 ] ] (version >= 3.0.0)
// msgid:32 inode:32 uid:32 0xFF:8 smode:8 storage_class:NAME                             (version >= 3.0.75 && (smode & SMODE_TMASK) != SMODE_EXCHANGE )
// msgid:32 inode:32 uid:32 0xFF:8 smode:8 old_storage_class:NAME new_storage_class:NAME  (version >= 3.0.75 && (smode & SMODE_TMASK) == SMODE_EXCHANGE )

// 0x01C1
#define MATOCL_FUSE_SETSCLASS (PROTO_BASE+449)
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32 [quotaexceeded:32]


// 0x01C2
#define CLTOMA_FUSE_GETTRASH (PROTO_BASE+450)
// msgid:32 (version < 3.0.64)
// msgid:32 trash_id:32 (version >= 3.0.64)

// 0x01C3
#define MATOCL_FUSE_GETTRASH (PROTO_BASE+451)
// msgid:32 status:8
// msgid:32 N*[ name:NAME inode:32 ]


// 0x01C4
#define CLTOMA_FUSE_GETDETACHEDATTR (PROTO_BASE+452)
// msgid:32 inode:32 [ dtype:8 ]

// 0x01C5
#define MATOCL_FUSE_GETDETACHEDATTR (PROTO_BASE+453)
// msgid:32 status:8
// msgid:32 attr:ATTR


// 0x01C6
#define CLTOMA_FUSE_GETTRASHPATH (PROTO_BASE+454)
// msgid:32 inode:32

// 0x01C7
#define MATOCL_FUSE_GETTRASHPATH (PROTO_BASE+455)
// msgid:32 status:8
// msgid:32 length:32 path:lengthB


// 0x01C8
#define CLTOMA_FUSE_SETTRASHPATH (PROTO_BASE+456)
// msgid:32 inode:32 length:32 path:lengthB

// 0x01C9
#define MATOCL_FUSE_SETTRASHPATH (PROTO_BASE+457)
// msgid:32 status:8


// 0x01CA
#define CLTOMA_FUSE_UNDEL (PROTO_BASE+458)
// msgid:32 inode:32

// 0x01CB
#define MATOCL_FUSE_UNDEL (PROTO_BASE+459)
// msgid:32 status:8


// 0x01CC
#define CLTOMA_FUSE_PURGE (PROTO_BASE+460)
// msgid:32 inode:32

// 0x01CD
#define MATOCL_FUSE_PURGE (PROTO_BASE+461)
// msgid:32 status:8


// 0x01CE
#define CLTOMA_FUSE_GETDIRSTATS (PROTO_BASE+462)
// msgid:32 inode:32

// 0x01CF
#define MATOCL_FUSE_GETDIRSTATS (PROTO_BASE+463)
// msgid:32 status:8
// deprecated:
//	msgid:32 inodes:32 dirs:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks:32 length:64 size:64 gsize:64
// current:
//	msgid:32 inodes:32 dirs:32 files:32 chunks:32 length:64 size:64 gsize:64


// 0x01D0
#define CLTOMA_FUSE_TRUNCATE (PROTO_BASE+464)
// msgid:32 inode:32 [ opened:8 ] uid:32 gid:32 length:64 (version < 2.0.0)
// msgid:32 inode:32 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] length:64 (version >= 2.0.0/3.0.0)
// msgid:32 inode:32 flags:8 uid:32 gcnt:32 gcnt * [ gid:32 ] length:64 (version >= 2.0.89/3.0.25)

// 0x01D1
#define MATOCL_FUSE_TRUNCATE (PROTO_BASE+465)
// msgid:32 status:8
// msgid:32 attr:ATTR (version < 3.0.113, version < 4.22)
// msgid:32 prevsize:64 attr:ATTR (version >= 3.0.113, version >= 4.22)

// 0x01D2
#define CLTOMA_FUSE_REPAIR (PROTO_BASE+466)
// msgid:32 inode:32 uid:32 gid:32 - version < 2.0.0
// msgid:32 inode:32 uid:32 gcnt:32 gcnt * [ gid:32 ]

// 0x01D3
#define MATOCL_FUSE_REPAIR (PROTO_BASE+467)
// msgid:32 status:8
// msgid:32 notchanged:32 erased:32 repaired:32


// 0x01D4
#define CLTOMA_FUSE_SNAPSHOT (PROTO_BASE+468)
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32 canoverwrite:8 (version <= 1.6.27)
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32 smode:8 umask:16 (version > 1.6.27 and version < 2.0.0)
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gcnt:32 gcnt * [ gid:32 ] smode:8 umask:16 (version >= 2.0.0)

// 0x01D5
#define MATOCL_FUSE_SNAPSHOT (PROTO_BASE+469)
// msgid:32 status:8


// 0x01D6
#define CLTOMA_FUSE_GETSUSTAINED (PROTO_BASE+470)
// msgid:32

// 0x01D7
#define MATOCL_FUSE_GETSUSTAINED (PROTO_BASE+471)
// msgid:32 status:8
// msgid:32 N*[ name:NAME inode:32 ]


// 0x01D8
#define CLTOMA_FUSE_GETEATTR (PROTO_BASE+472)
// msgid:32 inode:32 gmode:8

// 0x01D9
#define MATOCL_FUSE_GETEATTR (PROTO_BASE+473)
// maxsize=100000
// msgid:32 status:8
// msgid:32 eattrdirs:8 eattrfiles:8 eattrdirs*[ eattr:8 dirs:32 ] eattrfiles*[ eattr:8 files:32 ]


// 0x01DA
#define CLTOMA_FUSE_SETEATTR (PROTO_BASE+474)
// msgid:32 inode:32 uid:32 eattr:8 smode:8

// 0x01DB
#define MATOCL_FUSE_SETEATTR (PROTO_BASE+475)
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32


// 0x01DC
#define CLTOMA_FUSE_QUOTACONTROL (PROTO_BASE+476)
// msgid:32 inode:32 qflags:8 - delete quota (ver < 4.51.0)
// msgid:32 inode:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 - set quota (ver < 3.0.9)
// msgid:32 inode:32 qflags:8 graceperiod:32 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 - set quota (ver >= 3.0.9)

// 0x01DD
#define MATOCL_FUSE_QUOTACONTROL (PROTO_BASE+477)
// msgid:32 status:8
// msgid:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 curinodes:32 curlength:64 cursize:64 currealsize:64
// msgid:32 qflags:8 graceperiod:32 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 curinodes:32 curlength:64 cursize:64 currealsize:64
// msgid:32 qflags:8 pflags:8 graceperiod:32 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 curinodes:32 curlength:64 cursize:64 currealsize:64


// 0x01DE
#define CLTOMA_FUSE_GETXATTR (PROTO_BASE+478)
// msgid:32 inode:32 opened:8 uid:32 gid:32 nleng:8 name:nlengB mode:8 (version < 2.0.0)
// msgid:32 inode:32 nleng:8 name:nlengB mode:8 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] (version >= 2.0.0)
//   empty name = list names
//   mode:
//    0 - get data
//    1 - get length only

// 0x01DF
#define MATOCL_FUSE_GETXATTR (PROTO_BASE+479)
// maxsize=100000
// msgid:32 status:8
// msgid:32 vleng:32
// msgid:32 vleng:32 value:vlengB

// 0x01E0
#define CLTOMA_FUSE_SETXATTR (PROTO_BASE+480)
// msgid:32 inode:32 uid:32 gid:32 nleng:8 name:8[NLENG] vleng:32 value:8[VLENG] mode:8 (version < 2.0.0)
// msgid:32 inode:32 nleng:8 name:8[NLENG] vleng:32 value:8[VLENG] mode:8 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] (version >= 2.0.0)
//   mode:
//    0 - create or replace
//    1 - create only
//    2 - replace only
//    3 - remove

// 0x01E1
#define MATOCL_FUSE_SETXATTR (PROTO_BASE+481)
// msgid:32 status:8

// 0x01E2
#define CLTOMA_FUSE_CREATE (PROTO_BASE+482)
// msgid:32 inode:32 name:NAME mode:16 uid:32 gid:32 (version < 2.0.0)
// msgid:32 inode:32 name:NAME mode:16 umask:16 uid:32 gcnt:32 gcnt * [ gid:32 ] (version >= 2.0.0)

// 0x01E3
#define MATOCL_FUSE_CREATE (PROTO_BASE+483)
// msgid:32 status:8
// msgid:32 inode:32 attr:ATTR (version < 3.0.113, version < 4.22)
// msgid:32 flags:8 inode:32 attr:ATTR (version >= 3.0.113, version >= 4.22)

// 0x01E4
#define CLTOMA_FUSE_PARENTS (PROTO_BASE+484)
// msgid:32 inode:32

// 0x01E5
#define MATOCL_FUSE_PARENTS (PROTO_BASE+485)
// msgid:32 status:8
// msgid:32 N*[ inode:32 ]

// 0x01E6
#define CLTOMA_FUSE_PATHS (PROTO_BASE+486)
// msgid:32 inode:32

// 0x01E7
#define MATOCL_FUSE_PATHS (PROTO_BASE+487)
// msgid:32 status:8
// msgid:32 N*[ length:32 path:lengthB ]

// 0x01E8
#define CLTOMA_FUSE_GETFACL (PROTO_BASE+488)
// msgid:32 inode:32 acltype:8 opened:8 uid:32 gcnt:32 gcnt * [ gid:32 ] (version < 3.0.92)
// msgid:32 inode:32 acltype:8 (version >= 3.0.92)

// 0x01E9
#define MATOCL_FUSE_GETFACL (PROTO_BASE+489)
// msgid:32 status:8
// msgid:32 userperm:16 groupperm:16 otherperm:16 mask:16 namedusers:16 namedgroups:16 namedusers * [ id:32 perm:16 ] namedgroups * [ id:32 perm:16 ]

// 0x01EA
#define CLTOMA_FUSE_SETFACL (PROTO_BASE+490)
// msgid:32 inode:32 uid:32 acltype:8 userperm:16 groupperm:16 otherperm:16 mask:16 namedusers:16 namedgroups:16 namedusers * [ id:32 perm:16 ] namedgroups * [ id:32 perm:16 ]

// 0x01EB
#define MATOCL_FUSE_SETFACL (PROTO_BASE+491)
// msgid:32 status:8

// 0x01EC
#define CLTOMA_FUSE_FLOCK (PROTO_BASE+492)
// msgid:32 inode:32 reqid:32 owner:64 cmd:8

// 0x01ED
#define MATOCL_FUSE_FLOCK (PROTO_BASE+493)
// msgid:32 status:8


// 0x01EE
#define CLTOMA_FUSE_POSIX_LOCK (PROTO_BASE+494)
// msgid:32 inode:32 reqid:32 owner:64 pid:32 cmd:8 type:8 start:64 end:64

// 0x01EF
#define MATOCL_FUSE_POSIX_LOCK (PROTO_BASE+495)
// msgid:32 status:8 (cmd != POSIX_LOCK_CMD_GET || status != STATUS_OK)
// msgid:32 pid:32 type:8 start:64 end:64 (cmd == POSIX_LOCK_CMD_GET && status == STATUS_OK)


// 0x01F0
#define CLTOMA_FUSE_ARCHCTL (PROTO_BASE+496)
// msgid:32 inode:32 cmd:8 (cmd==ARCHCTL_GET)
// msgid:32 inode:32 cmd:8 uid:32 (cmd==ARCHCTL_SET or cmd==ARCHCTL_CLR)

// 0x01F1
#define MATOCL_FUSE_ARCHCTL (PROTO_BASE+497)
// msgid:32 status:8
// msgid:32 archchunks:64 notarchchunks:64 archinodes:32 partialinodes:32 notarchinodes:32 (cmd==ARCHCTL_GET)
// msgid:32 chunkschanged:64 chunksnotchanged:64 inodesnotpermitted:32 (cmd==ARCHCTL_SET or cmd==ARCHCTL_CLR)

// 0x01F2
#define CLTOMA_FUSE_FSYNC (PROTO_BASE+498)
// msgid:32 inode:32

// 0x01F3
#define MATOCL_FUSE_FSYNC (PROTO_BASE+499)
// msgid:32 status:8


// deprecated packet - since version 3.0.74 new packet should be used
// 0x01F3
#define CLTOMA_FUSE_SUSTAINED_INODES_DEPRECATED (PROTO_BASE+499)
// N*[ inode:32 ]




// MASTER STATS (stats - unregistered)


// 0x01F4
#define CLTOMA_CSERV_LIST (PROTO_BASE+500)
// [ mode:8 ]

// 0x01F5
#define MATOCL_CSERV_LIST (PROTO_BASE+501)
// N*[ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 ] (version < 1.5.13)
// N*[disconnected:8 version:24 ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 ] (version >= 1.5.13 && version < 1.6.28)
// N*[disconnected:8 version:24 ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 ] (version >= 1.6.28 && version < 1.7.25)
// N*[flags:8 version:24 ip:32 port:16 csid:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 ] (version >= 1.7.25 && version < 2.1.0)
// N*[flags:8 version:24 ip:32 port:16 csid:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 labelmask:32 ] (version >= 2.1.0 && version < 3.0.38)
// N*[flags:8 version:24 ip:32 port:16 csid:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 labelmask:32 mfrstatus:8 ] (version >= 3.0.38 && version < 4.26.0)
// N*[flags:8 version:24 originalip:32 ip:32 port:16 csid:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 labelmask:32 mfrstatus:8 ] (version >= 4.26.0 && version < 4.35.0 or mode == 0 && version >= 4.35.0)
// N*[flags:8 version:24 originalip:32 ip:32 port:16 csid:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 load:32 gracetime:32 labelmask:32 mfrstatus:8 maintenance_to:32 ] (version >= 4.35.0 and mode > 0)


// 0x01F6
#define CLTOAN_MONOTONIC_DATA (PROTO_BASE+502)

// 0x01F7
#define ANTOCL_MONOTONIC_DATA (PROTO_BASE+503)
// N:16 N * [ data:64 ] // master
// N:16 N * [ data:64 ] M:16 M * [ entrysize:16 path:NAME flags:8 errchunkid:64 errtime:32 used:64 total:64 chunkscount:32 bytesread:64 byteswritten:64 usecread:64 usecwrite:64 usecfsync:64 readops:32 writeops:32 fsyncops:32 usecreadmax:32 usecwritemax:32 usecfsyncmax:32 ] // chunkserver


// 0x01F8
#define CLTOAN_CHART (PROTO_BASE+504)
//	chartid:32
// since version 2.0:
//	chartid:32 width:16 height:16

// 0x01F9
#define ANTOCL_CHART (PROTO_BASE+505)
// maxsize=10000000
// chart:PNG

// 0x01FA
#define CLTOAN_CHART_DATA (PROTO_BASE+506)
// chartid:32 (version < 2.0.15)
// chartid:32 [ maxentries:32 ] (version >= 2.0.15)
// chartid:32 [ maxentries:32 ] [ multimode:8 ] (version >= 4.31.0)

// 0x01FB
#define ANTOCL_CHART_DATA (PROTO_BASE+507)
// maxsize=10000000
// time:32 N*[ data:64 ] (version < 2.0.15)
// time:32 entries:32 entries*[ data:64 ] (version >= 2.0.15 and version < 4.31.0)
// ranges:8 series:8 entries:32 percent:8 base:8 ranges * [ range:8 time:32 mul:32 div:32 series * [ entries * [ data:64 ] ] ] (version >= 4.31.0 and multimode>0 ; base : 0=micro,1=mili,2=none,3=kilo and so on)


// 0x01FC
#define CLTOMA_SESSION_LIST (PROTO_BASE+508)
// [ vmode:8 ]

// 0x01FD
#define MATOCL_SESSION_LIST (PROTO_BASE+509)
// N*[ sessionid:32 ip:32 version:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 16 * [ current_statdata:32 ] 16 * [ last_statdata:32 ] ] - vmode = 0 or empty and version < 1.6.21
// N*[ sessionid:32 ip:32 version:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 21 * [ current_statdata:32 ] 21 * [ last_statdata:32 ] ] - vmode = 0 or empty and version == 1.6.21
// stats:16 N*[ sessionid:32 ip:32 version:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 stats * [ current_statdata:32 ] stats * [ last_statdata:32 ] ] - vmode = 0 or empty and version > 1.6.21
// stats:16 N*[ sessionid:32 ip:32 version:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 stats * [ current_statdata:32 ] stats * [ last_statdata:32 ] ] - vmode = 1 (valid since version 1.6.26)
// stats:16 N*[ sessionid:32 ip:32 version:32 openfiles:32 nsocks:8 expire:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 stats * [ current_statdata:32 ] stats * [ last_statdata:32 ] ] - vmode = 2 (valid since version 1.7.8)
// stats:16 N*[ sessionid:32 ip:32 version:32 openfiles:32 nsocks:8 expire:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 stats * [ current_statdata:32 ] stats * [ last_statdata:32 ] ] - vmode = 3 (valid since version 3.0.72)
// stats:16 N*[ sessionid:32 ip:32 version:32 openfiles:32 nsocks:8 expire:32 ileng:32 info:ilengB pleng:32 path:plengB sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 disables:32 stats * [ current_statdata:32 ] stats * [ last_statdata:32 ] ] - vmode = 4 (valid since version 3.0.112 / 4.21.0)
// N*[ sessionid:32 ip:32 expire:32 ileng:32 info:ilengB ] - vmode = 0xFF (valid since version 4.7.0)


// 0x01FE
#define CLTOMA_INFO (PROTO_BASE+510)
// -

// 0x01FF
#define MATOCL_INFO (PROTO_BASE+511)
// version:32 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32 (size = 68,version < 2.0.0)
// version:32 memusage:64 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32 (size = 76,version < 2.0.0)
// version:32 memusage:64 syscpu:64 usercpu:64 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32 laststore_ts:32 laststore_duration:32 laststore_status:8 (size = 101,version < 2.0.0)
// version:32 memusage:64 syscpu:64 usercpu:64 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32 laststore_ts:32 laststore_duration:32 laststore_status:8 state:8 nstate:8 stable:8 sync:8 leaderip:32 state_chg_time:32 meta_version:64 (size = 121,version >= 2.0.0)
// version:32 memusage:64 syscpu:64 usercpu:64 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32 laststore_ts:32 laststore_duration:32 laststore_status:8 state:8 nstate:8 stable:8 sync:8 leaderip:32 state_chg_time:32 meta_version:64 exports_checksum:64 (size = 129,version >= 3.0.32)
// version:32 memusage:64 syscpu:64 usercpu:64 totalspace:64 availspace:64 trashspace:64 trashnodes:32 sustainedspace:64 sustainednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 copy_chunks:32 ec_chunks:32 laststore_ts:32 laststore_duration:32 laststore_status:8 state:8 nstate:8 stable:8 sync:8 leaderip:32 state_chg_time:32 meta_version:64 exports_checksum:64 metadata_id:64 last_store_meta_version:64 last_store_meta_checksum:32 all_chunk_copies:64 all_chunk_parts:64 all_chunk_hypothetical_copies:64 (size = 173,version >= 4.4.0)

// 0x0200
#define CLTOMA_FSTEST_INFO (PROTO_BASE+512)
// -

// 0x0201
#define MATOCL_FSTEST_INFO (PROTO_BASE+513)
// maxsize=10000000
// deprecated:
// 	loopstart:32 loopend:32 files:32 ugfiles:32 mfiles:32 msgleng:32 msgleng*[ char:8 ]
// current:
// 	loopstart:32 loopend:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks:32 msgleng:32 msgleng*[ char:8 ]


// 0x0202
#define CLTOMA_CHUNKSTEST_INFO (PROTO_BASE+514)
// -

// 0x0203
#define MATOCL_CHUNKSTEST_INFO (PROTO_BASE+515)
// loopstart:32 loopend:32 del_invalid:32 nodel_invalid:32 del_unused:32 nodel_unused:32 del_diskclean:32 nodel_diskclean:32 del_overgoal:32 nodel_overgoal:32 copy_undergoal:32 nocopy_undergoal:32 copy_rebalance:32
// loopstart:32 loopend:32 del_invalid:32 nodel_invalid:32 del_unused:32 nodel_unused:32 del_diskclean:32 nodel_diskclean:32 del_overgoal:32 nodel_overgoal:32 copy_undergoal:32 nocopy_undergoal:32 copy_rebalance:32 locked_unused:32 locked_used:32
// loopstart:32 loopend:32 del_invalid:32 nodel_invalid:32 del_unused:32 nodel_unused:32 del_diskclean:32 nodel_diskclean:32 del_overgoal:32 nodel_overgoal:32 copy_undergoal:32 nocopy_undergoal:32 copy_wronglabels:32 nocopy_wronglabels:32 copy_rebalance:32 labels_dont_match:32 locked_unused:32 locked_used:32 (version >= 2.1.4)


// 0x0204
#define CLTOMA_CHUNKS_MATRIX (PROTO_BASE+516)
// (version >= 1.7.0 - empty request means all matrices for all storage classes - 2,6 or 8 matrices - depending on master version)
// [matrix_id:8] (all versions)
// [null_byte:8 sclassid:8] (version >= 4.44.0 - all matrices for a given storage class id, sclassid==0 means all classes)

// 0x0205
#define MATOCL_CHUNKS_MATRIX (PROTO_BASE+517)
// maxsize=3873 minsize=484
// 11 * [ 11 * count:32 ] - 11x11 matrix of chunks counters (goal x validcopies), 10 means 10 or more (this will be sent back when request length==1 - with matrix_id)
// progress_status:8 N * [ 11 * [ 11 * count:32 ] ] - N can be 2,6 or 8 - depending on master version, (this will be sent back for requests with length==0 or 2)


// 0x0206
#define CLTOMA_QUOTA_INFO (PROTO_BASE+518)
// -

// 0x0207
#define MATOCL_QUOTA_INFO (PROTO_BASE+519)
// quota_time_limit:32 N*[ inode:32 pleng:32 path:plengB exceeded:8 qflags:8 stimestamp:32 sinodes:32 slength:64 ssize:64 sgoalsize:64 hinodes:32 hlength:64 hsize:64 hgoalsize:64 currinodes:32 currlength:64 currsize:64 currgoalsize:64 ]


// 0x0208
#define CLTOMA_EXPORTS_INFO (PROTO_BASE+520)
// [ vmode:8 ]

// 0x0209
#define MATOCL_EXPORTS_INFO (PROTO_BASE+521)
// N*[ fromip:32 toip:32 pleng:32 path:plengB version:32 extraflags:8 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 ] - vmode = 0 (or not present)
// N*[ fromip:32 toip:32 pleng:32 path:plengB version:32 extraflags:8 sesflags:8 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ] - vmode = 1 (valid since version 1.6.26)
// N*[ fromip:32 toip:32 pleng:32 path:plengB version:32 extraflags:8 sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 ] - vmode = 2 (valid since version 3.0.72)
// N*[ fromip:32 toip:32 pleng:32 path:plengB version:32 extraflags:8 sesflags:8 umask:16 rootuid:32 rootgid:32 mapalluid:32 mapallgid:32 mingoal:8 maxgoal:8 mintrashretention:32 maxtrashretention:32 disables:32 ] - vmode = 3 (valid since version 3.0.112 / 4.21.0)


// 0x020A
#define CLTOMA_MLOG_LIST (PROTO_BASE+522)
// -

// 0x020B
#define MATOCL_MLOG_LIST (PROTO_BASE+523)
// N*[ version:32 ip:32 ]


// 0x020C
#define CLTOMA_CSSERV_COMMAND (PROTO_BASE+524)
// ip:32 port:16 (version < 1.6.28)
// commandid:8 ip:32 port:16 (version >= 1.6.28)

// 0x020D
#define MATOCL_CSSERV_COMMAND (PROTO_BASE+525)
// [ status:8 ]

// 0x020E
#define CLTOMA_SESSION_COMMAND (PROTO_BASE+526)
// commandid:8 sessionid:32

// 0x020F
#define MATOCL_SESSION_COMMAND (PROTO_BASE+527)
// deprecated:
//	-
// current:
//	status:8

// 0x0210
#define CLTOMA_MEMORY_INFO (PROTO_BASE+528)
// -

// 0x0211
#define MATOCL_MEMORY_INFO (PROTO_BASE+529)
// maxsize=176
// N*[ allocated:64 used:64 ]
//   N = 11 (ver 1.7.15)
//     0 - chunk hash
//     1 - chunks
//     2 - chunk server lists
//     3 - edge hash
//     4 - edges (file names)
//     5 - i-node hash
//     6 - i-nodes
//     7 - free i-nodes
//     8 - chunk tabs
//     9 - symlinks
//     10 - quota

// 0x0212
#define CLTOAN_MODULE_INFO (PROTO_BASE+530)
// -

// 0x0213
#define ANTOCL_MODULE_INFO (PROTO_BASE+531)
// module_type:8 module_version:32 module_id:16 meta_id:64 leader_ip:32 leader_port:16

// 0x0214
#define CLTOMA_LIST_OPEN_FILES (PROTO_BASE+532)
// [ msgid:32 ] sessionid:32

// 0x0215
#define MATOCL_LIST_OPEN_FILES (PROTO_BASE+533)
// [ msgid:32 ] N*[ sessionid:32 inode:32 ] // if input sessionid==0
// [ msgid:32 ] N*[ inode:32 ] // if input sessionid>0

// 0x0216
#define CLTOMA_LIST_ACQUIRED_LOCKS (PROTO_BASE+534)
// [ msgid:32 ] inode:32

// 0x0217
#define MATOCL_LIST_ACQUIRED_LOCKS (PROTO_BASE+535)
// [ msgid:32 ] N*[ inode:32 sessionid:32 owner:64 pid:32 start:64 end:64 ctype:8 ] // if input inode==0
// [ msgid:32 ] N*[ sessionid:32 owner:64 pid:32 start:64 end:64 ctype:8 ] // if input inode>0

// 0x0218
#define CLTOMA_MASS_RESOLVE_PATHS (PROTO_BASE+536)
// N*[ inode:32 ]

// 0x0219
#define MATOCL_MASS_RESOLVE_PATHS (PROTO_BASE+537)
// N*[ inode:32 pathssize:32 M*[ pathleng:32 path:pathlengB ] ]

// 0x021A

// 0x021B

// 0x021C

// 0x021D

// 0x021E
#define CLTOMA_SCLASS_INFO (PROTO_BASE+542)
// [ fver:8 ] ( if not present then fver==0 - versions < 4.2.0 )

// 0x021F
#define MATOCL_SCLASS_INFO (PROTO_BASE+543)
// fver==0:
//	allservers:16 N*[ sclassid:8 sclassname:NAME files:32 dirs:32 3 * [ stdchunks:64 archchunks:64 ] admin_only:8 labels_mode:8 arch_delay:16 3 * [ canbefulfilled:8 labelscnt:8 ] 3 * [ labelscnt * [ MASKORGROUP * [ labelmask:32 ] matchingservers:16 ] ] ]
//  - redundancy classes (0 - undergoal ; 1 - ok ; 2 - overgoal)
//  - label sets (0 - create ; 1 - keep ; 2 - archive)
// fver==1: (version >= 4.2.0)
//	allservers:16 N*[
//		sclassid:8 sclassname:NAME files:32 dirs:32
//			keepchunks:64 archchunks:64 trashchunks:64 (UNDER)
//			keepchunks:64 archchunks:64 trashchunks:64 (EXACT)
//			keepchunks:64 archchunks:64 trashchunks:64 (OVER)
//		admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (CREATE)
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (KEEP)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (ARCH)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (TRASH)
//	]
// fver==2: (version >= 4.5.0)
//	allservers:16 N*[
//		sclassid:8 sclassname:NAME files:32 dirs:32
//			keepchunks:64 archchunks:64 trashchunks:64 (UNDER COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (UNDER EC)
//			keepchunks:64 archchunks:64 trashchunks:64 (EXACT COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (EXACT EC)
//			keepchunks:64 archchunks:64 trashchunks:64 (OVER COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (OVER EC)
//		admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 min_trashretention:16
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (CREATE)
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (KEEP)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (ARCH)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (TRASH)
//	]
// fver==2: (version >= 4.34.0)
//	allservers:16 N*[
//		sclassid:8 sclassname:NAME files:32 dirs:32
//			keepchunks:64 archchunks:64 trashchunks:64 (UNDER COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (UNDER EC)
//			keepchunks:64 archchunks:64 trashchunks:64 (EXACT COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (EXACT EC)
//			keepchunks:64 archchunks:64 trashchunks:64 (OVER COPY)
//			keepchunks:64 archchunks:64 trashchunks:64 (OVER EC)
//		admin_only:8 labels_mode:8 arch_mode:8 arch_delay:16 arch_min_size:64 min_trashretention:16
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (CREATE)
//			canbefulfilled:8 labelscnt:8 uniqmask:32 (KEEP)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (ARCH)
//			canbefulfilled:8 labelscnt:8 ec_level:8 uniqmask:32 (TRASH)
//	]
// fver==128: (version >= 4.44.0)
//	N * [ sclassid:8 sclassname:NAME has_chunks:8]

// 0x0220
#define CLTOMA_MISSING_CHUNKS (PROTO_BASE+544)
// -

// 0x0221
#define MATOCL_MISSING_CHUNKS (PROTO_BASE+545)
// N*[ chunkdid:64 inode:32 indx:32 ]

// 0x0222
#define CLTOMA_NODE_INFO (PROTO_BASE+546)
// [ msgid:32 ] inode:32 maxentries:32 continueid:64

// 0x0223
#define MATOCL_NODE_INFO (PROTO_BASE+547)
// [ msgid:32 ] status:8
// [ msgid:32 ] anstype:16 // anstype == 0 (node is neither directory nor file)
// [ msgid:32 ] anstype:16 continueid:64 N * [ inode:32 ] // anstype == 1 (node is directory)
// [ msgid:32 ] anstype:16 continueid:64 length:64 N * [ chunkid:64 chunksize:32 copies:8 ] // anstype == 2 (node is file)
// [ msgid:32 ] anstype:16 continueid:64 length:64 N * [ chunkid:64 chunksize:32 copies_eights:8 ] // anstype == 3 (node is file and version >= 4.0.0)

// 0x0224
#define CLTOMA_PATTERN_INFO (PROTO_BASE+548)
// -

// 0x0225
#define MATOCL_PATTERN_INFO (PROTO_BASE+549)
// N * [ glob:NAME euid:32 egid:32 priority:8 omask:8 sclass_name:NAME trashretention:16 seteattr:8 clreattr:8 ]

// 0x0226
#define CLTOMA_INSTANCE_NAME (PROTO_BASE+550)
// -

// 0x0227
#define MATOCL_INSTANCE_NAME (PROTO_BASE+551)
// iname:NAME

// 0x0228
#define CLTOMA_FULL_DIRECTORY_DATA (PROTO_BASE+552)
// msgid:32 inode:32 maxentries:32 continueid:64 [ flags:8 ]

// 0x0229
#define MATOCL_FULL_DIRECTORY_DATA (PROTO_BASE+553)
// msgid:32 status:8
// msgid:32 continueid:64 N * [ name:NAME inode:32 attr:ATTR [ chunkcnt:32 chunkcnt * [ chunkid:64 ] ] [ length:32 path:lengthB ] [ xattr_cnt:16 xattr_cnt * [ xattr_name_leng:8 xattr_name:xattr_name_lengB xattr_value_leng:16 xattr_value:xattr_value_lengB ] ] [ acl_cnt:8 acl_cnt * [acltype:8 userperm:16 groupperm:16 otherperm:16 mask:16 namedusers:16 namedgroups:16 namedusers * [ id:32 perm:16 ] namedgroups * [ id:32 perm:16 ] ] ] ]

// 0x022A
#define CLTOMA_SET_ALL_NODE_ATTRIBUTES (PROTO_BASE+554)
// msgid:32 inode:32 flags:8 [ attr:ATTR ] [ eattr:8 ] [ xattrcnt:16 xattrcnt * [xattr_name_leng:8 xattr_name:xattr_name_lengB xattr_value_leng:16 xattr_value:xattr_value_lengB ] ] [ acl_cnt:8 acl_cnt * [acltype:8 userperm:16 groupperm:16 otherperm:16 mask:16 namedusers:16 namedgroups:16 namedusers * [ id:32 perm:16 ] namedgroups * [ id:32 perm:16 ] ] ]

// 0x022B
#define MATOCL_SET_ALL_NODE_ATTRIBUTES (PROTO_BASE+555)
// msgid:32 statis:8

// CHUNKSERVER STATS

// 0x0258
#define CLTOCS_HDD_LIST (PROTO_BASE+600)
// -

// 0x0259
#define CSTOCL_HDD_LIST (PROTO_BASE+601)
// N * [ entrysize:16 path:NAME flags:8 errchunkid:64 errtime:32 used:64 total:64 chunkscount:32 3 * [ bytesread:64 byteswritten:64 usecread:64 usecwrite:64 usecfsync:64 readops:32 writeops:32 fsyncops:32 usecreadmax:32 usecwritemax:32 usecfsyncmax:32 ] ]



// CLIENT <-> MASTER meta data synchronization


#define CLTOMA_FUSE_SUSTAINED_INODES (PROTO_BASE+700)
// N*[ inode:32 ]

#define CLTOMA_FUSE_AMTIME_INODES (PROTO_BASE+701)
// N*[ inode:32 atime:32 mtime:32 ]

#define MATOCL_FUSE_CHUNK_HAS_CHANGED (PROTO_BASE+702)
// zero:32 inode:32 index:32 chunkid:64 version:32 fleng:64 truncateflag:8

#define MATOCL_FUSE_FLENG_HAS_CHANGED (PROTO_BASE+703)
// zero:32 inode:32 fleng:64

#define CLTOMA_FUSE_TIME_SYNC (PROTO_BASE+704)
// [ msgid:32 ]

#define MATOCL_FUSE_TIME_SYNC (PROTO_BASE+705)
// [ msgid:32 ] timestamp_useconds:64

#define MATOCL_FUSE_INVALIDATE_CHUNK_CACHE (PROTO_BASE+706)
// zero:32

// #define MATOCL_FUSE_INVALIDATE_DATA_CACHE (PROTO_BASE+707)
// zero:32 inode:32

#define CLTOMA_FUSE_OPDATA (PROTO_BASE+710)
// readbytes:64 writebytes:64 readcnt:32 writecnt:32 fsynccnt:32

#define CLTOMA_FUSE_WFLAGS (PROTO_BASE+711)
// wflags:8


#endif
