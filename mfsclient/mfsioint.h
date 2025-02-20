#ifndef _MFSIO_INT_H_
#define _MFSIO_INT_H_

// #include <limits.h> // NGROUPS_MAX
#include <inttypes.h>
#include "MFSCommunication.h" //constants

typedef struct _mfs_int_cfg {
	char *masterhost;
	char *masterport;
	char *masterbind;
	char *masterpassword;
	char *mastermd5pass;
	char *mountpoint;
	char *masterpath;
	char *preferedlabels;
	int read_cache_mb;
	int write_cache_mb;
	int io_try_cnt;
	int io_timeout;
	int min_log_entry;
	int readahead_leng;
	int readahead_trigger;
	int error_on_lost_chunk;
	int error_on_no_space;
	int sugid_clear_mode;
	int mkdir_copy_sgid;
	double lcache_retention;
	char *logident;
	int logdaemon;
	int logminlevel;
	int logelevateto;
	uint16_t master_min_version_maj;
	uint16_t master_min_version_mid;
} mfs_int_cfg;

#define MFS_NGROUPS_MAX 256

// mfs_int_*chown owner and group
#define MFS_UGID_NONE UINT32_C(0xFFFFFFFF)

// mfs_int_*times flags
#define MFS_TIMES_ATIME_NOW 1
#define MFS_TIMES_MTIME_NOW 2
#define MFS_TIMES_ATIME_OMIT 4
#define MFS_TIMES_MTIME_OMIT 8

// mfs_int_lseek whence / flock.whence
#define MFS_SEEK_SET 0
#define MFS_SEEK_CUR 1
#define MFS_SEEK_END 2

// mfs_int_open oflags
#define MFS_O_RDONLY 0
#define MFS_O_WRONLY 1
#define MFS_O_RDWR 2
#define MFS_O_ATTRONLY 3
#define MFS_O_ACCMODE 3
#define MFS_O_CREAT 4
#define MFS_O_TRUNC 8
#define MFS_O_EXCL 16
#define MFS_O_APPEND 32

// mfs_int_flock op
#define MFS_LOCK_SH 1
#define MFS_LOCK_EX 2
#define MFS_LOCK_NB 4
#define MFS_LOCK_UN 8

// mfs_int_lockf function
#define MFS_F_ULOCK 0
#define MFS_F_LOCK 1
#define MFS_F_TLOCK 2
#define MFS_F_TEST 3

// mfs_int_fcntl_locks function
#define MFS_F_GETLK 0
#define MFS_F_SETLK 1
#define MFS_F_SETLKW 2

// flock.type
#define MFS_F_RDLCK 0
#define MFS_F_WRLCK 1
#define MFS_F_UNLCK 2

typedef struct _mfs_int_cred {
	uint16_t umask;
	uint32_t uid;
	uint32_t gidcnt;
	uint32_t gidtab[MFS_NGROUPS_MAX];
} mfs_int_cred;

typedef struct _mfs_int_statfsrec {
	uint64_t totalspace;
	uint64_t availspace;
	uint64_t freespace;
	uint64_t trashspace;
	uint64_t sustainedspace;
	uint32_t inodes;
	uint32_t masterip;
	uint16_t masterport;
	uint32_t sessionid;
	uint64_t masterprocessid;
	uint32_t masterversion;
} mfs_int_statfsrec;

typedef struct _mfs_int_statrec {
	uint32_t inode;
	uint8_t type;
	uint8_t winattr;
	uint16_t mode;
	uint32_t uid,gid;
	uint32_t atime,mtime,ctime;
	uint32_t nlink,dev;
	uint64_t length;
} mfs_int_statrec;

typedef struct _mfs_int_flockrec {
	uint8_t type;
	uint8_t whence;
	int64_t start;
	int64_t len;
	uint32_t pid;
} mfs_int_flockrec;

typedef struct _mfs_int_direntry {
	uint32_t inode;
	uint8_t type;
	uint8_t name[MFS_NAME_MAX+1];
} mfs_int_direntry;

typedef struct _mfs_int_direntryplus {
	uint32_t inode;
	uint8_t type;
	uint8_t winattr;
	uint16_t mode;
	uint32_t uid,gid;
	uint32_t atime,mtime,ctime;
	uint32_t nlink,dev;
	uint64_t length;
	uint8_t name[MFS_NAME_MAX+1];
} mfs_int_direntryplus;

typedef struct _data_buff {
	uint32_t leng;
	uint8_t data[1];
} data_buff;

#ifdef __cplusplus
extern "C" {
#endif

uint8_t mfs_int_mknod(mfs_int_cred *cr, const char *path, uint8_t type, uint16_t mode, uint32_t dev);
uint8_t mfs_int_unlink(mfs_int_cred *cr, const char *path);
uint8_t mfs_int_mkdir(mfs_int_cred *cr, const char *path, uint16_t mode);
uint8_t mfs_int_rename(mfs_int_cred *cr, const char *src, const char *dst);
uint8_t mfs_int_rmdir(mfs_int_cred *cr, const char *path);
uint8_t mfs_int_link(mfs_int_cred *cr, const char *src, const char *dst);
uint8_t mfs_int_symlink(mfs_int_cred *cr, const char *nodepath, const char *linkpath);
uint8_t mfs_int_readlink(mfs_int_cred *cr, const char *nodepath, char linkpath[MFS_SYMLINK_MAX]);
uint8_t mfs_int_setwinattr(mfs_int_cred *cr, const char *path, uint8_t winattr);
uint8_t mfs_int_fsetwinattr(mfs_int_cred *cr, int fildes, uint8_t winattr);
uint8_t mfs_int_chmod(mfs_int_cred *cr, const char *path, uint16_t mode);
uint8_t mfs_int_fchmod(mfs_int_cred *cr, int fildes, uint16_t mode);
uint8_t mfs_int_chown(mfs_int_cred *cr, const char *path, uint32_t owner, uint32_t group);
uint8_t mfs_int_fchown(mfs_int_cred *cr, int fildes, uint32_t owner, uint32_t group);
uint8_t mfs_int_utimes(mfs_int_cred *cr, const char *path, uint8_t flags, uint32_t atime, uint32_t mtime);
uint8_t mfs_int_futimes(mfs_int_cred *cr, int fildes, uint8_t flags, uint32_t atime, uint32_t mtime);
uint8_t mfs_int_truncate(mfs_int_cred *cr, const char *path, int64_t size);
uint8_t mfs_int_ftruncate(mfs_int_cred *cr, int fildes, int64_t size);
uint8_t mfs_int_lseek(int fildes, int64_t *offset, uint8_t whence);
uint8_t mfs_int_stat(mfs_int_cred *cr, const char *path, mfs_int_statrec *buf);
uint8_t mfs_int_fstat(mfs_int_cred *cr, int fildes, mfs_int_statrec *buf);

uint8_t mfs_int_getxattr(mfs_int_cred *cr, const char *path, const char *name, const uint8_t **vbuff, uint32_t *vleng, uint8_t mode);
uint8_t mfs_int_fgetxattr(mfs_int_cred *cr, int fildes, const char *name, const uint8_t **vbuff, uint32_t *vleng, uint8_t mode);
uint8_t mfs_int_setxattr(mfs_int_cred *cr, const char *path, const char *name, const uint8_t *value, uint32_t vsize, uint8_t mode);
uint8_t mfs_int_fsetxattr(mfs_int_cred *cr, int fildes, const char *name, const uint8_t *value, uint32_t vsize, uint8_t mode);
uint8_t mfs_int_removexattr(mfs_int_cred *cr, const char *path, const char *name);
uint8_t mfs_int_fremovexattr(mfs_int_cred *cr, int fildes, const char *name);
uint8_t mfs_int_listxattr(mfs_int_cred *cr, const char *path, int32_t *rsize, char *list, uint32_t size);
uint8_t mfs_int_flistxattr(mfs_int_cred *cr, int fildes, int32_t *rsize, char *list, uint32_t size);

uint8_t mfs_int_getfacl(mfs_int_cred *cr, const char *path, uint8_t acltype, uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *maskperm,uint16_t *namedusers,uint16_t *namedgroups,const uint8_t **namedacls,uint32_t *namedaclssize);
uint8_t mfs_int_fgetfacl(mfs_int_cred *cr, int fildes, uint8_t acltype, uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *maskperm,uint16_t *namedusers,uint16_t *namedgroups,const uint8_t **namedacls,uint32_t *namedaclssize);
uint8_t mfs_int_setfacl(mfs_int_cred *cr, const char *path, uint8_t acltype, uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t maskperm,uint16_t namedusers,uint16_t namedgroups,uint8_t *namedacls,uint32_t namedaclssize);
uint8_t mfs_int_fsetfacl(mfs_int_cred *cr, int fildes, uint8_t acltype, uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t maskperm,uint16_t namedusers,uint16_t namedgroups,uint8_t *namedacls,uint32_t namedaclssize);

uint8_t mfs_int_statfs(mfs_int_statfsrec *buf);
uint8_t mfs_int_open(mfs_int_cred *cr, int *fildes, const char *path, int oflag, int mode);
uint8_t mfs_int_pread(int fildes,int64_t *rsize,uint8_t *buf,uint64_t nbyte,uint64_t offset);
uint8_t mfs_int_read(int fildes,int64_t *rsize,uint8_t *buf,uint64_t nbyte);
uint8_t mfs_int_pwrite(int fildes,int64_t *rsize,const uint8_t *buf,uint64_t nbyte,uint64_t offset);
uint8_t mfs_int_write(int fildes,int64_t *rsize,const uint8_t *buf,uint64_t nbyte);
uint8_t mfs_int_fsync(int fildes);
uint8_t mfs_int_close(int fildes);
uint8_t mfs_int_flock(int fildes, uint8_t op);
uint8_t mfs_int_lockf(int fildes, uint32_t pid, uint8_t function, int64_t size);
uint8_t mfs_int_fcntl_locks(int fildes, uint32_t pid, uint8_t function, mfs_int_flockrec *fl);
uint8_t mfs_int_opendir(mfs_int_cred *cr, int *dirdes, const char *path);
uint8_t mfs_int_readdir(mfs_int_cred *cr, int dirdes, mfs_int_direntry *de);
uint8_t mfs_int_readdirplus(mfs_int_cred *cr, int dirdes, mfs_int_direntryplus *de);
uint8_t mfs_int_telldir(int dirdes, uint64_t *offset);
uint8_t mfs_int_seekdir(int dirdes, uint64_t offset);
uint8_t mfs_int_rewinddir(int dirdes);
uint8_t mfs_int_closedir(int dirdes);

char* mfs_int_get_config_str(const char *option_name);
data_buff* mfs_int_get_config_file(const char *option_name);

// 0 - initialize everything
// 1 - connect to master only (pre fork section)
// 2 - initialize everything else (post fork section)

void mfs_int_set_defaults(mfs_int_cfg *mcfg);
int mfs_int_init(mfs_int_cfg *mcfg,uint8_t stage);
void mfs_int_term(void);

#ifdef __cplusplus
}
#endif

#endif
