#ifndef _MFSIO_H_
#define _MFSIO_H_

#include <inttypes.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

typedef struct _mfscfg {
	char *masterhost;
	char *masterport;
	char *masterpassword;
	char *mountpoint;
	char *masterpath;
	int read_cache_mb;
	int write_cache_mb;
	int io_try_cnt;
	int error_on_lost_chunk;
	int error_on_no_space;
	int sugid_clear_mode;
	int mkdir_copy_sgid;
} mfscfg;

#ifndef UTIME_NOW
# define UTIME_NOW	((1l << 30) - 1l)
#endif
#ifndef UTIME_OMIT
# define UTIME_OMIT	((1l << 30) - 2l)
#endif

#ifndef LOCK_SH
#define LOCK_SH 1
#endif
#ifndef LOCK_EX
#define LOCK_EX 2
#endif
#ifndef LOCK_NB
#define LOCK_NB 4
#endif
#ifndef LOCK_UN
#define LOCK_UN 8
#endif

int mfs_mknod(const char *path, mode_t mode, dev_t dev);
int mfs_unlink(const char *path);
int mfs_mkdir(const char *path, mode_t mode);
int mfs_rename(const char *src, const char *dst);
int mfs_rmdir(const char *path);
int mfs_chmod(const char *path, mode_t mode);
int mfs_fchmod(int fildes, mode_t mode);
int mfs_chown(const char *path, uid_t owner, gid_t group);
int mfs_fchown(int fildes, uid_t owner, gid_t group);
int mfs_truncate(const char *path, off_t size);
int mfs_ftruncate(int fildes, off_t size);
int mfs_utimes(const char *path, const struct timeval times[2]);
int mfs_futimes(int fildes, const struct timeval times[2]);
int mfs_futimens(int fildes, const struct timespec times[2]);
off_t mfs_lseek(int fildes, off_t offset, int whence);
int mfs_stat(const char *path, struct stat *buf);
int mfs_fstat(int fildes, struct stat *buf);
int mfs_open(const char *path,int oflag,...);
ssize_t mfs_read(int fildes,void *buf,size_t nbyte);
ssize_t mfs_pread(int fildes,void *buf,size_t nbyte,off_t offset);
ssize_t mfs_write(int fildes,const void *buf,size_t nbyte);
ssize_t mfs_pwrite(int fildes,const void *buf,size_t nbyte,off_t offset);
int mfs_fsync(int fildes);
int mfs_close(int fildes);
int mfs_flock(int fildes, int op);
int mfs_lockf(int fildes, int function, off_t size);
int mfs_fcntl_locks(int fildes, int function, struct flock *fl);

// 0 - initialize everything
// 1 - connect to master only (pre fork section)
// 2 - initialize everything else (post fork section)

int mfs_init(mfscfg *mcfg,uint8_t stage);
void mfs_term(void);

#endif
