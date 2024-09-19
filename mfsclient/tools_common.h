#ifndef _TOOLS_COMMON_H_
#define _TOOLS_COMMON_H_

#include <inttypes.h>
#include <sys/types.h>

#define tcpread(s,b,l) tcptoread(s,b,l,10000,30000)
#define tcpwrite(s,b,l) tcptowrite(s,b,l,10000,30000)

typedef int (*execfn)(int argc,char *argv[]);

/*
typedef struct _command {
	const char *command;
	const char *subcommand;
	execfn executable;
} command;
*/

typedef struct _command {
	const char *cmd;
	execfn handler;
//	const char *desc;
} command;

#define MAXCMD 50

#define _EXENAME_ "_EXENAME_"
#define _NUMBERDESC_ "_NUMBERDESC_"
#define _RECURSIVEDESC_	"_RECURSIVEDESC_"
#define _MOUNTPOINTDESC_ "_MOUNTPOINTDESC_"
#define _QMARKDESC_ "_QMARKDESC_"

uint32_t getmasterversion(void);

void set_hu_flags(int ch);

#define PNUM_NONE 0
#define PNUM_BYTES 1
#define PNUM_32BIT 2
#define PNUM_VALID 4
#define PNUM_ALIGN 8

void print_number(uint64_t number,uint8_t flags); // uses PNUM_BYTES,PNUM_32BIT,PNUM_ALIGN
void print_number_desc(const char *prefix, const char *suffix, uint64_t number, uint8_t flags); // uses PNUM_BYTES,PNUM_32BIT
void print_number_quota(const char *prefix, const char *suffix, uint64_t number, uint8_t flags); // all flags are used
uint8_t print_number_size(uint8_t flags); // uses PNUM_32BIT

int my_get_number(const char *str,uint64_t *ret,uint64_t max,uint8_t bytesflag);

int tparse_seconds(const char *str,uint32_t *ret);
int tparse_hours(const char *str,uint16_t *ret);

int master_socket(void);
void reset_master(void);
int master_reconnect(void);
int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, uint64_t *leng, uint8_t needsamedev, uint8_t needrwfs);

void print_humanized_number(const char *format, uint64_t number, uint8_t flags);
void dirname_inplace(char *path);
int bsd_basename(const char *path, char *bname);
int bsd_dirname(const char *path, char *bname);

int mystrmatch(const char *pat, const char *str, uint8_t insens);

void tcomm_print_help(const char *strings[]);
int tcomm_find_and_execute(int argc,char *argv[],const command *command_list);

#endif
