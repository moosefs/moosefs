#ifndef _SEARCHEXPR_H_
#define _SEARCHEXPR_H_

#include <inttypes.h>

typedef struct _inode {
	uint32_t inode;
	uint8_t type;
	uint8_t sclass;
	uint8_t flags;
	uint8_t winattr;
	uint16_t nlink;
	uint16_t mode;
	uint16_t uid;
	uint16_t gid;
	uint32_t atime;
	uint32_t mtime;
	uint32_t ctime;
	uint32_t trashretention;
	uint32_t time;
	uint16_t major;
	uint16_t minor;
	uint64_t length;
	uint64_t chunkid;
} inodestr;

void* expr_new(const char *str);
void expr_free(void *ev);
void expr_print(void *ev);
uint8_t expr_useschunkid(void *ev);
uint8_t expr_check(void *ev,inodestr *inodedata);

#endif
