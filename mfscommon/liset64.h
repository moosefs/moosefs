#ifndef _LISET64_H_
#define _LISET64_H_

#include <inttypes.h>

int liset_new();
void liset_clear(int setid);
void liset_remove(int setid);
uint64_t liset_card(int setid);
int liset_addval(int setid,uint64_t data);
int liset_delval(int setid,uint64_t data);
int liset_check(int setid,uint64_t data);

#endif
