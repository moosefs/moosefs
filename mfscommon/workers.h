#ifndef _WORKERS_H_
#define _WORKERS_H_

#include <inttypes.h>

void* workers_init(uint32_t maxworkers,uint32_t sustainworkers,uint32_t qleng,char *name,void (*workerfn)(void *data,uint32_t current_workers_count));
void workers_term(void *w);
void workers_newjob(void *w,void *data);

#endif
