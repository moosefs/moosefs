#ifndef _SUSTAINED_PARENTS_H_
#define _SUSTAINED_PARENTS_H_

void sparents_add(uint32_t inode,uint32_t parent,uint32_t timeout);
uint32_t sparents_get(uint32_t inode);

void sparents_term(void);
void sparents_init(void);

#endif
