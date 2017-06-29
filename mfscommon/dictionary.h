#ifndef _DICTIONARY_H_
#define _DICTIONARY_H_

#include <inttypes.h>

int dict_init(void);
void dict_cleanup(void);
void* dict_search(const uint8_t *data,uint32_t leng);
void* dict_insert(const uint8_t *data,uint32_t leng);
const uint8_t* dict_get_ptr(void *dptr);
uint32_t dict_get_leng(void *dptr);
uint32_t dict_get_hash(void *dptr);
void dict_dec_ref(void *dptr);
void dict_inc_ref(void *dptr);

#endif
