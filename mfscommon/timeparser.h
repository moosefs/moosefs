#ifndef _TIMEPARSER_H_
#define _TIMEPARSER_H_

#include <inttypes.h>
#include <stdlib.h>

int snprint_speriod(char *where,size_t mleng,uint32_t period);
int snprint_hperiod(char *where,size_t mleng,uint32_t period);

#define TPARSE_OK 0
#define TPARSE_UNEXPECTED_CHAR -1
#define TPARSE_VALUE_TOO_BIG -2

int parse_speriod(const char *str,uint32_t *ret);
int parse_hperiod(const char *str,uint32_t *ret);

#endif
