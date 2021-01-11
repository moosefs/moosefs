/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <inttypes.h>

#include "clocks.h"
#include "massert.h"

#define I2S_TIMEOUT 1.0

typedef struct _iptosesid {
	uint32_t ip;
	uint32_t sessionid;
	double time;
	struct _iptosesid *next;
} iptosesid;

static iptosesid *head = NULL;

void iptosesid_add(uint32_t ip,uint32_t sessionid) {
	iptosesid *i2s;
	i2s = malloc(sizeof(iptosesid));
	passert(i2s);
	i2s->ip = ip;
	i2s->sessionid = sessionid;
	i2s->time = monotonic_seconds();
	i2s->next = head;
	head = i2s;
}

uint8_t iptosesid_check(uint32_t ip) {
	iptosesid *i2s,**i2sp;
	double now;

	i2sp = &head;
	now = monotonic_seconds();

	while ((i2s = *i2sp)!=NULL) {
		if (i2s->time + I2S_TIMEOUT < now) {
			*i2sp = i2s->next;
			free(i2s);
		} else if (i2s->ip == ip) {
			return 1;
		} else {
			i2sp = &(i2s->next);
		}
	}
	return 0;
}

uint32_t iptosesid_get(uint32_t ip) {
	iptosesid *i2s,**i2sp;
	uint32_t sessionid;
	double now;

	i2sp = &head;
	now = monotonic_seconds();

	while ((i2s = *i2sp)!=NULL) {
		if (i2s->time + I2S_TIMEOUT < now) {
			*i2sp = i2s->next;
			free(i2s);
		} else if (i2s->ip == ip) {
			*i2sp = i2s->next;
			sessionid = i2s->sessionid;
			free(i2s);
			return sessionid;
		} else {
			i2sp = &(i2s->next);
		}
	}
	return 0;
}
