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

#include <inttypes.h>

#include "mastercomm.h"
#include "portable.h"
#include "MFSCommunication.h"

uint8_t do_truncate(uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t attrlength,uint8_t attr[ATTR_RECORD_SIZE],uint64_t *prevlength) {
	uint8_t status;
	uint32_t trycnt;
	trycnt = 0;
	while (1) {
		status = fs_truncate(inode,flags,uid,gids,gid,attrlength,attr,prevlength);
		if (status==MFS_STATUS_OK || status==MFS_ERROR_EROFS || status==MFS_ERROR_EACCES || status==MFS_ERROR_EPERM || status==MFS_ERROR_ENOENT || status==MFS_ERROR_QUOTA || status==MFS_ERROR_NOSPACE || status==MFS_ERROR_CHUNKLOST) {
			break;
		} else if (status!=MFS_ERROR_LOCKED) {
			trycnt++;
			if (trycnt>=30) {
				break;
			}
			portable_usleep(1000+((trycnt<30)?((trycnt-1)*300000):10000000));
		} else {
			portable_usleep(10000);
		}
	}
	return status;
}
